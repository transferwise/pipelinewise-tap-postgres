import copy
import json
import sys
import singer

from typing import List, Dict
from singer import metadata as metadata_util

from tap_postgres.db import open_connection
from tap_postgres.discovery_utils import discover_db

LOGGER = singer.get_logger('tap_postgres')


def dump_catalog(all_streams: List[Dict]) -> None:
    """
    Prints the catalog to the std output
    Args:
        all_streams: List of streams to dump
    """
    json.dump({'streams': all_streams}, sys.stdout, indent=2)


def is_selected_via_metadata(stream: Dict) -> bool:
    """
    Checks if stream is selected ia metadata
    Args:
        stream: stream dictionary

    Returns: True if selected, False otherwise.
    """
    table_md = metadata_util.to_map(stream['metadata']).get((), {})
    return table_md.get('selected', False)


def clear_state_on_replication_change(state: Dict,
                                      tap_stream_id: str,
                                      replication_key: str,
                                      replication_method: str) -> Dict:
    """
    Update state if replication method change is detected
    Returns: new state dictionary
    """
    # user changed replication, nuke state
    last_replication_method = singer.get_bookmark(state, tap_stream_id, 'last_replication_method')
    if last_replication_method is not None and (replication_method != last_replication_method):
        state = singer.reset_stream(state, tap_stream_id)

    # key changed
    if replication_method == 'INCREMENTAL' and \
            replication_key != singer.get_bookmark(state, tap_stream_id, 'replication_key'):
        state = singer.reset_stream(state, tap_stream_id)

    state = singer.write_bookmark(state, tap_stream_id, 'last_replication_method', replication_method)

    return state


def refresh_streams_schema(conn_config: Dict, streams: List[Dict]):
    """
    Updates the streams schema & metadata with new discovery
    The given streams list of dictionaries would be mutated and updated
    """
    LOGGER.debug('Refreshing streams schemas ...')

    LOGGER.debug('Current streams schemas %s', streams)

    # Run discovery to get the streams most up to date json schemas
    with open_connection(conn_config) as conn:
        new_discovery = {
            stream['tap_stream_id']: stream
            for stream in discover_db(conn, conn_config.get('filter_schemas'), [st['table_name'] for st in streams])
        }

    LOGGER.debug('New discovery schemas %s', new_discovery)

    # For every stream, update the schema and metadata from the corresponding discovered stream
    for idx, stream in enumerate(streams):
        discovered_stream = new_discovery[stream['tap_stream_id']]
        
        if not stream.get('schema', {}).get('properties'):
            discovered_schema = copy.deepcopy(discovered_stream['schema'])
            LOGGER.info('Overriding schema for %s with %s', stream['tap_stream_id'], discovered_schema)
            streams[idx]['schema'] = discovered_schema
            
        streams[idx]['metadata'] = _merge_stream_metadata(stream, discovered_stream)

    LOGGER.debug('Updated streams schemas %s', streams)


def _merge_stream_metadata(stream, discovered_stream):
    """
    Discovered metadata for a stream doesn't contain non-discoverable
    keys/values such as replication method, key, selected, or any other
    arbitrary overridden metadata from the catalog file. Merges the discovered
    metadata into the metadata from the catalog file.
    """
    stream_md = metadata_util.to_map(stream['metadata'])
    discovery_md = metadata_util.to_map(discovered_stream['metadata'])

    for breadcrumb, metadata in discovery_md.items():
        if breadcrumb in stream_md:
            stream_md[breadcrumb].update(metadata)
        else:
            stream_md[breadcrumb] = metadata

    return copy.deepcopy(metadata_util.to_list(stream_md))


def any_logical_streams(streams, default_replication_method):
    """
    Checks if streams list contains any stream with log_based method
    """
    for stream in streams:
        stream_metadata = metadata_util.to_map(stream['metadata'])
        replication_method = stream_metadata.get((), {}).get('replication-method', default_replication_method)
        if replication_method == 'LOG_BASED':
            return True

    return False
