#!/usr/bin/env python3
# pylint: disable=missing-docstring,not-an-iterable,too-many-locals,too-many-arguments,invalid-name,too-many-return-statements,too-many-branches,len-as-condition,too-many-nested-blocks,wrong-import-order,duplicate-code, anomalous-backslash-in-string, too-many-statements, singleton-comparison, consider-using-in

import singer
import datetime
import pytz
import decimal
import psycopg2
import copy
import json
import singer.metadata as metadata
import tap_postgres.db as post_db
import tap_postgres.sync_strategies.common as sync_common
from singer import utils, get_bookmark
from dateutil.parser import parse
from functools import reduce

LOGGER = singer.get_logger('tap_postgres')

UPDATE_BOOKMARK_PERIOD = 10000

def get_pg_version(conn_info):
    with post_db.open_connection(conn_info, False) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT setting::int AS version FROM pg_settings WHERE name='server_version_num'")
            version = cur.fetchone()[0]
    LOGGER.debug("Detected PostgreSQL version: {}".format(version))
    return version


def lsn_to_int(lsn):
    """Convert pg_lsn to int"""

    if not lsn:
        return None

    file, index = lsn.split('/')
    lsni = (int(file, 16)  << 32) + int(index, 16)
    return(lsni)


def int_to_lsn(lsni):
    """Convert int to pg_lsn"""

    if not lsni:
        return None

    # Convert the integer to binary
    lsnb = '{0:b}'.format(lsni)

    # file is the binary before the 32nd character, converted to hex
    if len(lsnb) > 32:
        file = (format(int(lsnb[:-32], 2), 'x')).upper()
    else:
        file = '0'

    # index is the binary from the 32nd character, converted to hex
    index = (format(int(lsnb[-32:], 2), 'x')).upper()
    # Formatting
    lsn = "{}/{}".format(file, index)
    return(lsn)


def fetch_current_lsn(conn_config):
    version = get_pg_version(conn_config)
    # Make sure PostgreSQL version is 9.4 or higher
    # Do not allow minor versions with PostgreSQL BUG #15114
    if (version >= 110000) and (version < 110002):
        raise Exception('PostgreSQL upgrade required to minor version 11.2')
    elif (version >= 100000) and (version < 100007):
        raise Exception('PostgreSQL upgrade required to minor version 10.7')
    elif (version >= 90600) and (version < 90612):
        raise Exception('PostgreSQL upgrade required to minor version 9.6.12')
    elif (version >= 90500) and (version < 90516):
        raise Exception('PostgreSQL upgrade required to minor version 9.5.16')
    elif (version >= 90400) and (version < 90421):
        raise Exception('PostgreSQL upgrade required to minor version 9.4.21')
    elif (version < 90400):
        raise Exception('Logical replication not supported before PostgreSQL 9.4')

    with post_db.open_connection(conn_config, False) as conn:
        with conn.cursor() as cur:
            # Use version specific lsn command
            if version >= 100000:
                cur.execute("SELECT pg_current_wal_lsn() AS current_lsn")
            elif version >= 90400:
                cur.execute("SELECT pg_current_xlog_location() AS current_lsn")
            else:
                raise Exception('Logical replication not supported before PostgreSQL 9.4')

            current_lsn = cur.fetchone()[0]
            return lsn_to_int(current_lsn)

def add_automatic_properties(stream, conn_config):
    stream['schema']['properties']['_sdc_deleted_at'] = {'type' : ['null', 'string'], 'format' :'date-time'}
    if conn_config.get('debug_lsn'):
        LOGGER.debug('debug_lsn is ON')
        stream['schema']['properties']['_sdc_lsn'] = {'type' : ['null', 'string']}
    else:
        LOGGER.debug('debug_lsn is OFF')

    return stream

def get_stream_version(tap_stream_id, state):
    stream_version = singer.get_bookmark(state, tap_stream_id, 'version')

    if stream_version is None:
        raise Exception("version not found for log miner {}".format(tap_stream_id))

    return stream_version

def tuples_to_map(accum, t):
    accum[t[0]] = t[1]
    return accum

def create_hstore_elem_query(elem):
    return psycopg2.sql.SQL("SELECT hstore_to_array({})").format(psycopg2.sql.Literal(elem))

def create_hstore_elem(conn_info, elem):
    with post_db.open_connection(conn_info) as conn:
        with conn.cursor() as cur:
            sql_stmt = create_hstore_elem_query(elem)
            cur.execute(sql_stmt)
            res = cur.fetchone()[0]
            hstore_elem = reduce(tuples_to_map, [res[i:i + 2] for i in range(0, len(res), 2)], {})
            return hstore_elem

def create_array_elem(elem, sql_datatype, conn_info):
    if elem is None:
        return None

    with post_db.open_connection(conn_info) as conn:
        with conn.cursor() as cur:
            if sql_datatype == 'bit[]':
                cast_datatype = 'boolean[]'
            elif sql_datatype == 'boolean[]':
                cast_datatype = 'boolean[]'
            elif sql_datatype == 'character varying[]':
                cast_datatype = 'character varying[]'
            elif sql_datatype == 'cidr[]':
                cast_datatype = 'cidr[]'
            elif sql_datatype == 'citext[]':
                cast_datatype = 'text[]'
            elif sql_datatype == 'date[]':
                cast_datatype = 'text[]'
            elif sql_datatype == 'double precision[]':
                cast_datatype = 'double precision[]'
            elif sql_datatype == 'hstore[]':
                cast_datatype = 'text[]'
            elif sql_datatype == 'integer[]':
                cast_datatype = 'integer[]'
            elif sql_datatype == 'inet[]':
                cast_datatype = 'inet[]'
            elif sql_datatype == 'json[]':
                cast_datatype = 'text[]'
            elif sql_datatype == 'jsonb[]':
                cast_datatype = 'text[]'
            elif sql_datatype == 'macaddr[]':
                cast_datatype = 'macaddr[]'
            elif sql_datatype == 'money[]':
                cast_datatype = 'text[]'
            elif sql_datatype == 'numeric[]':
                cast_datatype = 'text[]'
            elif sql_datatype == 'real[]':
                cast_datatype = 'real[]'
            elif sql_datatype == 'smallint[]':
                cast_datatype = 'smallint[]'
            elif sql_datatype == 'text[]':
                cast_datatype = 'text[]'
            elif sql_datatype in ('time without time zone[]', 'time with time zone[]'):
                cast_datatype = 'text[]'
            elif sql_datatype in ('timestamp with time zone[]', 'timestamp without time zone[]'):
                cast_datatype = 'text[]'
            elif sql_datatype == 'uuid[]':
                cast_datatype = 'text[]'

            else:
                #custom datatypes like enums
                cast_datatype = 'text[]'

            sql_stmt = """SELECT $stitch_quote${}$stitch_quote$::{}""".format(elem, cast_datatype)
            cur.execute(sql_stmt)
            res = cur.fetchone()[0]
            return res

#pylint: disable=too-many-branches,too-many-nested-blocks
def selected_value_to_singer_value_impl(elem, og_sql_datatype, conn_info):
    sql_datatype = og_sql_datatype.replace('[]', '')

    if elem is None:
        return elem
    if sql_datatype == 'timestamp without time zone':
        return parse(elem).isoformat() + '+00:00'
    if sql_datatype == 'timestamp with time zone':
        if isinstance(elem, datetime.datetime):
            return elem.isoformat()

        return parse(elem).isoformat()
    if sql_datatype == 'date':
        if  isinstance(elem, datetime.date):
            #logical replication gives us dates as strings UNLESS they from an array
            return elem.isoformat() + 'T00:00:00+00:00'
        return parse(elem).isoformat() + "+00:00"
    if sql_datatype == 'time with time zone':
        '''time with time zone values will be converted to UTC and time zone dropped'''
        # Replace hour=24 with hour=0
        if elem.startswith('24'): elem = elem.replace('24','00',1)
        # convert to UTC
        elem = elem + '00'
        elem_obj = datetime.datetime.strptime(elem, '%H:%M:%S%z')
        if elem_obj.utcoffset() != datetime.timedelta(seconds=0):
            LOGGER.warning('time with time zone values are converted to UTC'.format(og_sql_datatype))
        elem_obj = elem_obj.astimezone(pytz.utc)
        # drop time zone
        elem = elem_obj.strftime('%H:%M:%S')
        return parse(elem).isoformat().split('T')[1]
    if sql_datatype == 'time without time zone':
        # Replace hour=24 with hour=0
        if elem.startswith('24'):
            elem = elem.replace('24','00',1)
        return parse(elem).isoformat().split('T')[1]
    if sql_datatype == 'bit':
        #for arrays, elem will == True
        #for ordinary bits, elem will == '1'
        return elem == '1' or elem == True
    if sql_datatype == 'boolean':
        return elem
    if sql_datatype == 'hstore':
        return create_hstore_elem(conn_info, elem)
    if 'numeric' in sql_datatype:
        return decimal.Decimal(elem)
    if isinstance(elem, int):
        return elem
    if isinstance(elem, float):
        return elem
    if isinstance(elem, str):
        return elem

    raise Exception("do not know how to marshall value of type {}".format(elem.__class__))

def selected_array_to_singer_value(elem, sql_datatype, conn_info):
    if isinstance(elem, list):
        return list(map(lambda elem: selected_array_to_singer_value(elem, sql_datatype, conn_info), elem))

    return selected_value_to_singer_value_impl(elem, sql_datatype, conn_info)

def selected_value_to_singer_value(elem, sql_datatype, conn_info):
    #are we dealing with an array?
    if sql_datatype.find('[]') > 0:
        cleaned_elem = create_array_elem(elem, sql_datatype, conn_info)
        return list(map(lambda elem: selected_array_to_singer_value(elem, sql_datatype, conn_info), (cleaned_elem or [])))

    return selected_value_to_singer_value_impl(elem, sql_datatype, conn_info)

def row_to_singer_message(stream, row, version, columns, time_extracted, md_map, conn_info):
    row_to_persist = ()
    md_map[('properties', '_sdc_deleted_at')] = {'sql-datatype' : 'timestamp with time zone'}
    md_map[('properties', '_sdc_lsn')] = {'sql-datatype' : "character varying"}

    for idx, elem in enumerate(row):
        sql_datatype = md_map.get(('properties', columns[idx])).get('sql-datatype')

        if not sql_datatype:
            LOGGER.info("No sql-datatype found for stream %s: %s", stream, columns[idx])
            raise Exception("Unable to find sql-datatype for stream {}".format(stream))

        cleaned_elem = selected_value_to_singer_value(elem, sql_datatype, conn_info)
        row_to_persist += (cleaned_elem,)

    rec = dict(zip(columns, row_to_persist))

    return singer.RecordMessage(
        stream=post_db.calculate_destination_stream_name(stream, md_map),
        record=rec,
        version=version,
        time_extracted=time_extracted)

def consume_message(streams, state, msg, time_extracted, conn_info, end_lsn):
    # Strip leading comma generated by write-in-chunks and parse valid JSON
    try:
        payload = json.loads(msg.payload.lstrip(','))
    except:
        return state

    lsn = msg.data_start

    streams_lookup = {}
    for s in streams:
        streams_lookup[s['tap_stream_id']] = s

    tap_stream_id = post_db.compute_tap_stream_id(payload['schema'], payload['table'])
    if streams_lookup.get(tap_stream_id) is None:
        return state

    target_stream = streams_lookup[tap_stream_id]
    stream_version = get_stream_version(target_stream['tap_stream_id'], state)
    stream_md_map = metadata.to_map(target_stream['metadata'])

    desired_columns = [c for c in target_stream['schema']['properties'].keys() if sync_common.should_sync_column(stream_md_map, c)]

    if payload['kind'] == 'insert':
        col_names = []
        col_vals = []
        for idx, col in enumerate(payload['columnnames']):
            if col in set(desired_columns):
                col_names.append(col)
                col_vals.append(payload['columnvalues'][idx])

        col_names = col_names + ['_sdc_deleted_at']
        col_vals = col_vals + [None]
        if conn_info.get('debug_lsn'):
            col_names = col_names + ['_sdc_lsn']
            col_vals = col_vals + [str(lsn)]
        record_message = row_to_singer_message(target_stream, col_vals, stream_version, col_names, time_extracted, stream_md_map, conn_info)

    elif payload['kind'] == 'update':
        col_names = []
        col_vals = []
        for idx, col in enumerate(payload['columnnames']):
            if col in set(desired_columns):
                col_names.append(col)
                col_vals.append(payload['columnvalues'][idx])

        col_names = col_names + ['_sdc_deleted_at']
        col_vals = col_vals + [None]

        if conn_info.get('debug_lsn'):
            col_vals = col_vals + [str(lsn)]
            col_names = col_names + ['_sdc_lsn']
        record_message = row_to_singer_message(target_stream, col_vals, stream_version, col_names, time_extracted, stream_md_map, conn_info)

    elif payload['kind'] == 'delete':
        col_names = []
        col_vals = []
        for idx, col in enumerate(payload['oldkeys']['keynames']):
            if col in set(desired_columns):
                col_names.append(col)
                col_vals.append(payload['oldkeys']['keyvalues'][idx])

        col_names = col_names + ['_sdc_deleted_at']
        col_vals = col_vals  + [singer.utils.strftime(time_extracted)]
        if conn_info.get('debug_lsn'):
            col_vals = col_vals + [str(lsn)]
            col_names = col_names + ['_sdc_lsn']
        record_message = row_to_singer_message(target_stream, col_vals, stream_version, col_names, time_extracted, stream_md_map, conn_info)

    else:
        raise Exception("unrecognized replication operation: {}".format(payload['kind']))

    singer.write_message(record_message)
    state = singer.write_bookmark(state, target_stream['tap_stream_id'], 'lsn', lsn)

    return state

def locate_replication_slot(conn_info):
    with post_db.open_connection(conn_info, False) as conn:
        with conn.cursor() as cur:
            db_specific_slot = "pipelinewise_{}".format(conn_info['dbname'].lower())
            cur.execute("SELECT * FROM pg_replication_slots WHERE slot_name = %s AND plugin = %s", (db_specific_slot, 'wal2json'))
            if len(cur.fetchall()) == 1:
                LOGGER.info("Using pg_replication_slot %s", db_specific_slot)
                return db_specific_slot
            else:
                raise Exception("Unable to find replication slot {} with wal2json".format(db_specific_slot))


def sync_tables(conn_info, logical_streams, state, end_lsn, state_file):
    state_comitted = state
    lsn_comitted = min([get_bookmark(state_comitted, s['tap_stream_id'], 'lsn') for s in logical_streams])
    start_lsn = lsn_comitted
    lsn_to_flush = None
    time_extracted = utils.now()
    slot = locate_replication_slot(conn_info)
    lsn_last_processed = None
    lsn_currently_processing = None
    lsn_received_timestamp = None
    lsn_processed_count = 0
    start_run_timestamp = datetime.datetime.utcnow()
    max_run_seconds = conn_info['max_run_seconds']
    break_at_end_lsn = conn_info['break_at_end_lsn']
    logical_poll_total_seconds = conn_info['logical_poll_total_seconds'] or 10800 #3 hours
    poll_interval = 10
    poll_timestamp = None

    selected_tables = []
    for s in logical_streams:
        selected_tables.append("{}.{}".format(s['metadata'][0]['metadata']['schema-name'], s['table_name']))

    for s in logical_streams:
        sync_common.send_schema_message(s, ['lsn'])

    version = get_pg_version(conn_info)

    # Create replication connection and cursor
    conn = post_db.open_connection(conn_info, True)
    cur = conn.cursor()

    # Set session wal_sender_timeout for PG12 and above
    if (version >= 120000):
        wal_sender_timeout = 10800000 #10800000ms = 3 hours
        LOGGER.info("Set session wal_sender_timeout = {} milliseconds".format(wal_sender_timeout))
        cur.execute("SET SESSION wal_sender_timeout = {}".format(wal_sender_timeout))

    try:
        LOGGER.info("Request wal streaming from {} to {} (slot {})".format(int_to_lsn(start_lsn), int_to_lsn(end_lsn), slot))
        # psycopg2 2.8.4 will send a keep-alive message to postgres every status_interval
        cur.start_replication(slot_name=slot, decode=True, start_lsn=start_lsn, status_interval=poll_interval, options={'write-in-chunks': 1, 'add-tables': ','.join(selected_tables)})
    except psycopg2.ProgrammingError:
        raise Exception("Unable to start replication with logical replication (slot {})".format(slot))

    lsn_received_timestamp = datetime.datetime.utcnow()
    poll_timestamp = datetime.datetime.utcnow()

    while True:
        # Disconnect when no data received for logical_poll_total_seconds
        # needs to be long enough to wait for the largest single wal payload to avoid unplanned timeouts
        poll_duration = (datetime.datetime.utcnow() - lsn_received_timestamp).total_seconds()
        if poll_duration > logical_poll_total_seconds:
            LOGGER.info("Breaking - {} seconds of polling with no data".format(poll_duration))
            break

        try:
            msg = cur.read_message()
        except Exception as e:
            LOGGER.error("{}".format(e))
            raise

        if msg:
            if (break_at_end_lsn) and (msg.data_start > end_lsn):
                LOGGER.info("Breaking - latest wal message {} is past end_lsn {}".format(int_to_lsn(msg.data_start), int_to_lsn(end_lsn)))
                break

            if datetime.datetime.utcnow() >= (start_run_timestamp + datetime.timedelta(seconds=max_run_seconds)):
                LOGGER.info("Breaking - reached max_run_seconds of {}".format(max_run_seconds))
                break

            state = consume_message(logical_streams, state, msg, time_extracted, conn_info, end_lsn)

            # When using wal2json with write-in-chunks, multiple messages can have the same lsn
            # This is to ensure we only flush to lsn that has completed entirely
            if lsn_currently_processing is None:
                lsn_currently_processing = msg.data_start
                LOGGER.info("First wal message received is {}".format(int_to_lsn(lsn_currently_processing)))

                # Flush Postgres wal up to lsn comitted in previous run, or first lsn received in this run
                lsn_to_flush = lsn_comitted
                if lsn_currently_processing < lsn_to_flush: lsn_to_flush = lsn_currently_processing
                LOGGER.info("Confirming write up to {}, flush to {}".format(int_to_lsn(lsn_to_flush), int_to_lsn(lsn_to_flush)))
                cur.send_feedback(write_lsn=lsn_to_flush, flush_lsn=lsn_to_flush, reply=True, force=True)

            elif (int(msg.data_start) > lsn_currently_processing):
                lsn_last_processed = lsn_currently_processing
                lsn_currently_processing = msg.data_start
                lsn_received_timestamp = datetime.datetime.utcnow()
                lsn_processed_count = lsn_processed_count + 1
                if lsn_processed_count >= UPDATE_BOOKMARK_PERIOD:
                    LOGGER.debug("Updating bookmarks for all streams to lsn = {} ({})".format(lsn_last_processed, int_to_lsn(lsn_last_processed)))
                    for s in logical_streams:
                        state = singer.write_bookmark(state, s['tap_stream_id'], 'lsn', lsn_last_processed)
                    singer.write_message(singer.StateMessage(value=copy.deepcopy(state)))
                    lsn_processed_count = 0

        # Every poll_interval, update latest comitted lsn position from the state_file
        if datetime.datetime.utcnow() >= (poll_timestamp + datetime.timedelta(seconds=poll_interval)):
            if lsn_currently_processing is None:
                LOGGER.info("Waiting for first wal message")
            else:
                LOGGER.info("Lastest wal message received was {}".format(int_to_lsn(lsn_last_processed)))
                try:
                    state_comitted_file = open(state_file)
                    state_comitted = json.load(state_comitted_file)
                except:
                    LOGGER.debug("Unable to open and parse {}".format(state_file))
                finally:
                    lsn_comitted = min([get_bookmark(state_comitted, s['tap_stream_id'], 'lsn') for s in logical_streams])
                    if (lsn_currently_processing > lsn_comitted) and (lsn_comitted > lsn_to_flush):
                        lsn_to_flush = lsn_comitted
                        LOGGER.info("Confirming write up to {}, flush to {}".format(int_to_lsn(lsn_to_flush), int_to_lsn(lsn_to_flush)))
                        cur.send_feedback(write_lsn=lsn_to_flush, flush_lsn=lsn_to_flush, reply=True, force=True)

            poll_timestamp = datetime.datetime.utcnow()

    # Close replication connection and cursor
    cur.close()
    conn.close()

    if lsn_last_processed:
        if lsn_comitted > lsn_last_processed:
            lsn_last_processed = lsn_comitted
            LOGGER.info("Current lsn_last_processed {} is older than lsn_comitted {}".format(int_to_lsn(lsn_last_processed), int_to_lsn(lsn_comitted)))

        LOGGER.info("Updating bookmarks for all streams to lsn = {} ({})".format(lsn_last_processed, int_to_lsn(lsn_last_processed)))
        for s in logical_streams:
            state = singer.write_bookmark(state, s['tap_stream_id'], 'lsn', lsn_last_processed)

    singer.write_message(singer.StateMessage(value=copy.deepcopy(state)))
    return state
