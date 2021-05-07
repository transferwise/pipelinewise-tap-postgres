import copy
import time
import dateutil.parser
import psycopg2
import psycopg2.extras
import singer

from singer import utils
from functools import partial

import singer.metrics as metrics
import tap_postgres.db as post_db


LOGGER = singer.get_logger('tap_postgres')

UPDATE_BOOKMARK_PERIOD = 10000


# pylint: disable=invalid-name,missing-function-docstring
def fetch_max_replication_key(conn_config, replication_key, schema_name, table_name):
    with post_db.open_connection(conn_config, False) as conn:
        with conn.cursor() as cur:
            max_key_sql = """SELECT max({})
                              FROM {}""".format(post_db.prepare_columns_sql(replication_key),
                                                post_db.fully_qualified_table_name(schema_name, table_name))
            LOGGER.info("determine max replication key value: %s", max_key_sql)
            cur.execute(max_key_sql)
            max_key = cur.fetchone()[0]
            LOGGER.info("max replication key value: %s", max_key)
            return max_key

# pylint: disable=invalid-name,missing-function-docstring
def fetch_min_replication_key(conn_config, replication_key, schema_name, table_name):
    with post_db.open_connection(conn_config, False) as conn:
        with conn.cursor() as cur:
            max_key_sql = """SELECT min({})
                              FROM {}""".format(post_db.prepare_columns_sql(replication_key),
                                                post_db.fully_qualified_table_name(schema_name, table_name))
            LOGGER.info("determine min replication key value: %s", max_key_sql)
            cur.execute(max_key_sql)
            max_key = cur.fetchone()[0]
            LOGGER.info("min replication key value: %s", max_key)
            return max_key

def fetch_next_replication_key(conn_config, replication_key_value, replication_key_type, replication_time_interval):
    with post_db.open_connection(conn_config, False) as conn:
        with conn.cursor() as cur:
            next_key_sql = "SELECT CAST('{}' as {}) + INTERVAL '{}';".format(replication_key_value, replication_key_type, replication_time_interval)
            LOGGER.debug("Fetching next replication key after {}".format(replication_key_value))
            cur.execute(next_key_sql)
            next_key = cur.fetchone()[0]
            LOGGER.info("next replication key value: %s", next_key)
            return next_key

def fetch_bookmark_replication_key_value(state, stream):
    bookmark = singer.get_bookmark(state, stream['tap_stream_id'], 'replication_key_value')
    if bookmark:
        return dateutil.parser.parse(bookmark)
    else:
        return None

# pylint: disable=too-many-locals
def sync_table(conn_info, stream, state, desired_columns, md_map):
    time_extracted = utils.now()

    stream_version = singer.get_bookmark(state, stream['tap_stream_id'], 'version')
    if stream_version is None:
        stream_version = int(time.time() * 1000)

    state = singer.write_bookmark(state,
                                  stream['tap_stream_id'],
                                  'version',
                                  stream_version)
    singer.write_message(singer.StateMessage(value=copy.deepcopy(state)))

    schema_name = md_map.get(()).get('schema-name')

    escaped_columns = list(map(partial(post_db.prepare_columns_for_select_sql, md_map=md_map), desired_columns))

    activate_version_message = singer.ActivateVersionMessage(
        stream=post_db.calculate_destination_stream_name(stream, md_map),
        version=stream_version)


    singer.write_message(activate_version_message)

    replication_key = md_map.get((), {}).get('replication-key')

    replication_key_value = fetch_bookmark_replication_key_value(state, stream) or \
                            fetch_min_replication_key(conn_info, replication_key, schema_name, stream['table_name'] )

    replication_time_interval = md_map.get((), {}).get('replication-time-interval')
    replication_key_sql_datatype = md_map.get(('properties', replication_key)).get('sql-datatype')

    hstore_available = post_db.hstore_available(conn_info)
    with metrics.record_counter(None) as counter:
        with post_db.open_connection(conn_info) as conn:

            # Client side character encoding defaults to the value in postgresql.conf under client_encoding.
            # The server / db can also have its own configured encoding.
            with conn.cursor() as cur:
                cur.execute("show server_encoding")
                LOGGER.info("Current Server Encoding: %s", cur.fetchone()[0])
                cur.execute("show client_encoding")
                LOGGER.info("Current Client Encoding: %s", cur.fetchone()[0])

            if hstore_available:
                LOGGER.info("hstore is available")
                psycopg2.extras.register_hstore(conn)
            else:
                LOGGER.info("hstore is UNavailable")

            # Set initial conditions for sync
            max_replication_key_value = fetch_max_replication_key(conn_info, replication_key, schema_name, stream['table_name'])
            LOGGER.info("Beginning new time-based replication sync {version} from {min} to {max}".format(version=stream_version, min=replication_key_value, max=max_replication_key_value))
            next_replication_key_value = replication_key_value
            rows_saved = 0

            # Sync
            last_iteration = False
            while not last_iteration:
                if next_replication_key_value > max_replication_key_value:
                    # This lets us do 1 run that includes the max_replication_key_value as the last iteration
                    last_iteration = True

                with conn.cursor(cursor_factory=psycopg2.extras.DictCursor, name='pipelinewise') as cur:
                    cur.itersize = post_db.CURSOR_ITER_SIZE

                    select_sql = """SELECT {columns}
                                    FROM {source}
                                    WHERE {repl_key} >= '{repl_key_val}'::{repl_key_type}
                                    AND   {repl_key} < '{repl_key_val}'::{repl_key_type} + INTERVAL '{repl_time_interval}'""".format(
                        columns=','.join(escaped_columns),
                        source=post_db.fully_qualified_table_name(schema_name, stream['table_name']),
                        repl_key=post_db.prepare_columns_sql(replication_key),
                        repl_key_val=next_replication_key_value,
                        repl_key_type=replication_key_sql_datatype,
                        repl_time_interval=replication_time_interval
                    )

                    LOGGER.info('select statement: %s with itersize %s', select_sql, cur.itersize)
                    cur.execute(select_sql)

                    for rec in cur:
                        record_message = post_db.selected_row_to_singer_message(stream,
                                                                                rec,
                                                                                stream_version,
                                                                                desired_columns,
                                                                                time_extracted,
                                                                                md_map)

                        singer.write_message(record_message)
                        rows_saved = rows_saved + 1

                        replication_key_value = record_message.record[replication_key]
                        state = singer.write_bookmark(state,
                                                          stream['tap_stream_id'],
                                                          'replication_key_value',
                                                          replication_key_value)

                        if rows_saved % UPDATE_BOOKMARK_PERIOD == 0:
                            singer.write_message(singer.StateMessage(value=copy.deepcopy(state)))

                        counter.increment()
                    next_replication_key_value = fetch_next_replication_key(conn_info, next_replication_key_value, replication_key_sql_datatype, replication_time_interval)

    return state
