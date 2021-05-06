import unittest
import tap_postgres
import tap_postgres.sync_strategies.time_based as time_based
import tap_postgres.sync_strategies.common as pg_common
import singer
from singer import get_logger, metadata, write_bookmark
try:
    from tests.utils import get_test_connection, ensure_test_table, select_all_of_stream, insert_record, get_test_connection_config, set_time_based_replication_metadata_for_stream
except ImportError:
    from utils import get_test_connection, ensure_test_table, select_all_of_stream, set_time_based_replication_metadata_for_stream, insert_record, get_test_connection_config


LOGGER = get_logger()

CAUGHT_MESSAGES = []
COW_RECORD_COUNT = 0

def singer_write_message_no_cow(message):
    global COW_RECORD_COUNT

    if isinstance(message, singer.RecordMessage) and message.stream == 'public-COW':
        COW_RECORD_COUNT = COW_RECORD_COUNT + 1
        if COW_RECORD_COUNT > 2:
            raise Exception("simulated exception")
        CAUGHT_MESSAGES.append(message)
    else:
        CAUGHT_MESSAGES.append(message)

def singer_write_schema_ok(message):
    CAUGHT_MESSAGES.append(message)

def singer_write_message_ok(message):
    CAUGHT_MESSAGES.append(message)

def expected_record(fixture_row):
    expected_record = {}
    for k,v in fixture_row.items():
        expected_record[k.replace('"', '')] = v

    return expected_record

def do_not_dump_catalog(catalog):
    pass

tap_postgres.dump_catalog = do_not_dump_catalog

class TimeBasedReplication(unittest.TestCase):


    def setUp(self):
        table_spec_1 = {"columns": [{"name": "id", "type" : "serial",       "primary_key" : True},
                                    {"name" : 'name', "type": "character varying"},
                                    {"name" : 'colour', "type": "character varying"},
                                    {"name" : 'timestamp_ntz', "type": "timestamp without time zone"},
                                    {"name" : 'timestamp_tz', "type": "timestamp with time zone"},
                                    ],
                        "name" : 'COW'}
        ensure_test_table(table_spec_1)
        global COW_RECORD_COUNT
        COW_RECORD_COUNT = 0
        global CAUGHT_MESSAGES
        CAUGHT_MESSAGES.clear()

    def test_replication_success(self):
        # TODO: What are these?
        # singer.write_message = singer_write_message_no_cow
        # pg_common.write_schema_message = singer_write_message_ok

        conn_config = get_test_connection_config()
        streams = tap_postgres.do_discovery(conn_config)
        cow_stream = [s for s in streams if s['table_name'] == 'COW'][0]
        self.assertIsNotNone(cow_stream)
        cow_stream = select_all_of_stream(cow_stream)
        set_time_based_replication_metadata_for_stream(cow_stream, 'timestamp_ntz', '15 MINUTES')

        last_timestamp_ntz = '2020-09-01T12:12:12'

        with get_test_connection() as conn:
            conn.autocommit = True
            cur = conn.cursor()

            cow_rec = {'name' : 'betty', 'colour' : 'blue',
                       'timestamp_ntz': '2020-09-01 10:40:59', 'timestamp_tz': '2020-09-01 00:50:59+02'}
            insert_record(cur, 'COW', cow_rec)

            cow_rec = {'name' : 'freshy', 'colour' : 'brow',
                       'timestamp_ntz': '2020-09-01 10:40:59', 'timestamp_tz': '2020-09-01 00:50:59+02'}
            insert_record(cur, 'COW', cow_rec)

            cow_rec = {'name' : 'pooper', 'colour' : 'green',
                       'timestamp_ntz': '2020-09-01 11:40:59', 'timestamp_tz': '2020-09-01 01:50:59+02'}
            insert_record(cur, 'COW', cow_rec)

            cow_rec = {'name' : 'final boss', 'colour' : 'very very dark',
                       'timestamp_ntz': last_timestamp_ntz, 'timestamp_tz': '2020-09-01 01:50:59+02'}
            insert_record(cur, 'COW', cow_rec)

        state = {}

        test_connection_config = get_test_connection_config()
        tap_postgres.do_sync(test_connection_config, {'streams' : streams}, None, state)
        self.assertEqual(state['bookmarks']['public-COW']['replication_key_value'], last_timestamp_ntz + "+00:00")


if __name__ == "__main__":
    test1 = TimeBasedReplication()
    test1.setUp()
    test1.test_replication_success()
