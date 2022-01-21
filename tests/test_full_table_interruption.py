import re
import psycopg2
import unittest.mock
import pytest
import tap_postgres
import tap_postgres.sync_strategies.full_table as full_table
import tap_postgres.sync_strategies.common as pg_common
import singer
from singer import get_logger, metadata, write_bookmark
try:
    from tests.utils import get_test_connection, ensure_test_table, select_all_of_stream, set_replication_method_for_stream, insert_record, get_test_connection_config
except ImportError:
    from utils import get_test_connection, ensure_test_table, select_all_of_stream, set_replication_method_for_stream, insert_record, get_test_connection_config


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
full_table.UPDATE_BOOKMARK_PERIOD = 1

@pytest.mark.parametrize('use_secondary', [False, True])
@unittest.mock.patch('psycopg2.connect', wraps=psycopg2.connect)
class TestLogicalInterruption:
    maxDiff = None

    def setup_method(self):
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

    def test_catalog(self, mock_connect, use_secondary):
        singer.write_message = singer_write_message_no_cow
        pg_common.write_schema_message = singer_write_message_ok

        conn_config = get_test_connection_config(use_secondary=use_secondary)
        streams = tap_postgres.do_discovery(conn_config)

        # Assert that we connected to the correct database
        expected_connection = {
            'application_name': unittest.mock.ANY,
            'dbname': unittest.mock.ANY,
            'user': unittest.mock.ANY,
            'password': unittest.mock.ANY,
            'connect_timeout':unittest.mock.ANY,
            'host': conn_config['secondary_host'] if use_secondary else conn_config['host'],
            'port': conn_config['secondary_port'] if use_secondary else conn_config['port'],
        }
        mock_connect.assert_called_once_with(**expected_connection)
        mock_connect.reset_mock()

        cow_stream = [s for s in streams if s['table_name'] == 'COW'][0]
        assert cow_stream is not None
        cow_stream = select_all_of_stream(cow_stream)
        cow_stream = set_replication_method_for_stream(cow_stream, 'LOG_BASED')

        conn = get_test_connection()
        conn.autocommit = True

        with conn.cursor() as cur:
            cow_rec = {'name': 'betty', 'colour': 'blue',
                       'timestamp_ntz': '2020-09-01 10:40:59', 'timestamp_tz': '2020-09-01 00:50:59+02'}
            insert_record(cur, 'COW', cow_rec)

            cow_rec = {'name': 'smelly', 'colour': 'brow',
                       'timestamp_ntz': '2020-09-01 10:40:59 BC', 'timestamp_tz': '2020-09-01 00:50:59+02 BC'}
            insert_record(cur, 'COW', cow_rec)

            cow_rec = {'name': 'pooper', 'colour': 'green',
                       'timestamp_ntz': '30000-09-01 10:40:59', 'timestamp_tz': '10000-09-01 00:50:59+02'}
            insert_record(cur, 'COW', cow_rec)

        conn.close()

        blew_up_on_cow = False
        state = {}
        #the initial phase of cows logical replication will be a full table.
        #it will sync the first record and then blow up on the 2nd record
        try:
            tap_postgres.do_sync(get_test_connection_config(use_secondary=use_secondary), {'streams' : streams}, None, state)
        except Exception:
            blew_up_on_cow = True

        assert blew_up_on_cow is True

        mock_connect.assert_called_with(**expected_connection)
        mock_connect.reset_mock()

        assert 7 == len(CAUGHT_MESSAGES)

        assert CAUGHT_MESSAGES[0]['type'] =='SCHEMA'
        assert isinstance(CAUGHT_MESSAGES[1], singer.StateMessage)
        assert CAUGHT_MESSAGES[1].value['bookmarks']['public-COW'].get('xmin') is None
        assert CAUGHT_MESSAGES[1].value['bookmarks']['public-COW'].get('lsn') is not None
        end_lsn = CAUGHT_MESSAGES[1].value['bookmarks']['public-COW'].get('lsn')

        assert isinstance(CAUGHT_MESSAGES[2], singer.ActivateVersionMessage)
        new_version = CAUGHT_MESSAGES[2].version

        assert isinstance(CAUGHT_MESSAGES[3], singer.RecordMessage)
        assert CAUGHT_MESSAGES[3].record == {
            'colour': 'blue',
            'id': 1,
            'name': 'betty',
            'timestamp_ntz': '2020-09-01T10:40:59+00:00',
            'timestamp_tz': '2020-08-31T22:50:59+00:00'
        }

        assert 'public-COW' == CAUGHT_MESSAGES[3].stream

        assert isinstance(CAUGHT_MESSAGES[4], singer.StateMessage)
        #xmin is set while we are processing the full table replication
        assert CAUGHT_MESSAGES[4].value['bookmarks']['public-COW']['xmin'] is not None
        assert CAUGHT_MESSAGES[4].value['bookmarks']['public-COW']['lsn'] == end_lsn

        assert CAUGHT_MESSAGES[5].record == {
            'colour': 'brow',
            'id': 2,
            'name': 'smelly',
            'timestamp_ntz': '9999-12-31T23:59:59.999000+00:00',
            'timestamp_tz': '9999-12-31T23:59:59.999000+00:00'
        }

        assert 'public-COW' == CAUGHT_MESSAGES[5].stream

        assert isinstance(CAUGHT_MESSAGES[6], singer.StateMessage)
        last_xmin = CAUGHT_MESSAGES[6].value['bookmarks']['public-COW']['xmin']
        old_state = CAUGHT_MESSAGES[6].value

        #run another do_sync, should get the remaining record which effectively finishes the initial full_table
        #replication portion of the logical replication
        singer.write_message = singer_write_message_ok
        global COW_RECORD_COUNT
        COW_RECORD_COUNT = 0
        CAUGHT_MESSAGES.clear()
        tap_postgres.do_sync(get_test_connection_config(use_secondary=use_secondary), {'streams' : streams}, None, old_state)

        mock_connect.assert_called_with(**expected_connection)
        mock_connect.reset_mock()

        assert 8 == len(CAUGHT_MESSAGES)

        assert CAUGHT_MESSAGES[0]['type'] == 'SCHEMA'

        assert isinstance(CAUGHT_MESSAGES[1], singer.StateMessage)
        assert CAUGHT_MESSAGES[1].value['bookmarks']['public-COW'].get('xmin') == last_xmin
        assert CAUGHT_MESSAGES[1].value['bookmarks']['public-COW'].get('lsn') == end_lsn
        assert CAUGHT_MESSAGES[1].value['bookmarks']['public-COW'].get('version') == new_version

        assert isinstance(CAUGHT_MESSAGES[2], singer.RecordMessage)
        assert CAUGHT_MESSAGES[2].record == {
            'colour': 'brow',
            'id': 2,
            'name': 'smelly',
            'timestamp_ntz': '9999-12-31T23:59:59.999000+00:00',
            'timestamp_tz': '9999-12-31T23:59:59.999000+00:00'
        }

        assert 'public-COW' == CAUGHT_MESSAGES[2].stream

        assert isinstance(CAUGHT_MESSAGES[3], singer.StateMessage)
        assert CAUGHT_MESSAGES[3].value['bookmarks']['public-COW'].get('xmin'),last_xmin
        assert CAUGHT_MESSAGES[3].value['bookmarks']['public-COW'].get('lsn') == end_lsn
        assert CAUGHT_MESSAGES[3].value['bookmarks']['public-COW'].get('version') == new_version

        assert isinstance(CAUGHT_MESSAGES[4], singer.RecordMessage)
        assert CAUGHT_MESSAGES[4].record == {
            'colour': 'green',
            'id': 3,
            'name': 'pooper',
            'timestamp_ntz': '9999-12-31T23:59:59.999000+00:00',
            'timestamp_tz': '9999-12-31T23:59:59.999000+00:00'
        }
        assert 'public-COW' == CAUGHT_MESSAGES[4].stream

        assert isinstance(CAUGHT_MESSAGES[5], singer.StateMessage)
        assert CAUGHT_MESSAGES[5].value['bookmarks']['public-COW'].get('xmin') > last_xmin
        assert CAUGHT_MESSAGES[5].value['bookmarks']['public-COW'].get('lsn') == end_lsn
        assert CAUGHT_MESSAGES[5].value['bookmarks']['public-COW'].get('version') == new_version


        assert isinstance(CAUGHT_MESSAGES[6], singer.ActivateVersionMessage)
        assert CAUGHT_MESSAGES[6].version == new_version

        assert isinstance(CAUGHT_MESSAGES[7], singer.StateMessage)
        assert CAUGHT_MESSAGES[7].value['bookmarks']['public-COW'].get('xmin') is None
        assert CAUGHT_MESSAGES[7].value['bookmarks']['public-COW'].get('lsn') == end_lsn
        assert CAUGHT_MESSAGES[7].value['bookmarks']['public-COW'].get('version') == new_version

@pytest.mark.parametrize('use_secondary', [False, True])
@unittest.mock.patch('psycopg2.connect', wraps=psycopg2.connect)
class TestFullTableInterruption:
    maxDiff = None
    def setup_method(self):
        table_spec_1 = {"columns": [{"name": "id", "type" : "serial",       "primary_key" : True},
                                    {"name" : 'name', "type": "character varying"},
                                    {"name" : 'colour', "type": "character varying"}],
                        "name" : 'COW'}
        ensure_test_table(table_spec_1)

        table_spec_2 = {"columns": [{"name": "id", "type" : "serial",       "primary_key" : True},
                                    {"name" : 'name', "type": "character varying"},
                                    {"name" : 'colour', "type": "character varying"}],
                        "name" : 'CHICKEN'}
        ensure_test_table(table_spec_2)

        global COW_RECORD_COUNT
        COW_RECORD_COUNT = 0
        global CAUGHT_MESSAGES
        CAUGHT_MESSAGES.clear()

    def test_catalog(self, mock_connect, use_secondary):
        singer.write_message = singer_write_message_no_cow
        pg_common.write_schema_message = singer_write_message_ok

        conn_config = get_test_connection_config(use_secondary=use_secondary)
        streams = tap_postgres.do_discovery(conn_config)

        # Assert that we connected to the correct database
        expected_connection = {
            'application_name': unittest.mock.ANY,
            'dbname': unittest.mock.ANY,
            'user': unittest.mock.ANY,
            'password': unittest.mock.ANY,
            'connect_timeout':unittest.mock.ANY,
            'host': conn_config['secondary_host'] if use_secondary else conn_config['host'],
            'port': conn_config['secondary_port'] if use_secondary else conn_config['port'],
        }
        mock_connect.assert_called_once_with(**expected_connection)
        mock_connect.reset_mock()

        cow_stream = [s for s in streams if s['table_name'] == 'COW'][0]
        assert cow_stream is not None
        cow_stream = select_all_of_stream(cow_stream)
        cow_stream = set_replication_method_for_stream(cow_stream, 'FULL_TABLE')

        chicken_stream = [s for s in streams if s['table_name'] == 'CHICKEN'][0]
        assert chicken_stream is not None
        chicken_stream = select_all_of_stream(chicken_stream)
        chicken_stream = set_replication_method_for_stream(chicken_stream, 'FULL_TABLE')

        conn = get_test_connection()
        conn.autocommit = True

        with conn.cursor() as cur:
            cow_rec = {'name': 'betty', 'colour': 'blue'}
            insert_record(cur, 'COW', {'name': 'betty', 'colour': 'blue'})

            cow_rec = {'name': 'smelly', 'colour': 'brow'}
            insert_record(cur, 'COW', cow_rec)

            cow_rec = {'name': 'pooper', 'colour': 'green'}
            insert_record(cur, 'COW', cow_rec)

            chicken_rec = {'name': 'fred', 'colour': 'red'}
            insert_record(cur, 'CHICKEN', chicken_rec)

        conn.close()

        state = {}
        blew_up_on_cow = False

        #this will sync the CHICKEN but then blow up on the COW
        try:
            tap_postgres.do_sync(get_test_connection_config(use_secondary=use_secondary), {'streams' : streams}, None, state)
        except Exception as ex:
            # LOGGER.exception(ex)
            blew_up_on_cow = True

        assert blew_up_on_cow
        mock_connect.assert_called_with(**expected_connection)
        mock_connect.reset_mock()

        assert 14 == len(CAUGHT_MESSAGES)

        assert CAUGHT_MESSAGES[0]['type'] == 'SCHEMA'
        assert isinstance(CAUGHT_MESSAGES[1], singer.StateMessage)
        assert CAUGHT_MESSAGES[1].value['bookmarks']['public-CHICKEN'].get('xmin') is None

        assert isinstance(CAUGHT_MESSAGES[2], singer.ActivateVersionMessage)
        new_version = CAUGHT_MESSAGES[2].version

        assert isinstance(CAUGHT_MESSAGES[3], singer.RecordMessage)
        assert 'public-CHICKEN' == CAUGHT_MESSAGES[3].stream

        assert isinstance(CAUGHT_MESSAGES[4], singer.StateMessage)
        #xmin is set while we are processing the full table replication
        assert CAUGHT_MESSAGES[4].value['bookmarks']['public-CHICKEN']['xmin'] is not None

        assert isinstance(CAUGHT_MESSAGES[5], singer.ActivateVersionMessage)
        assert CAUGHT_MESSAGES[5].version == new_version

        assert isinstance(CAUGHT_MESSAGES[6], singer.StateMessage)
        assert None == singer.get_currently_syncing( CAUGHT_MESSAGES[6].value)
        #xmin is cleared at the end of the full table replication
        assert CAUGHT_MESSAGES[6].value['bookmarks']['public-CHICKEN']['xmin'] is None


        #cow messages
        assert CAUGHT_MESSAGES[7]['type'] == 'SCHEMA'

        assert "public-COW" == CAUGHT_MESSAGES[7]['stream']
        assert isinstance(CAUGHT_MESSAGES[8], singer.StateMessage)
        assert CAUGHT_MESSAGES[8].value['bookmarks']['public-COW'].get('xmin') is None
        assert "public-COW" == CAUGHT_MESSAGES[8].value['currently_syncing']

        assert isinstance(CAUGHT_MESSAGES[9], singer.ActivateVersionMessage)
        cow_version = CAUGHT_MESSAGES[9].version
        assert isinstance(CAUGHT_MESSAGES[10], singer.RecordMessage)

        assert CAUGHT_MESSAGES[10].record['name'] == 'betty'
        assert 'public-COW' == CAUGHT_MESSAGES[10].stream

        assert isinstance(CAUGHT_MESSAGES[11], singer.StateMessage)
        #xmin is set while we are processing the full table replication
        assert CAUGHT_MESSAGES[11].value['bookmarks']['public-COW']['xmin'] is not None


        assert CAUGHT_MESSAGES[12].record['name'] == 'smelly'
        assert 'public-COW' == CAUGHT_MESSAGES[12].stream
        old_state = CAUGHT_MESSAGES[13].value

        #run another do_sync
        singer.write_message = singer_write_message_ok
        CAUGHT_MESSAGES.clear()
        global COW_RECORD_COUNT
        COW_RECORD_COUNT = 0

        tap_postgres.do_sync(get_test_connection_config(use_secondary=use_secondary), {'streams' : streams}, None, old_state)

        mock_connect.assert_called_with(**expected_connection)
        mock_connect.reset_mock()

        assert CAUGHT_MESSAGES[0]['type'] == 'SCHEMA'
        assert isinstance(CAUGHT_MESSAGES[1], singer.StateMessage)

        # because we were interrupted, we do not switch versions
        assert CAUGHT_MESSAGES[1].value['bookmarks']['public-COW']['version'] == cow_version
        assert CAUGHT_MESSAGES[1].value['bookmarks']['public-COW']['xmin'] is not None
        assert "public-COW" == singer.get_currently_syncing(CAUGHT_MESSAGES[1].value)

        assert isinstance(CAUGHT_MESSAGES[2], singer.RecordMessage)
        assert CAUGHT_MESSAGES[2].record['name'] == 'smelly'
        assert 'public-COW' == CAUGHT_MESSAGES[2].stream


        #after record: activate version, state with no xmin or currently syncing
        assert isinstance(CAUGHT_MESSAGES[3], singer.StateMessage)
        #we still have an xmin for COW because are not yet done with the COW table
        assert CAUGHT_MESSAGES[3].value['bookmarks']['public-COW']['xmin'] is not None
        assert singer.get_currently_syncing( CAUGHT_MESSAGES[3].value) == 'public-COW'

        assert isinstance(CAUGHT_MESSAGES[4], singer.RecordMessage)
        assert CAUGHT_MESSAGES[4].record['name'] == 'pooper'
        assert 'public-COW' == CAUGHT_MESSAGES[4].stream

        assert isinstance(CAUGHT_MESSAGES[5], singer.StateMessage)
        assert CAUGHT_MESSAGES[5].value['bookmarks']['public-COW']['xmin'] is not None
        assert singer.get_currently_syncing( CAUGHT_MESSAGES[5].value) == 'public-COW'


        #xmin is cleared because we are finished the full table replication
        assert isinstance(CAUGHT_MESSAGES[6], singer.ActivateVersionMessage)
        assert CAUGHT_MESSAGES[6].version == cow_version

        assert isinstance(CAUGHT_MESSAGES[7], singer.StateMessage)
        assert singer.get_currently_syncing( CAUGHT_MESSAGES[7].value) is None
        assert CAUGHT_MESSAGES[7].value['bookmarks']['public-CHICKEN']['xmin'] is None
        assert singer.get_currently_syncing( CAUGHT_MESSAGES[7].value) is None
