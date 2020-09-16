import json
import unittest

from collections import namedtuple
from unittest.mock import patch

from tap_postgres.sync_strategies import logical_replication
from tap_postgres.sync_strategies.logical_replication import UnsupportedPayloadKindError


class PostgresCurReplicationSlotMock:
    """
    Postgres Cursor Mock with replication slot selection
    """

    def __init__(self, existing_slot_name):
        """Initialise by defining an existing replication slot"""
        self.existing_slot_name = existing_slot_name
        self.replication_slot_found = False

    def execute(self, sql):
        """Simulating to run an SQL query
        If the query is selecting the existing_slot_name then the replication slot found"""
        if sql == f"SELECT * FROM pg_replication_slots WHERE slot_name = '{self.existing_slot_name}'":
            self.replication_slot_found = True

    def fetchall(self):
        """Return the replication slot name as a List if the slot exists."""
        if self.replication_slot_found:
            return [self.existing_slot_name]

        return []


class TestLogicalReplication(unittest.TestCase):
    maxDiff = None

    def setUp(self):
        self.WalMessage = namedtuple('WalMessage', ['payload', 'data_start'])

    def test_streams_to_wal2json_tables(self):
        """Validate if table names are escaped to wal2json format"""
        streams = [
            {'metadata': [{'metadata': {'schema-name': 'public'}}],
             'table_name': 'dummy_table'},
            {'metadata': [{'metadata': {'schema-name': 'public'}}],
             'table_name': 'CaseSensitiveTable'},
            {'metadata': [{'metadata': {'schema-name': 'public'}}],
             'table_name': 'Case Sensitive Table With Space'},
            {'metadata': [{'metadata': {'schema-name': 'CaseSensitiveSchema'}}],
             'table_name': 'dummy_table'},
            {'metadata': [{'metadata': {'schema-name': 'Case Sensitive Schema With Space'}}],
             'table_name': 'CaseSensitiveTable'},
            {'metadata': [{'metadata': {'schema-name': 'Case Sensitive Schema With Space'}}],
             'table_name': 'Case Sensitive Table With Space'},
            {'metadata': [{'metadata': {'schema-name': 'public'}}],
             'table_name': 'table_with_comma_,'},
            {'metadata': [{'metadata': {'schema-name': 'public'}}],
             'table_name': "table_with_quote_'"}
        ]

        self.assertEqual(logical_replication.streams_to_wal2json_tables(streams),
                         'public.dummy_table,'
                         'public.CaseSensitiveTable,'
                         'public.Case\\ Sensitive\\ Table\\ With\\ Space,'
                         'CaseSensitiveSchema.dummy_table,'
                         'Case\\ Sensitive\\ Schema\\ With\\ Space.CaseSensitiveTable,'
                         'Case\\ Sensitive\\ Schema\\ With\\ Space.Case\\ Sensitive\\ Table\\ With\\ Space,'
                         'public.table_with_comma_\\,,'
                         "public.table_with_quote_\\'")

    def test_generate_replication_slot_name(self):
        """Validate if the replication slot name generated correctly"""
        # Provide only database name
        self.assertEqual(logical_replication.generate_replication_slot_name('some_db'),
                          'pipelinewise_some_db')

        # Provide database name and tap_id
        self.assertEqual(logical_replication.generate_replication_slot_name('some_db',
                                                                             'some_tap'),
                          'pipelinewise_some_db_some_tap')

        # Provide database name, tap_id and prefix
        self.assertEqual(logical_replication.generate_replication_slot_name('some_db',
                                                                             'some_tap',
                                                                             prefix='custom_prefix'),
                          'custom_prefix_some_db_some_tap')

        # Replication slot name should be lowercase
        self.assertEqual(logical_replication.generate_replication_slot_name('SoMe_DB',
                                                                             'SoMe_TaP'),
                          'pipelinewise_some_db_some_tap')

        # Invalid characters should be replaced by underscores
        self.assertEqual(logical_replication.generate_replication_slot_name('some-db',
                                                                            'some-tap'),
                          'pipelinewise_some_db_some_tap')

        self.assertEqual(logical_replication.generate_replication_slot_name('some.db',
                                                                            'some.tap'),
                          'pipelinewise_some_db_some_tap')

        # Replication slot name should be truncated to 64 characters
        self.assertEqual(
            logical_replication.generate_replication_slot_name('some_db_with_an_extremely_long_name',
                                                               'some_tap_with_an_extremely_long_name'),
             'pipelinewise_some_db_with_an_extremely_long_name_some_tap_with_a'
        )

    def test_locate_replication_slot_by_cur(self):
        """Validate if both v15 and v16 style replication slot located correctly"""
        # Should return v15 style slot name if v15 style replication slot exists
        cursor = PostgresCurReplicationSlotMock(existing_slot_name='pipelinewise_some_db')
        self.assertEqual(logical_replication.locate_replication_slot_by_cur(cursor,
                                                                             'some_db',
                                                                             'some_tap'),
                          'pipelinewise_some_db')

        # Should return v16 style slot name if v16 style replication slot exists
        cursor = PostgresCurReplicationSlotMock(existing_slot_name='pipelinewise_some_db_some_tap')
        self.assertEqual(logical_replication.locate_replication_slot_by_cur(cursor,
                                                                             'some_db',
                                                                             'some_tap'),
                          'pipelinewise_some_db_some_tap')

        # Should return v15 style replication slot if tap_id not provided and the v15 slot exists
        cursor = PostgresCurReplicationSlotMock(existing_slot_name='pipelinewise_some_db')
        self.assertEqual(logical_replication.locate_replication_slot_by_cur(cursor,
                                                                             'some_db'),
                          'pipelinewise_some_db')

        # Should raise an exception if no v15 or v16 style replication slot found
        cursor = PostgresCurReplicationSlotMock(existing_slot_name=None)
        with self.assertRaises(logical_replication.ReplicationSlotNotFoundError):
            self.assertEqual(logical_replication.locate_replication_slot_by_cur(cursor,
                                                                                 'some_db',
                                                                                 'some_tap'),
                              'pipelinewise_some_db_some_tap')

    def test_consume_with_message_payload_is_not_json_expect_same_state(self):

        output = logical_replication.consume_message([],
                                            {},
                                            self.WalMessage(payload='this is an invalid json message', data_start=None),
                                            None,
                                            {}
                                            )
        self.assertDictEqual({}, output)

    def test_consume_with_message_stream_in_payload_is_not_selected_expect_same_state(self):
        output = logical_replication.consume_message(
            [{'tap_stream_id': 'myschema-mytable'}],
            {},
            self.WalMessage(payload='{"schema": "myschema", "table": "notmytable"}',
                            data_start='some lsn'),
            None,
            {}
        )

        self.assertDictEqual({}, output)

    def test_consume_with_payload_kind_is_not_supported_expect_exception(self):

        with self.assertRaises(UnsupportedPayloadKindError):
            logical_replication.consume_message(
                [{'tap_stream_id': 'myschema-mytable'}],
                {},
                self.WalMessage(payload='{"kind":"truncate", "schema": "myschema", "table": "mytable"}',
                                data_start='some lsn'),
                None,
                {}
            )
    @patch('tap_postgres.logical_replication.singer.write_message')
    @patch('tap_postgres.logical_replication.sync_common.send_schema_message')
    @patch('tap_postgres.logical_replication.refresh_streams_schema')
    def test_consume_message_with_new_column_in_payload_will_refresh_schema(self,
                                                                            refresh_schema_mock,
                                                                            send_schema_mock,
                                                                            write_message_mock):
        streams = [
                {
                    'tap_stream_id': 'myschema-mytable',
                    'stream': 'mytable',
                    'schema': {
                        'properties': {
                            'id': {},
                            'date_created': {}
                        }
                    },
                    'metadata': [
                        {
                            'breadcrumb': [],
                            'metadata': {
                                'is-view': False,
                                'table-key-properties': ['id'],
                                'schema-name': 'myschema'
                            }
                        },
                        {
                            "breadcrumb": [
                                "properties",
                                "id"
                            ],
                            "metadata": {
                                "sql-datatype": "integer",
                                "inclusion": "automatic",
                            }
                        },
                        {
                            "breadcrumb": [
                                "properties",
                                "date_created"
                            ],
                            "metadata": {
                                "sql-datatype": "datetime",
                                "inclusion": "available",
                                "selected": True
                            }
                        }
                    ],
                }
            ]

        return_v = logical_replication.consume_message(
            streams,
            {
                'bookmarks': {
                    "myschema-mytable": {
                        "last_replication_method": "LOG_BASED",
                        "lsn": None,
                        "version": 1000,
                        "xmin": None
                    }
                }
            },
            self.WalMessage(payload='{"kind": "insert", '
                                    '"schema": "myschema", '
                                    '"table": "mytable",'
                                    '"columnnames": ["id", "date_created", "new_col"],'
                                    '"columnnames": [1, null, "some random text"]'
                                    '}',
                            data_start='some lsn'),
            None,
            {}
        )

        self.assertDictEqual(return_v,
                          {
                              'bookmarks': {
                                  "myschema-mytable": {
                                      "last_replication_method": "LOG_BASED",
                                      "lsn": "some lsn",
                                      "version": 1000,
                                      "xmin": None
                                  }
                              }
                          })

        refresh_schema_mock.assert_called_once_with({}, [streams[0]])
        send_schema_mock.assert_called_once()
        write_message_mock.assert_called_once()
