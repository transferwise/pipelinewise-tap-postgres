import unittest
import re

from tap_postgres.sync_strategies import logical_replication


class TestLogicalReplication(unittest.TestCase):
    maxDiff = None

    def setUp(self):
        pass

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

    def test_generate_tap_id(self):
        """Validate if the generated tap_id is between the MIN and MAX range"""
        self.assertTrue(logical_replication.MIN_GENERATED_TAP_ID <
                        logical_replication.generate_tap_id() <=
                        logical_replication.MAX_GENERATED_TAP_ID)

    def test_generate_replication_slot_name(self):
        """Validate if the replication slot name generated correctly"""
        # Provide only database name: tap_id should be generated
        self.assertTrue(re.compile('^pipelinewise_some_db_\d+$').match(
            logical_replication.generate_replication_slot_name('some_db')))

        # Provide database name and tap_id
        self.assertEquals(logical_replication.generate_replication_slot_name('some_db',
                                                                             'some_tap'),
                          'pipelinewise_some_db_some_tap')

        # Provide database name, tap_id and prefix
        self.assertEquals(logical_replication.generate_replication_slot_name('some_db',
                                                                             'some_tap',
                                                                             prefix='custom_prefix'),
                          'custom_prefix_some_db_some_tap')

        # Replication slot name should be lowercased
        self.assertEquals(logical_replication.generate_replication_slot_name('SoMe_DB',
                                                                             'SoMe_TaP'),
                          'pipelinewise_some_db_some_tap')

