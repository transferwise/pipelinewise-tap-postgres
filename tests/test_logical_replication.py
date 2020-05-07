import unittest

from tap_postgres.sync_strategies import logical_replication


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

    def test_generate_replication_slot_name(self):
        """Validate if the replication slot name generated correctly"""
        # Provide only database name
        self.assertEquals(logical_replication.generate_replication_slot_name('some_db'),
                          'pipelinewise_some_db')

        # Provide database name and tap_id
        self.assertEquals(logical_replication.generate_replication_slot_name('some_db',
                                                                             'some_tap'),
                          'pipelinewise_some_db_some_tap')

        # Provide database name, tap_id and prefix
        self.assertEquals(logical_replication.generate_replication_slot_name('some_db',
                                                                             'some_tap',
                                                                             prefix='custom_prefix'),
                          'custom_prefix_some_db_some_tap')

        # Replication slot name should be lowercase
        self.assertEquals(logical_replication.generate_replication_slot_name('SoMe_DB',
                                                                             'SoMe_TaP'),
                          'pipelinewise_some_db_some_tap')

    def test_locate_replication_slot_by_cur(self):
        """Validate if both v15 and v16 style replication slot located correctly"""
        # Should return v15 style slot name if v15 style replication slot exists
        cursor = PostgresCurReplicationSlotMock(existing_slot_name='pipelinewise_some_db')
        self.assertEquals(logical_replication.locate_replication_slot_by_cur(cursor,
                                                                             'some_db',
                                                                             'some_tap'),
                          'pipelinewise_some_db')

        # Should return v16 style slot name if v16 style replication slot exists
        cursor = PostgresCurReplicationSlotMock(existing_slot_name='pipelinewise_some_db_some_tap')
        self.assertEquals(logical_replication.locate_replication_slot_by_cur(cursor,
                                                                             'some_db',
                                                                             'some_tap'),
                          'pipelinewise_some_db_some_tap')

        # Should return v15 style replication slot if tap_id not provided and the v15 slot exists
        cursor = PostgresCurReplicationSlotMock(existing_slot_name='pipelinewise_some_db')
        self.assertEquals(logical_replication.locate_replication_slot_by_cur(cursor,
                                                                             'some_db'),
                          'pipelinewise_some_db')

        # Should raise an exception if no v15 or v16 style replication slot found
        cursor = PostgresCurReplicationSlotMock(existing_slot_name=None)
        with self.assertRaises(logical_replication.ReplicationSlotNotFoundError):
            self.assertEquals(logical_replication.locate_replication_slot_by_cur(cursor,
                                                                                 'some_db',
                                                                                 'some_tap'),
                              'pipelinewise_some_db_some_tap')
