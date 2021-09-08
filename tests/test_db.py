import unittest

from tap_postgres import db


class TestDbFunctions(unittest.TestCase):
    maxDiff = None

    def test_value_to_singer_value(self):
        """Test if every element converted from sql_datatype to the correct singer type"""
        # JSON and JSONB should be converted to dictionaries
        self.assertEqual(db.selected_value_to_singer_value_impl('{"test": 123}', 'json'), {'test': 123})
        self.assertEqual(db.selected_value_to_singer_value_impl('{"test": 123}', 'jsonb'), {'test': 123})

        # time with time zone values should be converted to UTC and time zone dropped
        # Hour 24 should be taken as 0
        self.assertEqual(db.selected_value_to_singer_value_impl('12:00:00-0800', 'time with time zone'), '20:00:00')
        self.assertEqual(db.selected_value_to_singer_value_impl('24:00:00-0800', 'time with time zone'), '08:00:00')

        # time without time zone values should be converted to UTC and time zone dropped
        self.assertEqual(db.selected_value_to_singer_value_impl('12:00:00', 'time without time zone'), '12:00:00')
        # Hour 24 should be taken as 0
        self.assertEqual(db.selected_value_to_singer_value_impl('24:00:00', 'time without time zone'), '00:00:00')

        # timestamp with time zone should be converted to iso format
        self.assertEqual(db.selected_value_to_singer_value_impl('2020-05-01T12:00:00-0800',
                                                                'timestamp with time zone'),
                         '2020-05-01T12:00:00-0800')

        # bit should be True only if elem is '1'
        self.assertEqual(db.selected_value_to_singer_value_impl('1', 'bit'), True)
        self.assertEqual(db.selected_value_to_singer_value_impl('0', 'bit'), False)
        self.assertEqual(db.selected_value_to_singer_value_impl(1, 'bit'), False)
        self.assertEqual(db.selected_value_to_singer_value_impl(0, 'bit'), False)

        # boolean should be True in case of numeric 1 and logical True
        self.assertEqual(db.selected_value_to_singer_value_impl(1, 'boolean'), True)
        self.assertEqual(db.selected_value_to_singer_value_impl(True, 'boolean'), True)
        self.assertEqual(db.selected_value_to_singer_value_impl(0, 'boolean'), False)
        self.assertEqual(db.selected_value_to_singer_value_impl(False, 'boolean'), False)

    def test_prepare_columns_sql(self):
        self.assertEqual(' "my_column" ', db.prepare_columns_sql('my_column'))

    def test_prepare_columns_for_select_sql_with_timestamp_ntz_column(self):
        self.assertEqual(
            'CASE WHEN  "my_column"  < \'0001-01-01 00:00:00.000\' OR '
            ' "my_column"  > \'9999-12-31 23:59:59.999\' THEN \'9999-12-31 23:59:59.999\' '
            'ELSE  "my_column"  END AS  "my_column" ',
            db.prepare_columns_for_select_sql('my_column',
                                              {
                                                  ('properties', 'my_column'): {
                                                      'sql-datatype': 'timestamp without time zone'
                                                  }
                                              }
                                              )
        )

    def test_prepare_columns_for_select_sql_with_timestamp_tz_column(self):
        self.assertEqual(
            'CASE WHEN  "my_column"  < \'0001-01-01 00:00:00.000\' OR '
            ' "my_column"  > \'9999-12-31 23:59:59.999\' THEN \'9999-12-31 23:59:59.999\' '
            'ELSE  "my_column"  END AS  "my_column" ',
            db.prepare_columns_for_select_sql('my_column',
                                              {
                                                  ('properties', 'my_column'): {
                                                      'sql-datatype': 'timestamp with time zone'
                                                  }
                                              }
                                              )
        )

    def test_prepare_columns_for_select_sql_with_timestamp_ntz_array_column(self):
        self.assertEqual(
            ' "my_column" ',
            db.prepare_columns_for_select_sql('my_column',
                                              {
                                                  ('properties', 'my_column'): {
                                                      'sql-datatype': 'timestamp without time zone[]'
                                                  }
                                              }
                                              )
        )

    def test_prepare_columns_for_select_sql_with_timestamp_tz_array_column(self):
        self.assertEqual(
            ' "my_column" ',
            db.prepare_columns_for_select_sql('my_column',
                                              {
                                                  ('properties', 'my_column'): {
                                                      'sql-datatype': 'timestamp with time zone[]'
                                                  }
                                              }
                                              )
        )

    def test_prepare_columns_for_select_sql_with_not_timestamp_column(self):
        self.assertEqual(
            ' "my_column" ',
            db.prepare_columns_for_select_sql('my_column',
                                              {
                                                  ('properties', 'my_column'): {
                                                      'sql-datatype': 'int'
                                                  }
                                              }
                                              )
        )

    def test_prepare_columns_for_select_sql_with_column_not_in_map(self):
        self.assertEqual(
            ' "my_column" ',
            db.prepare_columns_for_select_sql('my_column',
                                              {
                                                  ('properties', 'nope'): {
                                                      'sql-datatype': 'int'
                                                  }
                                              }
                                              )
        )

    def test_selected_value_to_singer_value_impl_with_null_json_returns_None(self):
        output = db.selected_value_to_singer_value_impl(None, 'json')

        self.assertEqual(None, output)

    def test_selected_value_to_singer_value_impl_with_empty_json_returns_empty_dict(self):
        output = db.selected_value_to_singer_value_impl('{}', 'json')

        self.assertEqual({}, output)

    def test_selected_value_to_singer_value_impl_with_non_empty_json_returns_equivalent_dict(self):
        output = db.selected_value_to_singer_value_impl('{"key1": "A", "key2": [{"kk": "yo"}, {}]}', 'json')

        self.assertEqual({
            'key1': 'A',
            'key2': [{'kk': 'yo'}, {}]
        }, output)

    def test_selected_value_to_singer_value_impl_with_null_jsonb_returns_None(self):
        output = db.selected_value_to_singer_value_impl(None, 'jsonb')

        self.assertEqual(None, output)

    def test_selected_value_to_singer_value_impl_with_empty_jsonb_returns_empty_dict(self):
        output = db.selected_value_to_singer_value_impl('{}', 'jsonb')

        self.assertEqual({}, output)

    def test_selected_value_to_singer_value_impl_with_non_empty_jsonb_returns_equivalent_dict(self):
        output = db.selected_value_to_singer_value_impl('{"key1": "A", "key2": [{"kk": "yo"}, {}]}', 'jsonb')

        self.assertEqual({
            'key1': 'A',
            'key2': [{'kk': 'yo'}, {}]
        }, output)
