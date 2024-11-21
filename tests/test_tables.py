# tests/test_tables.py

import unittest
from tests.base_test import BaseTestCase
from sqlalchemy import inspect

class TestTables(BaseTestCase):

    def test_pos_table_exists(self):
        inspector = inspect(self.engine)
        tables = inspector.get_table_names()
        self.assertIn('pos', tables, "The 'pos' table should exist in the database.")

    def test_vendors_table_exists(self):
        inspector = inspect(self.engine)
        tables = inspector.get_table_names()
        self.assertIn('vendors', tables, "The 'vendors' table should exist in the database.")

if __name__ == '__main__':
    unittest.main()