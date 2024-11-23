# tests/test_db.py

import unittest
from tests.base_test import BaseTestCase
from sqlalchemy import inspect

class TestDatabaseTables(BaseTestCase):

    def test_tables_exist(self):
        """
        Test to ensure that all necessary tables are created.
        """
        inspector = inspect(self.engine)
        tables = inspector.get_table_names()
        expected_tables = [
            'projects', 'vendors', 'contacts', 'tax_forms', 'pos',
            'invoices', 'receipts', 'bills', 'transactions',
            'spend_money_transactions', 'actuals', 'sub_items', 'main_items'
        ]
        for table in expected_tables:
            self.assertIn(table, tables, f"Table '{table}' should exist in the database.")

if __name__ == '__main__':
    unittest.main()