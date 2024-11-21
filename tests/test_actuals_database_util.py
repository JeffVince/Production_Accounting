# tests/test_actuals_database_util.py

import unittest
from datetime import datetime
from tests.base_test import BaseTestCase
from database.actuals_database_util import (
    add_actual_entry, get_actuals_by_po, reconcile_actuals
)
from database.po_repository import add_or_update_po
from database.utils import get_po_state
from database.models import POState

class TestActualsDatabaseUtil(BaseTestCase):

    def setUp(self):
        super().setUp()
        # Add a PO
        po_data = {
            'po_number': 'PO134',
            'amount': 12000.0,
            'description': 'Actuals PO'
        }
        self.po = add_or_update_po(po_data)

    def test_add_actual_entry(self):
        """
        Test adding an actual expense entry.
        """
        actual_data = {
            'amount': 6000.0,
            'description': 'Partial Payment',
            'date': datetime.now(),
            'source': 'Xero'
        }
        actual = add_actual_entry('PO134', actual_data)
        self.assertIsNotNone(actual)
        self.assertEqual(actual.amount, 6000.0)

    def test_get_actuals_by_po(self):
        """
        Test retrieving actuals by PO.
        """
        actual_data1 = {
            'amount': 6000.0,
            'description': 'First Payment',
            'date': datetime.now(),
            'source': 'Xero'
        }
        actual_data2 = {
            'amount': 6000.0,
            'description': 'Second Payment',
            'date': datetime.now(),
            'source': 'Mercury'
        }
        add_actual_entry('PO134', actual_data1)
        add_actual_entry('PO134', actual_data2)
        actuals = get_actuals_by_po('PO134')
        self.assertEqual(len(actuals), 2)

    def test_reconcile_actuals(self):
        """
        Test reconciling actuals for a PO.
        """
        # Add actuals that sum up to the PO amount
        actual_data1 = {
            'amount': 6000.0,
            'description': 'First Payment',
            'date': datetime.now(),
            'source': 'Xero'
        }
        actual_data2 = {
            'amount': 6000.0,
            'description': 'Second Payment',
            'date': datetime.now(),
            'source': 'Mercury'
        }
        add_actual_entry('PO134', actual_data1)
        add_actual_entry('PO134', actual_data2)
        result = reconcile_actuals('PO134')
        self.assertTrue(result)
        state = get_po_state('PO134')
        self.assertEqual(state, POState.RECONCILED)

if __name__ == '__main__':
    unittest.main()