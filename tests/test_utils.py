# tests/test_utils.py

import unittest
from tests.base_test import BaseTestCase
from database.utils import update_po_status, get_po_state
from database.po_repository import add_or_update_po
from database.models import POState

class TestUtils(BaseTestCase):

    def test_update_po_status(self):
        """
        Test updating the status of a PO.
        """
        po_data = {
            'po_number': 'PO135',
            'amount': 13000.0,
            'description': 'Utils PO'
        }
        add_or_update_po(po_data)
        update_po_status('PO135', 'PAID')
        state = get_po_state('PO135')
        self.assertEqual(state, POState.PAID)

    def test_get_po_state(self):
        """
        Test retrieving the state of a PO.
        """
        po_data = {
            'po_number': 'PO136',
            'amount': 14000.0,
            'description': 'State PO'
        }
        add_or_update_po(po_data)
        state = get_po_state('PO136')
        self.assertEqual(state, POState.PENDING)

if __name__ == '__main__':
    unittest.main()