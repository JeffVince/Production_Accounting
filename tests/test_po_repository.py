# tests/test_po_repository.py

import unittest
from tests.base_test import BaseTestCase
from database.po_repository import add_or_update_po, get_po_by_number
from database.models import POState


class TestPORepository(BaseTestCase):

    def test_add_po(self):
        """
        Test adding a new PO.
        """
        po_data = {
            'po_number': 'PO123',
            'amount': 1000.0,
            'description': 'Test Purchase Order'
        }
        po = add_or_update_po(po_data)
        self.assertIsNotNone(po.id)
        self.assertEqual(po.po_number, 'PO123')
        self.assertEqual(po.amount, 1000.0)
        self.assertEqual(po.state, POState.PENDING)

    def test_update_po(self):
        """
        Test updating an existing PO.
        """
        po_data = {
            'po_number': 'PO123',
            'amount': 1000.0,
            'description': 'Test Purchase Order'
        }
        add_or_update_po(po_data)

        # Update PO
        updated_data = {
            'po_number': 'PO123',
            'amount': 1500.0,
            'description': 'Updated Purchase Order'
        }
        po = add_or_update_po(updated_data)
        self.assertEqual(po.amount, 1500.0)
        self.assertEqual(po.description, 'Updated Purchase Order')

    def test_get_po_by_number(self):
        """
        Test retrieving a PO by its number.
        """
        po_data = {
            'po_number': 'PO124',
            'amount': 2000.0,
            'description': 'Another Purchase Order'
        }
        add_or_update_po(po_data)
        po = get_po_by_number('PO124')
        self.assertIsNotNone(po)
        self.assertEqual(po.po_number, 'PO124')
        self.assertEqual(po.amount, 2000.0)


if __name__ == '__main__':
    unittest.main()