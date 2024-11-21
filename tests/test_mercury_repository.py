# tests/test_mercury_repository.py

import unittest
from tests.base_test import BaseTestCase
from database.mercury_repository import (
    add_or_update_transaction, update_transaction_status,
    get_transaction_by_po, get_transaction_state
)
from database.po_repository import add_or_update_po
from database.models import TransactionState

class TestMercuryRepository(BaseTestCase):

    def setUp(self):
        super().setUp()
        # Add a PO to associate transactions with
        po_data = {
            'po_number': 'PO131',
            'amount': 9000.0,
            'description': 'Mercury PO'
        }
        self.po = add_or_update_po(po_data)

    def test_add_transaction(self):
        """
        Test adding a transaction.
        """
        transaction_data = {
            'transaction_id': 'T001',
            'po_id': self.po.id,
            'amount': 9000.0,
            'state': TransactionState.PENDING
        }
        transaction = add_or_update_transaction(transaction_data)
        self.assertIsNotNone(transaction.id)
        self.assertEqual(transaction.transaction_id, 'T001')
        self.assertEqual(transaction.state, TransactionState.PENDING)

    def test_update_transaction_status(self):
        """
        Test updating transaction status.
        """
        transaction_data = {
            'transaction_id': 'T002',
            'po_id': self.po.id,
            'amount': 9000.0,
            'state': TransactionState.PENDING
        }
        add_or_update_transaction(transaction_data)
        update_transaction_status('T002', 'PAID')
        state = get_transaction_state('T002')
        self.assertEqual(state, TransactionState.PAID)

    def test_get_transaction_by_po(self):
        """
        Test retrieving a transaction by PO number.
        """
        transaction_data = {
            'transaction_id': 'T003',
            'po_id': self.po.id,
            'amount': 9000.0,
            'state': TransactionState.PAID
        }
        add_or_update_transaction(transaction_data)
        transaction = get_transaction_by_po('PO131')
        self.assertIsNotNone(transaction)
        self.assertEqual(transaction.transaction_id, 'T003')

if __name__ == '__main__':
    unittest.main()