# tests/test_xero_repository.py

import unittest
from tests.base_test import BaseTestCase
from database.xero_repository import (
    add_or_update_bill, update_bill_status, get_bill_by_po,
    get_bill_state, add_or_update_spend_money_transaction
)
from database.po_repository import add_or_update_po
from database.models import BillState, SpendMoneyState

class TestXeroRepository(BaseTestCase):

    def setUp(self):
        super().setUp()
        # Add a PO to associate bills and transactions with
        po_data = {
            'po_number': 'PO130',
            'amount': 8000.0,
            'description': 'Xero PO'
        }
        self.po = add_or_update_po(po_data)

    def test_add_bill(self):
        """
        Test adding a bill.
        """
        bill_data = {
            'bill_id': 'B001',
            'po_id': self.po.id,
            'amount': 8000.0,
            'state': BillState.DRAFT
        }
        bill = add_or_update_bill(bill_data)
        self.assertIsNotNone(bill.id)
        self.assertEqual(bill.bill_id, 'B001')
        self.assertEqual(bill.state, BillState.DRAFT)

    def test_update_bill_status(self):
        """
        Test updating bill status.
        """
        bill_data = {
            'bill_id': 'B002',
            'po_id': self.po.id,
            'amount': 8000.0,
            'state': BillState.DRAFT
        }
        add_or_update_bill(bill_data)
        update_bill_status('B002', BillState.APPROVED)
        state = get_bill_state('B002')
        self.assertEqual(state, BillState.APPROVED)

    def test_get_bill_by_po(self):
        """
        Test retrieving a bill by PO number.
        """
        bill_data = {
            'bill_id': 'B003',
            'po_id': self.po.id,
            'amount': 8000.0,
            'state': BillState.SUBMITTED
        }
        add_or_update_bill(bill_data)
        bill = get_bill_by_po('PO130')
        self.assertIsNotNone(bill)
        self.assertEqual(bill.bill_id, 'B003')

    def test_add_spend_money_transaction(self):
        """
        Test adding a Spend Money transaction.
        """
        transaction_data = {
            'transaction_id': 'SM001',
            'po_id': self.po.id,
            'amount': 8000.0,
            'state': SpendMoneyState.DRAFT
        }
        transaction = add_or_update_spend_money_transaction(transaction_data)
        self.assertIsNotNone(transaction.id)
        self.assertEqual(transaction.transaction_id, 'SM001')
        self.assertEqual(transaction.state, SpendMoneyState.DRAFT)

if __name__ == '__main__':
    unittest.main()