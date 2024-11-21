# tests/test_xero_database_util.py

import unittest
from tests.base_test import BaseTestCase
from database.xero_database_util import (
    create_draft_bill,
    submit_bill_for_approval,
    approve_bill,
    create_spend_money_transaction,  # Imported correctly
)
from database.xero_repository import (
    update_bill_status,  # Imported correctly
)
from database.models import Bill, BillState, PO, POState, Contact, Vendor, SpendMoneyState
from database.po_repository import add_or_update_po
from sqlalchemy.exc import SQLAlchemyError

class TestXeroDatabaseUtil(BaseTestCase):

    def setUp(self):
        super().setUp()
        # Add a Contact
        contact_data = {
            'contact_id': 'C002',
            'name': 'Jane Smith',
            'email': 'jane@example.com',
            'phone': '0987654321'
        }
        with self.session_scope() as session:
            contact = session.query(Contact).filter_by(contact_id='C002').first()
            if not contact:
                contact = Contact(**contact_data)
                session.add(contact)
                session.commit()
            self.contact = contact

        # Add a Vendor
        vendor_data = {
            'vendor_name': 'Vendor C',
            'contact_id': self.contact.id
        }
        with self.session_scope() as session:
            vendor = session.query(Vendor).filter_by(vendor_name='Vendor C').first()
            if not vendor:
                vendor = Vendor(**vendor_data)
                session.add(vendor)
                session.commit()
            self.vendor = vendor

        # Add a PO
        po_data = {
            'po_number': 'PO132',
            'amount': 10000.0,
            'description': 'Xero Test PO',
            'vendor_id': self.vendor.id,
            'state': POState.PENDING
        }
        self.po = add_or_update_po(po_data)

    def test_create_draft_bill(self):
        """
        Test creating a draft bill.
        """
        bill_data = {
            'bill_id': 'B005',
            'amount': 10000.0,
            'due_date': None
        }
        bill = create_draft_bill('PO132', bill_data)  # Assuming this function exists
        self.assertIsNotNone(bill)
        self.assertEqual(bill.bill_id, 'B005')
        self.assertEqual(bill.amount, 10000.0)
        self.assertEqual(bill.state, BillState.DRAFT)

    def test_create_spend_money_transaction(self):
        """
        Test creating a spend money transaction.
        """
        transaction_data = {
            'transaction_id': 'SMT001',
            'amount': 5000.0,
            'description': 'Spend Money Transaction'
        }
        transaction = create_spend_money_transaction('PO132', transaction_data)  # Function is now imported
        self.assertIsNotNone(transaction)
        self.assertEqual(transaction.transaction_id, 'SMT001')
        self.assertEqual(transaction.amount, 5000.0)
        self.assertEqual(transaction.description, 'Spend Money Transaction')  # Assert description
        self.assertEqual(transaction.state, SpendMoneyState.DRAFT)  # Ensure correct enum usage

    def test_update_bill_status_invalid(self):
        """
        Test updating a bill status with an invalid status.
        """
        bill_data = {
            'bill_id': 'B006',
            'amount': 15000.0,
            'due_date': None
        }
        bill = create_draft_bill('PO132', bill_data)
        self.assertIsNotNone(bill)

        with self.assertRaises(ValueError):
            # Attempt to update with an invalid status (passing string instead of enum)
            update_bill_status('B006', 'INVALID_STATUS')  # This should raise ValueError

    def test_submit_and_approve_bill(self):
        """
        Test submitting and approving a bill.
        """
        bill_data = {
            'bill_id': 'B005',
            'amount': 10000.0,
            'due_date': None
        }
        bill = create_draft_bill('PO132', bill_data)
        self.assertIsNotNone(bill)
        self.assertEqual(bill.bill_id, 'B005')
        self.assertEqual(bill.state, BillState.DRAFT)

        # Submit the bill for approval
        submit_bill_for_approval('B005')
        with self.session_scope() as session:
            bill = session.query(Bill).filter_by(bill_id='B005').first()
            self.assertIsNotNone(bill)
            self.assertEqual(bill.state, BillState.SUBMITTED)

        # Approve the bill
        approve_bill('B005')
        with self.session_scope() as session:
            bill = session.query(Bill).filter_by(bill_id='B005').first()
            self.assertIsNotNone(bill)
            self.assertEqual(bill.state, BillState.APPROVED)

    def test_update_bill_status_invalid(self):
        """
        Test updating a bill status with an invalid status.
        """
        bill_data = {
            'bill_id': 'B006',
            'amount': 15000.0,
            'due_date': None
        }
        bill = create_draft_bill('PO132', bill_data)
        self.assertIsNotNone(bill)

        with self.assertRaises(ValueError):
            # Attempt to update with an invalid status
            update_bill_status('B006', 'INVALID_STATUS')  # Now imported

if __name__ == '__main__':
    unittest.main()