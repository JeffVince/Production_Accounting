# tests/test_mercury_database_util.py

import unittest
from tests.base_test import BaseTestCase
from database.mercury_database_util import (
    initiate_payment,
    confirm_payment_execution,
    confirm_payment_confirmation,
    get_transaction_status_by_po
)
from database.po_repository import add_or_update_po
from database.models import POState, TransactionState, Transaction, Contact, Vendor
from sqlalchemy.exc import SQLAlchemyError


class TestMercuryDatabaseUtil(BaseTestCase):

    def setUp(self):
        super().setUp()

        # Add a Contact
        contact_data = {
            'contact_id': 'C003',
            'name': 'John Doe',
            'email': 'john@example.com',
            'phone': '1234567890'
        }
        with self.session_scope() as session:
            contact = session.query(Contact).filter_by(contact_id='C003').first()
            if not contact:
                contact = Contact(**contact_data)
                session.add(contact)
                session.commit()
            self.contact = contact

        # Add a Vendor
        vendor_data = {
            'vendor_name': 'Vendor B',
            'contact_id': self.contact.id
        }
        with self.session_scope() as session:
            vendor = session.query(Vendor).filter_by(vendor_name='Vendor B').first()
            if not vendor:
                vendor = Vendor(**vendor_data)
                session.add(vendor)
                session.commit()
            self.vendor = vendor

        # Add a PO
        po_data = {
            'po_number': 'PO133',
            'amount': 11000.0,
            'description': 'Mercury Test PO',
            'vendor_id': self.vendor.id,
            'state': POState.PENDING
        }
        self.po = add_or_update_po(po_data)

    def test_confirm_payment_execution_and_confirmation(self):
        """
        Test confirming payment execution and bank confirmation.
        """
        transaction_data = {
            'transaction_id': 'T005',
            'amount': 11000.0
        }
        transaction = initiate_payment('PO133', transaction_data)
        self.assertIsNotNone(transaction)
        self.assertEqual(transaction.transaction_id, 'T005')
        self.assertEqual(transaction.state, TransactionState.PENDING)

        # Confirm Payment Execution
        confirm_payment_execution('T005')
        with self.session_scope() as session:
            updated_transaction = session.query(Transaction).filter_by(transaction_id='T005').first()
            self.assertIsNotNone(updated_transaction)
            self.assertEqual(updated_transaction.state, TransactionState.PAID)

        # Confirm Payment Confirmation
        confirm_payment_confirmation('T005')
        with self.session_scope() as session:
            updated_transaction = session.query(Transaction).filter_by(transaction_id='T005').first()
            self.assertIsNotNone(updated_transaction)
            self.assertEqual(updated_transaction.state, TransactionState.CONFIRMED)

    def test_get_transaction_status_by_po(self):
        """
        Test retrieving the transaction status associated with a PO.
        """
        transaction_data = {
            'transaction_id': 'T006',
            'amount': 5000.0
        }
        transaction = initiate_payment('PO133', transaction_data)
        self.assertIsNotNone(transaction)

        status = get_transaction_status_by_po('PO133')
        self.assertEqual(status, TransactionState.PENDING)

        confirm_payment_execution('T006')
        status = get_transaction_status_by_po('PO133')
        self.assertEqual(status, TransactionState.PAID)

    def test_initiate_payment(self):
        """
        Test initiating a payment transaction.
        """
        transaction_data = {
            'transaction_id': 'T007',
            'amount': 7500.0
        }
        transaction = initiate_payment('PO133', transaction_data)
        self.assertIsNotNone(transaction)
        self.assertEqual(transaction.transaction_id, 'T007')
        self.assertEqual(transaction.state, TransactionState.PENDING)


if __name__ == '__main__':
    unittest.main()