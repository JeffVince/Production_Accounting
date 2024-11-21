# tests/test_xero_service.py

import unittest
from unittest.mock import MagicMock, patch
from integrations.xero_api import XeroAPI
from tests.base_test import BaseTestCase
from database.models import PO, Vendor
from database.db_util import get_db_session

class TestXeroAPI(BaseTestCase):
    def setUp(self):
        super().setUp()

        # Add test data to the database
        with get_db_session() as session:
            vendor = Vendor(vendor_name='Vendor A')
            po = PO(po_number='PO123', amount=1000.0, vendor=vendor)
            session.add(po)
            session.commit()

    @patch('integrations.xero_api.Xero')
    def test_create_draft_bill(self, mock_xero):
        # Mock the Xero instance
        mock_xero_instance = mock_xero.return_value

        # Mock the invoices endpoint
        mock_invoices = MagicMock()
        mock_invoices.put.return_value = [{'InvoiceID': 'INV123'}]
        mock_xero_instance.invoices = mock_invoices

        # Initialize XeroAPI with the mocked Xero instance
        self.xero_api = XeroAPI()

        bill_data = {
            'Contact': {'Name': 'Vendor A'},
            'LineItems': [{'Description': 'Test Item', 'Quantity': 1, 'UnitAmount': 100}],
        }

        result = self.xero_api.create_draft_bill(bill_data)

        # Assertions
        self.assertEqual(result[0]['InvoiceID'], 'INV123')
        mock_invoices.put.assert_called_with(bill_data)

    @patch('integrations.xero_api.Xero')
    def test_create_spend_money_transaction(self, mock_xero):
        # Mock the Xero instance
        mock_xero_instance = mock_xero.return_value

        # Mock the bank_transactions endpoint
        mock_bank_transactions = MagicMock()
        mock_bank_transactions.put.return_value = [{'BankTransactionID': 'BT123'}]
        mock_xero_instance.bank_transactions = mock_bank_transactions

        # Initialize XeroAPI with the mocked Xero instance
        self.xero_api = XeroAPI()

        transaction_data = {
            'Contact': {'Name': 'Vendor A'},
            'LineItems': [{'Description': 'Test Expense', 'Quantity': 1, 'UnitAmount': 50}],
        }

        result = self.xero_api.create_spend_money_transaction(transaction_data)

        # Assertions
        self.assertEqual(result[0]['BankTransactionID'], 'BT123')
        mock_bank_transactions.put.assert_called_with(transaction_data)

if __name__ == '__main__':
    unittest.main()