# tests/test_xero_api.py

import unittest
from unittest.mock import MagicMock
from integrations.xero_api import XeroAPI

class TestXeroAPI(unittest.TestCase):
    def setUp(self):
        self.xero_api = XeroAPI()
        self.xero_api.xero = MagicMock()

    def test_create_draft_bill(self):
        self.xero_api.xero.invoices.put.return_value = [{'InvoiceID': 'INV123'}]
        bill_data = {'Contact': {'Name': 'Test Vendor'}, 'LineItems': [{'Description': 'Test Item', 'Quantity': 1, 'UnitAmount': 100}]}
        result = self.xero_api.create_draft_bill(bill_data)
        self.assertEqual(result[0]['InvoiceID'], 'INV123')
        self.xero_api.xero.invoices.put.assert_called()

    def test_submit_bill_for_approval(self):
        self.xero_api.xero.invoices.save.return_value = [{'InvoiceID': 'INV123', 'Status': 'SUBMITTED'}]
        result = self.xero_api.submit_bill_for_approval('INV123')
        self.assertEqual(result[0]['Status'], 'SUBMITTED')
        self.xero_api.xero.invoices.save.assert_called()

    def test_approve_bill(self):
        self.xero_api.xero.invoices.save.return_value = [{'InvoiceID': 'INV123', 'Status': 'AUTHORISED'}]
        result = self.xero_api.approve_bill('INV123')
        self.assertEqual(result[0]['Status'], 'AUTHORISED')
        self.xero_api.xero.invoices.save.assert_called()

    def test_create_spend_money_transaction(self):
        self.xero_api.xero.bank_transactions.put.return_value = [{'BankTransactionID': 'BT123'}]
        transaction_data = {'Contact': {'Name': 'Test Vendor'}, 'LineItems': [{'Description': 'Test Expense', 'Quantity': 1, 'UnitAmount': 50}]}
        result = self.xero_api.create_spend_money_transaction(transaction_data)
        self.assertEqual(result[0]['BankTransactionID'], 'BT123')
        self.xero_api.xero.bank_transactions.put.assert_called()

    def test_get_transaction_status(self):
        self.xero_api.xero.bank_transactions.get.return_value = [{'BankTransactionID': 'BT123', 'Status': 'AUTHORISED'}]
        result = self.xero_api.get_transaction_status('BT123')
        self.assertEqual(result[0]['Status'], 'AUTHORISED')
        self.xero_api.xero.bank_transactions.get.assert_called_with('BT123')

    def test_update_bill_status(self):
        self.xero_api.xero.invoices.save.return_value = [{'InvoiceID': 'INV123', 'Status': 'PAID'}]
        result = self.xero_api.update_bill_status('INV123', 'PAID')
        self.assertEqual(result[0]['Status'], 'PAID')
        self.xero_api.xero.invoices.save.assert_called()

    def test_reconcile_transaction(self):
        with self.assertRaises(NotImplementedError):
            self.xero_api.reconcile_transaction('BT123')

if __name__ == '__main__':
    unittest.main()