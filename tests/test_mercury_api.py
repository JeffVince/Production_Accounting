# tests/test_mercury_bank_api.py

import unittest
from unittest.mock import patch
from integrations.mercury_bank_api import MercuryBankAPI

class TestMercuryBankAPI(unittest.TestCase):
    def setUp(self):
        self.mercury_api = MercuryBankAPI()
        self.mercury_api.api_token = 'dummy_token'
        self.mercury_api.api_url = 'https://backend.mercury.com/api/v1'

    @patch('integrations.mercury_bank_api.requests.request')
    def test_create_payment_transaction(self, mock_request):
        mock_request.return_value.status_code = 200
        mock_request.return_value.json.return_value = {'id': 'TX123'}
        transaction_data = {'amount': 1000.0, 'recipient': 'Vendor A'}
        response = self.mercury_api.create_payment_transaction(transaction_data)
        self.assertEqual(response['id'], 'TX123')
        mock_request.assert_called()

    @patch('integrations.mercury_bank_api.requests.request')
    def test_get_transaction_status(self, mock_request):
        mock_request.return_value.status_code = 200
        mock_request.return_value.json.return_value = {'id': 'TX123', 'status': 'paid'}
        response = self.mercury_api.get_transaction_status('TX123')
        self.assertEqual(response['status'], 'paid')
        mock_request.assert_called()

    @patch('integrations.mercury_bank_api.requests.request')
    def test_execute_payment(self, mock_request):
        mock_request.return_value.status_code = 200
        mock_request.return_value.json.return_value = {'id': 'TX123', 'status': 'paid'}
        response = self.mercury_api.execute_payment('TX123')
        self.assertEqual(response['status'], 'paid')
        mock_request.assert_called()

    @patch('integrations.mercury_bank_api.requests.request')
    def test_fetch_transactions(self, mock_request):
        mock_request.return_value.status_code = 200
        mock_request.return_value.json.return_value = {'transactions': []}
        response = self.mercury_api.fetch_transactions({'limit': 10})
        self.assertIsInstance(response['transactions'], list)
        mock_request.assert_called()

if __name__ == '__main__':
    unittest.main()