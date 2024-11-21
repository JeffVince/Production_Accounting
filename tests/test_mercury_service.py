# tests/test_mercury_service.py

import unittest
from services.mercury_service import MercuryService
from unittest.mock import patch, MagicMock
from database.models import PO, Transaction, TransactionState
from database.db_util import get_db_session
from tests.base_test import BaseTestCase
from utilities.config import Config

class TestMercuryService(BaseTestCase):
    def setUp(self):
        super().setUp()
        Config.MERCURY_API_TOKEN = 'dummy_api_token'
        self.service = MercuryService()
        # Add test data to the database
        with self.session_scope() as session:
            po = PO(po_number='PO123', amount=1000.0)
            session.add(po)

    @patch('services.mercury_service.requests.post')
    @patch('services.mercury_service.add_or_update_transaction')
    def test_initiate_payment(self, mock_add_or_update_transaction, mock_post):
        mock_post.return_value.status_code = 200
        mock_post.return_value.json.return_value = {'id': 'TX123', 'amount': 1000.0}
        payment_data = {'amount': 1000.0, 'recipient': 'Vendor A'}
        self.service.initiate_payment('PO123', payment_data)
        mock_post.assert_called()
        mock_add_or_update_transaction.assert_called()

    @patch('services.mercury_service.requests.get')
    @patch('services.mercury_service.update_transaction_status')
    def test_monitor_payment_status(self, mock_update_transaction_status, mock_get):
        with self.session_scope() as session:
            po = session.query(PO).filter_by(po_number='PO123').first()
            transaction = Transaction(po_id=po.id, transaction_id='TX123', state=TransactionState.PENDING)
            session.add(transaction)
        mock_get.return_value.status_code = 200
        mock_get.return_value.json.return_value = {'status': 'paid'}
        self.service.monitor_payment_status('PO123')
        mock_get.assert_called()
        mock_update_transaction_status.assert_called_with('TX123', 'PAID')

if __name__ == '__main__':
    unittest.main()