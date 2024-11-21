# tests/test_payment_backpropagation_service.py
import unittest

from services.payment_backpropagation_service import PaymentBackpropagationService
from unittest.mock import MagicMock
from database.models import PO, POState
from tests.base_test import BaseTestCase


class TestPaymentBackpropagationService(BaseTestCase):
    def setUp(self):
        super().setUp()  # Call BaseTestCase's setUp to initialize the database
        self.service = PaymentBackpropagationService()

        # Mock dependencies
        self.service.mercury_service = MagicMock()
        self.service.xero_service = MagicMock()
        self.service.monday_service = MagicMock()

        # Add initial test data
        with self.session_scope() as session:
            po = PO(po_number='PO123', amount=1000.0, state=POState.APPROVED)
            session.add(po)

    def test_update_systems_on_payment(self):
        self.service.update_systems_on_payment('PO123')
        self.service.mercury_service.confirm_payment_execution.assert_called_with('PO123')
        self.service.xero_service.reconcile_transaction.assert_called_with('PO123')
        self.service.monday_service.update_po_status.assert_called_with('PO123', 'PAID')

    def test_backpropagate_payment_status(self):
        self.service.backpropagate_payment_status('PO123', 'PAID')
        with self.session_scope() as session:
            po = session.query(PO).filter_by(po_number='PO123').first()
            self.assertEqual(po.state, POState.PAID)
        self.service.monday_service.update_po_status.assert_called_with('PO123', 'PAID')

    def test_notify_stakeholders_of_payment(self):
        # Assuming a log message is emitted for notification
        with self.assertLogs(logger='services.payment_backpropagation_service', level='INFO') as log:
            self.service.notify_stakeholders_of_payment('PO123')
            self.assertIn('Notification sent for payment of PO PO123', log.output[0])


if __name__ == '__main__':
    unittest.main()