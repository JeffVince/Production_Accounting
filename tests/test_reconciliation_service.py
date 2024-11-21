from services.reconciliation_service import ReconciliationService
from tests.base_test import BaseTestCase
from unittest.mock import MagicMock
from database.models import PO, POState

class TestReconciliationService(BaseTestCase):
    def setUp(self):
        super().setUp()
        self.service = ReconciliationService()
        self.service.xero_service = MagicMock()

    def test_update_po_status_to_reconciled(self):
        # Insert test data
        with self.session_scope() as session:
            po = PO(po_number='PO123', state=POState.PAID)
            session.add(po)

        # Update status
        self.service.update_po_status_to_reconciled('PO123')

        # Verify status update
        with self.session_scope() as session:
            po = session.query(PO).filter_by(po_number='PO123').first()
            self.assertEqual(po.state, POState.RECONCILED)

    def test_handle_payment_reconciliation(self):
        # Insert test data
        with self.session_scope() as session:
            po = PO(po_number='PO123', state=POState.PAID)
            session.add(po)

        # Handle reconciliation
        self.service.handle_payment_reconciliation('PO123')

        # Verify external call and status update
        self.service.xero_service.reconcile_transaction.assert_called_with('PO123')
        with self.session_scope() as session:
            po = session.query(PO).filter_by(po_number='PO123').first()
            self.assertEqual(po.state, POState.RECONCILED)