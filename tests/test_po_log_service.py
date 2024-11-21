# tests/test_po_log_service.py

from services.po_log_service import POLogService
from tests.base_test import BaseTestCase
from unittest.mock import MagicMock
from database.models import MainItem, PO, POState

class MockMondayService:
    def update_po_status(self, po_number, status):
        pass  # Mock implementation

class TestPOLogService(BaseTestCase):
    def setUp(self):
        super().setUp()
        self.service = POLogService()
        self.service.monday_service = MockMondayService()

        # Add test data to the database
        with self.session_scope() as session:
            main_item = MainItem(
                item_id='PO123',
                name='Test PO',
                description='Test Description',
                amount='1000',
                po_status='PENDING',
            )
            session.add(main_item)

    def test_fetch_po_log_entries(self):
        entries = self.service.fetch_po_log_entries()
        self.assertIsInstance(entries, list)
        self.assertEqual(len(entries), 1)
        self.assertEqual(entries[0].item_id, 'PO123')

    def test_process_po_log_entries(self):
        entries = self.service.fetch_po_log_entries()
        self.service.process_po_log_entries(entries)
        with self.session_scope() as session:
            po = session.query(PO).filter_by(po_number='PO123').first()
            self.assertIsNotNone(po)
            self.assertEqual(po.amount, 1000.0)
            self.assertEqual(po.state, POState.PENDING)

    def test_trigger_rtp_in_monday(self):
        self.service.monday_service.update_po_status = MagicMock()
        self.service.trigger_rtp_in_monday('PO123')
        self.service.monday_service.update_po_status.assert_called_with('PO123', 'RTP')