from services.po_modification_service import POModificationService
from tests.base_test import BaseTestCase
from unittest.mock import MagicMock
from database.models import PO

class TestPOModificationService(BaseTestCase):
    def setUp(self):
        super().setUp()
        self.service = POModificationService()
        self.service.monday_service = MagicMock()

    def test_apply_modifications(self):
        with self.session_scope() as session:
            po = PO(po_number='PO123', amount=1000.0)
            session.add(po)

        changes = {'amount': 1200.0}
        self.service.apply_modifications('PO123', changes)

        with self.session_scope() as session:
            po = session.query(PO).filter_by(po_number='PO123').first()
            self.assertEqual(po.amount, 1200.0)

    def test_update_related_systems(self):
        self.service.update_related_systems('PO123')
        self.service.monday_service.update_po_status.assert_called_with('PO123', 'Modified')

    def test_handle_po_modification(self):
        self.service.detect_po_changes = MagicMock(return_value={'amount': 1200.0})
        self.service.apply_modifications = MagicMock()
        self.service.update_related_systems = MagicMock()

        self.service.handle_po_modification('PO123')

        self.service.apply_modifications.assert_called_with('PO123', {'amount': 1200.0})
        self.service.update_related_systems.assert_called_with('PO123')