# services/po_modification_service.py

from database.models import PO
from database.db_util import get_db_session
from monday_service import MondayService
import logging

logger = logging.getLogger(__name__)

class POModificationService:
    def __init__(self):
        self.monday_service = MondayService()

    def detect_po_changes(self, po_number: str) -> dict:
        """Detect changes made to a PO."""
        # Implementation logic to detect changes, possibly comparing with previous state
        # For demonstration, we assume some changes are detected
        changes = {'amount': 1200.0}
        logger.debug(f"Changes detected for PO {po_number}: {changes}")
        return changes

    def apply_modifications(self, po_number: str, changes: dict):
        """Apply modifications to a PO."""
        with get_db_session() as session:
            po = session.query(PO).filter_by(po_number=po_number).first()
            if not po:
                logger.warning(f"PO {po_number} not found")
                return
            for key, value in changes.items():
                setattr(po, key, value)
            session.commit()
            logger.debug(f"Applied changes to PO {po_number}")

    def update_related_systems(self, po_number: str):
        """Update systems affected by PO modifications."""
        # Notify Monday.com
        self.monday_service.update_po_status(po_number, 'Modified')
        # Update Xero records if necessary
        # Other systems...

    def handle_po_modification(self, po_number: str):
        """Main method to handle PO modifications."""
        changes = self.detect_po_changes(po_number)
        if changes:
            self.apply_modifications(po_number, changes)
            self.update_related_systems(po_number)