# services/reconciliation_service.py

from database.models import PO, POState
from database.db_util import get_db_session
from xero_service import XeroService
import logging

logger = logging.getLogger(__name__)

class ReconciliationService:
    def __init__(self):
        self.xero_service = XeroService()

    def handle_payment_reconciliation(self, po_number: str):
        """Handle reconciliation of a payment."""
        # Implementation logic, possibly checking Xero
        self.xero_service.reconcile_transaction(po_number)
        self.update_po_status_to_reconciled(po_number)

    def update_po_status_to_reconciled(self, po_number: str):
        """Update PO status to 'Reconciled'."""
        with get_db_session() as session:
            po = session.query(PO).filter_by(po_number=po_number).first()
            if po:
                po.state = POState.RECONCILED
                session.commit()
                logger.debug(f"PO {po_number} status updated to RECONCILED")
            else:
                logger.warning(f"PO {po_number} not found")

    def verify_reconciliation_in_xero(self, po_number: str) -> bool:
        """Verify that reconciliation is complete in Xero."""
        # Implementation logic, possibly querying Xero
        return True  # For demonstration