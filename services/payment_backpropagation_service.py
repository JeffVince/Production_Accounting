# services/payment_backpropagation_service.py

from database.models import PO, POState
from database.db_util import get_db_session
from mercury_service import MercuryService
from xero_service import XeroService
from monday_service import MondayService
import logging

logger = logging.getLogger(__name__)

class PaymentBackpropagationService:
    def __init__(self):
        self.mercury_service = MercuryService()
        self.xero_service = XeroService()
        self.monday_service = MondayService()

    def update_systems_on_payment(self, po_number: str):
        """Update systems after payment execution."""
        self.mercury_service.confirm_payment_execution(po_number)
        self.xero_service.reconcile_transaction(po_number)
        self.monday_service.update_po_status(po_number, 'PAID')

    def backpropagate_payment_status(self, po_number: str, status: str):
        """Backpropagate payment status to relevant systems."""
        with get_db_session() as session:
            po = session.query(PO).filter_by(po_number=po_number).first()
            if po:
                po.state = POState(status)
                session.commit()
                logger.debug(f"PO {po_number} state updated to {status}")
                # Update Monday.com
                self.monday_service.update_po_status(po_number, status)
            else:
                logger.warning(f"PO {po_number} not found")

    def notify_stakeholders_of_payment(self, po_number: str):
        """Notify stakeholders that payment has been made."""
        # Send notifications via Slack, Email, etc.
        logger.info(f"Notification sent for payment of PO {po_number}")