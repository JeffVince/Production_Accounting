# services/po_log_service.py

from database.models import PurchaseOrder, POState, DetailItemState, DetailItem
from database.db_util import get_db_session
from monday_service import MondayService
from typing import List
import logging

logger = logging.getLogger(__name__)


def update_po_database(po_number: str, main_item: PurchaseOrder):
    """Update the PO database with new entries."""
    with get_db_session() as session:
        po = session.query(PO).filter_by(po_number=po_number).first()
        if po:
            logger.debug(f"Updating PO {po_number}")
            po.description = main_item.description
            po.amount = float(main_item.amount) if main_item.amount else po.amount
            po.state = POState(main_item.po_status) if main_item.po_status else po.state
        else:
            logger.debug(f"Creating new PO {po_number}")
            po = PO(
                po_number=po_number,
                description=main_item.description,
                amount=float(main_item.amount) if main_item.amount else 0.0,
                state=POState(main_item.po_status) if main_item.po_status else POState.PENDING,
            )
            session.add(po)
        session.commit()


def process_receipt_entries(entries: List[DetailItem]):
    """Process receipt entries associated with POs."""
    for entry in entries:
        po_number = entry.main_item_id
        # Process receipts for the PO
        # For example, update the PO state based on receipt data
        pass


class POLogService:
    def __init__(self):
        self.monday_service = MondayService()

    def fetch_po_log_entries(self) -> List[PurchaseOrder]:
        """Fetch PO log entries from the database."""
        with get_db_session() as session:
            entries = session.query(PurchaseOrder).all()
            # Detach instances to avoid DetachedInstanceError
            return [session.expunge(entry) or entry for entry in entries]

    def process_po_log_entries(self, entries: List[PurchaseOrder]):
        """Process a list of PO log entries."""
        for entry in entries:
            po_number = entry.item_id
            update_po_database(po_number, entry)
            if entry.po_status == 'RTP':
                self.trigger_rtp_in_monday(po_number)

    def trigger_rtp_in_monday(self, po_number: str):
        """Trigger Ready To Pay status in Monday.com."""
        self.monday_service.update_po_status(po_number, 'RTP')
