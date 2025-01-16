from sqlalchemy.orm import Session
from sqlalchemy import select
import logging
from datetime import datetime

from dropbox_util import dropbox_util
from models import PurchaseOrder, DetailItem, Invoice
from database.db_util import get_db_session
from singleton import SingletonMeta


class DropboxDatabaseUtil():
    def __init__(self):
        self.logger = logging.getLogger("app_logger")
        self.dropbox_util = dropbox_util
        self.logger.info("📦 Dropbox Database Util initialized 🌟")
        #self._initialized = True

    def add_invoice_link_to_detail_items(self, project_id: str, po_number: str, invoice_number: int, file_link: str):
        with get_db_session() as session:
            # Find the purchase order
            po = session.execute(
                select(PurchaseOrder).where(PurchaseOrder.project_id == project_id, PurchaseOrder.po_number == po_number)
            ).scalar_one_or_none()

            if not po:
                self.logger.error(f"PurchaseOrder not found for {project_id}_{po_number}. Cannot add invoice link.")
                return

            # Update detail items matching this invoice number
            detail_items = session.execute(
                select(DetailItem).where(
                    DetailItem.parent_surrogate_id == po.po_surrogate_id,
                    DetailItem.detail_number == invoice_number
                )
            ).scalars().all()

            for di in detail_items:
                di.file_link = file_link

            session.commit()

    def create_or_update_invoice(self, project_id: str, po_number: str, invoice_number: int,
                                 transaction_date: str, term: int, total: float, file_link: str):
        # Convert transaction_date to datetime if needed
        tx_date = None
        if transaction_date:
            # Expecting YYYY-MM-DD format
            try:
                tx_date = datetime.strptime(transaction_date, "%Y-%m-%d")
            except ValueError:
                self.logger.warning(f"Transaction date '{transaction_date}' not in expected format YYYY-MM-DD.")

        with get_db_session() as session:
            # Find PO
            po = session.execute(
                select(PurchaseOrder).where(PurchaseOrder.project_id == project_id, PurchaseOrder.po_number == po_number)
            ).scalar_one_or_none()

            if not po:
                self.logger.error(f"PurchaseOrder not found for {project_id}_{po_number}. Cannot create/update invoice.")
                return

            # Check if invoice exists
            invoice = session.execute(
                select(Invoice).where(Invoice.po_id == po.po_surrogate_id, Invoice.invoice_number == invoice_number)
            ).scalar_one_or_none()

            if invoice:
                # Update existing invoice
                invoice.transaction_date = tx_date
                invoice.term = term
                invoice.total = total
                invoice.file_link = file_link
            else:
                # Create new invoice
                invoice = Invoice(
                    transaction_date=tx_date,
                    term=term,
                    total=total,
                    invoice_number=invoice_number,
                    file_link=file_link,
                    po_id=po.po_surrogate_id
                )
                session.add(invoice)

            session.commit()

    def get_detail_item_pulse_ids_for_invoice(self, project_id: str, po_number: str, invoice_number: int):
        with get_db_session() as session:
            # Find PO
            po = session.execute(
                select(PurchaseOrder).where(PurchaseOrder.project_id == project_id, PurchaseOrder.po_number == po_number)
            ).scalar_one_or_none()

            if not po:
                self.logger.error(
                    f"PurchaseOrder not found for {project_id}_{po_number}. Cannot get detail item pulse_ids."
                )
                return []

            detail_items = session.execute(
                select(DetailItem).where(
                    DetailItem.parent_surrogate_id == po.po_surrogate_id,
                    DetailItem.detail_number == invoice_number
                )
            ).scalars().all()

            # Return pulse_ids (for Monday updates), ensure they are not None
            return [d.pulse_id for d in detail_items if d.pulse_id is not None]


dropbox_database_util = DropboxDatabaseUtil()