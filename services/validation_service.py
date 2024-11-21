# services/validation_service.py

from database.models import PO, Invoice, Receipt, POState
from database.db_util import get_db_session
import logging

logger = logging.getLogger(__name__)

class ValidationService:
    def compare_invoice_with_po(self, po_number: str, invoice_data: dict) -> bool:
        """Compare invoice data with PO details."""
        with get_db_session() as session:
            po = session.query(PO).filter_by(po_number=po_number).first()
            if not po:
                logger.warning(f"PO {po_number} not found")
                return False
            # Retrieve the latest invoice for the PO
            invoice = session.query(Invoice).filter_by(po_id=po.id).order_by(Invoice.id.desc()).first()
            if not invoice:
                logger.warning(f"No invoice found for PO {po_number}")
                return False
            # Compare amounts
            invoice_amount = invoice_data.get('total_amount', 0.0)
            if po.amount != invoice_amount:
                logger.warning(f"Amount mismatch for PO {po_number}: PO amount {po.amount}, Invoice amount {invoice_amount}")
                self.flag_issues(po_number, 'Amount mismatch between PO and Invoice')
                return False
            return True

    def check_totals_match(self, po_number: str, amount: float) -> bool:
        """Check if totals match between documents."""
        with get_db_session() as session:
            po = session.query(PO).filter_by(po_number=po_number).first()
            if po and po.amount == amount:
                return True
            else:
                logger.warning(f"Totals do not match for PO {po_number}")
                self.flag_issues(po_number, 'Totals do not match')
                return False

    def flag_issues(self, po_number: str, issue_details: str):
        """Flag any issues found during validation."""
        with get_db_session() as session:
            po = session.query(PO).filter_by(po_number=po_number).first()
            if po:
                po.state = POState.ISSUE
                session.commit()
                logger.debug(f"Flagged issue for PO {po_number}: {issue_details}")
            else:
                logger.warning(f"PO {po_number} not found to flag issues")

    def resolve_issues(self, po_number: str):
        """Resolve flagged issues."""
        with get_db_session() as session:
            po = session.query(PO).filter_by(po_number=po_number).first()
            if po and po.state == POState.ISSUE:
                po.state = POState.TO_VERIFY
                session.commit()
                logger.debug(f"Issues resolved for PO {po_number}")
            else:
                logger.warning(f"No issues to resolve for PO {po_number}")

    def validate_receipt_data(self, po_number: str, receipt_data: dict) -> bool:
        """Validate receipt data against PO."""
        # Similar logic to compare_invoice_with_po
        with get_db_session() as session:
            po = session.query(PO).filter_by(po_number=po_number).first()
            if not po:
                logger.warning(f"PO {po_number} not found")
                return False
            # Retrieve the latest receipt for the PO
            receipt = session.query(Receipt).filter_by(po_id=po.id).order_by(Receipt.id.desc()).first()
            if not receipt:
                logger.warning(f"No receipt found for PO {po_number}")
                return False
            # Compare amounts or other details
            receipt_amount = receipt_data.get('total_amount', 0.0)
            if po.amount != receipt_amount:
                logger.warning(f"Amount mismatch for PO {po_number}: PO amount {po.amount}, Receipt amount {receipt_amount}")
                self.flag_issues(po_number, 'Amount mismatch between PO and Receipt')
                return False
            return True