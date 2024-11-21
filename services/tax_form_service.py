# services/tax_form_service.py

from ocr_service import OCRService
from database.models import TaxForm, Vendor, PO
from database.db_util import get_db_session
import logging

logger = logging.getLogger(__name__)

class TaxFormService:
    def __init__(self):
        self.ocr_service = OCRService()

    def process_tax_form(self, po_number: str, tax_form_data: bytes):
        """Process a tax form associated with a PO."""
        text_data = self.ocr_service.extract_text_from_w9(tax_form_data)
        valid = self.validate_tax_form(text_data)
        status = 'Validated' if valid else 'Invalid'
        self.update_tax_form_status(po_number, status, text_data)
        return valid

    def validate_tax_form(self, text_data: str) -> bool:
        """Validate the contents of a tax form."""
        required_fields = ['Name', 'Tax ID', 'Signature']
        for field in required_fields:
            if field not in text_data:
                logger.debug(f"Missing field {field} in tax form")
                return False
        return True

    def update_tax_form_status(self, po_number: str, status: str, data: str):
        """Update the status of a tax form."""
        with get_db_session() as session:
            po = session.query(PO).filter_by(po_number=po_number).first()
            if not po or not po.vendor:
                logger.warning(f"PO {po_number} or Vendor not found")
                return
            if not po.vendor.tax_form:
                tax_form = TaxForm(
                    vendor_id=po.vendor.id,
                    form_type='W-9',
                    status=status,
                    data=data,
                )
                session.add(tax_form)
            else:
                po.vendor.tax_form.status = status
                po.vendor.tax_form.data = data
            session.commit()
            logger.debug(f"Updated tax form status for PO {po_number} to {status}")