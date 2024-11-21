# services/ocr_service.py

import pytesseract
from PIL import Image
import io
import logging

logger = logging.getLogger(__name__)

class OCRService:
    def extract_text_from_file(self, file_data: bytes) -> str:
        """Extract text from a file (invoice, receipt, or W-9)."""
        try:
            image = Image.open(io.BytesIO(file_data))
            text = pytesseract.image_to_string(image)
            return text
        except Exception as e:
            logger.error(f"OCR extraction failed: {e}")
            return ""

    def extract_text_from_invoice(self, file_data: bytes) -> str:
        """Extract text specifically from an invoice file."""
        return self.extract_text_from_file(file_data)

    def parse_invoice_details(self, text_data: str) -> dict:
        """Parse invoice details from extracted text."""
        details = {}
        lines = text_data.split('\n')
        for line in lines:
            if 'Invoice Number:' in line:
                details['invoice_number'] = line.split(':')[1].strip()
            elif 'Total Amount:' in line:
                details['total_amount'] = float(line.split(':')[1].strip().replace('$', ''))
        return details

    def extract_text_from_w9(self, file_data: bytes) -> str:
        """Extract text from a W-9 form."""
        return self.extract_text_from_file(file_data)

    def parse_w9_details(self, text_data: str) -> dict:
        """Parse details from a W-9 form."""
        details = {}
        lines = text_data.split('\n')
        for i, line in enumerate(lines):
            if 'Name' in line:
                details['name'] = lines[i + 1].strip()
            if 'Tax ID' in line:
                details['tax_id'] = lines[i + 1].strip()
        return details

    def extract_text_from_receipt(self, file_data: bytes) -> str:
        """Extract text from a receipt."""
        return self.extract_text_from_file(file_data)

    def parse_receipt_details(self, text_data: str) -> dict:
        """Parse receipt details from extracted text."""
        details = {}
        # Implement parsing logic specific to receipts
        return details