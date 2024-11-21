# tests/test_ocr_service.py

import unittest
from unittest.mock import patch
from services.ocr_service import OCRService
from PIL import Image


class TestOCRService(unittest.TestCase):
    def setUp(self):
        self.service = OCRService()
        # Patch pytesseract globally for the service
        self.patcher = patch('pytesseract.image_to_string', return_value="Invoice Number: INV12345\nTotal Amount: $1000")
        self.mock_ocr = self.patcher.start()
        # Patch PIL.Image.open to simulate image opening
        self.image_patcher = patch('PIL.Image.open', return_value=Image.new('RGB', (100, 100)))
        self.mock_image_open = self.image_patcher.start()

    def tearDown(self):
        self.patcher.stop()
        self.image_patcher.stop()

    def test_extract_text_from_invoice(self):
        file_data = b'fake_image_data'
        text = self.service.extract_text_from_invoice(file_data)
        self.assertIn('Invoice Number', text)

    def test_parse_invoice_details(self):
        text_data = "Invoice Number: INV12345\nTotal Amount: $1000"
        details = self.service.parse_invoice_details(text_data)
        self.assertEqual(details['invoice_number'], 'INV12345')
        self.assertEqual(details['total_amount'], 1000.0)


if __name__ == '__main__':
    unittest.main()