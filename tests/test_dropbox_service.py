# tests/test_dropbox_service.py

import unittest
from services.dropbox_service import DropboxService
from unittest.mock import MagicMock, patch
from utilities.config import Config

class MockDropboxClient:
    def files_download(self, file_path):
        class Response:
            content = b'file content'
        return None, Response()

class MockOCRService:
    def extract_text_from_file(self, file_content):
        return "Extracted text from OCR"

class MockMondayService:
    def update_po_status(self, po_number, status):
        pass  # Mock implementation

class TestDropboxService(unittest.TestCase):
    def setUp(self):
        Config.DROPBOX_REFRESH_TOKEN = 'dummy_refresh_token'
        Config.DROPBOX_APP_KEY = 'dummy_app_key'
        Config.DROPBOX_APP_SECRET = 'dummy_app_secret'
        self.service = DropboxService()
        # Inject mock dependencies
        self.service.dbx = MockDropboxClient()
        self.service.ocr_service = MockOCRService()
        self.service.monday_service = MockMondayService()

    def test_parse_file_name(self):
        file_name = 'PO123_VendorName_20231118_invoice.pdf'
        metadata = self.service.parse_file_name(file_name)
        self.assertEqual(metadata['po_number'], 'PO123')
        self.assertEqual(metadata['file_type'], 'invoice')

    @patch('services.dropbox_service.add_file_record')
    def test_handle_new_file(self, mock_add_file_record):
        self.service.handle_new_file('/path/to/PO123_VendorName_20231118_invoice.pdf')
        mock_add_file_record.assert_called()
        self.service.monday_service.update_po_status('PO123', 'New invoice uploaded')

if __name__ == '__main__':
    unittest.main()