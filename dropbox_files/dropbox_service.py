# services/dropbox_service.py

import os
import dropbox
from typing import Dict
from utilities.config import Config
from ocr_service import OCRService
import logging

logger = logging.getLogger(__name__)

class DropboxService:
    def __init__(self):
        self.dbx = dropbox.Dropbox(
            oauth2_refresh_token=Config.DROPBOX_REFRESH_TOKEN,
            app_key=Config.DROPBOX_APP_KEY,
            app_secret=Config.DROPBOX_APP_SECRET,
        )
        self.ocr_service = OCRService()

    def handle_new_file(self, file_path: str):
        """Handle a new file uploaded to Dropbox."""
        _, file_name = os.path.split(file_path)
        metadata = self.parse_file_name(file_name)
        _, response = self.dbx.files_download(file_path)
        file_content = response.content
        file_data = {
            'po_number': metadata['po_number'],
            'file_type': metadata['file_type'],
            'file_path': file_path,
            'data': '',  # Will be filled after OCR
            'status': 'Pending',
        }
        # Perform OCR if necessary
        if metadata['file_type'] in ['invoice', 'receipt']:
            text_data = self.ocr_service.extract_text_from_file(file_content)
            file_data['data'] = text_data
        # Store file metadata in the database
        # add_file_record(file_data)
        # Notify Monday.com
        self.notify_monday_of_new_file(metadata['po_number'], metadata['file_type'])

    def parse_file_name(self, file_name: str) -> Dict[str, str]:
        """Parse the file name to extract metadata."""
        # Assuming file name format: PO123_VendorName_20231118_invoice.pdf
        parts = file_name.split('_')
        if len(parts) < 4:
            raise ValueError("Invalid file name format.")
        po_number = parts[0]
        file_type = parts[-1].split('.')[0]  # 'invoice.pdf' -> 'invoice'
        return {'po_number': po_number, 'file_type': file_type}

    def notify_monday_of_new_file(self, po_number: str, file_type: str):
        """Notify Monday.com of a new file associated with a PO."""
        self.monday_service.update_po_status(po_number, f'New {file_type} uploaded')