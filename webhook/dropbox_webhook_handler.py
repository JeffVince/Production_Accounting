# /webhooks/dropbox_webhook_handler.py

import threading
import logging
from flask import Flask, request
from integrations.dropbox_api import DropboxAPI
from services.dropbox_service import DropboxService
from services.ocr_service import OCRService
from services.validation_service import ValidationService
from utilities.config import Config
from utilities.logger import setup_logging

logger = logging.getLogger(__name__)
setup_logging()

app = Flask(__name__)

class DropboxWebhookHandler:
    def __init__(self):
        self.dropbox_service = DropboxService()
        self.ocr_service = OCRService()
        self.validation_service = ValidationService()
        self.port = Config.DROPBOX_WEBHOOK_PORT

    def handle_dropbox_event(self, event):
        """Handle incoming Dropbox webhook event."""
        logger.info("Received Dropbox event.")
        event_data = event.get('list_folder', {}).get('accounts', [])
        for account_id in event_data:
            self.process_new_file_event(account_id)

    def process_new_file_event(self, account_id):
        """Process new file event from Dropbox."""
        logger.info(f"Processing new file event for account: {account_id}")
        new_files = self.dropbox_service.get_new_files(account_id)
        for file in new_files:
            file_path = file['path_lower']
            logger.info(f"New file detected: {file_path}")
            # Perform OCR on the file
            ocr_result = self.ocr_service.perform_ocr(file_path)
            # Validate the document
            validation_result = self.validation_service.validate_document(ocr_result)
            if validation_result['status'] == 'valid':
                logger.info(f"Document {file_path} validated successfully.")
                # Update PO state or perform necessary actions
                po_number = validation_result['po_number']
                self.validation_service.update_po_state(po_number, 'PAID')
            else:
                logger.warning(f"Validation failed for document {file_path}.")
                po_number = validation_result.get('po_number')
                if po_number:
                    self.validation_service.update_po_state(po_number, 'ISSUE')

    def start(self):
        """Start the Flask app in a separate thread."""
        def run_app():
            app.run(port=self.port, debug=False)

        threading.Thread(target=run_app, daemon=True).start()

# Flask routes for the webhook
@app.route('/webhook/dropbox', methods=['GET', 'POST'])
def webhook():
    if request.method == 'GET':
        # Verification challenge
        challenge = request.args.get('challenge')
        return challenge
    elif request.method == 'POST':
        # Handle the Dropbox event
        event = request.get_json()
        handler = DropboxWebhookHandler()
        handler.handle_dropbox_event(event)
        return '', 200