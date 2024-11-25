# /webhooks/dropbox_webhook_handler.py

import logging
from flask import Blueprint, request, jsonify
from services.dropbox_service import DropboxService
from services.ocr_service import OCRService
from utilities.logger import setup_logging

logger = logging.getLogger(__name__)
setup_logging()

dropbox_blueprint = Blueprint('dropbox', __name__)


class DropboxWebhookHandler:
    def __init__(self):
        self.dropbox_service = DropboxService()
        self.ocr_service = OCRService()

    def handle_dropbox_event(self, event):
        """Handle incoming Dropbox webhook event."""
        logger.info("Received Dropbox event.")
        event_data = event.get('list_folder', {}).get('accounts', [])
        for account_id in event_data:
            self.process_new_file_event(account_id)
        return jsonify({"message": "Dropbox event processed"}), 200

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


handler = DropboxWebhookHandler()


@dropbox_blueprint.route('/', methods=['GET', 'POST'])
def dropbox_webhook():
    if request.method == 'GET':
        # Verification challenge (if applicable)
        challenge = request.args.get('challenge')
        if challenge:
            logger.info("Dropbox webhook verification challenge received.")
            return jsonify({'challenge': challenge}), 200
        return jsonify({"message": "No challenge provided."}), 400
    elif request.method == 'POST':
        # Handle the Dropbox event
        event = request.get_json()
        return handler.handle_dropbox_event(event)
