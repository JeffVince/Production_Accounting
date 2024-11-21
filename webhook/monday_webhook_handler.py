# /webhooks/monday_webhook_handler.py

import threading
import logging
from flask import Flask, request
from services.monday_service import MondayService
from services.po_modification_service import POModificationService
from utilities.config import Config
from utilities.logger import setup_logging

logger = logging.getLogger(__name__)
setup_logging()

app = Flask(__name__)

class MondayWebhookHandler:
    def __init__(self):
        self.monday_service = MondayService()
        self.po_modification_service = POModificationService()
        self.port = Config.MONDAY_WEBHOOK_PORT

    def handle_monday_event(self, event):
        """Handle incoming Monday.com webhook event."""
        logger.info("Received Monday.com event.")
        event_data = event.get('event', {})
        self.process_po_status_change(event_data)

    def process_po_status_change(self, event_data):
        """Process PO status change from Monday.com."""
        item_id = event_data.get('pulseId')
        new_status = event_data.get('value', {}).get('label', '')
        po_number = self.monday_service.get_po_number_from_item(item_id)
        logger.info(f"PO {po_number} status changed to {new_status} in Monday.com.")

        # Update PO state based on the new status
        if new_status == 'RTP':
            self.po_modification_service.update_po_state(po_number, 'RTP')
        elif new_status == 'Issue':
            self.po_modification_service.update_po_state(po_number, 'ISSUE')
        elif new_status == 'Approved':
            self.po_modification_service.update_po_state(po_number, 'APPROVED')
        # Add more status mappings as needed

    def start(self):
        """Start the Flask app in a separate thread."""
        def run_app():
            app.run(port=self.port, debug=False)

        threading.Thread(target=run_app, daemon=True).start()

# Flask routes for the webhook
@app.route('/webhook/monday', methods=['POST'])
def webhook():
    event = request.get_json()
    handler = MondayWebhookHandler()
    handler.handle_monday_event(event)
    return '', 200