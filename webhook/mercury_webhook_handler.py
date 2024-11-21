# /webhooks/mercury_webhook_handler.py

import threading
import logging
from flask import Flask, request
from services.mercury_service import MercuryService
from services.po_modification_service import POModificationService
from utilities.config import Config
from utilities.logger import setup_logging

logger = logging.getLogger(__name__)
setup_logging()

app = Flask(__name__)

class MercuryWebhookHandler:
    def __init__(self):
        self.mercury_service = MercuryService()
        self.po_modification_service = POModificationService()
        self.port = Config.MERCURY_WEBHOOK_PORT

    def handle_mercury_event(self, event):
        """Handle incoming Mercury Bank webhook event."""
        logger.info("Received Mercury Bank event.")
        transaction_data = event.get('transaction', {})
        self.process_payment_status_change(transaction_data)

    def process_payment_status_change(self, event_data):
        """Process payment status change from Mercury."""
        transaction_id = event_data.get('id')
        new_status = event_data.get('status')
        po_number = self.mercury_service.get_po_number_from_transaction(transaction_id)
        logger.info(f"Transaction {transaction_id} status changed to {new_status} in Mercury.")

        # Update PO state based on the new status
        if new_status == 'Paid':
            self.po_modification_service.update_po_state(po_number, 'PAID')
        elif new_status == 'Confirmed':
            self.po_modification_service.update_po_state(po_number, 'PAID')  # Or another state if needed
        # Add more status mappings as needed

    def start(self):
        """Start the Flask app in a separate thread."""
        def run_app():
            app.run(port=self.port, debug=False)

        threading.Thread(target=run_app, daemon=True).start()

# Flask routes for the webhook
@app.route('/webhook/mercury', methods=['POST'])
def webhook():
    event = request.get_json()
    handler = MercuryWebhookHandler()
    handler.handle_mercury_event(event)
    return '', 200