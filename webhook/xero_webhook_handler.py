# /webhooks/xero_webhook_handler.py

import threading
import logging
from flask import Flask, request
from services.xero_service import XeroService
from services.po_modification_service import POModificationService
from utilities.config import Config
from utilities.logger import setup_logging

logger = logging.getLogger(__name__)
setup_logging()

app = Flask(__name__)

class XeroWebhookHandler:
    def __init__(self):
        self.xero_service = XeroService()
        self.po_modification_service = POModificationService()
        self.port = Config.XERO_WEBHOOK_PORT

    def handle_xero_event(self, event):
        """Handle incoming Xero webhook event."""
        logger.info("Received Xero event.")
        events = event.get('events', [])
        for event_data in events:
            event_type = event_data.get('eventCategory')
            if event_type == 'INVOICE':
                self.process_bill_status_change(event_data)
            elif event_type == 'SPEND-MONEY':
                self.process_spend_money_status_change(event_data)

    def process_bill_status_change(self, event_data):
        """Process bill status change from Xero."""
        resource_id = event_data.get('resourceId')
        new_status = self.xero_service.get_bill_status(resource_id)
        po_number = self.xero_service.get_po_number_from_bill(resource_id)
        logger.info(f"Bill {resource_id} status changed to {new_status} in Xero.")

        # Update PO state based on the new status
        if new_status == 'Approved':
            self.po_modification_service.update_po_state(po_number, 'APPROVED')
        elif new_status == 'Paid':
            self.po_modification_service.update_po_state(po_number, 'PAID')
        elif new_status == 'Reconciled':
            self.po_modification_service.update_po_state(po_number, 'RECONCILED')
        # Add more status mappings as needed

    def process_spend_money_status_change(self, event_data):
        """Process Spend Money transaction status change from Xero."""
        resource_id = event_data.get('resourceId')
        new_status = self.xero_service.get_spend_money_status(resource_id)
        po_number = self.xero_service.get_po_number_from_spend_money(resource_id)
        logger.info(f"Spend Money Transaction {resource_id} status changed to {new_status} in Xero.")

        # Update PO subitem state based on the new status
        if new_status == 'Approved':
            self.po_modification_service.update_po_subitem_state(po_number, 'APPROVED')
        elif new_status == 'Reconciled':
            self.po_modification_service.update_po_subitem_state(po_number, 'RECONCILED')
        # Add more status mappings as needed

    def start(self):
        """Start the Flask app in a separate thread."""
        def run_app():
            app.run(port=self.port, debug=False)

        threading.Thread(target=run_app, daemon=True).start()

# Flask routes for the webhook
@app.route('/webhook/xero', methods=['POST'])
def webhook():
    event = request.get_json()
    handler = XeroWebhookHandler()
    handler.handle_xero_event(event)
    return '', 200