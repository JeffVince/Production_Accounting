# /webhooks/mercury_webhook_handler.py

import logging
from flask import Blueprint, request, jsonify
from services.po_modification_service import POModificationService
from utilities.logger import setup_logging

logger = logging.getLogger(__name__)
setup_logging()

mercury_blueprint = Blueprint('mercury', __name__)


class MercuryWebhookHandler:
    def __init__(self):
        self.po_modification_service = POModificationService()

    def handle_mercury_event(self, event):
        """Handle incoming Mercury Bank webhook event."""
        logger.info("Received Mercury Bank event.")
        transaction_data = event.get('transaction', {})
        self.process_payment_status_change(transaction_data)
        return jsonify({"message": "Event processed"}), 200

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


handler = MercuryWebhookHandler()


@mercury_blueprint.route('/', methods=['POST'])
def mercury_webhook():
    event = request.get_json()
    return handler.handle_mercury_event(event)
