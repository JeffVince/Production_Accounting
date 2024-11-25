# /webhooks/xero_webhook_handler.py

import logging
from flask import Blueprint, request, jsonify
from services.po_modification_service import POModificationService
from utilities.logger import setup_logging

logger = logging.getLogger(__name__)
setup_logging()

xero_blueprint = Blueprint('xero', __name__)


class XeroWebhookHandler:
    def __init__(self):
        self.po_modification_service = POModificationService()

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
        return jsonify({"message": "Xero event processed"}), 200

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


handler = XeroWebhookHandler()


@xero_blueprint.route('/', methods=['POST'])
def xero_webhook():
    event = request.get_json()
    return handler.handle_xero_event(event)
