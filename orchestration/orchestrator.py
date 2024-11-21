# /orchestration/orchestrator.py

import threading
import time
import logging
from integrations.dropbox_api import DropboxAPI
from integrations.monday_api import MondayAPI
from integrations.xero_api import XeroAPI
from integrations.mercury_bank_api import MercuryBankAPI
from services.po_log_service import POLogService
from services.dropbox_service import DropboxService
from services.monday_service import MondayService
from services.vendor_service import VendorService
from services.tax_form_service import TaxFormService
from services.ocr_service import OCRService
from services.validation_service import ValidationService
from services.xero_service import XeroService
from services.mercury_service import MercuryService
from services.payment_backpropagation_service import PaymentBackpropagationService
from services.po_modification_service import POModificationService
from services.reconciliation_service import ReconciliationService
from services.spend_money_service import SpendMoneyService
from webhook.dropbox_webhook_handler import DropboxWebhookHandler
from webhook.monday_webhook_handler import MondayWebhookHandler
from webhook.xero_webhook_handler import XeroWebhookHandler
from webhook.mercury_webhook_handler import MercuryWebhookHandler
from database import po_repository
from utilities.logger import setup_logging

logger = logging.getLogger(__name__)
setup_logging()

class Orchestrator:
    def __init__(self):
        self.po_log_service = POLogService()
        self.dropbox_service = DropboxService()
        self.monday_service = MondayService()
        self.vendor_service = VendorService()
        self.tax_form_service = TaxFormService()
        self.ocr_service = OCRService()
        self.validation_service = ValidationService()
        self.xero_service = XeroService()
        self.mercury_service = MercuryService()
        self.payment_backpropagation_service = PaymentBackpropagationService()
        self.po_modification_service = POModificationService()
        self.reconciliation_service = ReconciliationService()
        self.spend_money_service = SpendMoneyService()

        self.dropbox_webhook_handler = DropboxWebhookHandler()
        self.monday_webhook_handler = MondayWebhookHandler()
        self.xero_webhook_handler = XeroWebhookHandler()
        self.mercury_webhook_handler = MercuryWebhookHandler()

    def schedule_po_log_check(self, interval=60):
        """Periodically check the PO Log for updates."""
        def check_po_log():
            while True:
                logger.info("Fetching PO Log entries...")
                entries = self.po_log_service.fetch_po_log_entries()
                self.po_log_service.process_po_log_entries(entries)
                time.sleep(interval)
        threading.Thread(target=check_po_log, daemon=True).start()

    def initialize_webhook_listeners(self):
        """Initialize webhook listeners for integrations."""
        logger.info("Initializing webhook listeners...")
        self.dropbox_webhook_handler.start()
        self.monday_webhook_handler.start()
        self.xero_webhook_handler.start()
        self.mercury_webhook_handler.start()

    def coordinate_state_transitions(self, interval=60):
        """Periodically coordinate state transitions based on business logic."""
        def state_transition_loop():
            while True:
                logger.info("Coordinating state transitions...")
                # Process POs that are in 'RTP' state
                pos_in_rtp = po_repository.get_pos_by_status('RTP')
                for po in pos_in_rtp:
                    self.handle_po_in_rtp(po)
                time.sleep(interval)
        threading.Thread(target=state_transition_loop, daemon=True).start()

    def start_background_tasks(self):
        """Start any necessary background tasks."""
        logger.info("Starting background tasks...")

        # Ensure a small delay to guarantee initialization
        time.sleep(1)

        self.schedule_po_log_check()
        self.coordinate_state_transitions()

    def handle_po_in_rtp(self, po):
        """Handle a PO that is in 'RTP' state."""
        logger.info(f"Handling PO in RTP state: {po.po_number}")
        # Business manager begins verification
        # Update PO status to 'TO VERIFY' using po_repository
        po_repository.update_po_status(po.po_number, 'TO VERIFY')

        # Verify tax compliance using tax_form_service
        tax_form_data = None  # Assuming tax form data is retrieved elsewhere
        tax_form_valid = self.tax_form_service.validate_tax_form(tax_form_data)

        if not tax_form_valid:
            logger.warning(f"Tax form invalid for PO {po.po_number}")
            self.tax_form_service.update_tax_form_status(po.po_number, 'ISSUE')
            po_repository.update_po_status(po.po_number, 'ISSUE')
            return

        # Match vendor with contacts
        contact_id = self.vendor_service.match_vendor_with_contacts(po.vendor_name)
        self.vendor_service.link_contact_to_po(po.po_number, contact_id)

        # Update PO status to 'APPROVED'
        po_repository.update_po_status(po.po_number, 'APPROVED')

        # Generate draft bill in Xero
        self.xero_service.generate_draft_bill(po.po_number)

    def handle_new_po(self, po):
        """Handle a new PO added to the PO Log."""
        logger.info(f"Handling new PO: {po.po_number}")
        # Trigger RTP in Monday.com
        self.po_log_service.trigger_rtp_in_monday(po.po_number)
        # Update PO status to 'RTP'
        po_repository.update_po_status(po.po_number, 'RTP')

    def process_po_ready_for_payment(self, po):
        """Process POs that are approved and ready for payment."""
        logger.info(f"Processing approved PO for payment: {po.po_number}")
        # Initiate payment via Mercury
        payment_data = {'amount': po.amount, 'vendor_name': po.vendor_name}
        self.mercury_service.initiate_payment(po.po_number, payment_data)
        # Update PO status to 'PAID' (assuming payment is successful)
        po_repository.update_po_status(po.po_number, 'PAID')

    # Additional methods can be added to handle other state transitions

if __name__ == '__main__':
    orchestrator = Orchestrator()
    orchestrator.initialize_webhook_listeners()
    orchestrator.start_background_tasks()

    # Keep the main thread alive
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Shutting down the application...")