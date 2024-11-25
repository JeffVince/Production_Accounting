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
from services.ocr_service import OCRService
from services.po_modification_service import POModificationService
from database import po_repository
from utilities.logger import setup_logging

logger = logging.getLogger(__name__)
setup_logging()


class Orchestrator:
    def __init__(self):
        # Initialize Services
        self.po_log_service = POLogService()
        self.dropbox_service = DropboxService()
        self.monday_service = MondayService()
        self.ocr_service = OCRService()
        self.po_modification_service = POModificationService()

    def schedule_po_log_check(self, interval=60):
        """Periodically check the PO Log for updates."""

        def check_po_log():
            while True:
                logger.info("Fetching PO Log entries...")
                try:
                    entries = self.po_log_service.fetch_po_log_entries()
                    self.po_log_service.process_po_log_entries(entries)
                except Exception as e:
                    logger.error(f"Error fetching PO Log entries: {e}")
                time.sleep(interval)

        threading.Thread(target=check_po_log, daemon=True).start()

    def coordinate_state_transitions(self, interval=60):
        """Periodically coordinate state transitions based on business logic."""

        def state_transition_loop():
            while True:
                logger.info("Coordinating state transitions...")
                try:
                    # Process POs that are in 'RTP' state
                    pos_in_rtp = po_repository.get_pos_by_status('RTP')
                    for po in pos_in_rtp:
                        self.handle_po_in_rtp(po)
                except Exception as e:
                    logger.error(f"Error coordinating state transitions: {e}")
                time.sleep(interval)

        threading.Thread(target=state_transition_loop, daemon=True).start()

    def start_background_tasks(self):
        """Start any necessary background tasks."""
        logger.info("Starting background tasks...")
        #self.schedule_po_log_check()
        self.schedule_monday_main_items_sync()
        self.schedule_monday_sub_items_sync()
        #self.coordinate_state_transitions()

    def handle_po_in_rtp(self, po):
        """Handle a PO that is in 'RTP' state."""
        logger.info(f"Handling PO in RTP state: {po.po_number}")
        try:
            # Business manager begins verification
            # Update PO status to 'TO VERIFY' using po_repository
            po_repository.update_po_status(po.po_number, 'TO VERIFY')

            # Verify tax compliance using tax_form_service
            tax_form_data = self.tax_form_service.get_tax_form(po.po_number)  # Assuming a method to retrieve tax form
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

        except Exception as e:
            logger.error(f"Error handling PO {po.po_number} in RTP state: {e}")

    def handle_new_po(self, po):
        """Handle a new PO added to the PO Log."""
        logger.info(f"Handling new PO: {po.po_number}")
        try:
            # Trigger RTP in Monday.com
            self.po_log_service.trigger_rtp_in_monday(po.po_number)
            # Update PO status to 'RTP'
            po_repository.update_po_status(po.po_number, 'RTP')
        except Exception as e:
            logger.error(f"Error handling new PO {po.po_number}: {e}")

    def process_po_ready_for_payment(self, po):
        """Process POs that are approved and ready for payment."""
        logger.info(f"Processing approved PO for payment: {po.po_number}")
        try:
            # Initiate payment via Mercury
            payment_data = {'amount': po.amount, 'vendor_name': po.vendor_name}
            self.mercury_service.initiate_payment(po.po_number, payment_data)
            # Update PO status to 'PAID' (assuming payment is successful)
            po_repository.update_po_status(po.po_number, 'PAID')
        except Exception as e:
            logger.error(f"Error processing payment for PO {po.po_number}: {e}")

    def schedule_monday_main_items_sync(self, interval=90000):

        def sync_monday_to_main_items():
            time.sleep(interval)
            while True:
                logger.info("Fetching Main Item entries")
                try:
                    self.monday_service.sync_main_items_from_monday_board()
                except Exception as e:
                    logger.error(f"Error fetching Main Item entries: {e}")

        threading.Thread(target=sync_monday_to_main_items, daemon=True).start()

    def schedule_monday_sub_items_sync(self, interval=90000):

        def sync_monday_to_sub_items():
            while True:
                time.sleep(interval)
                logger.info("Fetching Sub Item entries")
                try:
                    self.monday_service.sync_sub_items_from_monday_board()
                except Exception as e:
                    logger.error(f"Error fetching Sub Item entries and syncing them to DB: {e}")

        threading.Thread(target=sync_monday_to_sub_items, daemon=True).start()

    # Additional methods can be added to handle other state transitions
