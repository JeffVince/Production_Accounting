# /orchestration/orchestrator.py

import threading
import time
import logging
from integrations.dropbox_api import DropboxAPI
from integrations.monday_api import MondayAPI
from integrations.xero_api import XeroAPI
from integrations.mercury_bank_api import MercuryBankAPI
from services.dropbox_service import DropboxService
from services.monday_service import MondayService
from services.ocr_service import OCRService

from utilities.logger import setup_logging

logger = logging.getLogger(__name__)
setup_logging()


class Orchestrator:
    def __init__(self):
        # Initialize Services
        self.dropbox_service = DropboxService()
        self.monday_service = MondayService()
        self.ocr_service = OCRService()

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

    def start_background_tasks(self):
        """Start any necessary background tasks."""
        logger.info("Starting background tasks...")
        #self.schedule_po_log_check()
        self.schedule_monday_main_items_sync()
        self.schedule_monday_sub_items_sync()
        self.schedule_monday_contact_sync()
        #self.coordinate_state_transitions()

    def schedule_monday_main_items_sync(self, interval=90000):

        def sync_monday_to_main_items():
            while True:
                time.sleep(interval)
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

    def schedule_monday_contact_sync(self, interval=90000):

        def sync_monday_to_sub_items():
            while True:
                time.sleep(interval)

                logger.info("Fetching Sub Item entries")
                try:
                    self.monday_service.sync_contacts_from_monday_board()
                except Exception as e:
                    logger.error(f"Error fetching Sub Item entries and syncing them to DB: {e}")



        threading.Thread(target=sync_monday_to_sub_items, daemon=True).start()

    # Additional methods can be added to handle other state transitions
