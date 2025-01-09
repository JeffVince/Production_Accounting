# /orchestration/orchestrator.py
import glob
import os
import threading
import time
import logging
import re

from xero_files.xero_api import xero_api
from config import Config
from dropbox_files.dropbox_service import dropbox_service
from ocr_service import OCRService
from monday_files.monday_service import monday_service
from xero_services import xero_services


class Orchestrator:
    def __init__(self):
        # Initialize Services
        self.logger = logging.getLogger(self.__class__.__name__)
        self.dropbox_service = dropbox_service
        self.config = Config()
        self.monday_service = monday_service
        self.ocr_service = OCRService()
        self.xero_api = xero_api
        self.logger = logging.getLogger("app_logger")
        self.xero_services = xero_services

    def start_background_tasks(self):
        """
        Start any necessary background tasks that should run automatically.
        Currently this includes an example of checking PO Log files in Dropbox (if needed).
        """
        self.logger.info("Starting background tasks...")

        if dropbox_service.USE_TEMP_FILE:
            log_dir = "./temp_files/"
            absolute_log_dir = os.path.abspath(log_dir)

            # Identify all PO_LOG files with a matching pattern
            log_files = glob.glob(os.path.join(absolute_log_dir, "PO_LOG_2416-*.txt"))

            if log_files:
                latest_log_file = max(log_files, key=os.path.getmtime)
                self.dropbox_service.po_log_orchestrator(latest_log_file)
            else:
                self.logger.error("No PO LOG FILES FOUND FOR TESTING")

        # If you previously scheduled tasks here, those calls have been removed.

    # --------------------------------------------------
    # Single-run methods (replacing previously scheduled tasks)
    # --------------------------------------------------

    def sync_monday_main_items(self):
        """
        Fetch Main Item entries from Monday.com and handle them immediately (one-time run).
        """
        self.logger.info("Fetching Main Item entries")
        try:
            self.monday_service.sync_main_items_from_monday_board()
        except Exception as e:
            self.logger.error(f"Error fetching Main Item entries: {e}")

    def sync_monday_sub_items(self):
        """
        Fetch Sub Item entries from Monday.com and handle them immediately (one-time run).
        """
        self.logger.info("Fetching Sub Item entries")
        try:
            self.monday_service.sync_sub_items_from_monday_board()
        except Exception as e:
            self.logger.error(f"Error fetching Sub Item entries and syncing them to DB: {e}")

    def sync_monday_contacts(self):
        """
        Fetch contact entries from Monday.com and handle them immediately (one-time run).
        """
        self.logger.info("Fetching Contact entries")
        try:
            self.monday_service.sync_contacts_from_monday_board()
        except Exception as e:
            self.logger.error(f"Error fetching Contact entries and syncing them to DB: {e}")


    def sync_spend_money_items(self):
        """
        Retrieve spend money transactions from Xero for a single run.
        """
        self.logger.info("Syncing spend money transactions...")
        result = self.xero_services.load_spend_money_transactions(project_id="2416")
        return result

    def sync_contacts(self):
        """
        Retrieve and populate Xero contacts in a single run.
        """
        self.logger.info("Syncing Xero contacts...")
        result = self.xero_services.populate_xero_contacts()
        return result

    def sync_xero_bills(self):
        """
        Retrieve Xero bills in a single run.
        """
        self.logger.info("Syncing Xero bills...")
        result = self.xero_services.load_bills("2416")
        return result