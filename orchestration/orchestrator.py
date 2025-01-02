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








class Orchestrator:
    def __init__(self):
        # Initialize Services
        # Set up logging
        self.logger = logging.getLogger(self.__class__.__name__)
        self.dropbox_service = dropbox_service
        self.config = Config()
        self.monday_service = monday_service
        self.ocr_service = OCRService()
        self.xero_api = xero_api
        self.logger = logging.getLogger("app_logger")

    def start_background_tasks(self):
        """Start any necessary background tasks."""
        self.logger.info("Starting background tasks...")
        #self.schedule_po_log_check()

        if dropbox_service.USE_TEMP_FILE:
            # Specify the directory containing the log files
            log_dir = "./temp_files/"

            # Debugging: Print the resolved absolute path
            absolute_log_dir = os.path.abspath(log_dir)

            # Identify all PO_LOG files with a matching pattern
            log_files = glob.glob(os.path.join(absolute_log_dir, "PO_LOG_2416-*.txt"))

            # Debugging: Print found files

            if log_files:
                # Find the most recently modified log file
                latest_log_file = max(log_files, key=os.path.getmtime)

                # Debugging: Print the selected log file

                # Pass the most recent file to the process_po_log function
                self.dropbox_service.po_log_orchestrator(latest_log_file)
            else:
                # Handle the case where no PO_LOG files are found
                self.logger.error("No PO LOG FILES FOUND FOR TESTING")

        #MAIN STUFF
        #self.schedule_monday_main_items_sync()
        #self.schedule_monday_sub_items_sync()
        #self.schedule_monday_contact_sync()
        #self.coordinate_state_transitions()
        self.sync_spend_money_items()

    def schedule_monday_main_items_sync(self, interval=90000):

        def sync_monday_to_main_items():
            while True:
                self.logger.info("Fetching Main Item entries")
                try:
                    self.monday_service.sync_main_items_from_monday_board()
                except Exception as e:
                    self.logger.error(f"Error fetching Main Item entries: {e}")
                time.sleep(interval)

        threading.Thread(target=sync_monday_to_main_items, daemon=True).start()

    def sync_spend_money_items(self):
        result = self.xero_api.get_spend_money_by_reference("2416")
        return result

    def schedule_monday_sub_items_sync(self, interval=90000):

        def sync_monday_to_sub_items():
            while True:
                self.logger.info("Fetching Sub Item entries")
                try:
                    self.monday_service.sync_sub_items_from_monday_board()
                except Exception as e:
                    self.logger.error(f"Error fetching Sub Item entries and syncing them to DB: {e}")
                time.sleep(interval)

        threading.Thread(target=sync_monday_to_sub_items, daemon=True).start()

    def schedule_monday_contact_sync(self, interval=90000):

        def sync_contacts_from_monday_board():
            while True:
                self.logger.info("Fetching Sub Item entries")
                try:
                    self.monday_service.sync_contacts_from_monday_board()
                except Exception as e:
                    self.logger.error(f"Error fetching Sub Item entries and syncing them to DB: {e}")
                time.sleep(interval)


        threading.Thread(target=sync_contacts_from_monday_board, daemon=True).start()

