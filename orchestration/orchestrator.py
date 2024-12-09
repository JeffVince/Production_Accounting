# /orchestration/orchestrator.py
import os
import threading
import time
import logging
import re

from config import Config
from dropbox_files.dropbox_service import dropbox_service
from ocr_service import OCRService
from monday_files.monday_service import monday_service

from utilities.logger import setup_logging

logger = logging.getLogger(__name__)
setup_logging()


class Orchestrator:
    def __init__(self):
        # Initialize Services
        # Set up logging
        self.logger = logging.getLogger(self.__class__.__name__)
        self.dropbox_service = dropbox_service
        self.config = Config()
        self.monday_service = monday_service
        self.ocr_service = OCRService()

    def start_background_tasks(self):
        """Start any necessary background tasks."""
        logger.info("Starting background tasks...")
        #self.schedule_po_log_check()

        ## TEMP STUFF
        if self.config.USE_TEMP:
             # grab the temp file from the directory
             self.dropbox_service.process_po_log("temp_2416.txt")


        #MAIN STUFF
        # self.schedule_monday_main_items_sync()
        # self.schedule_monday_sub_items_sync()
        # self.schedule_monday_contact_sync()
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

        def sync_contacts_from_monday_board():
            while True:
                time.sleep(interval)
                logger.info("Fetching Sub Item entries")
                try:
                    self.monday_service.sync_contacts_from_monday_board()
                except Exception as e:
                    logger.error(f"Error fetching Sub Item entries and syncing them to DB: {e}")


        threading.Thread(target=sync_contacts_from_monday_board, daemon=True).start()

