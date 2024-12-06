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
             file_path, project_id = self.find_and_process_temp_file()
             self.dropbox_service.process_po_log(file_path, project_id)


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

    def find_and_process_temp_file(self):
        """
        Finds a temp file in the parent directory with a pattern temp_<project_id>.txt,
        extracts the project ID, and processes it with the process_po_log function.
        """
        # Get the current script directory
        current_dir = os.path.dirname(os.path.abspath(__file__))

        # Move one level up
        parent_dir = os.path.dirname(current_dir)

        # List all files in the parent directory
        files = os.listdir(parent_dir)

        # Regex pattern for temp files with a project ID
        pattern = r"temp_(\d{4})\.txt"

        # Search for the matching file
        for file_name in files:
            match = re.match(pattern, file_name)
            if match:
                # Extract the project ID from the filename
                project_id = match.group(1)
                # Full path to the file
                file_path = project_id + ".txt"
                # Call the process_po_log function
                try:
                    return file_path, project_id
                except Exception as e:
                    print(f"Error processing file {file_path}: {e}")
                return

        print("No matching temp file found in the parent directory.")