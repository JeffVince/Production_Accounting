import glob
import os
import threading
import time
import logging
import re
from files_xero.xero_api import xero_api
from config import Config
from files_dropbox.dropbox_service import dropbox_service
from ocr_service import OCRService
from files_monday.monday_service import monday_service
from xero_services import xero_services

class Orchestrator:

    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.dropbox_service = dropbox_service
        self.config = Config()
        self.monday_service = monday_service
        self.ocr_service = OCRService()
        self.xero_api = xero_api
        self.logger = logging.getLogger('admin_logger')
        self.xero_services = xero_services

    def start_background_tasks(self):
        self.logger.info('[start_background_tasks] - Starting background tasks...')
        current_dir = os.getcwd()
        self.logger.info(f'[start_background_tasks] - Current working directory: {current_dir}')

        if dropbox_service.USE_TEMP_FILE:
            log_dir = './temp_files'
            absolute_log_dir = os.path.abspath(log_dir)

            # Debug: List files in the directory
            if os.path.exists(absolute_log_dir):
                self.logger.info(f'[start_background_tasks] - absolute_log_dir exists: {absolute_log_dir}')
                try:
                    files_in_dir = os.listdir(absolute_log_dir)
                    self.logger.info(f'[start_background_tasks] - Files in {absolute_log_dir}: {files_in_dir}')
                except Exception as e:
                    self.logger.error(f'[start_background_tasks] - Error listing files: {e}')
            else:
                self.logger.error(f'[start_background_tasks] - Directory does not exist: {absolute_log_dir}')

            # Now, try to find the log files by pattern
            log_files = glob.glob(os.path.join(absolute_log_dir, 'PO_LOG_2416-*.txt'))
            self.logger.info(f'[start_background_tasks] - Found log files: {log_files}')

            if log_files:
                latest_log_file = max(log_files, key=os.path.getmtime)
                self.logger.info(f'[start_background_tasks] - Latest log file: {latest_log_file}')
                self.dropbox_service.po_log_orchestrator(latest_log_file)
            else:
                self.logger.error('[start_background_tasks] - No PO LOG FILES FOUND FOR TESTING')



    def sync_monday_main_items(self):
        """
        Fetch Main Item entries from Monday.com and handle them immediately (one-time run).
        """
        self.logger.info('[sync_monday_main_items] - Fetching Main Item entries')
        try:
            self.monday_service.sync_main_items_from_monday_board()
        except Exception as e:
            self.logger.error(f'[sync_monday_main_items] - Error fetching Main Item entries: {e}')

    def sync_monday_sub_items(self):
        """
        Fetch Sub Item entries from Monday.com and handle them immediately (one-time run).
        """
        self.logger.info('[sync_monday_sub_items] - Fetching Sub Item entries')
        try:
            self.monday_service.sync_sub_items_from_monday_board()
        except Exception as e:
            self.logger.error(f'[sync_monday_sub_items] - Error fetching Sub Item entries and syncing them to DB: {e}')

    def sync_monday_contacts(self):
        """
        Fetch contact entries from Monday.com and handle them immediately (one-time run).
        """
        self.logger.info('[sync_monday_contacts] - Fetching Contact entries')
        try:
            self.monday_service.sync_contacts_from_monday_board()
        except Exception as e:
            self.logger.error(f'[sync_monday_contacts] - Error fetching Contact entries and syncing them to DB: {e}')

    def sync_spend_money_items(self):
        """
        Retrieve spend money transactions from Xero for a single run.
        """
        self.logger.info('[sync_spend_money_items] - Syncing spend money transactions...')
        result = self.xero_services.load_spend_money_transactions(project_id='2416')
        return result

    def sync_contacts(self):
        """
        Retrieve and populate Xero contacts in a single run.
        """
        self.logger.info('[sync_contacts] - Syncing Xero contacts...')
        result = self.xero_services.populate_xero_contacts()
        return result

    def sync_xero_bills(self):
        """
        Retrieve Xero bills in a single run.
        """
        self.logger.info('[sync_xero_bills] - Syncing Xero bills...')
        result = self.xero_services.load_bills('2416')
        return result

    def scan_project_receipts(self, project_number: str):
        """
        Calls the dropbox_service to scan a specific project folder for receipts
        and process each receipt into the database.
        """
        self.logger.info(f'[scan_project_receipts] - 📂 Orchestrator: scanning receipts for project {project_number}.')
        from files_dropbox.dropbox_service import dropbox_service
        dropbox_service.scan_project_receipts(project_number)

    def scan_project_invoices(self, project_number: str):
        """
        Calls the dropbox_service to scan a specific project folder for invoices
        and process each invoice into the database.
        """
        self.logger.info(f'[scan_project_invoices] - 📂 Orchestrator: scanning invoice for project {project_number}.')
        from files_dropbox.dropbox_service import dropbox_service
        dropbox_service.scan_project_invoices(project_number)