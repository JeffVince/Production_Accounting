import glob
import os
import threading
import time
import logging
import re
from files_xero.xero_api import xero_api
from utilities.config import Config
from files_dropbox.ocr_service import OCRService
from files_monday.monday_service import monday_service
from xero_services import xero_services

class Orchestrator:

    def __init__(self):
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
        from files_dropbox.dropbox_service import DropboxService
        dropbox_service = DropboxService()
        dropbox_service.scan_project_receipts(project_number)

    def scan_project_invoices(self, project_number: str):
        """
        Calls the dropbox_service to scan a specific project folder for invoices
        and process each invoice into the database.
        """
        self.logger.info(f'[scan_project_invoices] - 📂 Orchestrator: scanning invoice for project {project_number}.')
