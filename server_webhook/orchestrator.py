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
        self.xero_services.populate_xero_contacts()

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
        result = self.xero_services.load_xero_bills('2416')
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

    # New function: clear_po_log_data
    def clear_po_log_data(self, project_number):
        """
        Clears all Purchase Order and Detail Item records where the project_number
        matches the provided value.
        """
        self.logger.info(
            f"[clear_po_log_data] Clearing Purchase Orders and Detail Items for project number: {project_number}")
        # Import the required utilities and operations
        from database.db_util import get_db_session
        from database.database_util import DatabaseOperations

        # Convert project_number to int if possible
        try:
            project_number_int = int(project_number)
        except ValueError:
            self.logger.error(f"[clear_po_log_data] Invalid project number: {project_number}")
            raise ValueError("Invalid project number provided.")

        db_ops = DatabaseOperations()
        count_po = 0
        count_di = 0

        with get_db_session() as session:
            # Search for Purchase Order records with the given project number
            po_records = db_ops.search_purchase_orders(["project_number"], [project_number_int], session=session)
            # Search for Detail Item records with the given project number
            di_records = db_ops.search_detail_items(["project_number"], [project_number_int], session=session)
            # Search for Spend Monday Items, Xero Bills, and Xero Bills Items given project number
            sm_records = db_ops.search_spend_money(["project_number"], [project_number_int], session=session)
            xb_records = db_ops.search_xero_bills(["project_number"], [project_number_int], session=session)
            xbli_records = db_ops.search_xero_bill_line_items(["project_number"], [project_number_int], session=session)

            if sm_records:
                if isinstance(sm_records, dict):
                    sm_records = [sm_records]
                for record in sm_records:
                    if db_ops.delete_spend_money(record["id"], session=session):
                        count_po += 1

            if xb_records:
                if isinstance(xb_records, dict):
                    xb_records = [xb_records]
                for record in xb_records:
                    if db_ops.delete_xero_bill(record["id"], session=session):
                        count_po += 1

            if xbli_records:
                if isinstance(xbli_records, dict):
                    xbli_records = [xbli_records]
                for record in xbli_records:
                    if db_ops.delete_xero_bill_line_item(record["id"], session=session):
                        count_di += 1

            if po_records:
                if isinstance(po_records, dict):
                    po_records = [po_records]
                for record in po_records:
                    if db_ops.delete_purchase_order(record["id"], session=session):
                        count_po += 1

            if di_records:
                if isinstance(di_records, dict):
                    di_records = [di_records]
                for record in di_records:
                    if db_ops.delete_detail_item(record["id"], session=session):
                        count_di += 1

            session.commit()

        return {"purchase_orders_deleted": count_po, "detail_items_deleted": count_di}

    # (Other orchestrator functions remain unchanged)


orchestrator = Orchestrator()