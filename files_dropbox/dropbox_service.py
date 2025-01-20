"""
💻 Dropbox Service
=================
Processes files from Dropbox, using the flexible search, create, and update
functions from the new `DatabaseOperations` (database_util.py).

Key Flow for PO Logs:
1. Download/parse PO log.
2. For each PO entry: create/find a Contact, create/find the PurchaseOrder
   (with contact_id), then create/update the DetailItems.
"""
import json
import os
import re
import traceback
import logging

logger = logging.getLogger('dropbox')

from concurrent.futures import ThreadPoolExecutor, wait, ALL_COMPLETED
from datetime import datetime
from typing import Optional
from files_dropbox.dropbox_api import dropbox_api
from utilities.config import Config
from files_dropbox.dropbox_client import dropbox_client
from files_dropbox.dropbox_util import dropbox_util
from files_monday.monday_api import monday_api
from files_monday.monday_util import monday_util
from files_monday.monday_service import monday_service
from files_budget.po_log_database_util import po_log_database_util
from files_budget.po_log_processor import POLogProcessor
from utilities.singleton import SingletonMeta
from files_dropbox.ocr_service import OCRService
from database.database_util import DatabaseOperations

class DropboxService():
    """
    📦 DropboxService
    =================
    Singleton class that coordinates processing of files from Dropbox, with
    a strong focus on PO logs, contacts, and purchase orders.
    """
    PO_LOG_FOLDER_NAME = '1.5 PO Logs'
    PO_NUMBER_FORMAT = '{:02}'
    INVOICE_REGEX = 'invoice'
    TAX_FORM_REGEX = 'w9|w8-ben|w8-ben-e'
    RECEIPT_REGEX = 'receipt'
    SHOWBIZ_REGEX = '.mbb'
    PROJECT_NUMBER = ''

    USE_TEMP_FILE = True
    DEBUG_STARTING_PO_NUMBER = 0
    SKIP_DATABASE = False
    ADD_PO_TO_MONDAY = True
    GET_FOLDER_LINKS = False
    GET_TAX_LINKS = False
    GET_CONTACTS = True
    executor = ThreadPoolExecutor(max_workers=5)

    def __init__(self):
        """
        Initializes the DropboxService singleton, setting up logging, external
        APIs, and the new DatabaseOperations object for DB interactions.
        """
        if not hasattr(self, '_initialized'):
            self.logger = logger
            self.monday_service = monday_service
            self.dropbox_client = dropbox_client

            self.po_log_processor = POLogProcessor()
            self.dropbox_util = dropbox_util
            self.monday_api = monday_api
            self.monday_util = monday_util
            self.config = Config()
            self.dropbox_api = dropbox_api
            self.po_log_database_util = po_log_database_util
            self.database_util = DatabaseOperations()
            self.ocr_service = OCRService()
            self.logger.info('📦 Dropbox event manager initialized. Ready to manage PO logs and file handling!')
            self._initialized = True

    def determine_file_type(self, path: str):
        """
        Determine the file type by matching patterns in its name,
        then route the file to the appropriate process_* handler.

        :param path: The Dropbox file path
        """
        file_component = self.dropbox_util.get_last_path_component_generic(path)
        self.logger.info(f'[determine_file_type] - 🔍 Evaluating dropbox file: {file_component}')
        filename = os.path.basename(path)
        try:
            # Check if PO log
            if self.PO_LOG_FOLDER_NAME in path:
                project_number_match = re.match('^PO_LOG_(\\d{4})[-_]\\d{4}-\\d{2}-\\d{2}_\\d{2}-\\d{2}-\\d{2}\\.txt$', filename)
                if project_number_match:
                    project_number = project_number_match.group(1)
                    self.logger.info(f'[determine_file_type] - 🗂 Identified a PO Log file for project {project_number}. Dispatching to orchestrator...')
                    return self.po_log_orchestrator(path)
                else:
                    self.logger.warning(f"[determine_file_type] - ⚠️ '{filename}' not matching expected PO Log naming convention in dropbox.")
                    return

            # Check if Invoice
            if re.search(self.INVOICE_REGEX, filename, re.IGNORECASE):
                self.logger.info(f'[determine_file_type] - 💰 Recognized invoice pattern for {filename}.')
                return self.process_invoice(path)

            # Check if Tax Form
            if re.search(self.TAX_FORM_REGEX, filename, re.IGNORECASE):
                self.logger.info(f'[determine_file_type] - 💼 Recognized tax form pattern for {filename}.')
                return self.process_tax_form(path)

            # Check if Receipt
            if re.search(self.RECEIPT_REGEX, filename, re.IGNORECASE):
                self.logger.info(f'[determine_file_type] - 🧾 Recognized receipt pattern for {filename}.')
                return self.process_receipt(path)

            # Check if Budget (.mbb)
            if re.search(self.SHOWBIZ_REGEX, filename, re.IGNORECASE):
                self.logger.info(f'[determine_file_type] - 📑 Recognized Showbiz budget file for {filename}.')
                return self.process_budget(path)

            self.logger.debug(f'[determine_file_type] - ❌ No recognized type found for {filename}, ignoring.')
            return None
        except Exception as e:
            self.logger.exception(f'[determine_file_type] - 💥 Error while checking dropbox file {filename}: {e}', exc_info=True)
            return None

    def process_budget(self, dropbox_path: str):
        """
        Handle .mbb (Showbiz) budgets from Dropbox.
        """
        self.logger.info(f'[process_budget] - 💼 Handling Showbiz budget file from dropbox: {dropbox_path}')
        filename = os.path.basename(dropbox_path)

        # Validate .mbb extension
        try:
            if not filename.endswith('.mbb') or filename.endswith('.mbb.lck'):
                self.logger.info('[process_budget] - ❌ Invalid .mbb budget file or lock file encountered. Skipping.')
                return
        except Exception as e:
            self.logger.exception(f'[process_budget] - 💥 Error checking the .mbb extension: {e}', exc_info=True)
            return

        # Parse path segments to extract project folder
        try:
            segments = dropbox_path.strip('/').split('/')
            if len(segments) < 4:
                self.logger.info('[process_budget] - ❌ Folder structure is too short to be a recognized budget path.')
                return
            project_folder = segments[0]
            budget_folder = segments[1]
            phase_folder = segments[2]

            if budget_folder != '5. Budget' or phase_folder not in ['1.2 Working', '1.3 Actuals']:
                self.logger.info('[process_budget] - ❌ Budget file is not located in a recognized "5. Budget" dropbox folder.')
                return

            project_number_match = re.match('^\\d{4}', project_folder)
            if not project_number_match:
                self.logger.info("[process_budget] - ❌ Could not derive project number from budget file path.")
                return
            project_number = project_number_match.group()
            self.logger.info(f'[process_budget] - 🔑 Found project folder reference: {project_number}')
        except Exception as e:
            self.logger.exception(f'[process_budget] - 💥 Error parsing budget folder: {e}', exc_info=True)
            return

        # Potential reference to PO Logs path (not used in code, but stored for context)
        try:
            budget_root = '/'.join(segments[0:3])
            po_logs_path = f'/{budget_root}/1.5 PO Logs'
            self.logger.info(f'[process_budget] - 🗂 Potential PO Logs reference path: {po_logs_path}')
        except Exception as e:
            self.logger.exception(f'[process_budget] - 💥 Could not form PO Logs path: {e}', exc_info=True)
            return

        # Attempt to notify external ShowbizPoLogPrinter service
        import requests
        server_url = 'http://localhost:5004/enqueue'
        self.logger.info('[process_budget] - 🖨 Sending request to external ShowbizPoLogPrinter service...')
        try:
            response = requests.post(server_url, json={'project_number': project_number, 'file_path': dropbox_path}, timeout=10)
            if response.status_code == 200:
                job_id = response.json().get('job_id')
                self.logger.info(f'[process_budget] - 🎉 External service triggered successfully. job_id: {job_id}')
            else:
                self.logger.error(f'[process_budget] - ❌ External printer service returned an error: {response.status_code}, {response.text}')
                return
        except Exception as e:
            self.logger.exception(f'[process_budget] - 💥 Connection error with external ShowbizPoLogPrinter: {e}', exc_info=True)
            return

        self.logger.info('[process_budget] - ✅ Budget file processing complete. External PO log printing triggered!')

    def po_log_orchestrator(self, path: str):
        """
        Process a PO log file from Dropbox, parse it, then store the results in the DB.
        Includes adding Contacts, PurchaseOrders, and DetailItems.
        """
        self.logger.info(f'[po_log_orchestrator] - 📝 Received a PO Log file from dropbox: {path}')
        temp_file_path = f'../temp_files/{os.path.basename(path)}'
        project_number = self.extract_project_number(temp_file_path)
        self.PROJECT_NUMBER = project_number

        # Decide whether to physically download the file
        if not self.USE_TEMP_FILE:
            self.logger.info('[po_log_orchestrator] - 🛠 Not using local temp files? Attempting direct download from dropbox...')
            if not self.download_file_from_dropbox(path, temp_file_path):
                return

        # Extract data from the PO log
        (main_items, detail_items, contacts) = self.extract_data_from_po_log(temp_file_path, project_number)

        # Hand off data to the DB aggregator
        self.logger.info('[po_log_orchestrator] - 🔧 Passing parsed PO log data (main, detail, contacts) to DB aggregator...')
        self.add_po_data_to_db(main_items, detail_items, contacts, project_number)
        self.logger.info('[po_log_orchestrator] - ✅ PO Log orchestration complete!')

    def extract_data_from_po_log(self, temp_file_path: str, project_number: str):
        """
        Parse the local PO log file to extract main_items, detail_items, and contacts.

        :param temp_file_path: The local file path
        :param project_number: The project ID
        :return: (main_items, detail_items, contacts)
        """
        try:
            self.logger.info(f'[extract_data_from_po_log] - 🔎 Parsing PO log for project {project_number} at {temp_file_path}')
            (main_items, detail_items, contacts) = self.po_log_processor.parse_showbiz_po_log(temp_file_path)
            self.logger.info(f'[extract_data_from_po_log] - 📝 Extracted {len(main_items)} main items, {len(detail_items)} detail items, and {len(contacts)} contacts.')
            return (main_items, detail_items, contacts)
        except Exception as e:
            self.logger.exception(f'[extract_data_from_po_log] - 💥 Error while parsing PO Log data: {e}', exc_info=True)
            return ([], [], [])

    def add_po_data_to_db(self, main_items, detail_items, contacts, project_number: str):
        """
        🚀 DB Processing Method
        ----------------------
        *New Batch Logic*:
        1) Convert project_number to an int.
        2) Fetch all existing POs with that project_number in one shot.
        3) For each main_item (PO), decide if it's new or existing (based on po_number).
           Then batch create or update accordingly ONLY if data has changed.
        4) Fetch all existing DetailItems with that same project_number in one shot.
        5) For each detail_item, decide if new or existing (based on (po_number, detail_number, line_number)).
           Then batch create or update accordingly ONLY if data has changed (and unless it's in a final state).
        6) Link contact if needed. We skip manual `project_id` lookups.
        """
        self.logger.info(f'[add_po_data_to_db] - 🚀 Kicking off aggregator for PO log data with project_number={project_number}')
        pn_int = int(project_number)

        # Retrieve known contacts for reference
        all_db_contacts = self.database_util.search_contacts()
        self.logger.info('[add_po_data_to_db] - 🤝 Loaded existing contacts from the DB.')

        # Retrieve existing POs for the project
        existing_pos = self.database_util.search_purchase_orders(column_names=['project_number'], values=[pn_int])
        if existing_pos is None:
            existing_pos = []
        elif isinstance(existing_pos, dict):
            existing_pos = [existing_pos]
        pos_by_number = {po['po_number']: po for po in existing_pos}
        self.logger.info(f'[add_po_data_to_db] - 📄 Found {len(pos_by_number)} existing POs in DB for project {pn_int}.')

        def _po_has_changes(existing_po, new_data, contact_id):
            """
            Compare relevant PO fields to see if there's a difference
            """
            if existing_po.get('description') != new_data.get('description'):
                return True
            if existing_po.get('po_type') != new_data.get('po_type'):
                return True
            if existing_po.get('contact_id') != contact_id:
                return True
            return False

        # Process main_items
        for (i, item) in enumerate(main_items):
            try:
                if self.DEBUG_STARTING_PO_NUMBER and int(item['po_number']) < self.DEBUG_STARTING_PO_NUMBER:
                    continue
                contact_id = self._find_or_create_contact(item, contacts, i, all_db_contacts)
                po_number = int(item['po_number'])
                existing_po = pos_by_number.get(po_number)

                if existing_po:
                    # Check for changes
                    if _po_has_changes(existing_po, item, contact_id):
                        self.logger.info(f'[add_po_data_to_db] - 🔄 Updating existing PO {po_number} with new data from PO log...')
                        updated_po = self.database_util.update_purchase_order_by_keys(
                            project_number=pn_int,
                            po_number=po_number,
                            description=item.get('description'),
                            po_type=item.get('po_type'),
                            contact_id=contact_id
                        )
                        if updated_po:
                            pos_by_number[po_number] = updated_po
                    else:
                        self.logger.debug(f'[add_po_data_to_db] - ⏭ No changes detected for PO {po_number}, skipping update.')
                else:
                    # Create new
                    self.logger.info(f'[add_po_data_to_db] - 🆕 Creating a new PO record from PO log for {po_number}...')
                    new_po = self.database_util.create_purchase_order_by_keys(
                        project_number=pn_int,
                        po_number=po_number,
                        description=item.get('description'),
                        po_type=item.get('po_type'),
                        contact_id=contact_id
                    )
                    if new_po:
                        pos_by_number[po_number] = new_po
            except Exception as ex:
                self.logger.error(f"[add_po_data_to_db] - 💥 Error while processing PO {item.get('po_number')}: {ex}", exc_info=True)

        # Retrieve existing detail items
        existing_details = self.database_util.search_detail_items(['project_number'], [pn_int])
        if existing_details is None:
            existing_details = []
        elif isinstance(existing_details, dict):
            existing_details = [existing_details]
        detail_dict = {}
        for d in existing_details:
            key = (d['po_number'], d['detail_number'], d['line_number'])
            detail_dict[key] = d
        self.logger.info(f'[add_po_data_to_db] - 📋 Found {len(detail_dict)} existing detail items in DB for project {pn_int}.')

        COMPLETED_STATUSES = {'PAID', 'LOGGED', 'RECONCILED', 'REVIEWED'}

        def _detail_item_has_changes(existing_di, new_data):
            """
            Compare relevant DetailItem fields to see differences
            """
            from datetime import datetime

            def to_date(value):
                if not value:
                    return None
                if isinstance(value, datetime):
                    return value.date()
                if isinstance(value, str):
                    try:
                        parsed_dt = datetime.fromisoformat(value)
                        return parsed_dt.date()
                    except ValueError:
                        try:
                            parsed_dt = datetime.strptime(value, '%Y-%m-%d')
                            return parsed_dt.date()
                        except:
                            return value
                return value

            old_date = to_date(existing_di.get('transaction_date'))
            new_date = to_date(new_data.get('date'))
            if old_date != new_date:
                return True

            old_due = to_date(existing_di.get('due_date'))
            new_due = to_date(new_data.get('due date'))
            if old_due != new_due:
                return True

            if existing_di.get('vendor') != new_data.get('vendor'):
                return True
            if existing_di.get('description') != new_data.get('description'):
                return True
            if float(existing_di.get('rate', 0)) != float(new_data.get('rate', 0)):
                return True
            if float(existing_di.get('quantity', 1)) != float(new_data.get('quantity', 1)):
                return True
            if float(existing_di.get('ot', 0)) != float(new_data.get('OT', 0)):
                return True
            if float(existing_di.get('fringes', 0)) != float(new_data.get('fringes', 0)):
                return True
            if (existing_di.get('state') or '').upper() != (new_data.get('state') or '').upper():
                return True
            if existing_di.get('account_code') != new_data.get('account'):
                return True
            if existing_di.get('payment_type') != new_data.get('payment_type'):
                return True
            return False

        # Process detail_items
        for sub_item in detail_items:
            po_number = None
            detail_number = None
            line_number = None
            try:
                if self.DEBUG_STARTING_PO_NUMBER and int(sub_item['po_number']) < self.DEBUG_STARTING_PO_NUMBER:
                    continue
                po_number = int(sub_item['po_number'])
                detail_number = int(sub_item['detail_item_id'])
                line_number = int(sub_item['line_number'])
                key = (po_number, detail_number, line_number)
                existing_di = detail_dict.get(key)

                if existing_di:
                    current_state = (existing_di['state'] or '').upper()
                    if current_state in COMPLETED_STATUSES:
                        self.logger.info(f"[add_po_data_to_db] - ⏭ Detail {key} is in final state ({current_state}); skipping.")
                        continue

                    if _detail_item_has_changes(existing_di, sub_item):
                        self.logger.info(f'[add_po_data_to_db] - 🔄 Updating detail item {key} with new data from PO log...')
                        updated_di = self.database_util.update_detail_item_by_keys(
                            project_number=pn_int,
                            po_number=po_number,
                            detail_number=detail_number,
                            line_number=line_number,
                            vendor=sub_item.get('vendor'),
                            description=sub_item.get('description'),
                            transaction_date=sub_item.get('date'),
                            due_date=sub_item.get('due date'),
                            rate=sub_item.get('rate', 0),
                            quantity=sub_item.get('quantity', 1),
                            ot=sub_item.get('OT', 0),
                            fringes=sub_item.get('fringes', 0),
                            state=sub_item['state'],
                            account_code=sub_item['account'],
                            payment_type=sub_item['payment_type']
                        )
                        if updated_di:
                            detail_dict[key] = updated_di
                    else:
                        self.logger.debug(f'[add_po_data_to_db] - ⏭ No changes detected for detail item {key}. Skipping update.')
                else:
                    self.logger.debug(f'[add_po_data_to_db] - 🆕 Creating new detail item {key} from PO log data...')
                    new_di = self.database_util.create_detail_item_by_keys(
                        project_number=pn_int,
                        po_number=po_number,
                        detail_number=detail_number,
                        line_number=line_number,
                        vendor=sub_item.get('vendor'),
                        description=sub_item.get('description'),
                        transaction_date=sub_item.get('date'),
                        due_date=sub_item.get('due date'),
                        rate=sub_item.get('rate', 0),
                        quantity=sub_item.get('quantity', 1),
                        ot=sub_item.get('OT', 0),
                        fringes=sub_item.get('fringes', 0),
                        state=sub_item['state'],
                        account_code=sub_item['account'],
                        payment_type=sub_item['payment_type']
                    )
                    if new_di:
                        detail_dict[key] = new_di
            except Exception as ex:
                self.logger.error(f'[add_po_data_to_db] - 💥 Error processing detail item {key}: {ex}', exc_info=True)

        self.logger.info('[add_po_data_to_db] - ✅ Finished processing PO log data into DB aggregator.')
        return main_items

    def _find_or_create_contact(self, item, contacts, index, all_db_contacts):
        """
        Helper to match or create a contact from the 'main_items' list
        (or any data structure containing 'contact_name').
        """
        contact_name = item.get('contact_name')
        if not contact_name:
            return None
        self.logger.info(f'[_find_or_create_contact] - 🤝 Looking up or creating contact: {contact_name}')
        contact_search = self.database_util.find_contact_close_match(contact_name, all_db_contacts)
        if contact_search:
            if isinstance(contact_search, list):
                contact_search = contact_search[0]
            contact_id = contact_search['id']
        else:
            self.logger.info(f'[_find_or_create_contact] - 🆕 Creating a new contact record for: {contact_name}')
            new_contact = self.database_util.create_minimal_contact(contact_name)
            contact_id = new_contact['id'] if new_contact else None
        return contact_id

    def callback_add_po_data_to_DB(self, fut):
        return fut.result()

    def process_invoice(self, dropbox_path: str):
        """
        Minimal 'process_invoice' that only inserts or updates an 'invoice' record
        in the DB (plus a share link). Other logic (detail item linking, sum checks,
        RTP vs. MISMATCH, etc.) is handled by the triggers in invoice_receipt_triggers.py.
        """
        self.logger.info(f'[process_invoice] - 📄 Recognized invoice file from dropbox: {dropbox_path}')
        filename = os.path.basename(dropbox_path)
        try:
            match = re.match('^(\\d{4})_(\\d{1,2})(?:_(\\d{1,2}))?', filename)
            if not match:
                self.logger.warning(f"[process_invoice] - ⚠️ Invoice filename '{filename}' is not recognized by the pattern. Skipping.")
                return

            project_number_str = match.group(1)
            po_number_str = match.group(2)
            invoice_number_str = match.group(3) or '1'
            project_number = int(project_number_str)
            po_number = int(po_number_str)
            invoice_number = int(invoice_number_str)
            self.logger.info(f'[process_invoice] - 🧩 Parsed invoice references => project={project_number}, po={po_number}, invoice={invoice_number}')
        except Exception as e:
            self.logger.exception(f"[process_invoice] - 💥 Error parsing invoice filename '{filename}': {e}", exc_info=True)
            return

        # Get dropbox link
        try:
            file_share_link = self.dropbox_util.get_file_link(dropbox_path)
            self.logger.info(f'[process_invoice] - 🔗 Dropbox share link obtained: {file_share_link}')
        except Exception as e:
            self.logger.exception(f'[process_invoice] - 💥 Error getting share link for invoice: {e}', exc_info=True)
            file_share_link = None

        # Download invoice
        temp_file_path = f'./temp_files/{filename}'
        self.logger.info('[process_invoice] - 🚀 Attempting to download the invoice from dropbox...')
        if not self.download_file_from_dropbox(dropbox_path, temp_file_path):
            self.logger.error(f'[process_invoice] - ❌ Could not download invoice from dropbox path: {dropbox_path}')
            return

        (transaction_date, term, total) = (None, 30, 0.0)
        try:
            self.logger.info('[process_invoice] - 🔎 Extracting invoice details using OCR + OpenAI analysis...')
            extracted_text = self.ocr_service.extract_text(temp_file_path)
            (info, err) = self.ocr_service.extract_info_with_openai(extracted_text)
            if err or not info:
                self.logger.warning(f'[process_invoice] - ❌ OCR/AI extraction failed. Using default fallback. Error: {err}')
            else:
                date_str = info.get('invoice_date')
                try:
                    if date_str:
                        transaction_date = datetime.strptime(date_str, '%Y-%m-%d')
                except (ValueError, TypeError):
                    transaction_date = None

                total_str = info.get('total_amount')
                try:
                    total = float(total_str) if total_str else 0.0
                except (ValueError, TypeError):
                    total = 0.0

                term_str = info.get('payment_term')
                if term_str:
                    digits_only = re.sub('[^0-9]', '', term_str)
                    try:
                        t_val = int(digits_only) if digits_only else 30
                        if 7 <= t_val <= 60:
                            term = t_val
                    except Exception:
                        term = 30
        except Exception as e:
            self.logger.exception(f'[process_invoice] - 💥 Error during OCR/AI extraction: {e}', exc_info=True)

        # Create or update invoice in DB
        try:
            self.logger.info(f'[process_invoice] - 🤖 Sending invoice references (#{invoice_number}) to DB aggregator...')
            existing_invoice = self.database_util.search_invoice_by_keys(
                project_number=str(project_number),
                po_number=str(po_number),
                invoice_number=str(invoice_number)
            )
            if existing_invoice is None:
                self.logger.info(f'[process_invoice] - 🆕 Creating a new invoice record: #{invoice_number}')
                new_invoice = self.database_util.create_invoice(
                    project_number=project_number,
                    po_number=po_number,
                    invoice_number=invoice_number,
                    transaction_date=transaction_date,
                    term=term,
                    total=total,
                    file_link=file_share_link
                )
                invoice_id = new_invoice['id'] if new_invoice else None
            else:
                if isinstance(existing_invoice, list):
                    invoice_id = existing_invoice[0]['id']
                else:
                    invoice_id = existing_invoice['id']
                self.logger.info(f'[process_invoice] - 🔄 Updating invoice record: #{invoice_number}')
                self.database_util.update_invoice(
                    invoice_id=invoice_id,
                    transaction_date=transaction_date,
                    term=term,
                    total=total,
                    file_link=file_share_link
                )
        except Exception as e:
            self.logger.exception(f'[process_invoice] - 💥 Error updating invoice #{invoice_number} in DB: {e}', exc_info=True)
            self.cleanup_temp_file(temp_file_path)
            return

        # Cleanup
        self.cleanup_temp_file(temp_file_path)
        self.logger.info(f'[process_invoice] - ✅ Finished invoice processing for dropbox file: {dropbox_path}')

    def process_receipt(self, dropbox_path: str):
        """
        🧾 process_receipt
        -----------------
        1) Parse file name (project_number, po_number, detail_number, vendor_name).
        2) Download the receipt file from Dropbox.
        3) If PDF, try text extraction via PyPDF2. If that fails (or not a PDF), do OCR.
        4) Use OCRService's 'extract_receipt_info_with_openai' to parse total, date, description.
        5) Generate file link in Dropbox.
        6) Create or update the 'receipt' table, linking to the appropriate detail item *via project_number, po_number, detail_number*.
        7) Update the corresponding subitem in Monday with the link.
        8) After creation/update, link `receipt_id` to the relevant detail item.
        """
        self.logger.info(f'[process_receipt] - 🧾 Recognized a receipt file from dropbox: {dropbox_path}')
        temp_file_path = f'./temp_files/{os.path.basename(dropbox_path)}'
        filename = os.path.basename(dropbox_path)
        is_petty_cash = '3. Petty Cash' in dropbox_path or 'Crew PC Folders' in dropbox_path or filename.startswith('PC_')
        pattern = '^(?:PC_)?(\\d{4})_(\\d{2})_(\\d{2})\\s+(.*?)\\s+Receipt\\.(pdf|jpe?g|png)$'
        match = re.match(pattern, filename, re.IGNORECASE)
        if not match:
            self.logger.warning(f"[process_receipt] - ❌ Receipt filename '{filename}' doesn't match recognized pattern. Skipping.")
            return

        project_number_str = match.group(1)
        group2_str = match.group(2).lstrip('0')
        group3_str = match.group(3).lstrip('0')
        vendor_name = match.group(4)
        file_ext = match.group(5).lower()

        if is_petty_cash:
            po_number_str = '1'
            detail_item_str = group2_str
            line_number_str = group3_str
        else:
            po_number_str = group2_str
            detail_item_str = group3_str
            line_number_str = '1'

        project_number = int(project_number_str)
        po_number = int(po_number_str)
        detail_number = int(detail_item_str)
        line_number_number = int(line_number_str)

        self.logger.info('[process_receipt] - 🚀 Attempting to download the receipt file from dropbox...')
        success = self.download_file_from_dropbox(dropbox_path, temp_file_path)
        if not success:
            self.logger.warning(f'[process_receipt] - 🛑 Download failure for receipt: {filename}')
            return

        try:
            with open(temp_file_path, 'rb') as f:
                file_data = f.read()
            extracted_text = ''
            if file_ext == 'pdf':
                self.logger.debug('[process_receipt] - PDF file detected. Attempting direct PDF text extraction...')
                extracted_text = self._extract_text_from_pdf(file_data)
                if not extracted_text.strip():
                    self.logger.info('[process_receipt] - No text from PDF extraction; using OCR fallback...')
                    extracted_text = self._extract_text_from_pdf_with_ocr(file_data)
            else:
                self.logger.debug('[process_receipt] - Image file detected. Using OCR extraction...')
                extracted_text = self._extract_text_via_ocr(file_data)

            parse_failed = False
            if not extracted_text.strip():
                self.logger.warning(f'[process_receipt] - 🛑 Could not extract any text from receipt: {filename}')
                parse_failed = True

            # Attempt AI-based extraction
            ocr_service = OCRService()
            receipt_info = {}
            if not parse_failed:
                self.logger.debug('[process_receipt] - Using OCRService + OpenAI to interpret receipt text...')
                receipt_info = ocr_service.extract_receipt_info_with_openai(extracted_text)
                if not receipt_info:
                    self.logger.warning(f'[process_receipt] - 🛑 AI parse returned empty data for {filename}; marking parse as failed.')
                    parse_failed = True
            else:
                self.logger.warning('[process_receipt] - Skipping AI parse due to empty extraction result.')
                receipt_info = {}

            if parse_failed or not receipt_info:
                receipt_info = {'total_amount': 0.0, 'description': 'Could not parse', 'date': None}

            total_amount = receipt_info.get('total_amount', 0.0)
            purchase_date = receipt_info.get('date', '')
            try:
                datetime.strptime(purchase_date, '%Y-%m-%d')
            except (ValueError, TypeError):
                purchase_date = None

            short_description = receipt_info.get('description', '')

            # Generate dropbox share link
            try:
                shared_link_metadata = self.dropbox_util.get_file_link(dropbox_path)
                file_link = shared_link_metadata.replace('?dl=0', '?dl=1')
            except Exception as e:
                self.logger.warning(f'[process_receipt] - ❌ Unable to create dropbox link for {dropbox_path}: {e}')
                file_link = None

            # Link to DB detail
            self.logger.info('[process_receipt] - 🤖 Searching for corresponding detail item in DB...')
            existing_detail = self.database_util.search_detail_item_by_keys(
                project_number=str(project_number),
                po_number=po_number,
                detail_number=detail_number,
                line_number=line_number_number
            )
            if not existing_detail:
                self.logger.warning(f'[process_receipt] - ❗ No matching detail found (project={project_number}, PO={po_number}).')
                self.cleanup_temp_file(temp_file_path)
                return
            elif isinstance(existing_detail, list):
                existing_detail = existing_detail[0]

            spend_money_id = 1
            existing_receipts = self.database_util.search_receipts(
                ['project_number', 'po_number', 'detail_number', 'line_number'],
                [project_number, po_number, detail_number, line_number_number]
            )
            if not existing_receipts:
                self.logger.info('[process_receipt] - 🆕 Creating a new receipt record in DB...')
                new_receipt = self.database_util.create_receipt(
                    project_number=project_number,
                    po_number=po_number,
                    detail_number=detail_number,
                    line_number=line_number_number,
                    spend_money_id=spend_money_id,
                    total=total_amount,
                    purchase_date=purchase_date,
                    receipt_description=short_description,
                    file_link=file_link
                )
                receipt_id = new_receipt['id'] if new_receipt else None
            else:
                self.logger.info('[process_receipt] - 🔄 Updating existing receipt record in DB...')
                existing_receipt = existing_receipts[0] if isinstance(existing_receipts, list) else existing_receipts
                receipt_id = existing_receipt['id']
                self.database_util.update_receipt_by_keys(
                    project_number=project_number,
                    po_number=po_number,
                    detail_number=detail_number,
                    line_number=line_number_number,
                    total=total_amount,
                    purchase_date=purchase_date,
                    receipt_description=short_description,
                    file_link=file_link
                )

            # Determine detail item state
            state = 'PENDING'
            detail_subtotal = existing_detail.get('sub_total', 0.0)
            if not existing_detail['state'] == 'RECONCILED':
                if parse_failed:
                    state = 'ISSUE'
                    self.logger.info(f'[process_receipt] - Marking detail state as ISSUE due to parse failures for {filename}.')
                else:
                    try:
                        if float(total_amount) == float(detail_subtotal):
                            state = 'REVIEWED'
                            self.logger.info('[process_receipt] - Receipt total matches detail subtotal. Setting state=REVIEWED.')
                        else:
                            self.logger.info('[process_receipt] - Receipt total does not match detail subtotal. Setting state=PO MISMATCH.')
                            state = 'PO MISMATCH'
                    except Exception as e:
                        state = 'ISSUE'
            else:
                state = 'RECONCILED'

            # Update detail item state & receipt link
            self.logger.info('[process_receipt] - 🔧 Updating associated detail item with receipt_id and status...')
            self.database_util.update_detail_item_by_keys(
                project_number=project_number,
                po_number=po_number,
                detail_number=detail_number,
                line_number=line_number_number,
                state=state,
                receipt_id=receipt_id
            )
            self.logger.info(f'[process_receipt] - ✅ Receipt data fully processed for {dropbox_path}')
            self.cleanup_temp_file(temp_file_path)

        except Exception as e:
            self.logger.exception(f'[process_receipt] - 💥 Error processing receipt {filename}: {e}', exc_info=True)
            return

    def process_tax_form(self, dropbox_path: str):
        """
        Stub function for processing a tax form from Dropbox.
        """
        self.logger.info(f'[process_tax_form] - 🗂 Recognized a tax form file in dropbox: {dropbox_path}')
        pass

    def folder_tax_conact_trigger(self, fut):
        """
        Callback for when the DB process is complete.
        Possibly triggers Monday or Dropbox tasks next.
        """
        try:
            processed_items = fut.result()
            if self.GET_FOLDER_LINKS:
                self.logger.info('[folder_tax_conact_trigger] - ⏩ Attempting to update any folder references from dropbox...')

                def get_folder_links(processed_items):
                    for item in processed_items:
                        self.update_po_folder_link(item['project_number'], item['po_number'])

                folder_links_future = self.executor.submit(get_folder_links, processed_items)

            if self.GET_TAX_LINKS:
                self.logger.info('[folder_tax_conact_trigger] - ⏩ Attempting to update any tax form references from dropbox...')

                def get_tax_links(processed_items):
                    for item in processed_items:
                        self.update_po_tax_form_links(item['project_number'], item['po_number'])

                tax_links_future = self.executor.submit(get_tax_links, processed_items)

            futures_to_wait = []
            if self.GET_FOLDER_LINKS and 'folder_links_future' in locals():
                futures_to_wait.append(folder_links_future)
            if self.GET_TAX_LINKS and 'tax_links_future' in locals():
                futures_to_wait.append(tax_links_future)

            if futures_to_wait:
                wait(futures_to_wait, return_when=ALL_COMPLETED)

            # Possibly create POs in Monday
            if processed_items:
                project_number = processed_items[0].get('project_number')
                if project_number:
                    self.logger.info(f'[folder_tax_conact_trigger] - 🏁 Folder/tax updates done. Next: create POs in Monday for project {project_number} if enabled.')
                    if self.ADD_PO_TO_MONDAY:
                        self.create_pos_in_monday(int(project_number))
                    else:
                        self.logger.info('[folder_tax_conact_trigger] - PO creation in Monday is disabled. Done here.')
                else:
                    self.logger.warning('[folder_tax_conact_trigger] - ❌ No project_number found in processed items. Skipping Monday step.')
            else:
                self.logger.warning('[folder_tax_conact_trigger] - Nothing returned from DB aggregator to process. Skipping next step.')
        except Exception as e:
            self.logger.error(f'[folder_tax_conact_trigger] - ❌ Error finalizing dropbox references: {e}', exc_info=True)

    def update_po_folder_link(self, project_number, po_number):
        logger = self.logger
        logger.info(f'[update_po_folder_link] - 🚀 Checking dropbox folder references for PO {project_number}_{str(po_number).zfill(2)}')
        try:
            po_data = self.database_util.search_purchase_order_by_keys(project_number, po_number)
            if not po_data or not len(po_data) > 0:
                logger.warning(f'[update_po_folder_link] - ❌ No PO data found in DB for {project_number}_{str(po_number).zfill(2)}; skipping folder link update.')
                return

            if po_data['folder_link']:
                logger.debug('[update_po_folder_link] - Folder link already assigned. No updates.')
                return

            project_item = dropbox_api.get_project_po_folders_with_link(
                project_number=project_number,
                po_number=po_number
            )
            if not project_item or len(project_item) < 1:
                logger.warning(f'[update_po_folder_link] - ⚠️ No dropbox folder found for {project_number}_{str(po_number).zfill(2)}.')
                return

            project_item = project_item[0]
            po_folder_link = project_item['po_folder_link']
            po_folder_name = project_item['po_folder_name']
            logger.debug(f"[update_po_folder_link] - Found potential folder '{po_folder_name}' in dropbox.")
            if po_folder_link:
                logger.info(f'[update_po_folder_link] - ✅ Linking dropbox folder to PO {project_number}_{str(po_number).zfill(2)}...')
                self.database_util.update_purchase_order(po_id=po_data['id'], folder_link=po_folder_link)
            else:
                logger.warning('[update_po_folder_link] - ⚠️ No folder_link found in dropbox data.')
            logger.info(f'[update_po_folder_link] - 🎉 Folder reference update complete for PO {project_number}_{str(po_number).zfill(2)}')
        except Exception as e:
            logger.error('[update_po_folder_link] - 💥 Error while updating folder link:', exc_info=True)
            traceback.print_exc()

    def update_po_tax_form_links(self, project_number, po_number):
        """
        🚀 Update or set the tax_form_link for a PurchaseOrder in Dropbox if needed.
        """
        try:
            po_search = self.database_util.search_purchase_order_by_keys(project_number, po_number)
            if not po_search or not po_search['po_type'] == 'INV':
                return None
            if isinstance(po_search, dict):
                contact_id = po_search['contact_id']
            elif isinstance(po_search, list) and po_search:
                contact_id = po_search[0]['contact_id']
            else:
                return None

            new_tax_form_link = self.dropbox_api.get_po_tax_form_link(project_number=project_number, po_number=po_number)[0]['po_tax_form_link']
            self.database_util.update_contact(contact_id, tax_form_link=new_tax_form_link)
            self.logger.info(f'[update_po_tax_form_links] - 📑 Applied new tax form link for PO {project_number}_{po_number}')
            return new_tax_form_link
        except Exception as e:
            self.logger.error(f'[update_po_tax_form_links] - 💥 Could not update tax form link for PO {project_number}_{po_number}: {e}', exc_info=True)

    def create_pos_in_monday(self, project_number):
        """
        Demonstrates how to fetch all subitems once from Monday,
        then process them locally to avoid multiple queries.
        """
        self.logger.info('[create_pos_in_monday] - 🌐 Creating/Updating PO records in Monday.com based on dropbox aggregator data...')
        monday_items = self.monday_api.get_items_in_project(project_id=project_number)
        processed_items = self.database_util.search_purchase_order_by_keys(project_number=project_number)
        monday_items_map = {}

        # Map existing Monday items
        for mi in monday_items:
            pid = mi['column_values'].get(self.monday_util.PO_PROJECT_ID_COLUMN)['text']
            pono = mi['column_values'].get(self.monday_util.PO_NUMBER_COLUMN)['text']
            if pid and pono:
                monday_items_map[int(pid), int(pono)] = mi

        # Gather all subitems at once
        all_subitems = self.monday_api.get_subitems_in_board(project_number=project_number)
        global_subitem_map = {}
        for msub in all_subitems:
            identifiers = self.monday_util.extract_subitem_identifiers(msub)
            if identifiers is not None:
                global_subitem_map[identifiers] = msub

        items_to_create = []
        items_to_update = []

        # Compare DB items vs. Monday items
        for db_item in processed_items:
            contact_item = self.database_util.search_contacts(['id'], [db_item['contact_id']])
            db_item['contact_pulse_id'] = contact_item['pulse_id']
            db_item['contact_name'] = contact_item['name']
            db_item['project_number'] = project_number

            p_id = project_number
            po_no = int(db_item['po_number'])
            column_values_str = self.monday_util.po_column_values_formatter(
                project_id=str(project_number),
                po_number=db_item['po_number'],
                description=db_item.get('description'),
                contact_pulse_id=db_item['contact_pulse_id'],
                folder_link=db_item.get('folder_link'),
                producer_id=None,
                name=db_item['contact_name']
            )
            new_vals = json.loads(column_values_str)
            key = (p_id, po_no)

            # If Monday item exists, check for differences
            if key in monday_items_map:
                monday_item = monday_items_map[key]
                differences = self.monday_util.is_main_item_different(db_item, monday_item)
                if differences:
                    self.logger.debug(f'[create_pos_in_monday] - Main item differs for PO {po_no}, scheduling update...')
                    items_to_update.append({
                        'db_item': db_item,
                        'column_values': new_vals,
                        'monday_item_id': monday_item['id']
                    })
                else:
                    self.logger.debug(f'[create_pos_in_monday] - No changes for PO {po_no}, skipping.')
            else:
                items_to_create.append({'db_item': db_item, 'column_values': new_vals, 'monday_item_id': None})

        # Create needed items
        if items_to_create:
            self.logger.info(f'[create_pos_in_monday] - 🆕 Creating {len(items_to_create)} new main items in Monday...')
            created_mapping = self.monday_api.batch_create_or_update_items(
                items_to_create,
                project_id=project_number,
                create=True
            )
            for itm in created_mapping:
                db_item = itm['db_item']
                monday_item_id = itm['monday_item_id']
                self.database_util.update_purchase_order(db_item['id'], pulse_id=monday_item_id)
                db_item['pulse_id'] = monday_item_id
                p = project_number
                po = int(db_item['po_number'])
                monday_items_map[p, po] = {
                    'id': monday_item_id,
                    'name': f'PO #{po}',
                    'column_values': itm['column_values']
                }

        # Update existing items
        if items_to_update:
            self.logger.info(f'[create_pos_in_monday] - ✏️ Updating {len(items_to_update)} main items in Monday...')
            updated_mapping = self.monday_api.batch_create_or_update_items(
                items_to_update,
                project_id=project_number,
                create=False
            )
            for itm in updated_mapping:
                db_item = itm['db_item']
                monday_item_id = itm['monday_item_id']
                self.database_util.update_purchase_order_by_keys(project_number, db_item['po_number'], pulse_id=monday_item_id)
                db_item['pulse_id'] = monday_item_id
                p = project_number
                po = int(db_item['po_number'])
                monday_items_map[p, po]['column_values'] = itm['column_values']

        # Ensure newly-created items get updated in DB if no pulse_id was assigned
        for db_item in processed_items:
            p_id = project_number
            po_no = int(db_item['po_number'])
            main_monday_item = monday_items_map.get((p_id, po_no))
            if main_monday_item and not db_item.get('pulse_id'):
                monday_item_id = main_monday_item['id']
                updated = self.database_util.update_purchase_order_by_keys(project_number, db_item['po_number'], pulse_id=monday_item_id)
                if updated:
                    db_item['pulse_id'] = monday_item_id
                    self.logger.info(f'[create_pos_in_monday] - Linked newly created pulse_id {monday_item_id} to PO {po_no}.')

        # Subitems
        for db_item in processed_items:
            p_id = project_number
            po_no = int(db_item['po_number'])
            main_monday_item = monday_items_map.get((p_id, po_no))
            if not main_monday_item:
                self.logger.warning(f'[create_pos_in_monday] - ❌ No main Monday reference for PO {po_no}, skipping subitems.')
                continue

            main_monday_id = main_monday_item['id']
            sub_items_db = self.database_util.search_detail_item_by_keys(project_number, db_item['po_number'])
            if isinstance(sub_items_db, dict):
                sub_items_db = [sub_items_db]

            subitems_to_create = []
            subitems_to_update = []

            if not sub_items_db:
                pass

            for sdb in sub_items_db:
                if sdb.get('account_code_id'):
                    account_row = self.database_util.search_account_codes(['id'], [sdb['account_code_id']])
                    sdbcode = account_row['code'] if account_row else None
                else:
                    sdb['account_code'] = None

                file_link_for_subitem = ''
                if db_item['po_type'] in ['PC', 'CC']:
                    receipt_id = sdb.get('receipt_id')
                    if receipt_id:
                        existing_receipt = self.database_util.search_receipts(['id'], [receipt_id])
                        if existing_receipt and existing_receipt.get('file_link'):
                            file_link_for_subitem = existing_receipt['file_link']
                    else:
                        existing_receipts = self.database_util.search_receipts(
                            ['project_number', 'po_number', 'detail_number', 'line_number'],
                            [project_number, int(po_no), sdb['detail_number'], sdb['line_number']]
                        )
                        if existing_receipts:
                            first_receipt = existing_receipts[0] if isinstance(existing_receipts, list) else existing_receipts
                            file_link_for_subitem = first_receipt.get('file_link', '')
                            rid = first_receipt.get('id')
                            self.database_util.update_detail_item_by_keys(
                                project_number=p_id,
                                po_number=db_item['po_number'],
                                detail_number=sdb['detail_number'],
                                line_number=sdb['line_number'],
                                receipt_id=rid
                            )
                            sdb['receipt_id'] = rid
                elif db_item['po_type'] in ['INV', 'PROJ']:
                    invoice_id = sdb.get('invoice_id')
                    if invoice_id:
                        existing_invoice = self.database_util.search_invoices(['id'], [invoice_id])
                        if existing_invoice and existing_invoice.get('file_link'):
                            file_link_for_subitem = existing_invoice['file_link']
                    else:
                        existing_invoices = self.database_util.search_invoices(
                            ['project_number', 'po_number', 'invoice_number'],
                            [project_number, int(po_no), sdb['detail_number']]
                        )
                        if existing_invoices:
                            first_invoice = existing_invoices[0] if isinstance(existing_invoices, list) else existing_invoices
                            file_link_for_subitem = first_invoice.get('file_link', '')
                            inv_id = first_invoice.get('id')
                            self.database_util.update_detail_item_by_keys(
                                project_number=p_id,
                                po_number=str(po_no),
                                detail_number=sdb['detail_number'],
                                line_number=sdb['line_number'],
                                invoice_id=inv_id
                            )
                            sdb['invoice_id'] = inv_id

                sub_col_values_str = self.monday_util.subitem_column_values_formatter(
                    project_id=project_number,
                    po_number=db_item['po_number'],
                    detail_number=sdb['detail_number'],
                    line_number=sdb['line_number'],
                    status=sdb.get('state'),
                    description=sdb.get('description'),
                    quantity=sdb.get('quantity'),
                    rate=sdb.get('rate'),
                    date=sdb.get('transaction_date'),
                    due_date=sdb.get('due_date'),
                    account_number=sdb['account_code'],
                    link=file_link_for_subitem,
                    OT=sdb.get('ot'),
                    fringes=sdb.get('fringes')
                )
                new_sub_vals = json.loads(sub_col_values_str)
                sub_key = (project_number, db_item['po_number'], sdb['detail_number'], sdb['line_number'])

                if sub_key in global_subitem_map:
                    msub = global_subitem_map[sub_key]
                    sdb['project_number'] = db_item['project_number']
                    sdb['po_number'] = db_item['po_number']
                    sdb['file_link'] = file_link_for_subitem
                    differences = self.monday_util.is_sub_item_different(sdb, msub)

                    if differences:
                        self.logger.debug(f'[create_pos_in_monday] - Sub-item differs for detail #{sdb["detail_number"]}, scheduling update...')
                        subitems_to_update.append({
                            'db_sub_item': sdb,
                            'column_values': new_sub_vals,
                            'parent_id': main_monday_id,
                            'monday_item_id': msub['id']
                        })
                    else:
                        self.logger.debug(f'[create_pos_in_monday] - No changes for sub-item detail #{sdb["detail_number"]}.')
                        sub_pulse_id = msub['id']
                        current_pulse_id = sdb.get('pulse_id')
                        current_parent_id = sdb.get('parent_pulse_id')

                        if current_pulse_id != int(sub_pulse_id) or current_parent_id != int(main_monday_id):
                            self.logger.debug('[create_pos_in_monday] - Subitem pulse mismatch, updating DB references...')
                            self.database_util.update_detail_item_by_keys(
                                project_number=project_number,
                                po_number=db_item['po_number'],
                                detail_number=sdb['detail_number'],
                                line_number=sdb['line_number'],
                                pulse_id=sub_pulse_id,
                                parent_pulse_id=main_monday_id
                            )
                            sdb['pulse_id'] = sub_pulse_id
                            sdb['parent_pulse_id'] = main_monday_id
                else:
                    subitems_to_create.append({
                        'db_sub_item': sdb,
                        'column_values': new_sub_vals,
                        'parent_id': main_monday_id
                    })

            # Create and update subitems in batches
            if subitems_to_create:
                self.logger.info(f'[create_pos_in_monday] - 🆕 Creating {len(subitems_to_create)} new sub-items for PO {po_no}...')
                self._batch_create_subitems(subitems_to_create, main_monday_id, project_number, db_item)

            if subitems_to_update:
                self.logger.info(f'[create_pos_in_monday] - ✏️ Updating {len(subitems_to_update)} existing sub-items for PO {po_no}...')
                self._batch_update_subitems(subitems_to_update, main_monday_id, project_number, db_item)

        self.logger.info('[create_pos_in_monday] - ✅ Completed Monday.com integration for all processed PO data.')

    def _batch_create_subitems(self, subitems_to_create, parent_item_id, project_number, db_item):
        """
        Creates subitems in chunks, then updates DB with the new subitem IDs.
        """
        from concurrent.futures import ThreadPoolExecutor, as_completed
        chunk_size = 10
        create_chunks = [subitems_to_create[i:i + chunk_size] for i in range(0, len(subitems_to_create), chunk_size)]
        all_created_subs = []

        with ThreadPoolExecutor() as executor:
            future_to_index = {}
            for (idx, chunk) in enumerate(create_chunks):
                future = executor.submit(
                    self.monday_api.batch_create_or_update_subitems,
                    chunk,
                    parent_item_id=parent_item_id,
                    create=True
                )
                future_to_index[future] = idx

            for future in as_completed(future_to_index):
                idx = future_to_index[future]
                try:
                    chunk_result = future.result()
                    self.logger.debug(f'[_batch_create_subitems] - Subitem create-chunk #{idx + 1} done.')
                    all_created_subs.extend(chunk_result)
                except Exception as e:
                    self.logger.exception(f'[_batch_create_subitems] - ❌ Error creating subitems in chunk {idx + 1}: {e}')
                    raise

        for csub in all_created_subs:
            db_sub_item = csub['db_sub_item']
            monday_subitem_id = csub['monday_item_id']
            self.database_util.update_detail_item_by_keys(
                project_number,
                db_item['po_number'],
                db_sub_item['detail_number'],
                db_sub_item['line_number'],
                pulse_id=monday_subitem_id,
                parent_pulse_id=parent_item_id
            )
            db_sub_item['pulse_id'] = monday_subitem_id
            db_sub_item['parent_pulse_id'] = parent_item_id

    def _batch_update_subitems(self, subitems_to_update, parent_item_id, project_number, db_item):
        """
        Updates subitems in chunks, then updates DB with any new data (e.g., if we changed the link).
        """
        from concurrent.futures import ThreadPoolExecutor, as_completed
        chunk_size = 10
        update_chunks = [subitems_to_update[i:i + chunk_size] for i in range(0, len(subitems_to_update), chunk_size)]
        all_updated_subs = []

        with ThreadPoolExecutor() as executor:
            future_to_index = {}
            for (idx, chunk) in enumerate(update_chunks):
                future = executor.submit(
                    self.monday_api.batch_create_or_update_subitems,
                    chunk,
                    parent_item_id=parent_item_id,
                    create=False
                )
                future_to_index[future] = idx

            for future in as_completed(future_to_index):
                idx = future_to_index[future]
                try:
                    chunk_result = future.result()
                    self.logger.debug(f'[_batch_update_subitems] - Subitem update-chunk #{idx + 1} done.')
                    all_updated_subs.extend(chunk_result)
                except Exception as e:
                    self.logger.exception(f'[_batch_update_subitems] - ❌ Error updating subitems in chunk {idx + 1}: {e}')
                    raise

        for usub in all_updated_subs:
            db_sub_item = usub['db_sub_item']
            monday_subitem_id = usub['monday_item_id']
            self.database_util.update_detail_item_by_keys(
                project_number,
                db_item['po_number'],
                db_sub_item['detail_number'],
                db_sub_item['line_number'],
                pulse_id=monday_subitem_id,
                parent_pulse_id=parent_item_id
            )
            db_sub_item['pulse_id'] = monday_subitem_id
            db_sub_item['parent_pulse_id'] = parent_item_id

    def download_file_from_dropbox(self, path: str, temp_file_path: str) -> bool:
        """
        Download a file from Dropbox to a local temp_file_path.
        """
        try:
            self.logger.info(f'[Download File From Dropbox] - 🚀 Initiating download for path: {path}')
            dbx = self.dropbox_client.dbx
            self.logger.debug(f'[Download File From Dropbox] - Calling dbx.files_download for {path}')
            (_, res) = dbx.files_download(path)
            file_content = res.content
            self.logger.debug('[Download File From Dropbox] - Dropbox response received. Writing to local file...')

            with open(temp_file_path, 'wb') as temp_file:
                temp_file.write(file_content)

            self.logger.info(f'[Download File From Dropbox] - 📂 Successfully saved to {temp_file_path}')
            self.logger.info('[Download File From Dropbox] - ✅ Download completed with no errors!')
            return True
        except Exception as e:
            self.logger.exception(f'[Download File From Dropbox] - 💥 Encountered error while downloading {path}: {e}', exc_info=True)
            return False

    def _parse_tax_number(self, tax_str: str) -> Optional[int]:
        """
        Remove any hyphens from tax_str (e.g. SSN '123-45-6789' or EIN '12-3456789')
        and convert to integer. If parsing fails, return None.
        """
        if not tax_str:
            return None
        cleaned = tax_str.replace('-', '')
        try:
            return int(cleaned)
        except ValueError:
            self.logger.warning(f"[_parse_tax_number] - ⚠️ Could not parse '{tax_str}' into an integer after removing hyphens.")
            return None

    def cleanup_temp_file(self, temp_file_path: str):
        """
        Attempt to remove a temporary file.
        """
        try:
            if os.path.exists(temp_file_path):
                os.remove(temp_file_path)
                self.logger.info(f'[cleanup_temp_file] - 🧹 Temp file removed from local system: {temp_file_path}')
        except Exception as e:
            self.logger.warning(f'[cleanup_temp_file] - ⚠️ Could not remove temp file {temp_file_path}: {e}')

    def extract_project_number(self, file_name: str) -> str:
        """
        Extract the first 4-digit sequence from a file name
        to interpret as the project number.
        """
        digit_sequences = re.findall('\\d+', file_name)
        if not digit_sequences:
            raise ValueError(f"❗ No digits found in file name: '{file_name}' ❗")
        all_digits = ''.join(digit_sequences)
        if len(all_digits) < 4:
            raise ValueError(f"❗ File name '{file_name}' does not contain at least four digits for project_id. ❗")
        project_number = all_digits[:4]
        return project_number

    def _extract_text_from_pdf(self, file_data: bytes) -> str:
        """
        Attempt direct text extraction with PyPDF2, then fallback to PyMuPDF-based
        image extraction if minimal text is found.
        """
        import PyPDF2
        import fitz
        from io import BytesIO
        from PIL import Image
        Image.MAX_IMAGE_PIXELS = 200000000

        self.logger.debug('[process_receipt -> _extract_text_from_pdf] - Trying PyPDF2 direct extraction...')
        try:
            pdf_reader = PyPDF2.PdfReader(BytesIO(file_data))
            text_chunks = []
            for (idx, page) in enumerate(pdf_reader.pages, start=1):
                page_text = page.extract_text() or ''
                if page_text:
                    self.logger.debug(f'[process_receipt -> _extract_text_from_pdf] - Extracted text from page {idx}.')
                    text_chunks.append(page_text)
            extracted_text = '\n'.join(text_chunks)
            if len(extracted_text.strip()) < 20:
                self.logger.info('[process_receipt -> _extract_text_from_pdf] - PyPDF2 found minimal text; switching to fallback extraction.')
                extracted_text = ''

            # If we got enough text, return it
            if extracted_text.strip():
                return extracted_text

            # Otherwise, fallback to images
            pdf_document = fitz.open(stream=file_data, filetype='pdf')
            embedded_ocr_results = []
            for page_idx in range(pdf_document.page_count):
                page = pdf_document[page_idx]
                images = page.get_images(full=True)
                for (img_ix, img_info) in enumerate(images, start=1):
                    xref = img_info[0]
                    base_image = pdf_document.extract_image(xref)
                    image_data = base_image['image']
                    try:
                        Image.open(BytesIO(image_data)).convert('RGB')
                        text_in_image = self._extract_text_via_ocr(image_data)
                        embedded_ocr_results.append(text_in_image)
                    except Exception as e:
                        self.logger.warning(f'[process_receipt -> _extract_text_from_pdf] - Could not OCR embedded PDF image: {e}')
            fallback_text = '\n'.join(embedded_ocr_results)
            return fallback_text

        except Exception as e:
            self.logger.warning(f'[process_receipt -> _extract_text_from_pdf] - Could not parse PDF with PyPDF2 or fallback: {e}')
            return ''

    def _extract_text_via_ocr(self, file_data: bytes) -> str:
        """
        Use the OCRService to extract text from the provided file data.
        """
        try:
            return self.ocr_service.extract_text_from_receipt(file_data)
        except Exception as e:
            self.logger.warning(f'[process_receipt -> _extract_text_via_ocr] - OCR extraction issue: {e}')
            return ''

    def scan_project_receipts(self, project_number: str):
        """
        Scans both credit-card/vendor receipt folders (under 1. Purchase Orders)
        and petty-cash receipt folders (under 3. Petty Cash/1. Crew PC Folders)
        for the specified project_number, then processes matching receipts.
        """
        self.logger.info(f'[scan_project_receipts] - 🔎 Initiating dropbox scan for receipts in project {project_number}...')
        project_folder_path = self.dropbox_api.find_project_folder(project_number, namespace='2024')
        if not project_folder_path:
            self.logger.warning(f"[scan_project_receipts] - ❌ No matching project folder in dropbox for '{project_number}' under 2024.")
            return

        self.logger.info(f'[scan_project_receipts] - 📂 Resolved project folder path: {project_folder_path}')
        purchase_orders_path = f'{project_folder_path}/1. Purchase Orders'
        petty_cash_path = f'{project_folder_path}/3. Petty Cash/1. Crew PC Folders'

        # Perform recursive scanning
        self._scan_and_process_receipts_in_folder(purchase_orders_path, project_number)
        self._scan_and_process_receipts_in_folder(petty_cash_path, project_number)
        self.logger.info(f'[scan_project_receipts] - ✅ Finished scanning dropbox receipts for project {project_number}.')

    def _scan_and_process_receipts_in_folder(self, folder_path: str, project_number: str):
        """
        Recursively scans the given folder_path and its subfolders, and whenever
        it finds a file that looks like a 'receipt' (based on your naming pattern),
        calls process_receipt(...).
        """
        entries = self._list_folder_recursive(folder_path)
        if not entries:
            self.logger.debug(f"[_scan_and_process_receipts_in_folder] - No entries found in dropbox folder: '{folder_path}'")
            return

        # Identify potential receipts
        for entry in entries:
            if entry['is_folder']:
                continue
            dropbox_path = entry['path_display']
            file_name = entry['name']
            if re.search(self.RECEIPT_REGEX, file_name, re.IGNORECASE):
                self.logger.debug(f'[_scan_and_process_receipts_in_folder] - 🧾 Found a potential receipt file in dropbox: {dropbox_path}')
                self.process_receipt(dropbox_path)

    def _list_folder_recursive(self, folder_path: str):
        """
        Recursively lists all entries (files and subfolders) under folder_path.
        Return a list of entries, each entry is a dict containing
        {
          "name": str,
          "path_lower": str,
          "path_display": str,
          "is_folder": bool
        }
        """
        from dropbox import files
        results = []
        try:
            dbx = self.dropbox_client.dbx
            self.logger.info(f'[_list_folder_recursive] - 📁 Recursively listing dropbox folder: {folder_path}')
            res = dbx.files_list_folder(folder_path, recursive=True)
            entries = res.entries
            while res.has_more:
                res = dbx.files_list_folder_continue(res.cursor)
                entries.extend(res.entries)

            for e in entries:
                if isinstance(e, files.FolderMetadata):
                    results.append({
                        'name': e.name,
                        'path_lower': e.path_lower,
                        'path_display': e.path_display,
                        'is_folder': True
                    })
                elif isinstance(e, files.FileMetadata):
                    results.append({
                        'name': e.name,
                        'path_lower': e.path_lower,
                        'path_display': e.path_display,
                        'is_folder': False
                    })
        except Exception as ex:
            self.logger.warning(f'[_list_folder_recursive] - ⚠️ Could not list folder recursively in dropbox: {folder_path} => {ex}')
        return results

    def _scan_po_folder_for_invoices(self, folder_path: str, project_number: str, folder_po_number: str):
        """
        Given a subfolder that definitely references a valid PO with type='INV',
        scan the folder's files for invoice docs. Then call process_invoice(...)
        for each matching file.
        """
        entries = self._list_folder_recursive(folder_path)
        if not entries:
            return
        for entry in entries:
            if entry['is_folder']:
                continue
            dropbox_path = entry['path_display']
            file_name = entry['name']
            if re.search(self.INVOICE_REGEX, file_name, re.IGNORECASE):
                self.logger.debug(f'[_scan_po_folder_for_invoices] - Found potential invoice file in dropbox: {dropbox_path}')
                self.process_invoice(dropbox_path)
