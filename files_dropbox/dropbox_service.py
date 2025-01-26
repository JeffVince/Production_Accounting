"""
files_dropbox/dropbox_service.py

💻 Dropbox Service
=================
Processes files from Dropbox, using the flexible search, create, and update
functions from the new `DatabaseOperations` (database_util.py).

Key Flow for PO Logs:
1. Download/parse PO log.
2. For each PO entry: create/find a Contact, create/find the PurchaseOrder
   (with contact_id), then create/update the DetailItems.

Also handles logic for invoice/receipt processing, aggregator references, etc.
"""

# region Imports
import json
import os
import re
import logging
import traceback
from datetime import datetime
from typing import Optional
from concurrent.futures import ThreadPoolExecutor, wait, ALL_COMPLETED

# Dropbox Logging
logger = logging.getLogger('dropbox')

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
# endregion

# region Class Definition
class DropboxService(metaclass=SingletonMeta):
    """
    📦 DropboxService
    =================
    Singleton class that coordinates processing of files from Dropbox, with
    a strong focus on PO logs, contacts, and purchase orders, plus receipts & invoices.
    """

    # region Constants
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
    # endregion

    # region Initialization
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
    # endregion

    # region File-Type Router
    def determine_file_type(self, path: str):
        """
        Determine the file type by matching patterns in its name,
        then route the file to the appropriate process_* handler.
        """
        file_component = self.dropbox_util.get_last_path_component_generic(path)
        self.logger.info(f'[determine_file_type] - 🔍 Evaluating dropbox file: {file_component}')
        filename = os.path.basename(path)

        try:
            # Check if PO log
            if self.PO_LOG_FOLDER_NAME in path:
                project_number_match = re.match(
                    r'^PO_LOG_(\d{4})[-_]\d{4}-\d{2}-\d{2}_\d{2}-\d{2}-\d{2}\.txt$', 
                    filename
                )
                if project_number_match:
                    project_number = project_number_match.group(1)
                    self.logger.info(
                        f'[determine_file_type] - 🗂 Identified a PO Log file for project {project_number}. Dispatching...'
                    )
                    return self.po_log_orchestrator(path)
                else:
                    self.logger.warning(
                        f"[determine_file_type] - ⚠️ '{filename}' not matching expected PO Log naming convention."
                    )
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
            self.logger.exception(
                f'[determine_file_type] - 💥 Error while checking dropbox file {filename}: {e}',
                exc_info=True
            )
            return None
    # endregion

    # region PO Log Flow
    def po_log_orchestrator(self, path: str = None):
        """
        Process a PO log file from Dropbox, parse it, then store the results in the DB.
        Includes adding Contacts, PurchaseOrders, and DetailItems.
        """
        if path:
            self.logger.info(f'📝 Received a PO Log file from Dropbox: {path}')
        else:
            self.logger.warning(f'📝 TEST PO LOG - GRABBING LAST FILE FROM TEMP FOLDER')

        temp_file_path = f'./temp_files/{os.path.basename(path)}'
        project_number = self.extract_project_number(temp_file_path)
        self.PROJECT_NUMBER = project_number

        self.database_util.create_po_log(
            project_number=project_number,
            db_path=path,
            status='STARTED'
        )

        if path:
            self.logger.info('🛠 Attempting direct download from Dropbox...')
            if not self.download_file_from_dropbox(path, temp_file_path):
                return

        main_items, detail_items, contacts = self.extract_data_from_po_log(temp_file_path, project_number)
        self.logger.info('[po_log_orchestrator] - 🔧 Passing parsed PO log data to DB aggregator...')
        self.add_po_data_to_db(main_items, detail_items, contacts, project_number)
        self.logger.info('[po_log_orchestrator] - ✅ PO Log orchestration complete!')

    def extract_data_from_po_log(self, temp_file_path: str, project_number: str):
        """
        Parse the local PO log file to extract main_items, detail_items, and contacts.
        """
        self.logger.info(
            f'[extract_data_from_po_log] - 🔎 Parsing PO log for project {project_number} at {temp_file_path}'
        )
        try:
            (main_items, detail_items, contacts) = self.po_log_processor.parse_showbiz_po_log(temp_file_path)
            self.logger.info(
                f'[extract_data_from_po_log] - 📝 Extracted {len(main_items)} main items, '
                f'{len(detail_items)} detail items, and {len(contacts)} contacts.'
            )
            return (main_items, detail_items, contacts)
        except Exception:
            self.logger.exception('[extract_data_from_po_log] - 💥 Error while parsing PO Log data.', exc_info=True)
            return ([], [], [])

    def add_po_data_to_db(self, main_items, detail_items, contacts, project_number: str):
        """
        🚀 DB Processing Method
        ----------------------
        *New Batch Logic*:
        1) Convert project_number to int.
        2) Fetch existing POs for that project_number in one shot.
        3) For each main_item, decide if it's new or existing in DB; update or create.
        4) Fetch all existing DetailItems for that project_number.
        5) For each detail_item, decide if new or existing; link to DB.
        6) Link contact if needed (fuzzy or direct create).
        """
        self.logger.info(
            f' 🚀 Kicking off aggregator for PO log data, project_number={project_number}'
        )
        pn_int = int(project_number)

        # Preload all DB Contacts (for fuzzy match)
        all_db_contacts = self.database_util.search_contacts() or []
        self.logger.info(' 🤝 Loaded existing contacts from the DB.')

        # Fetch existing PurchaseOrders for this project_number
        existing_pos = self.database_util.search_purchase_orders(column_names=['project_number'], values=[pn_int])
        if existing_pos is None:
            existing_pos = []
        elif isinstance(existing_pos, dict):
            existing_pos = [existing_pos]

        pos_by_number = {po['po_number']: po for po in existing_pos}
        self.logger.info(
            f' 📄 Found {len(pos_by_number)} existing POs in DB for project {pn_int}.'
        )

        # --- Process main_items => PurchaseOrders
        for main_item in main_items:
            po_number = int(main_item.get('po_number', 0) or 0)
            vendor_name = main_item.get('vendor_name', '')
            description = main_item.get('description', '')
            po_type = main_item.get('po_type', 'INV')

            self.logger.info(f" 📝 Checking PO {po_number} for existence in DB...")
            existing_po = pos_by_number.get(po_number)

            if existing_po:
                changed = False
                if (existing_po.get('description') or '') != description:
                    changed = True
                if (existing_po.get('po_type') or '') != po_type:
                    changed = True
                if (existing_po.get('vendor_name') or '') != vendor_name:
                    changed = True
                    contact_id = self._find_or_create_contact_in_db(vendor_name, all_db_contacts)
                else:
                    contact_id = existing_po.get('contact_id')

                if changed:
                    self.logger.info(f"🔄 Updating existing PO => {po_number} with new data from aggregator.")
                    updated_po = self.database_util.update_purchase_order_by_keys(
                        project_number=pn_int,
                        po_number=po_number,
                        vendor_name=vendor_name,
                        description=description,
                        po_type=po_type,
                        contact_id=contact_id
                    )
                    if updated_po:
                        pos_by_number[po_number] = updated_po
                else:
                    self.logger.debug(f"⏭ No changes detected for existing PO {po_number}; skipping update.")
            else:
                self.logger.info(f"🆕 Creating new PO => po_number={po_number}, project={pn_int}")
                contact_id = self._find_or_create_contact_in_db(vendor_name, all_db_contacts)
                new_po = self.database_util.create_purchase_order_by_keys(
                    project_number=pn_int,
                    po_number=po_number,
                    description=description,
                    vendor_name=vendor_name,
                    po_type=po_type,
                    contact_id=contact_id
                )
                if new_po:
                    pos_by_number[po_number] = new_po

        # --- Process detail_items => DetailItems
        existing_details = self.database_util.search_detail_items(['project_number'], [pn_int])
        if existing_details is None:
            existing_details = []
        elif isinstance(existing_details, dict):
            existing_details = [existing_details]

        detail_dict = {}
        for d in existing_details:
            key = (d['po_number'], d['detail_number'], d.get('line_number', 1))
            detail_dict[key] = d
        self.logger.info(
            f' 🧱 Found {len(detail_dict)} existing detail items for project {pn_int}.'
        )

        for detail_entry in detail_items:
            di_po_number = int(detail_entry.get('po_number', 0) or 0)
            di_detail_number = int(detail_entry.get('detail_item_id', 0) or 0)
            di_line_number = int(detail_entry.get('line_number', 1) or 1)
            key = (di_po_number, di_detail_number, di_line_number)

            self.logger.debug(f"🔍 Checking detail item => {key}")
            existing_di = detail_dict.get(key)

            if existing_di:
                changed = False
                if (existing_di.get('vendor') or '') != detail_entry.get('vendor_name', ''):
                    changed = True
                if float(existing_di.get('sub_total', 0.0)) != float(detail_entry.get('sub_total', 0.0)):
                    changed = True

                if changed:
                    self.logger.info(f"🔄 Updating existing DetailItem => {key}")
                    updated_di = self.database_util.update_detail_item_by_keys(
                        project_number=pn_int,
                        po_number=di_po_number,
                        detail_number=di_detail_number,
                        line_number=di_line_number,
                        vendor=detail_entry.get("vendor"),
                        transaction_date=detail_entry.get("date"),
                        due_date=detail_entry.get("due date"),
                        quantity=detail_entry.get("quantity"),
                        rate=detail_entry.get("rate"),
                        detail_entry=detail_entry.get("description"),
                        state=detail_entry.get("state"),
                        account_code=detail_entry.get("account"),
                        payment_type=detail_entry.get('payment_type'),
                        ot=detail_entry.get("ot"),
                        fringes=detail_entry.get("fringes"),
                    )
                    if updated_di:
                        detail_dict[key] = updated_di
                else:
                    self.logger.debug(f"⏭ No changes for DetailItem => {key}. Skipping update.")
            else:
                self.logger.info(f"🆕 Creating new DetailItem => {key}")
                created_di = self.database_util.create_detail_item_by_keys(
                    project_number=pn_int,
                    po_number=di_po_number,
                    detail_number=di_detail_number,
                    line_number=di_line_number,
                    vendor=detail_entry.get("vendor"),
                    transaction_date=detail_entry.get("date"),
                    due_date=detail_entry.get("due date"),
                    quantity=detail_entry.get("quantity"),
                    rate=detail_entry.get("rate"),
                    detail_entry=detail_entry.get("description"),
                    state=detail_entry.get("state"),
                    account_code=detail_entry.get("account"),
                    payment_type=detail_entry.get('payment_type'),
                    ot=detail_entry.get("ot"),
                    fringes=detail_entry.get("fringes"),
                )
                if created_di:
                    detail_dict[key] = created_di

        self.logger.info(" ✅ Finished processing aggregator data for main_items & detail_items.")
    # endregion

    # region Budget Flow
    def process_budget(self, dropbox_path: str):
        """
        Handle .mbb (Showbiz) budgets from Dropbox.
        (Original logic remains here.)
        """
        self.logger.info(f'[process_budget] - 💼 Handling Showbiz budget file from dropbox: {dropbox_path}')
        filename = os.path.basename(dropbox_path)

        try:
            # Basic checks on file extension
            if not filename.endswith('.mbb') or filename.endswith('.mbb.lck'):
                self.logger.info('[process_budget] - ❌ Invalid .mbb or lock file encountered. Skipping.')
                return
        except Exception:
            self.logger.exception('[process_budget] - 💥 Error checking the .mbb extension.', exc_info=True)
            return

        try:
            segments = dropbox_path.strip('/').split('/')
            if len(segments) < 4:
                self.logger.info('[process_budget] - ❌ Folder structure is too short to be recognized as a budget path.')
                return

            project_folder = segments[0]
            budget_folder = segments[1]
            phase_folder = segments[2]

            if budget_folder != '5. Budget' or phase_folder not in ['1.2 Working', '1.3 Actuals']:
                self.logger.info('[process_budget] - ❌ Budget file not in a recognized "5. Budget" folder.')
                return

            project_number_match = re.match(r'^\d{4}', project_folder)
            if not project_number_match:
                self.logger.info("[process_budget] - ❌ Could not derive project number from budget file path.")
                return
            project_number = project_number_match.group()
            self.logger.info(f'[process_budget] - 🔑 Found project folder reference: {project_number}')
        except Exception:
            self.logger.exception('[process_budget] - 💥 Error parsing budget folder.', exc_info=True)
            return

        try:
            budget_root = '/'.join(segments[0:3])
            po_logs_path = f'/{budget_root}/1.5 PO Logs'
            self.logger.info(f'[process_budget] - 🗂 Potential PO Logs reference path: {po_logs_path}')
        except Exception:
            self.logger.exception('[process_budget] - 💥 Could not form PO Logs path.', exc_info=True)
            return

        import requests
        server_url = 'http://localhost:5004/enqueue'
        self.logger.info('[process_budget] - 🖨 Sending request to external ShowbizPoLogPrinter service...')

        try:
            response = requests.post(
                server_url, 
                json={'project_number': project_number, 'file_path': dropbox_path},
                timeout=10
            )
            if response.status_code == 200:
                job_id = response.json().get('job_id')
                self.logger.info(f'[process_budget] - 🎉 External service triggered successfully. job_id: {job_id}')
            else:
                self.logger.error(
                    f'[process_budget] - ❌ External printer service error: {response.status_code}, {response.text}'
                )
                return
        except Exception:
            self.logger.exception(
                '[process_budget] - 💥 Connection error with external ShowbizPoLogPrinter.',
                exc_info=True
            )
            return

        self.logger.info('[process_budget] - ✅ Budget file processing complete. External PO log printing triggered!')
    # endregion

    # region Invoice Flow
    def process_invoice(self, dropbox_path: str):
        """
        Insert or update an 'invoice' record in the DB (plus a share link).
        Other logic (detail item linking, sum checks, etc.) is handled by triggers.
        """
        self.logger.info(f'[process_invoice] - 📄 Recognized invoice file from dropbox: {dropbox_path}')
        filename = os.path.basename(dropbox_path)

        try:
            match = re.match(r'^(\d{4})_(\d{1,2})(?:_(\d{1,2}))?', filename)
            if not match:
                self.logger.warning(
                    f"[process_invoice] - ⚠️ Invoice filename '{filename}' not recognized by the pattern. Skipping."
                )
                return

            project_number_str = match.group(1)
            po_number_str = match.group(2)
            invoice_number_str = match.group(3) or '1'
            project_number = int(project_number_str)
            po_number = int(po_number_str)
            invoice_number = int(invoice_number_str)
            self.logger.info(
                f'[process_invoice] - 🧩 Parsed invoice => project={project_number}, po={po_number}, invoice={invoice_number}'
            )
        except Exception:
            self.logger.exception(
                f"[process_invoice] - 💥 Error parsing invoice filename '{filename}'.",
                exc_info=True
            )
            return

        try:
            file_share_link = self.dropbox_util.get_file_link(dropbox_path)
            self.logger.info(f'[process_invoice] - 🔗 Dropbox share link obtained: {file_share_link}')
        except Exception:
            self.logger.exception('[process_invoice] - 💥 Error getting share link for invoice.', exc_info=True)
            file_share_link = None

        temp_file_path = f'./temp_files/{filename}'
        self.logger.info('[process_invoice] - 🚀 Attempting to download the invoice from dropbox...')
        if not self.download_file_from_dropbox(dropbox_path, temp_file_path):
            self.logger.error(
                f'[process_invoice] - ❌ Could not download invoice from dropbox path: {dropbox_path}'
            )
            return

        transaction_date, term, total = None, 30, 0.0
        try:
            self.logger.info('[process_invoice] - 🔎 Extracting invoice details using OCR + OpenAI analysis...')
            extracted_text = self.ocr_service.extract_text(temp_file_path)
            (info, err) = self.ocr_service.extract_info_with_openai(extracted_text)

            if err or not info:
                self.logger.warning(f'[process_invoice] - ❌ OCR/AI extraction failed. Using default fallback. Error: {err}')
            else:
                # Attempt to parse date, total, term
                date_str = info.get('invoice_date')
                if date_str:
                    try:
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
        except Exception:
            self.logger.exception('[process_invoice] - 💥 Error during OCR/AI extraction.', exc_info=True)

        try:
            self.logger.info(
                f'[process_invoice] - 🤖 Sending invoice references (#{invoice_number}) to DB aggregator...'
            )
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
        except Exception:
            self.logger.exception(
                f'[process_invoice] - 💥 Error updating invoice #{invoice_number} in DB.',
                exc_info=True
            )
            self.cleanup_temp_file(temp_file_path)
            return

        self.cleanup_temp_file(temp_file_path)
        self.logger.info(f'[process_invoice] - ✅ Finished invoice processing for dropbox file: {dropbox_path}')
    # endregion

    # region Tax Form Flow
    def process_tax_form(self, dropbox_path: str):
        """
        Stub function for processing a tax form from Dropbox.
        """
        self.logger.info(f'[process_tax_form] - 🗂 Recognized a tax form file in dropbox: {dropbox_path}')
        # Could add more specific handling here if desired
        pass
    # endregion

    # region Receipt Flow
    def process_receipt(self, dropbox_path: str):
        """
        🧾 process_receipt
        -----------------
        1) Parse file name (project_number, po_number, detail_number, vendor_name).
        2) Download the receipt file from Dropbox.
        3) If PDF, try text extraction via PyPDF2. If that fails (or not a PDF), do OCR.
        4) Use OCRService's 'extract_receipt_info_with_openai' to parse total, date, description.
        5) Generate file link in Dropbox.
        6) Create or update the 'receipt' table, linking to the appropriate detail item.
        7) Update the corresponding subitem in Monday with the link.
        8) After creation/update, link `receipt_id` to the relevant detail item.
        """
        self.logger.info(f'[process_receipt] - 🧾 Recognized a receipt file from dropbox: {dropbox_path}')
        temp_file_path = f'./temp_files/{os.path.basename(dropbox_path)}'
        filename = os.path.basename(dropbox_path)

        is_petty_cash = (
            '3. Petty Cash' in dropbox_path 
            or 'Crew PC Folders' in dropbox_path 
            or filename.startswith('PC_')
        )
        pattern = r'^(?:PC_)?(\d{4})_(\d{2})_(\d{2})\s+(.*?)\s+Receipt\.(pdf|jpe?g|png)$'
        match = re.match(pattern, filename, re.IGNORECASE)

        if not match:
            self.logger.warning(
                f"[process_receipt] - ❌ Receipt filename '{filename}' doesn't match a recognized pattern. Skipping."
            )
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

            ocr_service = OCRService()
            receipt_info = {}
            if not parse_failed:
                self.logger.debug('[process_receipt] - Using OCRService + OpenAI to interpret receipt text...')
                receipt_info = ocr_service.extract_receipt_info_with_openai(extracted_text)
                if not receipt_info:
                    self.logger.warning(
                        f'[process_receipt] - 🛑 AI parse returned empty data for {filename}; marking parse as failed.'
                    )
                    parse_failed = True
            else:
                self.logger.warning('[process_receipt] - Skipping AI parse due to empty extraction result.')
                receipt_info = {}

            if parse_failed or not receipt_info:
                receipt_info = {
                    'total_amount': 0.0, 
                    'description': 'Could not parse', 
                    'date': None
                }

            total_amount = receipt_info.get('total_amount', 0.0)
            purchase_date = receipt_info.get('date', '')
            try:
                datetime.strptime(purchase_date, '%Y-%m-%d')
            except (ValueError, TypeError):
                purchase_date = None
            short_description = receipt_info.get('description', '')

            try:
                shared_link_metadata = self.dropbox_util.get_file_link(dropbox_path)
                file_link = shared_link_metadata.replace('?dl=0', '?dl=1')
            except Exception:
                self.logger.exception(
                    f'[process_receipt] - ❌ Unable to create dropbox link for {dropbox_path}.',
                    exc_info=True
                )
                file_link = None

            self.logger.info('[process_receipt] - 🤖 Searching for corresponding detail item in DB...')
            existing_detail = self.database_util.search_detail_item_by_keys(
                project_number=str(project_number),
                po_number=po_number,
                detail_number=detail_number,
                line_number=line_number_number
            )

            if not existing_detail:
                self.logger.warning(
                    f'[process_receipt] - ❗ No matching detail found (project={project_number}, PO={po_number}).'
                )
                self.cleanup_temp_file(temp_file_path)
                return
            elif isinstance(existing_detail, list):
                existing_detail = existing_detail[0]

            spend_money_id = 1  # Example/filler
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

            state = 'PENDING'
            detail_subtotal = existing_detail.get('sub_total', 0.0)

            # Determine detail's new state
            if existing_detail['state'] != 'RECONCILED':
                if parse_failed:
                    state = 'ISSUE'
                    self.logger.info(
                        f'[process_receipt] - Marking detail state as ISSUE due to parse failures for {filename}.'
                    )
                else:
                    try:
                        if float(total_amount) == float(detail_subtotal):
                            state = 'REVIEWED'
                            self.logger.info('[process_receipt] - Receipt total matches detail subtotal => REVIEWED.')
                        else:
                            self.logger.info(
                                '[process_receipt] - Receipt total != detail subtotal => PO MISMATCH.'
                            )
                            state = 'PO MISMATCH'
                    except Exception:
                        self.logger.exception(
                            '[process_receipt] - Error comparing totals; marking state as ISSUE.',
                            exc_info=True
                        )
                        state = 'ISSUE'
            else:
                state = 'RECONCILED'

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

        except Exception:
            self.logger.exception(f'[process_receipt] - 💥 Error processing receipt {filename}.', exc_info=True)
            return
    # endregion

    # region Contact Helpers
    def find_or_create_vendor_contact(self, po_record: dict):
        """
        If the PO record has no contact_id or vendor_name, attempt to find
        an existing contact by vendor_name. If none found, create one.
        Returns contact_id or None.
        """
        vendor_name = (po_record.get('vendor_name') or '').strip()
        if not vendor_name:
            self.logger.warning("🔎 No vendor_name on PO record. Skipping contact creation.")
            return None

        self.logger.info(f"🌐 Looking up or creating contact for vendor '{vendor_name}'...")
        all_contacts = self.database_util.search_contacts()
        if not all_contacts:
            all_contacts = []

        fuzzy_matches = self.database_util.find_contact_close_match(vendor_name, all_contacts)
        if fuzzy_matches:
            best_match = fuzzy_matches[0]
            self.logger.info(f"👤 Found fuzzy match => {best_match['name']}")
            return best_match['id']
        else:
            self.logger.info(f"🙅 No fuzzy match for '{vendor_name}'; creating new contact.")
            created = self.database_util.create_contact(name=vendor_name, vendor_type='Vendor')
            return created['id'] if created else None

    def _find_or_create_contact_in_db(self, vendor_name: str, all_db_contacts: list) -> Optional[int]:
        """
        Helper method to do a quick fuzzy search for an existing contact,
        or create a new contact if no match. Returns contact_id or None.
        """
        if not vendor_name:
            return None

        fuzzy_matches = self.database_util.find_contact_close_match(vendor_name, all_db_contacts)
        if fuzzy_matches:
            best = fuzzy_matches[0]
            return best['id']
        else:
            new_c = self.database_util.create_contact(name=vendor_name, vendor_type='Vendor')
            return new_c['id'] if new_c else None
    # endregion

    # region Receipt/Detail Matching
    def match_receipt_for_detail(self, detail_item: dict):
        """
        Attempt to match a receipt in DB to see if there's a total matching detail_item['sub_total'].
        Return a dict like {'total': float, 'path': str} if found, else None.
        """
        sub_total = float(detail_item.get('sub_total') or 0.0)
        project_number = detail_item.get('project_number')
        po_number = detail_item.get('po_number')
        detail_number = detail_item.get('detail_number')
        line_number = detail_item.get('line_number')

        try:
            found_receipts = self.database_util.search_receipt_by_keys(
                project_number=project_number,
                po_number=po_number,
                detail_number=detail_number,
                line_number=line_number
            )
            if not found_receipts:
                self.logger.info("🧾 No existing receipt found in DB. Returning None.")
                return None
            if isinstance(found_receipts, dict):
                found_receipts = [found_receipts]

            for rec in found_receipts:
                rec_total = float(rec.get('total') or 0.0)
                if abs(rec_total - sub_total) < 0.0001:
                    self.logger.info(f"🎯 Found matching receipt => total={rec_total}")
                    return {'total': rec_total, 'path': rec.get('file_link') or ''}
            return None
        except Exception:
            self.logger.exception("[match_receipt_for_detail] - Error matching receipts.", exc_info=True)
            return None
    # endregion

    # region Folder Linking
    def find_po_folder_link(self, po_record: dict):
        """
        Check Dropbox for a folder associated with this PO, e.g. by project_number + po_number.
        Return a shareable link or None if not found.
        """
        project_number = po_record.get('project_number')
        po_number = po_record.get('po_number')
        if not project_number or not po_number:
            self.logger.warning("🗂 Missing project_number or po_number => cannot find PO folder link.")
            return None

        # Simulated logic for demonstration
        simulated_link = f"https://www.dropbox.com/sh/example_{project_number}_{po_number}"
        self.logger.info(f"🔗 Simulated folder link => {simulated_link}")
        return simulated_link
    # endregion

    # region Dropbox Utilities
    def download_file_from_dropbox(self, path: str, temp_file_path: str) -> bool:
        """
        Download a file from Dropbox to a local temp_file_path.
        """
        try:
            self.logger.info(f'[Download File] - 🚀 Initiating download for path: {path}')
            dbx = self.dropbox_client.dbx
            self.logger.debug(f'[Download File] - Calling dbx.files_download for {path}')
            _, res = dbx.files_download(path)
            file_content = res.content

            with open(temp_file_path, 'wb') as temp_file:
                temp_file.write(file_content)

            self.logger.info(f'[Download File] - 📂 Saved to {temp_file_path}, download complete!')
            return True
        except Exception:
            self.logger.exception(
                f'[Download File] - 💥 Encountered error while downloading {path}.',
                exc_info=True
            )
            return False

    def cleanup_temp_file(self, temp_file_path: str):
        """
        Attempt to remove a temporary file.
        """
        try:
            if os.path.exists(temp_file_path):
                os.remove(temp_file_path)
                self.logger.info(f'[cleanup_temp_file] - 🧹 Temp file removed: {temp_file_path}')
        except Exception:
            self.logger.warning(
                f'[cleanup_temp_file] - ⚠️ Could not remove temp file {temp_file_path}.',
                exc_info=True
            )

    def extract_project_number(self, file_name: str) -> str:
        """
        Extract the first 4-digit sequence from a file name
        to interpret as the project number.
        """
        digit_sequences = re.findall(r'\d+', file_name)
        if not digit_sequences:
            raise ValueError(f"❗ No digits found in file name: '{file_name}' ❗")

        all_digits = ''.join(digit_sequences)
        if len(all_digits) < 4:
            raise ValueError(
                f"❗ File name '{file_name}' does not contain at least four digits for project_id. ❗"
            )
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

        self.logger.debug('[_extract_text_from_pdf] - Trying PyPDF2 direct extraction...')
        extracted_text = ''
        try:
            pdf_reader = PyPDF2.PdfReader(BytesIO(file_data))
            text_chunks = []
            for idx, page in enumerate(pdf_reader.pages, start=1):
                page_text = page.extract_text() or ''
                if page_text:
                    self.logger.debug(f'[_extract_text_from_pdf] - Extracted text from page {idx}.')
                    text_chunks.append(page_text)
            extracted_text = '\n'.join(text_chunks)

            if len(extracted_text.strip()) < 20:
                self.logger.info('[_extract_text_from_pdf] - Minimal text found; fallback to OCR method.')
                extracted_text = ''

            if extracted_text.strip():
                return extracted_text

            # Fallback with PyMuPDF to do OCR on embedded images
            pdf_document = fitz.open(stream=file_data, filetype='pdf')
            embedded_ocr_results = []
            for page_idx in range(pdf_document.page_count):
                page = pdf_document[page_idx]
                images = page.get_images(full=True)
                for _, img_info in enumerate(images, start=1):
                    xref = img_info[0]
                    base_image = pdf_document.extract_image(xref)
                    image_data = base_image['image']
                    try:
                        Image.open(BytesIO(image_data)).convert('RGB')
                        text_in_image = self._extract_text_via_ocr(image_data)
                        embedded_ocr_results.append(text_in_image)
                    except Exception:
                        self.logger.exception(
                            '[_extract_text_from_pdf] - Could not OCR embedded PDF image.',
                            exc_info=True
                        )
            fallback_text = '\n'.join(embedded_ocr_results)
            return fallback_text

        except Exception:
            self.logger.exception('[_extract_text_from_pdf] - Error parsing PDF with PyPDF2/fallback.', exc_info=True)
            return ''

    def _extract_text_from_pdf_with_ocr(self, file_data: bytes) -> str:
        """
        A direct OCR approach if PyPDF2 text extraction yields nothing.
        Reuses _extract_text_via_ocr for convenience.
        """
        return self._extract_text_via_ocr(file_data)

    def _extract_text_via_ocr(self, file_data: bytes) -> str:
        """
        Use the OCRService to extract text from the provided file data.
        """
        try:
            return self.ocr_service.extract_text_from_receipt(file_data)
        except Exception:
            self.logger.exception('[_extract_text_via_ocr] - OCR extraction issue.', exc_info=True)
            return ''
    # endregion

    # region Monday Integration
    def folder_tax_conact_trigger(self, fut):
        """
        Callback for DB process completion. Possibly triggers Monday or Dropbox tasks next.
        """
        try:
            processed_items = fut.result()
            if self.GET_FOLDER_LINKS:
                self.logger.info('[folder_tax_conact_trigger] - ⏩ Attempting to update folder references...')
                def get_folder_links(items):
                    for item in items:
                        self.update_po_folder_link(item['project_number'], item['po_number'])
                folder_links_future = self.executor.submit(get_folder_links, processed_items)

            if self.GET_TAX_LINKS:
                self.logger.info('[folder_tax_conact_trigger] - ⏩ Attempting to update any tax form references...')
                def get_tax_links(items):
                    for item in items:
                        self.update_po_tax_form_links(item['project_number'], item['po_number'])
                tax_links_future = self.executor.submit(get_tax_links, processed_items)

            futures_to_wait = []
            if self.GET_FOLDER_LINKS and 'folder_links_future' in locals():
                futures_to_wait.append(folder_links_future)
            if self.GET_TAX_LINKS and 'tax_links_future' in locals():
                futures_to_wait.append(tax_links_future)

            if futures_to_wait:
                wait(futures_to_wait, return_when=ALL_COMPLETED)

            if processed_items:
                project_number = processed_items[0].get('project_number')
                if project_number:
                    self.logger.info(
                        f'[folder_tax_conact_trigger] - Folder/tax updates done. Next: create POs in Monday for {project_number} if enabled.'
                    )
                    if self.ADD_PO_TO_MONDAY:
                        self.create_pos_in_monday(int(project_number))
                    else:
                        self.logger.info(
                            '[folder_tax_conact_trigger] - PO creation in Monday is disabled. Done here.'
                        )
                else:
                    self.logger.warning(
                        '[folder_tax_conact_trigger] - ❌ No project_number found in processed items. Skipping Monday step.'
                    )
            else:
                self.logger.warning(
                    '[folder_tax_conact_trigger] - Nothing returned from DB aggregator to process. Skipping next step.'
                )

        except Exception:
            self.logger.exception('[folder_tax_conact_trigger] - ❌ Error finalizing dropbox references.', exc_info=True)

    def update_po_folder_link(self, project_number, po_number):
        """
        Check and update the dropbox folder references for the specified PO.
        """
        self.logger.info(
            f'[update_po_folder_link] - 🚀 Checking dropbox folder references for PO {project_number}_{str(po_number).zfill(2)}'
        )
        try:
            po_data = self.database_util.search_purchase_order_by_keys(project_number, po_number)
            if not po_data:
                self.logger.warning(
                    f'[update_po_folder_link] - ❌ No PO data found in DB for {project_number}_{str(po_number).zfill(2)}.'
                )
                return

            if po_data.get('folder_link'):
                self.logger.debug('[update_po_folder_link] - Folder link already assigned. No updates needed.')
                return

            project_item = dropbox_api.get_project_po_folders_with_link(
                project_number=project_number,
                po_number=po_number
            )
            if not project_item:
                self.logger.warning(
                    f'[update_po_folder_link] - ⚠️ No dropbox folder found for {project_number}_{str(po_number).zfill(2)}.'
                )
                return

            project_item = project_item[0]
            po_folder_link = project_item['po_folder_link']
            po_folder_name = project_item['po_folder_name']
            self.logger.debug(f"[update_po_folder_link] - Found potential folder '{po_folder_name}' in dropbox.")

            if po_folder_link:
                self.logger.info(
                    f'[update_po_folder_link] - ✅ Linking dropbox folder to PO {project_number}_{str(po_number).zfill(2)}...'
                )
                self.database_util.update_purchase_order(po_id=po_data['id'], folder_link=po_folder_link)
            else:
                self.logger.warning('[update_po_folder_link] - ⚠️ No folder_link found in dropbox data.')

            self.logger.info(
                f'[update_po_folder_link] - 🎉 Folder reference update complete for PO {project_number}_{str(po_number).zfill(2)}'
            )
        except Exception:
            self.logger.exception('[update_po_folder_link] - 💥 Error while updating folder link.', exc_info=True)

    def update_po_tax_form_links(self, project_number, po_number):
        """
        Update or set the tax_form_link for a PurchaseOrder in Dropbox if needed.
        """
        try:
            po_search = self.database_util.search_purchase_order_by_keys(project_number, po_number)
            if not po_search or po_search.get('po_type') != 'INV':
                return None

            if isinstance(po_search, dict):
                contact_id = po_search['contact_id']
            elif isinstance(po_search, list) and po_search:
                contact_id = po_search[0]['contact_id']
            else:
                return None

            po_tax_data = self.dropbox_api.get_po_tax_form_link(project_number=project_number, po_number=po_number)
            new_tax_form_link = po_tax_data[0]['po_tax_form_link']
            self.database_util.update_contact(contact_id, tax_form_link=new_tax_form_link)
            self.logger.info(
                f'[update_po_tax_form_links] - 📑 Applied new tax form link for PO {project_number}_{po_number}'
            )
            return new_tax_form_link
        except Exception:
            self.logger.exception(
                f'[update_po_tax_form_links] - 💥 Could not update tax form link for PO {project_number}_{po_number}.',
                exc_info=True
            )

    def create_pos_in_monday(self, project_number):
        """
        Demonstrates how to fetch all subitems once from Monday,
        then process them locally to avoid multiple queries.
        """
        self.logger.info('[create_pos_in_monday] - 🌐 Creating/Updating PO records in Monday.com...')
        monday_items = self.monday_api.get_items_in_project(project_id=project_number)
        processed_items = self.database_util.search_purchase_order_by_keys(project_number=project_number)
        monday_items_map = {}

        # Build Monday items map
        for mi in monday_items:
            pid = mi['column_values'].get(self.monday_util.PO_PROJECT_ID_COLUMN)['text']
            pono = mi['column_values'].get(self.monday_util.PO_NUMBER_COLUMN)['text']
            if pid and pono:
                monday_items_map[int(pid), int(pono)] = mi

        all_subitems = self.monday_api.get_subitems_in_board(project_number=project_number)
        global_subitem_map = {}
        for msub in all_subitems:
            identifiers = self.monday_util.extract_subitem_identifiers(msub)
            if identifiers is not None:
                global_subitem_map[identifiers] = msub

        items_to_create = []
        items_to_update = []

        import json
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

            if key in monday_items_map:
                monday_item = monday_items_map[key]
                differences = self.monday_util.is_main_item_different(db_item, monday_item)
                if differences:
                    self.logger.debug(
                        f'[create_pos_in_monday] - Main item differs for PO {po_no}, scheduling update...'
                    )
                    items_to_update.append({
                        'db_item': db_item,
                        'column_values': new_vals,
                        'monday_item_id': monday_item['id']
                    })
                else:
                    self.logger.debug(
                        f'[create_pos_in_monday] - No changes for PO {po_no}, skipping.'
                    )
            else:
                items_to_create.append({'db_item': db_item, 'column_values': new_vals, 'monday_item_id': None})

        if items_to_create:
            self.logger.info(
                f'[create_pos_in_monday] - 🆕 Creating {len(items_to_create)} new main items in Monday...'
            )
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
                po = int(db_item['po_number'])
                monday_items_map[p_id, po] = {
                    'id': monday_item_id,
                    'name': f'PO #{po}',
                    'column_values': itm['column_values']
                }

        if items_to_update:
            self.logger.info(
                f'[create_pos_in_monday] - ✏️ Updating {len(items_to_update)} main items in Monday...'
            )
            updated_mapping = self.monday_api.batch_create_or_update_items(
                items_to_update,
                project_id=project_number,
                create=False
            )
            for itm in updated_mapping:
                db_item = itm['db_item']
                monday_item_id = itm['monday_item_id']
                self.database_util.update_purchase_order_by_keys(
                    project_number, 
                    db_item['po_number'], 
                    pulse_id=monday_item_id
                )
                db_item['pulse_id'] = monday_item_id
                po = int(db_item['po_number'])
                monday_items_map[p_id, po]['column_values'] = itm['column_values']

        # Subitems sync
        for db_item in processed_items:
            p_id = project_number
            po_no = int(db_item['po_number'])
            main_monday_item = monday_items_map.get((p_id, po_no))
            if not main_monday_item:
                self.logger.warning(
                    f'[create_pos_in_monday] - ❌ No main Monday reference for PO {po_no}, skipping subitems.'
                )
                continue

            main_monday_id = main_monday_item['id']
            sub_items_db = self.database_util.search_detail_item_by_keys(p_id, db_item['po_number'])
            if isinstance(sub_items_db, dict):
                sub_items_db = [sub_items_db]

            subitems_to_create = []
            subitems_to_update = []

            if not sub_items_db:
                continue

            for sdb in sub_items_db:
                if sdb.get('account_code_id'):
                    account_row = self.database_util.search_account_codes(['id'], [sdb['account_code_id']])
                    sdb['account_code'] = account_row['code'] if account_row else None
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
                            [p_id, po_no, sdb['detail_number'], sdb['line_number']]
                        )
                        if existing_receipts:
                            first_receipt = (
                                existing_receipts[0]
                                if isinstance(existing_receipts, list)
                                else existing_receipts
                            )
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
                            [p_id, po_no, sdb['detail_number']]
                        )
                        if existing_invoices:
                            first_invoice = (
                                existing_invoices[0] 
                                if isinstance(existing_invoices, list) 
                                else existing_invoices
                            )
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
                    project_id=p_id,
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
                sub_key = (p_id, db_item['po_number'], sdb['detail_number'], sdb['line_number'])

                if sub_key in global_subitem_map:
                    msub = global_subitem_map[sub_key]
                    differences = self.monday_util.is_sub_item_different(sdb, msub)

                    if differences:
                        self.logger.debug(
                            f'[create_pos_in_monday] - Sub-item differs for detail #{sdb["detail_number"]}; scheduling update...'
                        )
                        subitems_to_update.append({
                            'db_sub_item': sdb,
                            'column_values': new_sub_vals,
                            'parent_id': main_monday_id,
                            'monday_item_id': msub['id']
                        })
                    else:
                        sub_pulse_id = msub['id']
                        current_pulse_id = sdb.get('pulse_id')
                        current_parent_id = sdb.get('parent_pulse_id')

                        if current_pulse_id != int(sub_pulse_id) or current_parent_id != int(main_monday_id):
                            self.logger.debug(
                                '[create_pos_in_monday] - Subitem pulse mismatch, updating DB references...'
                            )
                            self.database_util.update_detail_item_by_keys(
                                project_number=p_id,
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

            if subitems_to_create:
                self.logger.info(
                    f'[create_pos_in_monday] - 🆕 Creating {len(subitems_to_create)} new sub-items for PO {po_no}...'
                )
                self._batch_create_subitems(subitems_to_create, main_monday_id, p_id, db_item)

            if subitems_to_update:
                self.logger.info(
                    f'[create_pos_in_monday] - ✏️ Updating {len(subitems_to_update)} existing sub-items for PO {po_no}...'
                )
                self._batch_update_subitems(subitems_to_update, main_monday_id, p_id, db_item)

        self.logger.info('[create_pos_in_monday] - ✅ Completed Monday.com integration for all processed PO data.')

    def _batch_create_subitems(self, subitems_to_create, parent_item_id, project_number, db_item):
        from concurrent.futures import ThreadPoolExecutor, as_completed
        chunk_size = 10
        create_chunks = [
            subitems_to_create[i:i + chunk_size] 
            for i in range(0, len(subitems_to_create), chunk_size)
        ]
        all_created_subs = []

        with ThreadPoolExecutor() as executor:
            future_to_index = {}
            for idx, chunk in enumerate(create_chunks):
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
                    self.logger.debug(
                        f'[_batch_create_subitems] - Subitem create-chunk #{idx + 1} completed.'
                    )
                    all_created_subs.extend(chunk_result)
                except Exception:
                    self.logger.exception(
                        f'[_batch_create_subitems] - ❌ Error creating subitems in chunk {idx + 1}.',
                        exc_info=True
                    )
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
        from concurrent.futures import ThreadPoolExecutor, as_completed
        chunk_size = 10
        update_chunks = [
            subitems_to_update[i:i + chunk_size] 
            for i in range(0, len(subitems_to_update), chunk_size)
        ]
        all_updated_subs = []

        with ThreadPoolExecutor() as executor:
            future_to_index = {}
            for idx, chunk in enumerate(update_chunks):
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
                    self.logger.debug(
                        f'[_batch_update_subitems] - Subitem update-chunk #{idx + 1} completed.'
                    )
                    all_updated_subs.extend(chunk_result)
                except Exception:
                    self.logger.exception(
                        f'[_batch_update_subitems] - ❌ Error updating subitems in chunk {idx + 1}.',
                        exc_info=True
                    )
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
    # endregion

    # region Project Scanning / syncing procedures
    def scan_project_receipts(self, project_number: str):
        """
        Scans credit-card/vendor receipt folders (1. Purchase Orders) and
        petty-cash receipt folders (3. Petty Cash/1. Crew PC Folders) for the project.
        """
        self.logger.info(
            f'[scan_project_receipts] - 🔎 Initiating dropbox scan for receipts, project={project_number}...'
        )
        project_folder_path = self.dropbox_api.find_project_folder(project_number, namespace='2024')
        if not project_folder_path:
            self.logger.warning(
                f"[scan_project_receipts] - ❌ No matching project folder in dropbox for '{project_number}' under 2024."
            )
            return

        self.logger.info(
            f'[scan_project_receipts] - 📂 Resolved project folder path: {project_folder_path}'
        )
        purchase_orders_path = f'{project_folder_path}/1. Purchase Orders'
        petty_cash_path = f'{project_folder_path}/3. Petty Cash/1. Crew PC Folders'

        self._scan_and_process_receipts_in_folder(purchase_orders_path, project_number)
        self._scan_and_process_receipts_in_folder(petty_cash_path, project_number)
        self.logger.info(
            f'[scan_project_receipts] - ✅ Finished scanning dropbox receipts for project {project_number}.'
        )

    def _scan_and_process_receipts_in_folder(self, folder_path: str, project_number: str):
        entries = self._list_folder_recursive(folder_path)
        if not entries:
            self.logger.debug(
                f"[_scan_and_process_receipts_in_folder] - No entries found in dropbox folder '{folder_path}'."
            )
            return

        for entry in entries:
            if entry['is_folder']:
                continue
            dropbox_path = entry['path_display']
            file_name = entry['name']
            if re.search(self.RECEIPT_REGEX, file_name, re.IGNORECASE):
                self.logger.debug(
                    f'[_scan_and_process_receipts_in_folder] - 🧾 Found a potential receipt: {dropbox_path}'
                )
                self.process_receipt(dropbox_path)

    def _list_folder_recursive(self, folder_path: str):
        from dropbox import files
        results = []

        try:
            dbx = self.dropbox_client.dbx
            self.logger.info(
                f'[_list_folder_recursive] - 📁 Recursively listing dropbox folder: {folder_path}'
            )
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
        except Exception:
            self.logger.exception(
                f'[_list_folder_recursive] - ⚠️ Could not list folder recursively for {folder_path}.',
                exc_info=True
            )
        return results

    def _scan_po_folder_for_invoices(self, folder_path: str, project_number: str, folder_po_number: str):
        entries = self._list_folder_recursive(folder_path)
        if not entries:
            return

        for entry in entries:
            if entry['is_folder']:
                continue
            dropbox_path = entry['path_display']
            file_name = entry['name']
            if re.search(self.INVOICE_REGEX, file_name, re.IGNORECASE):
                self.logger.debug(
                    f'[_scan_po_folder_for_invoices] - Found potential invoice: {dropbox_path}'
                )
                self.process_invoice(dropbox_path)
    # endregion

# region Singleton Instance
dropbox_service = DropboxService()
# endregion