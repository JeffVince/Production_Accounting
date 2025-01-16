# -*- coding: utf-8 -*-
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

# region Imports
import json
import os
import re
import tempfile
import traceback
import logging
from concurrent.futures import ThreadPoolExecutor, wait, ALL_COMPLETED, as_completed
from datetime import datetime
from typing import Optional

import pytesseract

from dropbox_files.dropbox_api import dropbox_api
from config import Config
from dropbox_files.dropbox_client import dropbox_client
from dropbox_files.dropbox_util import dropbox_util
from monday_files.monday_api import monday_api
from monday_files.monday_util import monday_util
from monday_files.monday_service import monday_service
from po_log_database_util import po_log_database_util
from po_log_files.po_log_processor import POLogProcessor
from utilities.singleton import SingletonMeta
from ocr_service import OCRService
from pdf2image import convert_from_path
from typing import List
from PIL import Image

# Import the updated DB ops:
from database.database_util import DatabaseOperations


# endregion


class DropboxService(metaclass=SingletonMeta):
    """
    📦 DropboxService
    =================
    Singleton class that coordinates processing of files from Dropbox, with
    a strong focus on PO logs, contacts, and purchase orders.
    """

    # region Class/Static Members
    PO_LOG_FOLDER_NAME = "1.5 PO Logs"
    PO_NUMBER_FORMAT = "{:02}"
    INVOICE_REGEX = r"invoice"
    TAX_FORM_REGEX = r"w9|w8-ben|w8-ben-e"
    RECEIPT_REGEX = r"receipt"
    SHOWBIZ_REGEX = r".mbb"
    PROJECT_NUMBER = ""

    USE_TEMP_FILE = True  # Whether to use a local temp file
    DEBUG_STARTING_PO_NUMBER = 0  # If set, skip POs below this number
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
            self.logger = logging.getLogger("app_logger")
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
            self.logger.info("📦 Dropbox Service initialized 🌟")
            self._initialized = True


    # endregion

    # region Type Determination
    def determine_file_type(self, path: str):
        """
        Determine the file type by matching patterns in its name,
        then route the file to the appropriate process_* handler.

        :param path: The Dropbox file path
        """
        self.logger.info(
            f"🔍 Checking file type for: {self.dropbox_util.get_last_path_component_generic(path)}"
        )
        filename = os.path.basename(path)

        try:
            # 1) PO_LOG check
            if self.PO_LOG_FOLDER_NAME in path:
                project_number_match = re.match(
                    r"^PO_LOG_(\d{4})[-_]\d{4}-\d{2}-\d{2}_\d{2}-\d{2}-\d{2}\.txt$",
                    filename
                )
                if project_number_match:
                    project_number = project_number_match.group(1)
                    self.logger.info(f"🗂 Identified as PO Log for Project ID {project_number}")
                    return self.po_log_orchestrator(path)
                else:
                    self.logger.warning(f"⚠️ Filename '{filename}' does not match PO Log format.")
                    return

            # 2) Invoice?
            if re.search(self.INVOICE_REGEX, filename, re.IGNORECASE):
                self.logger.info(f"💰 Identified as invoice: {filename}")
                return self.process_invoice(path)

            # 3) Tax form?
            if re.search(self.TAX_FORM_REGEX, filename, re.IGNORECASE):
                self.logger.info(f"💼 Identified as tax form: {filename}")
                return self.process_tax_form(path)

            # 4) Receipt?
            if re.search(self.RECEIPT_REGEX, filename, re.IGNORECASE):
                self.logger.info(f"🧾 Identified as receipt: {filename}")
                return self.process_receipt(path)

            # 5) Showbiz budget file?
            if re.search(self.SHOWBIZ_REGEX, filename, re.IGNORECASE):
                self.logger.info(f"📑 Identified as budget file: {filename}")
                return None #self.process_budget(path)

            # No recognized type
            self.logger.debug(f"❌ Unsupported file type: {filename}")
            return None

        except Exception as e:
            self.logger.exception(f"💥 Error determining file type for {filename}: {e}", exc_info=True)
            return None

    # endregion

    # region Event Processing - Budget -> PO LOG
    def process_budget(self, dropbox_path: str):
        self.logger.info(f"💼 Processing budget: {dropbox_path}")
        filename = os.path.basename(dropbox_path)
        try:
            if not filename.endswith(".mbb") or filename.endswith(".mbb.lck"):
                self.logger.info("❌ Not a valid .mbb file.")
                return
        except Exception as e:
            self.logger.exception(f"💥 Error checking extension: {e}", exc_info=True)
            return

        # Extract project_number
        try:
            segments = dropbox_path.strip("/").split("/")
            if len(segments) < 4:
                self.logger.info("❌ Not enough path segments.")
                return

            project_folder = segments[0]
            budget_folder = segments[1]
            phase_folder = segments[2]

            if budget_folder != "5. Budget" or phase_folder not in ["1.2 Working", "1.3 Actuals"]:
                self.logger.info("❌ Budget file not in correct folder.")
                return

            project_number_match = re.match(r"^\d{4}", project_folder)
            if not project_number_match:
                self.logger.info("❌ Can't determine Project ID.")
                return
            project_number = project_number_match.group()

            self.logger.info(f"🔑 Project ID: {project_number}")
        except Exception as e:
            self.logger.exception(f"💥 Error parsing path: {e}", exc_info=True)
            return

        try:
            budget_root = "/".join(segments[0:3])
            po_logs_path = f"/{budget_root}/1.5 PO Logs"
            self.logger.info(f"🗂 PO Logs folder: {po_logs_path}")
        except Exception as e:
            self.logger.exception(f"💥 Error determining PO Logs folder: {e}", exc_info=True)
            return

        # Trigger a server job (if required)
        import requests
        server_url = "http://localhost:5004/enqueue"
        self.logger.info("🖨 Triggering ShowbizPoLogPrinter via server with file URL...")
        try:
            response = requests.post(
                server_url,
                json={"project_number": project_number, "file_path": dropbox_path},
                timeout=10
            )
            if response.status_code == 200:
                job_id = response.json().get("job_id")
                self.logger.info(f"🎉 Triggered server job with job_id: {job_id}")
            else:
                self.logger.error(
                    f"❌ Failed to trigger server job. Status: {response.status_code}, Response: {response.text}")
                return
        except Exception as e:
            self.logger.exception(f"💥 Error triggering server job: {e}", exc_info=True)
            return

        self.logger.info("✅ process_budget completed successfully, server job triggered with file URL.")

    # endregion -

    # region  Event Processing - PO LOG -> DB
    def po_log_orchestrator(self, path: str):
        """
        Process a PO log file from Dropbox, parse it, then store the results in the DB.
        This includes adding Contacts, PurchaseOrders, and DetailItems.
        """
        self.logger.info(f"📝 Processing PO log: {path}")
        temp_file_path = f"./temp_files/{os.path.basename(path)}"
        project_number = self.extract_project_number(temp_file_path)
        self.PROJECT_NUMBER = project_number

        # Download from Dropbox if we are NOT using a preexisting temp file
        if not self.USE_TEMP_FILE:
            if not self.download_file_from_dropbox(path, temp_file_path):
                return

        # Parse the PO log (extract main_items, detail_items, contacts)
        main_items, detail_items, contacts = self.extract_data_from_po_log(temp_file_path, project_number)

        # Otherwise, process in DB asynchronously
        self.add_po_data_to_db(main_items, detail_items, contacts, project_number)

        #db_future = self.executor.submit(self.add_po_data_to_db, main_items, detail_items, contacts)
        #db_future.add_done_callback(self.callback_add_po_data_to_DB)

        self.logger.info("🔧 DB and Monday processing dispatched to background threads.")

    def extract_data_from_po_log(self, temp_file_path: str, project_number: str):
        """
        Parse the local PO log file to extract main_items, detail_items, and contacts.

        :param temp_file_path: The local file path
        :param project_number: The project ID
        :return: (main_items, detail_items, contacts)
        """
        try:
            main_items, detail_items, contacts = self.po_log_processor.parse_showbiz_po_log(temp_file_path)
            self.logger.info(
                f"📝 Parsed PO Log for project {project_number}: "
                f"{len(main_items)} main items, {len(detail_items)} detail items, {len(contacts)} contacts."
            )
            return main_items, detail_items, contacts
        except Exception as e:
            self.logger.exception(f"💥 Failed to parse PO Log: {e}", exc_info=True)
            return [], [], []

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
        self.logger.info(f"🔧 [NEW BATCH LOGIC] Processing project_number={project_number} with batch queries.")
        pn_int = int(project_number)

        # region 1) Process Contacts (Optional)
        all_db_contacts = self.database_util.search_contacts()
        self.logger.info(f"✅ Retrieved {len(all_db_contacts) if isinstance(all_db_contacts, list) else 1} contacts.")

        # region 2) Fetch all existing POs for this project_number
        existing_pos = self.database_util.search_purchase_orders(
            column_names=["project_number"], values=[pn_int]
        )
        if existing_pos is None:
            existing_pos = []
        elif isinstance(existing_pos, dict):
            existing_pos = [existing_pos]

        pos_by_number = {po["po_number"]: po for po in existing_pos}
        self.logger.info(f"✅ Found {len(existing_pos)} existing POs for project_number={pn_int}.")

        # Helper function to check if PO data has changed
        def _po_has_changes(existing_po, new_data, contact_id):
            """
            Compare relevant PO fields to see if there's a difference
            between what's in the database (existing_po) and
            the incoming data (new_data).
            """
            if existing_po.get("description") != new_data.get("description"):
                return True
            if existing_po.get("po_type") != new_data.get("po_type"):
                return True
            if existing_po.get("contact_id") != contact_id:
                return True
            return False

        # region 3) Create or update each main_item
        for i, item in enumerate(main_items):
            try:
                # Skip if PO number is below debug threshold
                if self.DEBUG_STARTING_PO_NUMBER and int(item["po_number"]) < self.DEBUG_STARTING_PO_NUMBER:
                    continue

                # 3a) Find or create the contact
                contact_id = self._find_or_create_contact(item, contacts, i, all_db_contacts)

                po_number = int(item["po_number"])
                existing_po = pos_by_number.get(po_number)

                # 3b) If existing, check for changes and update if needed
                if existing_po:
                    if _po_has_changes(existing_po, item, contact_id):
                        self.logger.info(f"🔄 Updating existing PO {po_number} for project {pn_int}.")
                        updated_po = self.database_util.update_purchase_order_by_keys(
                            project_number=pn_int,
                            po_number=po_number,
                            description=item.get("description"),
                            po_type=item.get("po_type"),
                            contact_id=contact_id
                        )
                        if updated_po:
                            pos_by_number[po_number] = updated_po  # refresh our dict
                    else:
                        self.logger.debug(f"⏭ No changes detected for existing PO {po_number}. Skipping update.")
                else:
                    # 3c) Create
                    self.logger.info(f"🆕 Creating new PO {po_number} for project {pn_int}.")
                    new_po = self.database_util.create_purchase_order_by_keys(
                        project_number=pn_int,
                        po_number=po_number,
                        description=item.get("description"),
                        po_type=item.get("po_type"),
                        contact_id=contact_id
                    )
                    if new_po:
                        pos_by_number[po_number] = new_po  # add it to our dict so detail items can see it
            except Exception as ex:
                self.logger.error(
                    f"💥 Error processing PO creation/update for po_number={item.get('po_number')}: {ex}",
                    exc_info=True
                )

        # region 4) Fetch all existing DetailItems for that project_number
        existing_details = self.database_util.search_detail_items(["project_number"], [pn_int])
        if existing_details is None:
            existing_details = []
        elif isinstance(existing_details, dict):
            existing_details = [existing_details]

        # Dictionary keyed by (po_number, detail_number, line_number)
        detail_dict = {}
        for d in existing_details:
            key = (d["po_number"], d["detail_number"], d["line_number"])
            detail_dict[key] = d
        self.logger.info(f"✅ Found {len(existing_details)} existing DetailItems for project_number={pn_int}.")

        # A set of final states:
        COMPLETED_STATUSES = {"PAID", "LOGGED", "RECONCILED", "REVIEWED"}

        # Helper function to check if Detail Item data has changed
        def _detail_item_has_changes(existing_di, new_data):
            """
            Compare relevant DetailItem fields to see if there's any difference
            between what's in the database (existing_di) and the incoming data (new_data).

            **Date fields are compared by day/month/year only, ignoring time.**
            """
            from datetime import datetime

            def to_date(value):
                """
                Safely convert a string or datetime value to a date (YYYY-MM-DD).
                If parsing fails or value is None, return the raw value or None.
                """
                if not value:
                    return None
                if isinstance(value, datetime):
                    return value.date()
                if isinstance(value, str):
                    # Try ISO format first
                    try:
                        parsed_dt = datetime.fromisoformat(value)
                        return parsed_dt.date()
                    except ValueError:
                        # Fallback to a basic YYYY-MM-DD parse
                        try:
                            parsed_dt = datetime.strptime(value, "%Y-%m-%d")
                            return parsed_dt.date()
                        except:
                            # If all else fails, return the raw string
                            return value
                return value

            old_date = to_date(existing_di.get("transaction_date"))
            new_date = to_date(new_data.get("date"))
            if old_date != new_date:
                return True

            old_due = to_date(existing_di.get("due_date"))
            new_due = to_date(new_data.get("due date"))
            if old_due != new_due:
                return True

            # Compare numeric/string fields directly
            if existing_di.get("vendor") != new_data.get("vendor"):
                return True
            if existing_di.get("description") != new_data.get("description"):
                return True
            if float(existing_di.get("rate", 0)) != float(new_data.get("rate", 0)):
                return True
            if float(existing_di.get("quantity", 1)) != float(new_data.get("quantity", 1)):
                return True
            if float(existing_di.get("ot", 0)) != float(new_data.get("OT", 0)):
                return True
            if float(existing_di.get("fringes", 0)) != float(new_data.get("fringes", 0)):
                return True
            if (existing_di.get("state") or "").upper() != (new_data.get("state") or "").upper():
                return True
            if existing_di.get("account_code") != new_data.get("account"):
                return True
            if existing_di.get("payment_type") != new_data.get("payment_type"):
                return True

            return False

        # region 5) Create or update each detail_item
        for sub_item in detail_items:
            po_number = None
            detail_number = None
            line_number = None
            try:
                # Skip if PO number is below debug threshold
                if self.DEBUG_STARTING_PO_NUMBER and int(sub_item["po_number"]) < self.DEBUG_STARTING_PO_NUMBER:
                    continue

                po_number = int(sub_item["po_number"])
                detail_number = int(sub_item["detail_item_id"])
                line_number = int(sub_item["line_number"])
                key = (po_number, detail_number, line_number)
                existing_di = detail_dict.get(key)

                # If existing => update, unless in final state
                if existing_di:
                    current_state = (existing_di["state"] or "").upper()
                    if current_state in COMPLETED_STATUSES:
                        self.logger.info(
                            f"⏭ DetailItem {key} is in final state '{current_state}'. Skipping update."
                        )
                        continue  # Skip any updates

                    if _detail_item_has_changes(existing_di, sub_item):
                        self.logger.info(f"🔄 Updating existing DetailItem {key} in the database.")
                        updated_di = self.database_util.update_detail_item_by_keys(
                            project_number=pn_int,
                            po_number=po_number,
                            detail_number=detail_number,
                            line_number=line_number,
                            vendor=sub_item.get("vendor"),
                            description=sub_item.get("description"),
                            transaction_date=sub_item.get("date"),
                            due_date=sub_item.get("due date"),
                            rate=sub_item.get("rate", 0),
                            quantity=sub_item.get("quantity", 1),
                            ot=sub_item.get("OT", 0),
                            fringes=sub_item.get("fringes", 0),
                            state=sub_item["state"],
                            account_code=sub_item["account"],
                            payment_type=sub_item["payment_type"]
                        )
                        if updated_di:
                            detail_dict[key] = updated_di  # refresh local dict
                    else:
                        self.logger.debug(f"⏭ No changes detected for existing DetailItem {key}. Skipping update.")
                else:
                    # Create new detail item
                    self.logger.debug(f"🆕 Creating detail item {key}")
                    new_di = self.database_util.create_detail_item_by_keys(
                        project_number=pn_int,
                        po_number=po_number,
                        detail_number=detail_number,
                        line_number=line_number,
                        vendor=sub_item.get("vendor"),
                        description=sub_item.get("description"),
                        transaction_date=sub_item.get("date"),
                        due_date=sub_item.get("due date"),
                        rate=sub_item.get("rate", 0),
                        quantity=sub_item.get("quantity", 1),
                        ot=sub_item.get("OT", 0),
                        fringes=sub_item.get("fringes", 0),
                        state=sub_item["state"],
                        account_code=sub_item["account"],
                        payment_type=sub_item["payment_type"]
                    )
                    if new_di:
                        detail_dict[key] = new_di
            except Exception as ex:
                self.logger.error(
                    f"💥 Error processing DetailItem creation/update for PO={po_number}, "
                    f"detail={detail_number}, line={line_number}: {ex}",
                    exc_info=True
                )

        self.logger.info("✅ [NEW BATCH LOGIC] Completed batch PO & DetailItem processing.")
        return main_items

    #region PO LOG - Helper Functions
    def _find_or_create_contact(self, item, contacts, index, all_db_contacts):
        """
        Helper to match or create a contact from the 'main_items' list
        (or any data structure containing 'contact_name').
        """
        contact_name = item.get("contact_name")
        if not contact_name:
            return None

        self.logger.info(f"🔍 Checking for Contact: {contact_name}")
        contact_search = self.database_util.find_contact_close_match(contact_name, all_db_contacts)
        if contact_search:
            if isinstance(contact_search, list):
                contact_search = contact_search[0]
            contact_id = contact_search["id"]
        else:
            self.logger.info(f"🆕 Creating Contact: {contact_name}")
            new_contact = self.database_util.create_minimal_contact(contact_name)
            contact_id = new_contact["id"] if new_contact else None

        return contact_id    # endregion

    def callback_add_po_data_to_DB(self, fut):
        return fut.result()

    #endregion

    #endregion

    #region Event Processing - Invoices
    def process_invoice(self, dropbox_path: str):
        """
        Minimal 'process_invoice' that only inserts or updates an 'invoice' record
        in the DB (plus a share link). All other logic (detail item linking, sum checks,
        RTP vs. MISMATCH, etc.) is handled by the triggers in invoice_receipt_triggers.py.
        """
        self.logger.info(f"📄 Processing invoice: {dropbox_path}")
        filename = os.path.basename(dropbox_path)

        # 1) Extract project_number, po_number, invoice_number from filename
        try:
            match = re.match(r"^(\d{4})_(\d{1,2})(?:_(\d{1,2}))?", filename)
            if not match:
                self.logger.warning(f"⚠️ Invoice filename '{filename}' doesn't match the expected pattern.")
                return
            project_number_str = match.group(1)
            po_number_str = match.group(2)
            invoice_number_str = match.group(3) or "1"

            project_number = int(project_number_str)
            po_number = int(po_number_str)
            invoice_number = int(invoice_number_str)
        except Exception as e:
            self.logger.exception(f"💥 Error parsing invoice filename '{filename}': {e}", exc_info=True)
            return

        # 2) Try to get the Dropbox share link *first* so we can store it at creation/update time
        try:
            file_share_link = self.dropbox_util.get_file_link(dropbox_path)
            self.logger.info(f"🔗 Obtained Dropbox link for invoice: {file_share_link}")
        except Exception as e:
            self.logger.exception(f"💥 Error retrieving Dropbox link: {e}", exc_info=True)
            file_share_link = None

        # 3) Download file from Dropbox
        temp_file_path = f"./temp_files/{filename}"
        if not self.download_file_from_dropbox(dropbox_path, temp_file_path):
            self.logger.error(f"❌ Could not download invoice file: {dropbox_path}")
            return

        # 4) Attempt to parse out invoice fields (date, term, total) via OCR
        transaction_date, term, total = None, 30, 0.0
        try:
            self.logger.info("🔎 Scanning invoice file for data with OCRService + OpenAI...")
            extracted_text = self.ocr_service.extract_text(temp_file_path)
            info, err = self.ocr_service.extract_info_with_openai(extracted_text)
            if err or not info:
                self.logger.warning(f"❌ Could not parse invoice data from OpenAI. Using defaults. Error: {err}")
            else:
                # Date
                date_str = info.get("invoice_date")
                try:
                    if date_str:
                        transaction_date = datetime.strptime(date_str, "%Y-%m-%d")
                except (ValueError, TypeError):
                    transaction_date = None

                # Total
                total_str = info.get("total_amount")
                try:
                    total = float(total_str) if total_str else 0.0
                except (ValueError, TypeError):
                    total = 0.0

                # Term
                term_str = info.get("payment_term")
                if term_str:
                    digits_only = re.sub(r"[^0-9]", "", term_str)
                    try:
                        t_val = int(digits_only) if digits_only else 30
                        if 7 <= t_val <= 60:
                            term = t_val
                    except Exception:
                        term = 30
        except Exception as e:
            self.logger.exception(f"💥 Error extracting invoice data: {e}", exc_info=True)

        # 5) Create/update the Invoice record (including the share link) in one go
        try:
            existing_invoice = self.database_util.search_invoice_by_keys(project_number=str(project_number),
                                                                         po_number=str(po_number),
                                                                         invoice_number=str(invoice_number))
            if existing_invoice is None:
                self.logger.info(
                    f"🆕 Creating new invoice #{invoice_number} for PO {po_number} (project {project_number})."
                )
                new_invoice = self.database_util.create_invoice(
                    project_number=project_number,
                    po_number=po_number,
                    invoice_number=invoice_number,
                    transaction_date=transaction_date,
                    term=term,
                    total=total,
                    file_link=file_share_link  # Store link here at creation
                )
                invoice_id = new_invoice["id"] if new_invoice else None
            else:
                if isinstance(existing_invoice, list):
                    invoice_id = existing_invoice[0]["id"]
                else:
                    invoice_id = existing_invoice["id"]
                self.logger.info(
                    f"🔄 Updating existing invoice #{invoice_number} for PO {po_number}."
                )
                self.database_util.update_invoice(
                    invoice_id=invoice_id,
                    transaction_date=transaction_date,
                    term=term,
                    total=total,
                    file_link=file_share_link  # Update link here at the same time
                )
        except Exception as e:
            self.logger.exception(f"💥 Error creating/updating invoice in DB: {e}", exc_info=True)
            self.cleanup_temp_file(temp_file_path)
            return

        # 6) Clean up the local temp file
        self.cleanup_temp_file(temp_file_path)
        self.logger.info(f"✅ Minimal invoice processing complete for: {dropbox_path}")

    # endregion ============================




    # region Event Processing - Receipts
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
        8) # ADDED/CHANGED: After we create or update the receipt, update that detail item with the `receipt_id`.
        """

        self.logger.info(f"🧾 Processing receipt: {dropbox_path}")
        temp_file_path = f"./temp_files/{os.path.basename(dropbox_path)}"

        filename = os.path.basename(dropbox_path)

        # ---------------------------------------------
        # 1) Detect if it's petty cash or credit card
        # ---------------------------------------------
        is_petty_cash = (
            "3. Petty Cash" in dropbox_path
            or "Crew PC Folders" in dropbox_path
            or filename.startswith("PC_")
        )

        # ---------------------------------------------
        # 2) Regex pattern
        # ---------------------------------------------
        pattern = r'^(?:PC_)?(\d{4})_(\d{2})_(\d{2})\s+(.*?)\s+Receipt\.(pdf|jpe?g|png)$'
        match = re.match(pattern, filename, re.IGNORECASE)

        if not match:
            self.logger.warning(f"❌ Filename '{filename}' does not match receipt pattern.")
            return

        project_number_str = match.group(1)
        group2_str = match.group(2).lstrip("0")
        group3_str = match.group(3).lstrip("0")
        vendor_name = match.group(4)
        file_ext = match.group(5).lower()

        # ---------------------------------------------
        # 3) Determine PO #, detail #, line #
        # ---------------------------------------------
        if is_petty_cash:
            po_number_str = "1"
            detail_item_str = group2_str
            line_number_str = group3_str
        else:
            po_number_str = group2_str
            detail_item_str = group3_str
            line_number_str = "1"

        project_number = int(project_number_str)
        po_number = int(po_number_str)
        detail_number = int(detail_item_str)
        line_number_number = int(line_number_str)

        try:
            success = self.download_file_from_dropbox(dropbox_path, temp_file_path)
            if not success:
                self.logger.warning(f"🛑 Failed to download receipt: {filename}")
                return

            with open(temp_file_path, 'rb') as f:
                file_data = f.read()

            # 3) EXTRACT TEXT
            extracted_text = ""
            if file_ext == "pdf":
                extracted_text = self._extract_text_from_pdf(file_data)
                if not extracted_text.strip():
                    self.logger.info("PDF extraction found no text; falling back to OCR.")
                    extracted_text = self._extract_text_from_pdf_with_ocr(file_data)
            else:
                extracted_text = self._extract_text_via_ocr(file_data)

            parse_failed = False
            if not extracted_text.strip():
                self.logger.warning(
                    f"🛑 No text extracted from receipt: {filename}. Will mark as ISSUE but continue."
                )
                parse_failed = True

            # 4) AI-based parsing
            ocr_service = OCRService()
            receipt_info = {}

            if not parse_failed:
                receipt_info = ocr_service.extract_receipt_info_with_openai(extracted_text)
                if not receipt_info:
                    self.logger.warning(f"🛑 Could not parse receipt info from AI for: {filename} - marking as ISSUE.")
                    parse_failed = True
            else:
                self.logger.warning(f"Skipping AI parsing because no text was extracted for {filename}.")
                receipt_info = {}

            if parse_failed or not receipt_info:
                receipt_info = {
                    "total_amount": 0.0,
                    "description": "Could not parse",
                    "date": None
                }

            total_amount = receipt_info.get("total_amount", 0.0)
            purchase_date = receipt_info.get("date", "")
            try:
                datetime.strptime(purchase_date, "%Y-%m-%d")
            except (ValueError, TypeError):
                purchase_date = None

            short_description = receipt_info.get("description", "")

            # 5) Generate Dropbox share link
            try:
                shared_link_metadata = self.dropbox_util.get_file_link(dropbox_path)
                file_link = shared_link_metadata.replace("?dl=0", "?dl=1")
            except Exception as e:
                self.logger.warning(f"❌ Could not generate shareable link for {dropbox_path}: {e}")
                file_link = None

            # 6) Look up detail item
            existing_detail = self.database_util.search_detail_item_by_keys(
                project_number=str(project_number),
                po_number=po_number,
                detail_number=detail_number,
                line_number=line_number_number
            )

            if not existing_detail:
                self.logger.warning(
                    f"❗ No matching detail item found for project={project_number}, "
                    f"PO={po_number}, detail={detail_number}, line={line_number_number}."
                )
                self.cleanup_temp_file(temp_file_path)
                return
            else:
                if isinstance(existing_detail, list):
                    existing_detail = existing_detail[0]

            # 6A) Create or update a receipt row
            spend_money_id = 1  # example placeholder

            existing_receipts = self.database_util.search_receipts(
                ["project_number", "po_number", "detail_number", "line_number"],
                [project_number, po_number, detail_number, line_number_number]
            )

            if not existing_receipts:
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
                receipt_id = new_receipt["id"] if new_receipt else None
                self.logger.info(
                    f"🔄 Created new receipt with ID={receipt_id} for detail_number={detail_number}"
                )
            else:
                existing_receipt = existing_receipts[0] if isinstance(existing_receipts, list) else existing_receipts
                receipt_id = existing_receipt["id"]
                updated_receipt = self.database_util.update_receipt_by_keys(
                    project_number=project_number,
                    po_number=po_number,
                    detail_number=detail_number,
                    line_number=line_number_number,
                    total=total_amount,
                    purchase_date=purchase_date,
                    receipt_description=short_description,
                    file_link=file_link
                )
                self.logger.info(
                    f"✏️ Updated existing receipt with ID={receipt_id} for detail_number={detail_number}"
                )

            # 7) Determine detail_item state
            state = "PENDING"
            detail_subtotal = existing_detail.get("sub_total", 0.0)
            if not existing_detail["state"] == "RECONCILED":
                if parse_failed:
                    state = "ISSUE"
                    self.logger.info(f"Marking this receipt as ISSUE because parsing failed for {filename}.")
                else:
                    try:
                        if float(total_amount) == float(detail_subtotal):
                            state = "REVIEWED"
                            self.logger.info(
                                "Receipt total matches the detail item subtotal. Setting state to REVIEWED."
                            )
                        else:
                            self.logger.info(
                                "Receipt total does not match the detail item subtotal. Setting state to PO MISMATCH."
                            )
                            state = "PO MISMATCH"
                    except Exception as e:
                        state = "ISSUE"
            else:
                state = "RECONCILED"

            # # ADDED/CHANGED: Update detail item with new state AND the new receipt_id
            self.database_util.update_detail_item_by_keys(
                project_number=project_number,
                po_number=po_number,
                detail_number=detail_number,
                line_number=line_number_number,
                state=state,
                receipt_id=receipt_id
            )
            self.cleanup_temp_file(temp_file_path)
            self.logger.info(f"✅ Receipt succesfully captured: {dropbox_path}")

        except Exception as e:
            self.logger.exception(f"💥 Error processing receipt {filename}: {e}", exc_info=True)
            return

    # endregion



    # region Event Processing - Tax Form
    def process_tax_form(self, dropbox_path: str):
        """
        Stub function for processing a tax form from Dropbox.
        Could perform OCR, fill out contact tax_form_link, etc.
        """
        self.logger.info(f"🗂 Processing Tax Form: {dropbox_path}")
        # ...
        pass

    # endregion



    # region Event Processing - PO Log - Step 2 -  Add Folder & Tax Links + Contact Data
    def folder_tax_conact_trigger(self, fut):
        """
        Callback for when the DB process is complete.
        Possibly triggers Monday or Dropbox tasks next.
        """
        try:
            processed_items = fut.result()

            # region GET DROPBOX FOLDER LINK
            if self.GET_FOLDER_LINKS:
                self.logger.info("SYNCING PO FOLDER LINKS")

                def get_folder_links(processed_items):
                    for item in processed_items:
                        db_project = self.database_util.search_projects(["project_number"], [item["project_number"]])
                        if isinstance(db_project, list):
                            db_project = db_project[0]
                        db_po = self.database_util.search_purchase_orders(["project_id", "po_number"],
                                                                          [db_project["id"], item["po_number"]])
                        if isinstance(db_po, list):
                            db_po = db_po[0]
                        if db_po["folder_link"]:
                            self.logger.debug(
                                f"✅Folder Link is already present for {db_project['project_number']}_{db_po['po_number']}")
                        else:
                            self.logger.debug(f"Folder link not found in DB -- retrieving from Dropbox")
                            self.update_po_folder_link(item["project_number"], item["po_number"])

                folder_links_future = self.executor.submit(get_folder_links, processed_items)
            # endregion

            # region TAX LINKS
            if self.GET_TAX_LINKS:
                def get_tax_links(processed_items):
                    for idx, item in enumerate(processed_items):
                        db_contact = self.database_util.search_contacts(["id"], [item.get("contact_id")])
                        if db_contact and db_contact["vendor_type"] == "Vendor":
                            tax_link = db_contact["tax_form_link"]
                            if not tax_link:
                                self.logger.debug(
                                    f"Database is missing the tax form link for Contact {db_contact['id']}. "
                                    f"Fetching a new link from Dropbox..."
                                )
                                tax_link = self.update_po_tax_form_links(
                                    item["project_number"],
                                    item["po_number"]
                                )
                                if tax_link:
                                    self.monday_api.update_monday_tax_form_link(db_contact["pulse_id"], tax_link)
                                else:
                                    self.logger.error("❌ - Unable to get link from contact")
                            else:
                                self.logger.debug(
                                    f"✅ Tax Link is already present for {db_contact['name']}")

                tax_links_future = self.executor.submit(get_tax_links, processed_items)
            # endregion

            futures_to_wait = []

            if self.GET_FOLDER_LINKS and 'folder_links_future' in locals():
                futures_to_wait.append(folder_links_future)
            if self.GET_TAX_LINKS and 'tax_links_future' in locals():
                futures_to_wait.append(tax_links_future)

            # Block until both tasks are done
            if futures_to_wait:
                wait(futures_to_wait, return_when=ALL_COMPLETED)

            # Now that both have finished, we can call create_pos_in_monday
            if processed_items:
                project_number = processed_items[0].get("project_number")
                if project_number:
                    self.logger.info(
                        f"Both folder links and contact sync done. Now creating POs in Monday for project_id={project_number}."
                    )
                    if self.ADD_PO_TO_MONDAY:
                        self.create_pos_in_monday(int(project_number))
                    else:
                        self.logger.info("SKIPPING MONDAY PO CREATION")
                else:
                    self.logger.warning("❌ Could not determine project_id from processed_items.")
            else:
                self.logger.warning("❌ processed_items is empty, skipping create_pos_in_monday.")

        except Exception as e:
            self.logger.error(f"❌ after_db_done encountered an error: {e}", exc_info=True)

    # region   DROPBOX FOLDER LINK
    def update_po_folder_link(self, project_number, po_number):
        logger = self.logger
        logger.info(f"🚀Finding folder link for PO: {project_number}_{str(po_number).zfill(2)}")

        try:
            po_data = self.database_util.search_purchase_order_by_keys(project_number, po_number)
            if not po_data or not len(po_data) > 0:
                logger.warning(
                    f"❌ No PO data returned for PO={project_number}_{str(po_number).zfill(2)}, aborting link updates.")
                return
            po_data = po_data
            if po_data["folder_link"]:
                logger.debug("Link already present: skipping")
                return

            project_item = dropbox_api.get_project_po_folders_with_link(project_number=project_number,
                                                                        po_number=po_number)
            if not project_item or len(project_item) < 1:
                logger.warning(
                    f"⚠️ Could not determine project folder name for {project_number}, no links will be found.")
                return
            project_item = project_item[0]
            po_folder_link = project_item["po_folder_link"]
            po_folder_name = project_item["po_folder_name"]
            logger.debug(f"Project folder name retrieved: '{po_folder_name}'")
            logger.debug(f"Link update for PO: {po_folder_name}', ")

            if po_folder_link:
                logger.info(f"✅ Folder link found")
                self.database_util.update_purchase_order(po_id=po_data["id"], folder_link=po_folder_link)
            else:
                logger.warning("⚠️ No folder link found.")

            logger.info(f"🎉 Completed folder linking for: {project_number}_{str(po_number).zfill(2)}")

        except Exception as e:
            logger.error("💥 Error linking dropbox folder:", exc_info=True)
            traceback.print_exc()

    # endregion

    # region   TAX FORM LINK
    def update_po_tax_form_links(self, project_number, po_number):
        """
        🚀 Update or set the tax_form_link for a PurchaseOrder in Dropbox if needed.
        Stub method to illustrate how you'd update the 'tax_form_link' column.
        """
        try:
            po_search = self.database_util.search_purchase_order_by_keys(project_number, po_number)
            if not po_search or not po_search["po_type"] == "INV":
                return None
            if isinstance(po_search, dict):
                contact_id = po_search["contact_id"]
            elif isinstance(po_search, list) and po_search:
                contact_id = po_search[0]["contact_id"]
            else:
                return None

            new_tax_form_link = \
                self.dropbox_api.get_po_tax_form_link(project_number=project_number, po_number=po_number)[0][
                    "po_tax_form_link"
                ]
            self.database_util.update_contact(contact_id, tax_form_link=new_tax_form_link)
            self.logger.info(f"📑 Updated tax form link for PO {project_number}_{po_number} => {new_tax_form_link}")
            return new_tax_form_link
        except Exception as e:
            self.logger.error(f"💥 Could not update PO tax form link for {project_number}_{po_number}: {e}",
                              exc_info=True)

    # endregion
    # endregion

    # region Event Processing - PO Log - Step 3 - Monday Processing
    def create_pos_in_monday(self, project_number):
        """
        Demonstrates how to fetch all subitems once from Monday,
        then process them locally to avoid multiple queries.
        """
        self.logger.info("🌐 Processing PO data in Monday.com...")

        # -------------------------------------------------------------------------
        # 1) FETCH MAIN ITEMS FROM MONDAY & FROM DB
        # -------------------------------------------------------------------------
        monday_items = self.monday_api.get_items_in_project(project_id=project_number)
        processed_items = self.database_util.search_purchase_order_by_keys(project_number=project_number)

        # Build a map for main items
        monday_items_map = {}
        for mi in monday_items:
            pid = mi["column_values"].get(self.monday_util.PO_PROJECT_ID_COLUMN)["text"]
            pono = mi["column_values"].get(self.monday_util.PO_NUMBER_COLUMN)["text"]
            if pid and pono:
                monday_items_map[(int(pid), int(pono))] = mi

        # -------------------------------------------------------------------------
        # 2) FETCH ALL SUBITEMS AT ONCE (FOR ENTIRE SUBITEM BOARD)
        # -------------------------------------------------------------------------
        all_subitems = self.monday_api.get_subitems_in_board(project_number=project_number)

        # -------------------------------------------------------------------------
        # 3) BUILD A GLOBAL DICTIONARY FOR SUBITEM LOOKUP
        #    Keyed by (project_number, po_number, detail_number, line_number)
        # -------------------------------------------------------------------------
        global_subitem_map = {}
        for msub in all_subitems:
            identifiers = self.monday_util.extract_subitem_identifiers(msub)
            if identifiers is not None:
                global_subitem_map[identifiers] = msub

        # -------------------------------------------------------------------------
        # 4) DETERMINE WHICH MAIN ITEMS NEED CREATION/UPDATE
        # -------------------------------------------------------------------------
        items_to_create = []
        items_to_update = []

        for db_item in processed_items:
            contact_item = self.database_util.search_contacts(['id'], [db_item["contact_id"]])
            db_item["contact_pulse_id"] = contact_item["pulse_id"]
            db_item["contact_name"] = contact_item["name"]
            db_item["project_number"] = project_number

            p_id = project_number
            po_no = int(db_item["po_number"])

            column_values_str = self.monday_util.po_column_values_formatter(
                project_id=str(project_number),
                po_number=db_item["po_number"],
                description=db_item.get("description"),
                contact_pulse_id=db_item["contact_pulse_id"],
                folder_link=db_item.get("folder_link"),
                producer_id=None,
                name=db_item["contact_name"]
            )
            new_vals = json.loads(column_values_str)

            key = (p_id, po_no)
            if key in monday_items_map:
                monday_item = monday_items_map[key]
                differences = self.monday_util.is_main_item_different(db_item, monday_item)
                if differences:
                    self.logger.debug(f"Item differs for PO {po_no}. Differences: {differences}")
                    items_to_update.append({
                        "db_item": db_item,
                        "column_values": new_vals,
                        "monday_item_id": monday_item["id"]
                    })
                else:
                    self.logger.debug(f"No changes for PO {po_no}, skipping update.")
            else:
                items_to_create.append({
                    "db_item": db_item,
                    "column_values": new_vals,
                    "monday_item_id": None
                })

        # -------------------------------------------------------------------------
        # 5) CREATE/UPDATE MAIN ITEMS
        # -------------------------------------------------------------------------
        if items_to_create:
            self.logger.info(f"🆕 Need to create {len(items_to_create)} main items on Monday.")
            created_mapping = self.monday_api.batch_create_or_update_items(
                items_to_create, project_id=project_number, create=True
            )
            for itm in created_mapping:
                db_item = itm["db_item"]
                monday_item_id = itm["monday_item_id"]
                self.database_util.update_purchase_order(db_item["id"], pulse_id=monday_item_id)
                db_item["pulse_id"] = monday_item_id
                p = project_number
                po = int(db_item["po_number"])
                monday_items_map[(p, po)] = {
                    "id": monday_item_id,
                    "name": f"PO #{po}",
                    "column_values": itm["column_values"]
                }

        if items_to_update:
            self.logger.info(f"✏️ Need to update {len(items_to_update)} main items on Monday.")
            updated_mapping = self.monday_api.batch_create_or_update_items(
                items_to_update, project_id=project_number, create=False
            )
            for itm in updated_mapping:
                db_item = itm["db_item"]
                monday_item_id = itm["monday_item_id"]
                self.database_util.update_purchase_order_by_keys(
                    project_number, db_item["po_number"], pulse_id=monday_item_id
                )
                db_item["pulse_id"] = monday_item_id
                p = project_number
                po = int(db_item["po_number"])
                monday_items_map[(p, po)]["column_values"] = itm["column_values"]

        # Ensure all main items have pulse_ids
        for db_item in processed_items:
            p_id = project_number
            po_no = int(db_item["po_number"])
            main_monday_item = monday_items_map.get((p_id, po_no))
            if main_monday_item and not db_item.get("pulse_id"):
                monday_item_id = main_monday_item["id"]
                updated = self.database_util.update_purchase_order_by_keys(
                    project_number, db_item["po_number"], pulse_id=monday_item_id
                )
                if updated:
                    db_item["pulse_id"] = monday_item_id
                    self.logger.info(f"🗂 PO {po_no} now has pulse_id {monday_item_id} in DB")

        # -------------------------------------------------------------------------
        # 6) CREATE/UPDATE SUBITEMS
        # -------------------------------------------------------------------------
        for db_item in processed_items:
            p_id = project_number
            po_no = int(db_item["po_number"])
            main_monday_item = monday_items_map.get((p_id, po_no))
            if not main_monday_item:
                self.logger.warning(f"❌ No Monday main item found for PO {po_no}, skipping subitems.")
                continue

            main_monday_id = main_monday_item["id"]
            sub_items_db = self.database_util.search_detail_item_by_keys(project_number, db_item["po_number"])

            if isinstance(sub_items_db, dict):
                sub_items_db = [sub_items_db]

            subitems_to_create = []
            subitems_to_update = []
            if not sub_items_db:
                pass

            for sdb in sub_items_db:
                # Attempt to look up an account code if present
                if sdb.get("account_code_id"):
                    account_row = self.database_util.search_account_codes(["id"], [sdb["account_code_id"]])
                    sdbcode = account_row["code"] if account_row else None
                else:
                    sdb["account_code"] = None

                # --------------------------
                # # CHANGED or ADDED:
                # Always see if there's a receipt or invoice link we can attach
                # --------------------------
                file_link_for_subitem = ""
                if db_item["po_type"] in ["PC", "CC"]:
                    receipt_id = sdb.get("receipt_id")
                    if receipt_id:
                        existing_receipt = self.database_util.search_receipts(["id"], [receipt_id])
                        if existing_receipt and existing_receipt.get("file_link"):
                            file_link_for_subitem = existing_receipt["file_link"]
                    else:
                        # Possibly there's a newly discovered receipt:
                        existing_receipts = self.database_util.search_receipts(
                            ["project_number", "po_number", "detail_number", "line_number"],
                            [project_number, int(po_no), sdb["detail_number"], sdb["line_number"]]
                        )
                        if existing_receipts:
                            first_receipt = existing_receipts[0] if isinstance(existing_receipts, list) else existing_receipts
                            file_link_for_subitem = first_receipt.get("file_link", "")
                            rid = first_receipt.get("id")
                            # update DB detail_item with that receipt_id
                            self.database_util.update_detail_item_by_keys(
                                project_number=p_id,
                                po_number=db_item["po_number"],
                                detail_number=sdb["detail_number"],
                                line_number=sdb["line_number"],
                                receipt_id=rid
                            )
                            sdb["receipt_id"] = rid
                elif db_item["po_type"] in ["INV", "PROJ"]:
                    # Possibly there's a newly discovered invoice:
                    invoice_id = sdb.get("invoice_id")
                    if invoice_id:
                        existing_invoice = self.database_util.search_invoices(["id"], [invoice_id])
                        if existing_invoice and existing_invoice.get("file_link"):
                            file_link_for_subitem = existing_invoice["file_link"]
                    else:
                        # If this detail item doesn't have an invoice_id yet, let's look it up
                        existing_invoices = self.database_util.search_invoices(
                            ["project_number", "po_number", "invoice_number", ],
                            [project_number, int(po_no), sdb["detail_number"]]
                        )
                        if existing_invoices:
                            first_invoice = existing_invoices[0] if isinstance(existing_invoices, list) else existing_invoices
                            file_link_for_subitem = first_invoice.get("file_link", "")
                            inv_id = first_invoice.get("id")
                            # Update DB detail_item with that invoice_id
                            self.database_util.update_detail_item_by_keys(
                                project_number=p_id,
                                po_number= str(po_no),
                                detail_number=sdb["detail_number"],
                                line_number=sdb["line_number"],
                                invoice_id=inv_id
                            )
                            sdb["invoice_id"] = inv_id

                # Build the subitem column values
                sub_col_values_str = self.monday_util.subitem_column_values_formatter(
                    project_id=project_number,
                    po_number=db_item["po_number"],
                    detail_number=sdb["detail_number"],
                    line_number=sdb["line_number"],
                    status=sdb.get("state"),
                    description=sdb.get("description"),
                    quantity=sdb.get("quantity"),
                    rate=sdb.get("rate"),
                    date=sdb.get("transaction_date"),
                    due_date=sdb.get("due_date"),
                    account_number=sdb["account_code"],
                    link=file_link_for_subitem,
                    OT=sdb.get("ot"),
                    fringes=sdb.get("fringes")
                )
                new_sub_vals = json.loads(sub_col_values_str)

                sub_key = (project_number, db_item["po_number"], sdb["detail_number"], sdb["line_number"])

                if sub_key in global_subitem_map:
                    # Possibly update
                    msub = global_subitem_map[sub_key]

                    # Tag extra info
                    sdb["project_number"] = db_item["project_number"]
                    sdb["po_number"] = db_item["po_number"]
                    sdb["file_link"] = file_link_for_subitem
                    payment_type = db_item["po_type"]

                    # Compare local data vs Monday subitem
                    differences = self.monday_util.is_sub_item_different(sdb, msub)

                    if differences:
                        self.logger.debug(
                            f"Sub-item differs for detail #{sdb['detail_number']} (PO {po_no}). {differences}"
                        )
                        subitems_to_update.append({
                            "db_sub_item": sdb,
                            "column_values": new_sub_vals,
                            "parent_id": main_monday_id,
                            "monday_item_id": msub["id"]
                        })
                    else:
                        # -------------------------------------
                        # NO COLUMN CHANGES => CHECK sdb PULSE
                        # -------------------------------------
                        self.logger.debug(
                            f"No changes for detail #{sdb['detail_number']} (PO {po_no})."
                        )
                        sub_pulse_id = msub["id"]

                        # Only update DB if sdb doesn't have the correct pulse_id/parent_pulse_id
                        current_pulse_id = sdb.get("pulse_id")
                        current_parent_id = sdb.get("parent_pulse_id")

                        if current_pulse_id != int(sub_pulse_id) or current_parent_id != int(main_monday_id):
                            self.logger.debug(
                                f"Pulse mismatch for detail #{sdb['detail_number']} (PO {po_no}). "
                                f"DB has pulse_id={current_pulse_id}, parent_pulse_id={current_parent_id}; "
                                f"Monday is {sub_pulse_id}, {main_monday_id}."
                            )
                            self.database_util.update_detail_item_by_keys(
                                project_number=project_number,
                                po_number=db_item["po_number"],
                                detail_number=sdb["detail_number"],
                                line_number=sdb["line_number"],
                                pulse_id=sub_pulse_id,
                                parent_pulse_id=main_monday_id
                            )
                            sdb["pulse_id"] = sub_pulse_id
                            sdb["parent_pulse_id"] = main_monday_id
                        else:
                            self.logger.debug(
                                f"Pulse IDs already match for detail #{sdb['detail_number']} (PO {po_no}), no DB update needed."
                            )
                else:
                    # Need to create
                    subitems_to_create.append({
                        "db_sub_item": sdb,
                        "column_values": new_sub_vals,
                        "parent_id": main_monday_id
                    })

            # -----------------------
            # SUB-ITEM CREATE (BATCH)
            # -----------------------
            if subitems_to_create:
                self.logger.info(f"🆕 Need to create {len(subitems_to_create)} sub-items for PO {po_no}.")
                self._batch_create_subitems(subitems_to_create, main_monday_id, project_number, db_item)

            # -----------------------
            # SUB-ITEM UPDATE (BATCH)
            # -----------------------
            if subitems_to_update:
                self.logger.info(f"✏️ Need to update {len(subitems_to_update)} sub-items for PO {po_no}.")
                self._batch_update_subitems(subitems_to_update, main_monday_id, project_number, db_item)

        self.logger.info("✅ Monday.com processing of PO data complete.")
        return processed_items

    def _batch_create_subitems(self, subitems_to_create, parent_item_id, project_number, db_item):
        """
        Creates subitems in chunks, then updates DB with the new subitem IDs.
        """
        from concurrent.futures import ThreadPoolExecutor, as_completed

        chunk_size = 10
        create_chunks = [
            subitems_to_create[i: i + chunk_size]
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
                    self.logger.debug(f"Subitems create-chunk #{idx + 1} completed.")
                    all_created_subs.extend(chunk_result)
                except Exception as e:
                    self.logger.exception(f"❌ Error creating subitems in chunk {idx + 1}: {e}")
                    raise

        # Update DB with newly-created subitem IDs
        for csub in all_created_subs:
            db_sub_item = csub["db_sub_item"]
            monday_subitem_id = csub["monday_item_id"]

            self.database_util.update_detail_item_by_keys(
                project_number,
                db_item["po_number"],
                db_sub_item["detail_number"],
                db_sub_item["line_number"],
                pulse_id=monday_subitem_id,
                parent_pulse_id=parent_item_id
            )
            db_sub_item["pulse_id"] = monday_subitem_id
            db_sub_item["parent_pulse_id"] = parent_item_id

    def _batch_update_subitems(self, subitems_to_update, parent_item_id, project_number, db_item):
        """
        Updates subitems in chunks, then updates DB with any new data (e.g., if we changed the link).
        """
        from concurrent.futures import ThreadPoolExecutor, as_completed

        chunk_size = 10
        update_chunks = [
            subitems_to_update[i: i + chunk_size]
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
                    self.logger.debug(f"Subitems update-chunk #{idx + 1} completed.")
                    all_updated_subs.extend(chunk_result)
                except Exception as e:
                    self.logger.exception(f"❌ Error updating subitems in chunk {idx + 1}: {e}")
                    raise

        # Update DB with newly-updated subitem data
        for usub in all_updated_subs:
            db_sub_item = usub["db_sub_item"]
            monday_subitem_id = usub["monday_item_id"]

            self.database_util.update_detail_item_by_keys(
                project_number,
                db_item["po_number"],
                db_sub_item["detail_number"],
                db_sub_item["line_number"],
                pulse_id=monday_subitem_id,
                parent_pulse_id=parent_item_id
            )
            db_sub_item["pulse_id"] = monday_subitem_id
            db_sub_item["parent_pulse_id"] = parent_item_id

    # endregion





    # region Helper Methods

    def download_file_from_dropbox(self, path: str, temp_file_path: str) -> bool:
        """
        Download a file from Dropbox to a local temp_file_path.
        """
        try:
            dbx = self.dropbox_client.dbx
            _, res = dbx.files_download(path)
            file_content = res.content
            with open(temp_file_path, 'wb') as temp_file:
                temp_file.write(file_content)
            self.logger.info(f"📂 File saved to {temp_file_path}")
            return True
        except Exception as e:
            self.logger.exception(f"💥 Failed to download file: {e}", exc_info=True)
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
            self.logger.warning(f"⚠️ Could not parse tax number '{tax_str}' as integer after removing hyphens.")
            return None

    def cleanup_temp_file(self, temp_file_path: str):
        """
        Attempt to remove a temporary file.
        """
        try:
            if os.path.exists(temp_file_path):
                os.remove(temp_file_path)
                self.logger.info(f"🧹 Cleaned up temp file {temp_file_path}")
        except Exception as e:
            self.logger.warning(f"⚠️ Could not remove temp file: {e}")

    def extract_project_number(self, file_name: str) -> str:
        digit_sequences = re.findall(r'\d+', file_name)

        if not digit_sequences:
            raise ValueError(f"❗ No digits found in file name: '{file_name}' ❗")

        all_digits = ''.join(digit_sequences)

        if len(all_digits) < 4:
            raise ValueError(f"❗ File name '{file_name}' does not contain at least four digits for project_id. ❗")

        project_number = all_digits[:4]
        return project_number

    def _extract_text_from_pdf(self, file_data: bytes) -> str:
        import PyPDF2
        import fitz
        from io import BytesIO
        from PIL import Image

        Image.MAX_IMAGE_PIXELS = 200_000_000

        self.logger.debug(f"_extract_text_from_pdf: PDF data length={len(file_data)} bytes.")

        try:
            pdf_reader = PyPDF2.PdfReader(BytesIO(file_data))
            self.logger.debug(f"PyPDF2 sees {len(pdf_reader.pages)} page(s).")

            text_chunks = []
            for idx, page in enumerate(pdf_reader.pages, start=1):
                page_text = page.extract_text() or ""
                if page_text:
                    self.logger.debug(f"PyPDF2 page {idx}: extracted {len(page_text)} chars.")
                    text_chunks.append(page_text)
                else:
                    self.logger.debug(f"PyPDF2 page {idx}: no text found.")

            extracted_text = "\n".join(text_chunks)
            self.logger.debug(f"PyPDF2 total text length={len(extracted_text)}.")

            if len(extracted_text.strip()) < 20:
                self.logger.info(
                    f"PyPDF2 extracted fewer than 20 chars ({len(extracted_text.strip())}), "
                    f"treating as no text found."
                )
                extracted_text = ""

            if extracted_text.strip():
                return extracted_text

            # Attempt embedded images with PyMuPDF if normal text is insufficient
            self.logger.info("No or insufficient text from PyPDF2; extracting images via PyMuPDF...")
            pdf_document = fitz.open(stream=file_data, filetype="pdf")
            embedded_ocr_results = []

            for page_idx in range(pdf_document.page_count):
                page = pdf_document[page_idx]
                images = page.get_images(full=True)
                self.logger.debug(f"Page {page_idx + 1}: found {len(images)} images.")
                for img_ix, img_info in enumerate(images, start=1):
                    xref = img_info[0]
                    base_image = pdf_document.extract_image(xref)
                    image_data = base_image["image"]
                    try:
                        pil_image = Image.open(BytesIO(image_data)).convert("RGB")
                        # OCR the image data
                        text_in_image = self._extract_text_via_ocr(image_data)
                        self.logger.debug(f"Image {img_ix}: OCR extracted {len(text_in_image)} chars.")
                        embedded_ocr_results.append(text_in_image)
                    except Exception as e:
                        self.logger.warning(f"Could not OCR an embedded PDF image: {e}")

            fallback_text = "\n".join(embedded_ocr_results)
            self.logger.info(f"PyMuPDF fallback extracted {len(fallback_text)} chars in total.")
            return fallback_text

        except Exception as e:
            self.logger.warning(f"Could not parse PDF with PyPDF2 or PyMuPDF: {e}")
            return ""

    def _extract_text_via_ocr(self, file_data: bytes) -> str:
        try:
            return self.ocr_service.extract_text_from_receipt(file_data)
        except Exception as e:
            self.logger.warning(f"OCR extraction failed: {e}")
            return ""
        # endregion

    def scan_project_receipts(self, project_number: str):
        """
        Scans both credit-card/vendor receipt folders (under 1. Purchase Orders)
        and petty-cash receipt folders (under 3. Petty Cash/1. Crew PC Folders)
        for the specified project_number, then processes matching receipts.
        """
        self.logger.info(f"🔎 Starting receipt scan for project_number={project_number}")

        # 1) Locate the *exact* project folder path under '2024' that contains the text "project_number"
        #    Example folder name might be "2416 - Whop Keynote"
        project_folder_path = self.dropbox_api.find_project_folder(project_number, namespace="2024")
        if not project_folder_path:
            self.logger.warning(f"❌ Could not find a matching project folder for '{project_number}' under 2024.")
            return

        self.logger.info(f"📂 Project folder resolved: {project_folder_path}")

        # 2) Paths to check (relative to the found project folder):
        #    - '1. Purchase Orders' for vendor/credit card receipts
        #    - '3. Petty Cash/1. Crew PC Folders' for petty cash receipts
        #    We will search each subfolder recursively for matching files.

        purchase_orders_path = f"{project_folder_path}/1. Purchase Orders"
        petty_cash_path = f"{project_folder_path}/3. Petty Cash/1. Crew PC Folders"

        # 3) Recursively scan both directories:
        #    We'll gather all 'receipt' files from vendor subfolders and petty-cash subfolders, then process them.
        self._scan_and_process_receipts_in_folder(purchase_orders_path, project_number)
        self._scan_and_process_receipts_in_folder(petty_cash_path, project_number)

        self.logger.info(f"✅ Finished scanning receipts for project_number={project_number}.")

    def _scan_and_process_receipts_in_folder(self, folder_path: str, project_number: str):
        """
        Recursively scans the given folder_path and its subfolders, and whenever
        it finds a file that looks like a 'receipt' (based on your naming pattern),
        calls process_receipt(...).
        """
        # 1) List all items (files + folders) in the current folder
        entries = self._list_folder_recursive(folder_path)
        if not entries:
            self.logger.debug(f"📂 No entries found under '{folder_path}'")
            return

        # 2) For each file, see if it matches your existing naming pattern:
        #    e.g. "2416_04_03 Some Vendor Receipt.pdf" OR "PC_2416_04_03 Some Vendor Receipt.png"
        for entry in entries:
            # The entry is a dict: { "is_folder": bool, "path_lower": "....", "name": "filename", ... }
            if entry["is_folder"]:
                continue  # We’re already recursing, so skip subfolders here.

            dropbox_path = entry["path_display"]
            file_name = entry["name"]

            # If the file name includes "receipt" and presumably matches your patterns,
            # you can do a more precise check or just pass to process_receipt() which
            # already does the final matching via regex in your code.
            if re.search(self.RECEIPT_REGEX, file_name, re.IGNORECASE):
                # For example: 2416_04_03 VendorName Receipt.pdf or PC_2416_04_03 VendorName Receipt.pdf
                self.logger.debug(f"🧾 Potential receipt found: {dropbox_path}")
                self.process_receipt(dropbox_path)
            else:
                # Not a receipt
                pass

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
            # Initial request
            res = dbx.files_list_folder(folder_path, recursive=True)

            # Accumulate results
            entries = res.entries
            while res.has_more:
                res = dbx.files_list_folder_continue(res.cursor)
                entries.extend(res.entries)

            # Convert each entry to a dict
            for e in entries:
                if isinstance(e, files.FolderMetadata):
                    results.append({
                        "name": e.name,
                        "path_lower": e.path_lower,
                        "path_display": e.path_display,
                        "is_folder": True
                    })
                elif isinstance(e, files.FileMetadata):
                    results.append({
                        "name": e.name,
                        "path_lower": e.path_lower,
                        "path_display": e.path_display,
                        "is_folder": False
                    })
        except Exception as ex:
            self.logger.warning(f"⚠️ Could not list folder recursively: {folder_path}, Error: {ex}")

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
            if entry["is_folder"]:
                # skip deeper subfolders, or optionally you could handle them
                continue

            dropbox_path = entry["path_display"]
            file_name = entry["name"]

            # If the file name includes "invoice" or matches your known pattern,
            # we pass it to process_invoice.
            # (Your process_invoice code also has a pattern for e.g. ^(\d{4})_(\d+)(?:_(\d+))?
            # so it won't do anything if it doesn't match.)
            if re.search(self.INVOICE_REGEX, file_name, re.IGNORECASE):
                self.logger.debug(f"📄 Potential invoice found in {folder_path}: {dropbox_path}")
                self.process_invoice(dropbox_path)


    # endregion
dropbox_service = DropboxService()