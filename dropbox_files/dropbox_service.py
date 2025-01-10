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
                return self.process_budget(path)

            # No recognized type
            self.logger.debug(f"❌ Unsupported file type: {filename}")
            return None

        except Exception as e:
            self.logger.exception(f"💥 Error determining file type for {filename}: {e}", exc_info=True)
            return None

    # endregion

    # region Event Processing - Budget
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

    # endregion

    # region  Event Processing - PO Log - Step 1 -  Add PO LOG data to DB
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
        db_future = self.executor.submit(self.add_po_data_to_db, main_items, detail_items, contacts)
        db_future.add_done_callback(self.callback_add_po_data_to_DB)

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

    def add_po_data_to_db(self, main_items, detail_items, contacts):
        """
        🚀 DB Processing Method
        ----------------------
        1) For each main_item, find or create the Contact
        2) Convert project_number -> real project.id
        3) Find/create the PurchaseOrder with the real project_number
        4) Create or update the DetailItems
        5) If there's a matching receipt, store receipt_id in the detail item
           and ensure we can propagate the link to Monday subitems.
        6) # region INVOICE CHECK AFTER DETAIL CREATION (NEW)
           After all detail items are processed, we group them by invoice and
           compare the sum of their sub_totals to the invoice.total. If they match,
           and none of these detail items is OVERDUE/RECONCILED/PAID, set them to RTP;
           otherwise, set PO MISMATCH (again skipping any that are already
           OVERDUE/RECONCILED/PAID).
        """
        self.logger.info("🔧 Processing PO data in the database...")

        processed_items = []
        all_db_contacts = self.database_util.search_contacts()

        # --- 1) PROCESS MAIN ITEMS (Purchase Orders) ---
        for i, item in enumerate(main_items):
            try:
                # 1) Debug skip check
                if self.DEBUG_STARTING_PO_NUMBER and int(item["po_number"]) < self.DEBUG_STARTING_PO_NUMBER:
                    self.logger.info(f"⏭ Skipping PO '{item['po_number']}' due to debug start number.")
                    continue

                # 2) Find or create Contact
                contact_name = item["contact_name"]
                self.logger.info(f"🔍 Checking for Contact: {contact_name}")

                contact_search = self.database_util.find_contact_close_match(contact_name, all_db_contacts)
                if contact_search:
                    if isinstance(contact_search, list):
                        contact_search = contact_search[0]
                    contact_id = contact_search["id"]
                    self.logger.info(f"🔄 Updating existing Contact id={contact_id}")
                    self.database_util.update_contact(
                        contact_id,
                        vendor_type=contacts[i].get("vendor_type", "Vendor"),
                    )
                else:
                    self.logger.info(f"🆕 Creating Contact: {contact_name}")
                    new_contact = self.database_util.create_minimal_contact(contact_name)
                    contact_id = new_contact["id"] if new_contact else None
                if not contact_id:
                    self.logger.warning(
                        f"⚠️ Could not determine contact_id for contact '{contact_name}'; skipping PO creation.")
                    continue

                # 3) Convert the PO's 'project_number' -> real Project.id
                project_number_str = str(item["project_number"])  # e.g. "2416"
                self.logger.info(f"🔍 Searching for Project by project_number={project_number_str}")

                found_proj = self.database_util.search_projects(["project_number"], [project_number_str])
                if not found_proj:
                    # Possibly auto-create the project
                    self.logger.info(f"🆕 Auto-creating Project with project_number={project_number_str}")
                    new_proj = self.database_util.create_project(
                        project_number=project_number_str,
                        name=f"Project {project_number_str}",
                        status="Active"
                    )
                    if not new_proj:
                        self.logger.warning(f"⚠️ No valid project row created for {project_number_str}. Skipping.")
                        continue

                # 4) Find or create the PurchaseOrder with the real project.id
                po_search = self.database_util.search_purchase_order_by_keys(
                    item["project_number"],
                    item["po_number"]
                )

                if not po_search:
                    self.logger.info(
                        f"🆕 Creating PurchaseOrder for Project {item['project_number']}, "
                        f"po_number={item['po_number']}"
                    )
                    new_po = self.database_util.create_purchase_order_by_keys(
                        project_number=item["project_number"],
                        po_number=item["po_number"],
                        description=item.get("description"),
                        po_type=item.get("po_type"),
                        contact_id=contact_id  # Link to the contact
                    )
                    po_surrogate_id = new_po["id"] if new_po else None

                elif isinstance(po_search, list):
                    existing_po = po_search[0]
                    po_surrogate_id = existing_po["id"]
                    self.database_util.update_purchase_order_by_keys(
                        project_number=item["project_number"],
                        po_number=item["po_number"],
                        description=item.get("description"),
                        po_type=item.get("po_type"),
                        contact_id=contact_id  # Link to the contact
                    )
                else:
                    # Single record found
                    po_surrogate_id = po_search["id"]
                    self.database_util.update_purchase_order_by_keys(
                        project_number=item["project_number"],
                        po_number=item["po_number"],
                        description=item.get("description"),
                        po_type=item.get("po_type"),
                        contact_id=contact_id  # Link to the contact
                    )

                item["po_surrogate_id"] = po_surrogate_id
                item["contact_id"] = contact_id
                processed_items.append(item)

            except Exception as ex:
                self.logger.error(
                    f"💥 Error processing main item (PO={item.get('po_number')}): {ex}",
                    exc_info=True
                )

        # --- 2) CREATE OR UPDATE DETAIL ITEMS ---
        self.logger.info("🔧 Creating/Updating Detail Items for each PO...")

        # A convenience set to treat as "complete" and skip normal detail updates
        PAYMENT_COMPLETE_STATUSES = ["PAID", "LOGGED", "RECONCILED", "REVIEWED"]

        created_or_updated_inv_details = []  # We'll track newly touched invoice details for the new step

        for sub_item in detail_items:
            try:
                if self.DEBUG_STARTING_PO_NUMBER and int(sub_item["po_number"]) < self.DEBUG_STARTING_PO_NUMBER:
                    self.logger.debug(
                        f"⏭ Skipping detail item for PO '{sub_item['po_number']}' due to debug start number.")
                    continue

                main_match = next((m for m in processed_items if m["po_number"] == sub_item["po_number"]), None)
                if not main_match or "po_surrogate_id" not in main_match or not main_match["po_surrogate_id"]:
                    self.logger.warning(
                        f"❗ DetailItem referencing PO '{sub_item['po_number']}' has no main match. Skipping."
                    )
                    continue

                # ------------------------------------------------------------------
                # 1) Before normal detail logic, check spend_money record for RECONCILED
                # ------------------------------------------------------------------
                project_str = str(sub_item["project_number"])
                po_str = str(sub_item["po_number"]).zfill(2)  # Pad PO number
                detail_str = str(sub_item["detail_item_id"]).zfill(2)  # Pad Detail Item number

                possible_refs = [
                    f"{project_str}_{po_str}_{detail_str}",  # full match
                    f"{project_str}_{po_str}"  # fallback if detail-level record not found
                ]

                spend_money_record = None
                for ref in possible_refs:
                    sm = self.database_util.search_spend_money(
                        column_names=['xero_spend_money_reference_number'],
                        values=[ref]
                    )
                    if sm and sm["state"] != "DELETED":
                        spend_money_record = sm
                        break

                existing_detail = self.database_util.search_detail_item_by_keys(
                    project_number=sub_item["project_number"],
                    po_number=sub_item["po_number"],
                    detail_number=sub_item["detail_item_id"],
                    line_id=sub_item["line_id"]
                )

                # If RECONCILED => skip normal updates
                if spend_money_record and existing_detail and spend_money_record["state"] == "RECONCILED":
                    self.logger.info(
                        f"SpendMoney found with state=RECONCILED for ref='{spend_money_record['xero_spend_money_reference_number']}'. "
                        f"Marking detail {detail_str} as RECONCILED."
                    )
                    updated_di = self.database_util.update_detail_item_by_keys(
                        project_number=sub_item["project_number"],
                        po_number=sub_item["po_number"],
                        detail_number=sub_item["detail_item_id"],
                        line_id=sub_item["line_id"],
                        state="RECONCILED"
                    )
                    continue

                # ------------------------------------------------------------------
                # 2) Create or update the detail item
                # ------------------------------------------------------------------
                if spend_money_record and spend_money_record["state"] == "RECONCILED":
                    sub_item["state"] = "RECONCILED"

                created_or_updated_detail = None

                if not existing_detail:
                    # Not in DB -> create
                    created_di = self.database_util.create_detail_item_by_keys(
                        project_number=sub_item["project_number"],
                        po_number=sub_item["po_number"],
                        detail_number=sub_item["detail_item_id"],
                        line_id=sub_item["line_id"],
                        rate=sub_item.get("rate", 0),
                        quantity=sub_item.get("quantity", 1),
                        ot=sub_item.get("OT", 0),
                        fringes=sub_item.get("fringes", 0),
                        vendor=sub_item.get("vendor"),
                        description=sub_item.get("description"),
                        transaction_date=sub_item.get("date"),
                        due_date=sub_item.get("due date"),
                        aicp_code=sub_item.get("account"),
                        state=sub_item["state"],
                        payment_type=sub_item["payment_type"]
                    )
                    if created_di:
                        created_or_updated_detail = created_di
                        self.logger.debug(f"✔️ Created DetailItem id={created_di['id']} for PO={sub_item['po_number']}")
                elif isinstance(existing_detail, list):
                    # Multiple found
                    first_detail = existing_detail[0]
                    payment_status = first_detail.get("state", "").upper()
                    if payment_status in [s.upper() for s in PAYMENT_COMPLETE_STATUSES]:
                        self.logger.debug(
                            f"⚠️ DetailItem id={first_detail['id']} has payment_status={payment_status}, skipping update."
                        )
                        continue
                    self.logger.debug(
                        f"🔎 Found multiple detail items, updating the first (id={first_detail['id']})."
                    )
                    updated_di = self.database_util.update_detail_item_by_keys(
                        project_number=sub_item["project_number"],
                        po_number=sub_item["po_number"],
                        detail_number=sub_item["detail_item_id"],
                        line_id=sub_item["line_id"],
                        rate=sub_item.get("rate", 0),
                        quantity=sub_item.get("quantity", 1),
                        ot=sub_item.get("OT", 0),
                        fringes=sub_item.get("fringes", 0),
                        vendor=sub_item.get("vendor"),
                        description=sub_item.get("description"),
                        transaction_date=sub_item.get("date"),
                        due_date=sub_item.get("due date"),
                        state=sub_item["state"],
                        payment_type=sub_item["payment_type"]
                    )
                    created_or_updated_detail = updated_di if updated_di else first_detail
                else:
                    # Exactly one found
                    payment_status = existing_detail.get("state", "").upper()
                    if payment_status in [s.upper() for s in PAYMENT_COMPLETE_STATUSES]:
                        self.logger.debug(
                            f"⚠️ DetailItem id={existing_detail['id']} has payment_status={payment_status}, skipping update."
                        )
                        continue

                    self.logger.debug(
                        f"🔎 Found existing detail item, updating id={existing_detail['id']}."
                    )
                    updated_di = self.database_util.update_detail_item_by_keys(
                        project_number=sub_item["project_number"],
                        po_number=sub_item["po_number"],
                        detail_number=sub_item["detail_item_id"],
                        line_id=sub_item["line_id"],
                        rate=sub_item.get("rate", 0),
                        quantity=sub_item.get("quantity", 1),
                        ot=sub_item.get("OT", 0),
                        fringes=sub_item.get("fringes", 0),
                        vendor=sub_item.get("vendor"),
                        description=sub_item.get("description"),
                        transaction_date=sub_item.get("date"),
                        due_date=sub_item.get("due date"),
                        state=sub_item["state"],
                        payment_type=sub_item["payment_type"]
                    )
                    created_or_updated_detail = updated_di if updated_di else existing_detail

                # -----------------------
                # 2a) If there's a receipt, store receipt_id & handle state
                # -----------------------
                if created_or_updated_detail:
                    existing_receipts = self.database_util.search_receipts(
                        ["project_number", "po_number", "detail_number", "line_id"],
                        [
                            sub_item["project_number"],
                            sub_item["po_number"],
                            sub_item["detail_item_id"],
                            sub_item["line_id"]
                        ]
                    )
                    if existing_receipts:
                        first_receipt = (
                            existing_receipts[0]
                            if isinstance(existing_receipts, list)
                            else existing_receipts
                        )
                        receipt_id = first_receipt.get("id")

                        # Only set receipt_id if detail doesn't already have it
                        if not created_or_updated_detail.get("receipt_id"):
                            self.logger.info(
                                f"Linking existing receipt_id={receipt_id} to DetailItem id={created_or_updated_detail['id']}."
                            )
                            self.database_util.update_detail_item_by_keys(
                                project_number=sub_item["project_number"],
                                po_number=sub_item["po_number"],
                                detail_number=sub_item["detail_item_id"],
                                line_id=sub_item["line_id"],
                                receipt_id=receipt_id
                            )

                # region Invoice linking logic
                # ---------------------------------------------------
                # If payment_type is "INV", then the detail_item_id
                # is effectively the invoice number. We look up an invoice
                # by (project_number, po_number, invoice_number=detail_item_id),
                # and if found, store that invoice’s ID in the detail item.
                # ---------------------------------------------------
                if created_or_updated_detail and sub_item.get("payment_type", "").upper() == "INV":
                    invoice_number = sub_item["detail_item_id"]
                    existing_invoices = self.database_util.search_invoices(
                        ["project_number", "po_number", "invoice_number"],
                        [
                            sub_item["project_number"],
                            sub_item["po_number"],
                            invoice_number
                        ]
                    )
                    if existing_invoices:
                        first_invoice = existing_invoices[0] if isinstance(existing_invoices,
                                                                           list) else existing_invoices
                        invoice_id = first_invoice.get("id")
                        # Only set invoice_id if detail doesn't already have it
                        if not created_or_updated_detail.get("invoice_id"):
                            self.logger.info(
                                f"Linking existing invoice_id={invoice_id} to DetailItem id={created_or_updated_detail['id']}."
                            )
                            self.database_util.update_detail_item_by_keys(
                                project_number=sub_item["project_number"],
                                po_number=sub_item["po_number"],
                                detail_number=sub_item["detail_item_id"],
                                line_id=sub_item["line_id"],
                                invoice_id=invoice_id
                            )
                        # Keep track of these items to do the sum check below
                        created_or_updated_inv_details.append(created_or_updated_detail)

                # -----------------------
                # 2b) Check for matching receipt total => set REVIEWED or mismatch
                # -----------------------
                if created_or_updated_detail:
                    detail_subtotal = float(created_or_updated_detail.get("sub_total", 0.0))

                    existing_receipts = self.database_util.search_receipts(
                        ["project_number", "po_number", "detail_number"],
                        [sub_item["project_number"], sub_item["po_number"], sub_item["detail_item_id"]]
                    )
                    if existing_receipts:
                        first_receipt = (
                            existing_receipts[0]
                            if isinstance(existing_receipts, list)
                            else existing_receipts
                        )
                        try:
                            receipt_total = float(first_receipt.get("total", 0.0))
                        except TypeError:
                            receipt_total = 0.0

                        if receipt_total == detail_subtotal:
                            new_state = "REVIEWED"
                            self.logger.info(
                                f"Receipt total matches detail subtotal for detail_item {sub_item['detail_item_id']} => Setting state=REVIEWED."
                            )
                            self.database_util.update_detail_item_by_keys(
                                project_number=sub_item["project_number"],
                                po_number=sub_item["po_number"],
                                detail_number=sub_item["detail_item_id"],
                                line_id=sub_item["line_id"],
                                state=new_state
                            )
                        else:
                            new_state = "PO MISMATCH"
                            self.logger.info(
                                f"Receipt total != detail subtotal for detail_item {sub_item['detail_item_id']} => Setting state=PO MISMATCH."
                            )
                            self.database_util.update_detail_item_by_keys(
                                project_number=sub_item["project_number"],
                                po_number=sub_item["po_number"],
                                detail_number=sub_item["detail_item_id"],
                                line_id=sub_item["line_id"],
                                state=new_state
                            )

                    # ----------------------------------------------------
                    # If due_date has passed => status = OVERDUE
                    # ----------------------------------------------------
                    due_date_str = created_or_updated_detail.get("due_date")
                    current_state = created_or_updated_detail.get("state", "").upper()
                    completed_statuses = {"PAID", "LOGGED", "RECONCILED", "REVIEWED"}
                    po_type = main_match.get("po_type", "").upper() if main_match else ""

                    if due_date_str and current_state not in completed_statuses and po_type not in ["PC", "CC"]:
                        try:
                            due_date_str = str(due_date_str)
                            due_date_obj = datetime.strptime(due_date_str, "%Y-%m-%d %H:%M:%S")
                            now = datetime.now()
                            if due_date_obj < now:
                                self.logger.info(
                                    f"Due date ({due_date_obj}) has passed for detail item => Setting state=OVERDUE."
                                )
                                self.database_util.update_detail_item_by_keys(
                                    project_number=sub_item["project_number"],
                                    po_number=sub_item["po_number"],
                                    detail_number=sub_item["detail_item_id"],
                                    line_id=sub_item["line_id"],
                                    state="OVERDUE"
                                )
                        except ValueError:
                            self.logger.warning(f"Could not parse due_date '{due_date_str}' for detail item.")
                            pass

            except Exception as ex:
                self.logger.error(
                    f"💥 Error processing DetailItem for PO={sub_item['po_number']}: {ex}",
                    exc_info=True
                )

        self.logger.info("✅ Database processing of PO data complete.")

        # region INVOICE CHECK AFTER DETAIL CREATION (NEW)
        try:
            # Filter only detail items that are payment_type="INV"
            # and that we've just created or updated
            inv_items = [d for d in created_or_updated_inv_details if d.get("payment_type", "").upper() == "INV"]

            if inv_items:
                # Group them by (project_number, po_number, invoice_id)
                from collections import defaultdict
                grouped = defaultdict(list)
                for d in inv_items:
                    pn = int(project_number_str),
                    po = int(po_str)
                    inv_id = d.get("invoice_id")
                    if inv_id:
                        grouped[(pn, po, inv_id)].append(d)

                # For each group, sum sub_totals and compare with invoice total
                for (pn, po, inv_id), detail_list in grouped.items():
                    invoice_record = self.database_util.search_invoices(["id"], [inv_id])
                    if not invoice_record:
                        continue

                    invoice_total = float(invoice_record.get("total", 0.0))

                    # We also want to ensure none of these detail items is in an "untouchable" status
                    skip_statuses = {"OVERDUE", "RECONCILED", "PAID"}
                    if any(d.get("state", "").upper() in skip_statuses for d in detail_list):
                        self.logger.info(
                            f"Skipping invoice check for invoice_id={inv_id} because at least one detail item is OVERDUE/RECONCILED/PAID."
                        )
                        continue

                    sum_subs = sum(float(d.get("sub_total", 0.0)) for d in detail_list)
                    if abs(sum_subs - invoice_total) < 0.0001:
                        # They match => set detail items to RTP
                        for d in detail_list:
                            self.logger.info(
                                f"Invoice total matches for invoice_id={inv_id}; setting detail item ID={d['id']} to RTP."
                            )
                            self.database_util.update_detail_item_by_keys(
                                project_number=pn,
                                po_number=po,
                                detail_number=d["detail_number"],
                                line_id=d["line_id"],
                                state="RTP"
                            )
                    else:
                        # Mismatch => set detail items to PO MISMATCH
                        for d in detail_list:
                            self.logger.info(
                                f"Invoice total mismatch for invoice_id={inv_id}; setting detail item ID={d['id']} to PO MISMATCH."
                            )
                            self.database_util.update_detail_item_by_keys(
                                project_number=pn,
                                po_number=po,
                                detail_number=d["detail_number"],
                                line_id=d["line_id"],
                                state="PO MISMATCH"
                            )
        except Exception as e:
            self.logger.exception("💥 Error in invoice check after detail creation: ", exc_info=True)
        # endregion

        return processed_items
    # endregion

    # region Event Processing - PO Log - Step 2 -  Add Folder & Tax Links + Contact Data
    def callback_add_po_data_to_DB(self, fut):
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
        #    Keyed by (project_number, po_number, detail_number, line_id)
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
                # Attempt to look up an aicp code if present
                if sdb.get("aicp_code_id"):
                    aicp_row = self.database_util.search_aicp_codes(["id"], [sdb["aicp_code_id"]])
                    sdb["account_code"] = aicp_row["aicp_code"] if aicp_row else None
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
                            ["project_number", "po_number", "detail_number", "line_id"],
                            [project_number, int(po_no), sdb["detail_number"], sdb["line_id"]]
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
                                line_id=sdb["line_id"],
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
                                po_number=int(po_no),
                                detail_number=sdb["detail_number"],
                                line_id=sdb["line_id"],
                                invoice_id=inv_id
                            )
                            sdb["invoice_id"] = inv_id

                # Build the subitem column values
                sub_col_values_str = self.monday_util.subitem_column_values_formatter(
                    project_id=project_number,
                    po_number=db_item["po_number"],
                    detail_item_number=sdb["detail_number"],
                    line_id=sdb["line_id"],
                    status=sdb.get("state"),
                    description=sdb.get("description"),
                    quantity=sdb.get("quantity"),
                    rate=sdb.get("rate"),
                    date=sdb.get("transaction_date"),
                    due_date=sdb.get("due_date"),
                    account_number=sdb["account_code"],
                    link=file_link_for_subitem,  # ADDED/UPDATED
                    OT=sdb.get("ot"),
                    fringes=sdb.get("fringes")
                )
                new_sub_vals = json.loads(sub_col_values_str)

                sub_key = (project_number, db_item["po_number"], sdb["detail_number"], sdb["line_id"])

                if sub_key in global_subitem_map:
                    # Possibly update
                    msub = global_subitem_map[sub_key]
                    sdb["project_number"] = db_item["project_number"]
                    sdb["po_number"] = db_item["po_number"]
                    sdb["file_link"] = file_link_for_subitem
                    payment_type = db_item["po_type"]
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
                        # Even if no changes in columns, we still want to ensure the DB has the correct pulse_id
                        sub_pulse_id = msub["id"]
                        self.database_util.update_detail_item_by_keys(
                            project_number=project_number,
                            po_number=db_item["po_number"],
                            detail_number=sdb["detail_number"],
                            line_id=sdb["line_id"],
                            pulse_id=sub_pulse_id,
                            parent_pulse_id=main_monday_id
                        )
                        sdb["pulse_id"] = sub_pulse_id
                        sdb["parent_pulse_id"] = main_monday_id
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
                db_sub_item["line_id"],
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
                db_sub_item["line_id"],
                pulse_id=monday_subitem_id,
                parent_pulse_id=parent_item_id
            )
            db_sub_item["pulse_id"] = monday_subitem_id
            db_sub_item["parent_pulse_id"] = parent_item_id

    # endregion

    # region Event Processing - Receipts
    def process_receipt(self, dropbox_path: str):
        """
        🧾 process_receipt
        -----------------
        1) Parse file name (project_number, po_number, detail_item_number, vendor_name).
        2) Download the receipt file from Dropbox.
        3) If PDF, try text extraction via PyPDF2. If that fails (or not a PDF), do OCR.
        4) Use OCRService's 'extract_receipt_info_with_openai' to parse total, date, description.
        5) Generate file link in Dropbox.
        6) Create or update the 'receipt' table, linking to the appropriate detail item *via project_number, po_number, detail_item_number*.
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
            line_id_str = group3_str
        else:
            po_number_str = group2_str
            detail_item_str = group3_str
            line_id_str = "1"

        project_number = int(project_number_str)
        po_number = int(po_number_str)
        detail_item_number = int(detail_item_str)
        line_id_number = int(line_id_str)

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
                detail_number=detail_item_number,
                line_id=line_id_number
            )

            if not existing_detail:
                self.logger.warning(
                    f"❗ No matching detail item found for project={project_number}, "
                    f"PO={po_number}, detail={detail_item_number}, line={line_id_number}."
                )
                self.cleanup_temp_file(temp_file_path)
                return
            else:
                if isinstance(existing_detail, list):
                    existing_detail = existing_detail[0]

            # 6A) Create or update a receipt row
            spend_money_id = 1  # example placeholder

            existing_receipts = self.database_util.search_receipts(
                ["project_number", "po_number", "detail_number", "line_id"],
                [project_number, po_number, detail_item_number, line_id_number]
            )

            if not existing_receipts:
                new_receipt = self.database_util.create_receipt(
                    project_number=project_number,
                    po_number=po_number,
                    detail_number=detail_item_number,
                    line_id=line_id_number,
                    spend_money_id=spend_money_id,
                    total=total_amount,
                    purchase_date=purchase_date,
                    receipt_description=short_description,
                    file_link=file_link
                )
                receipt_id = new_receipt["id"] if new_receipt else None
                self.logger.info(
                    f"🔄 Created new receipt with ID={receipt_id} for detail_item_number={detail_item_number}"
                )
            else:
                existing_receipt = existing_receipts[0] if isinstance(existing_receipts, list) else existing_receipts
                receipt_id = existing_receipt["id"]
                updated_receipt = self.database_util.update_receipt_by_keys(
                    project_number=project_number,
                    po_number=po_number,
                    detail_number=detail_item_number,
                    line_id=line_id_number,
                    total=total_amount,
                    purchase_date=purchase_date,
                    receipt_description=short_description,
                    file_link=file_link
                )
                self.logger.info(
                    f"✏️ Updated existing receipt with ID={receipt_id} for detail_item_number={detail_item_number}"
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
                project_number=str(project_number),
                po_number=po_number,
                detail_number=detail_item_number,
                line_id=line_id_number,
                state=state,
                receipt_id=receipt_id
            )

            # 8) Update the subitem in Monday
            column_values_str = monday_util.subitem_column_values_formatter(link=file_link, status=state)
            new_vals = column_values_str

            if existing_detail.get("pulse_id"):
                subitem_id = existing_detail["pulse_id"]
                monday_subitem = self.monday_api.update_item(
                    item_id=subitem_id,
                    column_values=new_vals,
                    type="subitem"
                )
                if not monday_subitem:
                    self.logger.warning(
                        f"❌ No Monday subitem found for detail {detail_item_number} in project {project_number}, PO {po_number}."
                    )
                else:
                    self.monday_api.update_item(
                        item_id=subitem_id,
                        column_values=new_vals,
                        type="subitem"
                    )
            else:
                self.logger.warning(
                    f"Detail item for project={project_number}, PO={po_number}, detail={detail_item_number} has no pulse_id in DB."
                )

            self.cleanup_temp_file(temp_file_path)
            self.logger.info(f"✅ Receipt processing complete for: {dropbox_path}")

        except Exception as e:
            self.logger.exception(f"💥 Error processing receipt {filename}: {e}", exc_info=True)
            return

    # endregion

    #region Event Processing - Invoices
    def process_invoice(self, dropbox_path: str):
        """
        Processes an invoice file from Dropbox. The filename is expected to match
        the pattern "{project_number}_{po_number}" or "{project_number}_{po_number}_{invoice_number}".
        If no invoice number is in the filename, defaults to 1.

        Steps:
         1) Parse filename to extract project_number, po_number, and invoice_number.
         2) Download the file from Dropbox to a local temp path.
         3) Use OCRService + OpenAI to extract total amount, transaction date, and term from the file.
         4) Locate the corresponding purchase_order (and project) in the DB.
         5) Create or update the 'invoice' record with the parsed info.
         6) Store the Dropbox share link in 'file_link'.
         7) Link matching detail items (payment_type="INV" & detail_number=invoice_number) to this invoice_id.
         8) (NEW) If none of the associated detail items are RECONCILED/OVERDUE/PAID,
            check whether the sum of their sub_totals matches the invoice total.
            - If match => set status=RTP
            - If mismatch => set status=PO MISMATCH
         9) Optionally push data to Monday (update subitems, etc.).
        """
        self.logger.info(f"📄 Processing invoice: {dropbox_path}")
        filename = os.path.basename(dropbox_path)

        # 1) Extract project_number, po_number, invoice_number
        try:
            match = re.match(r"^(\d{4})_(\d{1,2})(?:_(\d{1,2}))?", filename)
            if not match:
                self.logger.warning(f"⚠️ Invoice filename '{filename}' doesn't match the expected pattern.")
                return
            project_number_str = match.group(1)
            po_number_str = match.group(2)
            invoice_number_str = match.group(3) or "1"  # default to 1 if missing

            project_number = int(project_number_str)
            po_number = int(po_number_str)
            invoice_number = int(invoice_number_str)

            path_segments = dropbox_path.strip("/").split("/")
            if len(path_segments) >= 2:
                # Attempt to parse PO number from folder name if there's a mismatch
                folder_po_str = path_segments[-2].strip()
                folder_po_match = re.search(r'^\d{4}_(\d+)', folder_po_str)
                if folder_po_match:
                    folder_po_number = int(folder_po_match.group(1))
                    if folder_po_number != po_number:
                        self.logger.warning(
                            f"❗ Mismatch between folder PO number '{folder_po_number}' "
                            f"and invoice filename PO number '{po_number}'. "
                            f"Using folder PO number."
                        )
                        po_number = folder_po_number

        except Exception as e:
            self.logger.exception(f"💥 Error parsing invoice filename '{filename}': {e}", exc_info=True)
            return

        # 2) Download file from Dropbox
        temp_file_path = f"./temp_files/{filename}"
        try:
            self.logger.info(f"⬇️ Downloading invoice file to {temp_file_path}")
            if not self.download_file_from_dropbox(dropbox_path, temp_file_path):
                self.logger.error(f"❌ Could not download invoice file: {dropbox_path}")
                return
        except Exception as e:
            self.logger.exception(f"💥 Failed to download invoice file: {e}", exc_info=True)
            return

        # 3) Extract data with OCR + OpenAI
        try:
            self.logger.info("🔎 Scanning invoice file for data with OCRService + OpenAI...")
            extracted_text = self.ocr_service.extract_text(temp_file_path)
            info, err = self.ocr_service.extract_info_with_openai(extracted_text)

            if err or not info:
                self.logger.error(f"❌ Failed to parse invoice data from OpenAI. Error: {err}")
                transaction_date = None
                term = 30
                total = 0.0
            else:
                # Attempt to parse the date
                date_str = info.get("invoice_date")
                try:
                    transaction_date = datetime.strptime(date_str, "%Y-%m-%d") if date_str else None
                except (ValueError, TypeError):
                    transaction_date = None

                # Attempt to parse total as float
                total_str = info.get("total_amount")
                try:
                    total = float(total_str) if total_str else 0.0
                except (ValueError, TypeError):
                    total = 0.0

                # Attempt to parse the payment term
                term_str = info.get("payment_term")
                if term_str:
                    digits_only = re.sub(r"[^0-9]", "", term_str)
                    try:
                        term_int = int(digits_only) if digits_only else 30
                        if len(digits_only) > 2:
                            self.logger.warning(f"Extracted payment term '{term_int}' is too long. Defaulting to 30.")
                            term = 30
                        elif 7 <= term_int <= 60:
                            term = term_int
                            self.logger.info(f"Parsed payment term: {term} days.")
                        else:
                            self.logger.warning(
                                f"Payment term '{term_int}' is out of expected range. Defaulting to 30.")
                            term = 30
                    except (ValueError, TypeError) as e:
                        self.logger.exception(f"Error parsing payment term '{term_str}': {e}. Defaulting to 30.")
                        term = 30
                else:
                    self.logger.info("No payment term provided. Defaulting to 30.")
                    term = 30

        except Exception as e:
            self.logger.exception(f"💥 Error extracting invoice data: {e}", exc_info=True)
            transaction_date, term, total = None, 30, 0.0

        # 4) Find or create the purchase_order (and project)
        try:
            po_data = self.database_util.search_purchase_order_by_keys(project_number, po_number)
            if not po_data:
                self.logger.warning(f"⚠️ PO {project_number}_{po_number} not found; cannot create invoice.")
                return
            if isinstance(po_data, list):
                po_data = po_data[0]
        except Exception as e:
            self.logger.exception(f"💥 Error locating PO or project in DB: {e}", exc_info=True)
            return

        # 5) Create or update the invoice record
        try:
            existing_invoice = self.database_util.search_invoice_by_keys(project_number, po_number, invoice_number)
            if existing_invoice is None:
                self.logger.info(
                    f"🆕 Creating new invoice #{invoice_number} for PO {po_number} (project {project_number}).")
                new_invoice = self.database_util.create_invoice(
                    project_number=project_number,
                    po_number=po_number,
                    invoice_number=invoice_number,
                    transaction_date=transaction_date,
                    term=term,
                    total=total
                )
                invoice_id = new_invoice["id"] if new_invoice else None
            else:
                if isinstance(existing_invoice, list):
                    invoice_id = existing_invoice[0]["id"]
                else:
                    invoice_id = existing_invoice["id"]

                self.logger.info(f"🔄 Updating existing invoice #{invoice_number} for PO {po_number}.")
                self.database_util.update_invoice(
                    invoice_id=invoice_id,
                    transaction_date=transaction_date,
                    term=term,
                    total=total
                )
        except Exception as e:
            self.logger.exception(f"💥 Error creating/updating invoice in DB: {e}", exc_info=True)
            return

        # 6) Update the invoice file_link with Dropbox share link
        try:
            file_share_link = self.dropbox_util.get_file_link(dropbox_path)
            self.logger.info(f"🔗 Obtained Dropbox link for invoice: {file_share_link}")
            self.database_util.update_invoice(
                invoice_id=invoice_id,
                file_link=file_share_link
            )
        except Exception as e:
            self.logger.exception(f"💥 Error retrieving or storing Dropbox link: {e}", exc_info=True)

        # 7) Link matching detail items (payment_type="INV" & detail_number=invoice_number) to this invoice_id
        try:
            detail_matches = self.database_util.search_detail_items(
                column_names=["po_id", "detail_number", "payment_type"],
                values=[po_data['id'], invoice_number, "INV"]
            )
            if detail_matches:
                if not isinstance(detail_matches, list):
                    detail_matches = [detail_matches]
                for d_item in detail_matches:
                    if d_item.get("invoice_id") != invoice_id:
                        self.database_util.update_detail_item_by_keys(
                            project_number=project_number,
                            po_number=po_number,
                            detail_number=d_item["detail_number"],
                            line_id=d_item["line_id"],
                            invoice_id=invoice_id
                        )
                        self.logger.info(
                            f"Linked invoice_id={invoice_id} to detail_item_id={d_item['detail_number']} (ID={d_item['id']})."
                        )
            else:
                self.logger.info(
                    f"No existing detail items found for project={project_number}, PO={po_number}, detail={invoice_number}, payment_type=INV."
                )
        except Exception as e:
            self.logger.exception(f"💥 Error linking detail items to invoice {invoice_id}: {e}", exc_info=True)

        # 8) (NEW) Check if none of the associated details are RECONCILED, OVERDUE, or PAID,
        #    then compare sum(sub_totals) to invoice total => set "RTP" or "PO MISMATCH"
        try:
            if detail_matches:
                # Reload them (they may have been updated above)
                all_details = []
                for item in detail_matches:
                    reloaded = self.database_util.search_detail_item_by_keys(
                        project_number=project_number,
                        po_number=po_number,
                        detail_number=item["detail_number"],
                        line_id=item["line_id"]
                    )
                    if reloaded:
                        all_details.append(reloaded if isinstance(reloaded, dict) else reloaded[0])

                # Determine if any detail is RECONCILED, OVERDUE, or PAID
                skip_check = any(
                    d.get("state", "").upper() in {"RECONCILED", "OVERDUE", "PAID"}
                    for d in all_details
                )
                if skip_check:
                    self.logger.info(
                        "Skipping invoice total check because at least one detail item is RECONCILED/OVERDUE/PAID."
                    )
                else:
                    # Sum sub_totals from these detail items
                    sum_subtotals = sum(float(d.get("sub_total", 0.0)) for d in all_details)
                    if abs(sum_subtotals - total) < 0.0001:
                        # Mark invoice as RTP in DB
                        self.logger.info(
                            f"Invoice total matches sum of detail sub_totals. Setting invoice status to RTP."
                        )
                        self.database_util.update_invoice(invoice_id, status="RTP")

                        # Optionally update any Monday column for invoice (if you track invoice status there)
                        # For subitems, you could also do something similar:
                        # This is just an example—actual code may differ
                        for d in all_details:
                            if d.get("pulse_id"):
                                col_values_str = self.monday_util.subitem_column_values_formatter(status="RTP")
                                self.monday_util.update_subitem_columns(d["pulse_id"], json.loads(col_values_str))

                    else:
                        self.logger.info(
                            f"Invoice total != sum of detail sub_totals ({sum_subtotals}). Marking as PO MISMATCH."
                        )
                        self.database_util.update_invoice(invoice_id, status="PO MISMATCH")

                        # Similarly update subitems on Monday:
                        for d in all_details:
                            if d.get("pulse_id"):
                                col_values_str = self.monday_util.subitem_column_values_formatter(status="PO MISMATCH")
                                self.monday_util.update_subitem_columns(d["pulse_id"], json.loads(col_values_str))
        except Exception as e:
            self.logger.exception(f"💥 Error performing invoice total check for invoice_id={invoice_id}: {e}",
                                  exc_info=True)

        # 9) Update any Monday subitems if needed (existing logic)
        try:
            self.logger.info(
                f"🌐 Updating Monday subitems link for invoice #{invoice_number} (project {project_number}, PO {po_number})."
            )

            # Retrieve all subitems from your DB that correspond to this invoice number
            matching_subitems = self.database_util.search_detail_items(
                ["po_id", "detail_number"], [po_data["id"], invoice_number]
            )

            if not matching_subitems:
                self.logger.info("No subitems found in DB that link to this invoice.")
            else:
                if not isinstance(matching_subitems, list):
                    matching_subitems = [matching_subitems]

                updated_count = 0
                for sub in matching_subitems:
                    subitem_id = sub.get("pulse_id")
                    if not subitem_id:
                        self.logger.warning(f"Missing subitem_id (pulse_id) in DB record: {sub}")
                        continue

                    col_vals_str = self.monday_util.subitem_column_values_formatter(link=file_share_link)
                    col_vals_dict = json.loads(col_vals_str)

                    success = self.monday_util.update_subitem_columns(subitem_id, col_vals_dict)
                    if success:
                        updated_count += 1

                self.logger.info(f"Updated {updated_count} subitems on Monday with invoice link.")
        except Exception as e:
            self.logger.exception(f"💥 Error updating subitems on Monday: {e}", exc_info=True)

        # Cleanup temp file
        self.cleanup_temp_file(temp_file_path)
        self.logger.info(f"✅ Invoice processing complete for: {dropbox_path}")
        #endregion

    # region Tax Form
    def process_tax_form(self, dropbox_path: str):
        """
        Stub function for processing a tax form from Dropbox.
        Could perform OCR, fill out contact tax_form_link, etc.
        """
        self.logger.info(f"🗂 Processing Tax Form: {dropbox_path}")
        # ...
        pass

    # endregion

    # region Helper Methods
    def extract_project_number(self, path: str) -> str:
        """
        Extract the project number from a file path or file name (stub).
        """
        # Implementation depends on naming convention.
        # This is just a placeholder.
        return "9999"

    def download_file_from_dropbox(self, dropbox_path: str, local_path: str) -> bool:
        """
        Download a file from Dropbox to a local path.
        """
        try:
            self.dropbox_api.download_file(dropbox_path, local_path)
            return True
        except Exception as e:
            self.logger.error(f"Failed to download from Dropbox: {e}", exc_info=True)
            return False

    def cleanup_temp_file(self, temp_file_path: str):
        """
        Remove a temporary file if it exists.
        """
        try:
            if os.path.exists(temp_file_path):
                os.remove(temp_file_path)
        except Exception as e:
            self.logger.warning(f"Error cleaning up temp file '{temp_file_path}': {e}")

    def _extract_text_from_pdf(self, file_data: bytes) -> str:
        """
        Extract text from a PDF file using PyPDF2 or a similar library (placeholder).
        """
        # Implementation detail omitted...
        return ""

    def _extract_text_from_pdf_with_ocr(self, file_data: bytes) -> str:
        """
        Convert PDF to images, then run OCR on each page.
        """
        temp_dir = tempfile.mkdtemp()
        text_result = []
        try:
            pdf_path = os.path.join(temp_dir, "temp.pdf")
            with open(pdf_path, "wb") as f:
                f.write(file_data)

            images = convert_from_path(pdf_path)
            for i, img in enumerate(images):
                temp_img_path = os.path.join(temp_dir, f"page_{i}.png")
                img.save(temp_img_path, "PNG")
                extracted_text = pytesseract.image_to_string(Image.open(temp_img_path))
                text_result.append(extracted_text)
        except Exception as e:
            self.logger.warning(f"Error in _extract_text_from_pdf_with_ocr: {e}")
        finally:
            # Cleanup
            for f in os.listdir(temp_dir):
                os.remove(os.path.join(temp_dir, f))
            os.rmdir(temp_dir)

        return "\n".join(text_result)

    def _extract_text_via_ocr(self, image_data: bytes) -> str:
        """
        Extract text from image bytes using pytesseract.
        """
        try:
            with tempfile.NamedTemporaryFile(delete=False, suffix='.png') as temp_img_file:
                temp_img_file.write(image_data)
                temp_img_file_name = temp_img_file.name

            extracted_text = pytesseract.image_to_string(Image.open(temp_img_file_name))
        except Exception as e:
            self.logger.warning(f"Error in _extract_text_via_ocr: {e}")
            extracted_text = ""
        finally:
            if os.path.exists(temp_img_file_name):
                os.remove(temp_img_file_name)

        return extracted_text

    # endregion
dropbox_service = DropboxService()