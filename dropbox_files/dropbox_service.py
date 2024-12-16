# services/dropbox_service.py
# 📦✨ DropboxService: Processes files from Dropbox and integrates them with Monday.com! ✨📦
# Enhanced with additional logging and refactored to separate DB, Monday, and Dropbox logic.
# Added ONLY_DROPBOX_LINKS mode to skip DB and Monday logic and focus solely on Dropbox link retrieval.

import json
import os
import re
import threading
import traceback
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta, datetime
import PyPDF2
import pytesseract
from pdf2image import convert_from_path
import logging
import tempfile

from dropbox_files.dropbox_api import dropbox_api
from config import Config
from dropbox_database_util import DropboxDatabaseUtil, dropbox_database_util
from monday_files.monday_api import monday_api
from monday_files.monday_util import monday_util
from po_log_files.po_log_database_util import po_log_database_util
from dropbox_files.dropbox_client import dropbox_client
from monday_files.monday_service import monday_service
from dropbox_files.dropbox_util import dropbox_util
from po_log_files.po_log_processor import POLogProcessor
from utilities.singleton import SingletonMeta


class DropboxService(metaclass=SingletonMeta):

    PO_LOG_FOLDER_NAME = "1.5 PO Logs"
    PO_NUMBER_FORMAT = "{:02}"  # Ensures PO numbers are two digits
    INVOICE_REGEX = r"invoice"
    TAX_FORM_REGEX = r"w9|w8-ben|w8-bene|w8-ben-e"
    RECEIPT_REGEX = r"receipt"
    SHOWBIZ_REGEX = r".mbb"
    USE_TEMP_FILE = True
    DEBUG_STARTING_PO_NUMBER = 0
    SKIP_MONDAY = False
    SKIP_LINKS = True
    SKIP_FILE_SCAN = False
    ONLY_DROPBOX_LINKS = False
    executor = ThreadPoolExecutor(max_workers=5)

    def __init__(self):
        if not hasattr(self, '_initialized'):
            self.logger = logging.getLogger("app_logger")
            self.monday_service = monday_service
            self.dropbox_client = dropbox_client
            self.po_log_processor = POLogProcessor()
            self.dropbox_util = dropbox_util
            self.monday_api = monday_api
            self.monday_util = monday_util
            self.config = Config()
            self.database_util = dropbox_database_util
            self.dropbox_api = dropbox_api
            self.po_log_database_util = po_log_database_util
            self.logger.info("📦 Dropbox Service initialized 🌟")
            self._initialized = True

    def determine_file_type(self, path: str):
        self.logger.info(f"🔍 Checking file type for: {self.dropbox_util.get_last_path_component_generic(path)}")
        filename = os.path.basename(path)
        try:
            if self.PO_LOG_FOLDER_NAME in path:
                project_id_match = re.match(r"^PO_LOG_(\d{4})[-_]\d{4}-\d{2}-\d{2}_\d{2}-\d{2}-\d{2}\.txt$", filename)
                if project_id_match:
                    project_id = project_id_match.group(1)
                    self.logger.info(f"🗂 Identified as PO Log for Project ID {project_id}")
                    return self.process_po_log(path)
                else:
                    self.logger.warning(f"⚠️ Filename '{filename}' does not match expected PO Log format.")
                    return

            if re.search(self.INVOICE_REGEX, filename, re.IGNORECASE):
                self.logger.info(f"💰 Identified as invoice: {filename}")
                return self.process_invoice(path)

            if re.search(self.TAX_FORM_REGEX, filename, re.IGNORECASE):
                self.logger.info(f"💼 Identified as tax form: {filename}")
                return self.process_tax_form(path)

            if re.search(self.RECEIPT_REGEX, filename, re.IGNORECASE):
                self.logger.info(f"🧾 Identified as receipt: {filename}")
                return self.process_receipt(path)

            if re.search(self.SHOWBIZ_REGEX, filename, re.IGNORECASE):
                self.logger.info(f"📑 Identified as budget file: {filename}")
                return self.process_budget(path)

            self.logger.debug(f"❌ Unsupported file type: {filename}")
            return None
        except Exception as e:
            self.logger.exception(f"💥 Error determining file type for {filename}: {e}", exc_info=True)
            return None

    # ========== PO LOG PROCESSING ==========
    def process_po_log(self, path: str):
        self.logger.info(f"📝 Processing PO log: {path}")
        temp_file_path = f"./temp_files/{os.path.basename(path)}"
        project_id = self.extract_project_id(temp_file_path)

        # Download PO Log from Dropbox if not using a temp file
        if not self.USE_TEMP_FILE:
            if not self.download_file_from_dropbox(path, temp_file_path):
                return

        # Step 1: Parse data from file (no DB or Monday logic here)
        main_items, detail_items, contacts = self.parse_po_log_file(temp_file_path, project_id)

        # If ONLY_DROPBOX_LINKS is True, skip DB and Monday steps
        if self.ONLY_DROPBOX_LINKS:
            self.logger.info("🔗 ONLY_DROPBOX_LINKS mode enabled. Skipping DB and Monday creation/update steps.")
            # In ONLY_DROPBOX_LINKS mode, we assume items are already processed and have pulse_ids in DB.
            # We just focus on linking. To get those processed items with pulse_id and po_surrogate_id,
            # we can fetch them from the database since the items are already loaded and matched.
            processed_items = self.po_log_database_util.get_purchase_orders(project_id=project_id)
            if processed_items:
                self.logger.info("🗂 Scanning PO folders for links (ONLY_DROPBOX_LINKS mode)...")
                for item in processed_items:
                    if 'po_surrogate_id' in item and item['po_surrogate_id']:
                        self.update_po_links(item["project_id"], item["po_number"])
                    else:
                        self.logger.warning(
                            f"⚠️ No po_surrogate_id for PO {item['PO']}. Cannot start link update thread.")

            else:
                self.logger.warning("⚠️ No processed items found in DB for linking. Make sure items are already loaded.")
            return

        # Normal mode: Offload DB processing to a separate thread
        db_future = self.executor.submit(self.db_process_po_data, main_items, detail_items, contacts, project_id)

        # Once DB processing is done, chain Monday processing in another thread, then link updates
        def after_db_done(fut):
            processed_items = fut.result()

            if not self.SKIP_MONDAY:
                monday_future = self.executor.submit(self.monday_process_po_data, processed_items, detail_items, project_id)
                monday_future.add_done_callback(self.after_monday_done)

            if not self.USE_TEMP_FILE:
                self.cleanup_temp_file(temp_file_path)

        db_future.add_done_callback(after_db_done)

        # Return immediately. Processing now happens asynchronously.
        self.logger.info("🔧 DB and Monday processing dispatched to background threads.")

    def after_monday_done(self, fut):
        processed_items = fut.result()
        if not self.SKIP_FILE_SCAN:
            if processed_items:
                self.logger.info("🗂 Scanning PO folders for links...")
                for item in processed_items:
                    self.update_po_links(item["project_id"], item["PO"])

    def parse_po_log_file(self, temp_file_path: str, project_id: str):
        try:
            main_items, detail_items, contacts = self.po_log_processor.parse_showbiz_po_log(temp_file_path)
            self.logger.info(f"📝 Parsed PO Log for project {project_id}: {len(main_items)} main items, {len(detail_items)} detail items.")
            return main_items, detail_items, contacts
        except Exception as e:
            self.logger.exception(f"💥 Failed to parse PO Log: {e}", exc_info=True)
            return [], [], []

    def db_process_po_data(self, main_items, detail_items, contacts, project_id):
        self.logger.info("🔧 Processing PO data in the database...")
        processed_items = []

        # Process contacts in DB if needed
        self.process_contacts_in_db(contacts, project_id)

        for item in main_items:
            if self.DEBUG_STARTING_PO_NUMBER and int(item["PO"]) < self.DEBUG_STARTING_PO_NUMBER:
                self.logger.info(f"⏭ Skipping PO '{item['PO']}' due to debug start number.")
                continue

            # Populate contact details from DB
            item = self.populate_contact_details_from_db(item)

            # DB: find or create contact item
            item = self.po_log_database_util.find_or_create_contact_item_in_db(item)

            # DB: create or update main item
            item = self.po_log_database_util.create_or_update_main_item_in_db(item)

            # DB: handle detail items
            for sub_item in detail_items:
                if sub_item and sub_item["po_number"] == item["PO"]:
                    sub_item["po_surrogate_id"] = item["po_surrogate_id"]
                    sub_item["parent_status"] = item["status"]
                    self.po_log_database_util.create_or_update_sub_item_in_db(sub_item)

            processed_items.append(item)

        self.logger.info("✅ Database processing of PO data complete.")
        return processed_items

    def monday_process_po_data(self, processed_items, detail_items, project_id):
        self.logger.info("🌐 Processing PO data in Monday.com...")

        # Fetch all items in the group
        monday_items = monday_api.get_items_in_project(project_id=project_id)

        # Build a map from (project_id, po_number) to Monday item
        monday_items_map = {}
        for mi in monday_items:
            pid = mi["column_values"].get(monday_util.PO_PROJECT_ID_COLUMN)
            pono = mi["column_values"].get(monday_util.PO_NUMBER_COLUMN)
            if pid and pono:
                monday_items_map[(int(pid), int(pono))] = mi

        # Determine items to create or update
        items_to_create = []
        items_to_update = []

        for db_item in processed_items:
            p_id = int(db_item["project_id"])
            po_no = int(db_item["PO"])

            column_values_str = monday_util.po_column_values_formatter(
                project_id=db_item["project_id"],
                po_number=db_item["PO"],
                description=db_item.get("description"),
                contact_pulse_id=db_item.get("contact_pulse_id"),
                folder_link=db_item.get("folder_link"),
                status=db_item.get("contact_status"),
                producer_id=None,
                name=db_item['contact_name']
            )
            new_vals = json.loads(column_values_str)

            key = (p_id, po_no)
            if key in monday_items_map:
                monday_item = monday_items_map[key]
                differences = monday_util.is_main_item_different(db_item, monday_item)
                if differences:
                    self.logger.debug(f"Item differs for PO {po_no}. Differences: {differences}")
                    items_to_update.append({
                        "db_item": db_item,
                        "column_values": new_vals,
                        "monday_item_id": monday_item["id"]
                    })
                else:
                    self.logger.debug(f"No changes for PO {po_no}, skipping update. Differences: None")
            else:
                self.logger.debug(f"PO {po_no} does not exist on Monday, scheduling creation.")
                items_to_create.append({
                    "db_item": db_item,
                    "column_values": new_vals,
                    "monday_item_id": None
                })

        # Batch create main items
        if items_to_create:
            self.logger.info(f"🆕 Need to create {len(items_to_create)} main items on Monday.")
            created_mapping = monday_api.batch_create_or_update_items(items_to_create, project_id=project_id, create=True)
            for itm in created_mapping:
                db_item = itm["db_item"]
                monday_item_id = itm["monday_item_id"]
                self.po_log_database_util.update_main_item_pulse_id(db_item["project_id"], db_item["PO"], monday_item_id)
                db_item["pulse_id"] = monday_item_id
                p = int(db_item["project_id"])
                po = int(db_item["PO"])
                monday_items_map[(p, po)] = {
                    "id": monday_item_id,
                    "name": f"PO #{po}",
                    "column_values": itm["column_values"]
                }

        # Batch update main items
        if items_to_update:
            self.logger.info(f"✏️ Need to update {len(items_to_update)} main items on Monday.")
            updated_mapping = monday_api.batch_create_or_update_items(items_to_update, project_id=project_id, create=False)
            for itm in updated_mapping:
                db_item = itm["db_item"]
                monday_item_id = itm["monday_item_id"]
                self.po_log_database_util.update_main_item_pulse_id(db_item["project_id"], db_item["PO"], monday_item_id)
                db_item["pulse_id"] = monday_item_id
                p = int(db_item["project_id"])
                po = int(db_item["PO"])
                monday_items_map[(p, po)]["column_values"] = itm["column_values"]

        # Ensure all main items have pulse_ids
        for db_item in processed_items:
            p_id = int(db_item["project_id"])
            po_no = int(db_item["PO"])
            main_monday_item = monday_items_map.get((p_id, po_no))
            if main_monday_item and not db_item.get("pulse_id"):
                monday_item_id = main_monday_item["id"]
                updated = self.po_log_database_util.update_main_item_pulse_id(db_item["project_id"], db_item["PO"], monday_item_id)
                if updated:
                    db_item["pulse_id"] = monday_item_id
                    self.logger.info(f"🗂 Ensured main item PO {po_no} now has pulse_id {monday_item_id} in DB and processed_items.")

        # Handle sub-items
        for db_item in processed_items:
            p_id = int(db_item["project_id"])
            po_no = int(db_item["PO"])
            main_monday_item = monday_items_map.get((p_id, po_no))
            if not main_monday_item:
                self.logger.warning(f"❌ No Monday main item found for PO {po_no}, skipping subitems.")
                continue

            main_monday_id = main_monday_item["id"]
            sub_items_db = self.po_log_database_util.get_subitems(db_item["project_id"], db_item["PO"])

            monday_subitems = monday_api.get_subitems_for_item(main_monday_id)
            monday_sub_map = {}
            for msub in monday_subitems:
                identifiers = monday_util.extract_subitem_identifiers(msub)
                if identifiers is not None:
                    monday_sub_map[identifiers] = msub

            subitems_to_create = []
            subitems_to_update = []

            for sdb in sub_items_db:
                sub_col_values_str = monday_util.subitem_column_values_formatter(
                    project_id=sdb["project_id"],
                    po_number=sdb["po_number"],
                    detail_item_number=sdb["detail_item_number"],
                    line_id=sdb["line_id"],
                    notes=sdb.get("payment_type"),
                    status=sdb.get("state"),
                    description=sdb.get("description"),
                    quantity=sdb.get("quantity"),
                    rate=sdb.get("rate"),
                    date=sdb.get("transaction_date"),
                    due_date=sdb.get("due_date"),
                    account_number=sdb.get("account_number"),
                    link=sdb.get("file_link"),
                    OT=sdb.get("ot"),
                    fringes=sdb.get("fringes")
                )
                new_sub_vals = json.loads(sub_col_values_str)
                sub_key = (sdb["project_id"], sdb["po_number"], sdb["detail_item_number"], sdb["line_id"])

                if sub_key in monday_sub_map:
                    monday_sub = monday_sub_map[sub_key]
                    differences = monday_util.is_sub_item_different(sdb, monday_sub)
                    if differences:
                        self.logger.info(f"Sub-item differs for detail #{sdb['detail_item_number']} (PO {po_no}). Differences: {differences}")
                        subitems_to_update.append({
                            "db_sub_item": sdb,
                            "column_values": new_sub_vals,
                            "parent_id": main_monday_id,
                            "monday_item_id": monday_sub["id"]
                        })
                    else:
                        self.logger.debug(f"No changes for sub-item #{sdb['detail_item_number']} (PO {po_no}). Differences: None")
                        # Ensure pulse_id in DB
                        sub_pulse_id = monday_sub["id"]
                        self.po_log_database_util.update_detail_item_pulse_ids(
                            sdb["project_id"], sdb["po_number"], sdb["detail_item_number"], sdb["line_id"],
                            pulse_id=sub_pulse_id, parent_pulse_id=main_monday_id
                        )
                        sdb["pulse_id"] = sub_pulse_id
                        sdb["parent_pulse_id"] = main_monday_id
                else:
                    self.logger.debug(f"Sub-item #{sdb['detail_item_number']} (PO {po_no}) does not exist on Monday, scheduling creation.")
                    subitems_to_create.append({
                        "db_sub_item": sdb,
                        "column_values": new_sub_vals,
                        "parent_id": main_monday_id
                    })

            if subitems_to_create:
                self.logger.info(f"🆕 Need to create {len(subitems_to_create)} sub-items for PO {po_no}.")
                created_subs = monday_api.batch_create_or_update_subitems(subitems_to_create, parent_item_id=main_monday_id, create=True)
                for csub in created_subs:
                    db_sub_item = csub["db_sub_item"]
                    monday_subitem_id = csub["monday_item_id"]
                    self.po_log_database_util.update_detail_item_pulse_ids(
                        db_sub_item["project_id"], db_sub_item["po_number"], db_sub_item["detail_item_number"], db_sub_item["line_id"],
                        pulse_id=monday_subitem_id, parent_pulse_id=main_monday_id
                    )
                    db_sub_item["pulse_id"] = monday_subitem_id
                    db_sub_item["parent_pulse_id"] = main_monday_id

            if subitems_to_update:
                self.logger.info(f"✏️ Need to update {len(subitems_to_update)} sub-items for PO {po_no}.")
                updated_subs = monday_api.batch_create_or_update_subitems(subitems_to_update, parent_item_id=main_monday_id, create=False)
                for usub in updated_subs:
                    db_sub_item = usub["db_sub_item"]
                    monday_subitem_id = usub["monday_item_id"]
                    self.po_log_database_util.update_detail_item_pulse_ids(
                        db_sub_item["project_id"], db_sub_item["po_number"], db_sub_item["detail_item_number"], db_sub_item["line_id"],
                        pulse_id=monday_subitem_id, parent_pulse_id=main_monday_id
                    )
                    db_sub_item["pulse_id"] = monday_subitem_id
                    db_sub_item["parent_pulse_id"] = main_monday_id

        self.logger.info("✅ Monday.com processing of PO data complete.")
        return processed_items

    # ========== INVOICE PROCESSING ==========
    def process_invoice(self, dropbox_path: str):
        pass
        # Additional logic for invoices can be added here if needed.

    # ========== TAX FORM PROCESSING ==========
    def process_tax_form(self, path: str):
        self.logger.info(f"📜 Processing tax form: {path}")
        # Add parsing and DB logic here if needed.

    # ========== RECEIPT PROCESSING ==========
    def process_receipt(self, path: str):
        self.logger.info(f"🧾 Processing receipt: {path}")
        # Parse and DB/Monday logic if needed.

    # ========== BUDGET PROCESSING ==========
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

        # Extract project_id
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

            project_id_match = re.match(r"^\d{4}", project_folder)
            if not project_id_match:
                self.logger.info("❌ Can't determine Project ID.")
                return
            project_id = project_id_match.group()

            self.logger.info(f"🔑 Project ID: {project_id}")
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
                json={"project_id": project_id, "file_path": dropbox_path},
                timeout=10
            )
            if response.status_code == 200:
                job_id = response.json().get("job_id")
                self.logger.info(f"🎉 Triggered server job with job_id: {job_id}")
            else:
                self.logger.error(f"❌ Failed to trigger server job. Status: {response.status_code}, Response: {response.text}")
                return
        except Exception as e:
            self.logger.exception(f"💥 Error triggering server job: {e}", exc_info=True)
            return

        self.logger.info("✅ process_budget completed successfully, server job triggered with file URL.")

    # ========== HELPER METHODS ==========

    def process_contacts_in_db(self, contacts: list, project_id: str):
        # Handle contacts if needed
        pass

    def populate_contact_details_from_db(self, item: dict) -> dict:
        self.logger.debug("Loading contact from DB.")
        contact_dict = self.po_log_database_util.get_contact_by_name(name=item["contact_name"])
        if contact_dict:
            item['contact_pulse_id'] = None
            item['contact_payment_details'] = contact_dict['payment_details']
            item['contact_email'] = contact_dict['email']
            item['contact_phone'] = contact_dict['phone']
            item['address_line_1'] = contact_dict['address_line_1']
            item['city'] = contact_dict['city']
            item['zip'] = contact_dict['zip']
            item['tax_id'] = contact_dict['tax_id']
            item['tax_form_link'] = contact_dict['tax_form_link']
            item['contact_status'] = contact_dict['contact_status']
            item['contact_country'] = contact_dict['country']
            item['contact_tax_type'] = contact_dict['tax_type']
        else:
            self.logger.warning(f"No contact in DB for {item['name']}, using defaults.")
            keys = [
                'contact_pulse_id', 'contact_payment_details', 'contact_email', 'contact_phone',
                'address_line_1', 'city', 'zip', 'tax_id', 'tax_form_link', 'contact_status',
                'contact_country', 'contact_tax_type'
            ]
            for k in keys:
                item[k] = None
        return item

    def update_po_links(self, project_id, po_number):
        logger = self.logger
        logger.info(f"🚀Finding folder link for PO: {project_id}_{str(po_number).zfill(2)}")

        try:
            po_data = po_log_database_util.get_purchase_orders(project_id=project_id, po_number=po_number)
            if not po_data or not  len(po_data) > 0:
                logger.warning(f"❌ No PO data returned for PO={project_id}_{po_number.zfill(2)}, aborting link updates.")
                return
            po_data = po_data[0]
            if po_data["folder_link"]:
                logger.debug("Link already present: skipping")
                return

            project_id = po_data["project_id"]
            po_number = str(po_data["po_number"]).zfill(2)
            po_pulse_id = po_data.get("pulse_id")
            detail_items = po_data.get("detail_items", [])

            project_item = dropbox_api.get_project_po_folders_and_links(project_id=project_id, po_number=po_number)
            if not project_item or len(project_item < 1):
                logger.warning(f"⚠️ Could not determine project folder name for {project_id}, no links will be found.")
                return
            project_item = project_item[0]
            po_folder_link = project_item["po_folder_link"]
            po_folder_name = project_item["po_folder_name"]
            logger.debug(f"Project folder name retrieved: '{po_folder_name}'")

            logger.debug(
                f"Link update for PO: {po_folder_name}', "
                f"detail_items_count={len(detail_items)}"
            )

            if po_folder_link:
                logger.info(f"✅ Folder link found")
                po_log_database_util.update_po_folder_link(project_id=project_id, po_number=po_number, folder_link=po_folder_link)
                if not self.SKIP_MONDAY and po_pulse_id:
                    col_values = {monday_util.PO_FOLDER_LINK_COLUMN_ID: {"url": po_folder_link, "text": "📦"}}
                    monday_util.update_item_columns(po_pulse_id, col_values)
            else:
                logger.warning("⚠️ No folder link found.")

            logger.info(f"🎉 Completed background linking for PO: {project_id}_{po_number.zfill(2)}")

        except Exception as e:
            logger.error("💥 Error in update_po_links_in_background:", exc_info=True)
            traceback.print_exc()

    def parse_invoice_filename(self, filename: str):
        base_name = os.path.basename(filename)
        name_part = os.path.splitext(base_name)[0]
        parts = name_part.split("_")

        project_id = parts[0]
        po_number = parts[1]
        invoice_number = 1
        if len(parts) > 2 and parts[2].isdigit():
            invoice_number = int(parts[2])
        return project_id, po_number, invoice_number

    def extract_text_from_pdf(self, file_data: bytes) -> str:
        pdf_text = self.dropbox_util.extract_text_from_pdf(file_data)
        if pdf_text and pdf_text.strip():
            return pdf_text

        self.logger.warning("⚠️ No direct text from PDF, attempting OCR...")
        text_via_ocr = self.dropbox_util.extract_text_with_ocr(file_data)
        return text_via_ocr

    def process_invoice_with_openai(self, text: str):
        info, error = self.dropbox_util.extract_info_with_openai(text)
        if error:
            self.logger.warning(f"⚠️ OpenAI invoice process error: {error}")
            return None
        return info

    def download_file_from_dropbox(self, path: str, temp_file_path: str) -> bool:
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

    def cleanup_temp_file(self, temp_file_path: str):
        try:
            if os.path.exists(temp_file_path):
                os.remove(temp_file_path)
                self.logger.info(f"🧹 Cleaned up temp file {temp_file_path}")
        except Exception as e:
            self.logger.warning(f"⚠️ Could not remove temp file: {e}")

    def extract_project_id(self, file_name: str) -> str:
        digit_sequences = re.findall(r'\d+', file_name)
        if not digit_sequences:
            raise ValueError(f"❗ No digits in '{file_name}' for project_id.")
        all_digits = ''.join(digit_sequences)
        if len(all_digits) < 4:
            raise ValueError(f"❗ '{file_name}' doesn't have 4 digits for project_id.")
        project_id = all_digits[:4]
        return project_id

    def scan_and_process_po_folders(self, processed_items: list):
        self.logger.info("🔎 Scanning PO folders for additional files...")

        project_po_file_data = self.dropbox_api.get_project_po_folders_and_links(processed_items[0]["project_id"])

        for item in processed_items:
            project_id = item["project_id"]
            po_number = item["PO"]
            po_number_padded = self.PO_NUMBER_FORMAT.format(int(po_number))

            # Find matching folder
            vendor_name = item.get("contact_name") or "Unknown_Vendor"
            project_folder_name = self.po_log_database_util.get_project_folder_name(project_id)
            if not project_folder_name:
                self.logger.warning(f"⚠️ No project folder name found for {project_id}. Skipping PO {po_number}.")
                continue

            # Construct PO folder path
            po_folder_path = f"/{project_folder_name}/1. Purchase Orders/{project_id}_{po_number_padded} {vendor_name}"
            self.logger.debug(f"Checking PO folder: {po_folder_path}")

            # Create or retrieve the share link for the PO folder
            folder_link = self.dropbox_api.create_share_link(po_folder_path)
            if folder_link:
                self.logger.info(f"✅ Got share link for PO folder: {folder_link}")
                # Update DB with the folder link
                if "PO" in item and item["PO"]:
                    self.po_log_database_util.update_po_folder_link(project_id, item["PO"], folder_link)

                # Update Monday if not skipping
                if not self.SKIP_MONDAY and "pulse_id" in item and item["pulse_id"]:
                    col_values = {
                        self.monday_util.PO_FOLDER_LINK_COLUMN_ID: {
                            "url": folder_link,
                            "text": "📦"
                        }
                    }
                    self.monday_util.update_item_columns(item["pulse_id"], col_values)
            else:
                self.logger.warning(f"⚠️ Could not get share link for PO folder: {po_folder_path}")

            # Continue scanning files in the folder
            files_in_folder = self.dropbox_api.list_folder_contents(po_folder_path)
            for file_name in files_in_folder:
                full_path = f"{po_folder_path}/{file_name}"

                # Determine file type as before
                if re.search(self.INVOICE_REGEX, file_name, re.IGNORECASE):
                    self.logger.info(f"🧭 Found invoice file: {file_name}")
                    self.process_invoice(full_path)
                elif re.search(self.TAX_FORM_REGEX, file_name, re.IGNORECASE):
                    self.logger.info(f"🧭 Found tax form file: {file_name}")
                    self.process_tax_form(full_path)
                elif re.search(self.RECEIPT_REGEX, file_name, re.IGNORECASE):
                    self.logger.info(f"🧭 Found receipt file: {file_name}")
                    self.process_receipt(full_path)
                else:
                    self.logger.debug(f"❌ Unsupported or irrelevant file type: {file_name}. Skipping.")


dropbox_service = DropboxService()