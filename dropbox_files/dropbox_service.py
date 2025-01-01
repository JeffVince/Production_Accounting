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

#region Imports
import json
import os
import re
import traceback
import logging
from concurrent.futures import ThreadPoolExecutor, wait, ALL_COMPLETED, as_completed
from typing import Optional

from models import Contact

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

# Import the updated DB ops:
from database.database_util import DatabaseOperations
#endregion


class DropboxService(metaclass=SingletonMeta):
    """
    📦 DropboxService
    =================
    Singleton class that coordinates processing of files from Dropbox, with
    a strong focus on PO logs, contacts, and purchase orders.
    """

    #region Class/Static Members
    PO_LOG_FOLDER_NAME = "1.5 PO Logs"
    PO_NUMBER_FORMAT = "{:02}"
    INVOICE_REGEX = r"invoice"
    TAX_FORM_REGEX = r"w9|w8-ben|w8-ben-e"
    RECEIPT_REGEX = r"receipt"
    SHOWBIZ_REGEX = r".mbb"
    PROJECT_NUMBER = ""

    USE_TEMP_FILE = False         # Whether to use a local temp file
    DEBUG_STARTING_PO_NUMBER = 0 # If set, skip POs below this number
    SKIP_DATABASE = False
    ADD_PO_TO_MONDAY = True
    GET_FOLDER_LINKS = False
    GET_TAX_LINKS = False
    GET_CONTACTS = False

    executor = ThreadPoolExecutor(max_workers=5)
    #endregion

    #region Initialization
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

            self.logger.info("📦 Dropbox Service initialized 🌟")
            self._initialized = True
    #endregion

    #region Type Determination
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
    #endregion

    #region Event Processing - Budget
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
                self.logger.error(f"❌ Failed to trigger server job. Status: {response.status_code}, Response: {response.text}")
                return
        except Exception as e:
            self.logger.exception(f"💥 Error triggering server job: {e}", exc_info=True)
            return

        self.logger.info("✅ process_budget completed successfully, server job triggered with file URL.")
    #endregion

    #region  Event Processing - PO Log - Step 1 -  Add PO LOG data to DB
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
                    item["project_number"],  # real numeric ID from project table
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

                existing_detail = self.database_util.search_detail_item_by_keys(
                    project_number=sub_item["project_number"],
                    po_number=sub_item["po_number"],
                    detail_number=sub_item["detail_item_id"],
                    line_id=sub_item["line_id"]
                )



                # -----------------------
                # NEW: Check Payment Status
                # -----------------------
                # If existing detail item already has a payment status in ["PAID", "Logged", "Reconciled"],
                # we want to skip any further DB or Monday.com update.
                PAYMENT_COMPLETE_STATUSES = ["PAID", "LOGGED", "RECONCILED"]

                if not existing_detail:
                    # Not in DB yet -> create it
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
                        state=sub_item["state"]
                    )
                    if created_di:
                        self.logger.debug(f"✔️ Created DetailItem id={created_di['id']} for PO={sub_item['po_number']}")

                elif isinstance(existing_detail, list):
                    # Multiple found; typically you’d handle merging or pick one.
                    first_detail = existing_detail[0]
                    payment_status = first_detail.get("state", "").upper()
                    if payment_status in [s.upper() for s in PAYMENT_COMPLETE_STATUSES]:
                        self.logger.debug(
                            f"⚠️ DetailItem id={first_detail['id']} has payment_status={payment_status}, skipping update."
                        )
                        continue
                    # Otherwise, proceed with update
                    self.logger.debug(
                        f"🔎 Found multiple detail items, updating the first (id={first_detail['id']})."
                    )
                    self.database_util.update_detail_item_by_keys(
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
                        state=sub_item["state"]
                    )

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
                    self.database_util.update_detail_item_by_keys(
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
                        state=sub_item["state"]
                    )

            except Exception as ex:
                self.logger.error(
                    f"💥 Error processing DetailItem for PO={sub_item['po_number']}: {ex}",
                    exc_info=True
                )

        self.logger.info("✅ Database processing of PO data complete.")
        return processed_items
        #endregion
    #endregion

    #region Event Processing - PO Log - Step 2 -  Add Folder & Tax Links + Contact Data
    def callback_add_po_data_to_DB(self, fut):
        """
        Callback for when the DB process is complete.
        Possibly triggers Monday or Dropbox tasks next.
        """
        try:
            processed_items = fut.result()

            #region GET DROPBOX FOLDER LINK
            if self.GET_FOLDER_LINKS:
                self.logger.info("SYNCING PO FOLDER LINKS")
                def get_folder_links(processed_items):
                    for item in processed_items:
                        db_project = self.database_util.search_projects(["project_number"], [item["project_number"]])
                        if isinstance(db_project, list):
                            db_project = db_project[0]
                        db_po = self.database_util.search_purchase_orders(["project_id", "po_number"], [db_project["id"], item["po_number"]])
                        if isinstance(db_po, list):
                            db_po = db_po[0]
                        if db_po["folder_link"]:
                            self.logger.debug(f"✅Folder Link is already present for {db_project['project_number']}_{db_po['po_number']}")
                        else:
                            self.logger.debug(f"Folder link not found in DB -- retrieving from Dropbox")
                            self.update_po_folder_link(item["project_number"], item["po_number"])
                folder_links_future = self.executor.submit(get_folder_links, processed_items)
            #endregion

            #region TAX LINKS
            if self.GET_TAX_LINKS:
                def get_tax_links(processed_items):
                    for idx, item in enumerate(processed_items):
                        db_contact = self.database_util.search_contacts(["id"], [item.get("contact_id")])
                        if db_contact and db_contact["vendor_type"] == "Vendor":
                            tax_link = db_contact["tax_form_link"]
                            if not tax_link:
                                # Contact has no link
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
            #endregion

            futures_to_wait = []

            if  self.GET_FOLDER_LINKS and folder_links_future:
                futures_to_wait.append(folder_links_future)
            if  self.GET_TAX_LINKS and tax_links_future:
                futures_to_wait.append(tax_links_future)

            # Block until both tasks are done
            if futures_to_wait:
                wait(futures_to_wait, return_when=ALL_COMPLETED)

            # Now that both have finished, we can call create_pos_in_monday
            if processed_items:
                # e.g., just pick the first item’s project_number
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

    #region   DROPBOX FOLDER LINK
    def update_po_folder_link(self, project_number, po_number):
        logger = self.logger
        logger.info(f"🚀Finding folder link for PO: {project_number}_{str(po_number).zfill(2)}")

        try:
            po_data = self.database_util.search_purchase_order_by_keys(project_number, po_number)
            if not po_data or not len(po_data) > 0:
                logger.warning(
                    f"❌ No PO data returned for PO={project_number}_{po_number.zfill(2)}, aborting link updates.")
                return
            po_data = po_data
            if po_data["folder_link"]:
                logger.debug("Link already present: skipping")
                return

            project_item = dropbox_api.get_project_po_folders_with_link(project_number=project_number, po_number=po_number)
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

            logger.info(f"🎉 Completed folder linking for: {project_number}_{po_number.zfill(2)}")

        except Exception as e:
            logger.error("💥 Error linking dropbox folder:", exc_info=True)
            traceback.print_exc()
    #endregion

    #region   TAX FORM LINK
    def update_po_tax_form_links(self, project_number, po_number):
        """
        🚀 Update or set the tax_form_link for a PurchaseOrder in Dropbox if needed.
        Stub method to illustrate how you'd update the 'tax_form_link' column.
        """
        try:
            po_search = self.database_util.search_purchase_order_by_keys(project_number, po_number)
            if not po_search or not po_search["po_type"] == "INV" :
                return None
            if isinstance(po_search, dict):
                contact_id = po_search["contact_id"]
            elif isinstance(po_search, list) and po_search:
                contact_id = po_search[0]["contact_id"]
            else:
                return None

            new_tax_form_link = self.dropbox_api.get_po_tax_form_link(project_number=project_number, po_number=po_number)[0]["po_tax_form_link"]
            self.database_util.update_contact(contact_id, tax_form_link=new_tax_form_link)
            self.logger.info(f"📑 Updated tax form link for PO {project_number}_{po_number} => {new_tax_form_link}")
            return new_tax_form_link
        except Exception as e:
            self.logger.error(f"💥 Could not update PO tax form link for {project_number}_{po_number}: {e}",
                              exc_info=True)
        # endregion
    #endregion
    #endregion

    #region Event Processing - PO Log - Step 3 - Monday Processing
    def create_pos_in_monday(self, project_number):
        """
        Demonstrates how to fetch all subitems once from Monday,
        then process them locally to avoid multiple queries.
        """

        self.logger.info("🌐 Processing PO data in Monday.com...")

        # -------------------------------------------------------------------------
        # 1) FETCH MAIN ITEMS FROM MONDAY & FROM DB
        # -------------------------------------------------------------------------
        monday_items = monday_api.get_items_in_project(project_id=project_number)
        processed_items = self.database_util.search_purchase_order_by_keys(
            project_number=project_number
        )

        # Build a map for main items
        monday_items_map = {}
        for mi in monday_items:
            pid = mi["column_values"].get(monday_util.PO_PROJECT_ID_COLUMN)["text"]
            pono = mi["column_values"].get(monday_util.PO_NUMBER_COLUMN)["text"]
            if pid and pono:
                monday_items_map[(int(pid), int(pono))] = mi

        # -------------------------------------------------------------------------
        # 2) FETCH ALL SUBITEMS AT ONCE (FOR ENTIRE SUBITEM BOARD)
        # -------------------------------------------------------------------------
        # Let's assume you have a method like monday_api.get_subitems_in_board()
        # that returns all subitems in one shot from the subitem board.
        # Or you could call a special query if Monday supports this for you.
        # You need to have a “subitem board” or some method to get all subitems.
        all_subitems = monday_api.get_subitems_in_board(project_number=project_number)
        # Example structure of each subitem: {
        #   "id": "...",
        #   "column_values": {...},
        #   "parent_id": "...",
        #   ...
        # }

        # -------------------------------------------------------------------------
        # 3) BUILD A GLOBAL DICTIONARY FOR SUBITEM LOOKUP
        #    Keyed by (project_number, po_number, detail_number, line_id)
        # -------------------------------------------------------------------------
        global_subitem_map = {}
        for msub in all_subitems:
            # Extract your custom identifiers (like project_number, po_number, detail_number, line_id)
            # from the msub's column_values or however you store them.
            # For instance, if you store them in “column_values”, you might do:
            identifiers = monday_util.extract_subitem_identifiers(msub)
            # Suppose that returns (project_number, po_number, detail_number, line_id)
            # Or you might store them in columns or in the item name, etc.

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

            column_values_str = monday_util.po_column_values_formatter(
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
                # This main item exists in Monday => check if an update is needed
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
                    self.logger.debug(f"No changes for PO {po_no}, skipping update.")
            else:
                # Not in Monday => schedule creation
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
            created_mapping = monday_api.batch_create_or_update_items(
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
            updated_mapping = monday_api.batch_create_or_update_items(
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
        # 6) CREATE/UPDATE SUBITEMS WITHOUT FETCHING FROM MONDAY AGAIN
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

            # Build subitems_to_create and subitems_to_update from the *global* subitem map
            subitems_to_create = []
            subitems_to_update = []

            # Normalize to a list
            if isinstance(sub_items_db, dict):
                sub_items_db = [sub_items_db]

            for sdb in sub_items_db:
                sdb["account_code"] = self.database_util.search_aicp_codes(["id"], [sdb.get("aicp_code_id")])["aicp_code"]


                sub_col_values_str = monday_util.subitem_column_values_formatter(
                    project_id=project_number,
                    po_number=db_item["po_number"],
                    detail_item_number=sdb["detail_number"],
                    line_id=sdb["line_id"],
                    notes=sdb.get("payment_type"),
                    status=sdb.get("state"),
                    description=sdb.get("description"),
                    quantity=sdb.get("quantity"),
                    rate=sdb.get("rate"),
                    date=sdb.get("transaction_date"),
                    due_date=sdb.get("due_date"),
                    account_number=sdb["account_code"],
                    link=sdb.get("file_link"),
                    OT=sdb.get("ot"),
                    fringes=sdb.get("fringes")
                )
                new_sub_vals = json.loads(sub_col_values_str)

                sub_key = (
                    project_number,
                    db_item["po_number"],
                    sdb["detail_number"],
                    sdb["line_id"]
                )

                # Check if it exists in the global map
                if sub_key in global_subitem_map:
                    # Subitem already exists in Monday => see if there's a difference
                    msub = global_subitem_map[sub_key]
                    sdb["project_number"] = db_item["project_number"]
                    sdb["po_number"] = db_item["po_number"]
                    differences = monday_util.is_sub_item_different(sdb, msub)
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
                        # No changes, but ensure pulse_id in DB
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
                    # Need to create it
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

    # ---------------------------------------------------------------------
    # Below: same subitem batch-creation code from your snippet (unchanged)
    # ---------------------------------------------------------------------
    def _batch_create_subitems(self, subitems_to_create, parent_item_id, project_number, db_item):
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

        # Update DB
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

        # Update DB
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

    #endregion


    #region Event Processing - Receipts
    def process_receipt(self, dropbox_path: str):
        """
        🧾 process_receipt
        -----------------
        1) Parse file name (project_number, po_number, detail_item_number, vendor_name).
        2) Download the receipt file from Dropbox.
        3) If PDF, try text extraction via PyPDF2. If that fails (or not a PDF), do OCR.
        4) Use OCRService's 'extract_receipt_info_with_openai' to parse total, date, description.
        5) Generate file link in Dropbox.
        6) Create or update the 'receipt' table, linking to the appropriate detail_item.
        7) Update the corresponding subitem in Monday with the link.
        """

        self.logger.info(f"🧾 Processing receipt: {dropbox_path}")
        temp_file_path = f"./temp_files/{os.path.basename(dropbox_path)}"

        # ----------------------------------------------------------------------
        # 1) PARSE FILE NAME FOR PROJECT #, PO #, DETAIL #, VENDOR
        #    Example filename: "2416_02_07 Vendor_Name Receipt.pdf"
        #    We expect something like:
        #      group(1) = 2416 (project_number)
        #      group(2) = 02   (po_number)
        #      group(3) = 07   (detail_item_number)
        #      group(4) = Vendor_Name
        #      group(5) = pdf|jpg|jpeg|png
        # ----------------------------------------------------------------------
        filename = os.path.basename(dropbox_path)
        pattern = r'^(\d{4})_(\d{2})_(\d{2})\s+(.*?)\s+Receipt\.(pdf|jpe?g|png)$'
        match = re.match(pattern, filename, re.IGNORECASE)
        if not match:
            self.logger.warning(f"❌ Filename '{filename}' does not match receipt pattern.")
            return

        project_number_str = match.group(1)  # e.g. "2416"
        po_number_str = match.group(2).lstrip("0")  # e.g. "02" -> "2"
        detail_item_str = match.group(3).lstrip("0")  # e.g. "07" -> "7"
        vendor_name = match.group(4)  # e.g. "Vendor_Name"
        file_ext = match.group(5).lower()

        # Convert strings to int
        project_number = int(project_number_str)
        po_number = int(po_number_str)
        detail_item_number = int(detail_item_str)

        try:
            # 2) DOWNLOAD THE FILE FROM DROPBOX (IN MEMORY) → boolean success
            success = self.download_file_from_dropbox(dropbox_path, temp_file_path)
            if not success:
                self.logger.warning(f"🛑 Failed to download receipt: {filename}")
                return

            # Read the file data from the temporary file
            with open(temp_file_path, 'rb') as f:
                file_data = f.read()

            # 3) EXTRACT TEXT — PDF FIRST, THEN OCR
            extracted_text = ""
            if file_ext == "pdf":
                # Attempt PDF extraction
                extracted_text = self._extract_text_from_pdf(file_data)
                if not extracted_text.strip():
                    self.logger.info("PDF extraction found no text; falling back to OCR.")
                    extracted_text = self._extract_text_via_ocr(file_data)
            else:
                # Not a PDF → go straight to OCR
                extracted_text = self._extract_text_via_ocr(file_data)

            if not extracted_text.strip():
                self.logger.warning(f"🛑 No text extracted from receipt: {filename}")
                self.cleanup_temp_file(temp_file_path)
                return

            # 4) PARSE RECEIPT DETAILS (OpenAI)
            ocr_service = OCRService()
            receipt_info = ocr_service.extract_receipt_info_with_openai(extracted_text)
            if not receipt_info:
                self.logger.warning(f"🛑 Could not parse receipt info from AI for: {filename}.")
                self.cleanup_temp_file(temp_file_path)
                return

            # receipt_info is expected to have {"total_amount", "description", "date"}
            total_amount = receipt_info.get("total_amount", 0.0)
            purchase_date = receipt_info.get("date", "")
            short_description = receipt_info.get("description", "")  # up to 20 chars

            # 5) GENERATE DROPBOX SHARE LINK
            try:
                shared_link_metadata = self.dropbox_util.get_file_link(dropbox_path)
                # Optionally modify the link to make it directly downloadable
                file_link = shared_link_metadata.replace("?dl=0", "?dl=1")
            except Exception as e:
                self.logger.warning(f"❌ Could not generate shareable link for {dropbox_path}: {e}")
                file_link = None

            # 6) LOOK UP DETAIL ITEM (WE NEED ITS ID)
            existing_detail = self.database_util.search_detail_item_by_keys(
                str(project_number),
                po_number,
                detail_number=detail_item_number
            )
            if not existing_detail:
                self.logger.warning(
                    f"❗ No matching detail item found for {project_number_str}_{po_number_str}_{detail_item_str}."
                )
                self.cleanup_temp_file(temp_file_path)
                return
            else:
                # If multiple detail items are returned, handle accordingly. We'll assume a single record for brevity.
                if isinstance(existing_detail, list):
                    existing_detail = existing_detail[0]

            detail_item_id = existing_detail["id"]

            # 6A) CREATE OR UPDATE A RECEIPT RECORD
            #     We'll assume you have a "create_receipt" or "update_receipt" method in your DB utility
            #     that returns the newly created or updated row as a dict.
            #     Also note that 'spend_money_id' must be non-null, so you can set it to a default or something meaningful.
            spend_money_id = 1  # <--- Replace with your logic to get a valid spend_money row, if necessary

            # Check if there's already a receipt for this detail_item_id
            existing_receipts = self.database_util.search_receipts(["detail_item_id"], [detail_item_id])
            if not existing_receipts:
                # Create a new row in the 'receipt' table
                new_receipt = self.database_util.create_receipt(
                    detail_item_id=detail_item_id,
                    spend_money_id=spend_money_id,
                    total=total_amount,
                    purchase_date=purchase_date,
                    receipt_description=short_description,
                    file_link=file_link
                )
                receipt_id = new_receipt["id"] if new_receipt else None
                self.logger.info(f"🔄 Created new receipt with ID={receipt_id} for detail_item_id={detail_item_id}")
            else:
                # Potentially update the first existing receipt or handle merges
                existing_receipt = existing_receipts
                receipt_id = existing_receipt["id"]
                updated_receipt = self.database_util.update_receipt_by_id(
                    receipt_id=receipt_id,
                    total=total_amount,
                    purchase_date=purchase_date,
                    receipt_description=short_description,
                    file_link=file_link
                )
                self.logger.info(
                    f"✏️ Updated existing receipt with ID={receipt_id} for detail_item_id={detail_item_id}")

            # 7) UPDATE CORRESPONDING SUBITEM IN MONDAY
            #    Use your Monday API to find the subitem that matches (project_number, po_number, detail_item_number).
            #    Then call 'update_item(...)' with `type='subitem'` to insert the link.
            column_values_str = monday_util.subitem_column_values_formatter(
                link=file_link
            )
            new_vals = column_values_str

            monday_subitem = self.monday_api.update_item(
                item_id=existing_detail["pulse_id"],
                column_values=new_vals,
                type="subitem"
            )
            if not monday_subitem:
                self.logger.warning(
                    f"❌ No Monday subitem found for detail {detail_item_str} in project {project_number_str}, PO {po_number_str}."
                )
            else:
                subitem_id = monday_subitem["data"]["change_multiple_column_values"]["id"]

                column_values_str = monday_util.subitem_column_values_formatter(
                    link=file_link
                )
                new_vals = column_values_str

                # Now update the subitem board's item with the link
                self.monday_api.update_item(
                    item_id=subitem_id,
                    column_values=new_vals,
                    type="subitem"
                )

            # Clean up the temp file
            self.cleanup_temp_file(temp_file_path)

            self.logger.info(f"✅ Receipt processing complete for: {dropbox_path}")

        except Exception as e:
            self.logger.exception(f"💥 Error processing receipt {filename}: {e}", exc_info=True)
            return    #endregion

    #region Helpers
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

        # Remove all hyphens
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
        # region 🏗 Extract Project ID
        digit_sequences = re.findall(r'\d+', file_name)

        if not digit_sequences:
            raise ValueError(f"❗ No digits found in file name: '{file_name}' ❗")

        all_digits = ''.join(digit_sequences)

        if len(all_digits) < 4:
            raise ValueError(f"❗ File name '{file_name}' does not contain at least four digits for project_id. ❗")

        project_number = all_digits[-4:]
        # endregion
        return project_number

    def _extract_text_from_pdf(self, file_data: bytes) -> str:
        """
        Attempt to extract text from a PDF file using PyPDF2.
        Returns the extracted text if successful, or an empty string otherwise.
        """
        import PyPDF2
        from io import BytesIO

        try:
            pdf_reader = PyPDF2.PdfReader(BytesIO(file_data))
            all_text = []
            for page in pdf_reader.pages:
                page_text = page.extract_text()
                if page_text:
                    all_text.append(page_text)
            return "\n".join(all_text)
        except Exception as e:
            self.logger.warning(f"🛑 PDF text extraction failed: {e}")
            return ""

    def _extract_text_via_ocr(self, file_data: bytes) -> str:
        """
        Fallback OCR extraction using the existing OCRService.
        """
        from ocr_service import OCRService
        ocr_service = OCRService()
        return ocr_service.extract_text_from_receipt(file_data)

    #endregion

# Singleton instance
dropbox_service = DropboxService()