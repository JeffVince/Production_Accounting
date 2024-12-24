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
from concurrent.futures import ThreadPoolExecutor, wait, ALL_COMPLETED
from typing import Optional

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

    USE_TEMP_FILE = True         # Whether to use a local temp file
    DEBUG_STARTING_PO_NUMBER = 0 # If set, skip POs below this number
    SKIP_DATABASE = False
    ADD_PO_TO_MONDAY = False
    GET_FOLDER_LINKS = False
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

            # Our new DatabaseOperations instance for all DB logic
            self.database_util = DatabaseOperations(self.logger)

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

        for i, item in enumerate(main_items):
            try:
                # 1) Debug skip check
                if self.DEBUG_STARTING_PO_NUMBER and int(item["po_number"]) < self.DEBUG_STARTING_PO_NUMBER:
                    self.logger.info(f"⏭ Skipping PO '{item['po_number']}' due to debug start number.")
                    continue

                # 2) Find or create Contact
                contact_name = item["contact_name"]
                self.logger.info(f"🔍 Checking for Contact: {contact_name}")
                contact_search = self.database_util.search_contacts(["name"], [contact_name])

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
                    new_contact = self.database_util.create_contact(
                        name=contact_name,
                        vendor_type=contacts[i].get("vendor_type", "Vendor"),
                    )
                    contact_id = new_contact["id"] if new_contact else None

                if not contact_id:
                    self.logger.warning(f"⚠️ Could not determine contact_id for contact '{contact_name}'; skipping PO creation.")
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
                    if new_proj:
                        continue
                    else:
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
                    updated_po = self.database_util.update_purchase_order_by_keys(
                        project_number=item["project_number"],
                        po_number=item["po_number"],
                        description=item.get("description"),
                        po_type=item.get("po_type"),
                        contact_id=contact_id # Link to the contact
                    )
                else:
                    # Single record found
                    po_surrogate_id = po_search["id"]
                    updated_po = self.database_util.update_purchase_order_by_keys(
                        project_number=item["project_number"],
                        po_number=item["po_number"],
                        description=item.get("description"),
                        po_type=item.get("po_type"),
                        contact_id=contact_id  # Link to the contact
                    )

                item["po_surrogate_id"] = po_surrogate_id
                processed_items.append(item)

            except Exception as ex:
                self.logger.error(
                    f"💥 Error processing main item (PO={item.get('po_number')}): {ex}",
                    exc_info=True
                )

        # 5) Create or update Detail Items
        self.logger.info("🔧 Creating/Updating Detail Items for each PO...")
        for sub_item in detail_items:
            try:
                if self.DEBUG_STARTING_PO_NUMBER and int(sub_item["po_number"]) < self.DEBUG_STARTING_PO_NUMBER:
                    self.logger.debug(f"⏭ Skipping detail item for PO '{sub_item['po_number']}' due to debug start number.")
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

                parent_status = main_match.get("status", "PENDING")
                final_detail_state = "RTP" if parent_status == "RTP" else "PENDING"

                if not existing_detail:
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
                        state=final_detail_state
                    )
                    if created_di:
                        self.logger.debug(
                            f"✔️ Created DetailItem id={created_di['id']} for PO={sub_item['po_number']}"
                        )
                elif isinstance(existing_detail, list):
                    first_detail = existing_detail[0]
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
                        state=final_detail_state
                    )
                else:
                    # Exactly one found
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
                        state=final_detail_state
                    )
            except Exception as ex:
                self.logger.error(
                    f"💥 Error processing DetailItem for PO={sub_item['po_number']}: {ex}",
                    exc_info=True
                )

        self.logger.info("✅ Database processing of PO data complete.")
        return processed_items
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
                        self.update_po_folder_link(item["project_number"], item["po_number"])
                folder_links_future = self.executor.submit(get_folder_links, processed_items)
            #endregion

            #region GET CONTACT DATA & TAX LINKS
            if self.GET_CONTACTS:
                self.logger.info("SYNCING CONTACTS")

                # 1) Fetch all Monday contacts once
                all_monday_contacts = self.monday_api.fetch_all_contacts(self.monday_util.CONTACT_BOARD_ID)

                def get_contact_data(processed_items, all_monday_contacts):
                    self.logger.debug(f"Starting get_contact_data with {len(processed_items)} items")

                    for idx, item in enumerate(processed_items):
                        cname = item.get("contact_name", "")
                        self.logger.debug(f"Item {idx + 1}/{len(processed_items)} → contact_name='{cname}'")

                        try:
                            # (A) Sync DB contact with Monday first
                            db_contact = self.sync_contacts_to_DB(item, all_monday_contacts)
                            # Suppose this returns the updated DB contact record or None

                            # (B) If Monday is missing the tax form link, fetch a new one
                            if db_contact and db_contact["vendor_type"] == "Vendor":
                                monday_tax_link = self.monday_util._extract_tax_link_from_monday(
                                    db_contact.get("pulse_id"),
                                    all_monday_contacts
                                )
                                if not monday_tax_link:
                                    # Monday has no link → let’s fill it
                                    self.logger.debug(
                                        f"Monday is missing the tax form link for Contact {db_contact['id']}. "
                                        f"Fetching a new link from Dropbox..."
                                    )
                                    monday_tax_link = self.update_po_tax_form_links(
                                        item["project_number"],
                                        item["po_number"]
                                    )
                                    if monday_tax_link:
                                        self.monday_api._update_monday_tax_form_link(db_contact["pulse_id"], monday_tax_link)
                                    else:
                                        self.logger.error("❌ - Unable to get link from contact")
                                else:
                                    self.logger.debug(
                                        f"✅ Monday tax_form_link is already present for {db_contact['name']}"
                                    )

                        except Exception as e:
                            self.logger.exception(f"Exception processing contact '{cname}'", exc_info=e)

                    self.logger.debug("Finished get_contact_data")

                sync_contact_future = self.executor.submit(get_contact_data, processed_items, all_monday_contacts)

                try:
                    # If an exception was raised in get_contact_data, calling .result() will raise it here
                    sync_contact_future.result()
                    self.logger.info("Contact sync thread finished.")
                except Exception as exc:
                    self.logger.exception("Error occurred while syncing contacts", exc_info=exc)
            #endregion


            futures_to_wait = []

            if  self.GET_FOLDER_LINKS and folder_links_future:
                futures_to_wait.append(folder_links_future)
            if  self.GET_CONTACTS and sync_contact_future:
                futures_to_wait.append(sync_contact_future)

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
                    self.create_pos_in_monday(project_number)
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

    #region CONTACT SYNC
    def sync_contacts_to_DB(self, processed_item: dict, all_monday_contacts: list):
        """
        Example coordinator function that:
          1) Looks up (or creates) a DB Contact by name
          2) Checks for any near-match in the given all_monday_contacts
          3) If found, update DB with Monday columns
          4) If not found, create in Monday & store pulse_id
        """
        # region 🏁 Setup
        contact_name = processed_item.get("contact_name", "").strip()
        if not contact_name:
            self.logger.warning("⚠️ No contact_name in po_data, skipping.")
            return None

        self.logger.info(f"📦 Handling new PO with contact_name='{contact_name}'.")
        # endregion

        # region 1) Find/Create DB Contact
        db_contact = self.database_util.find_contact_by_name(contact_name)
        if not db_contact:
            # db_contact is either None or a dict with serialized fields
            db_contact = self.database_util.create_minimal_contact(contact_name)
            if not db_contact:
                self.logger.error(
                    f"💥 Could not create a new DB Contact for '{contact_name}'. Aborting."
                )
                return None
            else:
                self.logger.info(f"✅ Created new DB contact (id={db_contact['id']}) for '{contact_name}'.")
        else:
            self.logger.info(f"✅ Found existing DB contact (id={db_contact['id']}) for '{contact_name}'.")
        # endregion

        # region 2) Check Monday (using the provided all_monday_contacts)
        try:
            matched_contact = self._find_monday_contact_match(contact_name, all_monday_contacts)
        except Exception as e:
            self.logger.error(f"Error finding match in Monday.com: {e}")
            raise e
        if matched_contact:
            # region 3) If found, pull Monday fields => update DB
            monday_fields = self.monday_api.extract_monday_contact_fields(matched_contact)

            # Parse tax_number if any
            tax_number_int = None
            if monday_fields["tax_number_str"]:
                tax_number_int = self.database_util.parse_tax_number(monday_fields["tax_number_str"])

            # Only accept vendor_status if it's in your allowed set
            vendor_status = monday_fields["vendor_status"]
            if vendor_status not in ["PENDING", "TO VERIFY", "APPROVED", "ISSUE"]:
                vendor_status = None  # or handle differently

            # Now do the DB update — use contact_id, since db_contact is a dict
            db_contact = self.database_util.update_contact_with_monday_data(
                contact_id=db_contact["id"],
                pulse_id=monday_fields["pulse_id"],
                phone=monday_fields["phone"],
                email=monday_fields["email"],
                address_line_1=monday_fields["address_line_1"],
                city=monday_fields["city"],
                zip_code=monday_fields["zip_code"],
                country=monday_fields["country"],
                tax_type=monday_fields["tax_type"],
                tax_number=tax_number_int,
                payment_details=monday_fields["payment_details"],
                vendor_status=vendor_status,
                tax_form_link=monday_fields["tax_form_link"],
            )
            return db_contact
            # endregion
        else:
            # region 4) If not found, create in Monday, store pulse_id
            self.logger.info(f"❌ No Monday contact found/close match for '{contact_name}'. Creating new item...")
            new_pulse_id = self.monday_api.create_contact_in_monday(contact_name)

            # Update DB with the new pulse_id — again, pass contact_id
            db_contact = self.database_util.update_contact_with_monday_data(
                contact_id=db_contact["id"],
                pulse_id=new_pulse_id
            )
            return db_contact
            # endregion

        # endregion
    #endregion
    #endregion

    #region Event Processing - PO Log - Step 4 - Monday Processing
    def create_pos_in_monday(self, project_number):
        self.logger.info("🌐 Processing PO data in Monday.com...")

        # Fetch all items in the group
        monday_items = monday_api.get_items_in_project(project_id=project_number)

        # Fetch all po's from DB
        processed_items = self.database_util.search_purchase_order_by_keys(project_number = project_number)

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

            contact_item = self.database_util.search_contacts(['id'],[db_item["contact_id"]])
            contact_pulse_id = contact_item["pulse_id"]
            contact_name = contact_item["name"]
            p_id = project_number
            po_no = int(db_item["po_number"])

            column_values_str = monday_util.po_column_values_formatter(
                project_id=project_number,
                po_number=db_item["po_number"],
                description=db_item.get("description"),
                contact_pulse_id=contact_pulse_id,
                folder_link=db_item.get("folder_link"),
                producer_id=None,
                name=contact_name
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
            created_mapping = monday_api.batch_create_or_update_items(items_to_create, project_id=project_number, create=True)
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

        # Batch update main items
        if items_to_update:
            self.logger.info(f"✏️ Need to update {len(items_to_update)} main items on Monday.")
            updated_mapping = monday_api.batch_create_or_update_items(items_to_update, project_id=project_number, create=False)
            for itm in updated_mapping:
                db_item = itm["db_item"]
                monday_item_id = itm["monday_item_id"]
                self.database_util.update_purchase_order_by_keys(project_number, db_item["po_number"], pulse_id=monday_item_id)
                db_item["pulse_id"] = monday_item_id
                p =project_number
                po = int(db_item["po_number"])
                monday_items_map[(p, po)]["column_values"] = itm["column_values"]

        # Ensure all main items have pulse_ids
        for db_item in processed_items:
            p_id = project_number
            po_no = int(db_item["po_number"])
            main_monday_item = monday_items_map.get((p_id, po_no))
            if main_monday_item and not db_item.get("pulse_id"):
                monday_item_id = main_monday_item["id"]
                updated = self.database_util.update_purchase_order_by_keys(project_number, db_item["po_number"], pulse_id=monday_item_id)

                if updated:
                    db_item["pulse_id"] = monday_item_id
                    self.logger.info(f"🗂 Ensured main item PO {po_no} now has pulse_id {monday_item_id} in DB and processed_items.")

        # Handle sub-items
        for db_item in processed_items:
            p_id = project_number
            po_no = int(db_item["po_number"])
            main_monday_item = monday_items_map.get((p_id, po_no))
            if not main_monday_item:
                self.logger.warning(f"❌ No Monday main item found for PO {po_no}, skipping subitems.")
                continue

            main_monday_id = main_monday_item["id"]
            sub_items_db = self.database_util.search_detail_item_by_keys(project_number, db_item["po_number"])

            monday_subitems = monday_api.get_subitems_for_item(main_monday_id)
            monday_sub_map = {}
            for msub in monday_subitems:
                identifiers = monday_util.extract_subitem_identifiers(msub)
                if identifiers is not None:
                    monday_sub_map[identifiers] = msub

            subitems_to_create = []
            subitems_to_update = []
            subitems_list = []

            if isinstance(sub_items_db, dict):
                subitems_list.append(sub_items_db)
            else:
                subitems_list = sub_items_db

            for sdb in subitems_list:
                sub_col_values_str = monday_util.subitem_column_values_formatter(
                    project_id=project_number,
                    po_number= db_item["po_number"],
                    detail_item_number=sdb["detail_number"],
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
                sub_key = (project_number, db_item["po_number"], sdb["detail_number"], sdb["line_id"])

                if sub_key in monday_sub_map:
                    monday_sub = monday_sub_map[sub_key]
                    differences = monday_util.is_sub_item_different(sdb, monday_sub)
                    if differences:
                        self.logger.info(f"Sub-item differs for detail #{sdb['detail_number']} (PO {po_no}). Differences: {differences}")
                        subitems_to_update.append({
                            "db_sub_item": sdb,
                            "column_values": new_sub_vals,
                            "parent_id": main_monday_id,
                            "monday_item_id": monday_sub["id"]
                        })
                    else:
                        self.logger.debug(f"No changes for sub-item #{sdb['detail_number']} (PO {po_no}). Differences: None")
                        # Ensure pulse_id in DB
                        sub_pulse_id = monday_sub["id"]
                        self.po_log_database_util.update_detail_item_pulse_ids(
                            project_number, db_item["po_number"], sdb["detail_number"], sdb["line_id"],
                            pulse_id=sub_pulse_id, parent_pulse_id=main_monday_id
                        )
                        sdb["pulse_id"] = sub_pulse_id
                        sdb["parent_pulse_id"] = main_monday_id
                else:
                    self.logger.debug(f"Sub-item #{sdb['detail_number']} (PO {po_no}) does not exist on Monday, scheduling creation.")
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
                    self.database_util.update_detail_item_by_keys(
                        project_number, db_item["po_number"], db_sub_item["detail_number"], db_sub_item["line_id"],
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
                        project_number, db_sub_item["po_number"], db_sub_item["detail_item_number"], db_sub_item["line_id"],
                        pulse_id=monday_subitem_id, parent_pulse_id=main_monday_id
                    )
                    db_sub_item["pulse_id"] = monday_subitem_id
                    db_sub_item["parent_pulse_id"] = main_monday_id

        self.logger.info("✅ Monday.com processing of PO data complete.")
        return processed_items
    #endregion

    #region CALLBACKS
    def after_pos_added_to_monday(self, fut):
        """
        Callback for after we finish Monday updates.
        Possibly triggers Dropbox tasks next.
        """
        try:
            pass
            processed_items = fut.result()
            #dropbox_future = self.executor.submit(self.add_dropbox_folder_link, processed_items, self.PROJECT_NUMBER)
            #dropbox_future.add_done_callback(self.after_pos_linked_to_files)
        except Exception as e:
            self.logger.error(f"❌ after_monday_done encountered an error: {e}", exc_info=True)

    def after_pos_linked_to_files(self, fut):
        """
        Callback after finishing getting tax and folder links from POs
        """
        try:
            processed_items = fut.result()
            self.logger.info("✅ MEDIA LINKING COMPLETE.")
            if self.ADD_PO_TO_MONDAY:
                monday_future = self.executor.submit(self.create_pos_in_monday, processed_items, self.PROJECT_NUMBER)
                monday_future.add_done_callback(self.after_pos_added_to_monday)
        except Exception as e:
            self.logger.error(f"❌ MEDIA LINKING ENCOUNTERED AN ERROR: {e}", exc_info=True)

    def after_tax_forms_done(self, fut):
        """
        Callback after finishing the dropbox_process_tax_form.
        """
        try:
            _ = fut.result()
            self.logger.info("✅ dropbox_process_tax_form completed.")
        except Exception as e:
            self.logger.error(f"❌ after_tax_forms_done encountered an error: {e}", exc_info=True)
    #endregion

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

    def _levenshtein_distance(self, s1: str, s2: str) -> int:
        """
        Optimized Levenshtein distance calculation with early rejection if
        the first letters differ. Returns the edit distance (number of single-character edits).

        Requirements / Assumptions:
          - If the first characters do not match, we immediately return
            len(s1) + len(s2) (a large distance), effectively filtering out
            strings that don't share the same first letter.
          - Otherwise, use an iterative DP approach for speed.
        """
        original_s1, original_s2 = s1, s2  # Keep for logging
        # Convert to lowercase for case-insensitive comparison
        s1 = s1.lower()
        s2 = s2.lower()

        # Early exit if first letters differ
        if s1 and s2 and s1[0] != s2[0]:
            dist = len(s1) + len(s2)
            self.logger.debug(
                f"🚫 First-letter mismatch: '{original_s1}' vs '{original_s2}' "
                f"(distance={dist})"
            )
            return dist

        # If either is empty, distance is the length of the other
        if not s1:
            dist = len(s2)
            # If both empty, it's distance=0 => exact match
            if dist == 0:
                self.logger.debug(
                    f"🟢 EXACT MATCH: '{original_s1}' vs '{original_s2}' -> distance=0"
                )
            else:
                self.logger.debug(
                    f"🔎 '{original_s1}' vs '{original_s2}' -> distance={dist} (one empty)"
                )
            return dist
        if not s2:
            dist = len(s1)
            # If both empty, it's distance=0 => exact match
            if dist == 0:
                self.logger.debug(
                    f"🟢 EXACT MATCH: '{original_s1}' vs '{original_s2}' -> distance=0"
                )
            else:
                self.logger.debug(
                    f"🔎 '{original_s1}' vs '{original_s2}' -> distance={dist} (one empty)"
                )
            return dist

        # Iterative dynamic programming
        m, n = len(s1), len(s2)
        # dp[i][j] = edit distance between s1[:i] and s2[:j]
        dp = [[0] * (n + 1) for _ in range(m + 1)]

        # Base cases: distance to transform from empty string
        for i in range(m + 1):
            dp[i][0] = i
        for j in range(n + 1):
            dp[0][j] = j

        for i in range(1, m + 1):
            for j in range(1, n + 1):
                cost = 0 if s1[i - 1] == s2[j - 1] else 1
                dp[i][j] = min(
                    dp[i - 1][j] + 1,  # deletion
                    dp[i][j - 1] + 1,  # insertion
                    dp[i - 1][j - 1] + cost  # substitution (if needed)
                )

        dist = dp[m][n]
        if dist == 0:
            # Perfect match
            self.logger.debug(
                f"🟢 EXACT MATCH: '{original_s1}' vs '{original_s2}' -> distance=0"
            )
        else:
            self.logger.debug(
                f"🔎 '{original_s1}' vs '{original_s2}' -> distance={dist}"
            )

        return dist

    def _find_monday_contact_match(
            self, contact_name: str, all_monday_contacts: list, max_distance: int = 2
    ) -> dict:
        """
        Find an exact or "close" match for contact_name among the list of Monday
        contacts. Returns the matched contact dict if found, else None.
        Logs if match is 'close'.
        """
        contact_name_lower = contact_name.lower()

        # 1) Try exact matches first
        for c in all_monday_contacts:
            if c['name'].strip().lower() == contact_name_lower:
                self.logger.info(f"✅ Exact Monday match found: '{c['name']}' for '{contact_name}'.")
                return c

        # 2) If no exact match, look for "close matches" within allowable distance
        best_candidate = None
        best_distance = max_distance + 1  # Initialize beyond the threshold

        for i, c in enumerate(all_monday_contacts):
            if i % 50 == 0:
                self.logger.debug(
                    f"Comparing '{contact_name}' to '{c['name']}', index {i} of {len(all_monday_contacts)}"
                )
            # Perform distance check
            current_distance = self._levenshtein_distance(contact_name_lower, c['name'].strip().lower())
            if current_distance < best_distance:
                best_distance = current_distance
                best_candidate = c

        if best_candidate and best_distance <= max_distance:
            self.logger.info(
                f"⚠️ Close match found for '{contact_name}' → '{best_candidate['name']}', "
                f"distance={best_distance}. Accepting as match."
            )
            return best_candidate

        # No match or close match found
        return None

    #endregion


# Singleton instance
dropbox_service = DropboxService()