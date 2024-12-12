# services/dropbox_service.py
# 📦✨ DropboxService: Processes files from Dropbox and integrates them with Monday.com! ✨📦
# - Introduced a single flag `SKIP_MONDAY` to skip all Monday.com updates and only update the DB.
# - When SKIP_MONDAY is True, we now load contact data directly from the DB instead of Monday.
# - Wrapped Monday-related calls in conditionals so that if `SKIP_MONDAY` is True, the code doesn't call Monday API functions.
# - Adjusted code so that when SKIP_MONDAY is True, we do not overwrite pulse_id fields with None,
#   we simply avoid setting them entirely.

import json
import os
import re
from datetime import timedelta, datetime
import PyPDF2
import pytesseract
from pdf2image import convert_from_path
import os
from datetime import datetime

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
import logging
import tempfile

# 🧰 Debug Variable: Decide which PO number to start from
DEBUG_STARTING_PO_NUMBER = 6

# 🏳️‍🌈 FLAG: Set this to True to skip all Monday.com updates and only update the DB.
SKIP_MONDAY = False  # If True, no Monday API calls will be made, no pulse_id from Monday, and contacts come from DB.

class DropboxService(metaclass=SingletonMeta):
    """
    🎉 DropboxService Singleton Class 🎉

    This class handles the ingestion and processing of various file types (PO Logs, Invoices,
    Receipts, Tax Forms) from Dropbox, extracting information and synchronizing it with Monday.com.

    Key responsibilities:
    - Determining file type (PO Log, invoice, tax form, receipt) based on filenames
    - Processing and parsing content (e.g., extracting text from PDFs or images)
    - Interacting with Monday.com via Monday API for storing and retrieving data
    - Managing temporary files for OCR and text extraction
    - Logging events, warnings, and exceptions

    With SKIP_MONDAY set to True, it will skip all Monday updates and only update the DB.
    Contacts are also loaded from the DB instead of Monday.
    """

    # 🔑 CONSTANTS to AVOID MAGIC STRINGS 🔑
    PO_LOG_FOLDER_NAME = "1.5 PO Logs"
    INVOICE_REGEX = r"invoice"
    TAX_FORM_REGEX = r"w9|w8-ben|w8-bene|w8-ben-e"
    RECEIPT_REGEX = r"receipt"
    SHOWBIZ_REGEX = r".mbb"

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
        # region 🔍 Determine File Type
        self.logger.info(f"🔍 Validating file type and location: {self.dropbox_util.get_last_path_component_generic(path)} 💡")
        filename = os.path.basename(path)
        try:
            if self.PO_LOG_FOLDER_NAME in path:
                project_id_match = re.match(r"^\d{4}", filename)
                if project_id_match:
                    project_id = project_id_match.group()
                    self.logger.info(f"🗂 File identified as PO Log with Project ID {project_id}. 🗂")
                    return self.process_po_log(path)
                else:
                    self.logger.warning("⚠️ Could not determine Project ID from PO Log filename. ⚠️")
                    return None

            if re.search(self.INVOICE_REGEX, filename, re.IGNORECASE):
                self.logger.info(f"💰 File identified as an invoice: {filename}. 💰")
                return self.process_invoice(path)

            if re.search(self.TAX_FORM_REGEX, filename, re.IGNORECASE):
                self.logger.info(f"💼 File identified as a tax form: {filename}. 💼")
                return self.process_tax_form(path)

            if re.search(self.RECEIPT_REGEX, filename, re.IGNORECASE):
                self.logger.info(f"🧾 File identified as a receipt: {filename}. 🧾")
                return self.process_receipt(path)

            if re.search(self.SHOWBIZ_REGEX, filename, re.IGNORECASE):
                self.logger.info(f"🧾 File identified as a budget: {filename}. 🧾")
                return self.process_budget(path)

            self.logger.debug(f"❌ Invalid or unsupported file type: {filename}. ❌")
            return None
        except Exception as e:
            self.logger.exception(f"💥 Error determining file type for {filename}: {e} 💥", exc_info=True)
            return None
        # endregion

    def process_po_log(self, path: str):
        # region 🗂 Setup and File Download
        temp_file_path = f"./temp_files/{os.path.basename(path)}"
        self.logger.debug(f"🗄 Opening temporary file path {temp_file_path} 🗄")
        project_id = self.extract_project_id(temp_file_path)
        # endregion

        try:
            # region 💾 Download File if Not Using TEMP
            if not self.config.USE_TEMP:
                self.logger.info(
                    f"📝 Processing PO Log for project {project_id}: {self.dropbox_util.get_last_path_component_generic(path)}")
                try:
                    dbx_client = self.dropbox_client
                    dbx = dbx_client.dbx
                    metadata, res = dbx.files_download(path)
                    file_content = res.content

                    with open(temp_file_path, 'wb') as temp_file:
                        temp_file.write(file_content)
                        self.logger.info(f"📂 Temporary file created for processing: {temp_file_path}")
                except Exception as e:
                    self.logger.exception(f"💥 Failed to download/save file from Dropbox: {e}", exc_info=True)
                    return
            # endregion

            # region ✂ Parse PO Log
            try:
                main_items, detail_items, contacts = self.po_log_processor.parse_showbiz_po_log(temp_file_path)
                if not SKIP_MONDAY:
                    group_id = self.monday_api.fetch_group_ID(project_id)
                else:
                    group_id = "group_placeholder"
            except Exception as e:
                self.logger.exception(f"💥 Failed to parse PO Log: {e}", exc_info=True)
                return
            # endregion

            # region 🚀 Process Main Items, Contacts, and Sub-Items
            if not self.config.SKIP_MAIN:
                try:
                    for item in main_items:
                        if DEBUG_STARTING_PO_NUMBER and int(item["PO"]) < DEBUG_STARTING_PO_NUMBER:
                            self.logger.info(
                                f"⏭ Skipping PO '{item['PO']}' due to debug setting. Starting from '{DEBUG_STARTING_PO_NUMBER}'.")
                            continue

                        item["group_id"] = group_id

                        # region 👥 Populate Contact Details
                        item = self.populate_contact_details(item)
                        # endregion

                        # region 💾 DB Contact Creation/Update
                        item = self.po_log_database_util.find_or_create_contact_item_in_db(item)
                        # endregion

                        # region 🔧 Prepare Monday Column Values for PO Item
                        column_values = self.monday_util.po_column_values_formatter(
                            project_id=item["project_id"],
                            po_number=item["PO"],
                            description=item["description"],
                            contact_pulse_id=item.get("contact_pulse_id", None),
                            status=item.get("contact_status", None)
                        )
                        # endregion

                        # region 🌐 Monday.com PO Item Creation/Update
                        if not SKIP_MONDAY:
                            item = self.monday_api.find_or_create_item_in_monday(item, column_values)
                        else:
                            self.logger.debug("🌐 SKIP_MONDAY: Not calling Monday API for PO items.")
                            # Do not assign item['item_pulse_id'] at all if SKIP_MONDAY is True
                        # endregion

                        # region 💾 Update/Create Main Item in DB
                        self.po_log_database_util.create_or_update_main_item_in_db(item)
                        # endregion

                        # region 🔧 Process Sub-Items
                        for sub_item in detail_items:
                            if sub_item and sub_item["PO"] == item["PO"]:
                                detail_unchanged = self.po_log_database_util.is_unchanged(sub_item, item)
                                #if detail_unchanged:
                                    #continue
                                sub_item["po_surrogate_id"] = item["po_surrogate_id"]
                                sub_item["parent_status"] = item["status"]

                                if not SKIP_MONDAY:
                                    sub_item = self.monday_api.find_or_create_sub_item_in_monday(sub_item, item)
                                    sub_item["parent_pulse_id"] = item["item_pulse_id"]
                                else:
                                    self.logger.debug("🌐 SKIP_MONDAY: Not calling Monday API for sub-items.")
                                    # Do not assign sub_item["parent_pulse_id"] if SKIP_MONDAY is True

                                self.po_log_database_util.create_or_update_sub_item_in_db(sub_item)
                        # endregion

                except Exception as e:
                    self.logger.exception(f"💥 Failed to process items: {e}", exc_info=True)
                    return
            # endregion

            # region 🧹 Cleanup Temporary File
            if not self.config.USE_TEMP:
                try:
                    if os.path.exists(temp_file_path):
                        os.remove(temp_file_path)
                        self.logger.info(f"🧹 Temporary file removed: {temp_file_path}")
                except Exception as e:
                    self.logger.warning(f"⚠️ Failed to remove temporary file: {e} ⚠️")
            # endregion
        except Exception as e:
            self.logger.critical(f"💣 Unexpected error in process_po_log: {e}", exc_info=True)

    def link_main_item_to_contact_in_db(self):
        pass

    def send_contact_to_monday(self):
        pass

    def send_po_to_monday(self):
        pass

    def create_detail_item_in_db(self):
        pass

    def send_detail_item_to_monday(self):
        pass

    def process_invoice(self, dropbox_path: str):
        self.logger.info(f"💼 Processing invoice: {dropbox_path}")

        project_id, po_number, invoice_number = self.parse_invoice_filename(dropbox_path)
        file_link = self.dropbox_util.get_file_link(dropbox_path)
        self.logger.info(f"🔗 Retrieved file link: {file_link}")

        # Add file link to detail items in DB
        self.database_util.add_invoice_link_to_detail_items(project_id, po_number, invoice_number, file_link)
        self.logger.info(f"✅ Added invoice link to detail items for {project_id}_{po_number}_{invoice_number}")

        # If not skipping Monday, update Monday
        if not SKIP_MONDAY:
            detail_item_ids = self.database_util.get_detail_item_pulse_ids_for_invoice(project_id, po_number,
                                                                                       invoice_number)
            self.monday_api.update_detail_items_with_invoice_link(detail_item_ids, file_link)
            self.logger.info(
                f"🌐 Updated detail items with invoice link on Monday for {project_id}_{po_number}_{invoice_number}")

        # Download invoice file
        file_data = self.dropbox_api.download_file(dropbox_path)

        # Extract text from the PDF
        invoice_text = self.extract_text_from_pdf(file_data)

        # Process with OpenAI
        invoice_data = self.process_invoice_with_openai(invoice_text)

        if invoice_data:
            transaction_date = invoice_data.get('invoice_date')
            due_date = invoice_data.get('due_date')
            # 'term' may not be provided by OpenAI, set None if not present
            term = invoice_data.get('term')
            description = invoice_data.get('description')
            line_items = invoice_data.get('line_items', [])

            # Calculate total from line_items
            total = 0.0
            for item in line_items:
                q = float(item.get('quantity', 1))
                r = float(item.get('rate', 0.0))
                total += (q * r)

            # Insert or update invoice record
            self.database_util.create_or_update_invoice(
                project_id=project_id,
                po_number=po_number,
                invoice_number=invoice_number,
                transaction_date=transaction_date,
                term=term,
                total=total,
                file_link=file_link
            )
            self.logger.info(f"🗃 Invoice stored in DB for {project_id}_{po_number}_{invoice_number}")

    def parse_invoice_filename(self, filename: str):
        """
        Expected filename patterns:
        - 2416_08 Sho Schrock-Manabe Invoice.pdf  (invoice_number defaults to 1)
        - 2416_08_02 Sho Schrock-Manabe Invoice.pdf
        """
        base_name = os.path.basename(filename)
        name_part = os.path.splitext(base_name)[0]
        parts = name_part.split("_")
        project_id = parts[0]
        po_number = parts[1]
        if len(parts) > 2 and parts[2].isdigit():
            invoice_number = int(parts[2])
        else:
            invoice_number = 1
        return project_id, po_number, invoice_number

    def extract_text_from_pdf(self, file_data: bytes) -> str:
        # Attempt to extract text directly (if PDF)
        pdf_text = self.dropbox_util.extract_text_from_pdf(file_data)
        if pdf_text and pdf_text.strip():
            return pdf_text

        self.logger.warning("⚠️ No direct text extracted. Attempting OCR...")
        # Fallback to OCR
        text_via_ocr = self.ocr_service.extract_text_from_invoice(file_data)
        return text_via_ocr

    def process_invoice_with_openai(self, text: str):
        info, error = self.dropbox_util.extract_info_with_openai(text)
        if error:
            self.logger.warning(f"⚠️ Could not process invoice with OpenAI: {error}")
            return None
        return info

    def process_tax_form(self, path: str):
        # region 💼 Process Tax Form
        self.logger.info(f"📜 Processing tax form: {path}")
        # endregion
        pass

    def process_receipt(self, path: str):
        # region 🧾 Process Receipt
        self.logger.info(f"🧾 Processing receipt: {path}")
        # endregion
        pass

    def process_contacts(self, contacts: list, project_id: str):
        # region 📇 Process Contacts
        po_records = self.po_log_database_util.get_pos_by_project_id(project_id)
        existing_contacts = self.po_log_database_util.get_contact_surrogate_ids(contacts)

        self.logger.info("🌱 Starting synchronization with Database")
        self.po_log_database_util.link_contact_to_po(existing_contacts, project_id)

        self.logger.info("🌐 Starting synchronization with Monday.com")

        try:
            for po in po_records:
                po_number = po.get("po_number")
                po_pulse_id = po.get("pulse_id")
                contact_surrogate_id = po.get("'contact_surrogate_id")

                if not contact_surrogate_id:
                    self.logger.warning(
                        f"⚠️ No surrogate ID found for contact ID '{contact_surrogate_id}'. "
                        f"Skipping PO '{project_id}_{po_number}'. ⚠️"
                    )
                    continue

                contact_pulse_id = self.po_log_database_util.get_contact_pulse_id(contact_surrogate_id)
                column_values = self.monday_util.po_column_values_formatter(contact_pulse_id=contact_pulse_id)

                if not SKIP_MONDAY:
                    update_success = self.monday_api.update_item(po_pulse_id, column_values)
                    if update_success:
                        self.logger.info(
                            f"✅ Successfully linked PO '{po_number}' with Contact '{contact_surrogate_id}' in Monday.com. ✅")
                    else:
                        self.logger.warning(
                            f"⚠️ Failed to link PO '{po_number}' with Contact '{contact_surrogate_id}' in Monday.com. ⚠️")
                else:
                    self.logger.info(
                        f"🔧 SKIP_MONDAY is True, not updating PO '{po_number}' contact link in Monday.com.")
            self.logger.info("🎉 Completed synchronization with Monday.com")
        except Exception as parse_error:
            self.logger.exception(f"💥 Error processing contacts: {parse_error}", exc_info=True)
            raise
        # endregion

    def process_budget(self, dropbox_path: str):
        """
        🎉 process_budget Function 🎉
        This function takes a dropbox_path for a .mbb budget file, verifies that it's a valid budget file
        in the correct directory (Working or Actuals), locates the corresponding P.O Logs folder, and then
        invokes the ShowbizPoLogPrinter to open and print the PO Log associated with that budget.
        """

        # region 🛠 Initial Setup
        self.logger.info(f"💼 Processing new budget changes: {dropbox_path}")
        filename = os.path.basename(dropbox_path)
        # endregion

        # region 🔒 Validate File Extension
        try:
            if not filename.endswith(".mbb") or filename.endswith(".mbb.lck"):
                self.logger.info("❌ Not a valid .mbb file or it's an .lck file. Ignoring.")
                return
        except Exception as e:
            self.logger.exception(f"💥 Error checking file extension: {e}", exc_info=True)
            return
        # endregion

        # region 🔎 Check Folder Structure
        # Example path structure (Dropbox):
        # /2024/2416 - Whop Keynote/5. Budget/1.2 Working/mybudget.mbb
        # or
        # /2024/2416 - Whop Keynote/5. Budget/1.3 Actuals/mybudget.mbb
        try:
            segments = dropbox_path.strip("/").split("/")
            # segments example: ["2024", "2416 - Whop Keynote", "5. Budget", "1.2 Working", "mybudget.mbb"]

            if len(segments) < 4:
                self.logger.info("❌ Path does not have enough segments to identify project and phase. Ignoring.")
                return

            project_folder = segments[0]  # e.g. "2416 - Whop Keynote"
            budget_folder = segments[1]  # should be "5. Budget"
            phase_folder = segments[2]  # "1.2 Working" or "1.3 Actuals"

            # Check if we're in the correct budget folder and phase
            if budget_folder != "5. Budget" or phase_folder not in ["1.2 Working", "1.3 Actuals"]:
                self.logger.info("❌ Budget file not located in 'Working' or 'Actuals' folder. Ignoring.")
                return

            # Extract project_id from the project folder name.
            # Assuming project folder is like "2416 - Whop Keynote" -> project_id = "2416"
            project_id_match = re.match(r"^\d{4}", project_folder)
            if not project_id_match:
                self.logger.info("❌ Could not determine Project ID from folder name. Ignoring.")
                return
            project_id = project_id_match.group()

            self.logger.info(f"🔑 Determined Project ID: {project_id}")
        except Exception as e:
            self.logger.exception(f"💥 Error parsing path structure: {e}", exc_info=True)
            return
        # endregion

        # region 📁 Determine P.O Logs Folder
        # P.O Logs folder: .../5. Budget/1.5 PO Logs
        try:
            # We'll reconstruct the path to the P.O Logs folder based on known structure
            # segments = [year, projectID - projectName, "5. Budget", "1.2 Working", "mybudget.mbb"]
            # Remove filename and phase folder
            # ["2024", "2416 - Whop Keynote", "5. Budget"]
            budget_root = "/".join(segments[0:3])
            po_logs_path = f"/{budget_root}/1.5 PO Logs"  # Dropbox path to PO Logs folder
            self.logger.info(f"🗂 P.O Logs folder path: {po_logs_path}")
        except Exception as e:
            self.logger.exception(f"💥 Error determining PO Logs folder: {e}", exc_info=True)
            return
        # endregion

        # region 💾 Download Budget File
        # We'll download the .mbb file locally so we can open it with ShowbizPoLogPrinter.
        try:
            local_temp_dir = "./temp_files"
            if not os.path.exists(local_temp_dir):
                os.makedirs(local_temp_dir)

            local_file_path = os.path.join(local_temp_dir, filename)
            self.logger.info(f"⏬ Downloading '{filename}' to '{local_file_path}'...")

            # Assuming dropbox_api has a method `download_file` that returns binary content:
            file_data = self.dropbox_api.download_file(dropbox_path, local_file_path)
            if file_data:
                self.logger.info(f"✅ File downloaded successfully: {filename}")
            else:
                raise
        except Exception as e:
            self.logger.exception(f"💥 Error downloading budget file: {e}", exc_info=True)
            return
        # endregion

        # region 🖨 Invoke ShowbizPoLogPrinter
        # Now that we have the local .mbb file, let's run the printing script.
        # This script:
        # 1. Opens the budget file.
        # 2. Prints the PO_Log (as per your instructions).
        try:
            from po_log_files.showbiz_log_printer import ShowbizPoLogPrinter

            self.logger.info("🖨 Invoking ShowbizPoLogPrinter with the downloaded budget file...")
            printer = ShowbizPoLogPrinter(project_id=project_id, file_path=local_file_path)

            printer.run()
            self.logger.info("🎉 PO Log printing completed successfully!")
        except Exception as e:
            self.logger.exception(f"💥 Error invoking ShowbizPoLogPrinter: {e}", exc_info=True)
            return
        # endregion

        # region 🧹 Cleanup
        # If desired, remove the local file after printing
        try:
            if os.path.exists(local_file_path):
                os.remove(local_file_path)
                self.logger.info(f"🧹 Cleaned up temporary file: {local_file_path}")
        except Exception as e:
            self.logger.warning(f"⚠️ Failed to remove temporary file '{local_file_path}': {e}")
        # endregion

        self.logger.info("✅ process_budget completed successfully.")


    def extract_receipt_info_with_openai_from_file(self, dropbox_path: str):
        # region 🤖 Receipt Info with OpenAI
        try:
            text = self.extract_text_from_file(dropbox_path)
            if not text:
                logging.error(f"🚫 No text extracted from receipt file: {dropbox_path}")
                return None

            receipt_info = self.dropbox_util.extract_receipt_info_with_openai(text)
            if not receipt_info:
                logging.error(f"🚫 OpenAI failed to extract receipt info from file: {dropbox_path}")
                return None

            return receipt_info

        except Exception as e:
            logging.error(f"💥 Error extracting receipt info from file {dropbox_path}: {e}", exc_info=True)
            return None
        # endregion

    def extract_invoice_data_from_file(self, dropbox_path: str):
        # region 📑 Extract Invoice Data
        text = self.dropbox_util.extract_text_from_file(dropbox_path)
        if not text:
            logging.error("🚫 Failed to extract text from the invoice file.")
            return [], "Text extraction failed."

        try:
            invoice_data, error = self.dropbox_util.extract_info_with_openai(text)
            if error:
                logging.error(f"🚫 OpenAI processing failed with error: {error}. Manual entry may be required.")
                return [], "Failed to process invoice with OpenAI."
        except Exception as e:
            logging.error(f"💥 OpenAI processing failed: {e}")
            return [], "OpenAI processing failed."

        try:
            vendor_description = invoice_data.get("description", "")
            line_items = []
            for it in invoice_data.get("line_items", []):
                description = it.get("item_description", "")
                quantity = it.get("quantity", 1)
                rate = float(it.get("rate", 0.0))
                date = it.get("date", "")
                due_date = it.get("due_date", "") or (
                    (datetime.strptime(date, '%Y-%m-%d') + timedelta(days=30)).strftime('%Y-%m-%d')
                    if date else ""
                )
                account_number = it.get("account_number", "5000")

                line_items.append({
                    'description': description,
                    'quantity': quantity,
                    'rate': rate,
                    'date': date,
                    'due_date': due_date,
                    'account_number': account_number
                })

            return line_items, vendor_description

        except Exception as e:
            logging.error(f"💥 Error structuring invoice data: {e}")
            return [], "Failed to structure invoice data."
        # endregion

    def extract_text_from_file(self, dropbox_path: str) -> str:
        # region 🕵️ Extract Text from File
        dbx_client = dropbox_client
        dbx = dbx_client.dbx

        temp_file_path = None
        try:
            metadata, res = dbx.files_download(dropbox_path)
            file_content = res.content

            with tempfile.NamedTemporaryFile(delete=False, suffix=os.path.splitext(dropbox_path)[-1]) as temp_file:
                temp_file.write(file_content)
                temp_file_path = temp_file.name

            text = self.dropbox_util.extract_text_from_pdf(temp_file_path)
            if not text:
                logging.info(f"ℹ️ No direct text extracted from '{dropbox_path}'. Attempting OCR...")
                text = self.dropbox_util.extract_text_with_ocr(temp_file_path)

            return text or ""

        except Exception as e:
            logging.error(f"💥 Error extracting text from '{dropbox_path}': {e}", exc_info=True)
            return ""
        finally:
            if temp_file_path and os.path.exists(temp_file_path):
                try:
                    os.remove(temp_file_path)
                    logging.debug(f"🧹 Removed temporary file '{temp_file_path}'")
                except Exception as cleanup_error:
                    logging.warning(f"⚠️ Could not remove temp file '{temp_file_path}': {cleanup_error}")
        # endregion

    def extract_text_with_ocr(self, file_path: str) -> str:
        # region 👀 Extract Text with OCR
        try:
            images = convert_from_path(file_path)
            text = ""
            for img in images:
                text += pytesseract.image_to_string(img) + "\n"
            logging.debug(f"👁‍🗨 Extracted text from image '{file_path}' using OCR.")
            return text
        except Exception as e:
            self.logger.exception(f"💥 Error extracting text with OCR from '{file_path}': {e}", exc_info=True)
            return ""
        # endregion

    def populate_contact_details(self, item: dict) -> dict:
        # region 🏷 Populate Contact Details
        if "placeholder" in item["name"].lower():
            item["name"] = "PLACEHOLDER"

        if SKIP_MONDAY:
            # Load contact directly from DB as a dict
            self.logger.debug("SKIP_MONDAY: Loading contact details from DB instead of Monday.")
            contact_dict = self.po_log_database_util.get_contact_by_name(name=item["name"])

            if contact_dict:
                # Populate item fields from the contact_dict
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
                self.logger.warning(f"No contact found in DB for {item['name']}. Assigning defaults.")
                # Default to None if not found
                item['contact_pulse_id'] = None
                item['contact_payment_details'] = None
                item['contact_email'] = None
                item['contact_phone'] = None
                item['address_line_1'] = None
                item['city'] = None
                item['zip'] = None
                item['tax_id'] = None
                item['tax_form_link'] = None
                item['contact_status'] = None
                item['contact_country'] = None
                item['contact_tax_type'] = None
        else:
            # Use Monday API to find or create contact (unchanged logic)
            monday_contact_result = self.monday_api.find_or_create_contact_in_monday(item["name"])
            column_values = {}
            column_texts = {}

            for entry in monday_contact_result.get("column_values", []):
                key = entry.get('id')
                value = entry.get('value')
                text = entry.get('text')

                if value and isinstance(value, str):
                    try:
                        parsed_value = json.loads(value)
                        self.logger.debug(f"🔧 Parsed JSON for {key}: {parsed_value}")
                    except json.JSONDecodeError:
                        parsed_value = value
                        self.logger.debug(f"🔧 Value for {key} not valid JSON: {value}")
                else:
                    parsed_value = value
                    self.logger.debug(f"🔧 Value for {key}: {parsed_value}")

                column_values[key] = parsed_value
                column_texts[key] = text

            item['contact_pulse_id'] = monday_contact_result.get("id")
            self.logger.debug(f"🏷 Assigned contact_pulse_id: {item['contact_pulse_id']}")

            mapping = {
                'contact_payment_details': (self.monday_util.CONTACT_PAYMENT_DETAILS, True),
                'contact_email': (self.monday_util.CONTACT_EMAIL, True),
                'contact_phone': (self.monday_util.CONTACT_PHONE, True),
                'address_line_1': (self.monday_util.CONTACT_ADDRESS_LINE_1, False),
                'city': (self.monday_util.CONTACT_ADDRESS_CITY, False),
                'zip': (self.monday_util.CONTACT_ADDRESS_ZIP, False),
                'contact_status': (self.monday_util.CONTACT_STATUS, True),
                'contact_country': (self.monday_util.CONTACT_ADDRESS_COUNTRY, False),
                'contact_tax_type': (self.monday_util.CONTACT_TAX_TYPE, False)
            }

            # For Monday, we don't get tax_id or tax_form_link, so set them to None
            item['tax_id'] = None
            item['tax_form_link'] = None

            for item_key, (column_id, use_text) in mapping.items():
                if use_text:
                    value = column_texts.get(column_id)
                    item[item_key] = value
                    self.logger.debug(f"🏷 Assigned '{item_key}' with text from '{column_id}': {value}")
                else:
                    value = column_values.get(column_id)
                    item[item_key] = value
                    self.logger.debug(f"🏷 Assigned '{item_key}' with value from '{column_id}': {value}")
        # endregion
        return item

    def extract_project_id(self, file_name: str) -> str:
        # region 🏗 Extract Project ID
        digit_sequences = re.findall(r'\d+', file_name)

        if not digit_sequences:
            raise ValueError(f"❗ No digits found in file name: '{file_name}' ❗")

        all_digits = ''.join(digit_sequences)

        if len(all_digits) < 4:
            raise ValueError(f"❗ File name '{file_name}' does not contain at least four digits for project_id. ❗")

        project_id = all_digits[-4:]
        # endregion
        return project_id

# 🎉 Instantiate the DropboxService Singleton
dropbox_service = DropboxService()