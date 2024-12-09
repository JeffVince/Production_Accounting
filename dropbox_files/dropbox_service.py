# services/dropbox_service.py
import json
import os
import re
from datetime import timedelta, datetime
import PyPDF2
import pytesseract
from pdf2image import convert_from_path

from config import Config
from db_util import initialize_database
from monday_files.monday_api import monday_api
from monday_files.monday_util import monday_util
from po_log_files.po_log_database_util import po_log_database_util
from dropbox_files.dropbox_client import dropbox_client
from monday_files.monday_service import monday_service
from dropbox_files.dropbox_util import dropbox_util
from po_log_files.po_log_processor import POLogProcessor
from utilities.singleton import SingletonMeta
import logging


class DropboxService(metaclass=SingletonMeta):
    def __init__(self):

        if not hasattr(self, '_initialized'):
            # Set up logging
            self.logger = logging.getLogger("app_logger")

            # import monday service
            self.monday_service = monday_service
            self.dropbox_client = dropbox_client
            self.po_log_processor = POLogProcessor()
            self.dropbox_util = dropbox_util
            self.monday_api = monday_api
            self.monday_util = monday_util
            self.config = Config()
            self.po_log_database_util = po_log_database_util
            self.logger.info("Dropbox Service  initialized")
            self._initialized = True

    def determine_file_type(self, path):
        """
        Determine the type of file based on its path and name,
        and delegate processing to the appropriate handler.
        """
        self.logger.info(
            f"Validating file type and location path: {self.dropbox_util.get_last_path_component_generic(path)}")

        # Extract the filename
        filename = os.path.basename(path)

        try:
            # Check if the file is in a folder called "1.5 PO Logs"
            if "1.5 PO Logs" in path:
                # Extract the Project ID from the first 4 digits of the filename
                project_id_match = re.match(r"^\d{4}", filename)
                if project_id_match:
                    project_id = project_id_match.group()
                    self.logger.info(f"File identified as PO Log with Project ID {project_id}.")
                    return self.process_po_log(path, project_id)
                else:
                    self.logger.warning("Could not determine Project ID from PO Log filename.")
                    return

            # Check if the file is an invoice (case-insensitive)
            if re.search(r"invoice", filename, re.IGNORECASE):
                self.logger.info(f"File identified as an invoice: {filename}.")
                return self.process_invoice(path)

            # Check if the file is a tax form (case-insensitive)
            if re.search(r"w9|w8-ben|w8-bene|w8-ben-e", filename, re.IGNORECASE):
                self.logger.info(f"File identified as a tax form: {filename}.")
                return self.process_tax_form(path)

            # Check if the file is a receipt (case-insensitive)
            if re.search(r"receipt", filename, re.IGNORECASE):
                self.logger.info(f"File identified as a receipt: {filename}.")
                return self.process_receipt(path)

            # If the file type cannot be determined
            self.logger.debug(f"Invalid: {filename}.")
            return None

        except Exception as e:
            self.logger.exception( f"Error determining file type for {filename}: {e}", exc_info=True)
            return None

    def process_po_log(self, path):
        """
        Process a PO Log file.

        Args:
            path (str): The Dropbox path to the PO Log file.
            project_id (str): The Project ID extracted from the filename.
        """
        temp_file_path = f"./temp_files/{os.path.basename(path)}"
        self.logger.debug(f"Opening {temp_file_path}")
        project_id = self.extract_project_id(temp_file_path)
        try:
            if not self.config.USE_TEMP:  # 🙆‍
                self.logger.info(
                    f"Processing PO Log for project {project_id}: {self.dropbox_util.get_last_path_component_generic(path)}"
                )

                try:
                    dbx_client = self.dropbox_client
                    dbx = dbx_client.dbx
                    metadata, res = dbx.files_download(path)
                    file_content = res.content

                    with open(temp_file_path, 'wb') as temp_file:
                        temp_file.write(file_content)
                        self.logger.info(f"Temporary file created for processing: {temp_file_path}")
                except Exception as e:
                    self.logger.exception( f"Failed to download or save file from Dropbox: {e}")
                    return

            try:
                # PO_LOG GET LISTS
                main_items, detail_items, contacts = self.po_log_processor.parse_showbiz_po_log(temp_file_path)
                group_id = self.monday_api.fetch_group_ID(project_id)
            except Exception as e:
                self.logger.exception( f"Failed to parse PO Log: {e}")
                return

            if not self.config.SKIP_MAIN:  # 🙆‍
                try:
                    for item in main_items:
                        item["group_id"] = group_id

                    # PO_LOG BEGIN PROCESSING CONTACTS
                        # MONDAY FIND OR CREATE CONTACT IN MONDAY
                        item = self.populate_contact_details(item)
                        # DB FIND OR CREATE CONTACT IN DB
                        item = self.po_log_database_util.find_or_create_contact_item_in_db(item)

                    # PO_LOG  BEGIN PROCESSING ITEMS
                        # MONDAY find or create item in MONDAY
                        column_values = self.monday_util.po_column_values_formatter(project_id=item["project_id"], po_number=item["PO"], description=item["description"], contact_pulse_id=item["contact_pulse_id"], status=item["contact_status"] )
                        item = self.monday_api.find_or_create_item_in_monday(item, column_values)

                        # DB find or create item in DB
                        result = self.po_log_database_util.create_or_update_main_item_in_db(item)

                    #PO_LOG  BEGIN PROCESSING SUBITEMS
                        for sub_item in detail_items:
                            if sub_item:
                                if sub_item["PO"] == item["PO"]:

                                    #PRE PROCESSING
                                    sub_item["po_surrogate_id"] = item["po_surrogate_id"]
                                    sub_item["parent_status"] = item["status"]

                                    # MONDAY FIND OR CREATE DETAIL ITEM IN MONDAY
                                    sub_item = self.monday_api.find_or_create_sub_item_in_monday(sub_item, item)
                                    # DB FIND OR CREATE DETAIL ITEM IN DB
                                    sub_item["parent_pulse_id"] = item["item_pulse_id"]
                                    self.po_log_database_util.create_or_update_sub_item_in_db(sub_item)


                except Exception as e:
                    self.logger.exception( f"Failed to process items: {e}")
                    return
            if not self.config.USE_TEMP:
                try:
                    if os.path.exists(temp_file_path):
                        os.remove(temp_file_path)
                        self.logger.info(f"Temporary file removed: {temp_file_path}")
                except Exception as e:
                    self.logger.warning(f"Failed to remove temporary file: {e}")
        except Exception as e:
            self.logger.critical(f"Unexpected error in process_po_log: {e}")

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

    def process_invoice(self, path):
        """
        Process an invoice file.
        """
        self.logger.info(f"Processing invoice: {path}")
        # Implement specific logic for processing invoices
        pass

    def process_tax_form(self, path):
        """
        Process a tax form file.
        """
        self.logger.info(f"Processing tax form: {path}")
        # Implement specific logic for processing tax forms
        pass

    def process_receipt(self, path):
        """
        Process a receipt file.
        """
        self.logger.info(f"Processing receipt: {path}")
        # Implement specific logic for processing receipts
        pass

    def process_contacts(self, contacts, project_id):

        po_records = self.po_log_database_util.get_pos_by_project_id(project_id)

        existing_contacts = self.po_log_database_util.get_contact_surrogate_ids(contacts)

        self.logger.info("Starting synchronization with Database")

        self.po_log_database_util.link_contact_to_po(existing_contacts, project_id)

        self.logger.info("Starting synchronization with Monday.com")

        try:
            for po in po_records:
                po_number = po.get("po_number")
                po_pulse_id = po.get("pulse_id")
                contact_surrogate_id = po.get("'contact_surrogate_id")

                if not contact_surrogate_id:
                    self.logger.warning(
                        f"No surrogate ID found for contact ID '{contact_surrogate_id}'. Skipping PO '{project_id}_{po_number}'.")
                    continue

                # Retrieve the Contact's pulse ID from DB using the surrogate ID
                contact_pulse_id = self.po_log_database_util.get_contact_pulse_id(contact_surrogate_id)

                # create column_values object
                column_values = self.monday_util.po_column_values_formatter(contact_pulse_id=contact_pulse_id)

                # Update the connected column in the PO item to link to the Contact's pulse ID
                update_success = self.monday_api.update_item(po_pulse_id, column_values)

                if update_success:
                    self.logger.info(
                        f"Successfully linked PO '{po_number}' with Contact '{contact_surrogate_id}' in Monday.com.")
                else:
                    self.logger.warning(f"Failed to link PO '{po_number}' with Contact '{contact_surrogate_id}' in Monday.com.")

            self.logger.info("Completed synchronization with Monday.com")
        except Exception as parse_error:
            self.logger.exception( f"Error processing contacts: {parse_error}", exc_info=True)
            raise

    def extract_receipt_info_with_openai_from_file(self, dropbox_path):
        """
        Extracts receipt information from a file located in Dropbox.

        Args:
            dropbox_path (str): The Dropbox path to the receipt file.

        Returns:
            dict or None: Extracted receipt information or None if extraction fails.
        """
        try:
            text = self.extract_text_from_file(dropbox_path)
            if not text:
                logging.error(f"No text extracted from receipt file: {dropbox_path}")
                return None

            receipt_info = self.dropbox_util.extract_receipt_info_with_openai(text)
            if not receipt_info:
                logging.error(f"OpenAI failed to extract receipt info from file: {dropbox_path}")
                return None

            return receipt_info

        except Exception as e:
            logging.error(f"Error extracting receipt info from file {dropbox_path}: {e}", exc_info=True)
            return None

    def extract_invoice_data_from_file(self, dropbox_path):
        """
        Extracts invoice data from a given file and returns line items and vendor description.

        Args:
            dropbox_path (str): The path to the invoice file.

        Returns:
            tuple: A tuple containing a list of line items and the vendor description.
        """
        # Step 1: Extract text from the file (supports PDFs and images)
        text = self.dropbox_util.extract_text_from_file(dropbox_path)
        if not text:
            logging.error("Failed to extract text from the invoice file.")
            return [], "Text extraction failed."

        # Step 2: Use OpenAI to process the text and extract structured invoice data
        try:
            invoice_data, error = self.dropbox_util.extract_info_with_openai(text)
            if error:
                logging.error(f"OpenAI processing failed with error: {error}. Manual entry may be required.")
                return [], "Failed to process invoice with OpenAI."
        except Exception as e:
            logging.error(f"OpenAI processing failed: {e}")
            return [], "OpenAI processing failed."

        # Step 3: Organize extracted data into line items and vendor description
        try:
            vendor_description = invoice_data.get("description", "")
            line_items = []
            for item in invoice_data.get("line_items", []):
                # Extract and validate fields
                description = item.get("item_description", "")
                quantity = item.get("quantity", 1)
                rate = float(item.get("rate", 0.0))
                date = item.get("date", "")
                due_date = item.get("due_date", "") or (
                        datetime.strptime(date, '%Y-%m-%d') + timedelta(days=30)).strftime(
                    '%Y-%m-%d') if date else ""
                account_number = item.get("account_number", "5000")  # Default account number if not provided

                # Append the dictionary to line_items
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
            logging.error(f"Error structuring invoice data: {e}")
            return [], "Failed to structure invoice data."

    def extract_text_from_file(self, dropbox_path):
        """
        Extracts text from a file in Dropbox using OCR or direct text extraction.

        Args:
            dropbox_path (str): The Dropbox path to the file.

        Returns:
            str: Extracted text from the file.
        """
        dbx_client = dropbox_client
        dbx = dbx_client.dbx

        try:
            # Download the file content
            metadata, res = dbx.files_download(dropbox_path)
            file_content = res.content

            # Save the file temporarily
            temp_file_path = f"temp_{os.path.basename(dropbox_path)}"
            with open(temp_file_path, 'wb') as f:
                f.write(file_content)

            # Extract text based on file type
            text = self.dropbox_util.extract_text_from_pdf(temp_file_path)
            if not text:
                logging.info(f"No text extracted from '{dropbox_path}' using direct extraction. Attempting OCR.")
                text = self.dropbox_util.extract_text_with_ocr(temp_file_path)

            # Remove the temporary file
            os.remove(temp_file_path)
            return text
        except Exception as e:
            logging.error(f"Error extracting text from '{dropbox_path}': {e}", exc_info=True)
            return ""

    def extract_text_from_pdf(self, file_path):
        """
        Extracts text from a PDF file using PyPDF2.

        Args:
            file_path (str): Path to the PDF file.

        Returns:
            str: Extracted text or empty string if extraction fails.
        """
        try:
            with open(file_path, 'rb') as f:
                reader = PyPDF2.PdfReader(f)
                text = ""
                for page in reader.pages:
                    text += page.extract_text() or ""
            logging.debug(f"Extracted text from PDF '{file_path}'.")
            return text
        except Exception as e:
            logging.error(f"Error extracting text from PDF '{file_path}': {e}", exc_info=True)
            return ""

    def extract_text_with_ocr(self, file_path):
        """
        Extracts text from an image file using OCR with pytesseract.

        Args:
            file_path (str): Path to the image file.

        Returns:
            str: Extracted text or empty string if extraction fails.
        """
        try:
            images = convert_from_path(file_path)
            text = ""
            for img in images:
                text += pytesseract.image_to_string(img) + "\n"
            logging.debug(f"Extracted text from image '{file_path}' using OCR.")
            return text
        except Exception as e:
            self.logger.exception( f"Error extracting text with OCR from '{file_path}': {e}", exc_info=True)
            return ""

    def populate_contact_details(self, item):
        """
        Populates the contact details in the given item dictionary using the Monday API.

        Args:
            item (dict): The item dictionary to populate.
        """

        # Find or create contact in Monday
        if "placeholder" in item["name"].lower():
            item["name"] = "PLACEHOLDER"

        monday_contact_result = self.monday_api.find_or_create_contact_in_monday(item["name"])

        # Initialize dictionaries to hold processed column values
        column_values = {}
        column_texts = {}

        # Process each entry in the column_values list
        for entry in monday_contact_result.get("column_values", []):
            key = entry.get('id')
            value = entry.get('value')
            text = entry.get('text')

            # Parse JSON value if applicable
            if value and isinstance(value, str):
                try:
                    parsed_value = json.loads(value)
                    self.logger.debug(f"Parsed value for {key}: {parsed_value}")
                except json.JSONDecodeError:
                    parsed_value = value
                    self.logger.debug(f"Value for {key} is not valid JSON: {value}")
            else:
                parsed_value = value
                self.logger.debug(f"Value for {key}: {parsed_value}")

            # Store both parsed_value and text
            column_values[key] = parsed_value
            column_texts[key] = text

        # Populate the item dictionary with the necessary contact details
        item['contact_pulse_id'] = monday_contact_result.get("id")
        self.logger.debug(f"Assigned contact_pulse_id: {item['contact_pulse_id']}")

        # Define mapping: item_key -> (column_id, use_text)
        # Set use_text=True for fields where you want to use the 'text' instead of 'value'
        mapping = {
            'contact_payment_details': (self.monday_util.CONTACT_PAYMENT_DETAILS, True),
            'contact_email': (self.monday_util.CONTACT_EMAIL, True),
            'contact_phone': (self.monday_util.CONTACT_PHONE, True),
            'address_line_1': (self.monday_util.CONTACT_ADDRESS_LINE_1, False),
            'city': (self.monday_util.CONTACT_ADDRESS_CITY, False),
            'zip': (self.monday_util.CONTACT_ADDRESS_ZIP, False),
            'tax_id': (self.monday_util.CONTACT_TAX_NUMBER, False),
            'tax_form_link': (self.monday_util.CONTACT_TAX_FORM_LINK, False),
            'contact_status': (self.monday_util.CONTACT_STATUS, True),
            'contact_country': (self.monday_util.CONTACT_ADDRESS_COUNTRY, False),
            'contact_tax_type': (self.monday_util.CONTACT_TAX_TYPE, False)
        }

        # Iterate through the mapping and assign values to the item
        for item_key, (column_id, use_text) in mapping.items():
            if use_text:
                value = column_texts.get(column_id)
                item[item_key] = value
                self.logger.debug(f"Assigned '{item_key}' with text from '{column_id}': {value}")
            else:
                value = column_values.get(column_id)
                item[item_key] = value
                self.logger.debug(f"Assigned '{item_key}' with value from '{column_id}': {value}")

        return item

    def extract_project_id(self, file_name: str) -> str:
        """
        Extracts the project_id from the given file name.
        The project_id is defined as the last four digits in the file name.

        Args:
            file_name (str): The name of the file (e.g., 'temp_2416.txt').

        Returns:
            str: The extracted project_id (e.g., '2416').

        Raises:
            ValueError: If fewer than four digits are found in the file name.
        """
        # Find all sequences of digits in the file name
        digit_sequences = re.findall(r'\d+', file_name)

        if not digit_sequences:
            raise ValueError(f"No digits found in file name: '{file_name}'")

        # Concatenate all digit sequences into a single string
        all_digits = ''.join(digit_sequences)

        if len(all_digits) < 4:
            raise ValueError(f"File name '{file_name}' does not contain at least four digits for project_id.")

        # Extract the last four digits as project_id
        project_id = all_digits[-4:]

        return project_id


dropbox_service = DropboxService()


#TESTING
#path = "/temp_files/temp_2416.txt"
#result = dropbox_service.process_po_log(path)

#print(dropbox_service.populate_contact_details(item={"name": "Dwane Harris"}))