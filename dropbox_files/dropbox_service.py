# services/dropbox_service.py

import os
import re
from datetime import timedelta, datetime
import PyPDF2
import pytesseract
from pdf2image import convert_from_path

from po_log_files.po_log_database_util import po_log_database_util
from dropbox_files.dropbox_client import dropbox_client
from monday_files.monday_service import monday_service
from dropbox_files.dropbox_util import dropbox_util
from po_log_files.po_log_processor import po_log_processor
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
            self.po_log_processor = po_log_processor
            self.dropbox_util = dropbox_util
            self.po_log_database_util = po_log_database_util
            self.logger.info("Dropbox Service  initialized")
            self._initialized = True

    def determine_file_type(self, path):
        """
        Determine the type of file based on its path and name,
        and delegate processing to the appropriate handler.
        """
        self.logger.info(f"Validating file type and location path: {self.dropbox_util.get_last_path_component_generic(path)}")

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
            self.logger.error(f"Error determining file type for {filename}: {e}", exc_info=True)
            return None

    def process_po_log(self, path, project_id):
        """
        Process a PO Log file.

        Args:
            path (str): The Dropbox path to the PO Log file.
            project_id (str): The Project ID extracted from the filename.
        """
        try:
            self.logger.info(
                f"Processing PO Log for project {project_id}: {self.dropbox_util.get_last_path_component_generic(path)}")

            dbx_client = self.dropbox_client
            dbx = dbx_client.dbx
            metadata, res = dbx.files_download(path)
            file_content = res.content

            temp_file_path = f"temp_{os.path.basename(path)}"
            with open(temp_file_path, 'wb') as temp_file:
                temp_file.write(file_content)
            self.logger.info(f"Temporary file created for processing: {temp_file_path}")

            try:
                main_items = self.po_log_processor.parse_po_log_main_items(temp_file_path)
                detail_items = self.po_log_processor.parse_po_log_sub_items(temp_file_path)
                contacts = self.po_log_processor.get_contacts_list(main_items, detail_items)

                self.logger.debug(f"Parsed PO Log main items: {main_items}")
                self.logger.debug(f"Parsed PO Log detail items: {detail_items}")
                self.logger.info(f"Extracted contacts: {contacts}")

                contact_ids = self.po_log_database_util.get_contact_surrogate_ids(contacts)





            except Exception as parse_error:
                self.logger.error(f"Error parsing PO Log file: {parse_error}", exc_info=True)
                raise

            if os.path.exists(temp_file_path):
                os.remove(temp_file_path)
                self.logger.info(f"Temporary file removed: {temp_file_path}")

        except Exception as e:
            self.logger.error(f"Error processing PO Log for project {project_id}: {e}", exc_info=True)

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
            logging.error(f"Error extracting text with OCR from '{file_path}': {e}", exc_info=True)
            return ""

dropbox_service = DropboxService()
