# files_dropbox.dropbox_util.py

import logging
import os
import re
from pathlib import Path
from typing import Dict

import dropbox
from openai import OpenAI

from dropbox_client import dropbox_client
from utilities.singleton import SingletonMeta


class DropboxUtil(metaclass=SingletonMeta):
    def __init__(self):
        if not hasattr(self, '_initialized'):
            # Set up logging
            self.logger = logging.getLogger("dropbox_logger")
            self.logger.info("Dropbox Util  initialized")
            self._initialized = True

    def parse_file_name(self, file_name: str) -> Dict[str, str]:
        """Parse the file name to extract metadata."""
        # Assuming file name format: PO123_VendorName_20231118_invoice.pdf
        parts = file_name.split('_')
        if len(parts) < 4:
            raise ValueError("Invalid file name format.")
        po_number = parts[0]
        file_type = parts[-1].split('.')[0]  # 'invoice.pdf' -> 'invoice'
        return {'po_number': po_number, 'file_type': file_type}

    def is_po_folder(self, local_path):
        """
        Determines if the specified folder path is a valid PO folder within the correct project structure.

        Expected structure:
            .../<Project Folder>/1. Purchase Orders/<PO Folder>

        Args:
            local_path (str): The path to the folder or file to check.

        Returns:
            bool: True if the folder is a PO folder within the correct project structure, False otherwise.
        """
        # Normalize path to use forward slashes
        normalized_path = local_path.replace('\\', '/')
        path_parts = normalized_path.strip('/').split('/')

        # If the path points to a file, extract the parent folder
        if '.' in path_parts[-1]:
            folder_path = os.path.dirname(normalized_path)
            path_parts = folder_path.strip('/').split('/')
            self.logger.debug("Detected file path. Extracted parent folder path.")
        else:
            folder_path = normalized_path

        self.logger.debug(f"Normalized Path Parts: {path_parts}")

        # Adjust path depth check based on your actual structure
        if len(path_parts) < 3:
            self.logger.debug(f"Path '{local_path}' does not have enough parts to be a valid PO folder.")
            return False

        # Extract key components from the path
        folder_name = path_parts[-1]  # PO folder name
        purchase_orders_folder = path_parts[-2]  # Should be "1. Purchase Orders"
        project_folder = path_parts[-3]  # Project folder name

        self.logger.debug(
            f"Checking PO folder: Project='{project_folder}', Purchase Orders='{purchase_orders_folder}', PO Folder='{folder_name}'")

        # Check if the folder name matches the expected PO folder pattern
        po_folder_pattern = r'^(\d+)[_-](\d+)\s+.*$'
        if not re.match(po_folder_pattern, folder_name):
            self.logger.debug(f"Folder name '{folder_name}' does not match PO folder pattern.")
            return False

        # Check if the "1. Purchase Orders" folder is correct (case-insensitive)
        if purchase_orders_folder.lower() != "1. purchase orders":
            self.logger.debug(f"Expected '1. Purchase Orders', but found '{purchase_orders_folder}'.")
            return False

        # Check if the project folder starts with a numeric project ID
        project_pattern = r'^\d+\s*[-_]\s*.*'
        if not re.match(project_pattern, project_folder):
            self.logger.debug(f"Project folder '{project_folder}' does not match project pattern.")
            return False

        # If all conditions are met, it's a valid PO folder in the correct structure
        self.logger.debug(f"Path '{local_path}' is a valid PO folder.")
        return True

    def parse_folder_path(self, local_path):
        """
        Parses the folder path to extract the Project ID, PO Number, Vendor Name,
        and PO Type (vendor or cc).

        Expected folder structure: .../2024/Project Folder/Purchase Order Folder/...
        Example: '/Users/.../2024/2416 - Whop Keynote/1. Purchase Orders/2416_02 AMEX 8738/

        Returns:
            Tuple (project_id, po_number, vendor_name, po_type) if found, else (None, None, None, None)
        """
        # Split the path into parts
        path_parts = local_path.split(os.sep)
        self.logger.debug(f"Path parts for parsing: {path_parts}")
        # Reverse to start searching from the deepest directory
        path_parts_reversed = path_parts[::-1]

        project_id = None
        po_number = None
        vendor_name = None
        po_type = "vendor"  # default to vendor unless it's a credit card (cc) folder

        self.logger.debug(f"Parsing folder path: {local_path}")

        for part in path_parts_reversed:
            self.logger.debug(f"Checking folder part: '{part}'")

            # Match PO folder: e.g., '2416_02 AMEX 8738' or '2416-02 AMEX 8738'
            po_match = re.match(r'^(\d+)[_-](\d+)\s+(.*?)(\d{4})?$', part)
            if po_match:
                po_number = po_match.group(2)
                credit_card_digits = po_match.group(4)
                vendor_name = po_match.group(3).strip()

                # Check if the last four digits are present, indicating a credit card folder
                if credit_card_digits:
                    po_type = "cc"
                    vendor_name += " " + credit_card_digits

                self.logger.debug(
                    f"Found PO Number: '{po_number}', Vendor Name: '{vendor_name}', PO Type: '{po_type}' in folder part: '{part}'")
                continue

            # Match Project folder: e.g., '2416 - Whop Keynote' or '2416_Whop Keynote'
            project_match = re.match(r'^(\d+)\s*[-_]\s*.*', part)
            if project_match:
                project_id = project_match.group(1)
                self.logger.debug(f"Found Project ID: '{project_id}' in folder part: '{part}'")
                continue

            # If both Project ID and PO Number have been found, break
            if project_id and po_number:
                break

        if project_id and po_number:
            self.logger.debug(
                f"Successfully parsed Project ID: '{project_id}', PO Number: '{po_number}', Vendor Name: '{vendor_name}', PO Type: '{po_type}'")
            return project_id, po_number, vendor_name, po_type
        else:
            self.logger.error(
                f"Could not parse Project ID, PO Number, Vendor Name, or PO Type from folder path '{local_path}'.")
            return None, None, None, None

    def parse_filename(self, filename):
        """
        Parses the filename to extract project_id, po_number, receipt_number (optional), vendor_name, file_type, and invoice_number.
        Expected filename formats:
        - '2417_10 Vendor Name Invoice.pdf'
        - '2417_10 Vendor Name Invoice 3.pdf'
        - '2417_10_03 Citibank Receipt.pdf'
        """
        pattern = r'(?i)^(\d+)_(\d+)(?:_(\d+))?\s+(.+?)\s+(Invoice|W9|Receipt|W8-BEN|W8-BEN-E)(?:\s*(\d+))?\.(pdf|png|jpg|jpeg|tiff|bmp|heic)$'
        match = re.match(pattern, filename, re.IGNORECASE)
        if match:
            project_id = match.group(1)
            po_number = match.group(2)
            invoice_receipt_number = match.group(3) if match.group(3) else "01"
            vendor_name = match.group(4).strip()
            file_type = match.group(5).upper()  # Normalize to capitalize first letter
            invoice_number = match.group(6) if match.group(6) else "01"  # Invoice number may be 1

            # Debugging logs
            self.logger.debug(f"Parsed Filename '{filename}':")
            self.logger.debug(f"  Project ID: {project_id}")
            self.logger.debug(f"  PO Number: {po_number}")
            self.logger.debug(f"  Receipt Number: {invoice_receipt_number}")
            self.logger.debug(f"  Vendor Name: {vendor_name}")
            self.logger.debug(f"  File Type: {file_type}")
            self.logger.debug(f"  Invoice Number: {invoice_number}")

            return project_id, po_number, invoice_receipt_number, vendor_name, file_type
        else:
            self.logger.debug(f"Filename '{filename}' did not match the parsing pattern.")
            return None

    def get_parent_path(self, path_display):
        """
        Extracts the parent directory path from a given Dropbox path.

        Args:
            path_display (str): The full Dropbox path to a file or folder.

        Returns:
            str: The parent directory path. Returns an empty string if no parent exists.
        """
        if not path_display:
            self.logger.error("Empty path_display provided to get_parent_path.")
            return ""

        # Normalize the path to ensure consistent separators
        normalized_path = path_display.replace('\\', '/').rstrip('/')

        # Split the path into parts
        path_parts = normalized_path.split('/')

        if len(path_parts) <= 1:
            # No parent directory exists
            self.logger.debug(f"No parent directory for path: '{path_display}'. Returning empty string.")
            return ""

        # Extract the parent path by joining all parts except the last one
        parent_path = '/'.join(path_parts[:-1])
        self.logger.debug(f"Extracted parent path: '{parent_path}' from path: '{path_display}'")
        return parent_path



    def get_last_path_component_generic(self, path):
        return Path(path).parts[-1]

    def get_file_link(self, dropbox_path: str) -> str:
        dbx = dropbox_client.dbx
        try:
            result = dbx.sharing_create_shared_link_with_settings(dropbox_path)
            link = result.url
            self.logger.debug(f"Generated file link for {dropbox_path}: {link}")
            return link
        except dropbox.exceptions.ApiError as e:
            # Directly check if this error is shared_link_already_exists
            if e.error.is_shared_link_already_exists():
                # A link already exists, so let's retrieve it
                self.logger.debug("Shared link already exists, retrieving existing link.")
                return self.retrieve_existing_shared_link(dbx, dropbox_path)
            else:
                self.logger.error(f"Failed to get file link for {dropbox_path}: {e}", exc_info=True)
                return None

    def retrieve_existing_shared_link(self, dbx, dropbox_path: str) -> str:
        """
        Retrieve an existing shared link for the specified file.
        """
        try:
            # List all shared links for the user and filter by the path
            links = dbx.sharing_list_shared_links(path=dropbox_path)
            for link in links.links:
                if link.path_lower == dropbox_path.lower():
                    self.logger.debug(f"Found existing shared link for {dropbox_path}: {link.url}")
                    return link.url
            self.logger.warning(
                f"No existing shared link found for {dropbox_path}, even though Dropbox reported one exists.")
            return None
        except Exception as e:
            self.logger.error(f"Failed to retrieve existing shared link for {dropbox_path}: {e}", exc_info=True)
            return None

dropbox_util = DropboxUtil()
