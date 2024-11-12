import pytesseract
from pdf2image import convert_from_path
from datetime import datetime, timedelta

import PyPDF2
import re
import os
import logging

from processors.openai_util import (
    extract_receipt_info_with_openai,
    extract_info_with_openai
)

from webhook.database_util import update_event_status
from processors.monday_util import (
    find_item_by_project_and_po,
    find_contact_item_by_name,
    is_contact_info_complete,
    get_group_id_by_project_id,
    create_item,
    column_values_formatter,
    subitem_column_values_formatter,
    update_item_columns,
    find_subitem_by_invoice_or_receipt_number,
    create_subitem,
    update_vendor_description_in_monday
)

from webhook.dropbox_client import create_share_link, get_dropbox_client


def extract_text_from_file(dropbox_path):
    """
    Extracts text from a file in Dropbox using OCR or direct text extraction.

    Args:
        dropbox_path (str): The Dropbox path to the file.

    Returns:
        str: Extracted text from the file.
    """
    dbx_client = get_dropbox_client()
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
        text = extract_text_from_pdf(temp_file_path)
        if not text:
            logging.info(f"No text extracted from '{dropbox_path}' using direct extraction. Attempting OCR.")
            text = extract_text_with_ocr(temp_file_path)

        # Remove the temporary file
        os.remove(temp_file_path)
        return text
    except Exception as e:
        logging.error(f"Error extracting text from '{dropbox_path}': {e}", exc_info=True)
        raise  # Propagate exception to be handled by the caller


def extract_text_from_pdf(file_path):
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
        raise  # Propagate exception to be handled by the caller


def extract_text_with_ocr(file_path):
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
        raise  # Propagate exception to be handled by the caller


def is_po_folder(local_path):
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
        logging.debug("Detected file path. Extracted parent folder path.")
    else:
        folder_path = normalized_path

    logging.debug(f"Normalized Path Parts: {path_parts}")

    # Adjust path depth check based on your actual structure
    if len(path_parts) < 3:
        logging.debug(f"Path '{local_path}' does not have enough parts to be a valid PO folder.")
        return False

    # Extract key components from the path
    folder_name = path_parts[-1]  # PO folder name
    purchase_orders_folder = path_parts[-2]  # Should be "1. Purchase Orders"
    project_folder = path_parts[-3]  # Project folder name

    logging.debug(
        f"Checking PO folder: Project='{project_folder}', Purchase Orders='{purchase_orders_folder}', PO Folder='{folder_name}'")

    # Check if the folder name matches the expected PO folder pattern
    po_folder_pattern = r'^(\d+)[_-](\d+)\s+.*$'
    if not re.match(po_folder_pattern, folder_name):
        logging.debug(f"Folder name '{folder_name}' does not match PO folder pattern.")
        return False

    # Check if the "1. Purchase Orders" folder is correct (case-insensitive)
    if purchase_orders_folder.lower() != "1. purchase orders":
        logging.debug(f"Expected '1. Purchase Orders', but found '{purchase_orders_folder}'.")
        return False

    # Check if the project folder starts with a numeric project ID
    project_pattern = r'^\d+\s*[-_]\s*.*'
    if not re.match(project_pattern, project_folder):
        logging.debug(f"Project folder '{project_folder}' does not match project pattern.")
        return False

    # If all conditions are met, it's a valid PO folder in the correct structure
    logging.debug(f"Path '{local_path}' is a valid PO folder.")
    return True


def parse_folder_path(local_path):
    """
    Parses the folder path to extract the Project ID, PO Number, Vendor Name,
    and PO Type (vendor or cc).

    Expected folder structure: .../2024/Project Folder/Purchase Order Folder/...
    Example CC: '/2416 - Whop Keynote/1. Purchase Orders/2416_02 AMEX 8738/
    Example Vendor: '/2416 - Whop Keynote/1. Purchase Orders/2416_02 Jordan Sakai/


    Returns:
        Tuple (project_id, po_number, vendor_name, po_type) if found, else (None, None, None, None)
    """
    # Split the path into parts
    path_parts = local_path.strip(os.sep).split(os.sep)
    logging.debug(f"Path parts for parsing: {path_parts}")
    # Reverse to start searching from the deepest directory
    path_parts_reversed = path_parts[::-1]

    project_id = None
    po_number = None
    vendor_name = None
    po_type = "vendor"  # default to vendor unless it's a credit card (cc) folder

    logging.debug(f"Parsing folder path: {local_path}")

    for part in path_parts_reversed:
        if not part:
            continue  # Skip empty parts resulting from leading/trailing slashes
        logging.debug(f"Checking folder part: '{part}'")

        # Updated regex to ensure four digits are preceded by a space
        po_match = re.match(r'^(\d+)[_-](\d+)\s+([A-Za-z\s]+?)(?:\s+(\d{4}))?$', part)
        if po_match:
            project_id_matched = po_match.group(1)
            po_number_matched = po_match.group(2)
            vendor_name_matched = po_match.group(3).strip()
            credit_card_digits = po_match.group(4)

            # Debug logs for matched groups
            logging.debug(f"Regex Groups - Project ID: {project_id_matched}, PO Number: {po_number_matched}, Vendor Name: {vendor_name_matched}, CC Digits: {credit_card_digits}")

            # Check if the last four digits are present, indicating a credit card folder
            if credit_card_digits:
                po_type = "cc"
                vendor_name_matched += f" {credit_card_digits}"
                logging.debug(f"Identified as CC Vendor. PO Type set to '{po_type}'. Vendor Name updated to '{vendor_name_matched}'.")
            else:
                logging.debug(f"Identified as Vendor. PO Type remains '{po_type}'. Vendor Name is '{vendor_name_matched}'.")

            project_id = project_id_matched
            po_number = po_number_matched
            vendor_name = vendor_name_matched

            continue

        # Match Project folder: e.g., '2416 - Whop Keynote' or '2416_Whop Keynote'
        project_match = re.match(r'^(\d+)\s*[-_]\s*.*', part)
        if project_match:
            project_id_matched = project_match.group(1)
            project_id = project_id_matched
            logging.debug(f"Found Project ID: '{project_id}' in folder part: '{part}'")
            continue

        # If both Project ID and PO Number have been found, break
        if project_id and po_number:
            break

    if project_id and po_number and vendor_name:
        logging.debug(
            f"Successfully parsed Project ID: '{project_id}', PO Number: '{po_number}', Vendor Name: '{vendor_name}', PO Type: '{po_type}'")
        return project_id, po_number, vendor_name, po_type
    else:
        logging.error(
            f"Could not parse Project ID, PO Number, Vendor Name, or PO Type from folder path '{local_path}'.")
        return None, None, None, None


def parse_filename(filename):
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
        logging.debug(f"Parsed Filename '{filename}':")
        logging.debug(f"  Project ID: {project_id}")
        logging.debug(f"  PO Number: {po_number}")
        logging.debug(f"  Receipt/Invoice Number: {invoice_receipt_number}")
        logging.debug(f"  Vendor Name: {vendor_name}")
        logging.debug(f"  File Type: {file_type}")
        logging.debug(f"  Invoice Number: {invoice_number}")

        return project_id, po_number, invoice_receipt_number, vendor_name, file_type
    else:
        logging.debug(f"Filename '{filename}' did not match the parsing pattern.")
        return None


def add_tax_form_to_invoice(po_item_id, dropbox_path, tax_file_type, dbx_client, vendor_name):
    share_link = create_share_link(dbx_client, dropbox_path)

    # Attempt to find the contact associated with the vendor
    contact_info = find_contact_item_by_name(vendor_name)

    if contact_info:
        contact_id = contact_info['item_id']
        contact_columns = contact_info['column_values']
        logging.debug(f"Found contact ID for Vendor: {vendor_name}. Contact ID: {contact_id}")
    else:
        contact_id = None
        contact_columns = {}
        logging.debug(f"No contact found for Vendor: {vendor_name}. Proceeding without contact ID.")

    if contact_id and is_contact_info_complete(contact_columns):
        status = "Approved"
    else:
        status = "Needs Verification"

    logging.debug(f"Set PO status to '{status}' based on PO type and contact data completeness.")

    column_values = column_values_formatter(tax_file_link=share_link, status=status, tax_file_type=tax_file_type, contact_id=contact_id
)
    update_item_columns(po_item_id, column_values)


def get_parent_path(path_display):
    """
    Extracts the parent directory path from a given Dropbox path.

    Args:
        path_display (str): The full Dropbox path to a file or folder.

    Returns:
        str: The parent directory path. Returns an empty string if no parent exists.
    """
    if not path_display:
        logging.error("Empty path_display provided to get_parent_path.")
        return ""

    # Normalize the path to ensure consistent separators
    normalized_path = path_display.replace('\\', '/').rstrip('/')

    # Split the path into parts
    path_parts = normalized_path.split('/')

    if len(path_parts) <= 1:
        # No parent directory exists
        logging.debug(f"No parent directory for path: '{path_display}'. Returning empty string.")
        return ""

    # Extract the parent path by joining all parts except the last one
    parent_path = '/'.join(path_parts[:-1])
    logging.debug(f"Extracted parent path: '{parent_path}' from path: '{path_display}'")
    return parent_path


def process_folder(project_id, po_number, vendor_name, vendor_type, dropbox_path):
    """
    Processes a newly created folder to manage PO records and related tasks.

    Args:
        project_id (str): The project ID.
        po_number (str): The PO number.
        vendor_name (str): The vendor's name.
        vendor_type (str): The type of vendor ('vendor' or 'cc').
        dropbox_path (str): The Dropbox path to the folder.

    Returns:
        str: The created PO item ID in Monday.com.

    Raises:
        Exception: If any step fails during folder processing.
    """
    logging.info(f"Starting processing of folder: {dropbox_path}")

    dbx_client = get_dropbox_client()
    dbx = dbx_client.dbx

    # Check if a PO item already exists in Monday.com for this folder
    existing_item_id = find_item_by_project_and_po(project_id, po_number)
    if existing_item_id:
        logging.info(f"PO item already exists in Monday.com for Project ID {project_id} and PO Number {po_number}.")
        return existing_item_id  # Item already exists; return its ID

    logging.info(f"Creating new PO item in Monday.com for Project ID {project_id} and PO Number {po_number}.")

    # Generate a Dropbox folder link for this folder
    folder_link = create_share_link(dbx_client, dropbox_path)
    if not folder_link:
        logging.error(f"Could not generate Dropbox link for {dropbox_path}.")
        raise Exception(f"Failed to create share link for {dropbox_path}.")

    logging.debug(f"Generated Dropbox folder link: {folder_link}")

    # Set status based on contact data and PO type
    if vendor_type == "cc":
        status = "CC / PC"
        column_values = column_values_formatter(
            project_id=project_id,
            po_number=po_number,
            vendor_name=vendor_name,
            folder_link=folder_link,
            status=status
        )
    else:
        # Attempt to find the contact associated with the vendor
        contact_info = find_contact_item_by_name(vendor_name)
        if contact_info:
            contact_id = contact_info['item_id']
            contact_columns = contact_info['column_values']
            logging.debug(f"Found contact ID for Vendor: {vendor_name}. Contact ID: {contact_id}")
        else:
            contact_id = None
            contact_columns = {}
            logging.debug(f"No contact found for Vendor: {vendor_name}. Proceeding without contact ID.")

        if contact_id and is_contact_info_complete(contact_columns):
            status = "Approved"
        else:
            status = "Tax Form Needed"

        logging.debug(f"Set PO status to '{status}' based on PO type and contact data completeness.")

        column_values = column_values_formatter(
            project_id=project_id,
            po_number=po_number,
            vendor_name=vendor_name,
            folder_link=folder_link,
            status=status,
            contact_id=contact_id
        )

    # Get the group ID for the project and create the new item in Monday.com
    group_id = get_group_id_by_project_id(project_id)
    if not group_id:
        logging.error(f"Could not find group for Project ID {project_id}.")
        raise Exception(f"Group not found for Project ID {project_id}.")

    item_name = f"{vendor_name}"

    item_id = create_item(group_id, item_name, column_values)
    if not item_id:
        logging.error(f"Failed to create item in Monday.com for {item_name}.")
        raise Exception(f"Failed to create PO item for {vendor_name}.")

    logging.debug(f"Created new item in Monday.com with ID {item_id} and status '{status}'.")
    return item_id


def process_file(file_id, file_name, dropbox_path, project_id, po_number, vendor_name, vendor_type, file_type,
                 file_number):
    """
    Processes a file by validating the file path, parsing the filename, matching PO numbers,
    identifying or creating a PO item in Monday.com, determining file type, processing invoice
    or receipt data, generating item descriptions, and adding subitems to Monday.com.

    Args:
        file_id (str): The ID of the file/event.
        file_name (str): The name of the file.
        dropbox_path (str): The Dropbox path to the file.
        project_id (str): The project ID.
        po_number (str): The PO number.
        vendor_name (str): The vendor's name.
        vendor_type (str): The type of vendor ('vendor' or 'cc').
        file_type (str): The type of the file (e.g., 'INVOICE', 'RECEIPT').
        file_number (str): The invoice/receipt number.
    """
    logging.info(f"Starting processing of file: {dropbox_path}")

    try:

        # Verify that the project_id and po_number from the path match those provided
        folder_path = os.path.dirname(dropbox_path)
        folder_project_id, folder_po_number, folder_vendor_name, folder_vendor_type = parse_folder_path(folder_path)
        if folder_project_id != project_id or folder_po_number != po_number:
            logging.error(f"Project ID or PO Number mismatch between file and folder for file: {dropbox_path}")
            update_event_status(file_id, 'failed')
            return

        # Continue with existing processing logic...
        # Ensure that 'vendor_type' is used from the event data or re-assign it if necessary
        vendor_type = folder_vendor_type

        # Parse the filename to extract required information
        parsed_data = parse_filename(file_name)
        if not parsed_data:
            logging.error(f"Failed to parse filename for file: {dropbox_path}")
            update_event_status(file_id, 'failed')  # Report failure to the database
            return

        project_id_parsed, file_po_number, invoice_receipt_number, vendor_name_parsed, file_type_parsed = parsed_data

        # Verify that the PO number matches
        if file_po_number != po_number:
            logging.error(
                f"PO number mismatch: File PO number ({file_po_number}) does not match Database PO number ({po_number}).")
            update_event_status(file_id, 'failed')  # Report failure to the database
            return

        logging.debug(f"File PO number matches Database PO number: {file_po_number}")

        # Find or create the PO item in Monday.com
        po_item_id = find_item_by_project_and_po(project_id, po_number)
        if not po_item_id:
            logging.info(
                f"PO item not found in Monday.com for Project ID {project_id} and PO number {po_number}. Initiating creation.")
            # If the PO item doesn't exist, call process_folder to create it
            po_item_id = process_folder(project_id, po_number, vendor_name, vendor_type, os.path.dirname(dropbox_path))
            if not po_item_id:
                logging.error(
                    f"Failed to create or find PO item after processing folder for Project ID {project_id} and PO number {po_number}.")
                update_event_status(file_id, 'failed')  # Report failure to the database
                return
            logging.debug(f"Successfully created PO item in Monday.com with ID {po_item_id}.")

        else:
            logging.debug(f"Found PO item in Monday.com: {po_item_id}")

        # Access the Dropbox client
        dbx_client = get_dropbox_client()
        dbx = dbx_client.dbx

        # Determine File Type and Process Accordingly
        if file_type_parsed in ['W9', 'W8-BEN', 'W8-BEN-E']:
            logging.info(f"File is a {file_type_parsed}. Linking to PO item.")
            # Link tax form to the PO item as a reference
            add_tax_form_to_invoice(po_item_id, dropbox_path, file_type_parsed, dbx_client, vendor_name)
            logging.debug(f"Successfully linked {file_type_parsed} to PO item ID {po_item_id}.")
            # Mark as processed since linking is complete
            update_event_status(file_id, 'processed')
            return  # Tax form processing is complete here

        elif file_type_parsed == 'INVOICE' and vendor_type == 'vendor':
            # Check if the invoice has already been logged
            logging.info(f"File is an Invoice. Checking if it has already been logged.")
            if find_subitem_by_invoice_or_receipt_number(po_item_id, invoice_receipt_number=invoice_receipt_number):
                logging.info(f"Invoice already logged -- Skipping processing.")
                update_event_status(file_id, 'duplicate')  # Report duplicate to the database
                return

            # Process invoice data and line items
            logging.info(f"Processing Invoice: {file_name}")
            line_items, vendor_description = extract_invoice_data_from_file(dropbox_path)
            if not line_items:
                logging.error(f"Failed to extract invoice data from file: {dropbox_path}")
                update_event_status(file_id, 'failed')  # Report failure to the database
                return

            # Update vendor description if necessary
            if vendor_description:
                update_vendor_description_in_monday(po_item_id, vendor_description)

            # Get Dropbox file link
            share_link = create_share_link(dbx_client, dropbox_path)
            if not share_link:
                logging.error(f"Failed to create share link for {dropbox_path}")
                update_event_status(file_id, 'failed')  # Report failure to the database
                return

            # Create line items in Monday.com
            for item in line_items:
                # Defensive access to keys
                description = item.get('description', 'No Description Provided')
                if 'description' not in item:
                    logging.warning(f"'description' key missing in item: {item}")

                try:
                    line_item_column_values = subitem_column_values_formatter(
                        date=item['date'],
                        description=description,  # Correct key usage
                        rate=item['rate'],
                        quantity=item['quantity'],
                        status=None,
                        file_id=invoice_receipt_number,
                        account_number=item['account_number'],
                        link=share_link,
                        due_date=item['due_date']
                    )
                    created_subitem_id = create_subitem(po_item_id, vendor_name, line_item_column_values)
                    if not created_subitem_id:
                        logging.error(f"Failed to create subitem for invoice line item: {item}")
                        raise Exception(f"Failed to create subitem for invoice line item: {item}")
                except Exception as e:
                    logging.error(f"Exception during subitem creation: {e}")
                    raise  # Propagate exception to be handled by the caller

            logging.info(f"Successfully processed Invoice: {file_name}")
            # Mark as processed after successful invoice processing
            update_event_status(file_id, 'processed')
            return

        elif file_type_parsed == 'RECEIPT' and vendor_type == 'cc':
            # Check if the receipt has already been logged
            logging.info(f"File is a receipt. Checking if it has already been logged.")
            if find_subitem_by_invoice_or_receipt_number(po_item_id, invoice_receipt_number=invoice_receipt_number):
                logging.info(f"Receipt already logged -- Skipping processing.")
                update_event_status(file_id, 'duplicate')  # Report duplicate to the database
                return

            # Process receipt data
            logging.info(f"Processing receipt: {file_name}")
            receipt_data = extract_receipt_data_from_file(dropbox_path)
            if not receipt_data:
                logging.error(f"Failed to extract receipt data from file: {dropbox_path}")
                update_event_status(file_id, 'failed')  # Report failure to the database
                return

            # Extract receipt details
            total_amount = receipt_data.get('total_amount')
            date_of_purchase = receipt_data.get('date')
            description = receipt_data.get('description', 'Receipt')

            if total_amount is None or date_of_purchase is None:
                logging.error(f"Essential receipt data missing in file: {dropbox_path}")
                update_event_status(file_id, 'failed')  # Report failure to the database
                return

            # Get Dropbox file link
            share_link = create_share_link(dbx_client, dropbox_path)
            if not share_link:
                logging.error(f"Failed to create share link for {dropbox_path}")
                update_event_status(file_id, 'failed')  # Report failure to the database
                return

            # Create a single subitem for the receipt with total amount
            try:
                column_values = subitem_column_values_formatter(
                    date=date_of_purchase,
                    description=description,  # Description from receipt
                    rate=total_amount,  # Total amount as rate
                    quantity=1,  # Quantity is 1 for total
                    status='Paid',  # No specific status required
                    file_id=invoice_receipt_number,
                    account_number="5000",  # Always Cost of Goods Sold
                    link=share_link,
                    due_date=date_of_purchase  # Due date same as purchase date
                )
                created_subitem_id = create_subitem(po_item_id, vendor_name, column_values)
                if not created_subitem_id:
                    logging.error(f"Failed to create subitem for receipt: {receipt_data}")
                    raise Exception(f"Failed to create subitem for receipt: {receipt_data}")
            except Exception as e:
                logging.error(f"Exception during receipt subitem creation: {e}")
                raise  # Propagate exception to be handled by the caller

            logging.info(f"Successfully processed Receipt: {file_name}")
            # Mark as processed after successful receipt processing
            update_event_status(file_id, 'processed')
            return

        else:
            logging.error(f"Unknown file type '{file_type_parsed}' for file: {dropbox_path}")
            update_event_status(file_id, 'failed')  # Report failure to the database
            return

    except Exception as e:
        logging.error(f"Failed to process file {dropbox_path}: {e}", exc_info=True)
        update_event_status(file_id, 'failed')  # Report failure to the database


def extract_receipt_info_with_openai_from_file(dropbox_path):
    """
    Extracts receipt information from a file located in Dropbox.

    Args:
        dropbox_path (str): The Dropbox path to the receipt file.

    Returns:
        dict or None: Extracted receipt information or None if extraction fails.
    """
    try:
        text = extract_text_from_file(dropbox_path)
        if not text:
            logging.error(f"No text extracted from receipt file: {dropbox_path}")
            return None

        receipt_info = extract_receipt_info_with_openai(text)
        if not receipt_info:
            logging.error(f"OpenAI failed to extract receipt info from file: {dropbox_path}")
            return None

        return receipt_info

    except Exception as e:
        logging.error(f"Error extracting receipt info from file {dropbox_path}: {e}", exc_info=True)
        return None


def extract_invoice_data_from_file(dropbox_path):
    """
    Extracts invoice data from a given file and returns line items and vendor description.

    Args:
        dropbox_path (str): The path to the invoice file.

    Returns:
        tuple: A tuple containing a list of line items and the vendor description.
    """
    # Step 1: Extract text from the file (supports PDFs and images)
    text = extract_text_from_file(dropbox_path)
    if not text:
        logging.error("Failed to extract text from the invoice file.")
        return [], "Text extraction failed."

    # Step 2: Use OpenAI to process the text and extract structured invoice data
    try:
        invoice_data, error = extract_info_with_openai(text)
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
                datetime.strptime(date, '%Y-%m-%d') + timedelta(days=30)).strftime('%Y-%m-%d') if date else ""
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


def extract_receipt_data_from_file(dropbox_path):
    """
    Extracts receipt data from a given file and returns total amount, date, and description.

    Args:
        dropbox_path (str): The path to the receipt file.

    Returns:
        dict or None: A dictionary containing 'total_amount', 'date', and 'description' or None if extraction fails.
    """
    # Step 1: Extract text from the file (supports PDFs and images)
    text = extract_text_from_file(dropbox_path)
    if not text:
        logging.error("Failed to extract text from the receipt file.")
        return None

    # Step 2: Use OpenAI to process the text and extract receipt data
    try:
        receipt_data = extract_receipt_info_with_openai_from_file(dropbox_path)
        if not receipt_data:
            logging.error("OpenAI failed to extract receipt data.")
            return None
    except Exception as e:
        logging.error(f"OpenAI processing failed: {e}")
        return None

    # Step 3: Validate and return receipt data
    try:
        total_amount = receipt_data.get('total_amount')
        date_of_purchase = receipt_data.get('date')
        description = receipt_data.get('description', 'Receipt')

        if total_amount is None or date_of_purchase is None:
            logging.error("Essential receipt data missing.")
            return None

        # Ensure total_amount is a float
        total_amount = float(total_amount)

        # Validate date format
        try:
            datetime.strptime(date_of_purchase, '%Y-%m-%d')
        except ValueError:
            logging.error(f"Invalid date format: {date_of_purchase}. Expected YYYY-MM-DD.")
            return None

        return {
            'total_amount': total_amount,
            'date': date_of_purchase,
            'description': description
        }

    except Exception as e:
        logging.error(f"Error validating receipt data: {e}")
        return None