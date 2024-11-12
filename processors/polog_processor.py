import os
import logging
import csv
import sys
from processors.monday_util import (
    find_item_by_project_and_po,
    find_subitem_by_invoice_or_receipt_number,
    find_all_po_subitems,
    update_item_columns,
    update_subitem_columns,
    get_group_id_by_project_id,
    PO_STATUS_COLUMN_ID,
    SUBITEM_STATUS_COLUMN_ID,
    SUBITEM_RATE_COLUMN_ID,
    SUBITEM_QUANTITY_COLUMN_ID,
    SUBITEM_STATUS_COLUMN_ID,
)
from webhook.dropbox_client import get_dropbox_client

# Constants
BOARD_ID = "2562607316"  # Ensure this matches your Monday.com Board ID
SUBITEM_BOARD_ID = get_group_id_by_project_id  # Adjusted based on actual usage
RTP_STATUS = "RTP"
PO_LOG_MISMATCH_STATUS = "PO Log Mismatch"


def download_po_log(dropbox_path, output_path):
    """
    Downloads the PO log from Dropbox using the direct file path to the specified output path.

    Args:
        dropbox_path (str): The full Dropbox path to the PO log file.
        output_path (str): The local path where the file will be saved.

    Returns:
        bool: True if download is successful, False otherwise.
    """
    dbx_client = get_dropbox_client()
    dbx = dbx_client.dbx

    try:
        # Use the Dropbox API to download the file from the Dropbox path
        metadata, res = dbx.files_download(path=dropbox_path)
        with open(output_path, 'wb') as f:
            f.write(res.content)
        logging.info(f"Downloaded PO log to {output_path}")
        return True
    except Exception as e:
        logging.error(f"Error downloading PO log from '{dropbox_path}': {e}")
        return False


def list_folder_contents(folder_path):
    """
    Lists the contents of a specified Dropbox folder.

    Args:
        folder_path (str): The Dropbox path to the folder.

    Returns:
        list: A list of file and folder names within the specified folder.
    """
    dbx_client = get_dropbox_client()
    dbx = dbx_client.dbx

    try:
        result = dbx.files_list_folder(folder_path)
        contents = [entry.name for entry in result.entries]
        logging.info(f"Contents of '{folder_path}': {contents}")
        return contents
    except Exception as e:
        logging.error(f"Error listing contents of '{folder_path}': {e}")
        return []


def extract_project_id(dropbox_path):
    """
    Extracts the numeric project ID from the Dropbox file path.

    Args:
        dropbox_path (str): The full Dropbox path to the PO log file.

    Returns:
        str: The extracted numeric project ID or 'Unknown' if not found.
    """
    try:
        # Remove leading and trailing slashes and split the path
        parts = dropbox_path.strip('/').split('/')
        if len(parts) >= 1:
            project_folder = parts[0]  # Assuming the project folder is the first part
            # Extract the numeric part using regex
            import re
            match = re.match(r'(\d+)', project_folder)
            if match:
                project_id = match.group(1)
                logging.debug(f"Extracted project ID: {project_id}")
                return project_id
            else:
                logging.warning("No numeric project ID found in the project folder name.")
                return 'Unknown'
        else:
            logging.warning("Project ID not found in the Dropbox path.")
            return 'Unknown'
    except Exception as e:
        logging.error(f"Error extracting project ID from '{dropbox_path}': {e}")
        return 'Unknown'


def parse_po_log(file_path):
    """
    Parses the PO log file (txt, csv, tsv) and extracts 'RTP' entries.

    Args:
        file_path (str): The local path to the PO log file.

    Returns:
        list of tuples: Each tuple contains (po_number, vendor_name, total_amount).
    """
    try:
        # Determine the file extension to set the delimiter
        _, file_extension = os.path.splitext(file_path)
        file_extension = file_extension.lower()

        if file_extension == '.csv':
            delimiter = ','
        elif file_extension == '.tsv' or file_extension == '.txt':
            delimiter = '\t'
        else:
            logging.error(f"Unsupported file format: {file_extension}")
            return []

        with open(file_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f, delimiter=delimiter)
            to_process_entries = []

            for row in reader:
                status = row.get('Status', '').strip().upper()
                if status.lower() in {'to process', 'ready', 'rtp', 'ready to pay', 'for processing', 'to submit'}:
                    po_number = row.get('No.', '').strip()
                    vendor_name = row.get('Vendor', '').strip()
                    total_amount_str = row.get('Actualized $', '').replace(',', '').strip()

                    # Validate and convert total_amount to float
                    try:
                        total_amount = float(total_amount_str)
                    except ValueError:
                        logging.warning(f"Invalid total amount '{total_amount_str}' for PO {po_number}. Skipping entry.")
                        continue

                    # Handle missing PO numbers
                    if not po_number:
                        po_number = 'Unknown'

                    to_process_entries.append((po_number, vendor_name, total_amount))
                    logging.debug(f"Extracted RTP entry: PO {po_number}, Vendor '{vendor_name}', Amount {total_amount}")

            logging.info(f"Found {len(to_process_entries)} 'RTP' entries.")
            return to_process_entries

    except Exception as e:
        logging.error(f"Error parsing PO log file '{file_path}': {e}")
        return []


def process_po_log(dropbox_path):
    """
    Processes the PO log file from the given Dropbox file path.

    Args:
        dropbox_path (str): The full Dropbox path to the PO log file.

    Returns:
        bool: True if processing is successful, False otherwise.
    """
    # Determine the file extension
    _, file_extension = os.path.splitext(dropbox_path)
    file_extension = file_extension.lower()

    # Set output_path based on file extension
    if file_extension in ['.csv', '.tsv', '.txt']:
        output_path = 'po_log' + file_extension
    else:
        logging.error(f"Unsupported file format: {file_extension}")
        return False

    # Step 1: Download the PO log
    if not download_po_log(dropbox_path, output_path):
        logging.error("Failed to download PO log.")
        return False

    # Step 2: Parse the PO log
    to_process_entries = parse_po_log(output_path)
    if not to_process_entries:
        logging.error("No 'RTP' entries found in PO log.")
        os.remove(output_path)  # Clean up the downloaded file
        return False

    # Step 3: Extract Project ID from Dropbox Path
    project_id = extract_project_id(dropbox_path)
    if project_id == 'Unknown':
        logging.warning("Project ID is 'Unknown'. Entries may not be linked correctly.")

    # Step 4: Process each RTP entry
    for po_number, vendor_name, actual_total in to_process_entries:
        try:
            # Find the PO item in Monday.com using the extracted project_id
            po_item_id = find_item_by_project_and_po(project_id, po_number)
            if not po_item_id:
                logging.warning(f"PO item not found for PO {po_number} under project '{project_id}'. Skipping.")
                continue

            # Get all subitems under the PO item
            subitems = find_all_po_subitems(po_item_id)
            if not subitems:
                logging.warning(f"No subitems found under PO item {po_item_id} for PO {po_number}.")
                continue

            # Filter subitems: Skip those with status 'Paid' or 'RTP'
            relevant_subitems = []
            for subitem in subitems:
                subitem_status = None
                for column in subitem['column_values']:
                    if column['id'] == SUBITEM_STATUS_COLUMN_ID:
                        subitem_status = column.get('text', '').strip().upper()
                        break
                if subitem_status in ['PAID', 'RTP']:
                    logging.debug(f"Skipping subitem {subitem['id']} with status '{subitem_status}'.")
                    continue
                relevant_subitems.append(subitem)

            if not relevant_subitems:
                logging.info(f"No relevant subitems to process for PO {po_number}.")
                continue

            # Calculate the total amount from relevant subitems
            calculated_total = 0.0
            for subitem in relevant_subitems:
                rate = 0.0
                quantity = 1
                for column in subitem['column_values']:
                    if column['id'] == SUBITEM_RATE_COLUMN_ID:  # 'numbers9'
                        rate_text = column.get('text', '0').replace(',', '').replace('$', '')
                        try:
                            rate = float(rate_text) if rate_text else 0.0
                        except ValueError:
                            logging.warning(f"Invalid rate '{rate_text}' in subitem {subitem['id']}. Defaulting to 0.0.")
                            rate = 0.0
                    elif column['id'] == SUBITEM_QUANTITY_COLUMN_ID:  # 'numbers0'
                        quantity_text = column.get('text', '1').replace(',', '').replace('$', '')
                        try:
                            quantity = float(quantity_text) if quantity_text else 1.0
                        except ValueError:
                            logging.warning(f"Invalid quantity '{quantity_text}' in subitem {subitem['id']}. Defaulting to 1.0.")
                            quantity = 1.0
                calculated_total += rate * quantity

            logging.debug(f"Calculated total for PO {po_number}: {calculated_total}, Actual total: {actual_total}")

            # Compare calculated total with actual total
            if abs(calculated_total - actual_total) < 0.01:
                # Total matches: Set subitem status to 'RTP'
                new_status = RTP_STATUS
                for subitem in relevant_subitems:
                    subitem_id = subitem['id']
                    update_subitem_columns(subitem_id, {SUBITEM_STATUS_COLUMN_ID: {'label': new_status}})
                    logging.info(f"Updated subitem {subitem_id} status to '{new_status}'.")
            else:
                # Total mismatch: Set subitem status to 'PO Log Mismatch'
                new_status = PO_LOG_MISMATCH_STATUS
                for subitem in relevant_subitems:
                    subitem_id = subitem['id']
                    update_subitem_columns(subitem_id, {SUBITEM_STATUS_COLUMN_ID: {'label': new_status}})
                    logging.info(f"Updated subitem {subitem_id} status to '{new_status}' due to total mismatch.")

            # Note: The PO item's status is left unchanged as per user request.

        except Exception as e:
            logging.error(f"Error processing PO {po_number} for vendor '{vendor_name}': {e}")

    # Clean up the downloaded file
    os.remove(output_path)
    return True


if __name__ == "__main__":
    # Provide the Dropbox file path to the PO log file (txt, csv, tsv)
    dropbox_po_log_path = '/2416 - Whop Keynote/5. Budget/1.3 Actuals/PO Logs/2416 PO Log.txt'  # Replace with the actual Dropbox file path

    # Set up logging
    logging.basicConfig(
        level=logging.DEBUG,  # Set to DEBUG for more detailed logs; change to INFO in production
        format='%(levelname)s:%(name)s:%(message)s',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler("polog_processor.log")  # Log to a file for persistent storage
        ]
    )

    # Step 0: Verify the file exists in Dropbox
    folder_path = os.path.dirname(dropbox_po_log_path)
    logging.info(f"Listing contents of folder: {folder_path}")
    contents = list_folder_contents(folder_path)
    expected_file = os.path.basename(dropbox_po_log_path)
    if expected_file not in contents:
        logging.error(f"'{expected_file}' does not exist in '{folder_path}'. Please verify the file path and upload the file if necessary.")
        sys.exit(1)  # Exit the script as the file is not found

    # Step 1: Process the PO log
    success = process_po_log(dropbox_po_log_path)
    if success:
        logging.info("PO log processed successfully.")
    else:
        logging.error("Failed to process PO log.")