# po_log_processor.py

import os
import logging
import csv
from utils.monday_util import (
    find_item_by_project_and_po,
    update_item_columns,
    update_subitem_columns,
    get_group_id_by_project_id,
    get_subitem_board_id,
    ACTUALS_BOARD_ID
)
from webhook.dropbox_client import get_dropbox_client

# Constants
BOARD_ID = ACTUALS_BOARD_ID
SUBITEM_BOARD_ID = get_subitem_board_id(BOARD_ID)
RTP_STATUS = "RTP"
PO_LOG_MISMATCH_STATUS = "PO Log Mismatch"


def download_po_log(dropbox_path, output_path):
    """
    Downloads the PO log from Dropbox using the direct file path to the specified output path.
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


def parse_po_log(file_path):
    """
    Parses the PO log file (txt, csv, tsv) and extracts relevant entries.
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
            mapped_entries = []

            for row in reader:
                phase = row.get('Phase', '').strip()
                vendor_name = row.get('Vendor', '').strip()
                description = row.get('Description', '').strip()
                actualized_amount_str = row.get('Actualized $', '').replace(',', '').strip()
                account = row.get('Account', '').strip()
                pay_id = row.get('Pay ID', '').strip()

                # Convert actualized amount to float
                try:
                    actualized_amount = float(actualized_amount_str)
                except ValueError:
                    logging.warning(f"Invalid total amount '{actualized_amount_str}' for {vendor_name}. Skipping entry.")
                    continue

                po_number = row.get('No.', '').strip() or 'Unknown'

                # Map the extracted data to Monday.com schema
                mapped_entry = {
                    "name": vendor_name,  # Primary identifier for the item
                    "numbers08": po_number,  # PO #
                    "text6": description,  # Description of the item
                    "subitems_due__1": "Pending",  # TBP status (You might want to set this based on your logic)
                    "status": "Processing",  # Verification Status
                    "people": "Jeffrey Haskell",  # Producer / PM (Hardcoded to your name for now)
                    "subitems_status__1": "To Process",  # Payment Status (Default)
                    "subitems_sub_total": actualized_amount,  # Total cost from subitems
                    "connect_boards1": "Vendor Contacts",  # Contact links (you'll need to set this up manually)
                    "mirror76": f"{vendor_name} Email",  # Vendor email (You may want to adjust this)
                }

                # Map subitem-specific details
                subitems = [{
                    "status4": "To Process",  # Status of the subitem
                    "type__1": "INV",  # Type (Invoice)
                    "text0": "N/A",  # Generic text field for additional input
                    "dropdown": "148",  # Example dropdown selection
                    "date": row.get('Date', ''),  # Date
                    "text__1": "N/A",  # Quantity as text (Adjust this if needed)
                    "text98": description,  # Subitem description
                    "numbers9": actualized_amount,  # Unit Price
                    "numbers0": 1,  # Quantity (Default to 1 if not provided)
                    "creation_log__1": "2024-11-08 10:00:00 UTC"  # Default creation log (Set dynamically if needed)
                }]

                mapped_entry["subitems"] = subitems  # Add subitems to the main item

                mapped_entries.append(mapped_entry)
                logging.debug(f"Mapped entry: {mapped_entry}")

            logging.info(f"Mapped {len(mapped_entries)} entries.")
            return mapped_entries

    except Exception as e:
        logging.error(f"Error parsing PO log file '{file_path}': {e}")
        return []


def process_po_log(dropbox_path):
    """
    Processes the PO log file from the given Dropbox file path and maps it to Monday.com.
    """
    # Step 1: Download the PO log
    output_path = 'po_log.txt'
    if not download_po_log(dropbox_path, output_path):
        logging.error("Failed to download PO log.")
        return False

    # Step 2: Parse the PO log
    mapped_entries = parse_po_log(output_path)
    if not mapped_entries:
        logging.error("No entries to process.")
        os.remove(output_path)  # Clean up the downloaded file
        return False

    # Step 3: Update Monday.com based on the mapped entries
    for entry in mapped_entries:
        try:
            # Find or create the item for the vendor
            item_id = find_item_by_project_and_po(entry["name"], entry["numbers08"])

            if not item_id:
                # Create the item if it doesn't exist
                group_id = get_group_id_by_project_id(entry["name"])  # Get the group ID for the project
                item_id = create_item(group_id, entry["name"], entry)

            # Update the item columns with the mapped data
            update_item_columns(item_id, entry)

            # Process subitems
            for subitem in entry["subitems"]:
                subitem_id = find_subitem_by_invoice_or_receipt_number(item_id, subitem["text98"])  # Update logic here if needed
                update_subitem_columns(subitem_id, subitem)

            logging.info(f"Processed item {entry['name']} with PO {entry['numbers08']} successfully.")

        except Exception as e:
            logging.error(f"Error processing entry {entry['name']} (PO {entry['numbers08']}): {e}")

    # Clean up the downloaded file
    os.remove(output_path)
    return True


if __name__ == "__main__":
    # Provide the Dropbox file path to the PO log file
    dropbox_po_log_path = '/2416 - Whop Keynote/5. Budget/1.3 Actuals/PO Logs/2416 PO Log.txt'  # Replace with actual Dropbox path

    # Set up logging
    logging.basicConfig(level=logging.INFO, format='%(levelname)s:%(message)s')

    # Step 1: Process the PO log
    success = process_po_log(dropbox_po_log_path)
    if success:
        logging.info("PO log processed successfully.")
    else:
        logging.error("Failed to process PO log.")