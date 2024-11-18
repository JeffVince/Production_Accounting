# dropbox_service.py
import sys
import os

from utilities.file_util import extract_text_from_pdf, extract_text_with_ocr

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import logging
from database.dropbox_database_util import fetch_pending_events, update_event_status
from utilities.file_util import process_file, process_folder
from dotenv import load_dotenv
from webhook.dropbox_client import get_dropbox_client

# Load environment variables from .env file
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,  # Set to DEBUG for more detailed logs
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler("../Dropbox Listener/logs/event_processor.log"),
        logging.StreamHandler(sys.stdout)
    ]
)

dbx_client = get_dropbox_client()


def process_event(event):
    """
    Processes a single event based on its type.
    """
    try:
        # Access event details by column names
        event_id = event['id']
        file_id = event['file_id']
        file_name = event['file_name']
        path = event['path']
        old_path = event['old_path']
        event_type = event['event_type']
        timestamp = event['timestamp']
        status = event['status']
        project_id = event['project_id']
        po_number = event['po_number']
        vendor_name = event['vendor_name']
        vendor_type = event['vendor_type']
        file_type = event['file_type']
        file_number = event['file_number']

        logging.info(f"Processing event ID: {event_id}, Type: {event_type}, Path: {path}")

        if not event_type:
            logging.warning(f"Event ID {event_id} has no event type. Skipping.")
            update_event_status(event_id, 'skipped')
            return

        if event_type == 'file_added':
            # Process file events
            success = process_file(
                file_id=file_id,
                file_name=file_name,
                dropbox_path=path,
                project_id=project_id,
                po_number=po_number,
                vendor_name=vendor_name,
                vendor_type=vendor_type,
                file_type=file_type,
                file_number=file_number
            )
        elif event_type == 'folder_added':
            # Process folder events
            success = process_folder(
                project_id=project_id,
                po_number=po_number,
                vendor_name=vendor_name,
                vendor_type=vendor_type,
                dropbox_path=path
            )
        else:
            logging.warning(f"Unknown event type '{event_type}' for event ID {event_id}. Skipping.")
            update_event_status(event_id, 'skipped')
            return

        # If processing is successful, update the event status to 'processed'
        if success:
            update_event_status(event_id, 'processed')
            logging.info(f"Successfully processed event ID {event_id}.")

    except Exception as e:
        logging.error(f"Error processing event ID {event_id}: {e}", exc_info=True)
        # Update the event status to 'failed'
        update_event_status(event_id, 'failed')


def run_event_processor():
    """
    Fetches and processes all pending events.
    """
    logging.info("Starting event processing cycle.")
    pending_events = fetch_pending_events()

    if not pending_events:
        logging.info("No pending events to process.")
        return

    for event in pending_events:
        process_event(event)

    logging.info("Event processing cycle completed.")


def extract_text_from_file(dropbox_path):
    """
    Extracts text from a file in Dropbox using OCR or direct text extraction.

    Args:
        dropbox_path (str): The Dropbox path to the file.

    Returns:
        str: Extracted text from the file.
    """
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

