# tasks.py

import celeryapp
from database_util import update_event_in_db
import logging
from dropbox_client import DropboxClientSingleton
from processors.file_util import (
    create_share_link,
    extract_text_from_file
)
from processors.openai_util import (
    extract_receipt_info_with_openai,
    extract_info_with_openai
)


@celeryapp.celery_app.task
def test_task():
    logging.info("Test task executed successfully.")
    return "Task Completed"


@celeryapp.celery_app.task(bind=True, max_retries=3, default_retry_delay=60)
def enrich_file_event(self, event_id, dropbox_path, file_type, po_type):
    """
    Asynchronously enriches a file event by generating Dropbox share links,
    performing OCR, extracting data with OpenAI, and updating the database.

    Args:
        event_id (int): The ID of the event in the database.
        dropbox_path (str): The Dropbox path to the file.
        file_type (str): The type of the file (e.g., 'INVOICE', 'RECEIPT', etc.).
        po_type (str): The type of PO ('vendor' or 'cc').
    """
    logging.info(f"Starting enrichment for event ID: {event_id}, Path: {dropbox_path}")
    try:
        # Initialize Dropbox client
        dbx_client = DropboxClientSingleton()
        dbx = dbx_client.dbx

        # Generate Dropbox share link
        share_link = create_share_link(dbx, dropbox_path)
        logging.debug(f"Generated share link: {share_link}")

        # Perform OCR and extract text
        if file_type and file_type.upper() in ['INVOICE', 'RECEIPT']:
            text = extract_text_from_file(dropbox_path)
            if not text:
                logging.warning(f"No text extracted from '{dropbox_path}'. Skipping OCR and OpenAI processing.")
                ocr_data = None
                openai_data = None
            else:
                # Extract data using OpenAI
                if file_type.upper() == 'RECEIPT':
                    ocr_data = text  # Assuming OCR text is sufficient for receipts
                    openai_data = extract_receipt_info_with_openai(text)
                elif file_type.upper() == 'INVOICE':
                    ocr_data = text
                    openai_data, error = extract_info_with_openai(text)
                    if error:
                        logging.error(f"OpenAI processing failed for '{dropbox_path}': {error}")
                        openai_data = None
        else:
            ocr_data = None
            openai_data = None

        # Update the event in the database with enriched data
        update_event_in_db(
            event_id=event_id,
            dropbox_share_link=share_link,
            file_stream_link=share_link,  # Modify if different
            ocr_data=ocr_data,
            openai_data=openai_data
        )
        logging.info(f"Successfully enriched event ID: {event_id}")
    except Exception as e:
        logging.error(f"Failed to enrich event ID: {event_id} - {e}", exc_info=True)
        # Retry the task
        try:
            self.retry(exc=e)
        except self.MaxRetriesExceededError:
            logging.error(f"Max retries exceeded for event ID: {event_id}. Marking as failed.")
            update_event_in_db(
                event_id=event_id,
                status='failed'
            )


@celeryapp.celery_app.task(bind=True, max_retries=3, default_retry_delay=60)
def enrich_folder_event(self, event_id, dropbox_path):
    """
    Asynchronously enriches a folder event by generating Dropbox share links
    and performing any folder-specific processing.

    Args:
        event_id (int): The ID of the event in the database.
        dropbox_path (str): The Dropbox path to the folder.
    """
    logging.info(f"Starting enrichment for folder event ID: {event_id}, Path: {dropbox_path}")
    try:
        # Initialize Dropbox client
        dbx_client = DropboxClientSingleton()
        dbx = dbx_client.dbx

        # Generate Dropbox share link
        share_link = create_share_link(dbx, dropbox_path)
        logging.debug(f"Generated share link: {share_link}")

        # Perform any folder-specific processing here
        # Example: Aggregating data from multiple files within the folder
        # This can be customized based on your specific requirements

        # Update the event in the database with enriched data
        update_event_in_db(
            event_id=event_id,
            dropbox_share_link=share_link,
            file_stream_link=share_link  # Modify if different
            # Add any additional folder-specific fields here
        )
        logging.info(f"Successfully enriched folder event ID: {event_id}")
    except Exception as e:
        logging.error(f"Failed to enrich folder event ID: {event_id} - {e}", exc_info=True)
        # Retry the task
        try:
            self.retry(exc=e)
        except self.MaxRetriesExceededError:
            logging.error(f"Max retries exceeded for folder event ID: {event_id}. Marking as failed.")
            update_event_in_db(
                event_id=event_id,
                status='failed'
            )
