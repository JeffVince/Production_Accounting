# file_processor.py
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import logging
import time
import schedule
from database.dropbox_database_util import fetch_pending_events, update_event_status
from utils.file_util import process_file, process_folder
from dotenv import load_dotenv

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


def main():
    """
    Sets up the scheduler to run the event processor periodically.
    """
    # Schedule the processor to run every 5 seconds
    schedule.every(5).seconds.do(run_event_processor)

    logging.info("Event processor started. Waiting for scheduled runs...")

    while True:
        schedule.run_pending()
        time.sleep(1)


if __name__ == "__main__":
    main()