# event_router.py

import logging
from dropbox import files
from webhook.database_util import add_event_to_db
from webhook.dropbox_client import DropboxClientSingleton, get_dropbox_client
from processors.file_util import (
    parse_filename,
    parse_folder_path,
    get_parent_path
)
import os

# Assuming Celery or similar is used for task queues
# Uncomment and configure the following line based on your setup
# from celery_tasks import process_file_task


def process_event_data(event_data):
    """
    Processes the event data received from Dropbox webhook.
    Detects various event types (add, delete, rename, move) for files and folders.
    Extracts detailed information based on the path or filename.
    Adds relevant events to the database.
    """
    logging.info("Starting to process event data...")

    try:
        # Initialize Dropbox client
        dbx_client = get_dropbox_client()
        dbx = dbx_client.dbx
        # Load the cursor
        cursor = dbx_client.load_cursor()

        if not cursor:
            # Initialize cursor if not found
            logging.info(f"No cursor found for member ID {dbx_client.member_id}. Initializing cursor.")
            result = dbx_client.dbx.files_list_folder('', recursive=True)
            dbx_client.save_cursor(result.cursor)
            logging.info("Cursor initialized. No changes to process on first run.")
            return

        # Fetch changes since the last cursor
        changes, new_cursor = dbx_client.list_folder_changes(cursor)
        logging.debug(f"Fetched {len(changes)} changes.")

        # Separate changes into additions, deletions, and others
        additions = [change for change in changes if isinstance(change, (files.FileMetadata, files.FolderMetadata))]
        deletions = [change for change in changes if isinstance(change, files.DeletedMetadata)]
        other_changes = [change for change in changes if not isinstance(change, (files.FileMetadata, files.FolderMetadata, files.DeletedMetadata))]

        logging.debug(f"Additions: {len(additions)}, Deletions: {len(deletions)}, Others: {len(other_changes)}")

        # Process additions
        for add_event in additions:
            try:
                if isinstance(add_event, files.FileMetadata):
                    # Process file event
                    parsed_data = parse_filename(add_event.name)
                    if not parsed_data:
                        logging.error(f"Failed to parse filename for file: {add_event.path_display}")
                        continue

                    project_id, po_number, invoice_receipt_number, vendor_name, file_type = parsed_data

                    # Now, we need to get 'vendor_type' from the folder path
                    folder_path = os.path.dirname(add_event.path_display)
                    folder_parsed_data = parse_folder_path(folder_path)
                    if folder_parsed_data:
                        folder_project_id, folder_po_number, folder_vendor_name, po_type = folder_parsed_data
                        # Verify that the project_id and po_number match between file and folder
                        if folder_project_id != project_id or folder_po_number != po_number:
                            logging.error(f"Project ID or PO Number mismatch between file and folder for file: {add_event.path_display}")
                            continue
                        vendor_type = po_type
                    else:
                        logging.error(f"Failed to parse folder path for folder: {folder_path}")
                        continue

                    # Prepare data to add to the database
                    event_data = {
                        'file_id': add_event.id,
                        'file_name': add_event.name,
                        'path': add_event.path_display,
                        'old_path': None,
                        'event_type': 'file_added',
                        'project_id': project_id,
                        'po_number': po_number,
                        'vendor_name': vendor_name,
                        'vendor_type': vendor_type,
                        'file_type': file_type,
                        'file_number': invoice_receipt_number,
                        'dropbox_share_link': None,
                        'file_stream_link': None
                    }

                    # Add event to the database
                    event_id, is_duplicate = add_event_to_db(**event_data)
                    if is_duplicate:
                        logging.info(f"Processed and marked duplicate file event: {add_event.path_display} (Event ID: {event_id})")
                    else:
                        logging.info(f"Processed and added file event: {add_event.path_display} (Event ID: {event_id})")
                        # Enqueue the event for processing if it's not a duplicate
                        # process_file_task.delay(event_id)

                elif isinstance(add_event, files.FolderMetadata):
                    # Process folder event
                    parsed_data = parse_folder_path(add_event.path_display)
                    if not parsed_data:
                        logging.error(f"Failed to parse folder path for folder: {add_event.path_display}")
                        continue

                    project_id, po_number, vendor_name, po_type = parsed_data

                    # Prepare data to add to the database
                    event_data = {
                        'file_id': None,
                        'file_name': add_event.name,
                        'path': add_event.path_display,
                        'old_path': None,
                        'event_type': 'folder_added',
                        'project_id': project_id,
                        'po_number': po_number,
                        'vendor_name': vendor_name,
                        'vendor_type': po_type,
                        'file_type': None,
                        'file_number': None,
                        'dropbox_share_link': None,
                        'file_stream_link': None
                    }

                    # Add event to the database
                    event_id, is_duplicate = add_event_to_db(**event_data)
                    if is_duplicate:
                        logging.info(f"Processed and marked duplicate folder event: {add_event.path_display} (Event ID: {event_id})")
                    else:
                        logging.info(f"Processed and added folder event: {add_event.path_display} (Event ID: {event_id})")
                        # Enqueue the event for processing if it's not a duplicate
                        # process_folder_task.delay(event_id)

            except Exception as e:
                logging.error(f"Error processing event for {add_event.path_display}: {e}", exc_info=True)

        # Save the new cursor after processing all changes
        dbx_client.save_cursor(new_cursor)
        logging.info("Cursor successfully updated after processing changes.")

    except Exception as e:
        logging.error(f"Failed to process event data: {e}", exc_info=True)
