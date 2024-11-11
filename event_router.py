# event_router.py

import logging
from dropbox import files
from database_util import add_event_to_db
from dropbox_client import DropboxClientSingleton
from processors.file_util import (
    parse_filename,
    parse_folder_path,
    get_parent_path
)

# No need to import Celery tasks as we are not triggering them yet


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
        dbx_client = DropboxClientSingleton()
        dbx = dbx_client.dbx

        # Load the cursor
        cursor = dbx_client.load_cursor()

        if not cursor:
            # If no cursor, initialize it by getting the latest state
            logging.info(f"No cursor found for member ID {dbx_client.member_id}. Initializing cursor.")
            try:
                result = dbx.files_list_folder('', recursive=True)
                dbx_client.save_cursor(result.cursor)
                logging.info("Cursor initialized. No changes to process on first run.")
            except Exception as e:
                logging.error(f"Failed to initialize cursor: {e}", exc_info=True)
                return  # Exit as we couldn't initialize the cursor
            return  # Exit as we don't have any changes to process yet

        # Fetch changes since the last cursor
        try:
            changes, new_cursor = dbx_client.list_folder_changes(cursor)
            logging.debug(f"Fetched {len(changes)} changes.")
        except Exception as e:
            logging.error(f"Failed to fetch folder changes: {e}", exc_info=True)
            return  # Exit as we couldn't fetch changes

        # Separate changes into additions, deletions, and others
        additions = [
            change for change in changes
            if isinstance(change, (files.FileMetadata, files.FolderMetadata))
        ]
        deletions = [
            change for change in changes
            if isinstance(change, files.DeletedMetadata)
        ]
        other_changes = [
            change for change in changes
            if not isinstance(change, (files.FileMetadata, files.FolderMetadata, files.DeletedMetadata))
        ]

        logging.debug(f"Additions: {len(additions)}, Deletions: {len(deletions)}, Others: {len(other_changes)}")

        # Create dictionaries to map deletions and additions by (path, timestamp)
        deletions_map = {}
        for del_event in deletions:
            # DeletedMetadata might not have server_modified; use client_modified or another appropriate timestamp
            timestamp = getattr(del_event, 'server_modified', None) or getattr(del_event, 'client_modified', None)
            if timestamp:
                deletions_map[(del_event.path_display, timestamp)] = del_event

        additions_map = {}
        for add_event in additions:
            timestamp = getattr(add_event, 'server_modified', None) or getattr(add_event, 'client_modified', None)
            if timestamp:
                additions_map[(add_event.path_display, timestamp)] = add_event

        rename_events = []
        matched_additions_ids = set()
        matched_deletions_paths = set()

        # Match deletions and additions for rename detection
        for (old_path, del_timestamp), del_event in deletions_map.items():
            for (new_path, add_timestamp), add_event in additions_map.items():
                if del_timestamp == add_timestamp:
                    # Check if the event represents a rename (path has changed but parent is the same)
                    old_parent = get_parent_path(old_path)
                    new_parent = get_parent_path(new_path)

                    if old_parent == new_parent and del_event.name != add_event.name:
                        # Detected a rename within the same directory
                        rename_event = {
                            'file_id': getattr(add_event, 'id', None),
                            'file_name': add_event.name,
                            'path': new_path,
                            'old_path': old_path,
                            'event_type': 'file_renamed' if isinstance(add_event, files.FileMetadata) else 'folder_renamed'
                        }
                        rename_events.append(rename_event)
                        matched_additions_ids.add(add_event.id)
                        matched_deletions_paths.add(del_event.path_display)
                        logging.debug(f"Detected rename: {old_path} -> {new_path}")
                        break  # Move to the next deletion after a match

        logging.info(f"Detected {len(rename_events)} rename events.")

        # Process rename events and add to the database
        for rename in rename_events:
            try:
                # Extract details from the path or filename using file_util functions
                # For renames, path has changed, so we need to parse the new path
                parsed_data = parse_filename(rename['file_name']) if 'file_' in rename['event_type'] else parse_folder_path(rename['path'])
                if parsed_data:
                    project_id, po_number, invoice_receipt_number, vendor_name, file_type = parsed_data
                else:
                    project_id, po_number, vendor_name, po_type = parse_folder_path(rename['path'])

                # Prepare data to add to the database
                event_data = {
                    'file_id': rename['file_id'],
                    'file_name': rename['file_name'],
                    'path': rename['path'],
                    'old_path': rename['old_path'],
                    'event_type': rename['event_type'],
                    'project_id': project_id,
                    'po_number': po_number,
                    'vendor_name': vendor_name,
                    'vendor_type': po_type,
                    'file_type': file_type if 'file_' in rename['event_type'] else None,
                    'file_number': invoice_receipt_number,
                    'dropbox_share_link': None,
                    'file_stream_link': None
                }

                # Add event to the database
                event_id = add_event_to_db(**event_data)
                logging.info(f"Processed rename event: {rename['old_path']} -> {rename['path']} (Event ID: {event_id})")
            except Exception as e:
                logging.error(f"Failed to process rename event for {rename['path']}: {e}", exc_info=True)

        # Process remaining additions
        for add_event in additions:
            if add_event.id in matched_additions_ids:
                continue  # Already processed as rename

            try:
                # Determine if it's a file or folder
                if isinstance(add_event, files.FileMetadata):
                    # Process file event
                    parsed_data = parse_filename(add_event.name)
                    parent_path = get_parent_path(add_event.path_display)
                    project_id, po_number, vendor_name, po_type = parse_folder_path(parent_path)
                    if not parsed_data:
                        logging.error(f"Failed to parse filename for file: {add_event.path_display}")
                        continue

                    project_id, po_number, invoice_receipt_number, vendor_name, file_type = parsed_data

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
                        'vendor_type': po_type,
                        'file_type': file_type,
                        'file_number': invoice_receipt_number,
                        'dropbox_share_link': None,
                        'file_stream_link': None
                    }

                    # Add event to the database
                    event_id = add_event_to_db(**event_data)
                    logging.info(f"Processed and added file event: {add_event.path_display} (Event ID: {event_id})")

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
                    event_id = add_event_to_db(**event_data)
                    logging.info(f"Processed and added folder event: {add_event.path_display} (Event ID: {event_id})")

            except Exception as e:
                logging.error(f"Error processing event for {add_event.path_display}: {e}", exc_info=True)

        # Process remaining deletions (do not add to the database)
        for del_event in deletions:
            if del_event.path_display in matched_deletions_paths:
                continue  # Already processed as rename
            # Skip adding deletions to the database
            logging.info(f"Skipping deletion event: {del_event.path_display}")

        # Optionally, process other change types like moves or renames if Dropbox provides them
        for change in other_changes:
            try:
                if isinstance(change, files.FileMoveMetadata):
                    old_path = getattr(change, 'previous_path_display', None)
                    new_path = change.path_display

                    if not old_path:
                        logging.error(f"Move event missing old path for file: {new_path}")
                        continue

                    # Extract details from the new path
                    parsed_data = parse_filename(change.name)
                    if not parsed_data:
                        logging.error(f"Failed to parse filename for moved file: {new_path}")
                        continue
                    parent_path = get_parent_path(new_path)
                    project_id, po_number, vendor_name, po_type = parse_folder_path(parent_path)
                    project_id, po_number, invoice_receipt_number, vendor_name, file_type = parsed_data

                    # Prepare data to add to the database
                    event_data = {
                        'file_id': change.id,
                        'file_name': change.name,
                        'path': new_path,
                        'old_path': old_path,
                        'event_type': 'file_moved',
                        'project_id': project_id,
                        'po_number': po_number,
                        'vendor_name': vendor_name,
                        'vendor_type': po_type,
                        'file_type': file_type,
                        'file_number': invoice_receipt_number,
                        'dropbox_share_link': None,
                        'file_stream_link': None
                    }

                    # Add event to the database
                    event_id = add_event_to_db(**event_data)
                    logging.info(f"Processed file move event: {old_path} -> {new_path} (Event ID: {event_id})")

                elif isinstance(change, files.FolderMoveMetadata):
                    old_path = getattr(change, 'previous_path_display', None)
                    new_path = change.path_display

                    if not old_path:
                        logging.error(f"Move event missing old path for folder: {new_path}")
                        continue

                    # Extract details from the new path
                    parsed_data = parse_folder_path(new_path)
                    if not parsed_data:
                        logging.error(f"Failed to parse folder path for moved folder: {new_path}")
                        continue

                    project_id, po_number, vendor_name, po_type = parsed_data

                    # Prepare data to add to the database
                    event_data = {
                        'file_id': None,
                        'file_name': change.name,
                        'path': new_path,
                        'old_path': old_path,
                        'event_type': 'folder_moved',
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
                    event_id = add_event_to_db(**event_data)
                    logging.info(f"Processed folder move event: {old_path} -> {new_path} (Event ID: {event_id})")

                elif isinstance(change, files.FileRenameMetadata):
                    old_path = getattr(change, 'previous_path_display', None)
                    new_path = change.path_display

                    if not old_path:
                        logging.error(f"Rename event missing old path for file: {new_path}")
                        continue

                    # Extract details from the new path
                    parsed_data = parse_filename(change.name)
                    if not parsed_data:
                        logging.error(f"Failed to parse filename for renamed file: {new_path}")
                        continue
                    parent_path = get_parent_path(new_path)
                    project_id, po_number, vendor_name, po_type = parse_folder_path(parent_path)
                    project_id, po_number, invoice_receipt_number, vendor_name, file_type = parsed_data

                    # Prepare data to add to the database
                    event_data = {
                        'file_id': change.id,
                        'file_name': change.name,
                        'path': new_path,
                        'old_path': old_path,
                        'event_type': 'file_renamed',
                        'project_id': project_id,
                        'po_number': po_number,
                        'vendor_name': vendor_name,
                        'vendor_type': po_type,
                        'file_type': file_type,
                        'file_number': invoice_receipt_number,
                        'dropbox_share_link': None,
                        'file_stream_link': None
                    }

                    # Add event to the database
                    event_id = add_event_to_db(**event_data)
                    logging.info(f"Processed file rename event: {old_path} -> {new_path} (Event ID: {event_id})")

                elif isinstance(change, files.FolderRenameMetadata):
                    old_path = getattr(change, 'previous_path_display', None)
                    new_path = change.path_display

                    if not old_path:
                        logging.error(f"Rename event missing old path for folder: {new_path}")
                        continue

                    # Extract details from the new path
                    parsed_data = parse_folder_path(new_path)
                    if not parsed_data:
                        logging.error(f"Failed to parse folder path for renamed folder: {new_path}")
                        continue

                    project_id, po_number, vendor_name, po_type = parsed_data

                    # Prepare data to add to the database
                    event_data = {
                        'file_id': None,
                        'file_name': change.name,
                        'path': new_path,
                        'old_path': old_path,
                        'event_type': 'folder_renamed',
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
                    event_id = add_event_to_db(**event_data)
                    logging.info(f"Processed folder rename event: {old_path} -> {new_path} (Event ID: {event_id})")

                else:
                    logging.warning(f"Unhandled change type: {type(change)}")
            except Exception as e:
                logging.error(f"Failed to process other change event for {change.path_display}: {e}", exc_info=True)

        # Save the new cursor after processing all changes
        try:
            dbx_client.save_cursor(new_cursor)
            logging.info("Cursor successfully updated after processing changes.")
        except Exception as e:
            logging.error(f"Failed to save cursor: {e}", exc_info=True)

    finally:
        # Ensure that the cursor is saved even if an error occurs
        try:
            if 'new_cursor' in locals():
                dbx_client.save_cursor(new_cursor)
                logging.info("Cursor successfully updated in the finally block.")
        except Exception as e:
            logging.error(f"Failed to save cursor in the finally block: {e}", exc_info=True)
