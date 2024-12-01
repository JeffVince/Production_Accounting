# /webhooks/dropbox_webhook_handler.py

import logging

from dropbox import files
from flask import Blueprint, request, jsonify

from dropbox_files.dropbox_client import dropbox_client
from dropbox_files.dropbox_service import dropbox_service
from dropbox_files.dropbox_util import dropbox_util
from utilities.singleton import SingletonMeta

dropbox_blueprint = Blueprint('dropbox_files', __name__)


class DropboxWebhookHandler(metaclass=SingletonMeta):
    def __init__(self):
        if not hasattr(self, '_initialized'):
            # Set up logging
            self.logger = logging.getLogger("app_logger")
            self.dropbox_service = dropbox_service
            self.logger.info("Dropbox Webhook Handler  initialized")
            self.dropbox_client = dropbox_client
            self.dropbox_util = dropbox_util
            self._initialized = True

    def handle_dropbox_event(self, event):
        """Handle incoming Dropbox webhook event."""
        self.logger.info("Received Dropbox event.")
        self.process_event_data(event)
        return jsonify({"message": "Dropbox event processed"}), 200

    def process_event_data(self, event_data):
        """
        Processes the event data received from Dropbox webhook.
        Fetches the latest changes since the last cursor,
        categorizes them by folder/file and event type (added, deleted),
        logs the changes, and ensures duplicate events with the same cursor are ignored.
        """
        self.logger.info("Starting to process event data...")

        try:
            dbx = self.dropbox_client

            # Load the last saved cursor
            cursor = self.dropbox_client.load_cursor()

            if not cursor:
                # If no cursor exists, initialize it by listing the current state
                self.logger.debug("No cursor found. Initializing cursor.")
                try:
                    result = dbx.list_root_folder('', recursive=True)
                    self.dropbox_client.save_cursor(result.cursor)
                    self.logger.debug("Cursor initialized. No changes to process on first run.")
                except Exception as e:
                    self.logger.error(f"Failed to initialize cursor: {e}", exc_info=True)
                    return  # Exit as we couldn't initialize the cursor
                return  # Exit as there are no changes to process yet

            # Fetch changes since the last cursor
            try:
                entries, new_cursor = dbx.list_folder_changes(cursor)
                changes = entries

                # **Save the new cursor immediately to mark this cursor as processed**
                # This ensures that if the same cursor is received again, it won't have any changes to process
                self.dropbox_client.save_cursor(new_cursor)
                self.logger.debug("Cursor saved to prevent duplicate processing.")
            except Exception as e:
                self.logger.error(f"Failed to fetch folder changes: {e}", exc_info=True)
                return  # Exit as we couldn't fetch changes

            # Initialize data structures to categorize changes
            categorized_changes = {
                'files_added': [],
                'folders_added': [],
                'deleted': [],
            }

            # Categorize changes by type
            for change in changes:
                if isinstance(change, files.FileMetadata):
                    categorized_changes['files_added'].append({
                        'id': change.id,
                        'name': change.name,
                        'path': change.path_display,
                    })
                elif isinstance(change, files.FolderMetadata):
                    categorized_changes['folders_added'].append({
                        'id': change.id,
                        'name': change.name,
                        'path': change.path_display,
                    })
                elif isinstance(change, files.DeletedMetadata):
                    self.logger.debug(f"DELETE: {change}")
                    categorized_changes['deleted'].append({
                        'id': None,  # ID may not be available for deletions
                        'name': change.name,
                        'path': change.path_display,
                    })
                else:
                    self.logger.debug(f"Unhandled change type: {type(change)}")
                    continue  # Skip unhandled change types

            # Log the categorized changes for debugging and monitoring
            self.logger.info(f"Changes ({len(categorized_changes['files_added'])}):")
            for item in categorized_changes['files_added']:
                self.logger.info(f"File Added: {self.dropbox_util.get_last_path_component_generic(item['path'])}")

            #  SEND FOR PROCESSING
            for item in categorized_changes['files_added']:
                self.dropbox_service.determine_file_type(item.get("path"))

        finally:
            # Ensure that the cursor is saved even if an error occurs during processing
            # This acts as a safety net to prevent reprocessing in case of failures
            try:
                if 'new_cursor' in locals():
                    self.dropbox_client.save_cursor(new_cursor)
                    self.logger.debug("Cursor successfully updated in the finally block.")
            except Exception as e:
                self.logger.error(f"Failed to save cursor in the finally block: {e}", exc_info=True)


dropbox_webhook_handler = DropboxWebhookHandler()


@dropbox_blueprint.route('/', methods=['GET', 'POST'])
def dropbox_webhook():
    if request.method == 'GET':
        # Verification challenge (if applicable)
        challenge = request.args.get('challenge')
        if challenge:
            return challenge, 200
        return jsonify({"message": "No challenge provided."}), 400
    elif request.method == 'POST':
        # Handle the Dropbox event
        event = request.get_json()
        return dropbox_webhook_handler.handle_dropbox_event(event)
