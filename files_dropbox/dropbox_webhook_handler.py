import logging
from dropbox import files
from flask import Blueprint, request, jsonify
from files_dropbox.dropbox_client import dropbox_client
from files_dropbox.dropbox_service import dropbox_service
from files_dropbox.dropbox_util import dropbox_util
from utilities.singleton import SingletonMeta
dropbox_blueprint = Blueprint('files_dropbox', __name__)

class DropboxWebhookHandler(metaclass=SingletonMeta):

    def __init__(self):
        if not hasattr(self, '_initialized'):
            self.logger = logging.getLogger('dropbox_logger')
            self.dropbox_service = dropbox_service
            self.logger.info('[__init__] - Dropbox Webhook Handler  initialized')
            self.dropbox_client = dropbox_client
            self.dropbox_util = dropbox_util
            self._initialized = True

    def handle_dropbox_event(self, event):
        """Handle incoming Dropbox webhook event."""
        self.logger.info('[handle_dropbox_event] - Received Dropbox event.')
        self.process_event_data(event)
        return (jsonify({'message': 'Dropbox event processed'}), 200)

    def process_event_data(self, event_data):
        """
        Processes the event data received from Dropbox webhook.
        Fetches the latest changes since the last cursor,
        categorizes them by folder/file and event type (added, deleted),
        logs the changes, and ensures duplicate events with the same cursor are ignored.
        """
        self.logger.info('[process_event_data] - Starting to process event data...')
        try:
            dbx = self.dropbox_client
            cursor = self.dropbox_client.load_cursor()
            if not cursor:
                self.logger.debug('[process_event_data] - No cursor found. Initializing cursor.')
                try:
                    result = dbx.list_root_folder('', recursive=True)
                    self.dropbox_client.save_cursor(result.cursor)
                    self.logger.debug('[process_event_data] - Cursor initialized. No changes to process on first run.')
                except Exception as e:
                    self.logger.error(f'[process_event_data] - Failed to initialize cursor: {e}', exc_info=True)
                    return
                return
            try:
                (entries, new_cursor) = dbx.list_folder_changes(cursor)
                changes = entries
                self.dropbox_client.save_cursor(new_cursor)
                self.logger.debug('[process_event_data] - Cursor saved to prevent duplicate processing.')
            except Exception as e:
                self.logger.error(f'[process_event_data] - Failed to fetch folder changes: {e}', exc_info=True)
                return
            categorized_changes = {'files_added': [], 'folders_added': [], 'deleted': []}
            for change in changes:
                if isinstance(change, files.FileMetadata):
                    categorized_changes['files_added'].append({'id': change.id, 'name': change.name, 'path': change.path_display})
                elif isinstance(change, files.FolderMetadata):
                    categorized_changes['folders_added'].append({'id': change.id, 'name': change.name, 'path': change.path_display})
                elif isinstance(change, files.DeletedMetadata):
                    self.logger.debug(f'[process_event_data] - DELETE: {change}')
                    categorized_changes['deleted'].append({'id': None, 'name': change.name, 'path': change.path_display})
                else:
                    self.logger.debug(f'[process_event_data] - Unhandled change type: {type(change)}')
                    continue
            self.logger.info(f"[process_event_data] - Changes ({len(categorized_changes['files_added'])}):")
            for item in categorized_changes['files_added']:
                self.logger.info(f"[process_event_data] - File Added: {self.dropbox_util.get_last_path_component_generic(item['path'])}")
            for item in categorized_changes['files_added']:
                self.dropbox_service.determine_file_type(item.get('path'))
        finally:
            try:
                if 'new_cursor' in locals():
                    self.dropbox_client.save_cursor(new_cursor)
                    self.logger.debug('[process_event_data] - Cursor successfully updated in the finally block.')
            except Exception as e:
                self.logger.error(f'[process_event_data] - Failed to save cursor in the finally block: {e}', exc_info=True)
dropbox_webhook_handler = DropboxWebhookHandler()

@dropbox_blueprint.route('/', methods=['GET', 'POST'])
def dropbox_webhook():
    if request.method == 'GET':
        challenge = request.args.get('challenge')
        if challenge:
            return (challenge, 200)
        return (jsonify({'message': 'No challenge provided.'}), 400)
    elif request.method == 'POST':
        event = request.get_json()
        return dropbox_webhook_handler.handle_dropbox_event(event)