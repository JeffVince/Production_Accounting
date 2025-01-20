import os
import json
import time
import logging
import requests
import files_dropbox
from dropbox import DropboxTeam, common, files
import tempfile
import threading
from dotenv import load_dotenv
from utilities.singleton import SingletonMeta
load_dotenv('../.env')

class DropboxClient(metaclass=SingletonMeta):

    def __init__(self):
        if not hasattr(self, '_initialized'):
            self.logger = logging.getLogger('dropbox')
            self.OAUTH_TOKEN_URL = 'https://api.dropboxapi.com/oauth2/token'
            self.DROPBOX_REFRESH_TOKEN = os.getenv('DROPBOX_REFRESH_TOKEN')
            self.DROPBOX_APP_KEY = os.getenv('DROPBOX_APP_KEY')
            self.DROPBOX_APP_SECRET = os.getenv('DROPBOX_APP_SECRET')
            self.MY_EMAIL = os.getenv('MY_EMAIL')
            self.NAMESPACE_NAME = os.getenv('NAMESPACE_NAME')
            self.CURSOR_DIR = '../cursors'
            os.makedirs(self.CURSOR_DIR, exist_ok=True)
            self._internal_lock = threading.Lock()
            self.access_token = self.get_access_token()
            self.logger.info('[__init__] - Dropbox Client Initialized')
        try:
            self.dbx_team = DropboxTeam(oauth2_access_token=self.access_token, oauth2_refresh_token=self.DROPBOX_REFRESH_TOKEN, app_key=self.DROPBOX_APP_KEY, app_secret=self.DROPBOX_APP_SECRET)
            members = self.dbx_team.team_members_list().members
            self.member_id = None
            for member in members:
                if member.profile.email == self.MY_EMAIL:
                    self.member_id = member.profile.team_member_id
                    break
            if not self.member_id:
                self.logger.error(f"[__init__] - Member with email '{self.MY_EMAIL}' not found in the team.")
                raise Exception('Member not found.')
            self.dbx = self.dbx_team.as_user(self.member_id)
            self.logger.info(f"[__init__] - Impersonated user '{self.MY_EMAIL}' with member ID '{self.member_id}'.")
            namespaces = self.dbx_team.team_namespaces_list().namespaces
            self.namespace_id = None
            for ns in namespaces:
                if ns.name == self.NAMESPACE_NAME:
                    self.namespace_id = ns.namespace_id
                    break
            if self.namespace_id:
                self.logger.info(f"[__init__] - Found namespace '{self.NAMESPACE_NAME}' with ID '{self.namespace_id}'. Setting path root.")
                path_root = common.PathRoot.namespace_id(self.namespace_id)
                self.dbx = self.dbx.with_path_root(path_root)
                self.logger.debug(f"[__init__] - Path root set to namespace ID '{self.namespace_id}'.")
            else:
                self.logger.warning(f"[__init__] - Namespace '{self.NAMESPACE_NAME}' not found. Using default path root.")
            self.start_token_refresher()
        except Exception as e:
            self.logger.error(f'[__init__] - An error occurred while creating Dropbox client: {e}', exc_info=True)
            raise e
        self._initialized = True

    def get_new_access_token(self):
        """
        Use the refresh token to get a new access token.
        """
        data = {'grant_type': 'refresh_token', 'refresh_token': self.DROPBOX_REFRESH_TOKEN, 'client_id': self.DROPBOX_APP_KEY, 'client_secret': self.DROPBOX_APP_SECRET}
        response = requests.post(self.OAUTH_TOKEN_URL, data=data)
        if response.status_code == 200:
            token_data = response.json()
            new_access_token = token_data['access_token']
            expires_in = token_data['expires_in']
            self.save_access_token(new_access_token, expires_in)
            self.logger.info('[get_new_access_token] - Access token refreshed successfully.')
            return new_access_token
        else:
            self.logger.error(f'[get_new_access_token] - Failed to refresh token: {response.text}')
            raise Exception(f'Failed to refresh token: {response.text}')

    def save_access_token(self, access_token, expires_in):
        """
        Store the access token and its expiration time.
        """
        self.token_expiry_time = time.time() + expires_in
        token_data = {'access_token': access_token, 'expires_at': self.token_expiry_time}
        with open('../token.json', 'w') as token_file:
            json.dump(token_data, token_file)

    def load_access_token(self):
        """
        Load the access token from storage.
        """
        try:
            with open('../token.json', 'r') as token_file:
                raw_data = token_file.read()
                if raw_data.endswith('}}'):
                    raw_data = raw_data[:-1]
                try:
                    token_data = json.loads(raw_data)
                    if time.time() < token_data['expires_at']:
                        self.token_expiry_time = token_data['expires_at']
                        return token_data['access_token']
                    else:
                        self.logger.info('[load_access_token] - Access token expired.')
                        return None
                except json.JSONDecodeError as e:
                    self.logger.error(f'[load_access_token] - JSON decode error: {e}')
                    return None
        except FileNotFoundError:
            self.logger.info('[load_access_token] - Access token file not found.')
            return None

    def get_access_token(self):
        """
        Get the valid access token, refreshing it if necessary.
        """
        access_token = self.load_access_token()
        if not access_token:
            self.logger.info('[get_access_token] - Refreshing access token.')
            access_token = self.get_new_access_token()
        return access_token

    def list_root_folder(self):
        """
        Lists the contents of the root folder to verify connection.
        """
        try:
            result = self.dbx.files_list_folder('', recursive=False)
            self.logger.info('[list_root_folder] - Root folder contents:')
            for entry in result.entries:
                self.logger.info(f'[list_root_folder] -  - {entry.name}')
        except Exception as e:
            self.logger.error(f'[list_root_folder] - Error listing root folder: {e}', exc_info=True)

    def list_folder_changes(self, cursor):
        """
        Lists changes in the specified folder using the provided cursor.
        Returns a tuple of (changes, new_cursor).
        """
        with self._internal_lock:
            changes = []
            try:
                self.logger.debug('[list_folder_changes] - Fetching changes using the cursor.')
                result = self.dbx.files_list_folder_continue(cursor)
                changes.extend(result.entries)
                while result.has_more:
                    result = self.dbx.files_list_folder_continue(result.cursor)
                    changes.extend(result.entries)
                self.logger.debug(f'[list_folder_changes] - Fetched {len(changes)} changes.')
                return (changes, result.cursor)
            except files_dropbox.exceptions.ApiError as e:
                self.logger.error(f'[list_folder_changes] - Error listing folder changes: {e}', exc_info=True)
                return ([], cursor)

    def load_cursor(self):
        """
        Load the cursor for the member.
        """
        cursor_file = self.get_cursor_file_path()
        if not os.path.exists(cursor_file):
            self.logger.info(f'[load_cursor] - No cursor file found for member ID {self.member_id}. Initializing cursor.')
            return None
        try:
            with open(cursor_file, 'r') as f:
                data = json.load(f)
                cursor = data.get('cursor')
                self.logger.debug(f'[load_cursor] - Loaded cursor for member {self.member_id}: {cursor}')
                return cursor
        except Exception as e:
            self.logger.error(f'[load_cursor] - Error loading cursor for member {self.member_id}: {e}', exc_info=True)
            return None

    def save_cursor(self, cursor):
        """
        Save the cursor for the member.
        """
        cursor_file = self.get_cursor_file_path()
        (temp_fd, temp_path) = tempfile.mkstemp()
        try:
            with os.fdopen(temp_fd, 'w') as tmp_file:
                json.dump({'cursor': cursor}, tmp_file)
            os.replace(temp_path, cursor_file)
            self.logger.debug(f'[save_cursor] - Saved cursor for member {self.member_id}: {cursor}')
        except Exception as e:
            self.logger.error(f'[save_cursor] - Error saving cursor for member {self.member_id}: {e}', exc_info=True)
            if os.path.exists(temp_path):
                os.remove(temp_path)

    def get_cursor_file_path(self):
        """
        Get the file path for storing the cursor.
        """
        safe_member_id = self.member_id.replace(':', '_')
        return os.path.join(self.CURSOR_DIR, f'cursor_{safe_member_id}.json')

    def start_token_refresher(self):
        """
        Starts a background thread to refresh the access token before it expires.
        """
        refresher_thread = threading.Thread(target=self.token_refresher, daemon=True)
        refresher_thread.start()
        self.logger.info('[start_token_refresher] - Token refresher thread started.')

    def token_refresher(self):
        """
        Refreshes the access token periodically before it expires.
        """
        while True:
            time_to_refresh = self.token_expiry_time - time.time() - 60
            if time_to_refresh > 0:
                time.sleep(time_to_refresh)
            else:
                try:
                    self.logger.info('[token_refresher] - Refreshing access token.')
                    self.access_token = self.get_new_access_token()
                    self.dbx_team = DropboxTeam(oauth2_access_token=self.access_token, oauth2_refresh_token=self.DROPBOX_REFRESH_TOKEN, app_key=self.DROPBOX_APP_KEY, app_secret=self.DROPBOX_APP_SECRET)
                    self.dbx = self.dbx_team.as_user(self.member_id)
                    if self.namespace_id:
                        path_root = common.PathRoot.namespace_id(self.namespace_id)
                        self.dbx = self.dbx.with_path_root(path_root)
                    self.logger.info('[token_refresher] - Access token refreshed and Dropbox client updated.')
                except Exception as e:
                    self.logger.error(f'[token_refresher] - Failed to refresh access token: {e}', exc_info=True)
                    time.sleep(60)

dropbox_client = DropboxClient()