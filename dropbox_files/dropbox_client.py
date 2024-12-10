import os
import json
import time
import logging
import requests
import dropbox_files
from dropbox import DropboxTeam, common, files
import tempfile
import threading
from dotenv import load_dotenv

from utilities.singleton import SingletonMeta

load_dotenv("../.env")


class DropboxClient(metaclass=SingletonMeta):

    def __init__(self):
        if not hasattr(self, '_initialized'):
            # Set up logging
            self.logger = logging.getLogger("app_logger")
            self.OAUTH_TOKEN_URL = "https://api.dropboxapi.com/oauth2/token"
            self.DROPBOX_REFRESH_TOKEN = os.getenv("DROPBOX_REFRESH_TOKEN")
            self.DROPBOX_APP_KEY = os.getenv("DROPBOX_APP_KEY")
            self.DROPBOX_APP_SECRET = os.getenv("DROPBOX_APP_SECRET")
            self.MY_EMAIL = os.getenv("MY_EMAIL")
            self.NAMESPACE_NAME = os.getenv("NAMESPACE_NAME")
            self.CURSOR_DIR = '../cursors'
            os.makedirs(self.CURSOR_DIR, exist_ok=True)
            self._internal_lock = threading.Lock()
            # Initialize access token
            self.access_token = self.get_access_token()
            self._initialized = True
            self.logger.info("Dropbox Client Initialized")

        try:
            # Create DropboxTeam client
            self.dbx_team = DropboxTeam(
                oauth2_access_token=self.access_token,
                oauth2_refresh_token=self.DROPBOX_REFRESH_TOKEN,
                app_key=self.DROPBOX_APP_KEY,
                app_secret=self.DROPBOX_APP_SECRET
            )

            # Retrieve your member ID
            members = self.dbx_team.team_members_list().members
            self.member_id = None
            for member in members:
                if member.profile.email == self.MY_EMAIL:
                    self.member_id = member.profile.team_member_id
                    break

            if not self.member_id:
                logging.error(f"Member with email '{self.MY_EMAIL}' not found in the team.")
                raise Exception("Member not found.")

            # Impersonate your user account
            self.dbx = self.dbx_team.as_user(self.member_id)
            logging.info(f"Impersonated user '{self.MY_EMAIL}' with member ID '{self.member_id}'.")

            # Retrieve all team namespaces
            namespaces = self.dbx_team.team_namespaces_list().namespaces
            self.namespace_id = None
            for ns in namespaces:
                if ns.name == self.NAMESPACE_NAME:
                    self.namespace_id = ns.namespace_id
                    break

            if self.namespace_id:
                logging.info(
                    f"Found namespace '{self.NAMESPACE_NAME}' with ID '{self.namespace_id}'. Setting path root.")
                path_root = common.PathRoot.namespace_id(self.namespace_id)
                self.dbx = self.dbx.with_path_root(path_root)
                logging.debug(f"Path root set to namespace ID '{self.namespace_id}'.")
            else:
                logging.warning(f"Namespace '{self.NAMESPACE_NAME}' not found. Using default path root.")

            # List the root folder to verify connection
            # self.list_root_folder()

            # Start the token refresher thread
            self.start_token_refresher()

        except Exception as e:
            logging.error(f"An error occurred while creating Dropbox client: {e}", exc_info=True)
            raise e

    def get_new_access_token(self):
        """
        Use the refresh token to get a new access token.
        """
        data = {
            'grant_type': 'refresh_token',
            'refresh_token': self.DROPBOX_REFRESH_TOKEN,
            'client_id': self.DROPBOX_APP_KEY,
            'client_secret': self.DROPBOX_APP_SECRET
        }

        response = requests.post(self.OAUTH_TOKEN_URL, data=data)

        if response.status_code == 200:
            token_data = response.json()
            new_access_token = token_data['access_token']
            expires_in = token_data['expires_in']  # Token lifetime in seconds

            # Save the new access token securely
            self.save_access_token(new_access_token, expires_in)

            logging.info("Access token refreshed successfully.")
            return new_access_token
        else:
            logging.error(f"Failed to refresh token: {response.text}")
            raise Exception(f"Failed to refresh token: {response.text}")

    def save_access_token(self, access_token, expires_in):
        """
        Store the access token and its expiration time.
        """
        self.token_expiry_time = time.time() + expires_in  # Token expiration time
        token_data = {
            "access_token": access_token,
            "expires_at": self.token_expiry_time
        }

        # Save token data to a file securely
        with open('../token.json', 'w') as token_file:
            json.dump(token_data, token_file)

    def load_access_token(self):
        """
        Load the access token from storage.
        """
        try:
            with open('../token.json', 'r') as token_file:
                token_data = json.load(token_file)
                if time.time() < token_data['expires_at']:
                    self.token_expiry_time = token_data['expires_at']
                    return token_data['access_token']
                else:
                    logging.info("Access token expired.")
                    return None  # Token expired
        except FileNotFoundError:
            logging.info("Access token file not found.")
            return None

    def get_access_token(self):
        """
        Get the valid access token, refreshing it if necessary.
        """
        access_token = self.load_access_token()
        if not access_token:
            # Token expired or not found, refresh it
            logging.info("Refreshing access token.")
            access_token = self.get_new_access_token()
        return access_token

    def list_root_folder(self):
        """
        Lists the contents of the root folder to verify connection.
        """
        try:
            result = self.dbx.files_list_folder('', recursive=False)
            logging.info("Root folder contents:")
            for entry in result.entries:
                logging.info(f" - {entry.name}")
        except Exception as e:
            logging.error(f"Error listing root folder: {e}", exc_info=True)

    def list_folder_changes(self, cursor):
        """
        Lists changes in the specified folder using the provided cursor.
        Returns a tuple of (changes, new_cursor).
        """
        with self._internal_lock:
            changes = []
            try:
                self.logger.debug("Fetching changes using the cursor.")
                result = self.dbx.files_list_folder_continue(cursor)
                changes.extend(result.entries)
                while result.has_more:
                    result = self.dbx.files_list_folder_continue(result.cursor)
                    changes.extend(result.entries)
                self.logger.debug(f"Fetched {len(changes)} changes.")
                return changes, result.cursor
            except dropbox_files.exceptions.ApiError as e:
                logging.error(f"Error listing folder changes: {e}", exc_info=True)
                return [], cursor

    def load_cursor(self):
        """
        Load the cursor for the member.
        """
        cursor_file = self.get_cursor_file_path()
        if not os.path.exists(cursor_file):
            logging.info(f"No cursor file found for member ID {self.member_id}. Initializing cursor.")
            return None
        try:
            with open(cursor_file, 'r') as f:
                data = json.load(f)
                cursor = data.get('cursor')
                logging.debug(f"Loaded cursor for member {self.member_id}: {cursor}")
                return cursor
        except Exception as e:
            logging.error(f"Error loading cursor for member {self.member_id}: {e}", exc_info=True)
            return None

    def save_cursor(self, cursor):
        """
        Save the cursor for the member.
        """
        cursor_file = self.get_cursor_file_path()
        temp_fd, temp_path = tempfile.mkstemp()
        try:
            with os.fdopen(temp_fd, 'w') as tmp_file:
                json.dump({'cursor': cursor}, tmp_file)
            os.replace(temp_path, cursor_file)
            logging.debug(f"Saved cursor for member {self.member_id}: {cursor}")
        except Exception as e:
            logging.error(f"Error saving cursor for member {self.member_id}: {e}", exc_info=True)
            if os.path.exists(temp_path):
                os.remove(temp_path)

    def get_cursor_file_path(self):
        """
        Get the file path for storing the cursor.
        """
        safe_member_id = self.member_id.replace(":", "_")
        return os.path.join(self.CURSOR_DIR, f'cursor_{safe_member_id}.json')

    def start_token_refresher(self):
        """
        Starts a background thread to refresh the access token before it expires.
        """
        refresher_thread = threading.Thread(target=self.token_refresher, daemon=True)
        refresher_thread.start()
        logging.info("Token refresher thread started.")

    def token_refresher(self):
        """
        Refreshes the access token periodically before it expires.
        """
        while True:
            time_to_refresh = self.token_expiry_time - time.time() - 60  # Refresh 1 minute before expiry
            if time_to_refresh > 0:
                time.sleep(time_to_refresh)
            else:
                try:
                    logging.info("Refreshing access token.")
                    self.access_token = self.get_new_access_token()
                    # Update the DropboxTeam client with the new access token
                    self.dbx_team = DropboxTeam(
                        oauth2_access_token=self.access_token,
                        oauth2_refresh_token=self.DROPBOX_REFRESH_TOKEN,
                        app_key=self.DROPBOX_APP_KEY,
                        app_secret=self.DROPBOX_APP_SECRET
                    )
                    self.dbx = self.dbx_team.as_user(self.member_id)
                    if self.namespace_id:
                        path_root = common.PathRoot.namespace_id(self.namespace_id)
                        self.dbx = self.dbx.with_path_root(path_root)
                    logging.info("Access token refreshed and Dropbox client updated.")
                except Exception as e:
                    logging.error(f"Failed to refresh access token: {e}", exc_info=True)
                    # Implement retry logic or alerting as needed
                    time.sleep(60)  # Wait before retrying

    def create_share_link(dbx_client, dropbox_path):
        """
        Creates a shared link for the specified Dropbox path.
        """
        try:
            # Add namespace ID if applicable and ensure path format
            if dbx_client.namespace_id:
                dropbox_path = f"ns:{dbx_client.namespace_id}/{dropbox_path.lstrip('/')}"

            # Use the same path root for sharing endpoints
            dbx_sharing = dbx_client.dbx.with_path_root(common.PathRoot.namespace_id(dbx_client.namespace_id))

            # Check for existing shared links
            links = dbx_sharing.sharing_list_shared_links(path=dropbox_path, direct_only=True).links
            if links:
                logging.info(f"Existing shared link found for '{dropbox_path}': {links[0].url}")
                return links[0].url
            else:
                # Create a new shared link
                settings = dropbox_files.sharing.SharedLinkSettings(
                    requested_visibility=dropbox_files.sharing.RequestedVisibility.public
                )
                shared_link = dbx_sharing.sharing_create_shared_link_with_settings(dropbox_path, settings)
                logging.info(f"Created new shared link for '{dropbox_path}': {shared_link.url}")
                return shared_link.url
        except dropbox_files.exceptions.ApiError as e:
            logging.error(f"Error creating shared link for '{dropbox_path}': {e}")
            return None


dropbox_client = DropboxClient()


