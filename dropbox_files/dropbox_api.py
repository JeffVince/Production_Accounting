# integrations/dropbox_api.py

from dropbox_files.dropbox_client import dropbox_client
from dropbox.exceptions import ApiError

from singleton import SingletonMeta
from utilities.config import Config

class DropboxAPI(metaclass=SingletonMeta):
    def __init__(self):
        dbx_client = dropbox_client
        dbx = dbx_client.dbx

    def upload_file(self, file_path: str, destination_path: str):
        """Uploads a file to Dropbox."""
        with open(file_path, 'rb') as f:
            try:
                self.dbx.files_upload(f.read(), destination_path)
                return True
            except ApiError as e:
                print(f"Error uploading file: {e}")
                return False

    def download_file(self, file_path: str, local_destination: str):
        """Downloads a file from Dropbox."""
        try:
            metadata, res = self.dbx.files_download(file_path)
            with open(local_destination, 'wb') as f:
                f.write(res.content)
            return True
        except ApiError as e:
            print(f"Error downloading file: {e}")
            return False

    def get_file_metadata(self, file_path: str):
        """Retrieves metadata for a file."""
        try:
            metadata = self.dbx.files_get_metadata(file_path)
            return metadata
        except ApiError as e:
            print(f"Error getting file metadata: {e}")
            return None

    def list_folder_contents(self, folder_path: str):
        """Lists the contents of a folder."""
        try:
            result = self.dbx.files_list_folder(folder_path)
            entries = result.entries
            while result.has_more:
                result = self.dbx.files_list_folder_continue(result.cursor)
                entries.extend(result.entries)
            return entries
        except ApiError as e:
            print(f"Error listing folder contents: {e}")
            return []

    def create_folder(self, folder_path: str):
        """Creates a folder in Dropbox."""
        try:
            self.dbx.files_create_folder_v2(folder_path)
            return True
        except ApiError as e:
            print(f"Error creating folder: {e}")
            return False

    def delete_file_or_folder(self, path: str):
        """Deletes a file or folder in Dropbox."""
        try:
            self.dbx.files_delete_v2(path)
            return True
        except ApiError as e:
            print(f"Error deleting file or folder: {e}")
            return False

dropbox_api = DropboxAPI()