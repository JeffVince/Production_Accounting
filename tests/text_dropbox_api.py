# tests/test_dropbox_api.py

import unittest
from unittest.mock import MagicMock, patch
from integrations.dropbox_api import DropboxAPI

class TestDropboxAPI(unittest.TestCase):
    def setUp(self):
        self.dropbox_api = DropboxAPI()
        self.dropbox_api.dbx = MagicMock()

    def test_upload_file(self):
        with patch('builtins.open', unittest.mock.mock_open(read_data=b'data')):
            self.dropbox_api.dbx.files_upload.return_value = None
            result = self.dropbox_api.upload_file('local/file.txt', '/remote/file.txt')
            self.assertTrue(result)
            self.dropbox_api.dbx.files_upload.assert_called()

    def test_download_file(self):
        self.dropbox_api.dbx.files_download.return_value = (None, MagicMock(content=b'data'))
        with patch('builtins.open', unittest.mock.mock_open()) as mock_file:
            result = self.dropbox_api.download_file('/remote/file.txt', 'local/file.txt')
            self.assertTrue(result)
            mock_file.assert_called_with('local/file.txt', 'wb')

    def test_get_file_metadata(self):
        self.dropbox_api.dbx.files_get_metadata.return_value = MagicMock()
        result = self.dropbox_api.get_file_metadata('/remote/file.txt')
        self.assertIsNotNone(result)
        self.dropbox_api.dbx.files_get_metadata.assert_called_with('/remote/file.txt')

    def test_list_folder_contents(self):
        mock_entries = [MagicMock()]
        self.dropbox_api.dbx.files_list_folder.return_value = MagicMock(entries=mock_entries, has_more=False)
        result = self.dropbox_api.list_folder_contents('/remote/folder')
        self.assertEqual(result, mock_entries)
        self.dropbox_api.dbx.files_list_folder.assert_called_with('/remote/folder')

    def test_create_folder(self):
        self.dropbox_api.dbx.files_create_folder_v2.return_value = None
        result = self.dropbox_api.create_folder('/remote/new_folder')
        self.assertTrue(result)
        self.dropbox_api.dbx.files_create_folder_v2.assert_called_with('/remote/new_folder')

    def test_delete_file_or_folder(self):
        self.dropbox_api.dbx.files_delete_v2.return_value = None
        result = self.dropbox_api.delete_file_or_folder('/remote/file_or_folder')
        self.assertTrue(result)
        self.dropbox_api.dbx.files_delete_v2.assert_called_with('/remote/file_or_folder')

if __name__ == '__main__':
    unittest.main()