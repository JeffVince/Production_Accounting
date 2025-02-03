# test_dropbox_api.py
import os
import tempfile
import pytest
from unittest.mock import MagicMock
from dropbox.exceptions import ApiError
from dropbox.files import FolderMetadata
from dropbox_api import DropboxAPI

class TestDropboxAPI:
    @pytest.fixture(autouse=True)
    def setup_api(self):
        # Instantiate DropboxAPI and replace its internal client with a MagicMock.
        self.db_api = DropboxAPI()
        self.mock_dbx = MagicMock()
        self.db_api.dbx = self.mock_dbx

    def test_upload_file_success(self):
        with tempfile.NamedTemporaryFile(delete=False) as tmp:
            tmp.write(b"sample content")
            tmp_filename = tmp.name

        self.mock_dbx.files_upload.return_value = None
        result = self.db_api.upload_file(tmp_filename, "/destination/path")
        assert result is True
        self.mock_dbx.files_upload.assert_called_once()
        os.unlink(tmp_filename)

    def test_upload_file_failure(self):
        with tempfile.NamedTemporaryFile(delete=False) as tmp:
            tmp.write(b"sample content")
            tmp_filename = tmp.name

        # Pass all required arguments to ApiError.
        self.mock_dbx.files_upload.side_effect = ApiError(
            "fake request", "fake error", "fake user message", "en"
        )
        result = self.db_api.upload_file(tmp_filename, "/destination/path")
        assert result is False
        os.unlink(tmp_filename)

    def test_download_file_success(self):
        fake_response = MagicMock()
        fake_response.content = b"downloaded data"
        self.mock_dbx.files_download.return_value = (MagicMock(), fake_response)

        with tempfile.NamedTemporaryFile(delete=False) as tmp:
            local_dest = tmp.name

        result = self.db_api.download_file("/remote/path", local_dest)
        assert result is True

        with open(local_dest, "rb") as f:
            content = f.read()
        assert content == b"downloaded data"
        os.unlink(local_dest)

    def test_download_file_failure(self):
        self.mock_dbx.files_download.side_effect = ApiError(
            "fake request", "fake error", "fake user message", "en"
        )
        with tempfile.NamedTemporaryFile(delete=False) as tmp:
            local_dest = tmp.name

        result = self.db_api.download_file("/remote/path", local_dest)
        assert result is False
        os.unlink(local_dest)

    def test_get_file_metadata_success(self):
        fake_metadata = {"name": "file.txt", "id": "123"}
        self.mock_dbx.files_get_metadata.return_value = fake_metadata
        result = self.db_api.get_file_metadata("/remote/file.txt")
        assert result == fake_metadata

    def test_get_file_metadata_failure(self):
        self.mock_dbx.files_get_metadata.side_effect = ApiError(
            "req", "err", "user msg", "en"
        )
        result = self.db_api.get_file_metadata("/remote/file.txt")
        assert result is None

    def test_list_folder_contents_success(self):
        # Create fake file entries with the required 'path_display' attribute.
        fake_file1 = MagicMock()
        fake_file1.path_display = "/folder/file1.txt"
        fake_file2 = MagicMock()
        fake_file2.path_display = "/folder/file2.doc"

        fake_result = MagicMock()
        fake_result.entries = [fake_file1]
        fake_result.has_more = True
        fake_result.cursor = "cursor1"
        fake_result_continue = MagicMock()
        fake_result_continue.entries = [fake_file2]
        fake_result_continue.has_more = False

        self.mock_dbx.files_list_folder.return_value = fake_result
        self.mock_dbx.files_list_folder_continue.return_value = fake_result_continue

        items = self.db_api.list_folder_contents("/folder")
        # Expect both file basenames.
        assert "file1.txt" in items
        assert "file2.doc" in items

    def test_create_folder_success(self):
        self.mock_dbx.files_create_folder_v2.return_value = None
        result = self.db_api.create_folder("/new/folder")
        assert result is True

    def test_create_folder_failure(self):
        self.mock_dbx.files_create_folder_v2.side_effect = ApiError(
            "req", "err", "user msg", "en"
        )
        result = self.db_api.create_folder("/new/folder")
        assert result is False

    def test_delete_file_or_folder_success(self):
        self.mock_dbx.files_delete_v2.return_value = None
        result = self.db_api.delete_file_or_folder("/delete/path")
        assert result is True

    def test_delete_file_or_folder_failure(self):
        self.mock_dbx.files_delete_v2.side_effect = ApiError(
            "req", "err", "user msg", "en"
        )
        result = self.db_api.delete_file_or_folder("/delete/path")
        assert result is False

    def test_create_share_link_existing(self):
        fake_shared_link = MagicMock()
        fake_shared_link.url = "http://existing.link"
        fake_sharing = MagicMock()
        fake_sharing.sharing_list_shared_links.return_value = MagicMock(links=[fake_shared_link])
        self.mock_dbx.with_path_root.return_value = fake_sharing
        result = self.db_api.create_share_link("/folder")
        assert result == "http://existing.link"

    def test_create_share_link_new(self):
        fake_shared_link = MagicMock()
        fake_shared_link.url = "http://new.link"
        fake_sharing = MagicMock()
        fake_sharing.sharing_list_shared_links.return_value = MagicMock(links=[])
        fake_sharing.sharing_create_shared_link_with_settings.return_value = fake_shared_link
        self.mock_dbx.with_path_root.return_value = fake_sharing
        result = self.db_api.create_share_link("/folder")
        assert result == "http://new.link"

    def test_list_project_po_folders_success(self):
        # Create a fake folder that simulates FolderMetadata.
        fake_folder = MagicMock()
        fake_folder.name = "PO Folder 1"
        fake_folder.path_lower = "/proj/PO Folder 1"
        # Ensure that isinstance(fake_folder, FolderMetadata) returns True.
        fake_folder.__class__ = FolderMetadata

        fake_result = MagicMock()
        fake_result.entries = [fake_folder]
        fake_result.has_more = False
        fake_sharing = MagicMock()
        fake_sharing.files_list_folder.return_value = fake_result
        self.mock_dbx.with_path_root.return_value = fake_sharing

        folders = self.db_api.list_project_po_folders("/proj/1. Purchase Orders")
        assert len(folders) == 1
        assert folders[0]["name"] == "PO Folder 1"