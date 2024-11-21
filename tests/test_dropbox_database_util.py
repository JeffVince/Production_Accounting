# test_dropbox_database_util.py

import unittest
from unittest.mock import patch
import tempfile
import os
import sqlite3
import logging
from dropbox_database_util import (
    initialize_database,
    add_event_to_db,
    fetch_pending_events,
    update_event_status,
    add_po_log,
    fetch_unprocessed_po_logs,
    update_po_log_status,
)


class TestDropboxDatabaseUtil(unittest.TestCase):

    def setUp(self):
        """
        Set up a temporary file-based SQLite database for testing by patching `get_db_path`
        and setting the `TARGET_PURCHASE_ORDERS_FOLDER` environment variable.
        """
        # Configure logging to display debug messages during tests
        logging.basicConfig(level=logging.DEBUG)
        self.logger = logging.getLogger()

        # Create a temporary file for the database
        self.temp_db = tempfile.NamedTemporaryFile(delete=False)
        self.temp_db_path = self.temp_db.name
        self.temp_db.close()  # Close the file so SQLite can open it

        # Patch the `get_db_path` function in `dropbox_database_util` to return the temp file path
        patcher_db_path = patch('dropbox_database_util.get_db_path', return_value=self.temp_db_path)
        self.addCleanup(patcher_db_path.stop)  # Ensure the patcher stops after tests
        self.mock_get_db_path = patcher_db_path.start()

        # Patch the environment variable `TARGET_PURCHASE_ORDERS_FOLDER`
        patcher_env = patch.dict(os.environ, {'TARGET_PURCHASE_ORDERS_FOLDER': '1. Purchase Orders'})
        self.addCleanup(patcher_env.stop)  # Ensure the patcher stops after tests
        self.mock_env = patcher_env.start()

        # Initialize the database schema within the patched context
        initialize_database()

    def tearDown(self):
        """
        Clean up after tests by removing the temporary database file.
        """
        try:
            os.unlink(self.temp_db_path)
        except Exception as e:
            self.logger.error(f"Error deleting temporary database file: {e}")

    def test_add_event_to_db(self):
        """
        Test adding an event to the events table.
        """
        event_data = {
            'file_id': '12345',
            'file_name': 'test_file.txt',
            'path': '1. Purchase Orders/test_file.txt',
            'old_path': '',  # Added 'old_path' field
            'event_type': 'upload',
            'project_id': 'proj1',
            'po_number': 'PO123',
            'vendor_name': 'Vendor A',
            'vendor_type': 'Type A',
            'file_type': 'txt',
            'file_number': 'FN123',
            'dropbox_share_link': 'http://sharelink',
            'file_stream_link': 'http://streamlink',
        }

        event_id, is_duplicate = add_event_to_db(**event_data)
        self.assertIsNotNone(event_id, "Event ID should not be None for a new event")
        self.assertFalse(is_duplicate, "Event should not be marked as duplicate")

    def test_fetch_pending_events(self):
        """
        Test fetching events with status 'pending'.
        """
        event_data = {
            'file_id': '12345',
            'file_name': 'test_file.txt',
            'path': '1. Purchase Orders/test_file.txt',
            'old_path': '',  # Added 'old_path' field
            'event_type': 'upload',
            'project_id': 'proj1',
            'po_number': 'PO123',
            'vendor_name': 'Vendor A',
            'vendor_type': 'Type A',
            'file_type': 'txt',
            'file_number': 'FN123',
            'dropbox_share_link': 'http://sharelink',
            'file_stream_link': 'http://streamlink',
        }
        add_event_to_db(**event_data)

        pending_events = fetch_pending_events()
        self.assertEqual(len(pending_events), 1, "There should be exactly one pending event")
        self.assertEqual(pending_events[0]['file_name'], 'test_file.txt', "File name should match")

    def test_update_event_status(self):
        """
        Test updating the status of an event.
        """
        event_data = {
            'file_id': '12345',
            'file_name': 'test_file.txt',
            'path': '1. Purchase Orders/test_file.txt',
            'old_path': '',  # Added 'old_path' field
            'event_type': 'upload',
            'project_id': 'proj1',
            'po_number': 'PO123',
            'vendor_name': 'Vendor A',
            'vendor_type': 'Type A',
            'file_type': 'txt',
            'file_number': 'FN123',
            'dropbox_share_link': 'http://sharelink',
            'file_stream_link': 'http://streamlink',
        }
        event_id, _ = add_event_to_db(**event_data)

        update_event_status(event_id, 'processed')

        pending_events = fetch_pending_events()
        self.assertEqual(len(pending_events), 0, "There should be no pending events after updating status")

    def test_add_and_fetch_po_logs(self):
        """
        Test adding and fetching unprocessed PO logs.
        """
        po_log_data = {
            'file_name': 'log1.csv',
            'project_id': 'proj1',
            'dropbox_file_path': '/path/to/log1.csv',
            'file_format': 'csv',
        }
        add_po_log(**po_log_data)

        unprocessed_logs = fetch_unprocessed_po_logs()
        self.assertEqual(len(unprocessed_logs), 1, "There should be exactly one unprocessed PO log")
        self.assertEqual(unprocessed_logs[0]['file_name'], 'log1.csv', "File name should match")

    def test_update_po_log_status(self):
        """
        Test updating the status of a PO log.
        """
        po_log_data = {
            'file_name': 'log1.csv',
            'project_id': 'proj1',
            'dropbox_file_path': '/path/to/log1.csv',
            'file_format': 'csv',
        }
        log_id = add_po_log(**po_log_data)

        update_po_log_status(log_id, 'processed')

        unprocessed_logs = fetch_unprocessed_po_logs()
        self.assertEqual(len(unprocessed_logs), 0, "There should be no unprocessed PO logs after updating status")


if __name__ == "__main__":
    unittest.main()