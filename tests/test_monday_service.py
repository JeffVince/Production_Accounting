# tests/test_monday_service.py

import unittest
from unittest.mock import patch, MagicMock
from files_monday.monday_service import MondayService
import requests
from utilities.config import Config


class TestMondayService(unittest.TestCase):
    def setUp(self):
        """
        Initialize the MondayService instance before each test.
        """
        self.service = MondayService()

    def test_init(self):
        """
        Test the initialization of the MondayService class.
        """
        service = MondayService()
        self.assertIsNotNone(service.monday_util, "MondayUtil instance should be initialized.")
        self.assertIsNotNone(service.db_util, "MondayDatabaseUtil instance should be initialized.")
        self.assertIsNotNone(service.monday_api, "MondayAPI instance should be initialized.")
        self.assertEqual(service.api_token, Config.MONDAY_API_TOKEN, "API token should match the config.")
        self.assertEqual(service.board_id, service.monday_util.PO_BOARD_ID, "Board ID should match MondayUtil configuration.")
        self.assertEqual(service.subitem_board_id, service.monday_util.SUBITEM_BOARD_ID, "Subitem Board ID should match MondayUtil configuration.")
        self.assertEqual(service.contact_board_id, service.monday_util.CONTACT_BOARD_ID, "Contact Board ID should match MondayUtil configuration.")
        self.assertEqual(service.api_url, service.monday_util.MONDAY_API_URL, "API URL should match MondayUtil configuration.")

    @patch('files_monday.monday_service.requests.post')
    def test__make_request_success(self, mock_post):
        """
        Test the _make_request method for a successful API call.
        """
        # Arrange
        mock_response = MagicMock()
        mock_response.raise_for_status.return_value = None
        mock_response.json.return_value = {'data': {'result': 'success'}}
        mock_post.return_value = mock_response

        query = 'query { boards { id name } }'
        variables = {}

        # Act
        response = self.service._make_request(query, variables)

        # Assert
        mock_post.assert_called_once_with(
            self.service.api_url,
            json={'query': query, 'variables': variables},
            headers={"Authorization": self.service.api_token}
        )
        self.assertEqual(response, {'data': {'result': 'success'}}, "Response should match the mocked data.")

    @patch('files_monday.monday_service.requests.post')
    def test__make_request_http_error(self, mock_post):
        """
        Test the _make_request method when an HTTPError occurs.
        """
        # Arrange
        mock_response = MagicMock()
        mock_response.raise_for_status.side_effect = requests.HTTPError('HTTP Error')
        mock_post.return_value = mock_response

        query = 'query { boards { id name } }'
        variables = {}

        # Act & Assert
        with self.assertRaises(requests.HTTPError, msg="Should raise HTTPError when response contains an error."):
            self.service._make_request(query, variables)
        mock_post.assert_called_once_with(
            self.service.api_url,
            json={'query': query, 'variables': variables},
            headers={"Authorization": self.service.api_token}
        )

    @patch('files_monday.monday_service.MondayUtil')
    @patch('files_monday.monday_service.MondayDatabaseUtil')
    def test_update_po_status(self, mock_db_util, mock_monday_util):
        """
        Test the update_po_status method for a successful status update.
        """
        # Arrange
        mock_make_request = MagicMock(return_value={'data': {'change_column_value': {'id': '123'}}})
        self.service._make_request = mock_make_request

        pulse_id = 123
        status = 'Approved'

        # Act
        self.service.update_po_status(pulse_id=pulse_id, status=status)

        # Assert
        mock_make_request.assert_called_once()
        expected_query = '''
            mutation ($board_id: Int!, $item_id: Int!, $column_id: String!, $value: JSON!) {
                change_column_value(
                    board_id: $board_id,
                    item_id: $item_id,
                    column_id: $column_id,
                    value: $value
                ) {
                    id
                }
            }
            '''
        expected_variables = {
            'board_id': int(self.service.board_id),
            'item_id': pulse_id,
            'column_id': self.service.monday_util.PO_STATUS_COLUMN_ID,
            'value': '{"label": "Approved"}'
        }
        mock_make_request.assert_called_with(expected_query, expected_variables)

    @patch('files_monday.monday_service.MondayUtil')
    @patch('files_monday.monday_service.MondayDatabaseUtil')
    def test_match_or_create_contact_existing_contact(self, mock_db_util, mock_monday_util):
        """
        Test match_or_create_contact when contact already exists.
        """
        # Arrange
        mock_monday_util_instance = mock_monday_util.return_value
        mock_monday_util_instance.find_contact_item_by_name.return_value = {'item_id': '456'}
        mock_monday_util_instance.create_item.return_value = None  # Should not be called

        mock_db_util_instance = mock_db_util.return_value

        # Act
        contact_id = self.service.match_or_create_contact('VendorName', 'PO123')

        # Assert
        self.assertEqual(contact_id, '456', "Should return existing contact ID.")
        mock_monday_util_instance.find_contact_item_by_name.assert_called_once_with('VendorName')
        mock_monday_util_instance.create_item.assert_not_called()
        mock_db_util_instance.link_contact_to_po.assert_called_once_with('PO123', '456')

    @patch('files_monday.monday_service.MondayUtil')
    @patch('files_monday.monday_service.MondayDatabaseUtil')
    def test_match_or_create_contact_create_new_contact(self, mock_db_util, mock_monday_util):
        """
        Test match_or_create_contact when contact does not exist and needs to be created.
        """
        # Arrange
        mock_monday_util_instance = mock_monday_util.return_value
        mock_monday_util_instance.find_contact_item_by_name.return_value = None
        mock_monday_util_instance.create_item.return_value = '456'

        mock_db_util_instance = mock_db_util.return_value

        # Act
        contact_id = self.service.match_or_create_contact('VendorName', 'PO123')

        # Assert
        self.assertEqual(contact_id, '456', "Should return new contact ID.")
        mock_monday_util_instance.find_contact_item_by_name.assert_called_once_with('VendorName')
        mock_monday_util_instance.create_item.assert_called_once_with(
            group_id="contacts_group_id",
            item_name='VendorName',
            column_values={
                self.service.monday_util.CONTACT_NAME: 'VendorName',
                self.service.monday_util.CONTACT_EMAIL: 'vendor@example.com',
                self.service.monday_util.CONTACT_PHONE: '123-456-7890',
                # Add other necessary fields
            }
        )
        mock_db_util_instance.link_contact_to_po.assert_called_once_with('PO123', '456')

    @patch('files_monday.monday_service.MondayUtil')
    @patch('files_monday.monday_service.MondayDatabaseUtil')
    def test_match_or_create_contact_create_failure(self, mock_db_util, mock_monday_util):
        """
        Test match_or_create_contact when creating a new contact fails.
        """
        # Arrange
        mock_monday_util_instance = mock_monday_util.return_value
        mock_monday_util_instance.find_contact_item_by_name.return_value = None
        mock_monday_util_instance.create_item.return_value = None  # Simulate failure

        mock_db_util_instance = mock_db_util.return_value

        # Act & Assert
        with self.assertRaises(Exception) as context:
            self.service.match_or_create_contact('VendorName', 'PO123')
        self.assertIn('Contact creation failed.', str(context.exception), "Should raise Exception when contact creation fails.")
        mock_monday_util_instance.find_contact_item_by_name.assert_called_once_with('VendorName')
        mock_monday_util_instance.create_item.assert_called_once()
        mock_db_util_instance.link_contact_to_po.assert_not_called()

    @patch('files_monday.monday_service.MondayUtil')
    def test_get_po_number_from_item(self, mock_monday_util):
        """
        Test get_po_number_from_item when PO number exists.
        """
        # Arrange
        mock_monday_util_instance = mock_monday_util.return_value
        mock_monday_util_instance.get_po_number_and_data.return_value = ('PO123', {'some_data': 'value'})

        # Act
        po_number = self.service.get_po_number_from_item(123)

        # Assert
        self.assertEqual(po_number, 'PO123', "Should return the retrieved PO number.")
        mock_monday_util_instance.get_po_number_and_data.assert_called_once_with(123)

    @patch('files_monday.monday_service.MondayUtil')
    def test_get_po_number_from_item_not_found(self, mock_monday_util):
        """
        Test get_po_number_from_item when PO number is not found.
        """
        # Arrange
        mock_monday_util_instance = mock_monday_util.return_value
        mock_monday_util_instance.get_po_number_and_data.return_value = (None, {})

        # Act
        po_number = self.service.get_po_number_from_item(123)

        # Assert
        self.assertIsNone(po_number, "Should return None when PO number is not found.")
        mock_monday_util_instance.get_po_number_and_data.assert_called_once_with(123)

    @patch('files_monday.monday_service.MondayUtil')
    def test_get_po_number_from_item_exception(self, mock_monday_util):
        """
        Test get_po_number_from_item when an exception occurs.
        """
        # Arrange
        mock_monday_util_instance = mock_monday_util.return_value
        mock_monday_util_instance.get_po_number_and_data.side_effect = Exception('API Error')

        # Act
        with self.assertLogs(level='ERROR') as log:
            po_number = self.service.get_po_number_from_item(123)

        # Assert
        self.assertIsNone(po_number, "Should return None when an exception occurs.")
        self.assertIn('Error retrieving PO number for item ID 123: API Error', log.output[0])
        mock_monday_util_instance.get_po_number_and_data.assert_called_once_with(123)

    @patch('files_monday.monday_service.MondayAPI')
    @patch('files_monday.monday_service.MondayDatabaseUtil')
    @patch('files_monday.monday_service.MondayUtil')
    def test_sync_main_items_from_monday_board_success(self, mock_monday_util, mock_db_util, mock_monday_api):
        """
        Test syncing main items (POs) from Monday.com to the local database successfully.
        """
        # Arrange
        mock_monday_api_instance = mock_monday_api.return_value
        mock_monday_api_instance.fetch_all_items.return_value = [
            {'id': '1', 'name': 'PO1'},
            {'id': '2', 'name': 'PO2'}
        ]

        mock_db_util_instance = mock_db_util.return_value
        mock_monday_util_instance = mock_monday_util.return_value

        mock_db_util_instance.prep_main_item_event_for_db_creation.side_effect = [
            {'pulse_id': 1, 'po_number': 'PO123', 'state': 'Open'},
            {'pulse_id': 2, 'po_number': 'PO456', 'state': 'Closed'}
        ]
        mock_db_util_instance.create_or_update_main_item_in_db.side_effect = ['Created', 'Updated']

        # Act
        with self.assertLogs(level='INFO') as log:
            self.service.sync_main_items_from_monday_board()

        # Assert
        mock_monday_api_instance.fetch_all_items.assert_called_once_with(self.service.board_id)
        self.assertEqual(mock_db_util_instance.prep_main_item_event_for_db_creation.call_count, 2, "Should prepare two main items.")
        self.assertEqual(mock_db_util_instance.create_or_update_main_item_in_db.call_count, 2, "Should create/update two main items.")
        self.assertIn('Synced PO with pulse_id 1: Created', log.output[0])
        self.assertIn('Synced PO with pulse_id 2: Updated', log.output[1])

    @patch('files_monday.monday_service.MondayAPI')
    def test_sync_main_items_from_monday_board_exception_fetching(self, mock_monday_api):
        """
        Test sync_main_items_from_monday_board when fetching items raises an exception.
        """
        # Arrange
        mock_monday_api_instance = mock_monday_api.return_value
        mock_monday_api_instance.fetch_all_items.side_effect = Exception('API Error')

        # Act & Assert
        with self.assertLogs(level='ERROR') as log:
            self.service.sync_main_items_from_monday_board()
            self.assertIn('Unexpected error during main items synchronization: API Error', log.output[0])

    @patch('files_monday.monday_service.MondayAPI')
    @patch('files_monday.monday_service.MondayDatabaseUtil')
    @patch('files_monday.monday_service.MondayUtil')
    def test_sync_main_items_from_monday_board_exception_syncing(self, mock_monday_util, mock_db_util, mock_monday_api):
        """
        Test sync_main_items_from_monday_board when syncing items raises an exception.
        """
        # Arrange
        mock_monday_api_instance = mock_monday_api.return_value
        mock_monday_api_instance.fetch_all_items.return_value = [
            {'id': '1', 'name': 'PO1'}
        ]

        mock_db_util_instance = mock_db_util.return_value
        mock_monday_util_instance = mock_monday_util.return_value

        mock_db_util_instance.prep_main_item_event_for_db_creation.side_effect = Exception('Prep Error')

        # Act & Assert
        with self.assertLogs(level='ERROR') as log:
            self.service.sync_main_items_from_monday_board()
            self.assertIn('Unexpected error during main items synchronization: Prep Error', log.output[0])

    @patch('files_monday.monday_service.MondayAPI')
    def test_sync_main_items_from_monday_board_handle_error_in_create_update(self, mock_monday_api):
        """
        Test sync_main_items_from_monday_board when create_or_update_main_item_in_db returns 'Fail'.
        """
        # Arrange
        mock_monday_api_instance = mock_monday_api.return_value
        mock_monday_api_instance.fetch_all_items.return_value = [
            {'id': '1', 'name': 'PO1'}
        ]

        mock_db_util_instance = MagicMock()
        mock_db_util_instance.prep_main_item_event_for_db_creation.return_value = {'pulse_id': 1, 'po_number': 'PO123', 'state': 'Open'}
        mock_db_util_instance.create_or_update_main_item_in_db.return_value = 'Fail'

        self.service.db_util = mock_db_util_instance

        # Act
        with self.assertLogs(level='ERROR') as log:
            self.service.sync_main_items_from_monday_board()

        # Assert
        self.assertIn('Failed to sync PO with pulse_id 1: Fail', log.output[0])

    @patch('files_monday.monday_service.MondayAPI')
    def test_sync_sub_items_from_monday_board_success(self, mock_monday_api):
        """
        Test syncing sub-items from Monday.com to the local database successfully.
        """
        # Arrange
        mock_monday_api_instance = mock_monday_api.return_value
        mock_monday_api_instance.fetch_all_sub_items.return_value = [
            {'id': '1', 'parent_item': {'id': '10'}, 'name': 'SubItem1'},
            {'id': '2', 'parent_item': {'id': '20'}, 'name': 'SubItem2'}
        ]

        mock_db_util_instance = MagicMock()
        mock_db_util_instance.prep_sub_item_event_for_db_creation.side_effect = [
            {'pulse_id': 1, 'parent_id': 100, 'detail_number': 'D1', 'description': 'Desc1', 'quantity': 1.0,
             'account_number_id': 1, 'is_receipt': 0},
            {'pulse_id': 2, 'parent_id': 200, 'detail_number': 'D2', 'description': 'Desc2', 'quantity': 2.0,
             'account_number_id': 1, 'is_receipt': 0}
        ]
        mock_db_util_instance.create_or_update_sub_item_in_db.side_effect = [
            {'status': 'Created'},
            {'status': 'Updated'}
        ]

        self.service.db_util = mock_db_util_instance

        # Act
        with self.assertLogs(level='INFO') as log:
            self.service.sync_sub_items_from_monday_board()

        # Assert
        mock_monday_api_instance.fetch_all_sub_items.assert_called_once_with()
        self.assertEqual(mock_db_util_instance.prep_sub_item_event_for_db_creation.call_count, 2, "Should prepare two sub-items.")
        self.assertEqual(mock_db_util_instance.create_or_update_sub_item_in_db.call_count, 2, "Should create/update two sub-items.")
        self.assertIn('Successfully created sub-item with pulse_id: 1', log.output[0])
        self.assertIn('Successfully updated sub-item with pulse_id: 2', log.output[1])

    @patch('files_monday.monday_service.MondayAPI')
    def test_sync_sub_items_from_monday_board_fetch_exception(self, mock_monday_api):
        """
        Test sync_sub_items_from_monday_board when fetching sub-items raises an exception.
        """
        # Arrange
        mock_monday_api_instance = mock_monday_api.return_value
        mock_monday_api_instance.fetch_all_sub_items.side_effect = Exception('API Error')

        # Act & Assert
        with self.assertLogs(level='ERROR') as log:
            self.service.sync_sub_items_from_monday_board()
            self.assertIn('Error fetching sub-items from Monday.com: API Error', log.output[0])

    @patch('files_monday.monday_service.MondayAPI')
    def test_sync_sub_items_from_monday_board_sync_exception(self, mock_monday_api):
        """
        Test sync_sub_items_from_monday_board when syncing sub-items raises an exception.
        """
        # Arrange
        mock_monday_api_instance = mock_monday_api.return_value
        mock_monday_api_instance.fetch_all_sub_items.return_value = [
            {'id': '1', 'parent_item': {'id': '10'}, 'name': 'SubItem1'}
        ]

        mock_db_util_instance = MagicMock()
        mock_db_util_instance.prep_sub_item_event_for_db_creation.side_effect = Exception('Prep Error')

        self.service.db_util = mock_db_util_instance

        # Act & Assert
        with self.assertLogs(level='ERROR') as log:
            self.service.sync_sub_items_from_monday_board()
            self.assertIn('Unexpected error while syncing sub-items to DB: Prep Error', log.output[0])

    @patch('files_monday.monday_service.MondayAPI')
    def test_sync_sub_items_from_monday_board_orphan(self, mock_monday_api):
        """
        Test sync_sub_items_from_monday_board when a sub-item is orphaned (missing parent).
        """
        # Arrange
        mock_monday_api_instance = mock_monday_api.return_value
        mock_monday_api_instance.fetch_all_sub_items.return_value = [
            {'id': '1', 'parent_item': {'id': None}, 'name': 'SubItem1'},
            {'id': '2', 'parent_item': {'id': '20'}, 'name': 'SubItem2'}
        ]

        mock_db_util_instance = MagicMock()
        mock_db_util_instance.prep_sub_item_event_for_db_creation.side_effect = [
            None,
            {'pulse_id': 2, 'parent_id': 200, 'detail_number': 'D2',
             'description': 'Desc2', 'quantity': 2.0,
             'account_number_id': 1, 'is_receipt': 0}
        ]
        mock_db_util_instance.create_or_update_sub_item_in_db.side_effect = [
            {'status': 'Updated'}
        ]

        self.service.db_util = mock_db_util_instance

        # Act
        with self.assertLogs(level='WARNING') as log:
            self.service.sync_sub_items_from_monday_board()

        # Assert
        self.assertIn('Skipping sub-item with pulse_id 1 due to missing parent.', log.output[0])
        mock_db_util_instance.create_or_update_sub_item_in_db.assert_called_once_with({
            'pulse_id': 2, 'parent_id': 200, 'detail_number': 'D2',
            'description': 'Desc2', 'quantity': 2.0,
            'account_number_id': 1, 'is_receipt': 0
        })

    @patch('files_monday.monday_service.MondayAPI')
    def test_sync_sub_items_from_monday_board_handle_error_in_create_update(self, mock_monday_api):
        """
        Test sync_sub_items_from_monday_board when create_or_update_sub_item_in_db returns an error.
        """
        # Arrange
        mock_monday_api_instance = mock_monday_api.return_value
        mock_monday_api_instance.fetch_all_sub_items.return_value = [
            {'id': '1', 'parent_item': {'id': '10'}, 'name': 'SubItem1'}
        ]

        mock_db_util_instance = MagicMock()
        mock_db_util_instance.prep_sub_item_event_for_db_creation.return_value = {
            'pulse_id': 1, 'parent_id': 100, 'detail_number': 'D1',
            'description': 'Desc1', 'quantity': 1.0,
            'account_number_id': 1, 'is_receipt': 0
        }
        mock_db_util_instance.create_or_update_sub_item_in_db.return_value = {'status': 'Fail', 'error': 'DB Error'}

        self.service.db_util = mock_db_util_instance

        # Act
        with self.assertLogs(level='ERROR') as log:
            self.service.sync_sub_items_from_monday_board()
            self.assertIn('Failed to sync sub-item with pulse_id: 1. Error: DB Error', log.output[0])

    @patch('files_monday.monday_service.MondayAPI')
    def test_sync_sub_items_from_monday_board_empty_subitems(self, mock_monday_api):
        """
        Test syncing sub-items when there are no sub-items to sync.
        """
        # Arrange
        mock_monday_api_instance = mock_monday_api.return_value
        mock_monday_api_instance.fetch_all_sub_items.return_value = []

        mock_db_util_instance = MagicMock()
        self.service.db_util = mock_db_util_instance

        # Act
        with patch.object(self.service.db_util, 'prep_sub_item_event_for_db_creation') as mock_prep_sub, \
             patch.object(self.service.db_util, 'create_or_update_sub_item_in_db') as mock_create_update_sub:
            self.service.sync_sub_items_from_monday_board()

        # Assert
        mock_monday_api_instance.fetch_all_sub_items.assert_called_once_with()
        mock_prep_sub.assert_not_called()
        mock_create_update_sub.assert_not_called()

    @patch('files_monday.monday_service.MondayAPI')
    @patch('files_monday.monday_service.MondayDatabaseUtil')
    @patch('files_monday.monday_service.MondayUtil')
    def test_sync_contacts_from_monday_board_success(self, mock_monday_util, mock_db_util, mock_monday_api):
        """
        Test syncing contacts from Monday.com to the local database successfully.
        """
        # Arrange
        mock_monday_api_instance = mock_monday_api.return_value
        mock_monday_api_instance.fetch_all_contacts.return_value = [
            {'id': '1', 'name': 'Contact1'},
            {'id': '2', 'name': 'Contact2'}
        ]

        mock_monday_util_instance = mock_monday_util.return_value
        mock_db_util_instance = mock_db_util.return_value

        mock_monday_util_instance.prep_contact_event_for_db_creation.side_effect = [
            {'pulse_id': 1, 'name': 'Contact1', 'email': 'contact1@example.com'},
            {'pulse_id': 2, 'name': 'Contact2', 'email': 'contact2@example.com'}
        ]
        mock_db_util_instance.find_or_create_contact_item_in_db.side_effect = ['Created', 'Updated']

        # Act
        with self.assertLogs(level='INFO') as log:
            self.service.sync_contacts_from_monday_board()

        # Assert
        mock_monday_api_instance.fetch_all_contacts.assert_called_once_with(self.service.contact_board_id)
        self.assertEqual(mock_monday_util_instance.prep_contact_event_for_db_creation.call_count, 2, "Should prepare two contacts.")
        self.assertEqual(mock_db_util_instance.find_or_create_contact_item_in_db.call_count, 2, "Should create/update two contacts.")
        self.assertIn('Synced contact with pulse_id 1: Created', log.output[0])
        self.assertIn('Synced contact with pulse_id 2: Updated', log.output[1])

    @patch('files_monday.monday_service.MondayAPI')
    def test_sync_contacts_from_monday_board_fetch_exception(self, mock_monday_api):
        """
        Test sync_contacts_from_monday_board when fetching contacts raises an exception.
        """
        # Arrange
        mock_monday_api_instance = mock_monday_api.return_value
        mock_monday_api_instance.fetch_all_contacts.side_effect = Exception('API Error')

        # Act & Assert
        with self.assertLogs(level='ERROR') as log:
            self.service.sync_contacts_from_monday_board()
            self.assertIn('Error fetching contacts from Monday.com: API Error', log.output[0])

    @patch('files_monday.monday_service.MondayAPI')
    @patch('files_monday.monday_service.MondayDatabaseUtil')
    @patch('files_monday.monday_service.MondayUtil')
    def test_sync_contacts_from_monday_board_sync_exception(self, mock_monday_util, mock_db_util, mock_monday_api):
        """
        Test sync_contacts_from_monday_board when syncing contacts raises an exception.
        """
        # Arrange
        mock_monday_api_instance = mock_monday_api.return_value
        mock_monday_api_instance.fetch_all_contacts.return_value = [
            {'id': '1', 'name': 'Contact1'}
        ]

        mock_monday_util_instance = mock_monday_util.return_value
        mock_db_util_instance = mock_db_util.return_value

        mock_monday_util_instance.prep_contact_event_for_db_creation.side_effect = Exception('Prep Error')

        # Act & Assert
        with self.assertLogs(level='ERROR') as log:
            self.service.sync_contacts_from_monday_board()
            self.assertIn('Error syncing contacts to DB: Prep Error', log.output[0])

    @patch('files_monday.monday_service.MondayAPI')
    def test_sync_contacts_from_monday_board_handle_error_in_create_update(self, mock_monday_api):
        """
        Test sync_contacts_from_monday_board when create_or_update_contact_item_in_db returns 'Fail'.
        """
        # Arrange
        mock_monday_api_instance = mock_monday_api.return_value
        mock_monday_api_instance.fetch_all_contacts.return_value = [
            {'id': '1', 'name': 'Contact1'}
        ]

        mock_db_util_instance = MagicMock()
        mock_db_util_instance.prep_contact_event_for_db_creation.return_value = {'pulse_id': 1, 'name': 'Contact1', 'email': 'contact1@example.com'}
        mock_db_util_instance.create_or_update_contact_item_in_db.return_value = 'Fail'

        self.service.db_util = mock_db_util_instance

        # Act
        with self.assertLogs(level='ERROR') as log:
            self.service.sync_contacts_from_monday_board()

        # Assert
        self.assertIn('Error processing Contact in DB: Fail', log.output[0])

    def test_sync_contacts_from_monday_board_empty_contacts(self, mock_monday_util, mock_db_util, mock_monday_api):
        """
        Test syncing contacts when there are no contacts to sync.
        """
        # Arrange
        mock_monday_api_instance = mock_monday_api.return_value
        mock_monday_api_instance.fetch_all_contacts.return_value = []

        mock_db_util_instance = mock_db_util.return_value

        # Act
        with patch.object(self.service.db_util, 'prep_contact_event_for_db_creation') as mock_prep_contact, \
             patch.object(self.service.db_util, 'create_or_update_contact_item_in_db') as mock_create_or_update_contact:
            self.service.sync_contacts_from_monday_board()

        # Assert
        mock_monday_api_instance.fetch_all_contacts.assert_called_once_with(self.service.contact_board_id)
        mock_prep_contact.assert_not_called()
        mock_create_or_update_contact.assert_not_called()

    @patch('files_monday.monday_service.MondayAPI')
    @patch('files_monday.monday_service.MondayDatabaseUtil')
    @patch('files_monday.monday_service.MondayUtil')
    def test_sync_contacts_from_monday_board_handle_error_in_create_update(self, mock_monday_util, mock_db_util, mock_monday_api):
        """
        Test sync_contacts_from_monday_board when create_or_update_contact_item_in_db raises an exception.
        """
        # Arrange
        mock_monday_api_instance = mock_monday_api.return_value
        mock_monday_api_instance.fetch_all_contacts.return_value = [
            {'id': '1', 'name': 'Contact1'}
        ]

        mock_monday_util_instance = mock_monday_util.return_value
        mock_db_util_instance = mock_db_util.return_value

        mock_monday_util_instance.prep_contact_event_for_db_creation.side_effect = Exception('Prep Error')

        # Act & Assert
        with self.assertLogs(level='ERROR') as log:
            self.service.sync_contacts_from_monday_board()

        # Assert
        self.assertIn('Error syncing contacts to DB: Prep Error', log.output[0])

    @patch('files_monday.monday_service.MondayAPI')
    @patch('files_monday.monday_service.MondayDatabaseUtil')
    @patch('files_monday.monday_service.MondayUtil')
    def test_sync_contacts_from_monday_board_handle_error_in_create_or_update_failure(self, mock_monday_util, mock_db_util, mock_monday_api):
        """
        Test sync_contacts_from_monday_board when create_or_update_contact_item_in_db returns 'Fail'.
        """
        # Arrange
        mock_monday_api_instance = mock_monday_api.return_value
        mock_monday_api_instance.fetch_all_contacts.return_value = [
            {'id': '1', 'name': 'Contact1'}
        ]

        mock_monday_util_instance = mock_monday_util.return_value
        mock_db_util_instance = mock_db_util.return_value

        mock_monday_util_instance.prep_contact_event_for_db_creation.return_value = {'pulse_id': 1, 'name': 'Contact1', 'email': 'contact1@example.com'}
        mock_db_util_instance.find_or_create_contact_item_in_db.return_value = 'Fail'

        # Act
        with self.assertLogs(level='ERROR') as log:
            self.service.sync_contacts_from_monday_board()

        # Assert
        self.assertIn('Error processing Contact in DB: Fail', log.output[0])

    if __name__ == '__main__':
        unittest.main()