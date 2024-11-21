# tests/test_monday_api.py

import unittest
from unittest.mock import patch, MagicMock
import os
import textwrap  # Import textwrap for dedent
import logging

# Disable logging for the entire test suite to prevent clutter
logging.disable(logging.CRITICAL)

class TestMondayAPI(unittest.TestCase):
    # Apply patch decorators to mock logging handlers before any test runs
    @patch('integrations.monday_api.logging.FileHandler')
    @patch('integrations.monday_api.logging.StreamHandler')
    @patch.dict(os.environ, {
        'MONDAY_API_TOKEN': 'test_api_token',
        'MONDAY_PO_BOARD_ID': '123456',
        'MONDAY_PO_PROJECT_ID_COLUMN': 'project_id',
        'MONDAY_PO_NUMBER_COLUMN': 'po_number',
        'MONDAY_PO_DESCRIPTION_COLUMN': 'description',
        'MONDAY_PO_TAX_COLUMN': 'tax',
        'MONDAY_PO_FOLDER_LINK_COLUMN': 'folder_link',
        'MONDAY_PO_STATUS_COLUMN': 'status',
        'MONDAY_PRODUCER_PM_COLUMN': 'producer_pm',
        'MONDAY_UPDATED_DATE_COLUMN': 'updated_date',
        'MONDAY_SUBITEM_STATUS_COLUMN': 'subitem_status',
        'MONDAY_SUBITEM_ID_COLUMN': 'subitem_id',
        'MONDAY_SUBITEM_DESCRIPTION_COLUMN': 'subitem_description',
        'MONDAY_SUBITEM_RATE_COLUMN': 'subitem_rate',
        'MONDAY_SUBITEM_QUANTITY_COLUMN': 'subitem_quantity',
        'MONDAY_SUBITEM_ACCOUNT_NUMBER_COLUMN': 'subitem_account_number',
        'MONDAY_SUBITEM_DATE_COLUMN': 'subitem_date',
        'MONDAY_SUBITEM_LINK_COLUMN': 'subitem_link',
        'MONDAY_SUBITEM_DUE_DATE_COLUMN': 'subitem_due_date',
        'MONDAY_CREATION_LOG_COLUMN': 'creation_log__1',
        'MONDAY_API_LOG_PATH': './tests/logs/monday_api.log'  # Changed to tests/logs
    }, clear=True)
    def setUp(self, mock_stream_handler, mock_file_handler):
        """
        Initialize the MondayAPI instance with mocked logging handlers and environment variables.
        """
        # Import MondayAPI after mocking logging handlers and environment variables
        from integrations.monday_api import MondayAPI
        self.monday_api = MondayAPI()

    @patch('integrations.monday_api.requests.post')
    def test_create_item_success(self, mock_post):
        # Mock response for create_item
        mock_response = MagicMock()
        mock_response.raise_for_status = MagicMock()
        mock_response.json.return_value = {
            'data': {
                'create_item': {
                    'id': 'item_123',
                    'name': 'New Purchase Order'
                }
            }
        }
        mock_post.return_value = mock_response

        # Call create_item
        board_id = 123456
        item_name = "New Purchase Order"
        column_values = {
            'project_id': {"text": "Project XYZ"},
            'po_number': {"text": "PO-001"},
            'description': {"text": "Description of the PO"},
            'tax': {"text": "Tax Information"},
            'folder_link': {"text": "http://example.com/folder"},
            'status': {"label": "Pending"},
            'producer_pm': {"personsAndTeams": [{"id": 123, "kind": "person"}]},
            'updated_date': {"date": "2024-11-18"}
        }

        result = self.monday_api.create_item(board_id, item_name, column_values)

        # Assertions
        self.assertEqual(result['id'], 'item_123')
        self.assertEqual(result['name'], 'New Purchase Order')

        # Normalize both queries using dedent
        expected_query = textwrap.dedent("""
            mutation ($boardId: Int!, $itemName: String!, $columnValues: JSON!) {
                create_item(board_id: $boardId, item_name: $itemName, column_values: $columnValues) {
                    id
                    name
                }
            }
            """)

        # Extract actual_query from the mocked call
        args, kwargs = mock_post.call_args
        actual_query = kwargs['json']['query']
        actual_query_normalized = textwrap.dedent(actual_query)

        self.assertEqual(actual_query_normalized.strip(), expected_query.strip())
        self.assertEqual(kwargs['json']['variables']['boardId'], board_id)
        self.assertEqual(kwargs['json']['variables']['itemName'], item_name)
        self.assertEqual(kwargs['json']['variables']['columnValues'], column_values)

    @patch('integrations.monday_api.requests.post')
    def test_create_item_failure(self, mock_post):
        # Mock response with GraphQL error
        mock_response = MagicMock()
        mock_response.raise_for_status = MagicMock()
        mock_response.json.return_value = {
            'errors': [{'message': 'Invalid board ID'}]
        }
        mock_post.return_value = mock_response

        # Call create_item and expect an exception
        board_id = 999999  # Invalid board ID
        item_name = "New Purchase Order"
        column_values = {}

        with self.assertRaises(Exception) as context:
            self.monday_api.create_item(board_id, item_name, column_values)

        self.assertIn('Invalid board ID', str(context.exception))

    @patch('integrations.monday_api.requests.post')
    def test_update_item_success(self, mock_post):
        # Mock response for update_item
        mock_response = MagicMock()
        mock_response.raise_for_status = MagicMock()
        mock_response.json.return_value = {
            'data': {
                'change_multiple_columns': {
                    'id': 'item_123',
                    'name': 'Updated Purchase Order'
                }
            }
        }
        mock_post.return_value = mock_response

        # Call update_item
        item_id = 123
        column_values = {
            'status': {"label": "Approved"},
            'description': {"text": "Updated description"}
        }

        result = self.monday_api.update_item(item_id, column_values)

        # Assertions
        self.assertEqual(result['id'], 'item_123')
        self.assertEqual(result['name'], 'Updated Purchase Order')

    @patch('integrations.monday_api.requests.post')
    def test_create_subitem_success(self, mock_post):
        # Mock response for create_subitem
        mock_response = MagicMock()
        mock_response.raise_for_status = MagicMock()
        mock_response.json.return_value = {
            'data': {
                'create_subitem': {
                    'id': 'subitem_456',
                    'name': 'Subitem 1'
                }
            }
        }
        mock_post.return_value = mock_response

        # Call create_subitem
        parent_item_id = 123
        subitem_name = "Subitem 1"
        column_values = {
            'subitem_status': {"label": "Pending"},
            'subitem_description': {"text": "Description of subitem"},
            'subitem_rate': {"text": "50"},
            'subitem_quantity': {"text": "20"},
            'subitem_account_number': {"text": "ACC-001"},
            'subitem_date': {"date": "2024-11-18"},
            'subitem_link': {"text": "http://example.com/subitem"},
            'subitem_due_date': {"date": "2024-12-18"},
            'creation_log__1': {"text": "Log entry"}
        }

        result = self.monday_api.create_subitem(parent_item_id, subitem_name, column_values)

        # Assertions
        self.assertEqual(result['id'], 'subitem_456')
        self.assertEqual(result['name'], 'Subitem 1')

    @patch('integrations.monday_api.requests.post')
    def test_get_item_by_name_found(self, mock_post):
        # Mock response for get_item_by_name
        mock_response = MagicMock()
        mock_response.raise_for_status = MagicMock()
        mock_response.json.return_value = {
            'data': {
                'items_by_board': [
                    {
                        'id': 'item_123',
                        'name': 'Existing Purchase Order',
                        'column_values': []
                    }
                ]
            }
        }
        mock_post.return_value = mock_response

        # Call get_item_by_name
        board_id = 123456
        item_name = "Existing Purchase Order"

        result = self.monday_api.get_item_by_name(board_id, item_name)

        # Assertions
        self.assertIsNotNone(result)
        self.assertEqual(result['id'], 'item_123')
        self.assertEqual(result['name'], 'Existing Purchase Order')

    @patch('integrations.monday_api.requests.post')
    def test_get_item_by_name_not_found(self, mock_post):
        # Mock response for get_item_by_name with no matching items
        mock_response = MagicMock()
        mock_response.raise_for_status = MagicMock()
        mock_response.json.return_value = {
            'data': {
                'items_by_board': [
                    {
                        'id': 'item_124',
                        'name': 'Another PO',
                        'column_values': []
                    }
                ]
            }
        }
        mock_post.return_value = mock_response

        # Call get_item_by_name with a non-existent name
        board_id = 123456
        item_name = "Nonexistent PO"

        result = self.monday_api.get_item_by_name(board_id, item_name)

        # Assertions
        self.assertIsNone(result)

    @patch('integrations.monday_api.requests.post')
    def test_search_items_success(self, mock_post):
        # Mock response for search_items
        mock_response = MagicMock()
        mock_response.raise_for_status = MagicMock()
        mock_response.json.return_value = {
            'data': {
                'search_items': [
                    {
                        'id': 'item_123',
                        'name': 'Search PO 1',
                        'board': {'id': 'board_1', 'name': 'Board 1'},
                        'column_values': []
                    },
                    {
                        'id': 'item_124',
                        'name': 'Search PO 2',
                        'board': {'id': 'board_2', 'name': 'Board 2'},
                        'column_values': []
                    }
                ]
            }
        }
        mock_post.return_value = mock_response

        # Call search_items
        query_str = "Search PO"

        result = self.monday_api.search_items(query_str)

        # Assertions
        self.assertIsInstance(result, list)
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0]['id'], 'item_123')
        self.assertEqual(result[1]['id'], 'item_124')

    @patch('integrations.monday_api.requests.post')
    def test_link_contact_to_item_success(self, mock_post):
        # Mock response for link_contact_to_item
        mock_response = MagicMock()
        mock_response.raise_for_status = MagicMock()
        mock_response.json.return_value = {
            'data': {
                'change_multiple_columns': {
                    'id': 'item_123',
                    'name': 'Updated PO'
                }
            }
        }
        mock_post.return_value = mock_response

        # Call link_contact_to_item
        item_id = 123
        contact_id = 456

        result = self.monday_api.link_contact_to_item(item_id, contact_id)

        # Assertions
        self.assertEqual(result['id'], 'item_123')
        self.assertEqual(result['name'], 'Updated PO')

    @patch('integrations.monday_api.requests.post')
    def test_get_contact_list_success(self, mock_post):
        # Mock response for get_contact_list
        mock_response = MagicMock()
        mock_response.raise_for_status = MagicMock()
        mock_response.json.return_value = {
            'data': {
                'users': [
                    {'id': 1, 'name': 'User One', 'email': 'user1@example.com'},
                    {'id': 2, 'name': 'User Two', 'email': 'user2@example.com'}
                ]
            }
        }
        mock_post.return_value = mock_response

        # Call get_contact_list
        result = self.monday_api.get_contact_list()

        # Assertions
        self.assertIsInstance(result, list)
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0]['id'], 1)
        self.assertEqual(result[0]['name'], 'User One')
        self.assertEqual(result[1]['id'], 2)
        self.assertEqual(result[1]['email'], 'user2@example.com')

    @patch('integrations.monday_api.requests.post')
    def test_create_contact_success(self, mock_post):
        # Mock response for create_contact
        mock_response = MagicMock()
        mock_response.raise_for_status = MagicMock()
        mock_response.json.return_value = {
            'data': {
                'create_user': {
                    'id': 789,
                    'name': 'New User',
                    'email': 'newuser@example.com'
                }
            }
        }
        mock_post.return_value = mock_response

        # Call create_contact
        contact_data = {
            'name': 'New User',
            'email': 'newuser@example.com'
        }

        result = self.monday_api.create_contact(contact_data)

        # Assertions
        self.assertEqual(result['id'], 789)
        self.assertEqual(result['name'], 'New User')
        self.assertEqual(result['email'], 'newuser@example.com')

    @patch('integrations.monday_api.requests.post')
    def test_create_contact_failure(self, mock_post):
        # Mock response with GraphQL error
        mock_response = MagicMock()
        mock_response.raise_for_status = MagicMock()
        mock_response.json.return_value = {
            'errors': [{'message': 'Email already exists'}]
        }
        mock_post.return_value = mock_response

        # Call create_contact and expect an exception
        contact_data = {
            'name': 'Existing User',
            'email': 'existinguser@example.com'
        }

        with self.assertRaises(Exception) as context:
            self.monday_api.create_contact(contact_data)

        self.assertIn('Email already exists', str(context.exception))

    def test_init_without_api_token(self):
        # Remove MONDAY_API_TOKEN and attempt to initialize
        with patch.dict(os.environ, {'MONDAY_API_TOKEN': ''}):
            with self.assertRaises(ValueError) as context:
                from integrations.monday_api import MondayAPI
                MondayAPI()
            self.assertIn('Monday.com API token is required.', str(context.exception))


if __name__ == '__main__':
    unittest.main()