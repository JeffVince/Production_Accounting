# tests/test_monday_api.py

import unittest
from unittest.mock import patch, MagicMock, ANY
from monday_files.monday_api import monday_api
from requests.exceptions import HTTPError


class TestMondayAPI(unittest.TestCase):
    def setUp(self):
        """
        Set up the test environment by patching MondayClient before instantiating MondayAPI.
        This ensures that all instances of MondayClient within MondayAPI are mocked.
        """
        # Patch 'MondayClient' in 'monday_files.monday_api' before instantiating MondayAPI
        patcher = patch('monday_files.monday_api.MondayClient', autospec=True)
        self.mock_monday_client_class = patcher.start()
        self.addCleanup(patcher.stop)

        # Create a mock instance of MondayClient
        self.mock_monday_client_instance = self.mock_monday_client_class.return_value

        # Mock the 'items' attribute of MondayClient
        self.mock_monday_client_instance.items = MagicMock()

        # Initialize the MondayAPI instance (with the mocked MondayClient)
        self.monday_api = monday_api
        self.monday_api.api_token = 'test_token'  # Set a test token

    # --------------------- Tests for fetch_item_by_ID ---------------------

    def test_fetch_item_by_ID_success(self):
        """
        Test successful retrieval of an item by ID using MondayClient.
        """
        # Mock fetch_items_by_id to return a valid item
        self.mock_monday_client_instance.items.fetch_items_by_id.return_value = {
            'data': {
                'items': [{'id': '1', 'name': 'Item1'}]
            }
        }

        # Call the method under test
        item = self.monday_api.fetch_item_by_ID('1')

        # Assertions
        self.assertEqual(item['data']['items'][0]['id'], '1')
        self.assertEqual(item['data']['items'][0]['name'], 'Item1')

    def test_fetch_item_by_ID_no_items(self):
        """
        Test retrieval of an item by ID when no items are returned.
        """
        # Mock fetch_items_by_id to return an empty list
        self.mock_monday_client_instance.items.fetch_items_by_id.return_value = {
            'data': {
                'items': []
            }
        }

        # Call the method under test
        item = self.monday_api.fetch_item_by_ID('999')  # Assuming '999' does not exist

        # Assertions
        self.assertEqual(len(item['data']['items']), 0)

    @patch.object(monday_api.client.items, 'fetch_items_by_id')
    def test_fetch_item_by_ID_invalid_response(self, mock_fetch_items_by_id):
        """
        Test retrieval of an item by ID when the API returns an unexpected structure.
        Expecting a TypeError when attempting to access 'id' from a NoneType.
        """
        # Mock fetch_items_by_id to return 'items' as None
        mock_fetch_items_by_id.return_value = {
            'data': {
                'items': None  # Unexpected structure
            }
        }

        # Attempting to access ['id'] on None should raise a ValueError
        with self.assertRaises(ValueError):
            item = self.monday_api.fetch_item_by_ID('1')

    # --------------------- Tests for create_item ---------------------

    @patch('monday_files.monday_api.requests.post', autospec=True)
    def test_create_item_success(self, mock_post):
        """
        Test successful creation of an item.
        """
        # Mock the API response for item creation
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {'data': {'create_item': {'id': '123'}}}
        mock_post.return_value = mock_response

        # Call the method under test
        result = self.monday_api.create_item(
            board_id=123,
            item_name='Test Item',
            column_values={'status': {'label': 'Done'}}
        )

        # Assertions
        self.assertEqual(result['data']['create_item']['id'], '123')
        mock_post.assert_called_once_with(
            'https://api.monday.com/v2/',
            json={
                'query': ANY,  # Ignore the exact query string
                'variables': {
                    'board_id': 123,
                    'item_name': 'Test Item',
                    'column_values': {'status': {'label': 'Done'}}
                }
            },
            headers={"Authorization": 'test_token'}
        )

    @patch('monday_files.monday_api.requests.post', autospec=True)
    def test_create_item_failure(self, mock_post):
        """
        Test creation of an item when the API returns an error.
        Expecting an HTTPError to be raised.
        """
        # Mock the API response for failure
        mock_response = MagicMock()
        mock_response.status_code = 400
        mock_response.text = 'Bad Request'
        mock_response.raise_for_status.side_effect = HTTPError(
            "400 Client Error: Bad Request for url: https://api.monday.com/v2/"
        )
        mock_post.return_value = mock_response

        # Call the method under test and expect an exception
        with self.assertRaises(HTTPError):
            self.monday_api.create_item(
                board_id=123,
                item_name='Test Item',
                column_values={'status': {'label': 'Done'}}
            )

        mock_post.assert_called_once_with(
            'https://api.monday.com/v2/',
            json={
                'query': ANY,  # Ignore the exact query string
                'variables': {
                    'board_id': 123,
                    'item_name': 'Test Item',
                    'column_values': {'status': {'label': 'Done'}}
                }
            },
            headers={"Authorization": 'test_token'}
        )

    @patch('monday_files.monday_api.requests.post', autospec=True)
    def test_create_item_with_empty_column_values(self, mock_post):
        """
        Test creation of an item with empty column_values.
        """
        # Mock the API response for item creation
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {'data': {'create_item': {'id': '789'}}}
        mock_post.return_value = mock_response

        # Call the method under test
        result = self.monday_api.create_item(
            board_id=123,
            item_name='Item with No Columns',
            column_values={}
        )

        # Assertions
        self.assertEqual(result['data']['create_item']['id'], '789')
        mock_post.assert_called_once_with(
            'https://api.monday.com/v2/',
            json={
                'query': ANY,  # Ignore the exact query string
                'variables': {
                    'board_id': 123,
                    'item_name': 'Item with No Columns',
                    'column_values': {}
                }
            },
            headers={"Authorization": 'test_token'}
        )

    # --------------------- Tests for create_subitem ---------------------

    @patch('monday_files.monday_api.requests.post', autospec=True)
    def test_create_subitem_success(self, mock_post):
        """
        Test successful creation of a subitem.
        """
        # Mock the API response for subitem creation
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {'data': {'create_subitem': {'id': '456'}}}
        mock_post.return_value = mock_response

        # Call the method under test
        result = self.monday_api.create_subitem(
            parent_item_id=123,
            subitem_name='Test Subitem',
            column_values={'status4': {'label': 'Pending'}}
        )

        # Assertions
        self.assertEqual(result['data']['create_subitem']['id'], '456')
        mock_post.assert_called_once_with(
            'https://api.monday.com/v2/',
            json={
                'query': ANY,  # Ignore the exact query string
                'variables': {
                    'parent_item_id': 123,
                    'subitem_name': 'Test Subitem',
                    'column_values': {'status4': {'label': 'Pending'}}
                }
            },
            headers={"Authorization": 'test_token'}
        )

    @patch('monday_files.monday_api.requests.post', autospec=True)
    def test_create_subitem_failure(self, mock_post):
        """
        Test creation of a subitem when the API returns an error.
        Expecting an HTTPError to be raised.
        """
        # Mock the API response for failure
        mock_response = MagicMock()
        mock_response.status_code = 401
        mock_response.text = 'Unauthorized'
        mock_response.raise_for_status.side_effect = HTTPError(
            "401 Client Error: Unauthorized for url: https://api.monday.com/v2/"
        )
        mock_post.return_value = mock_response

        # Call the method under test and expect an exception
        with self.assertRaises(HTTPError):
            self.monday_api.create_subitem(
                parent_item_id=123,
                subitem_name='Test Subitem',
                column_values={'status4': {'label': 'Pending'}}
            )

        mock_post.assert_called_once_with(
            'https://api.monday.com/v2/',
            json={
                'query': ANY,  # Ignore the exact query string
                'variables': {
                    'parent_item_id': 123,
                    'subitem_name': 'Test Subitem',
                    'column_values': {'status4': {'label': 'Pending'}}
                }
            },
            headers={"Authorization": 'test_token'}
        )

    # --------------------- Tests for update_item ---------------------

    @patch('monday_files.monday_api.requests.post', autospec=True)
    def test_update_item_success(self, mock_post):
        """
        Test successful update of an item.
        """
        # Mock the API response for item update
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {'data': {'change_multiple_column_values': {'id': '123'}}}
        mock_post.return_value = mock_response

        # Call the method under test
        result = self.monday_api.update_item(
            item_id=123,
            column_values={'status': {'label': 'In Progress'}}
        )

        # Assertions
        self.assertEqual(result['data']['change_multiple_column_values']['id'], '123')
        mock_post.assert_called_once_with(
            'https://api.monday.com/v2/',
            json={
                'query': ANY,  # Ignore the exact query string
                'variables': {
                    'item_id': 123,
                    'column_values': {'status': {'label': 'In Progress'}}
                }
            },
            headers={"Authorization": 'test_token'}
        )

    @patch('monday_files.monday_api.requests.post', autospec=True)
    def test_update_item_failure(self, mock_post):
        """
        Test update of an item when the API returns an error.
        Expecting an HTTPError to be raised.
        """
        # Mock the API response for failure
        mock_response = MagicMock()
        mock_response.status_code = 500
        mock_response.text = 'Internal Server Error'
        mock_response.raise_for_status.side_effect = HTTPError(
            "500 Server Error: Internal Server Error for url: https://api.monday.com/v2/"
        )
        mock_post.return_value = mock_response

        # Call the method under test and expect an exception
        with self.assertRaises(HTTPError):
            self.monday_api.update_item(
                item_id=123,
                column_values={'status': {'label': 'In Progress'}}
            )

        mock_post.assert_called_once_with(
            'https://api.monday.com/v2/',
            json={
                'query': ANY,  # Ignore the exact query string
                'variables': {
                    'item_id': 123,
                    'column_values': {'status': {'label': 'In Progress'}}
                }
            },
            headers={"Authorization": 'test_token'}
        )

    @patch('monday_files.monday_api.requests.post', autospec=True)
    def test_update_item_with_invalid_column_values(self, mock_post):
        """
        Test updating an item with invalid column_values.
        Expecting an HTTPError to be raised.
        """
        # Mock the API response for failure
        mock_response = MagicMock()
        mock_response.status_code = 400
        mock_response.text = 'Bad Request'
        mock_response.raise_for_status.side_effect = HTTPError(
            "400 Client Error: Bad Request for url: https://api.monday.com/v2/"
        )
        mock_post.return_value = mock_response

        # Call the method under test and expect an exception
        with self.assertRaises(HTTPError):
            self.monday_api.update_item(
                item_id=123,
                column_values={'invalid_column': {'label': 'Unknown'}}
            )

        mock_post.assert_called_once_with(
            'https://api.monday.com/v2/',
            json={
                'query': ANY,  # Ignore the exact query string
                'variables': {
                    'item_id': 123,
                    'column_values': {'invalid_column': {'label': 'Unknown'}}
                }
            },
            headers={"Authorization": 'test_token'}
        )

    # --------------------- Tests for fetch_all_items ---------------------

    @patch('monday_files.monday_api.requests.post', autospec=True)
    def test_fetch_all_items_success_single_page(self, mock_post):
        """
        Test successful fetching of all items when all items are returned in a single page.
        """
        # Mock the API response for fetching items (single page)
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            'data': {
                'boards': [{
                    'items_page': {
                        'cursor': None,
                        'items': [
                            {'id': '1', 'name': 'Item1'},
                            {'id': '2', 'name': 'Item2'}
                        ]
                    }
                }]
            }
        }
        mock_post.return_value = mock_response

        # Call the method under test
        items = self.monday_api.fetch_all_items(board_id='123')

        # Assertions
        self.assertEqual(len(items), 2)
        self.assertEqual(items[0]['id'], '1')
        self.assertEqual(items[1]['id'], '2')
        mock_post.assert_called_once_with(
            'https://api.monday.com/v2/',
            json={
                'query': ANY,  # Ignore the exact query string
                'variables': {
                    'board_id': '123',  # Ensure board_id is a string
                    'limit': 50
                }
            },
            headers={"Authorization": 'test_token'}
        )

    @patch('monday_files.monday_api.requests.post', autospec=True)
    def test_fetch_all_items_success_multiple_pages(self, mock_post):
        """
        Test successful fetching of all items when items are spread across multiple pages.
        """
        # Mock the API responses for fetching items (multiple pages)
        mock_response_page1 = MagicMock()
        mock_response_page1.status_code = 200
        mock_response_page1.json.return_value = {
            'data': {
                'boards': [{
                    'items_page': {
                        'cursor': 'cursor1',
                        'items': [
                            {'id': '1', 'name': 'Item1'},
                            {'id': '2', 'name': 'Item2'}
                        ]
                    }
                }]
            }
        }

        mock_response_page2 = MagicMock()
        mock_response_page2.status_code = 200
        mock_response_page2.json.return_value = {
            'data': {
                'next_items_page': {
                    'cursor': None,
                    'items': [{'id': '3', 'name': 'Item3'}]
                }
            }
        }

        # Side effects for sequential calls
        mock_post.side_effect = [mock_response_page1, mock_response_page2]

        # Call the method under test with a lower limit to trigger pagination
        items = self.monday_api.fetch_all_items(board_id='123', limit=2)

        # Assertions
        self.assertEqual(len(items), 3)
        self.assertEqual(items[0]['id'], '1')
        self.assertEqual(items[1]['id'], '2')
        self.assertEqual(items[2]['id'], '3')
        self.assertEqual(mock_post.call_count, 2)

        # Define expected calls with ANY for query
        expected_calls = [
            unittest.mock.call(
                'https://api.monday.com/v2/',
                json={
                    'query': ANY,  # Ignore the exact query string
                    'variables': {
                        'board_id': '123',
                        'limit': 2
                    }
                },
                headers={"Authorization": 'test_token'}
            ),
            unittest.mock.call(
                'https://api.monday.com/v2/',
                json={
                    'query': ANY,  # Ignore the exact query string
                    'variables': {
                        'cursor': 'cursor1',
                        'limit': 2
                    }
                },
                headers={"Authorization": 'test_token'}
            )
        ]

        # Assert that the mock was called with the expected calls in order
        mock_post.assert_has_calls(expected_calls, any_order=False)

    @patch('monday_files.monday_api.requests.post', autospec=True)
    def test_fetch_all_items_failure(self, mock_post):
        """
        Test fetching all items when the API returns an error.
        Expecting an empty list to be returned due to failure.
        """
        # Mock the API response for failure
        mock_response = MagicMock()
        mock_response.status_code = 401
        mock_response.text = 'Unauthorized'
        mock_response.raise_for_status.side_effect = HTTPError(
            "401 Client Error: Unauthorized for url: https://api.monday.com/v2/"
        )
        mock_post.return_value = mock_response

        # Call the method under test
        items = self.monday_api.fetch_all_items(board_id='123')

        # Assertions
        self.assertEqual(items, [])  # Expecting an empty list due to failure
        mock_post.assert_called_once_with(
            'https://api.monday.com/v2/',
            json={
                'query': ANY,  # Ignore the exact query string
                'variables': {
                    'board_id': '123',  # Ensure board_id is a string
                    'limit': 50
                }
            },
            headers={"Authorization": 'test_token'}
        )


if __name__ == '__main__':
    unittest.main()