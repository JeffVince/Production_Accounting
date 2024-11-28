# tests/test_monday_api.py

import unittest
from unittest.mock import patch
from monday_files.monday_api import MondayAPI

class TestMondayAPI(unittest.TestCase):
    def setUp(self):
        self.monday_api = MondayAPI()
        self.monday_api.api_token = 'dummy_token'
        self.monday_api.api_url = 'https://api.monday.com/v2/'

    @patch('integrations.monday_api.requests.post')
    def test_create_item(self, mock_post):
        mock_post.return_value.status_code = 200
        mock_post.return_value.json.return_value = {'data': {'create_item': {'id': '123'}}}
        response = self.monday_api.create_item(12345, 'Test Item', '{"status": "Done"}')
        self.assertEqual(response['data']['create_item']['id'], '123')
        mock_post.assert_called()

    @patch('integrations.monday_api.requests.post')
    def test_update_item(self, mock_post):
        mock_post.return_value.status_code = 200
        mock_post.return_value.json.return_value = {'data': {'change_multiple_column_values': {'id': '123'}}}
        response = self.monday_api.update_item(123, '{"status": "Working on it"}')
        self.assertEqual(response['data']['change_multiple_column_values']['id'], '123')
        mock_post.assert_called()

    @patch('integrations.monday_api.requests.post')
    def test_create_subitem(self, mock_post):
        mock_post.return_value.status_code = 200
        mock_post.return_value.json.return_value = {'data': {'create_subitem': {'id': '456'}}}
        response = self.monday_api.create_subitem(123, 'Test Subitem', '{"status": "Stuck"}')
        self.assertEqual(response['data']['create_subitem']['id'], '456')
        mock_post.assert_called()

    @patch('integrations.monday_api.requests.post')
    def test_get_item_by_name(self, mock_post):
        mock_post.return_value.status_code = 200
        mock_post.return_value.json.return_value = {'data': {'boards': [{'items': [{'id': '123', 'name': 'Test Item'}]}]}}
        response = self.monday_api.get_item_by_name(12345, 'Test Item')
        self.assertEqual(response['data']['boards'][0]['items'][0]['name'], 'Test Item')
        mock_post.assert_called()

    @patch('integrations.monday_api.requests.post')
    def test_search_items(self, mock_post):
        mock_post.return_value.status_code = 200
        mock_post.return_value.json.return_value = {'data': {'items_by_column_values': [{'id': '123', 'name': 'Test Item'}]}}
        response = self.monday_api.search_items('Test Query')
        self.assertEqual(response['data']['items_by_column_values'][0]['name'], 'Test Item')
        mock_post.assert_called()

    @patch('integrations.monday_api.requests.post')
    def test_link_contact_to_item(self, mock_post):
        mock_post.return_value.status_code = 200
        mock_post.return_value.json.return_value = {'data': {'change_multiple_column_values': {'id': '123'}}}
        response = self.monday_api.link_contact_to_item(123, 456)
        self.assertEqual(response['data']['change_multiple_column_values']['id'], '123')
        mock_post.assert_called()

    @patch('integrations.monday_api.requests.post')
    def test_get_contact_list(self, mock_post):
        mock_post.return_value.status_code = 200
        mock_post.return_value.json.return_value = {'data': {'users': [{'id': '1', 'name': 'User One', 'email': 'user1@example.com'}]}}
        response = self.monday_api.get_contact_list()
        self.assertEqual(response['data']['users'][0]['name'], 'User One')
        mock_post.assert_called()

    def test_create_contact(self):
        with self.assertRaises(NotImplementedError):
            self.monday_api.create_contact({'name': 'New Contact', 'email': 'contact@example.com'})

if __name__ == '__main__':
    unittest.main()