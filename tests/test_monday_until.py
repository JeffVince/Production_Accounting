import unittest
from unittest.mock import patch, MagicMock
from files_monday.monday_util import monday_util
import requests

class TestMondayUtil(unittest.TestCase):

    def setUp(self):
        self.monday_util = monday_util
        self.monday_util.headers = {'Authorization': 'test_token'}

    @patch('requests.post')
    def test_get_subitems_column_id_success(self, mock_post):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {'data': {'boards': [{'columns': [{'id': 'subitems', 'type': 'subtasks'}, {'id': 'status', 'type': 'status'}]}]}}
        mock_post.return_value = mock_response
        column_id = self.monday_util.get_subitems_column_id('123')
        self.assertEqual(column_id, 'subitems')

    @patch('requests.post')
    def test_get_subitems_column_id_failure(self, mock_post):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {'data': {'boards': [{'columns': [{'id': 'status', 'type': 'status'}]}]}}
        mock_post.return_value = mock_response
        with self.assertRaises(Exception) as context:
            self.monday_util.get_subitems_column_id('123')
        self.assertIn('Subitems column not found', str(context.exception))

    @patch('requests.post')
    def test_create_item_success(self, mock_post):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {'data': {'create_item': {'id': '456', 'name': 'Test Item'}}}
        mock_post.return_value = mock_response
        item_id = self.monday_util.create_item(group_id='group_1', item_name='Test Item', column_values={'status': {'label': 'Done'}})
        self.assertEqual(item_id, '456')

    @patch('requests.post')
    def test_create_item_failure(self, mock_post):
        mock_response = MagicMock()
        mock_response.status_code = 400
        mock_response.text = 'Bad Request'
        mock_post.return_value = mock_response
        item_id = self.monday_util.create_item(group_id='group_1', item_name='Test Item', column_values={'status': {'label': 'Done'}})
        self.assertIsNone(item_id)

    @patch('requests.post')
    def test_update_item_columns_success(self, mock_post):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {'data': {'change_multiple_column_values': {'id': '456'}}}
        mock_post.return_value = mock_response
        result = self.monday_util.update_item_columns(item_id='456', column_values={'status': {'label': 'In Progress'}})
        self.assertTrue(result)

    @patch('requests.post')
    def test_update_item_columns_failure(self, mock_post):
        mock_response = MagicMock()
        mock_response.status_code = 400
        mock_response.text = 'Bad Request'
        mock_post.return_value = mock_response
        result = self.monday_util.update_item_columns(item_id='456', column_values={'status': {'label': 'In Progress'}})
        self.assertFalse(result)

    def test_subitem_column_values_formatter(self):
        column_values = self.monday_util.subitem_column_values_formatter(notes='Payment due', status='Pending', file_id='123', description='Test Description', quantity=2, rate=100.0, date='2023-10-01', due_date='2023-10-15', account_number='5000', link='http://example.com')
        expected_values = {'payment_notes__1': 'Payment due', 'status4': {'label': 'Pending'}, 'text0': '123', 'text98': 'Test Description', 'numbers0': 2, 'numbers9': 100.0, 'date': {'date': '2023-10-01'}, 'date_1__1': {'date': '2023-10-15'}, 'dropdown': {'ids': ['2']}, 'link': {'url': 'http://example.com', 'text': 'Link'}}
        self.assertEqual(column_values, expected_values)

    @patch('requests.post')
    def test_create_subitem_success(self, mock_post):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {'data': {'create_subitem': {'id': '789'}}}
        mock_post.return_value = mock_response
        subitem_id = self.monday_util.create_subitem(parent_item_id='456', subitem_name='Test Subitem', column_values={'status4': {'label': 'Pending'}})
        self.assertEqual(subitem_id, '789')

    @patch('requests.post')
    def test_create_subitem_failure(self, mock_post):
        mock_response = MagicMock()
        mock_response.status_code = 400
        mock_response.text = 'Bad Request'
        mock_post.return_value = mock_response
        subitem_id = self.monday_util.create_subitem(parent_item_id='456', subitem_name='Test Subitem', column_values={'status4': {'label': 'Pending'}})
        self.assertIsNone(subitem_id)

    @patch('requests.post')
    def test_update_subitem_columns_success(self, mock_post):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {'data': {'change_multiple_column_values': {'id': '789'}}}
        mock_post.return_value = mock_response
        result = self.monday_util.update_subitem_columns(subitem_id='789', column_values={'status4': {'label': 'Completed'}})
        self.assertTrue(result)

    @patch('requests.post')
    def test_update_subitem_columns_failure(self, mock_post):
        mock_response = MagicMock()
        mock_response.status_code = 400
        mock_response.text = 'Bad Request'
        mock_post.return_value = mock_response
        result = self.monday_util.update_subitem_columns(subitem_id='789', column_values={'status4': {'label': 'Completed'}})
        self.assertFalse(result)

    @patch('requests.post')
    def test_link_contact_to_po_item_success(self, mock_post):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {'data': {'change_column_value': {'id': '456'}}}
        mock_post.return_value = mock_response
        result = self.monday_util.link_contact_to_po_item(po_item_id='456', contact_item_id='123')
        self.assertTrue(result)

    @patch('requests.post')
    def test_link_contact_to_po_item_failure(self, mock_post):
        mock_response = MagicMock()
        mock_response.status_code = 400
        mock_response.text = 'Bad Request'
        mock_post.return_value = mock_response
        result = self.monday_util.link_contact_to_po_item(po_item_id='456', contact_item_id='123')
        self.assertFalse(result)

    def test_validate_monday_request_success(self):
        request_headers = {'Authorization': f'Bearer {self.monday_util.monday_api_token}'}
        result = self.monday_util.validate_monday_request(request_headers)
        self.assertTrue(result)

    def test_validate_monday_request_failure(self):
        request_headers = {'Authorization': 'Bearer invalid_token'}
        result = self.monday_util.validate_monday_request(request_headers)
        self.assertFalse(result)
if __name__ == '__main__':
    unittest.main()