# tests/test_monday_service.py

import unittest
from services.monday_service import MondayService
from unittest.mock import patch, MagicMock
from utilities.config import Config

class TestMondayService(unittest.TestCase):
    def setUp(self):
        Config.MONDAY_API_TOKEN = 'dummy_api_token'
        self.service = MondayService()

    @patch('services.monday_service.requests.post')
    @patch('services.monday_service.update_monday_po_status')
    def test_update_po_status(self, mock_update_status, mock_post):
        mock_post.return_value.status_code = 200
        mock_post.return_value.json.return_value = {'data': {'change_column_value': {'id': '1'}}}
        self.service.update_po_status('PO123', 'APPROVED')
        mock_post.assert_called()
        mock_update_status.assert_called_with('PO123', 'APPROVED')

    @patch('services.monday_service.link_contact_to_po')
    def test_match_or_create_contact(self, mock_link_contact):
        contact_id = self.service.match_or_create_contact('Vendor A', 'PO123')  # Pass 'PO123' as po_number
        self.assertEqual(contact_id, 'new_contact_id')
        mock_link_contact.assert_called_with('PO123', {
            'contact_id': 'new_contact_id',
            'name': 'Vendor A',
            'email': 'vendor@example.com',
            'phone': '123-456-7890',
        })

if __name__ == '__main__':
    unittest.main()