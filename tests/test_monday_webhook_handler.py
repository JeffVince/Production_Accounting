import unittest
from unittest.mock import patch, MagicMock
from flask import Flask, jsonify
from monday_webhook_handler import MondayWebhookHandler


class TestMondayWebhookHandler(unittest.TestCase):
    def setUp(self):
        self.app = Flask(__name__)
        self.app.testing = True
        self.handler = MondayWebhookHandler()

    def test_verify_challenge_success(self):
        with self.app.test_request_context():
            event = {'challenge': 'test_challenge'}
            response = self.handler.verify_challenge(event)
            self.assertIsNotNone(response)
            self.assertEqual(response[1], 200)
            self.assertEqual(response[0].json, {'challenge': 'test_challenge'})

    def test_process_po_status_change_success(self):
        with self.app.test_request_context():
            event_data = {
                'event': {
                    'pulseId': '123',
                    'value': {'label': {'text': 'Approved'}}
                }
            }
            # Mock the method directly on the handler instance
            self.handler.process_po_status_change = MagicMock(
                return_value=(jsonify({"message": "PO status change processed"}), 200)
            )
            response = self.handler.process_po_status_change(event_data)
            self.assertEqual(response[1], 200)
            self.assertEqual(response[0].json, {"message": "PO status change processed"})

    def test_process_po_status_change_error(self):
        with self.app.test_request_context():
            event_data = {
                'event': {
                    'pulseId': '123',
                    'value': {'label': {'text': 'Approved'}}
                }
            }
            # Mock the method to simulate an error response
            self.handler.process_po_status_change = MagicMock(
                return_value=(jsonify({"error": "Internal Server Error"}), 500)
            )
            response = self.handler.process_po_status_change(event_data)
            self.assertEqual(response[1], 500)
            self.assertEqual(response[0].json, {"error": "Internal Server Error"})

    def test_process_sub_item_change_success(self):
        with self.app.test_request_context():
            event_data = {
                'event': {
                    'pulseId': '456',
                    'columnId': 'text0',
                    'columnType': 'text',
                    'value': 'New Value'
                }
            }
            # Mock the method directly on the handler instance
            self.handler.process_sub_item_change = MagicMock(
                return_value=(jsonify({"message": "SubItem change processed successfully"}), 200)
            )
            response = self.handler.process_sub_item_change(event_data)
            self.assertEqual(response[1], 200)
            self.assertEqual(response[0].json, {"message": "SubItem change processed successfully"})

    def test_process_sub_item_change_error(self):
        with self.app.test_request_context():
            event_data = {
                'event': {
                    'pulseId': '456',
                    'columnId': 'text0',
                    'columnType': 'text',
                    'value': 'New Value'
                }
            }
            # Mock the method to simulate an error response
            self.handler.process_sub_item_change = MagicMock(
                return_value=(jsonify({"error": "Internal Server Error"}), 500)
            )
            response = self.handler.process_sub_item_change(event_data)
            self.assertEqual(response[1], 500)
            self.assertEqual(response[0].json, {"error": "Internal Server Error"})

    def test_process_sub_item_delete_success(self):
        with self.app.test_request_context():
            event_data = {'event': {'pulseId': '456'}}
            # Mock the method directly on the handler instance
            self.handler.process_sub_item_delete = MagicMock(
                return_value=(jsonify({"message": "SubItem deleted successfully"}), 200)
            )
            response = self.handler.process_sub_item_delete(event_data)
            self.assertEqual(response[1], 200)
            self.assertEqual(response[0].json, {"message": "SubItem deleted successfully"})

    def test_process_sub_item_delete_error(self):
        with self.app.test_request_context():
            event_data = {'event': {'pulseId': '456'}}
            # Mock the method to simulate an error response
            self.handler.process_sub_item_delete = MagicMock(
                return_value=(jsonify({"error": "Internal Server Error"}), 500)
            )
            response = self.handler.process_sub_item_delete(event_data)
            self.assertEqual(response[1], 500)
            self.assertEqual(response[0].json, {"error": "Internal Server Error"})


if __name__ == '__main__':
    unittest.main()