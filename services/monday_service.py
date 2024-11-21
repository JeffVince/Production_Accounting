# services/monday_service.py

import requests
from utilities.config import Config
from database.monday_repository import (
    add_or_update_monday_po,
    update_monday_po_status,
    link_contact_to_po,
)
import logging

logger = logging.getLogger(__name__)

class MondayService:
    def __init__(self):
        self.api_token = Config.MONDAY_API_TOKEN
        self.api_url = 'https://api.monday.com/v2/'

    def update_po_status(self, po_number: str, status: str):
        """Update the status of a PO in Monday.com."""
        query = '''
        mutation ($po_number: String!, $status: String!) {
            change_column_value(
                board_id: YOUR_BOARD_ID,
                item_id: $po_number,
                column_id: "status",
                value: $status
            ) {
                id
            }
        }
        '''
        variables = {'po_number': po_number, 'status': status}
        self._make_request(query, variables)
        # Update local database
        update_monday_po_status(po_number, status)

    def verify_po_tax_compliance(self, po_number: str) -> bool:
        """Verify tax compliance for a PO."""
        # Implementation logic, possibly interacting with tax_form_service
        return True

    def match_or_create_contact(self, vendor_name: str, po_number: str) -> int:
        """Match or create a contact for a vendor."""
        # Simulate querying Monday.com for existing contacts
        # If not found, create a new contact
        # Then, link contact to PO in local database
        contact_data = {
            'contact_id': 'new_contact_id',
            'name': vendor_name,
            'email': 'vendor@example.com',
            'phone': '123-456-7890',
        }
        link_contact_to_po(po_number, contact_data)  # Now po_number is passed in
        return contact_data['contact_id']

    def validate_po_detail_items(self, po_number: str) -> bool:
        """Validate the detail items of a PO."""
        # Implementation logic
        return True

    def notify_business_manager(self, po_number: str):
        """Notify the business manager about a PO."""
        # Send a notification via Slack or email
        pass

    def compare_receipt_with_po(self, po_number: str, receipt_data: dict) -> bool:
        """Compare receipt data with PO details."""
        # Implementation logic
        return True

    def _make_request(self, query: str, variables: dict):
        headers = {"Authorization": self.api_token}
        response = requests.post(self.api_url, json={'query': query, 'variables': variables}, headers=headers)
        if response.status_code != 200:
            logger.error(f"Monday API error: {response.text}")
            raise Exception(f"Monday API error: {response.text}")
        return response.json()