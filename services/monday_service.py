# services/monday_service.py

import requests
from utilities.config import Config
from database.monday_database_util import (
    update_main_item_from_monday,
    update_monday_po_status,
    link_contact_to_po, insert_main_item, insert_subitem
)
import utilities.monday_util as M
import logging
from monday_api import MondayAPI
from monday import MondayClient

logger = logging.getLogger(__name__)


class MondayService:
    def __init__(self):
        self.api_token = Config.MONDAY_API_TOKEN
        self.board_id = M.PO_BOARD_ID
        self.subitem_board_id = M.SUBITEM_BOARD_ID
        self.api_url = 'https://api.monday.com/v2/'
        self.monday_api = MondayAPI()
        self.monday_client = MondayClient(self.api_token)

    def _make_request(self, query: str, variables: dict = None):
        headers = {"Authorization": self.api_token}
        response = requests.post(
            self.api_url,
            json={'query': query, 'variables': variables},
            headers=headers
        )
        response.raise_for_status()
        return response.json()

    def update_po_status(self, po_number: str, status: str):
        """Update the status of a PO in Monday.com."""
        query = '''
        mutation ($po_number: String!, $status: String!, $board_id: String!) {
            change_column_value(
                board_id: $board_id,
                item_id: $po_number,
                column_id: "status",
                value: $status
            ) {
                id
            }
        }
        '''
        variables = {'po_number': po_number, 'status': status, 'board_id': self.board_id}
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

    def get_po_number_from_item(self, item_id):
        """
            Retrieve the PO number from a specific item in Monday.com.

            Parameters:
            - item_id (int): The ID of the item to query.

            Returns:
            - str: The PO number if found, else None.
            """
        query = '''
              query ($item_id: [ID!], $po_column_id: [String!]) {
        items (ids: $item_id) {
            column_values (ids: $po_column_id) {
                text
            }
        }
    }
            '''
        variables = {
            'item_id': [str(item_id)],
            'po_column_id': [M.PO_NUMBER_COLUMN]
        }
        response = self._make_request(query, variables)
        try:
            po_number = response['data']['items'][0]['column_values'][0]['text']
            return po_number
        except (KeyError, IndexError) as e:
            logger.error(f"Error retrieving PO number for item ID {item_id}: {e}")
            return None

    def link_contact_to_item(self, item_id: int, contact_id: int):
        """Links a contact to an item."""
        # Assuming a 'contacts' column exists
        column_values = '{"contacts": {"item_ids": [' + str(contact_id) + ']}}'
        return self.update_item(item_id, column_values)

    def sync_main_items_from_monday_board(self):
        """
        Synchronize all items from Monday.com boards to your database.
        """
        print(f"Fetching items from board {self.board_id}...")
        all_items = self.monday_api.fetch_all_main_items(self.board_id)
        print(f"Total items fetched from board {self.board_id}: {len(all_items)}")

        # Now, all_items contains all items from all specified boards
        # Proceed with syncing these items to your database
        for item in all_items:
            insert_main_item(item)

    def sync_sub_items_from_monday_board(self):
        """
        Synchronize all items from Monday.com boards to your database.
        """
        try:
            print(f"Fetching items from board {self.board_id}...")
            all_items = self.monday_api.fetch_all_sub_items(self.subitem_board_id)
            print(f"Total items fetched from board {self.board_id}: {len(all_items)}")
        except Exception as e:
            logger.error(f"Error fetching Sub Item from Monday: {e}")
            return

        try:
            for item in all_items:
                print(item)
                insert_subitem(item)
        except Exception as e:
            logger.error(f"Error adding Sub Item to DB: {e}")
            return

        return
