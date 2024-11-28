# services/monday_service.py
import json

import requests

from monday_database_util import create_or_update_contact_item_in_db, create_or_update_main_item_in_db, \
    create_or_update_sub_item_in_db
from utilities.config import Config

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
        self.contact_board_id = M.CONTACT_BOARD_ID
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

    def update_po_status(self, pulse_id: int, status: str):
        """Update the status of a PO in Monday.com."""
        query = '''
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
        variables = {
            'board_id': int(self.board_id),
            'item_id': pulse_id,
            'column_id': M.PO_STATUS_COLUMN_ID,
            'value': json.dumps({'label': status})
        }
        self._make_request(query, variables)

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
        #link_contact_to_po(po_number, contact_data)  # Now po_number is passed in
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

    #good to go
    def sync_main_items_from_monday_board(self):
        """
        Synchronize all items from Monday.com boards to your database.
        """
        print(f"Fetching items from board {self.board_id}...")
        all_items = self.monday_api.fetch_all_items(self.board_id)
        print(f"Total items fetched from board {self.board_id}: {len(all_items)}")

        # Now, all_items contains all items from all specified boards
        # Proceed with syncing these items to your database

        for item in all_items:
            creation_item = M.prep_main_item_event_for_db_creation(item)
            create_or_update_main_item_in_db(creation_item)

    #good to go
    def sync_sub_items_from_monday_board(self):
        """
        Synchronize all sub-items from a Monday.com board to your database.
        Each sub-item is associated with a parent item. This function fetches all sub-items,
        extracts their parent IDs, and updates the database accordingly.
        """
        try:
            print(f"Fetching sub-items from board {self.subitem_board_id}...")
            all_items = self.monday_api.fetch_all_sub_items(self.subitem_board_id)
            print(f"Total sub-items fetched from board {self.subitem_board_id}: {len(all_items)}")
        except Exception as e:
            logger.error(f"Error fetching Sub Items from Monday.com: {e}")
            return
        try:
            for item in all_items:
                creation_item = M.prep_sub_item_event_for_db_creation(item)
                if not creation_item:
                    continue# Skip to the next item
                result = create_or_update_sub_item_in_db(creation_item)

                if not result:
                    logger.error(f"Failed to create or update sub-item with pulse_id: {creation_item.get('pulse_id')}")
                    continue  # Skip to the next item

                if result.get("status") == "Created":
                    logger.info(
                        f"Successfully created sub-item with pulse_id: {creation_item.get('pulse_id')}, surrogate_id: {result.get('detail_item_id')}")
                else:
                    logger.error(
                        f"Failed to create sub-item with pulse_id: {creation_item.get('pulse_id')}. Error: {result.get('error')}")
        except Exception as e:
            logger.exception(f"Unexpected error while adding Sub Items to DB: {e}")
            return

        print("Sub-items synchronization completed successfully.")
        return

    def sync_contacts_from_monday_board(self):
        """
        Synchronize all contacts from a Monday.com board to your database.
        """
        try:
            print(f"Fetching contacts from board {self.contact_board_id}...")
            all_contacts = self.monday_api.fetch_all_contacts(self.contact_board_id)
            print(f"Total contacts fetched from board {self.subitem_board_id}: {len(all_contacts)}")
        except Exception as e:
            logger.error(f"Error fetching Contacts from Monday.com: {e}")
            return
        try:
            for item in all_contacts:
                prepped_item = M.prep_contact_event_for_db_creation(item)
                create_or_update_contact_item_in_db(prepped_item)
        except Exception as e:
            logger.error(f"Error adding contacts to DB: {e}")
            return
        print("Contacts synchronization completed successfully.")
        return