# services/monday_service.py

import json
import logging
from typing import Any

import requests

from logger import setup_logging
from singleton import SingletonMeta
from utilities.config import Config
from monday_files.monday_util import monday_util
from monday_database_util import monday_database_util
from monday_files.monday_api import monday_api


class MondayService(metaclass=SingletonMeta):
    def __init__(self):
        if not hasattr(self, '_initialized'):
            # Set up logging
            self.logger = logging.getLogger("app_logger")
            # Initialize the MondayUtil and MondayDatabaseUtil instances
            self.monday_util = monday_util
            self.db_util = monday_database_util
            self.monday_api = monday_api

            # Configuration parameters
            self.api_token = Config.MONDAY_API_TOKEN
            self.board_id = self.monday_util.PO_BOARD_ID
            self.subitem_board_id = self.monday_util.SUBITEM_BOARD_ID
            self.contact_board_id = self.monday_util.CONTACT_BOARD_ID
            self.api_url = self.monday_util.MONDAY_API_URL  # Reuse from MondayUtil
            self.logger.info("Monday Service initialized")
            self._initialized = True

    def _make_request(self, query: str, variables: dict = None):
        """
        Internal method to make GraphQL requests to the Monday.com API.

        Args:
            query (str): The GraphQL query or mutation.
            variables (dict, optional): Variables for the GraphQL query.

        Returns:
            dict: The JSON response from the API.
        """
        headers = {"Authorization": self.api_token}
        response = requests.post(
            self.api_url,
            json={'query': query, 'variables': variables},
            headers=headers
        )
        response.raise_for_status()
        return response.json()

    def update_po_status(self, pulse_id: int, status: str):
        """
        Update the status of a Purchase Order (PO) in Monday.com.

        Args:
            pulse_id (int): The ID of the PO item in Monday.com.
            status (str): The new status label to set.
        """
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
            'column_id': self.monday_util.PO_STATUS_COLUMN_ID,
            'value': json.dumps({'label': status})
        }
        try:
            self._make_request(query, variables)
            self.logger.info(f"Updated PO status for item ID {pulse_id} to '{status}'.")
        except requests.HTTPError as e:
            self.logger.error(f"Failed to update PO status for item ID {pulse_id}: {e}")
            raise

    def match_or_create_contact(self, vendor_name: str, po_number: str) -> int:
        """
        Match an existing contact or create a new one for a vendor, then link it to the PO.

        Args:
            vendor_name (str): The name of the vendor.
            po_number (str): The PO number to associate with the contact.

        Returns:
            int: The contact ID in Monday.com.
        """
        try:
            # Attempt to find an existing contact by vendor name
            contact = self.monday_util.find_contact_item_by_name(vendor_name)
            if contact:
                contact_id = contact['item_id']
                self.logger.info(f"Found existing contact '{vendor_name}' with ID {contact_id}.")
            else:
                # If not found, create a new contact
                self.logger.info(f"No existing contact found for '{vendor_name}'. Creating a new contact.")
                # Example column values; adjust as necessary
                column_values = {
                    self.monday_util.CONTACT_NAME: vendor_name,
                    self.monday_util.CONTACT_EMAIL: 'vendor@example.com',
                    self.monday_util.CONTACT_PHONE: '123-456-7890',
                    # Add other necessary fields
                }
                contact_id = self.monday_util.create_item(
                    group_id="contacts_group_id",  # Replace with actual group ID
                    item_name=vendor_name,
                    column_values=column_values
                )
                if not contact_id:
                    self.logger.error(f"Failed to create contact for vendor '{vendor_name}'.")
                    raise Exception("Contact creation failed.")

            # Link the contact to the PO in the database
            self.db_util.link_contact_to_po(po_number, contact_id)
            self.logger.info(f"Linked contact ID {contact_id} to PO number '{po_number}'.")
            return contact_id
        except Exception as e:
            self.logger.error(f"Error in match_or_create_contact: {e}")
            raise

    def get_po_number_from_item(self, item_id: int) -> Any | None:
        """
        Retrieve the PO number from a specific item in Monday.com.

        Args:
            item_id (int): The ID of the item to query.

        Returns:
            str: The PO number if found, else None.
        """
        try:
            po_number, _ = self.monday_util.get_po_number_and_data(item_id)
            if po_number:
                self.logger.info(f"Retrieved PO number '{po_number}' for item ID {item_id}.")
                return po_number
            else:
                self.logger.warning(f"PO number not found for item ID {item_id}.")
                return None
        except Exception as e:
            self.logger.error(f"Error retrieving PO number for item ID {item_id}: {e}")
            return None

    def sync_main_items_from_monday_board(self):
        """
        Synchronize all main items (POs) from the Monday.com board to the local database.
        """
        try:
            self.logger.info(f"Fetching items from board {self.board_id}...")
            all_items = self.monday_api.fetch_all_items(self.board_id)
            self.logger.info(f"Total items fetched from board {self.board_id}: {len(all_items)}")

            for item in all_items:
                creation_item = self.db_util.prep_main_item_event_for_db_creation(item)
                if creation_item:
                    status = self.db_util.create_or_update_main_item_in_db(creation_item)
                    self.logger.info(f"Synced PO with pulse_id {creation_item.get('pulse_id')}: {status}")
        except Exception as e:
            self.logger.exception(f"Unexpected error during main items synchronization: {e}")

    def sync_sub_items_from_monday_board(self):
        """
        Synchronize all sub-items from the Monday.com board to the local database.
        Each sub-item is associated with a parent item (PO). This function fetches all sub-items,
        prepares them for the database, and updates the database accordingly.
        """
        try:
            self.logger.info(f"Fetching sub-items from board {self.subitem_board_id}...")
            all_subitems = self.monday_api.fetch_all_sub_items()
            self.logger.info(f"Total sub-items fetched from board {self.subitem_board_id}: {len(all_subitems)}")
        except Exception as e:
            self.logger.error(f"Error fetching sub-items from Monday.com: {e}")
            return

        try:
            orphan_count = 0
            for subitem in all_subitems:
                creation_item = self.db_util.prep_sub_item_event_for_db_creation(subitem)
                if not creation_item:
                    orphan_count += 1
                    self.logger.debug(f"Skipping sub-item with pulse_id {subitem.get('id')} due to missing parent.")
                    continue  # Skip orphan sub-items

                result = self.db_util.create_or_update_sub_item_in_db(creation_item)

                if not result:
                    self.logger.error(f"Failed to sync sub-item with pulse_id: {creation_item.get('pulse_id')}")
                    continue  # Skip to the next sub-item

                if result.get("status") == "Orphan":
                    orphan_count += 1
                    self.logger.debug(
                        f"Skipped orphan with pulse_id: {creation_item.get('pulse_id')}, "
                    )
                elif result.get("status") == "Created":
                    self.logger.info(
                        f"Successfully created sub-item with pulse_id: {creation_item.get('pulse_id')}")
                elif result.get("status") == "Updated":
                    self.logger.info(
                        f"Successfully updated sub-item with pulse_id: {creation_item.get('pulse_id')}"
                    )
                else:
                    self.logger.error(
                        f"Failed to sync sub-item with pulse_id: {creation_item.get('pulse_id')}. "
                        f"Error: {result.get('error')}"
                    )
        except Exception as e:
            self.logger.exception(f"Unexpected error while syncing sub-items to DB: {e}")
        self.logger.info("Sub-items synchronization completed successfully.")
        self.logger.info(f"Skipped {orphan_count} orphans out of {len(all_subitems)} sub-items")

    def sync_contacts_from_monday_board(self):
        """
        Synchronize all contacts from the Monday.com board to the local database.
        """
        try:
            self.logger.info(f"Fetching contacts from board {self.contact_board_id}...")
            all_contacts = self.monday_api.fetch_all_contacts(self.contact_board_id)
            self.logger.info(f"Total contacts fetched from board {self.contact_board_id}: {len(all_contacts)}")
        except Exception as e:
            self.logger.error(f"Error fetching contacts from Monday.com: {e}")
            return

        try:
            for contact in all_contacts:
                prepped_contact = self.monday_util.prep_contact_event_for_db_creation(contact)
                if prepped_contact:
                    status = self.db_util.create_or_update_contact_item_in_db(prepped_contact)
                    self.logger.info(f"Synced contact with pulse_id {prepped_contact.get('pulse_id')}: {status}")
        except Exception as e:
            self.logger.error(f"Error syncing contacts to DB: {e}")

        self.logger.info("Contacts synchronization completed successfully.")


monday_service = MondayService()
