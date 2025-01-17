import json
import logging
from typing import Any
import requests
from database.database_util import DatabaseOperations
from logger import setup_logging
from utilities.singleton import SingletonMeta
from utilities.config import Config
from files_monday.monday_util import monday_util
from files_monday.monday_database_util import monday_database_util
from files_monday.monday_api import monday_api

class MondayService(metaclass=SingletonMeta):

    def __init__(self):
        self.database_util = DatabaseOperations()
        if not hasattr(self, '_initialized'):
            self.logger = logging.getLogger('monday_logger')
            self.monday_util = monday_util
            self.db_util = monday_database_util
            self.monday_api = monday_api
            self.api_token = Config.MONDAY_API_TOKEN
            self.board_id = self.monday_util.PO_BOARD_ID
            self.subitem_board_id = self.monday_util.SUBITEM_BOARD_ID
            self.contact_board_id = self.monday_util.CONTACT_BOARD_ID
            self.api_url = self.monday_util.MONDAY_API_URL
            self.logger.info('[__init__] - Monday Service initialized')
            self._initialized = True

    def _make_request(self, query: str, variables: dict=None):
        """
        Internal method to make GraphQL requests to the Monday.com API.

        Args:
            query (str): The GraphQL query or mutation.
            variables (dict, optional): Variables for the GraphQL query.

        Returns:
            dict: The JSON response from the API.
        """
        headers = {'Authorization': self.api_token}
        response = requests.post(self.api_url, json={'query': query, 'variables': variables}, headers=headers)
        response.raise_for_status()
        return response.json()

    def update_po_status(self, pulse_id: int, status: str):
        """
        Update the status of a Purchase Order (PO) in Monday.com.

        Args:
            pulse_id (int): The ID of the PO item in Monday.com.
            status (str): The new status label to set.
        """
        query = '\n        mutation ($board_id: Int!, $item_id: Int!, $column_id: String!, $value: JSON!) {\n            change_column_value(\n                board_id: $board_id,\n                item_id: $item_id,\n                column_id: $column_id,\n                value: $value\n            ) {\n                id\n            }\n        }\n        '
        variables = {'board_id': int(self.board_id), 'item_id': pulse_id, 'column_id': self.monday_util.PO_STATUS_COLUMN_ID, 'value': json.dumps({'label': status})}
        try:
            self._make_request(query, variables)
            self.logger.info(f"[update_po_status] - Updated PO status for item ID {pulse_id} to '{status}'.")
        except requests.HTTPError as e:
            self.logger.error(f'[update_po_status] - Failed to update PO status for item ID {pulse_id}: {e}')
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
            contact = self.monday_util.find_contact_item_by_name(vendor_name)
            if contact:
                contact_id = contact['item_id']
                self.logger.info(f"[match_or_create_contact] - Found existing contact '{vendor_name}' with ID {contact_id}.")
            else:
                self.logger.info(f"[match_or_create_contact] - No existing contact found for '{vendor_name}'. Creating a new contact.")
                column_values = {self.monday_util.CONTACT_NAME: vendor_name, self.monday_util.CONTACT_EMAIL: 'vendor@example.com', self.monday_util.CONTACT_PHONE: '123-456-7890'}
                contact_id = self.monday_util.create_item(group_id='contacts_group_id', item_name=vendor_name, column_values=column_values)
                if not contact_id:
                    self.logger.error(f"[match_or_create_contact] - Failed to create contact for vendor '{vendor_name}'.")
                    raise Exception('Contact creation failed.')
            self.db_util.link_contact_to_po(po_number, contact_id)
            self.logger.info(f"[match_or_create_contact] - Linked contact ID {contact_id} to PO number '{po_number}'.")
            return contact_id
        except Exception as e:
            self.logger.error(f'[match_or_create_contact] - Error in match_or_create_contact: {e}')
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
            (po_number, _) = self.monday_util.get_po_number_and_data(item_id)
            if po_number:
                self.logger.info(f"[get_po_number_from_item] - Retrieved PO number '{po_number}' for item ID {item_id}.")
                return po_number
            else:
                self.logger.warning(f'[get_po_number_from_item] - PO number not found for item ID {item_id}.')
                return None
        except Exception as e:
            self.logger.error(f'[get_po_number_from_item] - Error retrieving PO number for item ID {item_id}: {e}')
            return None

    def sync_main_items_from_monday_board(self):
        """
        Synchronize all main items (POs) from the Monday.com board to the local database.
        """
        try:
            self.logger.info(f'[sync_main_items_from_monday_board] - Fetching items from board {self.board_id}...')
            all_items = self.monday_api.fetch_all_items(self.board_id)
            self.logger.info(f'[sync_main_items_from_monday_board] - Total items fetched from board {self.board_id}: {len(all_items)}')
            for item in all_items:
                creation_item = self.db_util.prep_main_item_event_for_db_creation(item)
                if creation_item:
                    status = self.db_util.create_or_update_main_item_in_db(creation_item)
                    self.logger.info(f"[sync_main_items_from_monday_board] - Synced PO with pulse_id {creation_item.get('pulse_id')}: {status}")
        except Exception as e:
            self.logger.exception(f'[sync_main_items_from_monday_board] - Unexpected error during main items synchronization: {e}')

    def sync_sub_items_from_monday_board(self):
        """
        Synchronize all sub-items from the Monday.com board to the local database.
        Each sub-item is associated with a parent item (PO). This function fetches all sub-items,
        prepares them for the database, and updates the database accordingly.
        """
        try:
            self.logger.info(f'[sync_sub_items_from_monday_board] - Fetching sub-items from board {self.subitem_board_id}...')
            all_subitems = self.monday_api.fetch_all_sub_items()
            self.logger.info(f'[sync_sub_items_from_monday_board] - Total sub-items fetched from board {self.subitem_board_id}: {len(all_subitems)}')
        except Exception as e:
            self.logger.error(f'[sync_sub_items_from_monday_board] - Error fetching sub-items from Monday.com: {e}')
            return
        try:
            orphan_count = 0
            for subitem in all_subitems:
                creation_item = self.db_util.prep_sub_item_event_for_db_creation(subitem)
                if not creation_item:
                    orphan_count += 1
                    self.logger.debug(f"[sync_sub_items_from_monday_board] - Skipping sub-item with pulse_id {subitem.get('id')} due to missing parent.")
                    continue
                result = self.db_util.create_or_update_sub_item_in_db(creation_item)
                if not result:
                    self.logger.error(f"[sync_sub_items_from_monday_board] - Failed to sync sub-item with pulse_id: {creation_item.get('pulse_id')}")
                    continue
                if result.get('status') == 'Orphan':
                    orphan_count += 1
                    self.logger.debug(f"[sync_sub_items_from_monday_board] - Skipped orphan with pulse_id: {creation_item.get('pulse_id')}, ")
                elif result.get('status') == 'Created':
                    self.logger.info(f"[sync_sub_items_from_monday_board] - Successfully created sub-item with pulse_id: {creation_item.get('pulse_id')}")
                elif result.get('status') == 'Updated':
                    self.logger.info(f"[sync_sub_items_from_monday_board] - Successfully updated sub-item with pulse_id: {creation_item.get('pulse_id')}")
                else:
                    self.logger.error(f"[sync_sub_items_from_monday_board] - Failed to sync sub-item with pulse_id: {creation_item.get('pulse_id')}. Error: {result.get('error')}")
        except Exception as e:
            self.logger.exception(f'[sync_sub_items_from_monday_board] - Unexpected error while syncing sub-items to DB: {e}')
        self.logger.info('[sync_sub_items_from_monday_board] - Sub-items synchronization completed successfully.')
        self.logger.info(f'[sync_sub_items_from_monday_board] - Skipped {orphan_count} orphans out of {len(all_subitems)} sub-items')

    def sync_contacts_from_monday_board(self):
        """
        Synchronize all contacts from the Monday.com board to the local database.
        """
        try:
            self.logger.info(f'[sync_contacts_from_monday_board] - Fetching contacts from board {self.contact_board_id}...')
            all_contacts = self.monday_api.fetch_all_contacts()
            self.logger.info(f'[sync_contacts_from_monday_board] - Total contacts fetched from board {self.contact_board_id}: {len(all_contacts)}')
        except Exception as e:
            self.logger.error(f'[sync_contacts_from_monday_board] - Error fetching contacts from Monday.com: {e}')
            return
        try:
            for contact in all_contacts:
                monday_fields = self.monday_api.extract_monday_contact_fields(contact)
                tax_number_int = None
                if monday_fields['tax_number_str']:
                    tax_number_int = self.database_util.parse_tax_number(monday_fields['tax_number_str'])
                vendor_status = monday_fields['vendor_status']
                if vendor_status not in ['PENDING', 'TO VERIFY', 'APPROVED', 'ISSUE']:
                    vendor_status = 'PENDING'
                try:
                    existing_contact = self.database_util.find_contact_by_name(contact_name=contact['name'])
                    if not existing_contact:
                        db_contact = self.database_util.create_contact(name=contact['name'], pulse_id=monday_fields['pulse_id'], phone=monday_fields['phone'], email=monday_fields['email'], address_line_1=monday_fields['address_line_1'], address_line_2=monday_fields['address_line_2'], city=monday_fields['city'], zip=monday_fields['zip_code'], region=monday_fields['region'], country=monday_fields['country'], tax_type=monday_fields['tax_type'], tax_number=tax_number_int, payment_details=monday_fields['payment_details'], vendor_status=vendor_status, tax_form_link=monday_fields['tax_form_link'])
                    else:
                        db_contact = self.database_util.update_contact(contact_id=existing_contact['id'], name=contact['name'], pulse_id=monday_fields['pulse_id'], phone=monday_fields['phone'], email=monday_fields['email'], address_line_1=monday_fields['address_line_1'], address_line_2=monday_fields['address_line_2'], city=monday_fields['city'], zip=monday_fields['zip_code'], region=monday_fields['region'], country=monday_fields['country'], tax_type=monday_fields['tax_type'], tax_number=tax_number_int, payment_details=monday_fields['payment_details'], vendor_status=vendor_status, tax_form_link=monday_fields['tax_form_link'])
                    self.logger.info(f"[sync_contacts_from_monday_board] - Synced {contact['name']}")
                except Exception as e:
                    self.logger.error(f'[sync_contacts_from_monday_board] - Error adding contact to DB: {e}')
        except Exception as e:
            self.logger.error(f'[sync_contacts_from_monday_board] - Error syncing contacts to DB: {e}')
        self.logger.info('[sync_contacts_from_monday_board] - Contacts synchronization completed successfully.')
monday_service = MondayService()