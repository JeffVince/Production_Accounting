# utilities/monday_util.py

import json
import logging
import os
from datetime import datetime

import requests
from dotenv import load_dotenv


class MondayUtil:
    """
    A utility class for interacting with the Monday.com API.
    Encapsulates methods for creating, updating, fetching, and deleting items, subitems, and contacts.
    """

    # --------------------- CONSTANTS ---------------------

    MONDAY_API_URL = 'https://api.monday.com/v2'
    headers = ''

    ACTUALS_BOARD_ID = "7858669780"
    PO_BOARD_ID = "2562607316"  # Ensure this is a string
    CONTACT_BOARD_ID = '2738875399'
    SUBITEM_BOARD_ID = ''

    # Column IDs for POs
    PO_PROJECT_ID_COLUMN = 'project_id'  # Monday.com Project ID column ID
    PO_NUMBER_COLUMN = 'numbers08'  # Monday.com PO Number column ID
    PO_TAX_COLUMN_ID = 'dup__of_invoice'  # TAX link column ID
    PO_DESCRIPTION_COLUMN_ID = 'text6'  # Item Description column ID
    PO_CONTACT_CONNECTION_COLUMN_ID = 'connect_boards1'
    PO_FOLDER_LINK_COLUMN_ID = 'dup__of_tax_form__1'
    PO_STATUS_COLUMN_ID = 'status'
    PO_PRODUCER_COLUMN_ID = 'people'

    # Column IDs for Subitems
    SUBITEM_NOTES_COLUMN_ID = 'payment_notes__1'
    SUBITEM_STATUS_COLUMN_ID = 'status4'
    SUBITEM_ID_COLUMN_ID = 'text0'  # Receipt / Invoice column ID
    SUBITEM_DESCRIPTION_COLUMN_ID = 'text98'  # Description column ID
    SUBITEM_QUANTITY_COLUMN_ID = 'numbers0'  # Quantity column ID
    SUBITEM_RATE_COLUMN_ID = 'numbers9'  # Rate column ID
    SUBITEM_DATE_COLUMN_ID = 'date'  # Date column ID
    SUBITEM_DUE_DATE_COLUMN_ID = 'date_1__1'
    SUBITEM_ACCOUNT_NUMBER_COLUMN_ID = 'dropdown'  # Account Number column ID
    SUBITEM_LINK_COLUMN_ID = 'link'  # Link column ID

    # Column IDs for Contacts
    CONTACT_NAME = 'name'
    CONTACT_PHONE = 'phone'
    CONTACT_EMAIL = 'email'
    CONTACT_ADDRESS_LINE_1 = 'text1'
    CONTACT_ADDRESS_CITY = 'text3'
    CONTACT_ADDRESS_ZIP = 'text84'
    CONTACT_ADDRESS_COUNTRY = 'text6'
    CONTACT_TAX_TYPE = 'text14'
    CONTACT_TAX_NUMBER = 'text2'
    CONTACT_PAYMENT_DETAILS = 'status__1'

    # Mapping Monday.com Columns to DB Fields
    MAIN_ITEM_COLUMN_ID_TO_DB_FIELD = {
        # Monday Subitem Columns -> DB DetailItem Columns
        "id": "pulse_id",
        PO_PROJECT_ID_COLUMN: "project_id",
        PO_NUMBER_COLUMN: "po_number",
        PO_TAX_COLUMN_ID: "tax_form_link",
        PO_DESCRIPTION_COLUMN_ID: "description",
        PO_CONTACT_CONNECTION_COLUMN_ID: "contact_id",
        PO_FOLDER_LINK_COLUMN_ID: "folder_link",
        PO_STATUS_COLUMN_ID: "state",
        PO_PRODUCER_COLUMN_ID: "producer"
    }

    SUB_ITEM_COLUMN_ID_TO_DB_FIELD = {
        # Monday Subitem Columns -> DB DetailItem Columns
        SUBITEM_STATUS_COLUMN_ID: "state",  # Maps to the state ENUM in the DB
        SUBITEM_ID_COLUMN_ID: "detail_item_number",  # Maps to file_link in the DB
        SUBITEM_DESCRIPTION_COLUMN_ID: "description",  # Maps to description in the DB
        SUBITEM_QUANTITY_COLUMN_ID: "quantity",  # Maps to quantity in the DB
        SUBITEM_RATE_COLUMN_ID: "rate",  # Maps to rate in the DB
        SUBITEM_DATE_COLUMN_ID: "transaction_date",  # Maps to transaction_date in the DB
        SUBITEM_ACCOUNT_NUMBER_COLUMN_ID: "account_number_id",  # Maps to account_number_id in the DB
        SUBITEM_LINK_COLUMN_ID: "file_link"  # Maps to file_link for link references in the DB
    }

    CONTACT_COLUMN_ID_TO_DB_FIELD = {
        # Monday.com Columns -> DB Contact Model Fields
        CONTACT_PHONE: 'phone',
        CONTACT_EMAIL: 'email',
        CONTACT_ADDRESS_LINE_1: 'address_line_1',
        CONTACT_ADDRESS_CITY: 'city',
        CONTACT_ADDRESS_ZIP: 'zip',
        CONTACT_TAX_TYPE: 'tax_type',
        CONTACT_TAX_NUMBER: 'tax_ID',
        CONTACT_PAYMENT_DETAILS: 'payment_details'
    }

    # Mapping of column types to their handlers
    COLUMN_TYPE_HANDLERS = {
        "dropdown": "handle_dropdown_column",
        "default": "handle_default_column",
        "date": "handle_date_column",
        "color": "handle_status_column",
        "link": "handle_link_column"
    }

    # --------------------- INITIALIZATION ---------------------

    def __init__(self):
        # Set up logging
        self.logger = logging.getLogger(self.__class__.__name__)
        logging.basicConfig(level=logging.DEBUG)
        # Load environment variables
        load_dotenv()
        # Initialize headers for API requests
        self.monday_api_token = os.getenv("MONDAY_API_TOKEN")
        if not self.monday_api_token:
            self.logger.error("Monday API Token not found. Please set it in the environment variables.")
            raise EnvironmentError("Missing MONDAY_API_TOKEN")
        self.headers = {
            'Authorization': self.monday_api_token,
            'Content-Type': 'application/json',
            'API-Version': '2023-10'
        }
        self.SUBITEM_BOARD_ID = self.get_subitem_board_id(self.PO_BOARD_ID)

        # Initialize MondayDatabaseUtil instance

    # --------------------- HELPER METHODS ---------------------

    def _handle_date_column(self, event):
        """
        Date handler for columns, extracting a single date.
        """
        return event.get('value', {}).get('date', {})

    def _handle_link_column(self, event):
        """
        Handles dropdown column type and extracts chosen values.
        """
        try:
            return event.get('value', {}).get('url', {})
        except Exception as e:
            self.logger.warning("Setting Account ID to None because of unexpected Monday Account Value.")
            return None

    def _handle_dropdown_column(self, event):
        """
        Handles dropdown column type and extracts chosen values.
        """
        try:
            line_number = event.get('value', {}).get('chosenValues', [])[0].get("name")
            return self.db_util.get_aicp_code_surrogate_id(line_number)
        except Exception as e:
            self.logger.warning("Setting Account ID to None because of unexpected Monday Account Value.")
            return None

    def _handle_default_column(self, event):
        """
        Default handler for columns, extracting a single text label.
        """
        return event.get('value', {}).get('value', {})

    def _handle_status_column(self, event):
        return event.get('value', {}).get('label', {}).get('text')


    def _get_column_handler(self, column_type):
        """
        Retrieves the appropriate handler method based on the column type.
        """
        handler_name = self.COLUMN_TYPE_HANDLERS.get(column_type, "handle_default_column")
        return getattr(self, f"_{handler_name}")

    # --------------------- MAIN ITEM METHODS ---------------------

    def prep_main_item_event_for_db_creation(self, event):
        """
        Prepares the Monday event payload into a database creation item.

        Args:
            event (dict): The Monday event payload.

        Returns:
            dict: A dictionary representing the database creation item.
        """
        item = event
        self.logger.debug(f"MAIN ITEM FOR CREATION: {item}")

        # Prepare the database creation item
        creation_item = {
            "pulse_id": int(item["id"]),  # Monday subitem ID
        }

        po_type = "Vendor"  # Default po_type value

        for column in item.get("column_values", []):
            column_id = column.get("id")
            db_field = self.MAIN_ITEM_COLUMN_ID_TO_DB_FIELD.get(column_id)

            if db_field:
                # Map the corresponding value to the database field
                value = column.get("text") or column.get("value")

                # Handle cases where value is a JSON string
                if isinstance(value, str):
                    try:
                        value = json.loads(value)
                    except (ValueError, TypeError):
                        pass

                # Special handling for specific fields
                if db_field == "contact_id" and isinstance(value, dict):
                    # Extract the first linkedPulseId if present
                    linked_pulse_ids = value.get("linkedPulseIds", [])
                    value = linked_pulse_ids[0].get("linkedPulseId") if linked_pulse_ids else None

                # Check status to determine po_type
                if db_field == "state" and value == "CC / PC":
                    po_type = "CC / PC"

                # Assign processed value
                creation_item[db_field] = value

        # Add po_type to the creation item
        creation_item["po_type"] = po_type

        self.logger.debug(f"Prepared creation item: {creation_item}")
        return creation_item

    # --------------------- SUBITEM METHODS ---------------------

    def prep_sub_item_event_for_db_change(self, event):
        """
        Prepares the Monday event into a database change item.

        Args:
            event (dict): The Monday event payload.

        Returns:
            dict: A prepared database change item.
        """
        column_id = event.get('columnId')
        column_type = event.get('columnType', 'default')  # Use 'default' if columnType is not provided

        # Determine the database field for the column
        db_field = self.SUB_ITEM_COLUMN_ID_TO_DB_FIELD.get(column_id)
        if not db_field:
            raise ValueError(f"Column ID '{column_id}' is not mapped to a database field.")

        # Get the appropriate handler for the column type
        handler = self._get_column_handler(column_type)

        # Process the value using the appropriate handler
        new_value = handler(event)

        # Construct the change item
        change_item = {
            "pulse_id": int(event.get('pulseId')),  # Monday pulse ID (subitem ID)
            "db_field": db_field,  # Corresponding DB field
            "new_value": new_value,  # Extracted new value
            "changed_at": datetime.fromtimestamp(event.get('changedAt', 0)),
        }

        self.logger.debug(f"Prepared change item: {change_item}")
        return change_item

    def prep_sub_item_event_for_db_creation(self, event):
        """
        Prepares the Monday event payload into a database creation item.

        Args:
            event (dict): The Monday event payload.

        Returns:
            dict: A dictionary representing the database creation item, or None if orphan.
        """
        item = event
        self.logger.debug(f"ITEM DATA: {item}")
        parent_id = item["parent_item"]['id']
        # Remove orphan detail items
        if not parent_id:
            self.logger.warning("Orphan detail item detected; skipping creation.")
            return None

        # Prepare the database creation item
        creation_item = {
            "pulse_id": int(item["id"]),
            "parent_id": item["parent_item"]["id"]
        }

        for column in item.get("column_values", []):
            column_id = column.get("id")
            db_field = self.SUB_ITEM_COLUMN_ID_TO_DB_FIELD.get(column_id)
            if db_field:
                value = column.get("text") or column.get("value")
                if isinstance(value, str):
                    try:
                        value = json.loads(value)
                    except (ValueError, TypeError):
                        pass
                creation_item[db_field] = value

        self.logger.debug(f"Prepared subitem creation item: {creation_item}")
        return creation_item

    # --------------------- CONTACT METHODS ---------------------

    def prep_contact_event_for_db_creation(self, event):
        """
        Prepares the Monday event payload into a database creation item.

        Args:
            event (dict): The Monday event payload.

        Returns:
            dict: A dictionary representing the database creation item.
        """
        if not event or "id" not in event:
            self.logger.error(f"Invalid event structure: {event}")
            raise ValueError("Invalid event structure. Ensure the event payload is properly formatted.")

        # Extract the first item from the event payload (assuming a single contact event)
        item = event
        self.logger.debug(f"CONTACT ITEM DATA: {item}")

        # Prepare the database creation item
        creation_item = {
            "pulse_id": int(item["id"]),
            "name": item.get('name')
        }

        for column in item.get("column_values", []):
            column_id = column.get("id")
            db_field = self.CONTACT_COLUMN_ID_TO_DB_FIELD.get(column_id)

            if db_field:
                # Map the corresponding value to the database field
                value = column.get("text") or column.get("value")

                # Handle cases where value is a JSON string
                if isinstance(value, str):
                    try:
                        value = json.loads(value)
                    except (ValueError, TypeError):
                        pass

                creation_item[db_field] = value

        self.logger.debug(f"Prepared contact creation item: {creation_item}")
        return creation_item

    # --------------------- ITEM METHODS ---------------------

    def create_item(self, group_id, item_name, column_values):
        """
        Creates a new item in Monday.com within the specified group.

        Args:
            group_id (str): The ID of the group where the item will be created.
            item_name (str): The name of the new item.
            column_values (dict): A dictionary of column IDs and their corresponding values.

        Returns:
            str or None: The ID of the created item if successful, else None.
        """
        query = '''
        mutation ($board_id: ID!, $group_id: String!, $item_name: String!, $column_values: JSON!) {
            create_item(
                board_id: $board_id,
                group_id: $group_id,
                item_name: $item_name,
                column_values: $column_values
            ) {
                id
                name
            }
        }
        '''

        # Serialize `column_values` to JSON string format
        serialized_column_values = json.dumps(column_values)

        variables = {
            'board_id': self.PO_BOARD_ID,
            'group_id': group_id,
            'item_name': item_name,
            'column_values': serialized_column_values  # Pass serialized values here
        }
        self.logger.info(f"Creating item with variables: {variables}")

        response = requests.post(self.MONDAY_API_URL, headers=self.headers,
                                 json={'query': query, 'variables': variables})
        data = response.json()

        if response.status_code == 200:
            if 'data' in data and 'create_item' in data['data']:
                item_id = data['data']['create_item']['id']
                self.logger.info(f"Created new item '{item_name}' with ID {item_id}")
                return item_id
            elif 'errors' in data:
                self.logger.error(f"Error creating item in Monday.com: {data['errors']}")
                return None
            else:
                self.logger.error(f"Unexpected response structure: {data}")
                return None
        else:
            self.logger.error(f"HTTP Error {response.status_code}: {response.text}")
            return None

    def update_item_columns(self, item_id, column_values):
        """
        Updates multiple columns of an item in Monday.com.

        Args:
            item_id (str): The ID of the item to update.
            column_values (dict): A dictionary of column IDs and their corresponding values.

        Returns:
            bool: True if the update was successful, False otherwise.
        """
        # Convert the column values to JSON
        column_values_json = json.dumps(column_values).replace('"', '\\"')

        query = f'''
        mutation {{
            change_multiple_column_values(
                board_id: {self.PO_BOARD_ID},
                item_id: {item_id},
                column_values: "{column_values_json}"
            ) {{
                id
            }}
        }}
        '''

        self.logger.info(f"Updating item {item_id} with columns: {column_values}")

        response = requests.post(self.MONDAY_API_URL, headers=self.headers, json={'query': query})
        data = response.json()

        if response.status_code == 200:
            if 'data' in data:
                self.logger.info(f"Successfully updated item {item_id} in Monday.com.")
                return True
            elif 'errors' in data:
                self.logger.error(f"Error updating item in Monday.com: {data['errors']}")
                return False
            else:
                self.logger.error(f"Unexpected response structure: {data}")
                return False
        else:
            self.logger.error(f"HTTP Error {response.status_code}: {response.text}")
            return False

    # --------------------- SUBITEM METHODS ---------------------

    def subitem_column_values_formatter(self, notes=None, status=None, file_id=None, description=None,
                                        quantity=None, rate=None, date=None, due_date=None,
                                        account_number=None, link=None):
        """
        Formats the column values for creating or updating a subitem.

        Args:
            notes (str, optional): Payment notes.
            status (str, optional): Status of the subitem.
            file_id (str, optional): Receipt or invoice ID.
            description (str, optional): Description of the subitem.
            quantity (float, optional): Quantity value.
            rate (float, optional): Rate value.
            date (str, optional): Date in 'YYYY-MM-DD' format.
            due_date (str, optional): Due date in 'YYYY-MM-DD' format.
            account_number (str, optional): Account number.
            link (str, optional): URL link.

        Returns:
            dict: A dictionary of subitem column IDs and their corresponding values.
        """

        # Mapping of account numbers to Monday IDs
        account_number_to_id_map = {
            "5300": 1,
            "5000": 2,
            "6040": 3,
            "5330": 4
        }

        column_values = {}
        if notes:
            column_values[self.SUBITEM_NOTES_COLUMN_ID] = notes
        if status:
            column_values[self.SUBITEM_STATUS_COLUMN_ID] = {'label': status}
        if file_id:
            column_values[self.SUBITEM_ID_COLUMN_ID] = file_id
        if description:
            column_values[self.SUBITEM_DESCRIPTION_COLUMN_ID] = description
        if quantity is not None:
            column_values[self.SUBITEM_QUANTITY_COLUMN_ID] = quantity
        if rate is not None:
            column_values[self.SUBITEM_RATE_COLUMN_ID] = rate
        if date:
            column_values[self.SUBITEM_DATE_COLUMN_ID] = {'date': date}
        if due_date:
            column_values[self.SUBITEM_DUE_DATE_COLUMN_ID] = {'date': due_date}
        if account_number:
            mapped_id = account_number_to_id_map.get(str(account_number))
            column_values[self.SUBITEM_ACCOUNT_NUMBER_COLUMN_ID] = {'ids': [str(mapped_id)]} if mapped_id else {}
        if link:
            column_values[self.SUBITEM_LINK_COLUMN_ID] = {'url': link, 'text': 'Link'}

        self.logger.info(f"Formatted subitem column values: {column_values}")
        return column_values

    def create_subitem(self, parent_item_id, subitem_name, column_values):
        """
        Creates a subitem in Monday.com under a given parent item.

        Args:
            parent_item_id (str): The ID of the parent item to attach the subitem to.
            subitem_name (str): The name of the subitem.
            column_values (dict): A dictionary of column IDs and their corresponding values.

        Returns:
            str or None: The ID of the created subitem if successful, else None.
        """
        # Remove any None values from the column values
        column_values = {k: v for k, v in column_values.items() if v is not None}

        # Convert column_values to JSON string and escape it for GraphQL
        column_values_json = json.dumps(column_values).replace('"', '\\"')

        # GraphQL mutation for creating the subitem
        query = f'''
        mutation {{
            create_subitem (
                parent_item_id: "{parent_item_id}",
                item_name: "{subitem_name}",
                column_values: "{column_values_json}"
            ) {{
                id
            }}
        }}
        '''

        self.logger.info(f"Creating subitem under parent {parent_item_id} with name '{subitem_name}'.")

        # Execute the request and handle the response
        response = requests.post(self.MONDAY_API_URL, headers=self.headers, json={'query': query})
        data = response.json()

        # Process response
        if response.status_code == 200:
            if 'data' in data and 'create_subitem' in data['data']:
                subitem_id = data['data']['create_subitem']['id']
                self.logger.info(f"Created subitem with ID {subitem_id}")
                return subitem_id
            elif 'errors' in data:
                self.logger.error(f"Error creating subitem in Monday.com: {data['errors']}")
                return None
            else:
                self.logger.error(f"Unexpected response structure: {data}")
                return None
        else:
            self.logger.error(f"HTTP Error {response.status_code}: {response.text}")
            return None

    def update_subitem_columns(self, subitem_id, column_values):
        """
        Updates the specified columns of a subitem in Monday.com.

        Args:
            subitem_id (str): The ID of the subitem to update.
            column_values (dict): A dictionary where keys are column IDs and values are the new values for those columns.

        Returns:
            bool: True if the update was successful, False otherwise.
        """
        # Convert the column values to a JSON string and escape double quotes for GraphQL
        column_values_json = json.dumps(column_values).replace('"', '\\"')

        # GraphQL mutation to update the subitem's columns
        mutation = f'''
        mutation {{
            change_multiple_column_values(
                board_id: {self.SUBITEM_BOARD_ID},
                item_id: {subitem_id},
                column_values: "{column_values_json}"
            ) {{
                id
            }}
        }}
        '''

        self.logger.info(f"Updating subitem {subitem_id} with columns: {column_values}")

        # Send the GraphQL request to update the subitem's columns
        response = requests.post(self.MONDAY_API_URL, headers=self.headers, json={'query': mutation})
        data = response.json()

        if response.status_code == 200:
            if 'data' in data:
                self.logger.info(f"Successfully updated subitem {subitem_id} in Monday.com.")
                return True
            elif 'errors' in data:
                self.logger.error(f"Error updating subitem in Monday.com: {data['errors']}")
                return False
            else:
                self.logger.error(f"Unexpected response structure: {data}")
                return False
        else:
            self.logger.error(f"HTTP Error {response.status_code}: {response.text}")
            return False

    # --------------------- CONTACT METHODS ---------------------

    def link_contact_to_po_item(self, po_item_id, contact_item_id):
        """
        Links a contact item from the Contacts board to a PO item in the PO board using the Connect Boards column.

        Args:
            po_item_id (str): The ID of the PO item in the PO board.
            contact_item_id (str): The ID of the contact item in the Contacts board.

        Returns:
            bool: True if the link was successful, False otherwise.
        """
        # Define the Connect Boards column ID in the PO board
        connect_boards_column_id = self.PO_CONTACT_CONNECTION_COLUMN_ID

        # Prepare the value for the Connect Boards column
        column_value = {
            "item_ids": [contact_item_id]
        }

        # Convert the column value to a JSON string
        column_value_json = json.dumps(column_value).replace('"', '\\"')

        # GraphQL mutation to update the Connect Boards column
        mutation = f'''
        mutation {{
            change_column_value(
                board_id: {self.PO_BOARD_ID},
                item_id: {po_item_id},
                column_id: "{connect_boards_column_id}",
                value: "{column_value_json}"
            ) {{
                id
            }}
        }}
        '''

        self.logger.info(f"Linking contact {contact_item_id} to PO item {po_item_id}.")

        # Send the GraphQL request to the Monday.com API
        response = requests.post(self.MONDAY_API_URL, headers=self.headers, json={'query': mutation})
        data = response.json()

        # Check if the request was successful
        if response.status_code == 200:
            if 'data' in data and 'change_column_value' in data['data']:
                self.logger.info(f"Successfully linked contact item {contact_item_id} to PO item {po_item_id}.")
                return True
            elif 'errors' in data:
                self.logger.error(f"Error linking contact to PO item in Monday.com: {data['errors']}")
                return False
            else:
                self.logger.error(f"Unexpected response structure: {data}")
                return False
        else:
            self.logger.error(f"HTTP Error {response.status_code}: {response.text}")
            return False

    # --------------------- FETCH METHODS ---------------------

    def get_subitems_column_id(self, parent_board_id):
        """
        Retrieves the column ID for subitems in a given board.

        Args:
            parent_board_id (str): The ID of the parent board.

        Returns:
            str: The column ID for subitems.

        Raises:
            Exception: If the subitems column is not found or the API request fails.
        """
        query = f'''
        query {{
            boards(ids: {parent_board_id}) {{
                columns {{
                    id
                    type
                }}
            }}
        }}
        '''
        response = requests.post(self.MONDAY_API_URL, headers=self.headers, json={'query': query})
        data = response.json()

        if response.status_code == 200 and 'data' in data:
            columns = data['data']['boards'][0]['columns']
            for column in columns:
                if column['type'] == 'subtasks':
                    self.logger.info(f"Found subitems column ID: {column['id']}")
                    return column['id']
            raise Exception("Subitems column not found.")
        else:
            raise Exception(f"Failed to retrieve columns: {response.text}")

    def get_subitem_board_id(self, parent_board_id):
        """
        Retrieves the subitem board ID for a given parent board.

        Args:
            parent_board_id (str): The ID of the parent board.

        Returns:
            str: The subitem board ID.

        Raises:
            Exception: If the subitem board ID cannot be retrieved.
        """
        subitems_column_id = self.get_subitems_column_id(parent_board_id)
        query = f'''
        query {{
            boards(ids: {parent_board_id}) {{
                columns(ids: "{subitems_column_id}") {{
                    settings_str
                }}
            }}
        }}
        '''
        response = requests.post(self.MONDAY_API_URL, headers=self.headers, json={'query': query})
        data = response.json()

        if response.status_code == 200 and 'data' in data:
            settings_str = data['data']['boards'][0]['columns'][0]['settings_str']
            settings = json.loads(settings_str)
            subitem_board_id = settings['boardIds'][0]
            self.logger.info(f"Retrieved subitem board ID: {subitem_board_id}")
            return subitem_board_id
        else:
            raise Exception(f"Failed to retrieve subitem board ID: {response.text}")

    def get_po_number_and_data(self, item_id):
        """
        Fetches the PO number and item data for a given item ID.

        Args:
            item_id (str): The ID of the item.

        Returns:
            tuple: (po_number, item_data) if successful, else (None, None).
        """
        query = f'''
        query {{
            items(ids: {item_id}) {{
                id
                name
                column_values {{
                    id
                    text
                    value
                }}
            }}
        }}
        '''
        response = requests.post(self.MONDAY_API_URL, headers=self.headers, json={'query': query})
        data = response.json()

        if response.status_code == 200 and 'data' in data:
            items = data['data']['items']
            if items:
                item = items[0]
                po_number = None
                for col in item.get('column_values', []):
                    if col['id'] == self.PO_NUMBER_COLUMN:  # Replace with your actual PO Number column ID
                        po_number = col.get('text')
                        break
                self.logger.info(f"Fetched PO number: {po_number} for Item ID: {item_id}")
                return po_number, item
        self.logger.error(f"Failed to fetch item data for Item ID {item_id}: {data.get('errors')}")
        return None, None

    def find_item_by_project_and_po(self, project_id, po_number):
        """
        Finds an item in Monday.com based on the provided Project ID and PO Number.

        Args:
            project_id (str): The Project ID to match.
            po_number (str): The PO Number to match.

        Returns:
            str or None: The ID of the matched item if found, otherwise None.

        Raises:
            Exception: If the GraphQL query fails or if there's an HTTP error.
        """
        # Prepare the GraphQL columns argument to filter items by the specific Project ID
        columns_arg = json.dumps([{
            "column_id": self.PO_PROJECT_ID_COLUMN,
            "column_values": [project_id]
        }])

        # GraphQL query to retrieve items filtered by Project ID
        query = f'''
        query {{
            items_page_by_column_values(
                board_id: {self.PO_BOARD_ID},
                columns: {columns_arg},
                limit: 100
            ) {{
                items {{
                    id
                    name
                    column_values {{
                        id
                        value
                    }}
                }}
            }}
        }}
        '''

        self.logger.info(f"Searching for item with Project ID '{project_id}' and PO Number '{po_number}'.")

        # Send the GraphQL request to the Monday.com API
        response = requests.post(self.MONDAY_API_URL, headers=self.headers, json={'query': query})
        data = response.json()

        # Check if the request was successful
        if response.status_code == 200 and 'data' in data:
            items = data['data']['items_page_by_column_values'].get('items', [])
            for item in items:
                for column in item.get('column_values', []):
                    if column['id'] == self.PO_NUMBER_COLUMN:
                        # Extract and parse the column value (expected to be JSON-encoded)
                        value = column.get('value', '')
                        try:
                            parsed_value = json.loads(value)
                        except json.JSONDecodeError:
                            parsed_value = value.strip('"')  # Fallback if not JSON

                        # Check if the parsed value matches the PO number
                        if parsed_value == po_number:
                            self.logger.info(f"Found item with PO Number '{po_number}': ID {item['id']}")
                            return item['id']

            self.logger.info(f"No item found with Project ID '{project_id}' and PO Number '{po_number}'.")
            return None
        elif 'errors' in data:
            self.logger.error(f"Error fetching items from Monday.com: {data['errors']}")
            raise Exception("GraphQL query error")
        else:
            self.logger.error(f"Unexpected response structure: {data}")
            raise Exception("Unexpected GraphQL response")

    def get_group_id_by_project_id(self, project_id):
        """
        Retrieves the group ID in Monday.com based on the project ID.

        Args:
            project_id (str): The project ID to match.

        Returns:
            str or None: The group ID if found, otherwise None.
        """
        query = f'''
        query {{
            boards(ids: {self.PO_BOARD_ID}) {{
                groups {{
                    id
                    title
                }}
            }}
        }}
        '''

        response = requests.post(self.MONDAY_API_URL, headers=self.headers, json={'query': query})
        data = response.json()

        if response.status_code == 200 and 'data' in data:
            groups = data['data']['boards'][0]['groups']
            for group in groups:
                title_prefix = group['title'][:len(project_id)]
                self.logger.debug(f"Comparing group title prefix '{title_prefix}' with prefix '{project_id}'")
                if title_prefix == project_id:
                    self.logger.info(f"Found matching group: {group['title']} with ID {group['id']}")
                    return group['id']
            self.logger.error(f"No group found with title prefix '{project_id}'")
            return None
        elif 'errors' in data:
            self.logger.error(f"Error fetching groups from Monday.com: {data['errors']}")
            return None
        else:
            self.logger.error(f"Unexpected response structure: {data}")
            return None

    def find_subitem_by_invoice_or_receipt_number(self, parent_item_id, invoice_receipt_number=None):
        """
        Finds a subitem under a parent item in Monday.com based on the provided invoice number or receipt number.

        Args:
            parent_item_id (str): The ID of the parent item (PO item).
            invoice_receipt_number (str, optional): The invoice or receipt number to search for.

        Returns:
            list or None: The list of matched subitems if found, otherwise None.

        Raises:
            Exception: If the GraphQL query fails or if there's an HTTP error.
        """
        # Validate that at least one of invoice_number or receipt_number is provided
        if not invoice_receipt_number:
            raise ValueError("Either invoice_number or receipt_number must be provided.")

        # GraphQL query to retrieve subitems under the parent item
        query = f'''
        query {{
            items(ids: {parent_item_id}) {{
                subitems {{
                    id
                    name
                    column_values {{
                        id
                        text
                    }}
                }}
            }}
        }}
        '''

        self.logger.info(
            f"Searching for subitems under parent item {parent_item_id} matching '{invoice_receipt_number}'.")

        # Send the GraphQL request to the Monday.com API
        response = requests.post(self.MONDAY_API_URL, headers=self.headers, json={'query': query})
        data = response.json()

        # Check if the request was successful
        if response.status_code == 200 and 'data' in data:
            subitems = data['data']['items'][0].get('subitems', [])
            self.logger.debug(f"Retrieved subitems: {json.dumps(subitems, indent=2)}")

            # Iterate through each subitem to find a match
            for subitem in subitems:
                for column in subitem.get('column_values', []):
                    if column['id'] == self.SUBITEM_ID_COLUMN_ID:
                        # Extract the text value of the column
                        text_value = column.get('text', '').strip()

                        # Check if the text value matches the provided invoice or receipt number
                        if text_value == invoice_receipt_number:
                            self.logger.info(f"Found subitem with ID {subitem['id']} matching invoice/receipt number.")
                            return subitems

            self.logger.info(
                f"No subitem found under parent item {parent_item_id} matching '{invoice_receipt_number}'.")
            return None
        elif 'errors' in data:
            self.logger.error(f"Error fetching subitems from Monday.com: {data['errors']}")
            raise Exception("GraphQL query error")
        else:
            self.logger.error(f"Unexpected response structure: {data}")
            raise Exception("Unexpected GraphQL response")

    def find_all_po_subitems(self, parent_item_id):
        """
        Retrieves all subitems for a specified parent item.

        Args:
            parent_item_id (str): The ID of the parent item (PO item).

        Returns:
            list: A list of dictionaries, each representing a subitem with its details.
        """
        query = '''
        query ($parent_item_id: [ID!]) {
            items(ids: $parent_item_id) {
                subitems {
                    id
                    name
                    column_values {
                        id
                        text
                    }
                }
            }
        }
        '''

        variables = {'parent_item_id': [parent_item_id]}
        self.logger.info(f"Fetching all subitems for parent item {parent_item_id}.")

        response = requests.post(self.MONDAY_API_URL, headers=self.headers,
                                 json={'query': query, 'variables': variables})
        data = response.json()

        if response.status_code == 200 and 'data' in data:
            subitems = data['data']['items'][0].get('subitems', [])
            self.logger.debug(f"Retrieved all subitems: {json.dumps(subitems, indent=2)}")
            return subitems
        else:
            self.logger.error(f"Error fetching subitems: {response.text}")
            return []

    def get_all_groups_from_board(self, board_id):
        """
        Retrieves all groups from a specified board.

        Args:
            board_id (str): The ID of the board.

        Returns:
            list: A list of dictionaries, each representing a group with its details.
        """
        query = '''
        query ($board_id: Int!) {
            boards(ids: [$board_id]) {
                groups {
                    id
                    title
                }
            }
        }
        '''
        variables = {'board_id': board_id}
        self.logger.info(f"Fetching all groups from board {board_id}.")

        response = requests.post(self.MONDAY_API_URL, headers=self.headers,
                                 json={'query': query, 'variables': variables})
        data = response.json()

        if response.status_code == 200 and 'data' in data:
            groups = data['data']['boards'][0]['groups']
            self.logger.debug(f"Retrieved groups: {json.dumps(groups, indent=2)}")
            return groups
        else:
            self.logger.error(f"Error fetching groups: {response.text}")
            return []

    def get_all_subitems_for_item(self, parent_item_id):
        """
        Retrieves all subitems for a specified parent item.

        Args:
            parent_item_id (str): The ID of the parent item (as a string).

        Returns:
            list: A list of dictionaries, each representing a subitem with its details.
        """
        query = '''
        query ($parent_item_id: [ID!]) {
            items(ids: $parent_item_id) {
                subitems {
                    id
                    name
                    column_values {
                        id
                        text
                    }
                }
            }
        }
        '''
        variables = {'parent_item_id': [parent_item_id]}
        self.logger.info(f"Fetching all subitems for parent item {parent_item_id}.")

        response = requests.post(self.MONDAY_API_URL, headers=self.headers,
                                 json={'query': query, 'variables': variables})
        data = response.json()

        if response.status_code == 200 and 'data' in data:
            subitems = data['data']['items'][0].get('subitems', [])
            self.logger.debug(f"Retrieved subitems: {json.dumps(subitems, indent=2)}")
            return subitems
        else:
            self.logger.error(f"Error fetching subitems: {response.text}")
            return []

    def find_contact_item_by_name(self, contact_name):
        """
        Finds a contact item in Monday.com based on the provided contact name and retrieves specified column values.

        Args:
            contact_name (str): The name of the contact to match.

        Returns:
            dict or None: A dictionary containing the item ID and the specified column values if found, otherwise None.

        Raises:
            Exception: If the GraphQL query fails or if there's an HTTP error.
        """
        # Prepare the columns argument for filtering by contact name
        columns_arg = json.dumps([{
            "column_id": self.CONTACT_NAME,
            "column_values": [contact_name]
        }])

        # GraphQL query using items_page_by_column_values with the columns argument
        query = f'''
            query {{
                items_page_by_column_values(
                    board_id: {self.CONTACT_BOARD_ID},
                    columns: {columns_arg},
                    limit: 1
                ) {{
                    items {{
                        id
                        name
                        column_values (ids: [
                            "{self.CONTACT_PHONE}",
                            "{self.CONTACT_EMAIL}",
                            "{self.CONTACT_ADDRESS_LINE_1}",
                            "{self.CONTACT_ADDRESS_CITY}",
                            "{self.CONTACT_ADDRESS_ZIP}",
                            "{self.CONTACT_ADDRESS_COUNTRY}",
                            "{self.CONTACT_TAX_TYPE}",
                            "{self.CONTACT_TAX_NUMBER}"
                        ]) {{
                            id
                            text
                        }}
                    }}
                }}
            }}
        '''

        self.logger.info(f"Searching for contact with name '{contact_name}'.")

        # Send the GraphQL request to the Monday.com API
        response = requests.post(self.MONDAY_API_URL, headers=self.headers, json={'query': query})
        data = response.json()
        self.logger.debug(f"Contact search response: {data}")

        # Check if the request was successful
        if response.status_code == 200 and 'data' in data:
            items = data['data']['items_page_by_column_values'].get('items', [])
            if items:
                item = items[0]
                item_id = item['id']
                column_values = {col['id']: col['text'] for col in item.get('column_values', [])}
                self.logger.info(f"Found contact item with ID {item_id} for contact name '{contact_name}'.")
                return {'item_id': item_id, 'column_values': column_values}
            else:
                # No items were found in the response
                self.logger.info(f"No contact found with name '{contact_name}'.")
                return None
        elif 'errors' in data:
            self.logger.error(f"Error fetching contacts from Monday.com: {data['errors']}")
            raise Exception("GraphQL query error")
        else:
            self.logger.error(f"Unexpected response structure: {data}")
            raise Exception("Unexpected GraphQL response")

    # --------------------- FETCHING ITEMS BY COLUMNS ---------------------

    def get_items_by_column_values(self, board_id, column_filters):
        """
        Retrieves all items from a board that match specified column values.

        Args:
            board_id (str): The ID of the board.
            column_filters (list): A list of dictionaries, each containing 'column_id' and 'column_values' keys.

        Returns:
            list: A list of dictionaries, each representing an item with its details.
        """
        query = '''
        query ($board_id: Int!, $columns: [ItemsPageByColumnValuesQuery!]!, $cursor: String) {
            items_page_by_column_values(board_id: $board_id, columns: $columns, cursor: $cursor, limit: 100) {
                cursor
                items {
                    id
                    name
                    column_values {
                        id
                        text
                    }
                }
            }
        }
        '''

        headers = self.headers

        items = []
        cursor = None

        self.logger.info(f"Fetching items from board {board_id} with filters {column_filters}.")

        while True:
            variables = {'board_id': board_id, 'columns': column_filters, 'cursor': cursor}
            response = requests.post(self.MONDAY_API_URL, headers=headers,
                                     json={'query': query, 'variables': variables})
            data = response.json()

            if response.status_code == 200 and 'data' in data:
                items_page = data['data']['items_page_by_column_values']
                items.extend(items_page.get('items', []))
                cursor = items_page.get('cursor')
                self.logger.debug(f"Fetched {len(items_page.get('items', []))} items. Cursor: {cursor}")
                if not cursor:
                    break
            else:
                self.logger.error(f"Error fetching items: {response.text}")
                break

        self.logger.info(f"Total items fetched: {len(items)}")
        return items

    # --------------------- VALIDATION METHODS ---------------------

    def validate_monday_request(self, request_headers):
        """
        Validates incoming webhook requests from Monday.com using the API token.

        Args:
            request_headers (dict): The headers from the incoming request.

        Returns:
            bool: True if the request is valid, False otherwise.
        """
        token = request_headers.get('Authorization')
        if not token:
            self.logger.warning("Missing 'Authorization' header.")
            return False
        # Assuming the token is sent as 'Bearer YOUR_TOKEN'
        try:
            received_token = token.split()[1]
        except IndexError:
            self.logger.warning("Invalid 'Authorization' header format.")
            return False
        if received_token != self.monday_api_token:
            self.logger.warning("Invalid API token.")
            return False
        self.logger.info("Request validated successfully.")
        return True
