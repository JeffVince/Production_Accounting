# utilities/monday_util.py

import json
import logging
import os
import requests
from dotenv import load_dotenv
from monday import MondayClient
from utilities.singleton import SingletonMeta


class MondayUtil(metaclass=SingletonMeta):
    """
    A utility class for interacting with the Monday.com API.
    Encapsulates methods for creating, updating, fetching, and deleting items, subitems, and contacts.
    """

    # --------------------- CONSTANTS ---------------------

    MONDAY_API_URL = 'https://api.monday.com/v2'

    ACTUALS_BOARD_ID = "7858669780"
    PO_BOARD_ID = "2562607316"  # Ensure this is a string
    CONTACT_BOARD_ID = '2738875399'

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
        # Monday Main Item Columns -> DB Fields
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
        SUBITEM_ID_COLUMN_ID: "detail_item_number",  # Maps to detail_item_number in the DB
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
        "link": "handle_link_column",
        "text": "handle_default_column"
    }

    # --------------------- INITIALIZATION ---------------------

    def __init__(self):
        if not hasattr(self, '_initialized'):
            # Set up logging
            self.logger = logging.getLogger("app_logger")

            # Load environment variables
            load_dotenv()

            # Initialize headers for API requests
            self.monday_api_token = os.getenv("MONDAY_API_TOKEN")
            self._subitem_board_id = None  # Instance-level cache

            if not self.monday_api_token:
                self.logger.error("Monday API Token not found. Please set it in the environment variables.")
                raise EnvironmentError("Missing MONDAY_API_TOKEN")

            self.headers = {
                'Authorization': self.monday_api_token,
                'Content-Type': 'application/json',
                'API-Version': '2023-10'
            }

            self.client = MondayClient(self.monday_api_token)

            # Initialize SUBITEM_BOARD_ID once during instantiation
            self._subitem_board_id = self.retrieve_subitem_board_id()
            self.logger.info(f"Retrieved subitem board ID: {self._subitem_board_id}")

            self._initialized = True


    # --------------------- PROPERTIES ---------------------

    @property
    def SUBITEM_BOARD_ID(self):
        return self._subitem_board_id

    def retrieve_subitem_board_id(self):
        """
        Retrieves the subitem board ID by first fetching the subitems column ID
        and then extracting the board ID from its settings.

        Returns:
            str: The subitem board ID.

        Raises:
            Exception: If unable to retrieve the subitem board ID.
        """
        subitems_column_id = self.get_subitems_column_id(self.PO_BOARD_ID)
        subitem_board_id = self.get_subitem_board_id(subitems_column_id)
        return subitem_board_id

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
                    self.logger.debug(f"Found subitems column ID: {column['id']}")
                    return column['id']
            raise Exception("Subitems column not found.")
        else:
            raise Exception(f"Failed to retrieve columns: {response.text}")

    def get_subitem_board_id(self, subitems_column_id):
        """
        Retrieves the subitem board ID for a given subitems column ID.

        Args:
            subitems_column_id (str): The ID of the subitems column.

        Returns:
            str: The subitem board ID.

        Raises:
            Exception: If the subitem board ID cannot be retrieved.
        """
        query = f'''
        query {{
            boards(ids: {self.PO_BOARD_ID}) {{
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
            return subitem_board_id
        else:
            raise Exception(f"Failed to retrieve subitem board ID: {response.text}")

    # --------------------- HELPER METHODS ---------------------

    def _handle_date_column(self, event):
        """
        Date handler for columns, extracting a single date.
        """
        return event.get('value', {}).get('date', {})

    def _handle_link_column(self, event):
        """
        Handles link column type and extracts URL.
        """
        try:
            return event.get('value', {}).get('url', {})
        except Exception:
            self.logger.warning("Setting link to None because of unexpected Monday Link Value.")
            return None

    def _handle_dropdown_column(self, event):
        """
        Handles dropdown column type and extracts chosen values.
        """
        try:
            line_number = event.get('value', {}).get('chosenValues', [])[0].get("name")
            # Adjust this according to your logic or database utility
            return line_number  # Or adjust as needed
        except Exception:
            self.logger.warning("Setting Account ID to None because of unexpected Monday Account Value.")
            return None

    def _handle_default_column(self, event):
        """
        Default handler for columns, extracting a single text label.
        """
        if not event or not event.get('value'):
            return None
        return event['value'].get('value')

    def _handle_status_column(self, event):
        """
        Handles status column type and extracts the label text.
        """
        return event.get('value', {}).get('label', {}).get('text')

    def get_column_handler(self, column_type):
        """
        Retrieves the appropriate handler method based on the column type.
        """
        handler_name = self.COLUMN_TYPE_HANDLERS.get(column_type, "handle_default_column")
        return getattr(self, f"_{handler_name}")

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

    # The rest of your methods remain unchanged...

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


monday_util = MondayUtil()
