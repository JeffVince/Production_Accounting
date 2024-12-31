# utilities/monday_util.py

import json
import logging
import os
import re

from dateutil import parser
from datetime import datetime, timedelta

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
    safe_PO_BOARD_ID = "2562607316"  # Ensure this is a string
    PO_BOARD_ID = '7969894467'
    CONTACT_BOARD_ID = '2738875399'

    # Column IDs for POs
    PO_PROJECT_ID_COLUMN = 'project_id'  # Monday.com Project ID column ID
    PO_NUMBER_COLUMN = 'numeric__1'  # Monday.com PO Number column ID
    PO_TAX_COLUMN_ID = 'dup__of_invoice'  # TAX link column ID
    PO_DESCRIPTION_COLUMN_ID = 'text6'  # Item Description column ID
    PO_CONTACT_CONNECTION_COLUMN_ID = 'connect_boards1'
    PO_FOLDER_LINK_COLUMN_ID = 'dup__of_tax_form__1'
    PO_PRODUCER_COLUMN_ID = 'people'
    PO_TAX_FORM_COLUMN_ID = 'mirror__1'

    # Column IDs for Subitems
    SUBITEM_NOTES_COLUMN_ID = 'payment_notes__1'
    SUBITEM_STATUS_COLUMN_ID = 'status4'
    SUBITEM_ID_COLUMN_ID = 'numeric__1'  # Receipt / Invoice column ID
    SUBITEM_DESCRIPTION_COLUMN_ID = 'text98'  # Description column ID
    SUBITEM_QUANTITY_COLUMN_ID = 'numbers0'  # Quantity column ID
    SUBITEM_RATE_COLUMN_ID = 'numbers9'  # Rate column ID
    SUBITEM_DATE_COLUMN_ID = 'date'  # Date column ID
    SUBITEM_DUE_DATE_COLUMN_ID = 'date_1__1'
    SUBITEM_ACCOUNT_NUMBER_COLUMN_ID = 'numbers__1'  # Account Number column ID
    SUBITEM_LINK_COLUMN_ID = 'link'  # Link column ID
    SUBITEM_OT_COLUMN_ID = 'numbers0__1'
    SUBITEM_FRINGE_COLUMN_ID = 'numbers9__1'
    SUBITEM_LINE_NUMBER_COLUMN_ID = "numbers_Mjj5uYts"
    SUBITEM_PO_COLUMN_ID = "numbers_Mjj60Olh"
    SUBITEM_PROJECT_ID_COLUMN_ID = "numbers_Mjj8k8Yt"

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
    CONTACT_PAYMENT_STATUS = "status__1"
    CONTACT_STATUS = "status7__1"
    CONTACT_TAX_FORM_LINK = "link__1"

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
        SUBITEM_ACCOUNT_NUMBER_COLUMN_ID: "account_number",  # Maps to account_number in the DB
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
            try:
                columns = data['data']['boards'][0]['columns']
                for column in columns:
                    if column['type'] == 'subtasks':
                        self.logger.debug(f"Found subitems column ID: {column['id']}")
                        return column['id']
            except Exception as e:
                self.logger.error(f"Failed to retrieve columns: {data}")

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

    def update_item_columns(self, item_id, column_values, board="po"):
        """
        Updates multiple columns of an item in Monday.com.

        Args:
            item_id (str): The ID of the item to update.
            column_values (dict): A dictionary of column IDs and their corresponding values.

        Returns:
            bool: True if the update was successful, False otherwise.
            :param item_id:
            :param column_values:
            :param board:
        """
        # Convert the column values to JSON
        column_values_json = json.dumps(column_values).replace('"', '\\"')

        if board == "po":
            board_id = self.PO_BOARD_ID
        elif board == "contact":
            board_id = self.CONTACT_BOARD_ID
        elif board == "subitem":
            board_id = self.SUBITEM_BOARD_ID
        else:
            board_id = self.PO_BOARD_ID

        query = f'''
        mutation {{
            change_multiple_column_values(
                board_id: {board_id},
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

    def po_column_values_formatter(self, project_id=None, po_number=None, tax_id=None, description=None,
                                   contact_pulse_id=None, folder_link=None, status=None, producer_id=None, name=None):
        column_values = {}
        if project_id:
            column_values[self.PO_PROJECT_ID_COLUMN] = project_id
        if name:
            # Ensure `name` is not a set
            column_values["name"] = list(name) if isinstance(name, set) else name
        if po_number:
            column_values[self.PO_NUMBER_COLUMN] = po_number
        if tax_id:
            column_values[self.PO_TAX_COLUMN_ID] = tax_id
        if description:
            column_values[self.PO_DESCRIPTION_COLUMN_ID] = description
        if contact_pulse_id:
            column_values[self.PO_CONTACT_CONNECTION_COLUMN_ID] = {'item_ids': [contact_pulse_id]}
        if folder_link:
            column_values[self.PO_FOLDER_LINK_COLUMN_ID] = {'url': folder_link, 'text': '📦'}
        if producer_id:
            column_values[self.PO_PRODUCER_COLUMN_ID] = {'personsAndTeams': [{'id': producer_id, 'kind': 'person'}]}

        # Ensure all values are JSON-serializable
        for key, value in column_values.items():
            if isinstance(value, set):
                column_values[key] = list(value)

        return json.dumps(column_values)

    def prep_po_log_item_for_monday(self, item):
        pass

    # --------------------- SUBITEM METHODS ---------------------

    def subitem_column_values_formatter(self, project_id=None, po_number=None, detail_item_number=None, line_id=None,
                                        notes=None, status=None, description=None,
                                        quantity=None, rate=None, date=None, due_date=None,
                                        account_number=None, link=None, OT=None, fringes=None):
        column_values = {}

        if notes: column_values[self.SUBITEM_NOTES_COLUMN_ID] = notes
        if status: column_values[self.SUBITEM_STATUS_COLUMN_ID] = {'label': status}
        if description: column_values[self.SUBITEM_DESCRIPTION_COLUMN_ID] = description

        if quantity is not None:
            try:
                cleaned_quantity = float(str(quantity).replace(',', '').strip())
                column_values[self.SUBITEM_QUANTITY_COLUMN_ID] = float(cleaned_quantity)
            except (ValueError) as e:
                self.logger.error(f"Invalid quantity '{quantity}': {e}")
                column_values[self.SUBITEM_QUANTITY_COLUMN_ID] = None

        if rate is not None:
            try:
                cleaned_rate = float(str(rate).replace(',', '').strip())
                column_values[self.SUBITEM_RATE_COLUMN_ID] = float(cleaned_rate)
            except (ValueError) as e:
                self.logger.error(f"Invalid rate '{rate}': {e}")
                column_values[self.SUBITEM_RATE_COLUMN_ID] = None

        if OT is not None:
            try:
                cleaned_OT = float(str(OT).replace(',','').strip())
                column_values[self.SUBITEM_OT_COLUMN_ID] = float(cleaned_OT)
            except (ValueError) as e:
                self.logger.error(f"Invalid OT '{OT}': {e}")
                column_values[self.SUBITEM_OT_COLUMN_ID] = None

        if fringes is not None:
            try:
                cleaned_fringe = float(str(fringes).replace(',', '').strip())
                column_values[self.SUBITEM_FRINGE_COLUMN_ID] = float(cleaned_fringe)
            except (ValueError) as e:
                self.logger.error(f"Invalid fringes '{fringes}': {e}")
                column_values[self.SUBITEM_FRINGE_COLUMN_ID] = None

        if date:
            try:
                if isinstance(date, str) and date.strip():
                    parsed_date = parser.parse(date.strip())
                elif isinstance(date, datetime):
                    parsed_date = date
                else:
                    raise ValueError("Unsupported date format")
                column_values[self.SUBITEM_DATE_COLUMN_ID] = {'date': parsed_date.strftime('%Y-%m-%d')}
            except Exception as e:
                self.logger.error(f"Error parsing date '{date}': {e}")

        if due_date:
            try:
                # Debugging/logging to ensure due_date is being received correctly
                self.logger.debug(f"Processing due_date: {due_date}")

                # Check and handle both string and datetime object formats
                if isinstance(due_date, str) and due_date.strip():
                    parsed_due_date = parser.parse(due_date.strip())
                elif isinstance(due_date, datetime):
                    parsed_due_date = due_date
                else:
                    raise ValueError("Unsupported due_date format")

                # Add the parsed due_date to column_values
                column_values[self.SUBITEM_DUE_DATE_COLUMN_ID] = {'date': parsed_due_date.strftime('%Y-%m-%d')}
            except Exception as e:
                # Log and re-raise the exception for better debugging
                self.logger.error(f"Error parsing due_date '{due_date}': {e}")
                raise  # Re-raise to identify issues during debugging

        if account_number:
            try:
                cleaned_account_number = re.sub(r'[^\d]', '', str(account_number).strip())
                if cleaned_account_number:
                    column_values[self.SUBITEM_ACCOUNT_NUMBER_COLUMN_ID] = int(cleaned_account_number)
                else:
                    raise ValueError(f"Account number '{account_number}' invalid after cleaning.")
            except (ValueError, TypeError) as e:
                self.logger.error(f"Invalid account number '{account_number}': {e}")
                column_values[self.SUBITEM_ACCOUNT_NUMBER_COLUMN_ID] = None

        if link:
            column_values[self.SUBITEM_LINK_COLUMN_ID] = {'url': link, 'text': 'Link'}

        if po_number is not None:
            column_values[self.SUBITEM_PO_COLUMN_ID] = po_number

        if detail_item_number is not None:
            column_values[self.SUBITEM_ID_COLUMN_ID] = float(detail_item_number)

        if line_id is not None:
            column_values[self.SUBITEM_LINE_NUMBER_COLUMN_ID] = int(line_id)

        if project_id is not None:
            column_values[self.SUBITEM_PROJECT_ID_COLUMN_ID] = project_id

        # Ensure all values are JSON-serializable
        for key, value in column_values.items():
            if isinstance(value, set):
                column_values[key] = list(value)

        return json.dumps(column_values)

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

    def prep_po_log_detail_for_monday(self, item):
        pass

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

    def prep_po_log_contact_for_monday(self, item):
        pass

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

    # --------------------- HELPER METHODS ---------------------
    def get_item_data(self, monday_response):

        item_dict = monday_response['data']['items'][0]
        columns_dict = {item['id']: item for item in item_dict['column_values']}

        return item_dict, columns_dict

    def get_contact_pulse_id(self, columns_dict):
        parsed_value = json.loads(columns_dict['value'])
        linked_pulse_id = [item['linkedPulseId'] for item in parsed_value.get('linkedPulseIds', [])]
        return linked_pulse_id

    def is_main_item_different(self, db_item, monday_item):
        differences = []

        # Extract Monday column values for easy access
        col_vals = monday_item["column_values"]

        if "connect_boards1" in col_vals and col_vals["connect_boards1"]:
            # If it’s the new format:
            if "value" in col_vals["connect_boards1"]:
                if json.loads(col_vals["connect_boards1"]["value"]):
                    if json.loads(col_vals["connect_boards1"]["value"]).get("linkedPulseIds"):
                        linked_pulse_id = json.loads(col_vals["connect_boards1"]["value"]).get("linkedPulseIds")[0]["linkedPulseId"]
                    else:
                        linked_pulse_id = None
                else:
                        linked_pulse_id = None
            else:
                linked_pulse_id = None
        else:
            linked_pulse_id = None


        # Define a mapping between DB fields and Monday fields
        field_map = [
            {
                "field": "project_number",
                "db_value": db_item.get("project_number"),
                "monday_value": col_vals.get("project_id")["text"]
            },
            {
                "field": "contact_name",
                "db_value": db_item.get("contact_name"),
                "monday_value": monday_item.get("name")
            },
            {
                "field": "PO",
                "db_value": str(db_item.get("po_number")),
                "monday_value": col_vals.get("numeric__1")["text"]
            },
            {
                "field": "description",
                "db_value": db_item.get("description"),
                "monday_value": col_vals.get("text6")["text"]
            },
            {
                "field": "Connected Contact",
                "db_value": db_item.get("contact_pulse_id"),
                "monday_value": linked_pulse_id
            }
        ]

        # Compare each mapped field
        for f in field_map:
            db_val = f["db_value"] if f["db_value"] is not None else ""
            mon_val = f["monday_value"] if f["monday_value"] is not None else ""

            # Convert both sides to strings trimmed of whitespace for uniform comparison
            db_str = str(db_val).strip()
            mon_str = str(mon_val).strip()

            if db_str != mon_str:
                differences.append({
                    "field": f["field"],
                    "db_value": db_str,
                    "monday_value": mon_str
                })

        return differences

    def is_sub_item_different(self, db_sub_item, monday_sub_item):
        differences = []

        col_vals = monday_sub_item["column_values"]

        def safe_str(val):
            return str(val).strip() if val is not None else ""

        def are_values_equal(db_val, monday_val):
            # Try comparing as numbers if possible
            try:
                return float(db_val) == float(monday_val)
            except ValueError:
                # Fallback to string comparison
                return db_val == monday_val

        # Example field mapping for sub-items:
        # Adjust these mappings to your actual column IDs for sub-items
        field_map = [
            {
                "field": "quantity",
                "db_value": safe_str(db_sub_item.get("quantity")),
                "monday_value": safe_str(col_vals.get(self.SUBITEM_QUANTITY_COLUMN_ID)['text'])
            },
            {
                "field": "rate",
                "db_value": safe_str(db_sub_item.get("rate")),
                "monday_value": safe_str(col_vals.get(self.SUBITEM_RATE_COLUMN_ID)['text'])
            },
            {
                "field": "ot",
                "db_value": safe_str(db_sub_item.get("ot")),
                "monday_value": safe_str(col_vals.get(self.SUBITEM_OT_COLUMN_ID)['text'])
            },
            {
                "field": "fringes",
                "db_value": safe_str(db_sub_item.get("fringes")),
                "monday_value": safe_str(col_vals.get(self.SUBITEM_FRINGE_COLUMN_ID)['text'])
            },
            {
                "field": "transaction_date",
                "db_value": (
                    safe_str(db_sub_item.get("transaction_date")).split(" ")[0]
                    if safe_str(db_sub_item.get("transaction_date"))
                    else None
                ),
                "monday_value": safe_str(
                    col_vals[self.SUBITEM_DATE_COLUMN_ID]["text"]
                )
            },
            {
                "field": "due_date",
                "db_value": (
                    safe_str(db_sub_item.get("due_date")).split(" ")[0]
                    if safe_str(db_sub_item.get("due_date"))
                    else None
                ),
                "monday_value": safe_str(
                    col_vals[self.SUBITEM_DUE_DATE_COLUMN_ID]["text"]
                    if isinstance(col_vals[self.SUBITEM_DUE_DATE_COLUMN_ID], dict)
                    else col_vals[self.SUBITEM_DUE_DATE_COLUMN_ID]
                )
            }
        ]

        # Compare each field and record differences
        for f in field_map:
            if not are_values_equal(f["db_value"], f["monday_value"]):
                differences.append({
                    "field": f["field"],
                    "db_value": f["db_value"],
                    "monday_value": f["monday_value"]
                })

        return differences

    def extract_subitem_identifiers(self, monday_sub_item):
        col_vals = monday_sub_item["column_values"]

        def safe_int(val):
            try:
                return int(val)
            except (ValueError, TypeError):
                return None

        project_id = safe_int(float(col_vals[self.SUBITEM_PROJECT_ID_COLUMN_ID]['text']))
        po_number = safe_int(float(col_vals[self.SUBITEM_PO_COLUMN_ID]['text']))
        detail_num = safe_int(float(col_vals[self.SUBITEM_ID_COLUMN_ID]['text']))
        line_id = safe_int(float(col_vals[self.SUBITEM_LINE_NUMBER_COLUMN_ID]['text']))

        if project_id is not None and po_number is not None and detail_num is not None and line_id is not None:
            return project_id, po_number, detail_num, line_id
        else:
            self.logger.warning("Subitem missing one of the required identifiers.")
            return None

    def _extract_tax_link_from_monday(self, pulse_id, all_monday_contacts):
        """
        Given a contact's Monday pulse_id, find that contact in `all_monday_contacts`
        and return the link's 'url' if it exists.
        """
        if not pulse_id:
            return None

        for c in all_monday_contacts:
            if c["id"] == str(pulse_id):
                # Look through column_values for the tax form link column:
                for col in c.get("column_values", []):
                    if col["id"] == self.CONTACT_TAX_FORM_LINK:
                        try:
                            val = json.loads(col["value"])
                            return val.get("url")  # or text
                        except:
                            return col.get("text")
        return None


monday_util = MondayUtil()
