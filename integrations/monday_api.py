# integrations/monday_api.py

import os
import requests
import logging
from typing import Any, Dict, List, Optional
from dotenv import load_dotenv

# Load environment variables from a .env file if present
load_dotenv()

# Column IDs for Main Items
PO_BOARD_ID = os.getenv("MONDAY_PO_BOARD_ID")
PO_PROJECT_ID_COLUMN = os.getenv("MONDAY_PO_PROJECT_ID_COLUMN")
PO_NUMBER_COLUMN = os.getenv("MONDAY_PO_NUMBER_COLUMN")
PO_DESCRIPTION_COLUMN = os.getenv("MONDAY_PO_DESCRIPTION_COLUMN")
PO_TAX_COLUMN = os.getenv("MONDAY_PO_TAX_COLUMN")
PO_FOLDER_LINK_COLUMN = os.getenv("MONDAY_PO_FOLDER_LINK_COLUMN")
PO_STATUS_COLUMN = os.getenv("MONDAY_PO_STATUS_COLUMN")
PRODUCER_PM_COLUMN = os.getenv("MONDAY_PRODUCER_PM_COLUMN")
UPDATED_DATE_COLUMN = os.getenv("MONDAY_UPDATED_DATE_COLUMN")

# Column IDs for Subitems
SUBITEM_STATUS_COLUMN = os.getenv("MONDAY_SUBITEM_STATUS_COLUMN")
SUBITEM_ID_COLUMN = os.getenv("MONDAY_SUBITEM_ID_COLUMN")
SUBITEM_DESCRIPTION_COLUMN = os.getenv("MONDAY_SUBITEM_DESCRIPTION_COLUMN")
SUBITEM_RATE_COLUMN = os.getenv("MONDAY_SUBITEM_RATE_COLUMN")
SUBITEM_QUANTITY_COLUMN = os.getenv("MONDAY_SUBITEM_QUANTITY_COLUMN")
SUBITEM_ACCOUNT_NUMBER_COLUMN = os.getenv("MONDAY_SUBITEM_ACCOUNT_NUMBER_COLUMN")
SUBITEM_DATE_COLUMN = os.getenv("MONDAY_SUBITEM_DATE_COLUMN")
SUBITEM_LINK_COLUMN = os.getenv("MONDAY_SUBITEM_LINK_COLUMN")
SUBITEM_DUE_DATE_COLUMN = os.getenv("MONDAY_SUBITEM_DUE_DATE_COLUMN")
CREATION_LOG_COLUMN = os.getenv("MONDAY_CREATION_LOG_COLUMN")

# Log Path
MONDAY_API_LOG_PATH = os.getenv("MONDAY_API_LOG_PATH", "./logs/monday_api.log")

# Ensure the log directory exists
log_dir = os.path.dirname(MONDAY_API_LOG_PATH)
if not os.path.exists(log_dir):
    os.makedirs(log_dir, exist_ok=True)

# Configure logging for this module
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(MONDAY_API_LOG_PATH),  # Logs to a file
        logging.StreamHandler()  # Also logs to the console
    ]
)

class MondayAPI:
    def __init__(self, api_token: Optional[str] = None):
        """
        Initializes the MondayAPI with the provided API token.

        Args:
            api_token (str, optional): Monday.com API token. If not provided, it will be read from the environment variable 'MONDAY_API_TOKEN'.
        """
        self.api_token = api_token or os.getenv('MONDAY_API_TOKEN')
        if not self.api_token:
            logging.error("Monday.com API token not provided and 'MONDAY_API_TOKEN' environment variable not set.")
            raise ValueError("Monday.com API token is required.")
        self.endpoint = "https://api.monday.com/v2"
        self.headers = {
            "Authorization": self.api_token,
            "Content-Type": "application/json"
        }

    def _send_query(self, query: str, variables: Dict[str, Any] = {}) -> Any:
        """
        Sends a GraphQL query to the Monday.com API.

        Args:
            query (str): The GraphQL query string.
            variables (dict): Variables for the GraphQL query.

        Returns:
            dict: The JSON response from the API.

        Raises:
            Exception: If the API returns an error.
        """
        try:
            response = requests.post(
                self.endpoint,
                json={"query": query, "variables": variables},
                headers=self.headers
            )
            response.raise_for_status()
            result = response.json()
            if 'errors' in result:
                logging.error(f"GraphQL errors: {result['errors']}")
                raise Exception(result['errors'])
            return result['data']
        except requests.exceptions.RequestException as e:
            logging.error(f"Request exception: {e}")
            raise e
        except Exception as e:
            logging.error(f"General exception: {e}")
            raise e

    def create_item(self, board_id: int, item_name: str, column_values: Dict[str, Any]) -> Dict[str, Any]:
        """
        Creates a new item on a specified board.

        Args:
            board_id (int): The ID of the board where the item will be created.
            item_name (str): The name of the new item.
            column_values (dict): A dictionary of column values.

        Returns:
            dict: The created item's details.
        """
        query = """
        mutation ($boardId: Int!, $itemName: String!, $columnValues: JSON!) {
            create_item(board_id: $boardId, item_name: $itemName, column_values: $columnValues) {
                id
                name
            }
        }
        """
        variables = {
            "boardId": board_id,
            "itemName": item_name,
            "columnValues": column_values
        }
        logging.info(f"Creating item '{item_name}' on board ID {board_id}.")
        data = self._send_query(query, variables)
        return data['create_item']

    def update_item(self, item_id: int, column_values: Dict[str, Any]) -> Dict[str, Any]:
        """
        Updates an existing item with new column values.

        Args:
            item_id (int): The ID of the item to update.
            column_values (dict): A dictionary of column values to update.

        Returns:
            dict: The updated item's details.
        """
        query = """
        mutation ($itemId: Int!, $columnValues: JSON!) {
            change_multiple_columns(item_id: $itemId, column_values: $columnValues) {
                id
                name
            }
        }
        """
        variables = {
            "itemId": item_id,
            "columnValues": column_values
        }
        logging.info(f"Updating item ID {item_id} with new column values.")
        data = self._send_query(query, variables)
        return data['change_multiple_columns']

    def create_subitem(self, parent_item_id: int, subitem_name: str, column_values: Dict[str, Any]) -> Dict[str, Any]:
        """
        Creates a subitem under a specified parent item.

        Args:
            parent_item_id (int): The ID of the parent item.
            subitem_name (str): The name of the new subitem.
            column_values (dict): A dictionary of column values for the subitem.

        Returns:
            dict: The created subitem's details.
        """
        query = """
        mutation ($parentItemId: Int!, $subitemName: String!, $columnValues: JSON!) {
            create_subitem(parent_item_id: $parentItemId, item_name: $subitemName, column_values: $columnValues) {
                id
                name
            }
        }
        """
        variables = {
            "parentItemId": parent_item_id,
            "subitemName": subitem_name,
            "columnValues": column_values
        }
        logging.info(f"Creating subitem '{subitem_name}' under parent item ID {parent_item_id}.")
        data = self._send_query(query, variables)
        return data['create_subitem']

    def get_item_by_name(self, board_id: int, item_name: str) -> Optional[Dict[str, Any]]:
        """
        Retrieves an item by its name from a specified board.

        Args:
            board_id (int): The ID of the board.
            item_name (str): The name of the item to retrieve.

        Returns:
            dict or None: The item's details if found, else None.
        """
        query = """
        query ($boardId: Int!, $itemName: String!) {
            items_by_board(board_id: $boardId, search: $itemName) {
                id
                name
                column_values {
                    id
                    text
                }
            }
        }
        """
        variables = {
            "boardId": board_id,
            "itemName": item_name
        }
        logging.info(f"Searching for item '{item_name}' on board ID {board_id}.")
        data = self._send_query(query, variables)
        items = data.get('items_by_board', [])
        for item in items:
            if item['name'].lower() == item_name.lower():
                logging.info(f"Item '{item_name}' found with ID {item['id']}.")
                return item
        logging.info(f"Item '{item_name}' not found on board ID {board_id}.")
        return None

    def search_items(self, query_str: str) -> List[Dict[str, Any]]:
        """
        Searches for items across all boards based on a query string.

        Args:
            query_str (str): The search query.

        Returns:
            list: A list of items matching the search query.
        """
        query = """
        query ($query: String!) {
            search_items(query: $query) {
                id
                name
                board {
                    id
                    name
                }
                column_values {
                    id
                    text
                }
            }
        }
        """
        variables = {
            "query": query_str
        }
        logging.info(f"Searching for items with query '{query_str}'.")
        data = self._send_query(query, variables)
        return data.get('search_items', [])

    def link_contact_to_item(self, item_id: int, contact_id: int) -> Dict[str, Any]:
        """
        Links a contact to an item.

        Args:
            item_id (int): The ID of the item.
            contact_id (int): The ID of the contact to link.

        Returns:
            dict: The updated item's details.
        """
        # Assuming 'producer_pm' is the column ID for contacts
        column_values = {
            PRODUCER_PM_COLUMN: {"personsAndTeams": [{"id": contact_id, "kind": "person"}]}
        }
        query = """
        mutation ($itemId: Int!, $columnValues: JSON!) {
            change_multiple_columns(item_id: $itemId, column_values: $columnValues) {
                id
                name
            }
        }
        """
        variables = {
            "itemId": item_id,
            "columnValues": column_values
        }
        logging.info(f"Linking contact ID {contact_id} to item ID {item_id}.")
        data = self._send_query(query, variables)
        return data['change_multiple_columns']

    def get_contact_list(self) -> List[Dict[str, Any]]:
        """
        Retrieves the list of all contacts.

        Returns:
            list: A list of contacts.
        """
        query = """
        query {
            users {
                id
                name
                email
            }
        }
        """
        logging.info("Fetching contact list.")
        data = self._send_query(query)
        return data.get('users', [])

    def create_contact(self, contact_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Creates a new contact.

        Args:
            contact_data (dict): A dictionary containing contact information (e.g., name, email).

        Returns:
            dict: The created contact's details.
        """
        query = """
        mutation ($name: String!, $email: String!) {
            create_user(name: $name, email: $email) {
                id
                name
                email
            }
        }
        """
        variables = {
            "name": contact_data.get("name"),
            "email": contact_data.get("email")
        }
        logging.info(f"Creating new contact with name '{variables['name']}' and email '{variables['email']}'.")
        data = self._send_query(query, variables)
        return data['create_user']

