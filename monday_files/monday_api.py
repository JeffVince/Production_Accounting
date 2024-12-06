# monday_files/monday_api.py
import json
import logging

from dotenv import load_dotenv
import requests

from helper_functions import list_to_dict
from logger import setup_logging
from utilities.singleton import SingletonMeta
from utilities.config import Config
from monday import MondayClient
from monday_files.monday_util import monday_util

load_dotenv()


class MondayAPI(metaclass=SingletonMeta):
    def __init__(self):
        if not hasattr(self, '_initialized'):
            # Setup logging and get the configured logger
            self.logger = logging.getLogger("app_logger")
            self.monday_api = None
            self.api_token = Config.MONDAY_API_TOKEN
            self.api_url = 'https://api.monday.com/v2/'
            self.client = MondayClient(self.api_token)
            self.monday_util = monday_util
            self.po_board_id = self.monday_util.PO_BOARD_ID
            self.SUBITEM_BOARD_ID = self.monday_util.SUBITEM_BOARD_ID
            self.project_id_column = self.monday_util.PO_PROJECT_ID_COLUMN
            self.po_number_column = self.monday_util.PO_NUMBER_COLUMN
            self.CONTACT_BOARD_ID = self.monday_util.CONTACT_BOARD_ID
            self.logger.info("Monday API  initialized")
            self._initialized = True


    def _make_request(self, query: str, variables: dict = None):
        headers = {"Authorization": self.api_token}
        response = requests.post(
            self.api_url,
            json={'query': query, 'variables': variables},
            headers=headers
        )
        response.raise_for_status()
        return response.json()

    def create_item(self, board_id: int, group_id: str, name: str, column_values: dict):
        """Creates a new item on a board."""
        query = '''
        mutation ($board_id: ID!, $group_id: String, $item_name: String!, $column_values: JSON) {
            create_item(board_id: $board_id, group_id: $group_id, item_name: $item_name, column_values: $column_values) {
                id
            }
        }
        '''
        variables = {
            'board_id': int(board_id),
            'group_id': group_id,
            'item_name': name,
            'column_values': column_values
        }
        return self._make_request(query, variables)

    def create_subitem(self, parent_item_id: int, subitem_name: str, column_values: dict):
        """Creates a subitem under a parent item."""
        query = '''
        mutation ($parent_item_id: Int!, $subitem_name: String!, $column_values: JSON!) {
            create_subitem(parent_item_id: $parent_item_id, item_name: $subitem_name, column_values: $column_values) {
                id
            }
        }
        '''
        variables = {
            'parent_item_id': parent_item_id,
            'subitem_name': subitem_name,
            'column_values': column_values
        }
        return self._make_request(query, variables)

    def create_contact(self, name):
        """Creates a new contact on a board."""
        query = '''
        mutation ($board_id: ID!, $item_name: String!) {
            create_item(board_id: $board_id, item_name: $item_name) {
                id,
                name
            }
        }
        '''
        variables = {
            'board_id': int(self.CONTACT_BOARD_ID),
            'item_name': name,
        }
        return self._make_request(query, variables)

    def update_item(self, item_id: str, column_values: dict):
        """Updates an existing item."""
        query = '''
        mutation ($board_id: ID!, $item_id: ID!, $column_values: JSON!) {
            change_multiple_column_values(board_id: $board_id, item_id: $item_id, column_values: $column_values) {
                id
            }
        }
        '''
        variables = {
            'board_id': str(self.po_board_id),  # Ensure this is the board ID
            'item_id': str(item_id),  # Ensure this is the item ID
            'column_values': column_values  # JSON object with column values
        }
        return self._make_request(query, variables)

    def fetch_all_items(self, board_id, limit=50):
        """
        Fetch all items from a Monday.com board using cursor-based pagination.

        :param board_id: The ID of the board to fetch items from.
        :param limit: Number of items to fetch per request (maximum is 500).
        :return: List of all items from the board.
        """
        all_items = []
        cursor = None

        while True:
            if cursor:
                # Use next_items_page for subsequent requests
                query = """
                query ($cursor: String!, $limit: Int!) {
                    next_items_page(cursor: $cursor, limit: $limit) {
                        cursor
                        items {
                            id
                            name
                            column_values {
                                id
                                text
                                value
                            }
                        }
                    }
                }
                """
                variables = {
                    'cursor': cursor,
                    'limit': limit
                }
            else:
                # Use items_page for the initial request
                query = """
                query ($board_id: [ID!]!, $limit: Int!) {
                        boards(ids: $board_id) {
                            items_page(limit: $limit) {
                                cursor
                                items {
                                    id
                                    name
                                    column_values {
                                        id
                                        text
                                        value
                                    }
                                }
                            }
                        }
                    }
                """
                variables = {
                    'board_id': str(board_id),  # Ensure the board_id is a string
                    'limit': limit
                }

            try:
                response = self._make_request(query, variables)
            except Exception as e:
                print(f"Error fetching items: {e}")
                break

            if cursor:
                items_data = response.get('data', {}).get('next_items_page', {})
            else:
                boards_data = response.get('data', {}).get('boards', [])
                if not boards_data:
                    print(f"No boards found for board_id {board_id}. Please verify the board ID and your permissions.")
                    break
                items_data = boards_data[0].get('items_page', {})

            items = items_data.get('items', [])
            all_items.extend(items)

            # Extract the next cursor
            cursor = items_data.get('cursor')

            # If there's no cursor, we've fetched all items
            if not cursor:
                break

            print(f"Fetched {len(items)} items from board {board_id}.")

        return all_items

    def fetch_all_sub_items(self, limit=100):
        """
            Fetch all items from a Monday.com board using cursor-based pagination,
            excluding sub-items with a parent_item of None.

            :param board_id: The ID of the board to fetch items from.
            :param limit: Number of items to fetch per request (maximum is 500).
            :return: List of all items from the board with valid parent items.
            """
        all_items = []
        cursor = None

        while True:
            if cursor:
                # Use next_items_page for subsequent requests
                query = """
                    query ($cursor: String!, $limit: Int!) {
                        next_items_page(cursor: $cursor, limit: $limit) {
                            cursor
                            items  {
                                id
                                name
                                parent_item {
                                    id
                                    name
                                }
                                column_values {
                                    id
                                    text
                                    value
                                }
                            }
                        }
                    }
                    """
                variables = {
                    'cursor': cursor,
                    'limit': limit
                }
            else:
                # Use items_page for the initial request
                query = """
                    query ($board_id: [ID!]!, $limit: Int!) {
                        boards(ids: $board_id) {
                            items_page(limit: $limit) {
                                cursor
                                items {
                                    id
                                    name
                                    parent_item {
                                        id
                                        name
                                    }
                                    column_values {
                                        id
                                        text
                                        value
                                    }
                                }
                            }
                        }
                    }
                    """
                variables = {
                    'board_id': str(self.SUBITEM_BOARD_ID),  # Ensure the board_id is a string
                    'limit': limit
                }

            try:
                response = self._make_request(query, variables)
            except Exception as e:
                print(f"Error fetching items: {e}")
                break

            if cursor:
                items_data = response.get('data', {}).get('next_items_page', {})
            else:
                boards_data = response.get('data', {}).get('boards', [])
                if not boards_data:
                    print(
                        f"No boards found for board_id {self.SUBITEM_BOARD_ID}. Please verify the board ID and your permissions.")
                    break
                items_data = boards_data[0].get('items_page', {})

            items = items_data.get('items', [])

            # Filter out items with parent_item as None
            valid_items = [item for item in items if item.get('parent_item') is not None]

            all_items.extend(valid_items)

            # Extract the next cursor
            cursor = items_data.get('cursor')

            # If there's no cursor, we've fetched all items
            if not cursor:
                break

            self.logger.info(f"Fetched {len(valid_items)} valid items from board {self.SUBITEM_BOARD_ID}.")

        return all_items

    def fetch_all_contacts(self, board_id: object, limit: object = 150) -> object:
        """
        Fetch all items from a Monday.com board using cursor-based pagination.

        :param board_id: The ID of the board to fetch items from.
        :param limit: Number of items to fetch per request (maximum is 500).
        :return: List of all items from the board.
        """
        all_items = []
        cursor = None

        while True:
            if cursor:
                # Use next_items_page for subsequent requests
                query = """
                query ($cursor: String!, $limit: Int!) {
                    next_items_page(cursor: $cursor, limit: $limit) {
                        cursor
                        items {
                            id
                            name
                            column_values {
                                id
                                text
                                value
                            }
                        }
                    }
                }
                """
                variables = {
                    'cursor': cursor,
                    'limit': limit
                }
            else:
                # Use items_page for the initial request
                query = """
                query ($board_id: [ID!]!, $limit: Int!) {
                        boards(ids: $board_id) {
                            items_page(limit: $limit) {
                                cursor
                                items {
                                    id
                                    name
                                    column_values {
                                        id
                                        text
                                        value
                                    }
                                }
                            }
                        }
                    }
                """
                variables = {
                    'board_id': str(board_id),  # Ensure the board_id is a string
                    'limit': limit
                }

            try:
                response = self._make_request(query, variables)
            except Exception as e:
                print(f"Error fetching items: {e}")
                break

            if cursor:
                items_data = response.get('data', {}).get('next_items_page', {})
            else:
                boards_data = response.get('data', {}).get('boards', [])
                if not boards_data:
                    print(f"No boards found for board_id {board_id}. Please verify the board ID and your permissions.")
                    break
                items_data = boards_data[0].get('items_page', {})

            items = items_data.get('items', [])
            all_items.extend(items)

            # Extract the next cursor
            cursor = items_data.get('cursor')

            # If there's no cursor, we've fetched all items
            if not cursor:
                break

            print(f"Fetched {len(items)} items from board {board_id}.")

        return all_items

    def fetch_item_by_ID(self, id: str):
        try:
            query = '''query ( $ID: ID!)
                            {
                                items (ids: [$ID]) {
                                    id,
                                    name,
                                    group {
                                        id
                                        title
                                    }
                                    column_values {
                                        id,
                                        text,
                                        value
                                    }
                                }
                            }'''
            variables = {
                "ID": id
            }
            response = self._make_request(query, variables)
            items = response['data']["items"]
            if  len(items) == 0:
                return None
            return items[0]
        except (TypeError, IndexError, KeyError) as e:
            self.logger.error(f"Error fetching item by ID: {e}")
            raise

    def fetch_group_ID(self, project_id):
        query = f'''
                    query {{
              boards (ids: {self.po_board_id}) {{
                groups {{
                  title
                  id
                }}
              }}
            }}
        '''
        response = self._make_request(query, {})
        groups = response["data"]["boards"][0]["groups"]
        for group in groups:
            if group["title"].__contains__(project_id):
                return group["id"]
        return None

    def fetch_item_by_po_and_project(self, project_id, po_number):
        query = '''
        query ($board_id: ID!, $po_number: String!, $project_id: String!, $project_id_column: String!, $po_column: String!) {
                  items_page_by_column_values (limit: 1, board_id: $board_id, columns: [{column_id: $project_id_column, column_values: [$project_id]}, {column_id: $po_column, column_values: [$po_number]}]) {
                    items {
                      id
                      name
                    column_values {
                        id
                        value
                    }
                    }
                  }
                }'''
        variables = {
            'board_id': int(self.po_board_id),
            'po_number': str(po_number),
            'project_id': str(project_id),
            'po_column': str(self.po_number_column),
            'project_id_column': str(self.project_id_column)
        }
        return self._make_request(query, variables)

    def fetch_subitem_by_project_PO_receipt(self, project_id, po_number, receipt_number, parent_pulse_id):
        query = '''
                        query ($ID: ID!) {
                                  items (ids: [$ID]) {
                                    subitems {
                                      id
                                      column_values {
                                        id
                                        value
                                        text
                                      }
                                    }
                                  }
                }'''
        variables = {
            'ID': parent_pulse_id
        }
        return self._make_request(query, variables)

    def fetch_contact_by_name(self, name):
        query = '''
        query ($board_id: ID!, $name: String!) {
                  items_page_by_column_values (limit: 1, board_id: $board_id, columns: [{column_id: "name", column_values: [$name]}]) {
                    items {
                      id
                      name
                      
                    }
                  }
                }'''
        variables = {
            'board_id': int(self.CONTACT_BOARD_ID),
            'name': str(name),
        }
        response = self._make_request(query, variables)
        if len(response["data"]["items_page_by_column_values"]["items"]) == 1:
            contact = response["data"]["items_page_by_column_values"]["items"][0]
            contact_item = self.fetch_item_by_ID(contact["id"])
            if contact_item:
                return contact_item
            else:
                return None
        else:
            contact = None

    def fetch_item_by_name(self, name):
        query = '''
        query ($board_id: ID!, $name: String!) {
                  items_page_by_column_values (limit: 1, board_id: $board_id, columns: [{column_id: "name", column_values: [$name]}]) {
                    items {
                      id
                      name
                      column_values {
                        id
                        value
                    }
                  }
                }
                }'''
        variables = {
            'board_id': int(self.po_board_id),
            'name': str(name),
        }
        response = self._make_request(query, variables)
        item = response["data"]["items_page_by_column_values"]["items"]
        if not len(item) == 1:
            return None
        return item
    #🙆‍
    def find_or_create_contact_in_monday(self, name):
        contact = self.fetch_contact_by_name(name)
        if contact:  # contact exists
            return contact
        else:  # create a contact
            response = self.create_contact(name)
            return response["data"]["create_item"]
    #💰
    def find_or_create_item_in_monday(self, item, column_values):
        response = self.fetch_item_by_po_and_project(item["project_id"], item["PO"])
        response_item = response["data"]["items_page_by_column_values"]["items"]
        if len(response_item) == 1:  # item exists
            response_item = response_item[0]
            item["item_pulse_id"] = response_item["id"]
            if not response_item["name"] == item["Vendor"] and not item["po_type"] == "CC / PC": #if the names don't match for a vendor
                # update name in Monday
                column_values = self.monday_util.po_column_values_formatter(name=item["Vendor"], contact_pulse_id=item["contact_pulse_id"])
                self.update_item(response_item["id"], column_values)
                return item
            else:
                return item
        else:  # create item
            response = self.create_item(self.po_board_id, item["group_id"], item["Vendor"], column_values)
            item["item_pulse_id"] = response["data"]['create_item']["id"]
            return item

    def find_or_create_sub_item_in_monday(self, sub_item, parent_item, column_values):
        # search for item
        result =  self.fetch_subitem_by_project_PO_receipt(parent_item["project_id"], sub_item["PO"], sub_item["Detail Item Number"], parent_item["item_pulse_id"])
        subitems = result['data']['items'][0]['subitems']
        if len(subitems) > 0:
            for subitem in subitems:
                subitem["column_values"] = list_to_dict(subitem["column_values"])
                if subitem["column_values"]["text0"]["text"] == sub_item["Detail Item Number"]:
                        sub_item["pulse_id"] = subitem['id']
                        #IF STATUS IS NOT PAID OR RECONCILED
                        #UPDATE THE SUBITEM DETAILS
                        #THEN RETURN THE SUBITEM + PULSE
                        return sub_item
        # not found
                # create
                #return  subitem + pulse

monday_api = MondayAPI()
