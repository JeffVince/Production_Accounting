# integrations/monday_api.py

import requests
from utilities.config import Config
import monday_util
from monday import MondayClient


class MondayAPI:
    def __init__(self):
        self.monday_api = None
        self.api_token = Config.MONDAY_API_TOKEN
        self.api_url = 'https://api.monday.com/v2/'
        self.client = MondayClient(self.api_token)

    def _make_request(self, query: str, variables: dict = None):
        headers = {"Authorization": self.api_token}
        response = requests.post(
            self.api_url,
            json={'query': query, 'variables': variables},
            headers=headers
        )
        response.raise_for_status()
        return response.json()

    def create_item(self, board_id: int, item_name: str, column_values: dict):
        """Creates a new item on a board."""
        query = '''
        mutation ($board_id: Int!, $item_name: String!, $column_values: JSON!) {
            create_item(board_id: $board_id, item_name: $item_name, column_values: $column_values) {
                id
            }
        }
        '''
        variables = {
            'board_id': board_id,
            'item_name': item_name,
            'column_values': column_values
        }
        return self._make_request(query, variables)

    def update_item(self, item_id: int, column_values: dict):
        """Updates an existing item."""
        query = '''
        mutation ($item_id: Int!, $column_values: JSON!) {
            change_multiple_column_values(item_id: $item_id, column_values: $column_values) {
                id
            }
        }
        '''
        variables = {
            'item_id': item_id,
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

    # update_subitem

    def get_item_by_po_number(self, project_id: str, po_number: str):
        """Retrieves an item by its PO and Project_D."""
        query = """
        query ($boardId: Int!, $column1Id: String!, $column1Values: [String]!, $column2Id: String!, $column2Values: [String]!) {
          items_page_by_column_values(
            board_id: $boardId,
            columns: [
              { column_id: $column1Id, column_values: $column1Values },
              { column_id: $column2Id, column_values: $column2Values }
            ],
            limit: 1
          ) {
            items {
              id
              name
              column_values {
                id
                text
              }
            }
            cursor
          }
        }
        """

        variables = {
            "boardId": monday_util.PO_BOARD_ID,
            "column1Id": monday_util.PO_NUMBER_COLUMN,
            "column1Values": [po_number],
            "column2Id": monday_util.PO_PROJECT_ID_COLUMN,
            "column2Values": [project_id]
        }

        response = self._make_request(query, variables)
        print(response)
        return response

    def get_item_by_ID(self, id: str):
        return self.client.items.fetch_items_by_id(id)

    # get_subitems_from_po

    # get_subitem_by_invoice

    # get_all_items
    def get_all_items(self, board_id: str):
        return self.client.items.list_items(board_id)

    # get_all_subitems

    def get_contact_list(self):
        """Retrieves the list of contacts."""
        query = '''
        query {
            users(kind: non_guests) {
                id
                name
                email
            }
        }
        '''
        return self._make_request(query)

    def create_contact(self, contact_data: dict):
        """Creates a new contact."""
        # Monday.com API doesn't directly support creating contacts via API
        # This is a placeholder for potential custom implementation
        raise NotImplementedError("Monday.com API does not support creating contacts via API.")

    def fetch_all_main_items(self, board_id, limit=50):
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

    def fetch_all_sub_items(self, board_id, limit=100):
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
