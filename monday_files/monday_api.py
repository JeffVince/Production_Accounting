# monday_files/monday_api.py
from dotenv import load_dotenv
import requests
from utilities.config import Config
from monday import MondayClient
from monday_files.monday_util import MondayUtil

load_dotenv()


class MondayAPI:
    def __init__(self):
        self.monday_api = None
        self.api_token = Config.MONDAY_API_TOKEN
        self.api_url = 'https://api.monday.com/v2/'
        self.client = MondayClient(self.api_token)
        self.monday_util = MondayUtil()
        self.SUBITEM_BOARD_ID = self.monday_util.SUBITEM_BOARD_ID

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
                    print(f"No boards found for board_id {self.SUBITEM_BOARD_ID}. Please verify the board ID and your permissions.")
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

            print(f"Fetched {len(valid_items)} valid items from board {self.SUBITEM_BOARD_ID}.")

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
        return self.client.items.fetch_items_by_id(id)

