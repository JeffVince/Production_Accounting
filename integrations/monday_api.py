# integrations/monday_api.py

import requests
from utilities.config import Config

class MondayAPI:
    def __init__(self):
        self.api_token = Config.MONDAY_API_TOKEN
        self.api_url = 'https://api.monday.com/v2/'

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

    def get_item_by_name(self, board_id: int, item_name: str):
        """Retrieves an item by its name."""
        query = '''
        query ($board_id: Int!, $item_name: String!) {
            boards(ids: $board_id) {
                items (limit:1, page:1, newest_first:true, search: $item_name) {
                    id
                    name
                }
            }
        }
        '''
        variables = {
            'board_id': board_id,
            'item_name': item_name
        }
        return self._make_request(query, variables)

    def search_items(self, query_text: str):
        """Searches for items matching the query."""
        query = '''
        query ($query_text: String!) {
            items_by_column_values (board_id: YOUR_BOARD_ID, column_id: "name", column_value: $query_text) {
                id
                name
            }
        }
        '''
        variables = {'query_text': query_text}
        return self._make_request(query, variables)

    def link_contact_to_item(self, item_id: int, contact_id: int):
        """Links a contact to an item."""
        # Assuming a 'contacts' column exists
        column_values = '{"contacts": {"item_ids": [' + str(contact_id) + ']}}'
        return self.update_item(item_id, column_values)

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