# integrations/monday_api.py

import requests
from utilities.config import Config
import monday_util
from monday.resources import items
from monday import MondayClient


class MondayAPI:
    def __init__(self):
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
