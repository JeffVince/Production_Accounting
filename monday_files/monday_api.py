# monday_files/monday_api.py
import json
import logging
import time
from dotenv import load_dotenv
import requests

from helper_functions import list_to_dict
from logger import setup_logging
from utilities.singleton import SingletonMeta
from utilities.config import Config
from monday import MondayClient
from monday_files.monday_util import monday_util

load_dotenv()

# region 🔧 Configuration and Constants
# =====================================
# Emojis and structured comments have been added to improve readability and highlight key sections.

MAX_RETRIES = 3  # 🔄 Number of retries for transient errors
RETRY_BACKOFF_FACTOR = 2  # 🔄 Exponential backoff factor for retries
# endregion

class MondayAPI(metaclass=SingletonMeta):
    # region 🚀 Initialization
    # ============================================================
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
            self.logger.info("Monday API initialized 🏗️")
            self._initialized = True
    # endregion

    # region 🛡️ Request Handling with Complexity & Rate Limits
    # =========================================================
    def _make_request(self, query: str, variables: dict = None):
        """
        Execute a GraphQL request against the Monday.com API with complexity and retry logic.
        Ensures that complexity is queried, handles transient errors, and respects rate limits.

        Returns JSON response as before.
        """
        # Ensure complexity query is included
        if "complexity" not in query:
            # Insert complexity block right after the first '{' after 'query' or 'mutation'
            # This will give us complexity info on every request.
            insertion_index = query.find('{', query.find('query') if 'query' in query else query.find('mutation'))
            if insertion_index != -1:
                query = query[:insertion_index+1] + " complexity { query before after } " + query[insertion_index+1:]

        headers = {"Authorization": self.api_token}
        attempt = 0

        while attempt < MAX_RETRIES:
            try:
                response = requests.post(
                    self.api_url,
                    json={'query': query, 'variables': variables},
                    headers=headers,
                    timeout=10
                )
                response.raise_for_status()
                data = response.json()

                if "errors" in data:
                    self._handle_graphql_errors(data["errors"])

                self._log_complexity(data)
                return data

            except requests.exceptions.ConnectionError as ce:
                self.logger.warning(
                    f"⚠️ Connection error: {ce}. Attempt {attempt+1}/{MAX_RETRIES}. Retrying..."
                )
                time.sleep(RETRY_BACKOFF_FACTOR ** (attempt+1))
                attempt += 1
            except requests.exceptions.HTTPError as he:
                self.logger.error(f"❌ HTTP error: {he}")
                if response.status_code == 429:
                    # Rate limit hit
                    retry_after = response.headers.get("Retry-After", 10)
                    self.logger.warning(f"🔄 Rate limit hit. Waiting {retry_after} seconds before retry.")
                    time.sleep(int(retry_after))
                    attempt += 1
                else:
                    raise
            except Exception as e:
                self.logger.error(f"❌ Unexpected exception: {e}")
                raise

        self.logger.error("❌ Max retries reached without success.")
        raise ConnectionError("Failed to complete request after multiple retries.")

    def _handle_graphql_errors(self, errors):
        """
        Handle GraphQL-level errors returned by Monday.com.
        """
        for error in errors:
            message = error.get("message", "")
            if "ComplexityException" in message:
                self.logger.error("💥 Complexity limit reached!")
                raise Exception("ComplexityException")
            elif "DAILY_LIMIT_EXCEEDED" in message:
                self.logger.error("💥 Daily limit exceeded!")
                raise Exception("DAILY_LIMIT_EXCEEDED")
            elif "Minute limit rate exceeded" in message:
                self.logger.warning("⌛ Minute limit exceeded! Waiting before retry...")
                raise Exception("Minute limit exceeded")
            elif "Concurrency limit exceeded" in message:
                self.logger.warning("🕑 Concurrency limit exceeded!")
                raise Exception("Concurrency limit exceeded")
            else:
                self.logger.error(f"💥 GraphQL error: {message}")
                raise Exception(message)

    def _log_complexity(self, data):
        """
        Log complexity information if available.
        """
        complexity = data.get("data", {}).get("complexity", {})
        if complexity:
            query_complexity = complexity.get("query")
            before = complexity.get("before")
            after = complexity.get("after")
            self.logger.info(f"🔎 Complexity: query={query_complexity}, before={before}, after={after}")
    # endregion

    # region ✨ CRUD Operations and Fetch Methods
    # =========================================================
    # All methods below return the same structure as before.

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
        mutation ($parent_item_id: ID!, $subitem_name: String!, $column_values: JSON!) {
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

    def update_item(self, item_id: str, column_values: dict, type="main"):
        """Updates an existing item."""
        query = '''
        mutation ($board_id: ID!, $item_id: ID!, $column_values: JSON!) {
            change_multiple_column_values(board_id: $board_id, item_id: $item_id, column_values: $column_values) {
                id
            }
        }
        '''

        if type == "main":
            board_id = self.po_board_id
        elif type == "subitem":
            board_id = self.SUBITEM_BOARD_ID
        else:
            board_id = self.CONTACT_BOARD_ID

        variables = {
            'board_id': str(board_id),
            'item_id': str(item_id),
            'column_values': column_values
        }
        return self._make_request(query, variables)

    def fetch_all_items(self, board_id, limit=50):
        """
        Fetch all items from a Monday.com board using cursor-based pagination.
        Returns a list of items, unchanged.
        """
        all_items = []
        cursor = None

        while True:
            if cursor:
                query = """
                query ($cursor: String!, $limit: Int!) {
                    complexity { query before after }
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
                query = """
                query ($board_id: [ID!]!, $limit: Int!) {
                    complexity { query before after }
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
                    'board_id': str(board_id),
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
            cursor = items_data.get('cursor')

            if not cursor:
                break

            print(f"Fetched {len(items)} items from board {board_id}.")

        return all_items

    def fetch_all_sub_items(self, limit=100):
        """
        Fetch all subitems from the subitem board, filtering out those without a parent.
        Returns the list of valid items as before.
        """
        all_items = []
        cursor = None

        while True:
            if cursor:
                query = """
                query ($cursor: String!, $limit: Int!) {
                    complexity { query before after }
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
                query = """
                query ($board_id: [ID!]!, $limit: Int!) {
                    complexity { query before after }
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
                    'board_id': str(self.SUBITEM_BOARD_ID),
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
            valid_items = [item for item in items if item.get('parent_item') is not None]

            all_items.extend(valid_items)
            cursor = items_data.get('cursor')

            if not cursor:
                break

            self.logger.info(f"Fetched {len(valid_items)} valid items from board {self.SUBITEM_BOARD_ID}.")

        return all_items

    def fetch_all_contacts(self, board_id: object, limit: object = 150) -> object:
        """
        Fetch all contacts from a given board, returns the list of items as before.
        """
        all_items = []
        cursor = None

        while True:
            if cursor:
                query = """
                query ($cursor: String!, $limit: Int!) {
                    complexity { query before after }
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
                query = """
                query ($board_id: [ID!]!, $limit: Int!) {
                    complexity { query before after }
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
                    'board_id': str(board_id),
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
            cursor = items_data.get('cursor')

            if not cursor:
                break

            print(f"Fetched {len(items)} items from board {board_id}.")

        return all_items

    def fetch_item_by_ID(self, id: str):
        try:
            query = '''query ( $ID: ID!)
                            {
                                complexity { query before after }
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
            if len(items) == 0:
                return None
            return items[0]
        except (TypeError, IndexError, KeyError) as e:
            self.logger.error(f"Error fetching item by ID: {e}")
            raise

    def fetch_group_ID(self, project_id):
        query = f'''
        query {{
            complexity {{ query before after }}
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
            complexity { query before after }
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
            complexity { query before after }
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
            complexity { query before after }
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

    def fetch_item_by_name(self, name, board='PO'):
        query = '''
        query ($board_id: ID!, $name: String!) {
            complexity { query before after }
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

        if board == "PO":
            board_id = self.po_board_id
        elif board == "Contacts":
            board_id = self.CONTACT_BOARD_ID
        else:
            board_id = self.SUBITEM_BOARD_ID

        variables = {
            'board_id': int(board_id),
            'name': str(name),
        }
        response = self._make_request(query, variables)
        item = response["data"]["items_page_by_column_values"]["items"]
        if not len(item) == 1:
            return None
        return item

    # 🙆‍
    def find_or_create_contact_in_monday(self, name):
        contact = self.fetch_contact_by_name(name)
        if contact:  # contact exists
            return contact
        else:  # create a contact
            response = self.create_contact(name)
            return response["data"]["create_item"]

    # 💰
    def find_or_create_item_in_monday(self, item, column_values):
        response = self.fetch_item_by_po_and_project(item["project_id"], item["PO"])
        response_item = response["data"]["items_page_by_column_values"]["items"]
        if len(response_item) == 1:  # item exists
            response_item = response_item[0]
            item["item_pulse_id"] = response_item["id"]
            if not response_item["name"] == item["name"] and not item["po_type"] == "CC" and not item["po_type"] == "PC":
                # update name in Monday
                column_values = self.monday_util.po_column_values_formatter(name=item["name"], contact_pulse_id=item["contact_pulse_id"])
                self.update_item(response_item["id"], column_values)
                return item
            else:
                return item
        else:  # create item
            response = self.create_item(self.po_board_id, item["group_id"], item["name"], column_values)
            try:
                item["item_pulse_id"] = response["data"]['create_item']["id"]
            except Exception as e:
                self.logger.error(f"Response Error: {response}")
                raise e
            return item

    # 👻
    def find_or_create_sub_item_in_monday(self, sub_item, parent_item):
        try:
            # Prepare column values for Monday subitem as JSON string
            status = "RTP" if parent_item.get("status") == "RTP" else "PENDING"
            incoming_values_json = self.monday_util.subitem_column_values_formatter(
                date=sub_item.get("date"),
                due_date=sub_item["due date"],
                account_number=sub_item.get("account"),
                description=sub_item.get("description"),
                rate=sub_item["rate"],
                OT=sub_item["OT"],
                fringes=sub_item["fringes"],
                quantity=sub_item["quantity"],
                status=status,
                item_number=sub_item["item_id"]
            )

            # Deserialize incoming_values_json to a dictionary for comparison
            try:
                incoming_values = json.loads(incoming_values_json)
            except json.JSONDecodeError as jde:
                self.logger.error(f"JSON decode error for incoming_values: {jde}")
                return sub_item

            # If we already have a pulse_id, try updating that specific subitem directly
            if "pulse_id" in sub_item and sub_item["pulse_id"]:
                pulse_id = sub_item["pulse_id"]
                self.logger.info(f"Sub_item already has a pulse_id: {pulse_id}. Checking existing item.")

                existing_item = self.fetch_item_by_ID(pulse_id)
                if not existing_item:
                    self.logger.warning(f"Existing pulse_id {pulse_id} not found on Monday. Creating a new subitem.")
                    # Create a new subitem
                    create_result = self.create_subitem(
                        parent_item["item_pulse_id"],
                        sub_item.get("vendor", parent_item["name"]),
                        incoming_values_json  # Pass JSON string directly
                    )
                    new_pulse_id = create_result.get('data', {}).get('create_subitem', {}).get('id')
                    if new_pulse_id:
                        sub_item["pulse_id"] = new_pulse_id
                        self.logger.info(
                            f"Created new subitem with pulse_id {sub_item['pulse_id']} for surrogate_id {sub_item.get('detail_item_surrogate_id')}.")
                    else:
                        self.logger.exception("Failed to create a new subitem. 'id' not found in the response.")
                    return sub_item
                else:
                    # Deserialize existing_item's column_values for comparison
                    existing_vals = list_to_dict(existing_item["column_values"])
                    all_match = True
                    for col_id, new_val in incoming_values.items():
                        existing_val = existing_vals.get(col_id, {}).get("text", "")
                        if str(existing_val) != str(new_val):
                            all_match = False
                            break

                    if all_match:
                        self.logger.info(f"Subitem {pulse_id} is identical to incoming data. Skipping update.")
                        return sub_item
                    else:
                        self.logger.info(f"Updating existing subitem {pulse_id} due to changed values.")
                        self.update_item(pulse_id, incoming_values_json, type="subitem")  # Pass JSON string directly
                        return sub_item

            else:
                # No pulse_id known, search subitems by (project_id, PO, item_id)
                result = self.fetch_subitem_by_project_PO_receipt(
                    parent_item["project_id"],
                    sub_item["PO"],
                    sub_item["item_id"],
                    parent_item["item_pulse_id"]
                )

                if not result.get('data', {}).get('items'):
                    self.logger.error("No items found in the result data.")
                    subitems = []
                else:
                    subitems = result['data']['items'][0].get('subitems', [])

                if subitems:
                    self.logger.info(
                        f"Found {len(subitems)} subitems for PO {sub_item['PO']}. Checking for identical match.")
                    identical_subitem = None
                    for existing_subitem in subitems:
                        # Deserialize existing_subitem's column_values
                        existing_vals = list_to_dict(existing_subitem["column_values"])
                        all_match = True
                        for col_id, new_val in incoming_values.items():
                            existing_val = existing_vals.get(col_id, {}).get("text", "")
                            if str(existing_val) != str(new_val):
                                all_match = False
                                break

                        if all_match:
                            identical_subitem = existing_subitem
                            break

                    if identical_subitem:
                        # Identical subitem found, use its pulse_id so we don't create duplicates
                        sub_item["pulse_id"] = identical_subitem["id"]
                        self.logger.info(
                            f"Found an identical subitem with pulse_id {sub_item['pulse_id']}. Skipping creation.")
                        return sub_item
                    else:
                        # No identical subitem found, create a new one
                        self.logger.info("No identical subitem found. Creating a new subitem.")
                        create_result = self.create_subitem(
                            parent_item["item_pulse_id"],
                            sub_item.get("vendor", parent_item["name"]),
                            incoming_values_json  # Pass JSON string directly
                        )
                        new_pulse_id = create_result.get('data', {}).get('create_subitem', {}).get('id')
                        if new_pulse_id:
                            sub_item["pulse_id"] = new_pulse_id
                            self.logger.info(f"Created new subitem with pulse_id {sub_item['pulse_id']}.")
                        else:
                            self.logger.exception("Failed to create a new subitem. 'id' not found in the response.")

                else:
                    # No subitems at all, just create a new one
                    self.logger.info("No existing subitems found. Creating a new subitem.")
                    create_result = self.create_subitem(
                        parent_item["item_pulse_id"],
                        parent_item["name"],
                        incoming_values_json  # Pass JSON string directly
                    )
                    new_pulse_id = create_result['data']['create_subitem']['id']
                    if new_pulse_id:
                        sub_item["pulse_id"] = new_pulse_id
                        self.logger.info(f"Created new subitem with pulse_id {sub_item['pulse_id']}.")
                    else:
                        self.logger.exception("Failed to create a new subitem. 'id' not found in the response.")

        except Exception as e:
            self.logger.exception(f"Exception occurred in find_or_create_sub_item_in_monday: {e}")

        return sub_item
    #endregion

    def update_detail_items_with_invoice_link(self, detail_item_ids: list, file_link: str):
        # Pseudocode: update Monday detail items
        for detail_id in detail_item_ids:
            # Update Monday. For example:
            # column_values = {"files_column": [{"url": file_link, "name": "Invoice"}]}
            # result = self.update_item(detail_id, column_values)
            # if not successful:
            #    self.logger.warning(f"Failed to update item {detail_id} on Monday.")
            pass


monday_api = MondayAPI()