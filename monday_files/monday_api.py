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
            self.PO_BOARD_ID = self.monday_util.PO_BOARD_ID
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
                    timeout=200
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
            self.logger.debug(f"🔎 Complexity: query={query_complexity}, before={before}, after={after}")
    # endregion

    # region ✨ CRUD Operations and Fetch Methods
    # =========================================================
    # All methods below return the same structure as before.

    def _safe_get_text(self, vals_dict, col_id):
        """
        Helper function to safely extract text value from column_values dict.
        Replace `col_id` strings with the actual column IDs for PO, item number, and line_id from your Monday board.
        """
        return vals_dict.get(col_id, {}).get("text", "")

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

    def update_item(self, item_id: str, column_values, type="main"):
        """Updates an existing item."""
        query = '''
        mutation ($board_id: ID!, $item_id: ID!, $column_values: JSON!) {
            change_multiple_column_values(board_id: $board_id, item_id: $item_id, column_values: $column_values) {
                id
            }
        }
        '''

        if type == "main":
            board_id = self.PO_BOARD_ID
        elif type == "subitem":
            board_id = self.SUBITEM_BOARD_ID
        elif type == "contact":
            board_id = self.CONTACT_BOARD_ID
        else:
            board_id = "main"

        variables = {
            'board_id': str(board_id),
            'item_id': str(item_id),
            'column_values': column_values
        }
        return self._make_request(query, variables)

    def fetch_all_items(self, board_id, limit=200):
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

            self.logger.info(f"Fetched {len(items)} items from board {board_id}.")

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

    def fetch_all_contacts(self, board_id: object, limit: object = 250) -> object:
        """
        Fetch all contacts from a given board, returns the list of items as before.
        """
        all_items = []
        cursor = None
        self.logger.info(f"Fetching all contacts from Monday.com")
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

            self.logger.debug(f"Fetched {len(items)} items from board {board_id}.")

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
            boards (ids: {self.PO_BOARD_ID}) {{
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

    def fetch_subitem_by_receipt_and_line(self, receipt_number, line_id):
        # Replace these with actual column IDs from your subitem board
        receipt_number_column_id = "numeric__1"
        line_number_column_id = "numbers_Mjj5uYts"

        query = '''
        query ($board_id: ID!, $receipt_number: String!, $line_id: String!) {
            complexity { query before after }
            items_page_by_column_values(
                board_id: $board_id, 
                columns: [
                  {column_id: "%s", column_values: [$receipt_number]}, 
                  {column_id: "%s", column_values: [$line_id]}
                ], 
                limit: 1
            ) {
                items {
                    id
                    column_values {
                        id
                        text
                        value
                    }
                }
            }
        }
        ''' % (receipt_number_column_id, line_number_column_id)

        variables = {
            'board_id': int(self.SUBITEM_BOARD_ID),
            'receipt_number': str(receipt_number),
            'line_id': str(line_id)
        }

        response = self._make_request(query, variables)
        items = response.get("data", {}).get("items_page_by_column_values", {}).get("items", [])
        if items:
            return items[0]  # Return the matched subitem
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
            'board_id': int(self.PO_BOARD_ID),
            'po_number': str(po_number),
            'project_id': str(project_id),
            'po_column': str(self.po_number_column),
            'project_id_column': str(self.project_id_column)
        }
        return self._make_request(query, variables)

    def fetch_subitem_by_po_receipt_line(self, po_number, receipt_number, line_id):
        # Replace these with the actual column IDs from your subitem board
        po_number_column_id = self.monday_util.SUBITEM_PO_COLUMN_ID
        receipt_number_column_id = self.monday_util.SUBITEM_ID_COLUMN_ID
        line_number_column_id = self.monday_util.SUBITEM_LINE_NUMBER_COLUMN_ID

        query = f'''
        query ($board_id: ID!, $po_number: String!, $receipt_number: String!, $line_id: String!) {{
            complexity {{ query before after }}
            items_page_by_column_values(
                board_id: $board_id, 
                columns: [
                    {{column_id: "{po_number_column_id}", column_values: [$po_number]}},
                    {{column_id: "{receipt_number_column_id}", column_values: [$receipt_number]}},
                    {{column_id: "{line_number_column_id}", column_values: [$line_id]}}
                ], 
                limit: 1
            ) {{
                items {{
                    id
                    column_values {{
                        id
                        text
                        value
                    }}
                }}
            }}
        }}
        '''

        variables = {
            'board_id': int(self.SUBITEM_BOARD_ID),
            'po_number': str(po_number),
            'receipt_number': str(receipt_number),
            'line_id': str(line_id)
        }

        response = self._make_request(query, variables)
        items = response.get("data", {}).get("items_page_by_column_values", {}).get("items", [])
        return items[0] if items else None


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
            board_id = self.PO_BOARD_ID
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
            response = self.create_item(self.PO_BOARD_ID, item["group_id"], item["name"], column_values)
            try:
                item["item_pulse_id"] = response["data"]['create_item']["id"]
            except Exception as e:
                self.logger.error(f"Response Error: {response}")
                raise e
            return item

    # 👻
    def find_or_create_sub_item_in_monday(self, sub_item, parent_item):
        try:
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
                item_number=sub_item["detail_item_id"],
                line_number=sub_item["line_id"],
                PO=sub_item["po_number"]
            )

            # Parse incoming values
            try:
                incoming_values = json.loads(incoming_values_json)
            except json.JSONDecodeError as jde:
                self.logger.error(f"JSON decode error for incoming_values: {jde}")
                return sub_item

            # If subitem already has a pulse_id, handle update logic (unchanged)
            if "pulse_id" in sub_item and sub_item["pulse_id"]:
                pulse_id = sub_item["pulse_id"]
                self.logger.info(f"Sub_item already has a pulse_id: {pulse_id}. Checking if it exists on Monday.")

                existing_item = self.fetch_item_by_ID(pulse_id)
                if not existing_item:
                    self.logger.warning(f"Existing pulse_id {pulse_id} not found on Monday. Creating a new subitem.")
                    create_result = self.create_subitem(
                        parent_item["item_pulse_id"],
                        sub_item.get("vendor", parent_item["name"]),
                        incoming_values_json
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
                    # Compare existing item’s values with incoming_values
                    existing_vals = list_to_dict(existing_item["column_values"])
                    all_match = True
                    for col_id, new_val in incoming_values.items():
                        existing_val = existing_vals.get(col_id, {}).get("text", "")
                        if str(existing_val) != str(new_val):
                            all_match = False
                            break

                    if all_match:
                        self.logger.info(f"Subitem {pulse_id} is identical to incoming data. No update needed.")
                        return sub_item
                    else:
                        self.logger.info(f"Updating existing subitem {pulse_id} due to changed values.")
                        self.update_item(pulse_id, incoming_values_json, type="subitem")
                        return sub_item
            else:
                # No known pulse_id. Search for an existing subitem by PO, receipt_number, and line_id
                existing_subitem = self.fetch_subitem_by_po_receipt_line(
                    po_number=sub_item["po_number"],
                    receipt_number=sub_item["detail_item_id"],
                    # Use the correct field if "item_id" is actually "receipt_number"
                    line_id=sub_item["line_id"]
                )

                if existing_subitem:
                    # Found a match, so no need to create a duplicate
                    sub_item["pulse_id"] = existing_subitem["id"]
                    self.logger.info(
                        f"Found existing subitem with PO {sub_item['po_number']}, item_id (receipt) {sub_item['detail_item_id']}, and line_id {sub_item['line_id']}. Not creating duplicate."
                    )
                    return sub_item
                else:
                    # No matching subitem found, create a new one
                    self.logger.info("No matching subitem found. Creating a new subitem.")
                    create_result = self.create_subitem(
                        parent_item["item_pulse_id"],
                        sub_item.get("vendor", parent_item["name"]),
                        incoming_values_json
                    )
                    new_pulse_id = create_result.get('data', {}).get('create_subitem', {}).get('id')
                    if new_pulse_id:
                        sub_item["pulse_id"] = new_pulse_id
                        self.logger.info(f"Created new subitem with pulse_id {sub_item['pulse_id']}.")
                    else:
                        self.logger.exception("Failed to create a new subitem. 'id' not found in the response.")

            return sub_item

        except Exception as e:
            self.logger.exception(f"Exception occurred in find_or_create_sub_item_in_monday: {e}")
            return sub_item

    def update_detail_items_with_invoice_link(self, detail_item_ids: list, file_link: str):
        # Pseudocode: update Monday detail items
        for detail_id in detail_item_ids:
            # Update Monday. For example:
            # column_values = {"files_column": [{"url": file_link, "name": "Invoice"}]}
            # result = self.update_item(detail_id, column_values)
            # if not successful:
            #    self.logger.warning(f"Failed to update item {detail_id} on Monday.")
            pass

    def get_items_in_project(self, project_id):
        """
        Fetch all items from a board filtered by project_id.
        Returns a list of items: [{"id": ..., "name": ..., "column_values": {...}}, ...]
        where column_values is a dict of column_id -> text_value
        """
        query = '''
        query ($board_id: ID!, $project_id_column: String!, $project_id_val: String!, $limit: Int, $cursor: String) {
            items_page_by_column_values(
                board_id: $board_id,
                columns: [{column_id: $project_id_column, column_values: [$project_id_val]}],
                limit: $limit,
                cursor: $cursor
            ) {
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
        }'''

        variables = {
            'board_id': self.PO_BOARD_ID,
            'project_id_column': self.project_id_column,
            'project_id_val': str(project_id),
            'limit': 500,  # Maximum number of items per request
            'cursor': None  # Start without a cursor
        }

        all_items = []

        try:
            while True:
                response = self._make_request(query, variables)
                data = response.get("data", {}).get("items_page_by_column_values", {})
                items_data = data.get("items", [])
                cursor = data.get("cursor")

                for it in items_data:
                    cv_dict = {
                        cv["id"]: {
                            "text": cv.get("text"),
                            "value": cv.get("value")
                        } for cv in it.get("column_values", [])
                    }
                    all_items.append({
                        "id": it["id"],
                        "name": it["name"],
                        "column_values": cv_dict
                    })

                if not cursor:
                    break  # No more items to fetch

                variables['cursor'] = cursor  # Set cursor for the next request

            return all_items

        except Exception as e:
            self.logger.exception(f"Error fetching items by project_id {project_id}: {e}")
            raise

    def get_subitems_for_item(self, item_id):
        """
        Fetch subitems for a given main item_id.
        Returns a list of { "id": subitem_id, "name": subitem_name, "column_values": {...} }
        """
        # According to Monday's API, we fetch subitems via the parent item query
        # Example query for subitems:
        query = '''
        query ($item_id: [ID!]!) {
            complexity { query before after }
            items (ids: $item_id) {
                id
                name
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
        variables = {
            'item_id': str(item_id),
        }
        try:
            response = self._make_request(query, variables)
            items = response.get("data", {}).get("items", [])
            if not items:
                return []
            parent_item = items[0]
            subitems = parent_item.get("subitems", [])
            results = []
            for si in subitems:
                cv_dict = {cv["id"]: cv["text"] for cv in si.get("column_values", [])}
                results.append({
                    "id": si["id"],
                    "name": si["name"],
                    "column_values": cv_dict
                })
            return results
        except Exception as e:
            self.logger.exception(f"Error fetching subitems for item {item_id}: {e}")
            raise

    def batch_create_or_update_items(self, batch, project_id, create=True):
        """
        Batch create or update main PO items on Monday.
        If create=True, we use the new create_items_batch method to create all items in one request.
        If create=False, we update each item individually as before.
        Return the same structure with "monday_item_id" filled where needed.
        """
        if create:
            # Call create_items_batch for all items that need to be created
            updated_batch = self.create_items_batch(batch, project_id)
            return updated_batch
        else:
            # Update existing items individually
            updated_batch = []
            for itm in batch:
                db_item = itm["db_item"]
                column_values = itm["column_values"]
                column_values_json = json.dumps(column_values)
                item_id = itm["monday_item_id"]
                if not item_id:
                    self.logger.warning(f"No monday_item_id provided for update. Skipping item: {db_item}")
                    continue
                try:
                    self.update_item(item_id, column_values_json, type="main")
                    updated_batch.append(itm)
                except Exception as e:
                    self.logger.exception(f"Error updating item {item_id}: {e}")
                    raise
            return updated_batch

        return updated_batch

    def batch_create_or_update_subitems(self, subitems_batch, parent_item_id, create=True):
        """
        Batch create or update subitems.
        subitems_batch: [{ "db_sub_item": sdb, "column_values": {..}, "monday_subitem_id": maybe_id, "parent_id": parent_item_id }]
        If create=True, call create_subitem for each that doesn't have monday_subitem_id.
        If create=False, call update_item on each that has monday_subitem_id.
        """
        updated_batch = []
        for si in subitems_batch:
            db_sub_item = si.get("db_sub_item")
            column_values = si["column_values"]
            column_values_json = json.dumps(column_values)
            if create:
                # Create a new subitem
                # We use parent_item_id and db_sub_item["name"] or vendor name for the subitem name
                subitem_name = db_sub_item.get("name", db_sub_item.get("vendor", "Subitem"))
                try:
                    create_response = self.create_subitem(parent_item_id, subitem_name, column_values_json)
                    new_id = create_response["data"]["create_subitem"]["id"]
                    si["monday_item_id"] = new_id
                    updated_batch.append(si)
                except Exception as e:
                    self.logger.exception(f"Error creating subitem for {db_sub_item}: {e}")
                    raise
            else:
                # Update existing subitems
                sub_id = si["monday_item_id"]
                try:
                    self.update_item(sub_id, column_values_json, type="subitem")
                    updated_batch.append(si)
                except Exception as e:
                    self.logger.exception(f"Error updating subitem {sub_id}: {e}")
                    raise

        return updated_batch

    def create_items_batch(self, batch, project_id):
        """
        Create multiple items in a single Monday API request.
        Accepts a batch similar to the one passed to batch_create_or_update_items, where each element has:
        {
            'db_item': { ... },
            'column_values': { ... },
            'monday_item_id': None  # since these are new items
        }

        Returns the batch with 'monday_item_id' filled in.
        """
        # Build the mutation string dynamically
        # We'll name each mutation "createX" where X is the index
        # Each create_item call:
        # create_item(board_id: Int!, group_id: String, item_name: String!, column_values: JSON)

        mutation_parts = []
        for i, itm in enumerate(batch):
            column_values = itm["column_values"]
            # Convert column_values to JSON string
            column_values_json = json.dumps(column_values)

            # Extract item_name from column_values or db_item
            # Assuming 'name' is always present in column_values
            item_name = column_values.get("name", "Unnamed Item")

            # Inline arguments. Make sure strings are properly escaped. json.dumps handles this for values.
            # For item_name, we can also wrap in json.dumps to ensure proper escaping.
            safe_item_name = json.dumps(item_name)
            safe_column_values = json.dumps(column_values_json)  # This double-JSON encoding is not needed.
            # column_values_json is already a JSON string.
            # Just use column_values_json directly.

            # Correct usage: column_values expects a JSON object as a string, so no need to double-encode
            # safe_column_values = json.dumps(column_values_json) would produce a string containing JSON with escape chars
            # Instead, we can directly inline column_values_json since it's already a JSON string:
            # But we must ensure quotes are escaped. We'll do that by embedding it directly:

            mutation_parts.append(
                f'create{i}: create_item('
                f'board_id: {self.PO_BOARD_ID}, '
                f'item_name: {safe_item_name}, '
                f'column_values: {json.dumps(column_values_json)}) '
                '{ id }'
            )

        mutation_body = " ".join(mutation_parts)
        query = f'mutation {{ {mutation_body} }}'

        # Perform the single request
        response = self._make_request(query)

        # Parse the response and update batch
        for i, itm in enumerate(batch):
            create_key = f"create{i}"
            created_item = response.get("data", {}).get(create_key)
            if created_item and "id" in created_item:
                itm["monday_item_id"] = created_item["id"]
            else:
                self.logger.warning(f"No ID returned for item {i} in batch.")

        return batch

    # region 🙌 Contact Utilities

    def parse_tax_number(self, tax_str: str):
        """
        Removes hyphens (e.g., for SSN '123-45-6789' or EIN '12-3456789') and attempts to parse as int.
        Returns None if parsing fails or if the string is empty.
        """
        if not tax_str:
            return None

        cleaned = tax_str.replace('-', '')
        try:
            return int(cleaned)
        except ValueError:
            self.logger.warning(
                f"⚠️ Could not parse tax number '{tax_str}' as int after removing hyphens."
            )
            return None

    def extract_monday_contact_fields(self, contact_item: dict) -> dict:
        """
        Convert a Monday contact_item (including its column_values) into a structured dict of:
            {
              "pulse_id": ...,
              "phone": ...,
              "email": ...,
              "address_line_1": ...,
              "city": ...,
              "zip_code": ...,
              "country": ...,
              "tax_type": ...,
              "tax_number_str": ...,
              "payment_details": ...,
              "vendor_status": ...,
              "tax_form_link": ...
            }
        Use your contact column IDs from monday_util.
        """
        column_values = contact_item.get("column_values", [])

        # Helper function to parse the link from cv["value"] if it exists,
        # otherwise default to the "text" field.
        def parse_column_value(cv):
            """
            Returns the link if 'url' is found in cv['value'], else returns cv['text'].
            """
            raw_text = cv.get("text") or ""
            raw_value = cv.get("value")

            # Attempt to parse JSON from "value" to see if there's a 'url' field
            if raw_value:
                try:
                    data = json.loads(raw_value)
                    # If it's a Link column, 'data["url"]' usually holds the actual link
                    if isinstance(data, dict) and data.get("url"):
                        return data["url"]
                except (ValueError, TypeError):
                    pass

            # If we get here, either no "url" was found or no JSON could be parsed,
            # so return the plain text field
            return raw_text

        # Create a dict keyed by column ID, choosing the best representation (URL if present)
        parsed_values = {}
        for cv in column_values:
            col_id = cv["id"]
            parsed_values[col_id] = parse_column_value(cv)

        # If you want to see how each column ID was interpreted, uncomment:
        # print(parsed_values)

        return {
            "pulse_id": contact_item["id"],
            "phone": parsed_values.get(self.monday_util.CONTACT_PHONE),
            "email": parsed_values.get(self.monday_util.CONTACT_EMAIL),
            "address_line_1": parsed_values.get(self.monday_util.CONTACT_ADDRESS_LINE_1),
            "city": parsed_values.get(self.monday_util.CONTACT_ADDRESS_CITY),
            "zip_code": parsed_values.get(self.monday_util.CONTACT_ADDRESS_ZIP),
            "country": parsed_values.get(self.monday_util.CONTACT_ADDRESS_COUNTRY),
            "tax_type": parsed_values.get(self.monday_util.CONTACT_TAX_TYPE),
            "tax_number_str": parsed_values.get(self.monday_util.CONTACT_TAX_NUMBER),
            "payment_details": parsed_values.get(self.monday_util.CONTACT_PAYMENT_DETAILS),
            "vendor_status": parsed_values.get(self.monday_util.CONTACT_STATUS),
            "tax_form_link": parsed_values.get(self.monday_util.CONTACT_TAX_FORM_LINK),
        }

    def create_contact_in_monday(self, name: str) -> dict:
        """
        Finds a contact by name. If it exists, returns the Monday item (including column_values).
        If it does not exist, creates a new contact in Monday, then fetches its item and returns it.
        """
        self.logger.info(f"➕ Creating new Monday contact for '{name}'.")
        create_resp = self.create_contact(name)
        new_id = create_resp["data"]["create_item"]["id"]
        self.logger.info(f"✅ Created Monday contact with pulse_id={new_id}. Retrieving its data...")
        # fetch the full item (with column_values) so we can return consistent data
        created_item = self.fetch_item_by_ID(new_id)
        return created_item

    def sync_db_contact_to_monday(self, db_contact):
        """
        Pushes DB contact fields up to Monday.com, updating the matching contact pulse.
        If the contact has no pulse_id, it logs a warning and returns.
        """
        if not db_contact.pulse_id:
            self.logger.warning(
                f"⚠️ DB Contact id={db_contact.id}, name='{db_contact.name}' has no pulse_id. "
                "Use 'find_or_create_contact_in_monday' first."
            )
            return

        self.logger.info(
            f"🔄 Updating Monday contact (pulse_id={db_contact.pulse_id}) with DB fields..."
        )

        # Build out the column_values dict by referencing the contact fields in your DB
        # and mapping them to your Monday "Contacts" board columns.
        column_values = {
            # Basic contact info
            self.monday_util.CONTACT_PHONE: db_contact.phone or "",
            self.monday_util.CONTACT_EMAIL: db_contact.email or "",
            self.monday_util.CONTACT_ADDRESS_LINE_1: db_contact.address_line_1 or "",

            # Address fields
            self.monday_util.CONTACT_CITY: db_contact.city or "",
            self.monday_util.CONTACT_STATE: db_contact.state or "",
            self.monday_util.CONTACT_COUNTRY: db_contact.country or "",
            self.monday_util.CONTACT_ZIP: db_contact.zip_code or "",

            # Tax info
            self.monday_util.CONTACT_TAX_TYPE: db_contact.tax_type or "",
            self.monday_util.CONTACT_TAX_NUMBER: str(db_contact.tax_number) if db_contact.tax_number else "",
            self.monday_util.CONTACT_TAX_FORM_LINK: db_contact.tax_form_link or "",

            # Payment details, vendor status
            self.monday_util.CONTACT_PAYMENT_DETAILS: db_contact.payment_details or "",
            self.monday_util.CONTACT_VENDOR_STATUS: db_contact.vendor_status or "",
        }

        # Update the contact item in Monday
        self.update_item(item_id=db_contact.pulse_id, column_values=column_values, type="Contacts")
        self.logger.info(
            f"✅ Monday contact (pulse_id={db_contact.pulse_id}) updated with DB field values."
        )

    def _update_monday_tax_form_link(self, pulse_id, new_link):
        """
        Update Monday contact's tax_form_link column with the new link.
        The 'text' of the link will reflect the type of tax form
        (e.g., 'W-9', 'W-8BEN', or a default 'Tax Form').
        """
        if not pulse_id:
            self.logger.warning("No pulse_id to update Monday link.")
            return

        # 1) Determine the tax form type from the URL
        #    We'll do a case-insensitive check in the link.
        #    If you have more patterns, just add them to the if/elif chain below.
        link_lower = new_link.lower()
        if "w9" in link_lower:
            link_text = "W-9"
        elif "w8-ben-e" in link_lower:
            link_text = "W-8BEN-E"
        elif "w8-ben" in link_lower:
            link_text = "W-8BEN"
        else:
            link_text = "Tax Form"

        # 2) Build the JSON link object: {"url": "...", "text": "..."}
        link_value = {
            "url": new_link,
            "text": link_text
        }

        # 3) Convert to JSON and send the update to Monday
        column_values =json.dumps({
            self.monday_util.CONTACT_TAX_FORM_LINK:link_value
        })
        try:
            self.update_item(
                item_id=str(pulse_id),
                column_values=column_values,
                type="contact"
            )
            self.logger.info(
                f"✅ Updated Monday contact (pulse_id={pulse_id}) "
                f"tax_form_link='{new_link}' (text='{link_text}')."
            )
        except Exception as e:
            self.logger.exception(
                f"Failed to update Monday contact (pulse_id={pulse_id}) "
                f"with link '{new_link}': {e}",
                exc_info=True
            )
    # endregion


monday_api = MondayAPI()