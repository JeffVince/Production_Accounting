# monday_files/monday_api.py

import json
import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

from dotenv import load_dotenv
import requests

from helper_functions import list_to_dict
from logger import setup_logging
from utilities.singleton import SingletonMeta
from utilities.config import Config
from monday import MondayClient
from monday_files.monday_util import monday_util

load_dotenv("../.env")

# region 🔧 Global Configuration and Constants
# ============================================
MAX_RETRIES = 3         # 🔄 Number of retries for transient errors
RETRY_BACKOFF_FACTOR = 2  # 🔄 Exponential backoff factor for retries
# endregion


class MondayAPI(metaclass=SingletonMeta):

    # region 🚀 Initialization & Setup
    # ============================================================
    def __init__(self):
        """
        🏗️ Sets up the Monday API singleton with proper logging, token initialization,
        and references to critical board and column IDs.
        """
        if not hasattr(self, '_initialized'):
            try:
                # Setup logging and get the configured logger
                self.logger = logging.getLogger("app_logger")
                self.logger.debug("Initializing MondayAPI singleton... ⚙️")

                self.api_token = Config.MONDAY_API_TOKEN
                if not self.api_token:
                    self.logger.warning("⚠️ MONDAY_API_TOKEN is not set. Check .env or your configuration.")

                self.api_url = 'https://api.monday.com/v2/'
                self.client = MondayClient(self.api_token)

                # Monday utility references
                self.monday_util = monday_util
                # Board IDs
                self.PO_BOARD_ID = self.monday_util.PO_BOARD_ID
                self.SUBITEM_BOARD_ID = self.monday_util.SUBITEM_BOARD_ID
                self.CONTACT_BOARD_ID = self.monday_util.CONTACT_BOARD_ID
                # Column references
                self.project_id_column = self.monday_util.PO_PROJECT_ID_COLUMN
                self.po_number_column = self.monday_util.PO_NUMBER_COLUMN

                self.logger.info("✅ Monday API initialized successfully 🏗️")

                self._initialized = True
            except Exception as init_ex:
                self.logger.exception(f"❌ Error during MondayAPI initialization: {init_ex}")
                raise init_ex
    # endregion

    # region 🔐 Private Methods (Requests, Logging, Error Handling)
    # ============================================================
    def _make_request(self, query: str, variables: dict = None):
        """
        🔒 Private Method: Executes a GraphQL request against the Monday.com API with:
          - Complexity query insertion
          - Retry logic for transient failures (e.g., connection errors)
          - Handling of 429 (rate-limit) responses

        :param query: GraphQL query string
        :param variables: Optional variables dict
        :return: Parsed JSON response from Monday API
        :raises: ConnectionError if MAX_RETRIES exceeded or any unhandled error occurs
        """
        # Inject complexity block if missing
        if "complexity" not in query:
            insertion_index = query.find('{', query.find('query') if 'query' in query else query.find('mutation'))
            if insertion_index != -1:
                query = (query[:insertion_index + 1]
                         + " complexity { query before after } "
                         + query[insertion_index + 1:])

        headers = {"Authorization": self.api_token}
        attempt = 0

        while attempt < MAX_RETRIES:
            try:
                self.logger.debug(f"📡 Attempt {attempt + 1}/{MAX_RETRIES}: Sending request to Monday.com")
                response = requests.post(
                    self.api_url,
                    json={'query': query, 'variables': variables},
                    headers=headers,
                    timeout=200
                )
                response.raise_for_status()  # Raises HTTPError if status != 200
                data = response.json()

                # If GraphQL-level errors exist, handle them
                if "errors" in data:
                    self._handle_graphql_errors(data["errors"])

                # Log the complexity usage
                self._log_complexity(data)
                return data

            except requests.exceptions.ConnectionError as ce:
                self.logger.warning(f"⚠️ Connection error: {ce}. Attempt {attempt + 1}/{MAX_RETRIES}. Retrying...")
                time.sleep(RETRY_BACKOFF_FACTOR ** (attempt + 1))
                attempt += 1

            except requests.exceptions.HTTPError as he:
                self.logger.error(f"❌ HTTP error encountered: {he}")
                if response.status_code == 429:
                    # Rate limit hit
                    retry_after = response.headers.get("Retry-After", 10)
                    self.logger.warning(f"🔄 Rate limit (429) hit. Waiting {retry_after} seconds before retry.")
                    time.sleep(int(retry_after))
                    attempt += 1
                else:
                    raise

            except Exception as e:
                self.logger.error(f"❌ Unexpected exception during request: {e}")
                raise

        self.logger.error("❌ Max retries reached without success. Failing the request.")
        raise ConnectionError("Failed to complete request after multiple retries.")

    def _handle_graphql_errors(self, errors):
        """
        🔒 Private Method: Handles GraphQL-level errors returned by Monday.com.
        Raises specific exceptions based on error messages for clarity.
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
                self.logger.warning("⌛ Minute limit exceeded! Consider waiting and retrying.")
                raise Exception("Minute limit exceeded")
            elif "Concurrency limit exceeded" in message:
                self.logger.warning("🕑 Concurrency limit exceeded! Throttling requests.")
                raise Exception("Concurrency limit exceeded")
            else:
                self.logger.error(f"💥 GraphQL error: {message}")
                raise Exception(message)

    def _log_complexity(self, data):
        """
        🔒 Private Method: Logs complexity usage if available in the API response data.
        Helps track usage and avoid hitting Monday API limits.
        """
        complexity_info = data.get("data", {}).get("complexity", {})
        if complexity_info:
            query_complexity = complexity_info.get("query")
            before = complexity_info.get("before")
            after = complexity_info.get("after")
            self.logger.debug(f"🔎 Complexity: query={query_complexity}, before={before}, after={after}")
    # endregion

    # region ✨ CRUD Operations & Fetch Methods
    # ============================================================

    # region 🚀 Create Operations
    def create_item(self, board_id: int, group_id: str, name: str, column_values: dict):
        """
        🎨 Create a new item on a board.
        :param board_id: Board ID where the item will be created
        :param group_id: The group_id to place the item in
        :param name: Name of the new item
        :param column_values: Column values in JSON or dict format
        :return: GraphQL response
        """
        self.logger.debug(f"🆕 Creating item on board {board_id}, group '{group_id}', name='{name}'...")
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
        """
        🧩 Create a subitem (child item) under a given parent item.
        :param parent_item_id: The parent item's ID
        :param subitem_name: Subitem name
        :param column_values: Column values in JSON or dict format
        :return: GraphQL response
        """
        self.logger.debug(f"🆕 Creating subitem under parent {parent_item_id} with name='{subitem_name}'...")
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
        """
        🗂️ Create a new contact in the 'Contacts' board.
        :param name: Contact Name
        :return: GraphQL response with ID and name
        """
        self.logger.debug(f"🆕 Creating contact with name='{name}'...")
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
    # endregion

    # region 🔧 Update Operations
    def update_item(self, item_id: str, column_values, type="main"):
        """
        🔧 Updates an existing item, subitem, or contact.
        :param item_id: Pulse (item) ID to update
        :param column_values: Dict/JSON of column values to update
        :param type: 'main', 'subitem', or 'contact' to determine board
        :return: GraphQL response
        """
        self.logger.debug(f"⚙️ Updating item {item_id} on type='{type}' board...")
        query = '''
        mutation ($board_id: ID!, $item_id: ID!, $column_values: JSON!) {
            change_multiple_column_values(board_id: $board_id, item_id: $item_id, column_values: $column_values) {
                id
            }
        }
        '''

        # Determine board_id based on 'type'
        if type == "main":
            board_id = self.PO_BOARD_ID
        elif type == "subitem":
            board_id = self.SUBITEM_BOARD_ID
        elif type == "contact":
            board_id = self.CONTACT_BOARD_ID
        else:
            # fallback, though it's unusual: pass "main" or raise an error
            board_id = self.PO_BOARD_ID

        variables = {
            'board_id': str(board_id),
            'item_id': str(item_id),
            'column_values': column_values
        }
        return self._make_request(query, variables)
    # endregion

    # region 🔎 Fetch: All Items, Subitems, Contacts
    def fetch_all_items(self, board_id, limit=200):
        """
        🔎 Fetches all items from a given board using cursor-based pagination.
        :param board_id: Board ID to fetch items from
        :param limit: # of items to fetch per query
        :return: List of item dicts as returned by Monday
        """
        self.logger.debug(f"📥 Fetching all items from board {board_id} with limit={limit}...")
        all_items = []
        cursor = None

        while True:
            if cursor:
                # Fetch next page
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
                variables = {'cursor': cursor, 'limit': limit}
            else:
                # Initial page
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
                variables = {'board_id': str(board_id), 'limit': limit}

            try:
                response = self._make_request(query, variables)
            except Exception as e:
                self.logger.error(f"❌ Error fetching items: {e}")
                break

            if cursor:
                items_data = response.get('data', {}).get('next_items_page', {})
            else:
                boards_data = response.get('data', {}).get('boards', [])
                if not boards_data:
                    self.logger.warning(f"⚠️ No boards found for board_id {board_id}. Check your permissions or ID.")
                    break
                items_data = boards_data[0].get('items_page', {})

            items = items_data.get('items', [])
            all_items.extend(items)
            cursor = items_data.get('cursor')

            if not cursor:
                self.logger.debug("✅ No more pages left to fetch for this board.")
                break

            self.logger.info(f"📄 Fetched {len(items)} items from board {board_id}. Continuing pagination...")

        return all_items

    def fetch_all_sub_items(self, limit=100):
        """
        🔎 Fetch all subitems from the subitem board, filtering out those without a parent_item.
        Returns only valid subitems that have a parent.
        """
        self.logger.debug(f"📥 Fetching all subitems from subitem board {self.SUBITEM_BOARD_ID}, limit={limit}...")
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
                variables = {'cursor': cursor, 'limit': limit}
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
                variables = {'board_id': str(self.SUBITEM_BOARD_ID), 'limit': limit}

            try:
                response = self._make_request(query, variables)
            except Exception as e:
                self.logger.error(f"❌ Error fetching subitems: {e}")
                break

            if cursor:
                items_data = response.get('data', {}).get('next_items_page', {})
            else:
                boards_data = response.get('data', {}).get('boards', [])
                if not boards_data:
                    self.logger.warning(
                        f"⚠️ No boards found for board_id {self.SUBITEM_BOARD_ID}. Check your permissions or ID."
                    )
                    break
                items_data = boards_data[0].get('items_page', {})

            items = items_data.get('items', [])
            valid_items = [item for item in items if item.get('parent_item') is not None]

            all_items.extend(valid_items)
            cursor = items_data.get('cursor')

            if not cursor:
                self.logger.debug("✅ No more subitem pages left to fetch.")
                break

            self.logger.info(f"📄 Fetched {len(valid_items)} valid subitems from board {self.SUBITEM_BOARD_ID}. Continuing...")

        return all_items

    def get_subitems_in_board(self, project_number=None):
        """
        Fetches subitems from the subitem board (self.SUBITEM_BOARD_ID).

        - If project_number is None, returns all subitems from the subitem board.
        - If project_number is provided, returns all subitems whose
          project_id column (self.monday_util.SUBITEM_PROJECT_ID_COLUMN_ID)
          matches the given project_number.

        For each subitem, we transform its 'column_values' list into a dict:
          "column_values": {
              <column_id>: {
                  "text": <string>,
                  "value": <raw JSON string or None>
              },
              ...
          }

        Returns: a list of subitem dicts like:
        [
          {
            "id": <subitem_id>,
            "name": <subitem_name>,
            "parent_item": {
                "id": <parent_item_id>,
                "name": <parent_item_name>
            },
            "column_values": {
                "<col_id>": { "text": ..., "value": ... },
                ...
            }
          },
          ...
        ]
        """
        board_id = self.SUBITEM_BOARD_ID
        column_id = self.monday_util.SUBITEM_PROJECT_ID_COLUMN_ID
        limit = 200

        self.logger.info(
            f"📥 Fetching subitems from board_id={board_id}, project_number={project_number}"
        )

        all_items = []
        cursor = None

        # ---------------------------
        # If no project_number, fetch everything
        # ---------------------------
        if project_number is None:
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
                                state
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
                    variables = {"cursor": cursor, "limit": limit}
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
                                    state
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
                    variables = {"board_id": str(board_id), "limit": limit}

                try:
                    response = self._make_request(query, variables)
                except Exception as e:
                    self.logger.error(f"❌ Error fetching subitems: {e}")
                    break

                if cursor:
                    items_data = response.get("data", {}).get("next_items_page", {})
                else:
                    boards_data = response.get("data", {}).get("boards", [])
                    if not boards_data:
                        self.logger.warning(f"⚠️ No boards found for board_id={board_id}.")
                        break
                    items_data = boards_data[0].get("items_page", {})

                items = items_data.get("items", [])
                # Optional: exclude subitems that don't have a parent
                valid_items = [
                    item for item in items
                    if item.get("parent_item") is not None and item.get("state") not in ["archived", "deleted"]
                ]
                # Convert column_values from a list to a dict
                for item in valid_items:
                    # Replace the list with a dict version
                    item["column_values"] = {
                        cv["id"]: {
                            "text": cv["text"],
                            "value": cv["value"]
                        }
                        for cv in item.get("column_values", [])
                    }

                all_items.extend(valid_items)
                cursor = items_data.get("cursor")

                if not cursor:
                    self.logger.debug("✅ No more subitem pages to fetch.")
                    break

                self.logger.info(
                    f"🔄 Fetched {len(valid_items)} subitems so far, continuing pagination..."
                )

            return all_items

        # ---------------------------
        # If project_number is provided, fetch only matching subitems
        # ---------------------------
        else:
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
                                state
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
                    variables = {"cursor": cursor, "limit": limit}
                else:
                    # Filter using items_page_by_column_values with project_number
                    query = """
                    query ($board_id: ID!, $column_id: String!, $project_number: String!, $limit: Int!) {
                        complexity { query before after }
                        items_page_by_column_values(
                            board_id: $board_id, 
                            columns: [{column_id: $column_id, column_values: [$project_number]}],
                            limit: $limit
                        ) {
                            cursor
                            items {
                                id
                                name
                                state
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
                        "board_id": str(board_id),
                        "column_id": column_id,
                        "project_number": str(project_number),
                        "limit": limit
                    }

                try:
                    response = self._make_request(query, variables)
                except Exception as e:
                    self.logger.error(f"❌ Error fetching subitems by project_number: {e}")
                    break

                if cursor:
                    items_data = response.get("data", {}).get("next_items_page", {})
                else:
                    items_data = response.get("data", {}).get("items_page_by_column_values", {})

                items = items_data.get("items", [])
                valid_items = [
                    item for item in items
                    if item.get("parent_item") is not None and item.get("state") not in ["archived", "deleted"]
                ]
                # Convert column_values from a list to a dict
                for item in valid_items:
                    item["column_values"] = {
                        cv["id"]: {
                            "text": cv["text"],
                            "value": cv["value"]
                        }
                        for cv in item.get("column_values", [])
                    }

                all_items.extend(valid_items)
                cursor = items_data.get("cursor")

                if not cursor:
                    self.logger.debug("✅ No more pages for filtered subitems.")
                    break

                self.logger.info(
                    f"🔄 Fetched {len(valid_items)} matching subitems so far, continuing pagination..."
                )

            return all_items

    def fetch_all_contacts(self, limit: int = 250) -> list:
        """
        🔎 Fetch all contacts from the 'Contacts' board with pagination.
        :param limit: number of items to fetch per page
        :return: List of contact items
        """
        self.logger.info("📥 Fetching all contacts from the Contacts board...")
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
                variables = {'cursor': cursor, 'limit': limit}
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
                variables = {'board_id': str(self.monday_util.CONTACT_BOARD_ID), 'limit': limit}

            try:
                response = self._make_request(query, variables)
            except Exception as e:
                self.logger.error(f"❌ Error fetching contacts: {e}")
                break

            if cursor:
                items_data = response.get("data", {}).get("next_items_page", {})
            else:
                boards_data = response.get('data', {}).get('boards', [])
                if not boards_data:
                    self.logger.warning(
                        f"⚠️ No boards found for board_id {self.monday_util.CONTACT_BOARD_ID}. Check your permissions or ID."
                    )
                    break
                items_data = boards_data[0].get('items_page', {})

            items = items_data.get('items', [])
            all_items.extend(items)
            cursor = items_data.get('cursor')

            if not cursor:
                self.logger.debug("✅ All contacts fetched successfully.")
                break

            self.logger.debug(f"🔄 Fetched {len(items)} contacts so far. Continuing pagination...")

        return all_items
    # endregion

    # region 🔎 Fetch: Single Items & Groups
    def fetch_item_by_ID(self, id: str):
        """
        🔎 Fetch a single item by ID.
        :param id: Item (pulse) ID
        :return: The item dict, or None if not found
        """
        self.logger.debug(f"🕵️ Searching for item by ID '{id}'...")
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
            variables = {"ID": id}
            response = self._make_request(query, variables)
            items = response['data']["items"]
            if len(items) == 0:
                self.logger.info(f"👀 No item found with ID {id}. Returning None.")
                return None
            return items[0]
        except (TypeError, IndexError, KeyError) as e:
            self.logger.error(f"❌ Error fetching item by ID {id}: {e}")
            raise

    def fetch_group_ID(self, project_id):
        """
        🔎 Fetches the group ID whose title contains the given project_id.
        :param project_id: The project identifier string
        :return: Group ID as string or None if no match
        """
        self.logger.debug(f"🕵️ Searching for group ID matching project_id='{project_id}' on board {self.PO_BOARD_ID}...")
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
            if group["title"] and project_id in group["title"]:
                self.logger.debug(f"✅ Found group '{group['title']}' with ID '{group['id']}'.")
                return group["id"]
        self.logger.debug("🕵️ No matching group found.")
        return None
    # endregion

    # region 🔎 Specialized Fetches
    def fetch_subitem_by_receipt_and_line(self, receipt_number, line_number):
        """
        🔎 Fetch subitem matching receipt_number & line_number from subitem board.
        Replace 'receipt_number_column_id' and 'line_number_column_id' with your real subitem board columns.
        """
        self.logger.debug(f"🔍 Searching subitem by receipt_number='{receipt_number}', line_number='{line_number}'...")
        receipt_number_column_id = "numeric__1"
        line_number_column_id = "numbers_Mjj5uYts"

        query = f'''
        query ($board_id: ID!, $receipt_number: String!, $line_number: String!) {{
            complexity {{ query before after }}
            items_page_by_column_values(
                board_id: $board_id, 
                columns: [
                  {{column_id: "{receipt_number_column_id}", column_values: [$receipt_number]}}, 
                  {{column_id: "{line_number_column_id}", column_values: [$line_number]}}
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
            'receipt_number': str(receipt_number),
            'line_number': str(line_number)
        }

        response = self._make_request(query, variables)
        items = response.get("data", {}).get("items_page_by_column_values", {}).get("items", [])
        return items[0] if items else None

    def fetch_item_by_po_and_project(self, project_id, po_number):
        """
        🔎 Fetch a main item by matching project_id and po_number columns.
        :param project_id: The project identifier
        :param po_number: The Purchase Order number
        :return: GraphQL response with item(s) in 'data.items_page_by_column_values.items'
        """
        self.logger.debug(f"🔍 Searching for item with project_id='{project_id}', po_number='{po_number}'...")
        query = '''
        query ($board_id: ID!, $po_number: String!, $project_id: String!, $project_id_column: String!, $po_column: String!) {
            complexity { query before after }
            items_page_by_column_values (limit: 1, board_id: $board_id, 
                columns: [
                   {column_id: $project_id_column, column_values: [$project_id]}, 
                   {column_id: $po_column, column_values: [$po_number]}
                ]) {
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

    def fetch_subitem_by_po_receipt_line(self, po_number, receipt_number, line_number):
        """
        🔎 Fetch a subitem by matching PO number, receipt number, and line ID columns.
        """
        self.logger.debug(f"🔍 Searching subitem (PO='{po_number}', receipt='{receipt_number}', line_number='{line_number}')...")
        po_number_column_id = self.monday_util.SUBITEM_PO_COLUMN_ID
        receipt_number_column_id = self.monday_util.SUBITEM_ID_COLUMN_ID
        line_number_column_id = self.monday_util.SUBITEM_LINE_NUMBER_COLUMN_ID

        query = f'''
        query ($board_id: ID!, $po_number: String!, $receipt_number: String!, $line_number: String!) {{
            complexity {{ query before after }}
            items_page_by_column_values(
                board_id: $board_id, 
                columns: [
                    {{column_id: "{po_number_column_id}", column_values: [$po_number]}},
                    {{column_id: "{receipt_number_column_id}", column_values: [$receipt_number]}},
                    {{column_id: "{line_number_column_id}", column_values: [$line_number]}}
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
            'line_number': str(line_number)
        }

        response = self._make_request(query, variables)
        items = response.get("data", {}).get("items_page_by_column_values", {}).get("items", [])
        if items:
            self.logger.debug(f"✅ Found subitem with ID {items[0]['id']}")
        else:
            self.logger.debug("🕵️ No subitem found for the given PO, receipt, and line.")
        return items[0] if items else None

    def fetch_item_by_name(self, name, board='PO'):
        """
        🔎 Fetch a single item by 'name' column on the specified board.
        :param name: The item's name to search for
        :param board: 'PO', 'Contacts', or fallback to subitem board
        :return: The single matching item dict or None if not found
        """
        self.logger.debug(f"🔎 Searching item by name='{name}' on '{board}' board...")
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

        variables = {'board_id': int(board_id), 'name': str(name)}
        response = self._make_request(query, variables)
        item_list = response["data"]["items_page_by_column_values"]["items"]
        if len(item_list) != 1:
            self.logger.debug("🕵️ No single matching item found or multiple matches encountered.")
            return None
        self.logger.debug(f"✅ Found item with ID={item_list[0]['id']}.")
        return item_list
    # endregion

    # region 👷 Helper & Utility Methods
    def _safe_get_text(self, vals_dict, col_id):
        """
        🛡️ Safe retrieval of text from column_values dict.
        Useful if the value doesn't exist or is None.
        """
        return vals_dict.get(col_id, {}).get("text", "")
    # endregion

    # region 💼 Get Items in Project + Subitems
    def get_items_in_project(self, project_id):
        """
        🔎 Retrieve all items from the PO_BOARD_ID that match a given project_id column value.
        Uses cursor-based pagination if needed.
        :param project_id: The project identifier (string)
        :return: A list of items with column_values as a dict
        """
        self.logger.debug(f"📥 Fetching all items in project_id='{project_id}' from board {self.PO_BOARD_ID} ...")
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
            'limit': 500,
            'cursor': None
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
                        }
                        for cv in it.get("column_values", [])
                    }
                    all_items.append({
                        "id": it["id"],
                        "name": it["name"],
                        "column_values": cv_dict
                    })

                if not cursor:
                    self.logger.debug("✅ No further cursor. All project items fetched.")
                    break

                self.logger.debug("🔄 Found next cursor. Fetching additional items...")
                variables['cursor'] = cursor

            return all_items

        except Exception as e:
            self.logger.exception(f"❌ Error fetching items by project_id='{project_id}': {e}")
            raise

    def get_subitems_for_item(self, item_id):
        """
        🔎 Fetch subitems for a given parent item_id in the main board.
        :param item_id: Main item ID
        :return: List of subitem dicts: { "id": subitem_id, "name": subitem_name, "column_values": {..} }
        """
        self.logger.debug(f"📥 Fetching subitems for item_id={item_id} ...")
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
        variables = {'item_id': str(item_id)}
        try:
            response = self._make_request(query, variables)
            items = response.get("data", {}).get("items", [])
            if not items:
                self.logger.info(f"🕵️ No parent item found with ID {item_id}. Returning empty list.")
                return []

            parent_item = items[0]
            subitems = parent_item.get("subitems", [])
            results = []
            for si in subitems:
                cv_dict = {cv["id"]: cv["text"] for cv in si.get("column_values", [])}
                results.append({"id": si["id"], "name": si["name"], "column_values": cv_dict})
            self.logger.debug(f"✅ Retrieved {len(subitems)} subitems for item {item_id}.")
            return results

        except Exception as e:
            self.logger.exception(f"❌ Error fetching subitems for item_id='{item_id}': {e}")
            raise
    # endregion

    # region 🔁 Batch Operations
    def batch_create_or_update_items(self, batch, project_id, create=True):
        """
        🔁 Batch create or update multiple main items (PO items).
        :param batch: List of dicts -> each has {"db_item": ..., "column_values": {...}, "monday_item_id": ...}
        :param project_id: The project ID (for logging or grouping)
        :param create: True -> create new items. False -> update existing.
        :return: The updated batch with "monday_item_id" filled as needed.
        """
        self.logger.info(
            f"⚙️ Processing a batch of {len(batch)} items for project_id='{project_id}', create={create}..."
        )

        # Helper function to process one sub-batch
        def create_sub_batch(sub_batch):
            """Create items in Monday from a sub-batch."""
            return self.create_items_batch(sub_batch, project_id)

        if create:
            # 1. Split into chunks of 10 items each
            chunk_size = 10
            sub_batches = [
                batch[i: i + chunk_size] for i in range(0, len(batch), chunk_size)
            ]

            self.logger.info(
                f"Splitting batch into {len(sub_batches)} sub-batches of up to {chunk_size} items each."
            )

            # 2. Dispatch parallel creation tasks
            results = []
            with ThreadPoolExecutor() as executor:
                future_to_index = {}
                for idx, sub_batch in enumerate(sub_batches):
                    self.logger.debug(
                        f"Submitting sub-batch #{idx + 1} with {len(sub_batch)} items."
                    )
                    future = executor.submit(create_sub_batch, sub_batch)
                    future_to_index[future] = idx

                # 3. Gather results (wait for all sub-batches to complete)
                for future in as_completed(future_to_index):
                    idx = future_to_index[future]
                    try:
                        sub_result = future.result()  # This is the returned list of items
                        self.logger.debug(f"Sub-batch #{idx + 1} completed.")
                        results.extend(sub_result)
                    except Exception as e:
                        self.logger.exception(
                            f"❌ Error creating sub-batch #{idx + 1}: {e}"
                        )
                        # Optionally raise here if you want to stop everything upon failure
                        raise

            return results

        else:
            # If we're updating items, do them one by one (or you could similarly
            # batch these if that is needed/desired).
            updated_batch = []
            for itm in batch:
                db_item = itm["db_item"]
                column_values = itm["column_values"]
                column_values_json = json.dumps(column_values)
                item_id = itm["monday_item_id"]

                if not item_id:
                    self.logger.warning(
                        f"⚠️ No monday_item_id provided for update. Skipping item: {db_item}"
                    )
                    continue

                try:
                    self.logger.debug(
                        f"🔄 Updating item {item_id} with new column values..."
                    )
                    self.update_item(item_id, column_values_json, type="main")
                    updated_batch.append(itm)
                except Exception as e:
                    self.logger.exception(f"❌ Error updating item {item_id}: {e}")
                    raise

            return updated_batch

    def batch_create_or_update_subitems(self, subitems_batch, parent_item_id, create=True):
        """
        🔁 Batch create or update multiple subitems under a given parent_item_id.
        :param subitems_batch: List of dicts -> each has:
            {
              "db_sub_item": ...,nnb
              "column_values": {...},
              "monday_subitem_id": maybe_id,
              "parent_id": parent_item_id
            }
        :param parent_item_id: Parent item ID
        :param create: If True, create new subitems. Otherwise, update existing ones.
        :return: Updated batch with new subitem IDs if created
        """
        self.logger.info(f"⚙️ Processing a batch of {len(subitems_batch)} subitems for parent_item_id='{parent_item_id}', create={create}...")
        updated_batch = []

        for si in subitems_batch:
            db_sub_item = si.get("db_sub_item")
            column_values = si["column_values"]
            column_values_json = json.dumps(column_values)

            if create:
                # Create subitem
                subitem_name = db_sub_item.get("name", db_sub_item.get("vendor", "Subitem"))
                try:
                    self.logger.debug(f"👶 Creating subitem '{subitem_name}' under parent {parent_item_id}...")
                    create_response = self.create_subitem(parent_item_id, subitem_name, column_values_json)
                    new_id = create_response["data"]["create_subitem"]["id"]
                    si["monday_item_id"] = new_id
                    updated_batch.append(si)
                except Exception as e:
                    self.logger.exception(f"❌ Error creating subitem for {db_sub_item}: {e}")
                    raise
            else:
                # Update existing subitem
                sub_id = si["monday_item_id"]
                try:
                    self.logger.debug(f"🔄 Updating subitem '{sub_id}' with new column values...")
                    self.update_item(sub_id, column_values_json, type="subitem")
                    updated_batch.append(si)
                except Exception as e:
                    self.logger.exception(f"❌ Error updating subitem {sub_id}: {e}")
                    raise

        return updated_batch

    def create_items_batch(self, batch, project_id):
        """
        🎉 Bulk-create multiple items in a single GraphQL request.
        Each item in 'batch' should have a dict: { 'db_item': ..., 'column_values': {...}, 'monday_item_id': None }
        :param batch: Items to create
        :param project_id: Project identifier for logging
        :return: The batch updated with 'monday_item_id' for each newly created item.
        """
        self.logger.info(f"🔨 Bulk-creating {len(batch)} items for project_id='{project_id}' in one request...")

        mutation_parts = []
        for i, itm in enumerate(batch):
            column_values = itm["column_values"]
            column_values_json = json.dumps(column_values)

            # Use 'name' if present, otherwise fallback
            item_name = column_values.get("name", "Unnamed Item")
            safe_item_name = json.dumps(item_name)

            mutation_parts.append(
                f'create{i}: create_item('
                f'board_id: {self.PO_BOARD_ID}, '
                f'item_name: {safe_item_name}, '
                f'column_values: {json.dumps(column_values_json)}) '
                '{ id }'
            )

        mutation_body = " ".join(mutation_parts)
        query = f'mutation {{ {mutation_body} }}'

        response = self._make_request(query)

        # Parse response and assign IDs
        for i, itm in enumerate(batch):
            create_key = f"create{i}"
            created_item = response.get("data", {}).get(create_key)
            if created_item and "id" in created_item:
                itm["monday_item_id"] = created_item["id"]
                self.logger.debug(f"✅ Created item index {i} with new ID {itm['monday_item_id']}")
            else:
                self.logger.warning(f"⚠️ No ID returned for item index {i} in batch.")

        return batch
    # endregion

    # region 💰 Create or Find Items (POs & Subitems)
    def find_or_create_item_in_monday(self, item, column_values):
        """
        🔎 Finds an item by project_id & PO. If it exists, returns it.
        Otherwise, creates a new item.
        :param item: dict with keys ["project_id", "PO", "name", "group_id", ...]
        :param column_values: JSON/dict of column values
        :return: The updated item with "item_pulse_id" assigned
        """
        self.logger.info(f"🔎 Checking if item with project_id='{item['project_id']}' and PO='{item['PO']}' exists...")
        response = self.fetch_item_by_po_and_project(item["project_id"], item["PO"])
        response_item = response["data"]["items_page_by_column_values"]["items"]

        if len(response_item) == 1:
            self.logger.debug("✅ Found existing item. Updating if needed...")
            response_item = response_item[0]
            item["item_pulse_id"] = response_item["id"]

            # Update the name if different (and if PO type isn't CC or PC)
            if response_item["name"] != item["name"] and item["po_type"] not in ("CC", "PC"):
                self.logger.info(f"🔄 Updating item name from '{response_item['name']}' to '{item['name']}'...")
                updated_column_values = self.monday_util.po_column_values_formatter(
                    name=item["name"],
                    contact_pulse_id=item["contact_pulse_id"]
                )
                self.update_item(response_item["id"], updated_column_values)
                return item
            return item

        else:
            self.logger.info("🆕 No matching item found. Creating a new item...")
            response = self.create_item(self.PO_BOARD_ID, item["group_id"], item["name"], column_values)
            try:
                item["item_pulse_id"] = response["data"]['create_item']["id"]
                self.logger.info(f"🎉 Created new item with pulse_id={item['item_pulse_id']}.")
            except Exception as e:
                self.logger.error(f"❌ Response Error: {response}")
                raise e
            return item

    def find_or_create_sub_item_in_monday(self, sub_item, parent_item):
        """
        🔎 Finds or creates a subitem in Monday corresponding to external data (invoice lines, hours, etc.).
        :param sub_item: dict with keys like ["line_number", "date", "due date", "po_number", "vendor", etc.]
        :param parent_item: dict with at least ["item_pulse_id", "status", "name", ...]
        :return: The updated sub_item with "pulse_id" assigned if created or found
        """
        try:
            self.logger.debug(f"🔎 Checking subitem with line_number='{sub_item.get('line_number')}' under parent {parent_item.get('item_pulse_id')}...")
            status = "RTP" if parent_item.get("status") == "RTP" else "PENDING"

            # Format column values for the subitem
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
                line_number=sub_item["line_number"],
                PO=sub_item["po_number"]
            )

            # Try decoding JSON just to confirm it's valid
            try:
                incoming_values = json.loads(incoming_values_json)
            except json.JSONDecodeError as jde:
                self.logger.error(f"❌ JSON decode error for incoming_values: {jde}")
                return sub_item

            # Case 1: sub_item already has a pulse_id
            if "pulse_id" in sub_item and sub_item["pulse_id"]:
                pulse_id = sub_item["pulse_id"]
                self.logger.info(f"🔎 Subitem already has pulse_id={pulse_id}. Checking if it exists on Monday...")
                existing_item = self.fetch_item_by_ID(pulse_id)

                if not existing_item:
                    # If it doesn't actually exist, create it anew
                    self.logger.warning(f"⚠️ pulse_id {pulse_id} not found on Monday. Creating a new subitem.")
                    create_result = self.create_subitem(
                        parent_item["item_pulse_id"],
                        sub_item.get("vendor", parent_item["name"]),
                        incoming_values_json
                    )
                    new_pulse_id = create_result.get('data', {}).get('create_subitem', {}).get('id')
                    if new_pulse_id:
                        sub_item["pulse_id"] = new_pulse_id
                        self.logger.info(f"✅ Created new subitem with pulse_id={new_pulse_id}.")
                    else:
                        self.logger.exception("❌ Failed to create a new subitem. 'id' not found in the response.")
                    return sub_item

                else:
                    # Compare existing columns to incoming
                    existing_vals = list_to_dict(existing_item["column_values"])
                    all_match = True
                    for col_id, new_val in incoming_values.items():
                        existing_val = existing_vals.get(col_id, {}).get("text", "")
                        if str(existing_val) != str(new_val):
                            all_match = False
                            break

                    if all_match:
                        self.logger.info(f"🔎 Subitem {pulse_id} is identical to incoming data. No update needed.")
                        return sub_item
                    else:
                        self.logger.info(f"💾 Updating subitem {pulse_id} due to changes in column values.")
                        self.update_item(pulse_id, incoming_values_json, type="subitem")
                        return sub_item

            else:
                # Case 2: No known pulse_id -> we check if a matching subitem already exists
                self.logger.debug("🕵️ Searching subitem by PO, receipt_number, line_number to avoid duplicates...")
                existing_subitem = self.fetch_subitem_by_po_receipt_line(
                    po_number=sub_item["po_number"],
                    receipt_number=sub_item["detail_item_id"],
                    line_number=sub_item["line_number"]
                )

                if existing_subitem:
                    sub_item["pulse_id"] = existing_subitem["id"]
                    self.logger.info(f"✅ Found existing subitem with ID={existing_subitem['id']}. Not creating duplicate.")
                    return sub_item
                else:
                    self.logger.info("🆕 No matching subitem found. Creating a new subitem.")
                    create_result = self.create_subitem(
                        parent_item["item_pulse_id"],
                        sub_item.get("vendor", parent_item["name"]),
                        incoming_values_json
                    )
                    new_pulse_id = create_result.get('data', {}).get('create_subitem', {}).get('id')
                    if new_pulse_id:
                        sub_item["pulse_id"] = new_pulse_id
                        self.logger.info(f"✅ New subitem created with pulse_id={new_pulse_id}.")
                    else:
                        self.logger.exception("❌ Failed to create a new subitem. 'id' not found in the response.")

            return sub_item

        except Exception as e:
            self.logger.exception(f"🔥 Exception in find_or_create_sub_item_in_monday: {e}")
            return sub_item
    # endregion

    # region 🧾 Contact Utilities
    def parse_tax_number(self, tax_str: str):
        """
        🧾 Removes hyphens (e.g., for SSN '123-45-6789' or EIN '12-3456789') and attempts to parse as int.
        Returns None if parsing fails or if the string is empty.
        """
        if not tax_str:
            self.logger.debug("No tax_str provided. Returning None.")
            return None

        cleaned = tax_str.replace('-', '')
        try:
            parsed = int(cleaned)
            self.logger.debug(f"🧾 Parsed tax number '{tax_str}' -> {parsed}")
            return parsed
        except ValueError:
            self.logger.warning(f"⚠️ Could not parse tax number '{tax_str}' as int after removing hyphens.")
            return None

    def extract_monday_contact_fields(self, contact_item: dict) -> dict:
        """
        🗂️ Converts a Monday contact_item (including its column_values) into a structured dict of fields.
        """
        self.logger.debug(f"📦 Extracting contact fields from item ID={contact_item.get('id')}...")
        column_values = contact_item.get("column_values", [])

        def parse_column_value(cv):
            raw_text = cv.get("text") or ""
            raw_value = cv.get("value")
            if raw_value:
                try:
                    data = json.loads(raw_value)
                    if isinstance(data, dict) and data.get("url"):
                        return data["url"]
                except (ValueError, TypeError):
                    pass
            return raw_text

        parsed_values = {}
        for cv in column_values:
            col_id = cv["id"]
            parsed_values[col_id] = parse_column_value(cv)

        return {
            "pulse_id": contact_item["id"],
            "phone": parsed_values.get(self.monday_util.CONTACT_PHONE),
            "email": parsed_values.get(self.monday_util.CONTACT_EMAIL),
            "address_line_1": parsed_values.get(self.monday_util.CONTACT_ADDRESS_LINE_1),
            "address_line_2": parsed_values.get(self.monday_util.CONTACT_ADDRESS_LINE_2),
            "city": parsed_values.get(self.monday_util.CONTACT_ADDRESS_CITY),
            "zip_code": parsed_values.get(self.monday_util.CONTACT_ADDRESS_ZIP),
            "region": parsed_values.get(self.monday_util.CONTACT_REGION),
            "country": parsed_values.get(self.monday_util.CONTACT_ADDRESS_COUNTRY),
            "tax_type": parsed_values.get(self.monday_util.CONTACT_TAX_TYPE),
            "tax_number_str": parsed_values.get(self.monday_util.CONTACT_TAX_NUMBER),
            "payment_details": parsed_values.get(self.monday_util.CONTACT_PAYMENT_DETAILS),
            "vendor_status": parsed_values.get(self.monday_util.CONTACT_STATUS),
            "tax_form_link": parsed_values.get(self.monday_util.CONTACT_TAX_FORM_LINK),
        }

    def create_contact_in_monday(self, name: str) -> dict:
        """
        ➕ Create a contact in Monday and immediately fetch its full item data.
        :param name: Name of the contact
        :return: The newly created contact item
        """
        self.logger.info(f"➕ Creating new Monday contact with name='{name}'...")
        create_resp = self.create_contact(name)
        new_id = create_resp["data"]["create_item"]["id"]
        self.logger.info(f"✅ Contact created with pulse_id={new_id}. Fetching the new item's data...")
        created_item = self.fetch_item_by_ID(new_id)
        return created_item

    def sync_db_contact_to_monday(self, db_contact):
        """
        🔄 Syncs local DB contact fields to an existing Monday contact.
        :param db_contact: DB contact object with attributes matching your columns
        """
        if not db_contact.pulse_id:
            self.logger.warning(f"⚠️ DB Contact id={db_contact.id} has no pulse_id. Use 'find_or_create_contact_in_monday' first.")
            return

        self.logger.info(f"🔄 Updating Monday contact (pulse_id={db_contact.pulse_id}) with DB fields...")
        column_values = {
            self.monday_util.CONTACT_PHONE: db_contact.phone or "",
            self.monday_util.CONTACT_EMAIL: db_contact.email or "",
            self.monday_util.CONTACT_ADDRESS_LINE_1: db_contact.address_line_1 or "",
            self.monday_util.CONTACT_ADDRESS_LINE_2: db_contact.address_line_2 or "",
            self.monday_util.CONTACT_CITY: db_contact.city or "",
            self.monday_util.CONTACT_STATE: db_contact.state or "",
            self.monday_util.CONTACT_COUNTRY: db_contact.country or "",
            self.monday_util.CONTACT_REGION: db_contact.region or "",
            self.monday_util.CONTACT_ZIP: db_contact.zip_code or "",
            self.monday_util.CONTACT_TAX_TYPE: db_contact.tax_type or "",
            self.monday_util.CONTACT_TAX_NUMBER: str(db_contact.tax_number) if db_contact.tax_number else "",
            self.monday_util.CONTACT_TAX_FORM_LINK: db_contact.tax_form_link or "",
            self.monday_util.CONTACT_PAYMENT_DETAILS: db_contact.payment_details or "",
            self.monday_util.CONTACT_VENDOR_STATUS: db_contact.vendor_status or "",
        }

        try:
            self.update_item(item_id=db_contact.pulse_id, column_values=column_values, type="contact")
            self.logger.info(f"✅ Monday contact (pulse_id={db_contact.pulse_id}) updated successfully.")
        except Exception as sync_ex:
            self.logger.exception(f"❌ Error syncing DB contact to Monday: {sync_ex}")

    def update_monday_tax_form_link(self, pulse_id, new_link):
        """
        ✏️ Update the tax_form_link column for a Monday contact, setting an appropriate link text label.
        """
        if not pulse_id:
            self.logger.warning("⚠️ No pulse_id provided to update Monday link. Aborting update.")
            return

        link_lower = new_link.lower()
        if "w9" in link_lower:
            link_text = "W-9"
        elif "w8-ben-e" in link_lower:
            link_text = "W-8BEN-E"
        elif "w8-ben" in link_lower:
            link_text = "W-8BEN"
        else:
            link_text = "Tax Form"

        link_value = {"url": new_link, "text": link_text}
        column_values = json.dumps({self.monday_util.CONTACT_TAX_FORM_LINK: link_value})

        try:
            self.logger.debug(f"🔗 Updating tax_form_link for pulse_id={pulse_id} to '{new_link}' (label='{link_text}')...")
            self.update_item(item_id=str(pulse_id), column_values=column_values, type="contact")
            self.logger.info(f"✅ Updated tax_form_link for contact (pulse_id={pulse_id}) to '{new_link}'.")
        except Exception as e:
            self.logger.exception(
                f"❌ Failed to update tax_form_link for pulse_id={pulse_id} with '{new_link}': {e}",
                exc_info=True
            )
    # endregion


# region 🎉 Singleton Instance
monday_api = MondayAPI()
# endregion