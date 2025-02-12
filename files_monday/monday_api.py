# region 1: Imports
import json
import logging
import time
from dotenv import load_dotenv
import requests

from utilities.singleton import SingletonMeta
from utilities.config import Config
from monday import MondayClient
from files_monday.monday_util import monday_util

load_dotenv('../.env')
MAX_RETRIES = 3
RETRY_BACKOFF_FACTOR = 2


# endregion

# region 2: Helper Functions
def _parse_subitem_mutation_response(response_data: dict, subitems_batch: list, create: bool) -> list:
    """
    Parses the GraphQL response after a batch subitem mutation and re-associates
    each result with the original subitem dict.
    """
    results = []
    data = response_data.get("data", {})
    keys_sorted = sorted(data.keys())
    for i, key in enumerate(keys_sorted):
        sub_result = data[key]
        original = subitems_batch[i]
        results.append({
            "db_sub_item": original["db_sub_item"],
            "monday_item_id": sub_result.get("id"),
            "mutation_type": "create" if create else "update",
        })
    return results


# endregion

# region 3: MondayAPI Class Definition
class MondayAPI(metaclass=SingletonMeta):
    """
    MondayAPI singleton for interacting with the Monday.com GraphQL API.
    """

    # Define thresholds for complexity-based rate limiting.
    MINIMUM_COMPLEXITY_THRESHOLD = 50
    WAIT_TIME_FOR_COMPLEXITY_RESET = 60  # seconds

    # region 3.1: Initialization
    def __init__(self):
        """
        Initializes logging, sets up API token, board IDs, and batching parameters.
        """
        if not hasattr(self, '_initialized'):
            try:
                self.logger = logging.getLogger('monday_logger')
                self.logger.debug('Initializing MondayAPI singleton... ⚙️')
                self.api_token = Config.MONDAY_API_TOKEN
                if not self.api_token:
                    self.logger.warning('⚠️ MONDAY_API_TOKEN is not set. Check your configuration.')
                self.api_url = 'https://api.monday.com/v2/'
                self.client = MondayClient(self.api_token)
                self.monday_util = monday_util
                self.PO_BOARD_ID = self.monday_util.PO_BOARD_ID
                self.SUBITEM_BOARD_ID = self.monday_util.SUBITEM_BOARD_ID
                self.CONTACT_BOARD_ID = self.monday_util.CONTACT_BOARD_ID
                self.project_id_column = self.monday_util.PO_PROJECT_ID_COLUMN
                self.po_number_column = self.monday_util.PO_NUMBER_COLUMN
                self.logger.info('✅ Monday API initialized successfully 🏗️')
                self.remaining_complexity = None

                # Batching parameters.
                self.subitem_batch_size = 5
                self.subitem_rate_limit_window = 12
                self.last_batch_time = 0

                self.po_batch_size = 5
                self.po_rate_limit_window = 12
                self.last_po_batch_time = 0

                self._initialized = True
            except Exception as init_ex:
                self.logger.exception(f'❌ Error during MondayAPI initialization: {init_ex}')
                raise init_ex

    # endregion

    # region 3.2: Internal Request Method
    def _make_request(self, query: str, variables: dict = None) -> dict:
        """
        Executes a GraphQL request against Monday.com, with retries, rate limiting,
        and now a check against complexity tokens to prevent exhausting them too quickly.
        """
        # Before constructing the query, check if our remaining complexity is low.
        if self.remaining_complexity is not None and self.remaining_complexity < self.MINIMUM_COMPLEXITY_THRESHOLD:
            self.logger.info(
                f"Remaining complexity ({self.remaining_complexity}) is below threshold ({self.MINIMUM_COMPLEXITY_THRESHOLD}). "
                f"Pausing for {self.WAIT_TIME_FOR_COMPLEXITY_RESET} seconds to allow token recovery.")
            time.sleep(self.WAIT_TIME_FOR_COMPLEXITY_RESET)

        # Ensure the query includes complexity metrics if not already present.
        if 'complexity' not in query:
            insertion_index = query.find('{', query.find('query') if 'query' in query else query.find('mutation'))
            if insertion_index != -1:
                query = query[:insertion_index + 1] + ' complexity { query before after } ' + query[
                                                                                              insertion_index + 1:]
        headers = {'Authorization': self.api_token}
        attempt = 0
        response = None
        while attempt < MAX_RETRIES:
            try:
                self.logger.debug(f'📡 Attempt {attempt + 1}/{MAX_RETRIES}: Sending GraphQL request.')
                response = requests.post(
                    self.api_url,
                    json={'query': query, 'variables': variables},
                    headers=headers,
                    timeout=200
                )
                response.raise_for_status()
                data = response.json()
                if 'errors' in data:
                    self._handle_graphql_errors(data['errors'])
                self._log_complexity(data)
                return data
            except requests.exceptions.ConnectionError as ce:
                self.logger.warning(f'⚠️ Connection error: {ce}. Retrying...')
                time.sleep(RETRY_BACKOFF_FACTOR ** (attempt + 1))
                attempt += 1
            except requests.exceptions.HTTPError as he:
                self.logger.error(f'❌ HTTP error: {he}')
                if response and response.status_code == 429:
                    retry_after = int(response.headers.get('Retry-After', 10))
                    self.logger.warning(f'🔄 Rate limit hit. Retrying after {retry_after} seconds.')
                    time.sleep(retry_after)
                    attempt += 1
                else:
                    raise
            except Exception as e:
                self.logger.error(f'❌ Unexpected error: {e}')
                raise
        self.logger.error('❌ Max retries reached. Request failed.')
        raise ConnectionError('Request failed after maximum retries.')

    # endregion

    # region 3.3: Error and Complexity Handlers
    def _handle_graphql_errors(self, errors):
        """
        Processes GraphQL errors and raises exceptions.
        """
        for error in errors:
            message = error.get('message', '')
            if 'ComplexityException' in message:
                self.logger.error(' 💥 ComplexityException encountered.')
                raise Exception('ComplexityException')
            elif 'DAILY_LIMIT_EXCEEDED' in message:
                self.logger.error(' 💥 DAILY_LIMIT_EXCEEDED encountered.')
                raise Exception('DAILY_LIMIT_EXCEEDED')
            elif 'Minute limit rate exceeded' in message:
                self.logger.warning(' ⌛ Minute limit exceeded.')
                raise Exception('Minute limit exceeded')
            elif 'Concurrency limit exceeded' in message:
                self.logger.warning(' 🕑 Concurrency limit exceeded.')
                raise Exception('Concurrency limit exceeded')
            else:
                self.logger.error(f' 💥 GraphQL error: {message}')
                raise Exception(message)

    def _log_complexity(self, data):
        """
        Logs API complexity information from the response.
        """
        complexity = data.get('data', {}).get('complexity', {})
        if complexity:
            before = complexity.get('before')
            after = complexity.get('after')
            self.remaining_complexity = int(after * 0.8) if after is not None else None
            self.logger.debug(
                f"[_log_complexity] Complexity: before={before}, after={after}, remaining={self.remaining_complexity}")

    # endregion

    # region 3.4: Item Methods
    def create_item(self, board_id: int, group_id: str, name: str, column_values: dict) -> dict:
        """
        Creates a new item on a board.
        """
        self.logger.debug(f"[create_item] Creating item on board {board_id} with name '{name}'")
        query = '''
        mutation ($board_id: ID!, $group_id: String, $item_name: String!, $column_values: JSON) {
            create_item(board_id: $board_id, group_id: $group_id, item_name: $item_name, column_values: $column_values) {
                id
            }
        }
        '''
        variables = {'board_id': board_id, 'item_name': name, 'column_values': column_values}
        if group_id:
            variables['group_id'] = group_id
        return self._make_request(query, variables)

    def create_subitem(self, parent_item_id: int, subitem_name: str, column_values: dict) -> dict:
        """
        Creates a subitem under a parent item.
        """
        self.logger.debug(f"[create_subitem] Creating subitem under parent {parent_item_id}")
        query = '''
        mutation ($parent_item_id: ID!, $subitem_name: String!, $column_values: JSON!) {
            create_subitem(parent_item_id: $parent_item_id, item_name: $subitem_name, column_values: $column_values) {
                id
            }
        }
        '''
        variables = {'parent_item_id': parent_item_id, 'subitem_name': subitem_name, 'column_values': column_values}
        return self._make_request(query, variables)

    def update_item(self, item_id: str, column_values, item_type: str = 'main') -> dict:
        """
        Updates an existing item, subitem, or contact.
        """
        self.logger.debug(f"[update_item] Updating item {item_id} on board item_type '{item_type}'")
        query = '''
        mutation ($board_id: ID!, $item_id: ID!, $column_values: JSON!) {
            change_multiple_column_values(board_id: $board_id, item_id: $item_id, column_values: $column_values) {
                id
            }
        }
        '''
        if item_type == 'main':
            board_id = self.PO_BOARD_ID
        elif item_type == 'subitem':
            board_id = self.SUBITEM_BOARD_ID
        elif item_type == 'contact':
            board_id = self.CONTACT_BOARD_ID
        else:
            board_id = self.PO_BOARD_ID
        variables = {'board_id': str(board_id), 'item_id': str(item_id), 'column_values': column_values}
        return self._make_request(query, variables)

    # endregion

    # region 3.5: Batch Mutation Methods
    def batch_create_or_update_items(self, batch: list, project_id: str, create: bool = True) -> list:
        """
        Batch creates or updates items (e.g. POs) using multithreading.
        """
        import concurrent.futures

        self.logger.info(
            f"[batch_create_or_update_items] Processing {len(batch)} items for project {project_id}, create={create}")
        results = []
        futures = []
        idx = 0
        with concurrent.futures.ThreadPoolExecutor() as executor:
            while idx < len(batch):
                chunk = batch[idx: idx + self.po_batch_size]
                query = self._build_batch_item_mutation(chunk, create)
                futures.append(executor.submit(self._make_request, query, None))
                idx += self.po_batch_size
            for future in concurrent.futures.as_completed(futures):
                resp = future.result()
                data = resp.get("data", {})
                for key in sorted(data.keys()):
                    results.append(data[key])
        self.logger.info(f"[batch_create_or_update_items] Completed with {len(results)} submutations.")
        return results

    def _build_batch_item_mutation(self, batch: list, create: bool) -> str:
        """
        Constructs a single GraphQL mutation string for a batch of items.
        """
        mutations = []
        board_id = self.PO_BOARD_ID
        for i, item in enumerate(batch):
            db_item = item.get("db_item", {})
            raw_values = self.monday_util.po_column_values_formatter(
                project_id=db_item.get("project_number"),
                po_number=db_item.get("po_number"),
                tax_id=db_item.get("tax_id"),
                description=db_item.get("description"),
                contact_pulse_id=item.get("monday_contact_id"),
                folder_link=db_item.get("folder_link"),
                status=db_item.get("status"),
                producer_id=db_item.get("producer_id")
            )
            escaped_values = raw_values.replace('"', '\\"')
            column_values_arg = f"\"{escaped_values}\""
            if create:
                item_name = db_item.get("vendor_name", "Unnamed")
                mutation = f'''
                mutation_{i}: create_item(
                    board_id: {board_id},
                    item_name: "{item_name}",
                    column_values: {column_values_arg}
                ) {{
                    id
                    name
                    column_values {{
                        id
                        text
                    }}
                }}
                '''
            else:
                monday_item_id = item.get("monday_item_id")
                mutation = f'''
                mutation_{i}: change_multiple_column_values(
                    board_id: {board_id},
                    item_id: {monday_item_id},
                    column_values: {column_values_arg}
                ) {{
                    id
                    name
                    column_values {{
                        id
                        text
                    }}
                }}
                '''
            mutations.append(mutation.strip())
        return "mutation {" + " ".join(mutations) + "}"

    def batch_create_or_update_subitems(self, subitems_batch: list, create: bool = True) -> list:
        """
        Batch creates or updates subitems (i.e. detail items) on the Subitem board.
        Uses a similar pattern to batch_create_or_update_items but for subitems.
        """
        import concurrent.futures

        self.logger.info(
            f"[batch_create_or_update_subitems] Processing {len(subitems_batch)} subitems, create={create}")
        results = []
        futures = []
        idx = 0
        while idx < len(subitems_batch):
            chunk = subitems_batch[idx: idx + self.subitem_batch_size]
            query = self._build_batch_subitem_mutation(chunk, create)
            futures.append(concurrent.futures.ThreadPoolExecutor().submit(self._make_request, query, None))
            idx += self.subitem_batch_size

        for future in concurrent.futures.as_completed(futures):
            resp = future.result()
            data = resp.get("data", {})
            # The keys should be the mutation aliases (e.g. mutation_0, mutation_1, etc.)
            for key in sorted(data.keys()):
                results.append(data[key])
        self.logger.info(f"[batch_create_or_update_subitems] Completed with {len(results)} submutations.")
        return results

    def _build_batch_subitem_mutation(self, subitems_batch: list, create: bool) -> str:
        """
        Constructs a GraphQL mutation string for a batch of subitems.
        This method uses the subitem-specific fields and mutation calls.
        """
        mutations = []
        board_id = self.SUBITEM_BOARD_ID
        for i, subitem in enumerate(subitems_batch):
            db_sub_item = subitem.get("db_sub_item", {})
            column_values = self.monday_util.subitem_column_values_formatter(
                project_id=db_sub_item.get("project_number"),
                po_number=db_sub_item.get("po_number"),
                detail_number=db_sub_item.get("detail_number"),
                line_number=db_sub_item.get("line_number"),
                description=db_sub_item.get("description"),
                quantity=db_sub_item.get("quantity"),
                rate=db_sub_item.get("rate"),
                date=db_sub_item.get("transaction_date"),
                due_date=db_sub_item.get("due_date"),
                account_number=db_sub_item.get("account_code"),
                link=db_sub_item.get("file_link"),
                OT=db_sub_item.get("ot"),
                fringes=db_sub_item.get("fringes"),
                xero_link=db_sub_item.get("xero_link"),
                status=db_sub_item.get("state")
            )
            escaped_values = column_values.replace('"', '\\"')
            column_values_arg = f"\"{escaped_values}\""
            if create:
                parent_id = subitem.get("parent_id")
                subitem_name = db_sub_item.get("description") or f"Subitem {i}"
                mutation = f'''
                mutation_{i}: create_subitem(
                    parent_item_id: {parent_id},
                    item_name: "{subitem_name}",
                    column_values: {column_values_arg}
                ) {{
                    id
                }}
                '''
            else:
                monday_item_id = subitem.get("monday_item_id")
                mutation = f'''
                mutation_{i}: change_multiple_column_values(
                    board_id: {board_id},
                    item_id: {monday_item_id},
                    column_values: {column_values_arg}
                ) {{
                    id
                }}
                '''
            mutations.append(mutation.strip())
        return "mutation {" + " ".join(mutations) + "}"

    # endregion

    # region 3.6: Fetch Methods
    def fetch_all_items(self, board_id, limit=200) -> list:
        """
        Fetches all items from the specified board using cursor-based pagination.
        """
        self.logger.debug(f"[fetch_all_items] Fetching items from board {board_id} with limit {limit}")
        all_items = []
        cursor = None
        while True:
            if cursor:
                query = '''
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
                '''
                variables = {'cursor': cursor, 'limit': limit}
            else:
                query = '''
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
                '''
                variables = {'board_id': str(board_id), 'limit': limit}
            try:
                response = self._make_request(query, variables)
            except Exception as e:
                self.logger.error(f"[fetch_all_items] Error: {e}")
                break
            if cursor:
                items_data = response.get('data', {}).get('next_items_page', {})
            else:
                boards_data = response.get('data', {}).get('boards', [])
                if not boards_data:
                    self.logger.warning(f"[fetch_all_items] No boards found for board_id {board_id}")
                    break
                items_data = boards_data[0].get('items_page', {})
            items = items_data.get('items', [])
            all_items.extend(items)
            cursor = items_data.get('cursor')
            if not cursor:
                self.logger.debug("[fetch_all_items] No more pages to fetch.")
                break
        return all_items

    def fetch_all_sub_items(self, limit=100) -> list:
        """
        Fetches all subitems from the subitem board.
        """
        self.logger.debug(
            f"[fetch_all_sub_items] Fetching subitems from board {self.SUBITEM_BOARD_ID} with limit {limit}")
        all_items = []
        cursor = None
        while True:
            if cursor:
                query = '''
                query ($cursor: String!, $limit: Int!) {
                    complexity { query before after }
                    next_items_page(cursor: $cursor, limit: $limit) {
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
                '''
                variables = {'cursor': cursor, 'limit': limit}
            else:
                query = '''
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
                '''
                variables = {'board_id': str(self.SUBITEM_BOARD_ID), 'limit': limit}
            try:
                response = self._make_request(query, variables)
            except Exception as e:
                self.logger.error(f"[fetch_all_sub_items] Error: {e}")
                break
            if cursor:
                items_data = response.get('data', {}).get('next_items_page', {})
            else:
                boards_data = response.get('data', {}).get('boards', [])
                if not boards_data:
                    self.logger.warning(f"[fetch_all_sub_items] No boards found for board_id {self.SUBITEM_BOARD_ID}")
                    break
                items_data = boards_data[0].get('items_page', {})
            items = items_data.get('items', [])
            valid_items = [item for item in items if item.get('parent_item') is not None]
            all_items.extend(valid_items)
            cursor = items_data.get('cursor')
            if not cursor:
                self.logger.debug("[fetch_all_sub_items] No more subitem pages to fetch.")
                break
        return all_items

    def get_subitems_in_board(self, project_number=None) -> list:
        """
        Fetches subitems from the subitem board, optionally filtering by project_number.
        """
        board_id = self.SUBITEM_BOARD_ID
        column_id = self.monday_util.SUBITEM_PROJECT_ID_COLUMN_ID
        limit = 200
        self.logger.info(
            f"[get_subitems_in_board] Fetching subitems from board {board_id} with project_number={project_number}")
        all_items = []
        cursor = None
        if project_number is None:
            while True:
                if cursor:
                    query = '''
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
                    '''
                    variables = {'cursor': cursor, 'limit': limit}
                else:
                    query = '''
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
                    '''
                    variables = {'board_id': str(board_id), 'limit': limit}
                try:
                    response = self._make_request(query, variables)
                except Exception as e:
                    self.logger.error(f"[get_subitems_in_board] Error: {e}")
                    break
                if cursor:
                    items_data = response.get('data', {}).get('next_items_page', {})
                else:
                    boards_data = response.get('data', {}).get('boards', [])
                    if not boards_data:
                        self.logger.warning(f"[get_subitems_in_board] No boards found for board_id {board_id}")
                        break
                    items_data = boards_data[0].get('items_page', {})
                items = items_data.get('items', [])
                valid_items = [item for item in items if
                               item.get('parent_item') is not None and item.get('state') not in ['archived', 'deleted']]
                for item in valid_items:
                    item['column_values'] = {cv['id']: {'text': cv['text'], 'value': cv['value']} for cv in
                                             item.get('column_values', [])}
                all_items.extend(valid_items)
                cursor = items_data.get('cursor')
                if not cursor:
                    self.logger.debug("[get_subitems_in_board] No more pages to fetch.")
                    break
            return all_items
        else:
            while True:
                if cursor:
                    query = '''
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
                    '''
                    variables = {'cursor': cursor, 'limit': limit}
                else:
                    query = '''
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
                    '''
                    variables = {'board_id': str(board_id), 'column_id': column_id,
                                 'project_number': str(project_number), 'limit': limit}
                try:
                    response = self._make_request(query, variables)
                except Exception as e:
                    self.logger.error(f"[get_subitems_in_board] Error: {e}")
                    break
                if cursor:
                    items_data = response.get('data', {}).get('next_items_page', {})
                else:
                    items_data = response.get('data', {}).get('items_page_by_column_values', {})
                items = items_data.get('items', [])
                valid_items = [item for item in items if
                               item.get('parent_item') is not None and item.get('state') not in ['archived', 'deleted']]
                for item in valid_items:
                    item['column_values'] = {cv['id']: {'text': cv['text'], 'value': cv['value']} for cv in
                                             item.get('column_values', [])}
                all_items.extend(valid_items)
                cursor = items_data.get('cursor')
                if not cursor:
                    self.logger.debug("[get_subitems_in_board] No more pages for filtered subitems.")
                    break
            return all_items

    def fetch_all_contacts(self, limit: int = 250) -> list:
        """
        Fetches all contacts from the Contacts board using pagination.
        """
        self.logger.info("[fetch_all_contacts] Fetching contacts from the Contacts board...")
        all_items = []
        cursor = None
        while True:
            if cursor:
                query = '''
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
                '''
                variables = {'cursor': cursor, 'limit': limit}
            else:
                query = '''
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
                '''
                variables = {'board_id': str(self.CONTACT_BOARD_ID), 'limit': limit}
            try:
                response = self._make_request(query, variables)
            except Exception as e:
                self.logger.error(f"[fetch_all_contacts] Error: {e}")
                break
            if cursor:
                items_data = response.get('data', {}).get('next_items_page', {})
            else:
                boards_data = response.get('data', {}).get('boards', [])
                if not boards_data:
                    self.logger.warning(f"[fetch_all_contacts] No boards found for board_id {self.CONTACT_BOARD_ID}")
                    break
                items_data = boards_data[0].get('items_page', {})
            items = items_data.get('items', [])
            all_items.extend(items)
            cursor = items_data.get('cursor')
            if not cursor:
                self.logger.debug("[fetch_all_contacts] Completed fetching contacts.")
                break
        return all_items

    def fetch_item_by_ID(self, item_id: str) -> dict:
        """
        Fetches a single item by its ID.
        """
        self.logger.debug(f"[fetch_item_by_ID] Searching for item with ID '{item_id}'")
        try:
            query = '''query ($ID: ID!) {
                            complexity { query before after }
                            items (ids: [$ID]) {
                                id,
                                name,
                                group {
                                    id
                                    title
                                },
                                column_values {
                                    id,
                                    text,
                                    value
                                }
                            }
                        }'''
            variables = {'ID': item_id}
            response = self._make_request(query, variables)
            items = response.get('data', {}).get('items', [])
            if not items:
                self.logger.info(f"[fetch_item_by_ID] No item found with ID {item_id}")
                return {}
            return items[0]
        except Exception as e:
            self.logger.error(f"[fetch_item_by_ID] Error: {e}")
            raise

    # endregion

    # region 3.7: Build Subitem Column Values
    def build_subitem_column_values(self, detail_item: dict) -> dict:
        """
        Constructs column values for a detail item subitem using monday_util's formatter.
        """
        formatted = self.monday_util.subitem_column_values_formatter(
            project_id=detail_item.get('project_number'),
            po_number=detail_item.get('po_number'),
            detail_number=detail_item.get('detail_number'),
            line_number=detail_item.get('line_number'),
            description=detail_item.get('description'),
            quantity=detail_item.get('quantity'),
            rate=detail_item.get('rate'),
            date=detail_item.get('transaction_date'),
            due_date=detail_item.get('due_date'),
            account_number=detail_item.get('account_code'),
            link=detail_item.get('file_link'),
            OT=detail_item.get('ot'),
            fringes=detail_item.get('fringes'),
            xero_link=detail_item.get('xero_link'),
            status=detail_item.get('state')
        )
        return json.loads(formatted)
    # endregion


# endregion

# region 4: Instantiate MondayAPI
monday_api = MondayAPI()
# endregion