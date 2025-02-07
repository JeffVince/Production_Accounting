import json
import logging
import os
import re
from datetime import datetime

import requests
from dateutil import parser
from dotenv import load_dotenv

from monday import MondayClient
from utilities.singleton import SingletonMeta


class MondayUtil(metaclass=SingletonMeta):
    """
    A utility class for interacting with the Monday.com API.
    Encapsulates methods for creating, updating, fetching, and deleting
    items, subitems, and contacts.
    """

    MONDAY_API_URL = "https://api.monday.com/v2"
    ACTUALS_BOARD_ID = "7858669780"
    safe_PO_BOARD_ID = "2562607316"
    PO_BOARD_ID = "7969894467"
    CONTACT_BOARD_ID = "2738875399"

    PO_PROJECT_ID_COLUMN = "project_id"
    PO_NUMBER_COLUMN = "numeric__1"
    PO_TAX_COLUMN_ID = "dup__of_invoice"
    PO_DESCRIPTION_COLUMN_ID = "text6"
    PO_CONTACT_CONNECTION_COLUMN_ID = "connect_boards1"
    PO_FOLDER_LINK_COLUMN_ID = "dup__of_tax_form__1"
    PO_PRODUCER_COLUMN_ID = "people"
    PO_TAX_FORM_COLUMN_ID = "mirror__1"

    SUBITEM_NOTES_COLUMN_ID = "payment_notes__1"
    SUBITEM_STATUS_COLUMN_ID = "status4"
    SUBITEM_ID_COLUMN_ID = "numeric__1"
    SUBITEM_DESCRIPTION_COLUMN_ID = "text98"
    SUBITEM_QUANTITY_COLUMN_ID = "numbers0"
    SUBITEM_RATE_COLUMN_ID = "numbers9"
    SUBITEM_DATE_COLUMN_ID = "date"
    SUBITEM_DUE_DATE_COLUMN_ID = "date_1__1"
    SUBITEM_ACCOUNT_NUMBER_COLUMN_ID = "numbers__1"
    SUBITEM_LINK_COLUMN_ID = "link"
    SUBITEM_OT_COLUMN_ID = "numbers0__1"
    SUBITEM_FRINGE_COLUMN_ID = "numbers9__1"
    SUBITEM_LINE_NUMBER_COLUMN_ID = "numbers_Mjj5uYts"
    SUBITEM_PO_COLUMN_ID = "numbers_Mjj60Olh"
    SUBITEM_PROJECT_ID_COLUMN_ID = "numbers_Mjj8k8Yt"
    SUBITEM_XERO_LINK_COLUMN_ID = "link_mkm0s83t"

    CONTACT_NAME = "name"
    CONTACT_PHONE = "phone"
    CONTACT_EMAIL = "email"
    CONTACT_ADDRESS_LINE_1 = "text1"
    CONTACT_ADDRESS_LINE_2 = "text_mkm0s5w9"
    CONTACT_ADDRESS_CITY = "text3"
    CONTACT_ADDRESS_ZIP = "text84"
    CONTACT_REGION = "text19"
    CONTACT_ADDRESS_COUNTRY = "text6"
    CONTACT_TAX_TYPE = "text14"
    CONTACT_TAX_NUMBER = "text2"
    CONTACT_PAYMENT_DETAILS = "status__1"
    CONTACT_PAYMENT_STATUS = "status__1"
    CONTACT_STATUS = "status7__1"
    CONTACT_TAX_FORM_LINK = "link__1"

    MAIN_ITEM_COLUMN_ID_TO_DB_FIELD = {
        "name": "vendor_name",
        "id": "pulse_id",
        PO_PROJECT_ID_COLUMN: "project_id",
        PO_NUMBER_COLUMN: "po_number",
        PO_TAX_COLUMN_ID: "tax_form_link",
        PO_DESCRIPTION_COLUMN_ID: "description",
        PO_CONTACT_CONNECTION_COLUMN_ID: "contact_pulse_id",
        PO_FOLDER_LINK_COLUMN_ID: "folder_link",
        PO_PRODUCER_COLUMN_ID: "producer_id",
    }

    SUB_ITEM_COLUMN_ID_TO_DB_FIELD = {
        SUBITEM_STATUS_COLUMN_ID: "state",
        SUBITEM_ID_COLUMN_ID: "detail_number",
        SUBITEM_DESCRIPTION_COLUMN_ID: "description",
        SUBITEM_QUANTITY_COLUMN_ID: "quantity",
        SUBITEM_RATE_COLUMN_ID: "rate",
        SUBITEM_DATE_COLUMN_ID: "transaction_date",
        SUBITEM_ACCOUNT_NUMBER_COLUMN_ID: "account_number",
        SUBITEM_LINK_COLUMN_ID: "file_link",
    }

    CONTACT_COLUMN_ID_TO_DB_FIELD = {
        CONTACT_PHONE: "phone",
        CONTACT_EMAIL: "email",
        CONTACT_ADDRESS_LINE_1: "address_line_1",
        CONTACT_ADDRESS_LINE_2: "address_line_2",
        CONTACT_ADDRESS_CITY: "city",
        CONTACT_ADDRESS_ZIP: "zip",
        CONTACT_REGION: "region",
        CONTACT_ADDRESS_COUNTRY: "country",
        CONTACT_TAX_TYPE: "tax_type",
        CONTACT_TAX_NUMBER: "tax_ID",
        CONTACT_PAYMENT_DETAILS: "payment_details",
    }

    COLUMN_TYPE_HANDLERS = {
        "dropdown": "handle_dropdown_column",
        "default": "handle_default_column",
        "date": "handle_date_column",
        "color": "handle_status_column",
        "link": "handle_link_column",
        "text": "handle_default_column",
    }

    def __init__(self):
        if not hasattr(self, "_initialized"):
            self.logger = logging.getLogger("monday_logger")
            load_dotenv()
            self.monday_api_token = os.getenv("MONDAY_API_TOKEN")
            self._subitem_board_id = None

            if not self.monday_api_token:
                self.logger.error(
                    "Monday API Token not found. "
                    "Please set it in the environment variables."
                )
                raise EnvironmentError("Missing MONDAY_API_TOKEN")

            self.headers = {
                "Authorization": self.monday_api_token,
                "Content-Type": "application/json",
                "API-Version": "2023-10",
            }

            self.client = MondayClient(self.monday_api_token)
            self._subitem_board_id = self.retrieve_subitem_board_id()
            self.logger.info(
                f"Retrieved subitem board ID: {self._subitem_board_id}"
            )
            self._initialized = True

    @property
    def SUBITEM_BOARD_ID(self):
        return self._subitem_board_id

    def retrieve_subitem_board_id(self):
        """
        Retrieves the subitem board ID by first fetching the subitems
        column ID and then extracting the board ID from its settings.

        Returns:
            str: The subitem board ID.

        Raises:
            Exception: If unable to retrieve the subitem board ID.
        """
        subitems_column_id = self.get_subitems_column_id(self.PO_BOARD_ID)
        subitem_board_id = self.get_subitem_board_id(subitems_column_id)
        return subitem_board_id

    def get_subitems_column_id(self, parent_board_id):
        """
        Retrieves the column ID for subitems in a given board.

        Args:
            parent_board_id (str): The ID of the parent board.

        Returns:
            str: The column ID for subitems.

        Raises:
            Exception: If the subitems column is not found or the
            API request fails.
        """
        query = (
            f"\n        query {{\n            boards(ids: {parent_board_id}) "
            "{\n                columns {\n                    id\n                    "
            "type\n                }\n            }\n        }\n        "
        )

        response = requests.post(
            self.MONDAY_API_URL, headers=self.headers, json={"query": query}
        )
        data = response.json()

        if response.status_code == 200 and "data" in data:
            try:
                columns = data["data"]["boards"][0]["columns"]
                for column in columns:
                    if column["type"] == "subtasks":
                        self.logger.debug(
                            f"[get_subitems_column_id] - "
                            f"Found subitems column ID: {column['id']}"
                        )
                        return column["id"]
            except Exception as e:
                self.logger.error(
                    f"[get_subitems_column_id] - Failed to "
                    f"retrieve columns: {data}"
                )

    def get_subitem_board_id(self, subitems_column_id):
        """
        Retrieves the subitem board ID for a given subitems column ID.

        Args:
            subitems_column_id (str): The ID of the subitems column.

        Returns:
            str: The subitem board ID.

        Raises:
            Exception: If the subitem board ID cannot be retrieved.
        """
        query = (
            f"\n        query {{\n            boards(ids: {self.PO_BOARD_ID}) "
            f'{{\n                columns(ids: "{subitems_column_id}") '
            "{\n                    settings_str\n                }\n            }"
            "\n        }\n        "
        )

        response = requests.post(
            self.MONDAY_API_URL, headers=self.headers, json={"query": query}
        )
        data = response.json()

        if response.status_code == 200 and "data" in data:
            settings_str = data["data"]["boards"][0]["columns"][0]["settings_str"]
            settings = json.loads(settings_str)
            subitem_board_id = settings["boardIds"][0]
            return subitem_board_id
        else:
            raise Exception(
                f"Failed to retrieve subitem board ID: {response.text}"
            )

    def _handle_date_column(self, event):
        """
        Date handler for columns, extracting a single date.
        """
        return event.get("value", {}).get("date", {})

    def _handle_link_column(self, event):
        """
        Handles link column type and extracts URL.
        """
        try:
            return event.get("value", {}).get("url", {})
        except Exception:
            self.logger.warning(
                "[_handle_link_column] - Setting link to None because of "
                "unexpected Monday Link Value."
            )
            return None

    def _handle_dropdown_column(self, event):
        """
        Handles dropdown column type and extracts chosen values.
        """
        try:
            line_number = (
                event.get("value", {})
                .get("chosenValues", [])[0]
                .get("name")
            )
            return line_number
        except Exception:
            self.logger.warning(
                "[_handle_dropdown_column] - Setting Account ID to None "
                "because of unexpected Monday Account Value."
            )
            return None

    def _handle_default_column(self, event):
        """
        Default handler for columns, extracting a single text label.
        """
        if not event or not event.get("value"):
            return None
        return event["value"].get("value")

    def _handle_status_column(self, event):
        """
        Handles status column type and extracts the label text.
        """
        return event.get("value", {}).get("label", {}).get("text")

    def get_column_handler(self, column_type):
        """
        Retrieves the appropriate handler method based on the column type.
        """
        handler_name = self.COLUMN_TYPE_HANDLERS.get(
            column_type, "handle_default_column"
        )
        return getattr(self, f"_{handler_name}")

    def create_item(self, group_id, item_name, column_values):
        """
        Creates a new item in Monday.com within the specified group.

        Args:
            group_id (str): The ID of the group where the item will be created.
            item_name (str): The name of the new item.
            column_values (dict): A dictionary of column IDs and their
                corresponding values.

        Returns:
            str or None: The ID of the created item if successful, else None.
        """
        query = (
            "\n        mutation ($board_id: ID!, $group_id: String!, "
            "$item_name: String!, $column_values: JSON!) {\n            "
            "create_item(\n                board_id: $board_id,\n"
            "                group_id: $group_id,\n                "
            "item_name: $item_name,\n                column_values: "
            "$column_values\n            ) {\n                id\n"
            "                name\n            }\n        }\n        "
        )
        serialized_column_values = json.dumps(column_values)
        variables = {
            "board_id": self.PO_BOARD_ID,
            "group_id": group_id,
            "item_name": item_name,
            "column_values": serialized_column_values,
        }

        self.logger.info(
            f"[create_item] - Creating item with variables: {variables}"
        )

        response = requests.post(
            self.MONDAY_API_URL,
            headers=self.headers,
            json={"query": query, "variables": variables},
        )
        data = response.json()

        if response.status_code == 200:
            if "data" in data and "create_item" in data["data"]:
                item_id = data["data"]["create_item"]["id"]
                self.logger.info(
                    f"[create_item] - Created new item '{item_name}' "
                    f"with ID {item_id}"
                )
                return item_id
            elif "errors" in data:
                self.logger.error(
                    f"[create_item] - Error creating item in Monday.com: "
                    f"{data['errors']}"
                )
                return None
            else:
                self.logger.error(
                    f"[create_item] - Unexpected response structure: {data}"
                )
                return None
        else:
            self.logger.error(
                f"[create_item] - HTTP Error {response.status_code}: "
                f"{response.text}"
            )
            return None

    def update_item_columns(self, item_id, column_values, board="po"):
        """
        Updates multiple columns of an item in Monday.com.

        Args:
            item_id (str): The ID of the item to update.
            column_values (dict): A dictionary of column IDs and their
                corresponding values.
            board (str): Which board to update (e.g. 'po', 'contact',
                or 'subitem').

        Returns:
            bool: True if the update was successful, False otherwise.
        """
        column_values_json = json.dumps(column_values).replace('"', '\\"')

        if board == "po":
            board_id = self.PO_BOARD_ID
        elif board == "contact":
            board_id = self.CONTACT_BOARD_ID
        elif board == "subitem":
            board_id = self.SUBITEM_BOARD_ID
        else:
            board_id = self.PO_BOARD_ID

        query = (
            f"\n        mutation {{\n            "
            f"change_multiple_column_values(\n                board_id: "
            f"{board_id},\n                item_id: {item_id},\n"
            f'                column_values: "{column_values_json}"\n'
            f"            ) {{\n                id\n            }}\n"
            f"        }}\n        "
        )

        self.logger.info(
            f"[update_item_columns] - Updating item {item_id} with columns: "
            f"{column_values}"
        )

        response = requests.post(
            self.MONDAY_API_URL, headers=self.headers, json={"query": query}
        )
        data = response.json()

        if response.status_code == 200:
            if "data" in data:
                self.logger.info(
                    f"[update_item_columns] - Successfully updated item "
                    f"{item_id} in Monday.com."
                )
                return True
            elif "errors" in data:
                self.logger.error(
                    f"[update_item_columns] - Error updating item in "
                    f"Monday.com: {data['errors']}"
                )
                return False
            else:
                self.logger.error(
                    f"[update_item_columns] - Unexpected response "
                    f"structure: {data}"
                )
                return False
        else:
            self.logger.error(
                f"[update_item_columns] - HTTP Error {response.status_code}: "
                f"{response.text}"
            )
            return False

    def po_column_values_formatter(
            self,
            project_id=None,
            po_number=None,
            tax_id=None,
            description=None,
            contact_pulse_id=None,
            folder_link=None,
            status=None,
            producer_id=None,
            name=None,
    ):
        column_values = {}

        # Convert numeric values to strings
        if project_id:
            column_values[self.PO_PROJECT_ID_COLUMN] = str(project_id)
        if name:
            # Convert set to list if needed
            column_values["name"] = list(name) if isinstance(name, set) else name
        if po_number:
            column_values[self.PO_NUMBER_COLUMN] = str(po_number)
        if tax_id:
            column_values[self.PO_TAX_COLUMN_ID] = str(tax_id)
        if description:
            column_values[self.PO_DESCRIPTION_COLUMN_ID] = description
        if contact_pulse_id:
            # Correctly structure the linkedPulseIds as a list of objects with a pulseId key
            column_values[self.PO_CONTACT_CONNECTION_COLUMN_ID] = {
                 "linkedPulseIds": [{"linkedPulseId": contact_pulse_id}]
            }
        if folder_link:
            column_values[self.PO_FOLDER_LINK_COLUMN_ID] = {
                "url": folder_link,
                "text": "📦",
            }
        if producer_id:
            column_values[self.PO_PRODUCER_COLUMN_ID] = {
                "personsAndTeams": [{"id": str(producer_id), "kind": "person"}]
            }

        # Convert any set values to lists (if necessary)
        for key, value in column_values.items():
            if isinstance(value, set):
                column_values[key] = list(value)

        return json.dumps(column_values)

    def prep_po_log_item_for_monday(self, item):
        pass

    def subitem_column_values_formatter(
            self,
            project_id=None,
            po_number=None,
            detail_number=None,
            line_number=None,
            notes=None,
            status=None,
            description=None,
            quantity=None,
            rate=None,
            date=None,
            due_date=None,
            account_number=None,
            link=None,
            OT=None,
            fringes=None,
            xero_link=None,
    ):
        column_values = {}

        if notes:
            column_values[self.SUBITEM_NOTES_COLUMN_ID] = notes
        if status:
            column_values[self.SUBITEM_STATUS_COLUMN_ID] = {"label": status}
        if description:
            column_values[self.SUBITEM_DESCRIPTION_COLUMN_ID] = description

        if quantity is not None:
            try:
                cleaned_quantity = float(str(quantity).replace(",", "").strip())
                # Convert to string for the numeric column
                column_values[self.SUBITEM_QUANTITY_COLUMN_ID] = str(cleaned_quantity)
            except ValueError as e:
                self.logger.error(
                    f"[subitem_column_values_formatter] - Invalid quantity '{quantity}': {e}"
                )
                column_values[self.SUBITEM_QUANTITY_COLUMN_ID] = None

        if rate is not None:
            try:
                cleaned_rate = float(str(rate).replace(",", "").strip())
                column_values[self.SUBITEM_RATE_COLUMN_ID] = str(cleaned_rate)
            except ValueError as e:
                self.logger.error(
                    f"[subitem_column_values_formatter] - Invalid rate '{rate}': {e}"
                )
                column_values[self.SUBITEM_RATE_COLUMN_ID] = None

        if OT is not None:
            try:
                cleaned_OT = float(str(OT).replace(",", "").strip())
                column_values[self.SUBITEM_OT_COLUMN_ID] = str(cleaned_OT)
            except ValueError as e:
                self.logger.error(
                    f"[subitem_column_values_formatter] - Invalid OT '{OT}': {e}"
                )
                column_values[self.SUBITEM_OT_COLUMN_ID] = None

        if fringes is not None:
            try:
                cleaned_fringe = float(str(fringes).replace(",", "").strip())
                column_values[self.SUBITEM_FRINGE_COLUMN_ID] = str(cleaned_fringe)
            except ValueError as e:
                self.logger.error(
                    f"[subitem_column_values_formatter] - Invalid fringes '{fringes}': {e}"
                )
                column_values[self.SUBITEM_FRINGE_COLUMN_ID] = None

        if date:
            try:
                if isinstance(date, str) and date.strip():
                    parsed_date = parser.parse(date.strip())
                elif isinstance(date, datetime):
                    parsed_date = date
                else:
                    raise ValueError("Unsupported date format")
                column_values[self.SUBITEM_DATE_COLUMN_ID] = {
                    "date": parsed_date.strftime("%Y-%m-%d")
                }
            except Exception as e:
                self.logger.error(
                    f"[subitem_column_values_formatter] - Error parsing date '{date}': {e}"
                )

        if due_date:
            try:
                if isinstance(due_date, str) and due_date.strip():
                    parsed_due_date = parser.parse(due_date.strip())
                elif isinstance(due_date, datetime):
                    parsed_due_date = due_date
                else:
                    raise ValueError("Unsupported due_date format")
                column_values[self.SUBITEM_DUE_DATE_COLUMN_ID] = {
                    "date": parsed_due_date.strftime("%Y-%m-%d")
                }
            except Exception as e:
                self.logger.error(
                    f"[subitem_column_values_formatter] - Error parsing due_date '{due_date}': {e}"
                )
                raise

        if account_number:
            try:
                cleaned_account_number = re.sub("[^\\d]", "", str(account_number).strip())
                if cleaned_account_number:
                    column_values[self.SUBITEM_ACCOUNT_NUMBER_COLUMN_ID] = str(
                        int(cleaned_account_number)
                    )
                else:
                    raise ValueError(
                        f"Account number '{account_number}' invalid after cleaning."
                    )
            except (ValueError, TypeError) as e:
                self.logger.error(
                    f"[subitem_column_values_formatter] - Invalid account number '{account_number}': {e}"
                )
                column_values[self.SUBITEM_ACCOUNT_NUMBER_COLUMN_ID] = None

        if link:
            column_values[self.SUBITEM_LINK_COLUMN_ID] = {
                "url": link,
                "text": "🧾",
            }

        if xero_link:
            column_values[self.SUBITEM_XERO_LINK_COLUMN_ID] = {
                "url": xero_link,
                "text": "📊",
            }

        if po_number is not None:
            column_values[self.SUBITEM_PO_COLUMN_ID] = po_number

        if detail_number is not None:
            column_values[self.SUBITEM_ID_COLUMN_ID] = detail_number

        if line_number is not None:
            column_values[self.SUBITEM_LINE_NUMBER_COLUMN_ID] = line_number

        if project_id is not None:
            column_values[self.SUBITEM_PROJECT_ID_COLUMN_ID] = project_id

        for (key, value) in column_values.items():
            if isinstance(value, set):
                column_values[key] = list(value)

        return json.dumps(column_values)

    def create_subitem(self, parent_item_id, subitem_name, column_values):
        """
        Creates a subitem in Monday.com under a given parent item.

        Args:
            parent_item_id (str): The ID of the parent item to attach
                the subitem to.
            subitem_name (str): The name of the subitem.
            column_values (dict): A dictionary of column IDs and their
                corresponding values.

        Returns:
            str or None: The ID of the created subitem if successful,
            else None.
        """
        column_values = {
            k: v for (k, v) in column_values.items() if v is not None
        }
        column_values_json = json.dumps(column_values).replace('"', '\\"')

        query = (
            f'\n        mutation {{\n            create_subitem (\n'
            f'                parent_item_id: "{parent_item_id}",\n'
            f'                item_name: "{subitem_name}",\n'
            f'                column_values: "{column_values_json}"\n'
            f"            ) {{\n                id\n            }}\n"
            f"        }}\n        "
        )

        self.logger.info(
            f"[create_subitem] - Creating subitem under parent "
            f"{parent_item_id} with name '{subitem_name}'."
        )

        response = requests.post(
            self.MONDAY_API_URL, headers=self.headers, json={"query": query}
        )
        data = response.json()

        if response.status_code == 200:
            if "data" in data and "create_subitem" in data["data"]:
                subitem_id = data["data"]["create_subitem"]["id"]
                self.logger.info(
                    f"[create_subitem] - Created subitem with ID {subitem_id}"
                )
                return subitem_id
            elif "errors" in data:
                self.logger.error(
                    f"[create_subitem] - Error creating subitem in "
                    f"Monday.com: {data['errors']}"
                )
                return None
            else:
                self.logger.error(
                    f"[create_subitem] - Unexpected response "
                    f"structures: {data}"
                )
                return None
        else:
            self.logger.error(
                f"[create_subitem] - HTTP Error {response.status_code}: "
                f"{response.text}"
            )
            return None

    def update_subitem_columns(self, subitem_id, column_values):
        """
        Updates the specified columns of a subitem in Monday.com.

        Args:
            subitem_id (str): The ID of the subitem to update.
            column_values (dict): A dictionary where keys are column IDs
                and values are the new values for those columns.

        Returns:
            bool: True if the update was successful, False otherwise.
        """
        column_values_json = json.dumps(column_values).replace('"', '\\"')

        mutation = (
            f"\n        mutation {{\n            "
            f"change_multiple_column_values(\n                board_id: "
            f"{self.SUBITEM_BOARD_ID},\n                item_id: {subitem_id},\n"
            f'                column_values: "{column_values_json}"\n'
            f"            ) {{\n                id\n            }}\n"
            f"        }}\n        "
        )

        self.logger.info(
            f"[update_subitem_columns] - Updating subitem {subitem_id} "
            f"with columns: {column_values}"
        )

        response = requests.post(
            self.MONDAY_API_URL, headers=self.headers, json={"query": mutation}
        )
        data = response.json()

        if response.status_code == 200:
            if "data" in data:
                self.logger.info(
                    f"[update_subitem_columns] - Successfully updated subitem "
                    f"{subitem_id} in Monday.com."
                )
                return True
            elif "errors" in data:
                self.logger.error(
                    f"[update_subitem_columns] - Error updating subitem in "
                    f"Monday.com: {data['errors']}"
                )
                return False
            else:
                self.logger.error(
                    f"[update_subitem_columns] - Unexpected response "
                    f"structure: {data}"
                )
                return False
        else:
            self.logger.error(
                f"[update_subitem_columns] - HTTP Error {response.status_code}: "
                f"{response.text}"
            )
            return False

    def prep_po_log_detail_for_monday(self, item):
        pass

    def link_contact_to_po_item(self, po_item_id, contact_item_id):
        """
        Links a contact item from the Contacts board to a PO item
        in the PO board using the Connect Boards column.

        Args:
            po_item_id (str): The ID of the PO item in the PO board.
            contact_item_id (str): The ID of the contact item in the
                Contacts board.

        Returns:
            bool: True if the link was successful, False otherwise.
        """
        connect_boards_column_id = self.PO_CONTACT_CONNECTION_COLUMN_ID
        column_value = {"linkedPulseIds": [contact_item_id]}
        column_value_json = json.dumps(column_value).replace('"', '\\"')

        mutation = (
            f"\n        mutation {{\n            change_column_value(\n"
            f"                board_id: {self.PO_BOARD_ID},\n"
            f"                item_id: {po_item_id},\n"
            f'                column_id: "{connect_boards_column_id}",\n'
            f'                value: "{column_value_json}"\n'
            f"            ) {{\n                id\n            }}\n"
            f"        }}\n        "
        )

        self.logger.info(
            f"[link_contact_to_po_item] - Linking contact {contact_item_id} "
            f"to PO item {po_item_id}."
        )

        response = requests.post(
            self.MONDAY_API_URL, headers=self.headers, json={"query": mutation}
        )
        data = response.json()

        if response.status_code == 200:
            if "data" in data and "change_column_value" in data["data"]:
                self.logger.info(
                    f"[link_contact_to_po_item] - Successfully linked "
                    f"contact item {contact_item_id} to PO item "
                    f"{po_item_id}."
                )
                return True
            elif "errors" in data:
                self.logger.error(
                    "[link_contact_to_po_item] - Error linking contact to PO "
                    f"item in Monday.com: {data['errors']}"
                )
                return False
            else:
                self.logger.error(
                    f"[link_contact_to_po_item] - Unexpected response "
                    f"structure: {data}"
                )
                return False
        else:
            self.logger.error(
                f"[link_contact_to_po_item] - HTTP Error {response.status_code}: "
                f"{response.text}"
            )
            return False

    def prep_po_log_contact_for_monday(self, item):
        pass

    def validate_monday_request(self, request_headers):
        """
        Validates incoming webhook requests from Monday.com
        using the API token.

        Args:
            request_headers (dict): The headers from the incoming request.

        Returns:
            bool: True if the request is valid, False otherwise.
        """
        token = request_headers.get("Authorization")

        if not token:
            self.logger.warning(
                "[validate_monday_request] - Missing 'Authorization' header."
            )
            return False

        try:
            received_token = token.split()[1]
        except IndexError:
            self.logger.warning(
                "[validate_monday_request] - Invalid 'Authorization' "
                "header format."
            )
            return False

        if received_token != self.monday_api_token:
            self.logger.warning("[validate_monday_request] - Invalid API token.")
            return False

        self.logger.info("[validate_monday_request] - Request validated successfully.")
        return True

    def get_item_data(self, monday_response):
        item_dict = monday_response["data"]["items"][0]
        columns_dict = {item["id"]: item for item in item_dict["column_values"]}
        return item_dict, columns_dict

    def get_contact_pulse_id(self, columns_dict):
        parsed_value = json.loads(columns_dict["value"])
        linked_pulse_id = [
            item["linkedPulseId"]
            for item in parsed_value.get("linkedPulseIds", [])
        ]
        return linked_pulse_id

    def is_main_item_different(self, db_item, monday_item):
        differences = []
        col_vals = monday_item["column_values"]
        linked_pulse_id = None

        if "connect_boards1" in col_vals and col_vals["connect_boards1"]:
            if "value" in col_vals["connect_boards1"]:
                if json.loads(col_vals["connect_boards1"]["value"]):
                    if json.loads(col_vals["connect_boards1"]["value"]).get(
                        "linkedPulseIds"
                    ):
                        linked_pulse_id = (
                            json.loads(
                                col_vals["connect_boards1"]["value"]
                            ).get("linkedPulseIds")[0]["linkedPulseId"]
                        )

        field_map = [
            {
                "field": "project_number",
                "db_value": db_item.get("project_number"),
                "monday_value": col_vals.get("project_id")["text"],
            },
            {
                "field": "contact_name",
                "db_value": db_item.get("contact_name"),
                "monday_value": monday_item.get("name"),
            },
            {
                "field": "PO",
                "db_value": str(db_item.get("po_number")),
                "monday_value": col_vals.get("numeric__1")["text"],
            },
            {
                "field": "description",
                "db_value": db_item.get("description"),
                "monday_value": col_vals.get("text6")["text"],
            },
            {
                "field": "Connected Contact",
                "db_value": db_item.get("contact_pulse_id"),
                "monday_value": linked_pulse_id,
            },
        ]

        for f in field_map:
            db_val = f["db_value"] if f["db_value"] is not None else ""
            mon_val = f["monday_value"] if f["monday_value"] is not None else ""
            db_str = str(db_val).strip()
            mon_str = str(mon_val).strip()

            if db_str != mon_str:
                differences.append(
                    {
                        "field": f["field"],
                        "db_value": db_str,
                        "monday_value": mon_str,
                    }
                )
        return differences

    def is_sub_item_different(self, db_sub_item, monday_sub_item):
        differences = []
        col_vals = monday_sub_item["column_values"]

        def safe_str(val):
            return str(val).strip() if val is not None else ""

        def are_values_equal(db_val, monday_val):
            try:
                return float(db_val) == float(monday_val)
            except ValueError:
                return db_val == monday_val

        field_map = [
            {
                "field": "quantity",
                "db_value": safe_str(db_sub_item.get("quantity")),
                "monday_value": safe_str(
                    col_vals.get(self.SUBITEM_QUANTITY_COLUMN_ID)["text"]
                ),
            },
            {
                "field": "file_link",
                "db_value": safe_str(db_sub_item.get("file_link")),
                "monday_value": safe_str(
                    json.loads(
                        col_vals.get(self.SUBITEM_LINK_COLUMN_ID)["value"]
                        or "{}"
                    ).get("url")
                ),
            },
            {
                "field": "detail_number",
                "db_value": safe_str(db_sub_item.get("detail_number")),
                "monday_value": safe_str(
                    col_vals.get(self.SUBITEM_ID_COLUMN_ID)["text"]
                ),
            },
            {
                "field": "line_number",
                "db_value": safe_str(db_sub_item.get("line_number")),
                "monday_value": safe_str(
                    col_vals.get(self.SUBITEM_LINE_NUMBER_COLUMN_ID)["text"]
                ),
            },
            {
                "field": "rate",
                "db_value": safe_str(db_sub_item.get("rate")),
                "monday_value": safe_str(
                    col_vals.get(self.SUBITEM_RATE_COLUMN_ID)["text"]
                ),
            },
            {
                "field": "account_code",
                "db_value": safe_str(db_sub_item.get("account_code")),
                "monday_value": safe_str(
                    col_vals.get(self.SUBITEM_ACCOUNT_NUMBER_COLUMN_ID)["text"]
                ),
            },
            {
                "field": "ot",
                "db_value": safe_str(db_sub_item.get("ot")),
                "monday_value": safe_str(
                    col_vals.get(self.SUBITEM_OT_COLUMN_ID)["text"]
                ),
            },
            {
                "field": "state",
                "db_value": safe_str(db_sub_item.get("state")),
                "monday_value": safe_str(
                    col_vals.get(self.SUBITEM_STATUS_COLUMN_ID)["text"]
                ),
            },
            {
                "field": "fringes",
                "db_value": safe_str(db_sub_item.get("fringes")),
                "monday_value": safe_str(
                    col_vals.get(self.SUBITEM_FRINGE_COLUMN_ID)["text"]
                ),
            },
            {
                "field": "transaction_date",
                "db_value": safe_str(
                    db_sub_item.get("transaction_date")
                ).split(" ")[0]
                if safe_str(db_sub_item.get("transaction_date"))
                else None,
                "monday_value": safe_str(
                    col_vals[self.SUBITEM_DATE_COLUMN_ID]["text"]
                ),
            },
            {
                "field": "due_date",
                "db_value": safe_str(db_sub_item.get("due_date")).split(" ")[
                    0
                ]
                if safe_str(db_sub_item.get("due_date"))
                else None,
                "monday_value": safe_str(
                    col_vals[self.SUBITEM_DUE_DATE_COLUMN_ID].get("text")
                    if isinstance(
                        col_vals[self.SUBITEM_DUE_DATE_COLUMN_ID], dict
                    )
                    else col_vals[self.SUBITEM_DUE_DATE_COLUMN_ID]
                ),
            },
        ]

        for f in field_map:
            if not are_values_equal(f["db_value"], f["monday_value"]):
                differences.append(
                    {
                        "field": f["field"],
                        "db_value": f["db_value"],
                        "monday_value": f["monday_value"],
                    }
                )
        return differences

    def extract_subitem_identifiers(self, monday_sub_item):
        col_vals = monday_sub_item["column_values"]

        def safe_int(val):
            try:
                return int(val)
            except (ValueError, TypeError):
                return None

        project_id = safe_int(float(col_vals[self.SUBITEM_PROJECT_ID_COLUMN_ID]["text"]))
        po_number = safe_int(float(col_vals[self.SUBITEM_PO_COLUMN_ID]["text"]))
        detail_num = safe_int(float(col_vals[self.SUBITEM_ID_COLUMN_ID]["text"]))
        line_number = safe_int(float(col_vals[self.SUBITEM_LINE_NUMBER_COLUMN_ID]["text"]))

        if (
            project_id is not None
            and po_number is not None
            and (detail_num is not None)
            and (line_number is not None)
        ):
            return (project_id, po_number, detail_num, line_number)
        else:
            self.logger.warning(
                "[extract_subitem_identifiers] - Subitem missing one of the "
                "required identifiers."
            )
            return None

    def _extract_tax_link_from_monday(self, pulse_id, all_monday_contacts):
        """
        Given a contact's Monday pulse_id, find that contact in
        `all_monday_contacts` and return the link's 'url' if it exists.
        """
        if not pulse_id:
            return None

        for c in all_monday_contacts:
            if c["id"] == str(pulse_id):
                for col in c.get("column_values", []):
                    if col["id"] == self.CONTACT_TAX_FORM_LINK:
                        try:
                            val = json.loads(col["value"])
                            return val.get("url")
                        except:
                            return col.get("text")
        return None


monday_util = MondayUtil()