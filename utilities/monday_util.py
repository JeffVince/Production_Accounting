# utilities/Monday_util.py

import requests


import json
import logging
import os
from dotenv import load_dotenv

from logger import logger

load_dotenv()


# FIND SUBITEM BOARD ID
def get_subitems_column_id(parent_board_id):
    query = f'''
    query {{
        boards(ids: {parent_board_id}) {{
            columns {{
                id
                type
            }}
        }}
    }}
    '''
    headers = {
        'Authorization': os.getenv("MONDAY_API_TOKEN"),
        'Content-Type': 'application/json',
        'API-Version': '2023-10'
    }
    response = requests.post(MONDAY_API_URL, headers=headers, json={'query': query})
    data = response.json()
    if response.status_code == 200 and 'data' in data:
        columns = data['data']['boards'][0]['columns']
        for column in columns:
            if column['type'] == 'subtasks':
                return column['id']
        raise Exception("Subitems column not found.")
    else:
        raise Exception(f"Failed to retrieve columns: {response.text}")


def get_subitem_board_id(parent_board_id):
    subitems_column_id = get_subitems_column_id(parent_board_id)
    query = f'''
    query {{
        boards(ids: {parent_board_id}) {{
            columns(ids: "{subitems_column_id}") {{
                settings_str
            }}
        }}
    }}
    '''
    headers = {
        'Authorization': os.getenv("MONDAY_API_TOKEN"),
        'Content-Type': 'application/json',
        'API-Version': '2023-10'
    }
    response = requests.post(MONDAY_API_URL, headers=headers, json={'query': query})
    data = response.json()
    if response.status_code == 200 and 'data' in data:
        settings_str = data['data']['boards'][0]['columns'][0]['settings_str']
        settings = json.loads(settings_str)
        subitem_board_id = settings['boardIds'][0]
        return subitem_board_id
    else:
        raise Exception(f"Failed to retrieve subitem board ID: {response.text}")

### CONSTANTS ####


MONDAY_API_URL = 'https://api.monday.com/v2'

ACTUALS_BOARD_ID = "7858669780"
PO_BOARD_ID = "2562607316"  # Ensure this is a string
SUBITEM_BOARD_ID = get_subitem_board_id(PO_BOARD_ID)
CONTACT_BOARD_ID = '2738875399'
MONDAY_API_TOKEN = os.getenv("MONDAY_API_TOKEN")

PO_PROJECT_ID_COLUMN = 'project_id'  # Monday.com Project ID column ID
PO_NUMBER_COLUMN = 'numbers08'  # Monday.com PO Number column ID
PO_TAX_COLUMN_ID = 'dup__of_invoice'  # TAX link column ID
PO_DESCRIPTION_COLUMN_ID = 'text6' # Item Description column ID
PO_CONNECTION_COLUMN_ID = 'connect_boards1'
PO_FOLDER_LINK_COLUMN_ID = 'dup__of_tax_form__1'
PO_STATUS_COLUMN_ID = 'status'


SUBITEM_NOTES_COLUMN_ID = 'payment_notes__1'
SUBITEM_STATUS_COLUMN_ID = 'status4'
SUBITEM_ID_COLUMN_ID = 'text0'  # Receipt / Invoice column ID
SUBITEM_DESCRIPTION_COLUMN_ID = 'text98'  # Description column ID
SUBITEM_QUANTITY_COLUMN_ID = 'numbers0'  # Quantity column ID
SUBITEM_RATE_COLUMN_ID = 'numbers9'  # Rate column ID
SUBITEM_DATE_COLUMN_ID = 'date'  # Date column ID
SUBITEM_DUE_DATE_COLUMN_ID ='date_1__1'
SUBITEM_ACCOUNT_NUMBER_COLUMN_ID = 'dropdown'  # Account Number column ID
SUBITEM_LINK_COLUMN_ID ='link' # link column ID

CONTACT_PHONE = 'phone'
CONTACT_EMAIL = 'email'
CONTACT_ADDRESS_LINE_1 = 'text1'
CONTACT_ADDRESS_CITY = 'text3'
CONTACT_ADDRESS_ZIP = 'text84'
CONTACT_ADDRESS_COUNTRY = 'text6'
CONTACT_ADDRESS_TAX_TYPE = 'text14'
CONTACT_ADDRESS_TAX_NUMBER = 'text2'


def get_po_number_and_data(self, item_id):
    """
    Fetches the PO number and item data for a given item ID.
    """
    query = f'''
    query {{
        items(ids: {item_id}) {{
            id
            name
            column_values {{
                id
                text
                value
            }}
        }}
    }}
    '''
    response = requests.post(self.api_url, headers=self.headers, json={'query': query})
    data = response.json()

    if response.status_code == 200 and 'data' in data:
        items = data['data']['items']
        if items:
            item = items[0]
            # Extract PO number based on your specific column ID
            po_number = None
            for col in item['column_values']:
                if col['id'] == 'numbers08':  # Replace with your actual PO Number column ID
                    po_number = col.get('text')
                    break
            return po_number, item
    logger.error(f"Failed to fetch item data for Item ID {item_id}: {data.get('errors')}")
    return None, None


# FIND PROJECT GROUP IN MONDAY
def get_group_id_by_project_id(project_id):
    """
    Retrieves the group ID in Monday.com based on the project ID.
    """
    query = f'''
    query {{
        boards(ids: {PO_BOARD_ID}) {{
            groups {{
                id
                title
            }}
        }}
    }}
    '''

    headers = {
        'Authorization': os.getenv("MONDAY_API_TOKEN"),
        'Content-Type': 'application/json',
        'API-Version': '2023-10'
    }

    response = requests.post(MONDAY_API_URL, headers=headers, json={'query': query})
    data = response.json()

    if response.status_code == 200:
        if 'data' in data and 'boards' in data['data']:
            groups = data['data']['boards'][0]['groups']
            for group in groups:
                title_prefix = group['title'][:len(project_id)]
                logging.info(f"Comparing group title prefix '{title_prefix}' with prefix '{project_id}'")
                if title_prefix == project_id:
                    logging.info(f"Found matching group: {group['title']} with ID {group['id']}")
                    return group['id']
            logging.error(f"No group found with title prefix '{project_id}'")
        elif 'errors' in data:
            logging.error(f"Error fetching groups from Monday.com: {data['errors']}")
            return None
        else:
            logging.error(f"Unexpected response structure: {data}")
            return None
    else:
        logging.error(f"HTTP Error {response.status_code}: {response.text}")
        return None


# FIND PO ITEM
def find_item_by_project_and_po(project_id, po_number):
    """
    Finds an item in Monday.com based on the provided Project ID and PO Number.

    Args:
        board_id (int): The ID of the board in Monday.com.
        project_id (str): The Project ID to match.
        po_number (str): The PO Number to match.

    Returns:
        str: The ID of the matched item if found, otherwise None.

    Raises:
        Exception: If the GraphQL query fails or if there's an HTTP error.
    """
    # Prepare the GraphQL columns argument to filter items by the specific Project ID
    columns_arg = f'''[
        {{
            column_id: "{PO_PROJECT_ID_COLUMN}",
            column_values: ["{project_id}"]
        }}
    ]'''

    # GraphQL query to retrieve items filtered by Project ID
    query = f'''
    query {{
        items_page_by_column_values(
            board_id: {PO_BOARD_ID},
            columns: {columns_arg},
            limit: 100
        ) {{
            items {{
                id
                name
                column_values {{
                    id
                    value
                }}
            }}
        }}
    }}
    '''

    # Headers for authorization and content type
    headers = {
        'Authorization': os.getenv("MONDAY_API_TOKEN"),
        'Content-Type': 'application/json',
        'API-Version': '2023-10'
    }

    # Send the GraphQL request to the Monday.com API
    response = requests.post(MONDAY_API_URL, headers=headers, json={'query': query})
    data = response.json()

    # Check if the request was successful
    if response.status_code == 200:
        if 'data' in data and 'items_page_by_column_values' in data['data']:
            # Extract items from the response
            items = data['data']['items_page_by_column_values']['items']
            #logging.debug(f"Retrieved items: {json.dumps(items, indent=2)}")

            # Search for an item with the matching PO number
            for item in items:
                for column in item['column_values']:
                    if column['id'] == PO_NUMBER_COLUMN:
                        # Extract and parse the column value (expected to be JSON-encoded)
                        value = column.get('value', '')
                        try:
                            parsed_value = json.loads(value)
                        except json.JSONDecodeError:
                            parsed_value = value.strip('"')  # Fallback if not JSON

                        # Check if the parsed value matches the PO number
                        if parsed_value == po_number:
                            logging.info(f"Found item with PO Number '{po_number}': ID {item['id']}")
                            return item['id']

            logging.info(f"No item found with Project ID '{project_id}' and PO Number '{po_number}'.")
            return None
        elif 'errors' in data:
            logging.error(f"Error fetching items from Monday.com: {data['errors']}")
            raise Exception("GraphQL query error")
        else:
            logging.error(f"Unexpected response structure: {data}")
            raise Exception("Unexpected GraphQL response")
    else:
        logging.error(f"HTTP Error {response.status_code}: {response.text}")
        raise Exception(f"HTTP Error {response.status_code}")


# FIND PO SUB-ITEMS
def find_subitem_by_invoice_or_receipt_number(parent_item_id, invoice_receipt_number=None):
    """
    Finds a subitem under a parent item in Monday.com based on the provided invoice number or receipt number.

    Args:
        parent_item_id (int): The ID of the parent item (PO item).
        invoice_number (str, optional): The invoice number to search for.
        receipt_number (str, optional): The receipt number to search for.

    Returns:
        str: The ID of the matched subitem if found, otherwise None.

    Raises:
        Exception: If the GraphQL query fails or if there's an HTTP error.
    """

    # Validate that at least one of invoice_number or receipt_number is provided
    if not invoice_receipt_number:
        raise ValueError("Either invoice_number or receipt_number must be provided.")

    # GraphQL query to retrieve subitems under the parent item
    query = f'''
    query {{
        items(ids: {parent_item_id}) {{
            subitems {{
                id
                name
                column_values {{
                    id
                    text
                }}
            }}
        }}
    }}
    '''

    # Headers for authorization and content type
    headers = {
        'Authorization': os.getenv("MONDAY_API_TOKEN"),
        'Content-Type': 'application/json',
        'API-Version': '2023-10'
    }

    # Send the GraphQL request to the Monday.com API
    response = requests.post(MONDAY_API_URL, headers=headers, json={'query': query})
    data = response.json()

    # Check if the request was successful
    if response.status_code == 200:
        if 'data' in data and 'items' in data['data'] and data['data']['items']:
            # Extract subitems from the response
            subitems = data['data']['items'][0].get('subitems', [])
            logging.debug(f"Retrieved subitems: {json.dumps(subitems, indent=2)}")

            # Determine the search value
            search_value = invoice_receipt_number

            # Iterate through each subitem to find a match
            for subitem in subitems:
                for column in subitem['column_values']:
                    if column['id'] == SUBITEM_ID_COLUMN_ID:
                        # Extract the text value of the column
                        text_value = column.get('text', '').strip()

                        # Check if the text value matches the provided invoice or receipt number
                        if text_value == search_value:
                            logging.info(f"Found subitem with ID {subitem['id']} matching invoice/receipt number.")
                            return subitems

            logging.info(
                f"No subitem found under parent item {parent_item_id} matching the given invoice or receipt number.")
            return None
        elif 'errors' in data:
            logging.error(f"Error fetching subitems from Monday.com: {data['errors']}")
            raise Exception("GraphQL query error")
        else:
            logging.error(f"Unexpected response structure: {data}")
            raise Exception("Unexpected GraphQL response")
    else:
        logging.error(f"HTTP Error {response.status_code}: {response.text}")
        raise Exception(f"HTTP Error {response.status_code}")


# FIND ALL PO SUB-ITEMS
def find_all_po_subitems(parent_item_id):
    """
    Finds a subitem under a parent item in Monday.com based on the provided invoice number or receipt number.

    Args:
        parent_item_id (int): The ID of the parent item (PO item).
        invoice_number (str, optional): The invoice number to search for.
        receipt_number (str, optional): The receipt number to search for.

    Returns:
        str: The ID of the matched subitem if found, otherwise None.

    Raises:
        Exception: If the GraphQL query fails or if there's an HTTP error.
    """

    # GraphQL query to retrieve subitems under the parent item
    query = f'''
    query {{
        items(ids: {parent_item_id}) {{
            subitems {{
                id
                name
                column_values {{
                    id
                    text
                }}
            }}
        }}
    }}
    '''

    # Headers for authorization and content type
    headers = {
        'Authorization': os.getenv("MONDAY_API_TOKEN"),
        'Content-Type': 'application/json',
        'API-Version': '2023-10'
    }

    # Send the GraphQL request to the Monday.com API
    response = requests.post(MONDAY_API_URL, headers=headers, json={'query': query})
    data = response.json()

    # Check if the request was successful
    if response.status_code == 200:
        if 'data' in data and 'items' in data['data'] and data['data']['items']:
            # Extract subitems from the response
            subitems = data['data']['items'][0].get('subitems', [])
            logging.debug(f"Retrieved subitems: {json.dumps(subitems, indent=2)}")
            return subitems
        elif 'errors' in data:
            logging.error(f"Error fetching subitems from Monday.com: {data['errors']}")
            raise Exception("GraphQL query error")
        else:
            logging.error(f"Unexpected response structure: {data}")
            raise Exception("Unexpected GraphQL response")
    else:
        logging.error(f"HTTP Error {response.status_code}: {response.text}")
        raise Exception(f"HTTP Error {response.status_code}")


#  FIND CONTACT
def find_contact_item_by_name(contact_name):
    """
    Finds a contact item in Monday.com based on the provided contact name and retrieves specified column values.

    Args:
        board_id (int): The ID of the board in Monday.com.
        contact_name (str): The name of the contact to match.

    Returns:
        dict: A dictionary containing the item ID and the specified column values if found, otherwise None.

    Raises:
        Exception: If the GraphQL query fails or if there's an HTTP error.
    """
    # Prepare the columns argument for filtering by contact name
    columns_arg = f'''[
            {{
                column_id: "name",
                column_values: ["{contact_name}"]
            }}
        ]'''

    # GraphQL query using items_page_by_column_values with the columns argument
    query = f'''
        query {{
            items_page_by_column_values(
                board_id: {CONTACT_BOARD_ID},
                columns: {columns_arg},
                limit: 1
            ) {{
            items {{
                id
                name
                column_values (ids: [
                    "{CONTACT_PHONE}",
                    "{CONTACT_EMAIL}",
                    "{CONTACT_ADDRESS_LINE_1}",
                    "{CONTACT_ADDRESS_CITY}",
                    "{CONTACT_ADDRESS_ZIP}",
                    "{CONTACT_ADDRESS_COUNTRY}",
                    "{CONTACT_ADDRESS_TAX_TYPE}",
                    "{CONTACT_ADDRESS_TAX_NUMBER}"
                ]) {{
                    id
                    text
                }}
            }}
        }}
    }}
    '''

    # Headers for authorization and content type
    headers = {
        'Authorization': os.getenv("MONDAY_API_TOKEN"),
        'Content-Type': 'application/json',
        'API-Version': '2023-10'
    }

    # Send the GraphQL request to the Monday.com API
    response = requests.post(MONDAY_API_URL, headers=headers, json={'query': query})
    data = response.json()
    print(data)

    # Check if the request was successful
    if response.status_code == 200:
        if 'data' in data:
            # Check if 'items' is in the expected response structure and is not empty
            items = data['data']['items_page_by_column_values'].get('items', [])
            if items:
                item = items[0]
                item_id = item['id']
                column_values = {col['id']: col['text'] for col in item['column_values']}
                logging.info(f"Found contact item with ID {item_id} for contact name '{contact_name}'.")
                return {'item_id': item_id, 'column_values': column_values}
            else:
                # No items were found in the response
                logging.info(f"No item found with contact name '{contact_name}'.")
                return None
        elif 'errors' in data:
            logging.error(f"Error fetching items from Monday.com: {data['errors']}")
            raise Exception("GraphQL query error")
        else:
            logging.error(f"Unexpected response structure: {data}")
            raise Exception("Unexpected GraphQL response")
    else:
        logging.error(f"HTTP Error {response.status_code}: {response.text}")
        raise Exception(f"HTTP Error {response.status_code}")


# FORMAT ITEM COLUMN VALUES
def column_values_formatter(project_id=None, po_number=None, vendor_name=None, folder_link=None,  status=None, contact_id=None, tax_file_link=None, tax_file_type=None, description=None):
    """
    Formats the column values for updating a PO item.

    Args:
        project_id (str, optional): The project ID.
        po_number (str, optional): The PO number.
        vendor_name (str, optional): The vendor's name.
        folder_link (str, optional): The Dropbox folder link.
        status (str, optional): The status of the PO.
        contact_id (str, optional): The contact ID for linking the contact.

    Returns:
        dict: A dictionary of column IDs and their corresponding values.
    """
    column_values = {}
    if project_id:
        column_values[PO_PROJECT_ID_COLUMN] = project_id
    if po_number:
        column_values[PO_NUMBER_COLUMN] = po_number
    if vendor_name:
        column_values[PO_DESCRIPTION_COLUMN_ID] = description
    if folder_link:
        column_values[PO_FOLDER_LINK_COLUMN_ID] = {'url': folder_link, 'text': 'Folder'}
    if tax_file_link:
        column_values[PO_TAX_COLUMN_ID] = {'url': tax_file_link, 'text': tax_file_type}
    if status:
        column_values[PO_STATUS_COLUMN_ID] = {'label': status}
    if contact_id:
        column_values[PO_CONNECTION_COLUMN_ID] = {"item_ids": [contact_id]}  # Link contact directly in the column
    logging.info(column_values)
    return column_values


# CREATE ITEM IN MONDAY BASED ON FILENAME
def create_item(group_id, item_name, column_values):
    """
    Creates a new item in Monday.com within the specified group.

    Args:
        group_id (str): The ID of the group where the item will be created.
        item_name (str): The name of the new item.
        column_values (dict): A dictionary of column IDs and their corresponding values.

    Returns:
        str or None: The ID of the created item if successful, else None.
    """
    query = '''
    mutation ($board_id: ID!, $group_id: String!, $item_name: String!, $column_values: JSON!) {
        create_item(
            board_id: $board_id,
            group_id: $group_id,
            item_name: $item_name,
            column_values: $column_values
        ) {
            id
            name
        }
    }
    '''

    # Serialize `column_values` to JSON string format
    serialized_column_values = json.dumps(column_values)

    variables = {
        'board_id': PO_BOARD_ID,
        'group_id': group_id,
        'item_name': item_name,
        'column_values': serialized_column_values  # Pass serialized values here
    }
    logging.info(variables)

    headers = {
        'Authorization': os.getenv("MONDAY_API_TOKEN"),
        'Content-Type': 'application/json',
        'API-Version': '2023-10'
    }

    response = requests.post(MONDAY_API_URL, headers=headers, json={'query': query, 'variables': variables})
    data = response.json()

    if response.status_code == 200:
        if 'data' in data and 'create_item' in data['data']:
            item_id = data['data']['create_item']['id']
            logging.info(f"Created new item '{item_name}' with ID {item_id}")
            return item_id
        elif 'errors' in data:
            logging.error(f"Error creating item in Monday.com: {data['errors']}")
            return None
        else:
            logging.error(f"Unexpected response structure: {data}")
            return None
    else:
        logging.error(f"HTTP Error {response.status_code}: {response.text}")
        return None


# UPDATE ITEM COLUMNS
def update_item_columns(item_id, column_values):
    # Convert the column values to JSON
    column_values_json = json.dumps(column_values).replace('"', '\\"')

    query = f'''
    mutation {{
        change_multiple_column_values(
            board_id: {PO_BOARD_ID},
            item_id: {item_id},
            column_values: "{column_values_json}"
        ) {{
            id
        }}
    }}
    '''

    headers = {
        'Authorization': os.getenv("MONDAY_API_TOKEN"),
        'Content-Type': 'application/json',
        'API-Version': '2023-10'
    }

    response = requests.post(MONDAY_API_URL, headers=headers, json={'query': query})
    data = response.json()

    if response.status_code == 200:
        if 'data' in data:
            logging.info(f"Successfully updated item {item_id} in Monday.com.")
            return True
        elif 'errors' in data:
            logging.error(f"Error updating item in Monday.com: {data['errors']}")
            return False
        else:
            logging.error(f"Unexpected response structure: {data}")
            return False
    else:
        logging.error(f"HTTP Error {response.status_code}: {response.text}")
        return False


# FORMAT SUBITEM COLUMN VALUES
def subitem_column_values_formatter(notes=None, status=None, file_id=None, description=None, quantity=None, rate=None, date=None, due_date=None, account_number=None, link=None):
    """
    Formats the column values for creating or updating a subitem.

    Args:
        notes (str, optional): Payment notes.
        status (str, optional): Status of the subitem.
        receipt_id (str, optional): Receipt or invoice ID.
        description (str, optional): Description of the subitem.
        quantity (float, optional): Quantity value.
        rate (float, optional): Rate value.
        date (str, optional): Date in 'YYYY-MM-DD' format.
        due_date (str, optional): Due date in 'YYYY-MM-DD' format.
        account_number (str, optional): Account number.
        link (str, optional): URL link.

    Returns:
        dict: A dictionary of subitem column IDs and their corresponding values.
    """

    # Mapping of account numbers to Monday IDs
    account_number_to_id_map = {
        "5300": 1,
        "5000": 2,
        "6040": 3,
        "5330": 4
    }

    column_values = {}
    if notes:
        column_values[SUBITEM_NOTES_COLUMN_ID] = notes
    if status:
        column_values[SUBITEM_STATUS_COLUMN_ID] = {'label': status}
    if file_id:
        column_values[SUBITEM_ID_COLUMN_ID] = file_id
    if description:
        column_values[SUBITEM_DESCRIPTION_COLUMN_ID] = description
    if quantity is not None:
        column_values[SUBITEM_QUANTITY_COLUMN_ID] = quantity
    if rate is not None:
        column_values[SUBITEM_RATE_COLUMN_ID] = rate
    if date:
        column_values[SUBITEM_DATE_COLUMN_ID] = {'date': date}
    if due_date:
        column_values[SUBITEM_DUE_DATE_COLUMN_ID] = {'date': due_date}
    if account_number:
        mapped_id = account_number_to_id_map.get(str(account_number))
        column_values[SUBITEM_ACCOUNT_NUMBER_COLUMN_ID] = {'ids': [str(mapped_id)]}
    if link:
        column_values[SUBITEM_LINK_COLUMN_ID] = {'url': link, 'text': 'Link'}

    return column_values


# CREATE SUB-ITEM IN MONDAY
def create_subitem(parent_item_id, subitem_name, column_values):
    """
    Creates a subitem in Monday.com under a given parent item.

    Args:
        parent_item_id (int): The ID of the parent item to attach the subitem to.
        subitem_name (str): The name of the subitem.
        column_values (dict): A dictionary of column IDs and their corresponding values.

    Returns:
        str or None: The ID of the created subitem if successful, else None.
    """
    # Remove any None values from the column values
    column_values = {k: v for k, v in column_values.items() if v is not None}

    # Convert column_values to JSON string and escape it for GraphQL
    column_values_json = json.dumps(column_values).replace('"', '\\"')

    # GraphQL mutation for creating the subitem
    query = f'''
    mutation {{
        create_subitem (
            parent_item_id: "{parent_item_id}",
            item_name: "{subitem_name}",
            column_values: "{column_values_json}"
        ) {{
            id
        }}
    }}
    '''

    # Set up headers for API request
    headers = {
        'Authorization': os.getenv("MONDAY_API_TOKEN"),
        'Content-Type': 'application/json',
        'API-Version': '2023-10'
    }

    # Execute the request and handle the response
    response = requests.post(MONDAY_API_URL, headers=headers, json={'query': query})
    data = response.json()

    # Process response
    if response.status_code == 200:
        if 'data' in data and 'create_subitem' in data['data']:
            subitem_id = data['data']['create_subitem']['id']
            logging.info(f"Created subitem with ID {subitem_id}")
            return subitem_id
        elif 'errors' in data:
            logging.error(f"Error creating subitem in Monday.com: {data['errors']}")
        else:
            logging.error(f"Unexpected response structure: {data}")
    else:
        logging.error(f"HTTP Error {response.status_code}: {response.text}")

    return None


# UPDATE SUB-ITEM COLUMNS
def update_subitem_columns(subitem_id, column_values):
    """
    Updates the specified columns of a subitem in Monday.com.

    Args:
        subitem_board_id (int): The ID of the board containing the subitem.
        subitem_id (int): The ID of the subitem to update.
        column_values (dict): A dictionary where keys are column IDs and values are the new values for those columns.

    Returns:
        bool: True if the update was successful, False otherwise.

    Raises:
        Exception: If the GraphQL query fails or if there's an HTTP error.
    """
    # Convert the column values to a JSON string and escape double quotes for GraphQL
    column_values_json = json.dumps(column_values).replace('"', '\\"')

    # GraphQL mutation to update the subitem's columns
    mutation = f'''
    mutation {{
        change_multiple_column_values(
            board_id: {SUBITEM_BOARD_ID},
            item_id: {subitem_id},
            column_values: "{column_values_json}"
        ) {{
            id
        }}
    }}
    '''

    headers = {
        'Authorization': os.getenv("MONDAY_API_TOKEN"),
        'Content-Type': 'application/json',
        'API-Version': '2023-10'
    }

    # Send the GraphQL request to update the subitem's columns
    response = requests.post(MONDAY_API_URL, headers=headers, json={'query': mutation})
    data = response.json()

    if response.status_code == 200:
        if 'data' in data:
            logging.info(f"Successfully updated subitem {subitem_id} in Monday.com.")
            return True
        elif 'errors' in data:
            logging.error(f"Error updating subitem in Monday.com: {data['errors']}")
            return False
        else:
            logging.error(f"Unexpected response structure: {data}")
            return False
    else:
        logging.error(f"HTTP Error {response.status_code}: {response.text}")
        return False


# VERIFY IF CONTACT INFO IS COMPLETE
def is_contact_info_complete(column_values):
    """
    Checks if the contact's information is complete based on required fields.
    """
    required_fields = [
        CONTACT_PHONE,
        CONTACT_EMAIL,
        CONTACT_ADDRESS_LINE_1,
        CONTACT_ADDRESS_CITY,
        CONTACT_ADDRESS_ZIP,
        CONTACT_ADDRESS_TAX_NUMBER
    ]
    for field in required_fields:
        if not column_values.get(field):
            return False
    return True


# UPDATE VENDOR DESCRIPTION
def update_vendor_description_in_monday(item_id, vendor_description):
    # Create the column values dictionary with the vendor description
    column_values = {
        'text6': vendor_description  # Using the column ID 'text6' as provided for PO_DESCRIPTION_COLUMN_ID
    }

    # Call the update function to apply this change in Monday.com
    return update_item_columns(item_id, column_values)


# LINK CONTACT TO PO
def link_contact_to_po_item(po_item_id, contact_item_id):
    """
    Links a contact item from the Contacts board to a PO item in the PO board using the Connect Boards column.

    Args:
        po_item_id (int): The ID of the PO item in the PO board.
        contact_item_id (int): The ID of the contact item in the Contacts board.

    Returns:
        bool: True if the link was successful, False otherwise.

    Raises:
        Exception: If the GraphQL mutation fails or if there's an HTTP error.
    """
    # Define the Connect Boards column ID in the PO board
    connect_boards_column_id = 'connect_boards1'

    # Prepare the value for the Connect Boards column
    column_value = {
        "item_ids": [contact_item_id]
    }

    # Convert the column value to a JSON string
    column_value_json = json.dumps(column_value).replace('"', '\\"')

    # GraphQL mutation to update the Connect Boards column
    mutation = f'''
    mutation {{
        change_column_value(
            board_id: {PO_BOARD_ID},
            item_id: {po_item_id},
            column_id: "{connect_boards_column_id}",
            value: "{column_value_json}"
        ) {{
            id
        }}
    }}
    '''

    # Headers for authorization and content type
    headers = {
        'Authorization': os.getenv("MONDAY_API_TOKEN"),
        'Content-Type': 'application/json',
        'API-Version': '2023-10'
    }

    # Send the GraphQL request to the Monday.com API
    response = requests.post(MONDAY_API_URL, headers=headers, json={'query': mutation})
    data = response.json()

    # Check if the request was successful
    if response.status_code == 200:
        if 'data' in data and 'change_column_value' in data['data']:
            logging.info(f"Successfully linked contact item {contact_item_id} to PO item {po_item_id}.")
            return True
        elif 'errors' in data:
            logging.error(f"Error linking contact to PO item in Monday.com: {data['errors']}")
            return False
        else:
            logging.error(f"Unexpected response structure: {data}")
            return False
    else:
        logging.error(f"HTTP Error {response.status_code}: {response.text}")
        return False


def get_items_by_column_values(board_id, column_filters):
    """
    Retrieves all items from a board that match specified column values.

    Args:
        board_id (int): The ID of the board.
        column_filters (list): A list of dictionaries, each containing 'column_id' and 'column_values' keys.

    Returns:
        list: A list of dictionaries, each representing an item with its details.
    """
    query = '''
    query ($board_id: Int!, $columns: [ItemsPageByColumnValuesQuery!]!, $cursor: String) {
        items_page_by_column_values(board_id: $board_id, columns: $columns, cursor: $cursor, limit: 100) {
            cursor
            items {
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

    headers = {
        'Authorization': os.getenv("MONDAY_API_TOKEN"),
        'Content-Type': 'application/json',
        'API-Version': '2023-10'
    }

    items = []
    cursor = None

    while True:
        variables = {'board_id': board_id, 'columns': column_filters, 'cursor': cursor}
        response = requests.post(MONDAY_API_URL, headers=headers, json={'query': query, 'variables': variables})
        data = response.json()

        if response.status_code == 200 and 'data' in data:
            items_page = data['data']['items_page_by_column_values']
            items.extend(items_page['items'])
            cursor = items_page['cursor']
            if not cursor:
                break
        else:
            logging.error(f"Error fetching items: {response.text}")
            break

    return items


def get_all_groups_from_board(board_id):
    """
    Retrieves all groups from a specified board.

    Args:
        board_id (int): The ID of the board.

    Returns:
        list: A list of dictionaries, each representing a group with its details.
    """
    query = '''
    query ($board_id: Int!) {
        boards(ids: [$board_id]) {
            groups {
                id
                title
            }
        }
    }
    '''

    headers = {
        'Authorization': os.getenv("MONDAY_API_TOKEN"),
        'Content-Type': 'application/json',
        'API-Version': '2023-10'
    }

    variables = {'board_id': board_id}
    response = requests.post(MONDAY_API_URL, headers=headers, json={'query': query, 'variables': variables})
    data = response.json()

    if response.status_code == 200 and 'data' in data:
        return data['data']['boards'][0]['groups']
    else:
        logging.error(f"Error fetching groups: {response.text}")
        return []


def get_all_subitems_for_item(parent_item_id):
    """
    Retrieves all subitems for a specified parent item.

    Args:
        parent_item_id (str): The ID of the parent item (as a string).

    Returns:
        list: A list of dictionaries, each representing a subitem with its details.
    """
    query = '''
    query ($parent_item_id: [ID!]) {
        items(ids: $parent_item_id) {
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

    headers = {
        'Authorization': os.getenv("MONDAY_API_TOKEN"),
        'Content-Type': 'application/json',
        'API-Version': '2023-10'
    }

    # Ensure parent_item_id is passed as a list of strings
    variables = {'parent_item_id': [parent_item_id]}
    response = requests.post(MONDAY_API_URL, headers=headers, json={'query': query, 'variables': variables})
    data = response.json()

    if response.status_code == 200 and 'data' in data:
        return data['data']['items'][0].get('subitems', [])
    else:
        logging.error(f"Error fetching subitems: {response.text}")
        return []


def process_monday_update(event_data):
    print("SUCCESS")


def validate_monday_request(request):
    """
    Validate incoming webhook requests from Monday.com using the API token.
    """
    token = request.headers.get('Authorization')
    if not token:
        logging.warning("Missing 'Authorization' header.")
        return False
    # Assuming the token is sent as 'Bearer YOUR_TOKEN'
    if token.split()[1] != MONDAY_API_TOKEN:
        logging.warning("Invalid API token.")
        return False
    return True


def get_all_items_from_board(board_id):
    """
    Retrieves all items from a specified board using pagination.

    Args:
        board_id (str): The ID of the board (as a string).

    Returns:
        list: A list of dictionaries, each representing an item with its details.
    """
    query = '''
    query ($board_id: [ID!], $cursor: String) {
        boards(ids: $board_id) {
            items_page(cursor: $cursor) {
                cursor
                items {
                    id
                    name
                    column_values {
                        id
                        text
                    }
                }
            }
        }
    }
    '''

    headers = {
        'Authorization': os.getenv("MONDAY_API_TOKEN"),
        'Content-Type': 'application/json',
        'API-Version': '2023-10'
    }

    items = []
    cursor = None

    while True:
        variables = {'board_id': [board_id], 'cursor': cursor}  # Ensure board_id is passed as a list of strings
        response = requests.post(MONDAY_API_URL, headers=headers, json={'query': query, 'variables': variables})
        data = response.json()

        if response.status_code == 200 and 'data' in data:
            items_page = data['data']['boards'][0]['items_page']
            items.extend(items_page['items'])
            cursor = items_page['cursor']
            if not cursor:
                break
        else:
            logging.error(f"Error fetching items: {response.text}")
            break

    return items


def get_po_number_and_data(item_id):
    """
    Fetches the PO number and item data for a given item ID.
    """
    query = f'''
    query {{
        items(ids: {item_id}) {{
            id
            name
            column_values {{
                id
                text
                value
            }}
        }}
    }}
    '''
    response = requests.post(MONDAY_API_URL, headers=headers, json={'query': query})
    data = response.json()

    if response.status_code == 200 and 'data' in data:
        items = data['data']['items']
        if items:
            item = items[0]
            # Extract PO number based on your specific column ID
            po_number = None
            for col in item['column_values']:
                if col['id'] == PO_NUMBER_COLUMN:
                    po_number = col.get('text')
                    break
            return po_number, item
    logging.error(f"Failed to fetch item data for Item ID {item_id}: {data.get('errors')}")
    return None, None

def get_subitem_data(subitem_id):
    """
    Fetches the subitem data for a given subitem ID.
    """
    query = f'''
    query {{
        items(ids: {subitem_id}) {{
            id
            name
            parent_item {{
                id
            }}
            column_values {{
                id
                text
                value
            }}
        }}
    }}
    '''
    response = requests.post(MONDAY_API_URL, headers=headers, json={'query': query})
    data = response.json()

    if response.status_code == 200 and 'data' in data:
        items = data['data']['items']
        if items:
            subitem = items[0]
            return subitem
    logging.error(f"Failed to fetch subitem data for SubItem ID {subitem_id}: {data.get('errors')}")
    return None