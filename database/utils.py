# database/utils.py
import json
from datetime import datetime

from sqlalchemy.exc import SQLAlchemyError
from database.models import PurchaseOrder, POState, PurchaseOrder, DetailItem
from database.db_util import get_db_session
import logging

from monday_util import SUBITEM_RATE_COLUMN_ID, SUBITEM_QUANTITY_COLUMN_ID

logger = logging.getLogger(__name__)


def get_po_state(item_id):
    """
    Gets the current state of a PO.
    """
    try:
        with get_db_session() as session:
            po = session.query(PurchaseOrder).filter_by(pulse_id=item_id).first()
            if po:
                return po.state
            else:
                logger.warning(f"PO {item_id} not found")
                return None
    except SQLAlchemyError as e:
        logger.error(f"Error retrieving PO state: {e}")
        raise e

def extract_url(column_values, target_id):
    """
    Safely extracts the URL from the 'value' field of a column.

    Parameters:
    - column_values (dict): Dictionary of column values.
    - target_id (str): The ID of the target column.

    Returns:
    - str: Extracted URL or None if not available.
    """
    value = column_values.get(target_id, {}).get("value")
    if value:
        try:
            # print(f"Parsing value for {target_id}: {value}")  # Debugging log
            parsed_value = json.loads(value)
            # print(parsed_value.get("url"))
            return parsed_value.get("url")
        except (json.JSONDecodeError, TypeError) as e:
            # print(f"Error parsing JSON for {target_id}: {e}")
            return None
    # else:
        # print(f"No value found for {target_id}")
    return None


def parse_float(value):
    """
    Safely converts a string to a float.

    Parameters:
    - value (str): The string to convert.

    Returns:
    - float: The converted float or None if conversion fails.
    """
    try:
        return float(value)
    except (ValueError, TypeError):
        # print(f"Error converting value to float: {value}")
        return None


def map_event_to_update_data(event):
    """
    Maps the event data to the fields that need to be updated in the DetailItem.
    """
    column_id = event.get('columnId')
    if not column_id:
        logger.error("Missing 'columnId' in the event.")
        return None, "Missing 'columnId' in the event."

    column_mapping = get_sub_item_column_mapping()
    field_name = column_mapping.get(column_id)

    if not field_name:
        logger.warning(f"No mapping found for column ID: {column_id}")
        return None, f"No mapping found for column ID: {column_id}"

    # Extract the new value based on columnType
    new_value = extract_text(event)

    # Handle special parsing based on the field type
    if column_id in [SUBITEM_RATE_COLUMN_ID, SUBITEM_QUANTITY_COLUMN_ID]:
        parsed_value = parse_float(new_value)
    elif column_id == "link":
        parsed_value = new_value  # Already extracted URL
    else:
        parsed_value = new_value

    update_data = {field_name: parsed_value}
    # logger.debug(f"Mapped update data: {update_data}")
    return update_data, None


def get_sub_item_column_mapping():
    """
    Returns a mapping from column IDs to DetailItem model fields.
    Update this mapping based on your actual column IDs and model fields.
    """
    return {
        "status4": "state",
        "text0": "detail_item_id",
        "text98": "description",
        "numbers9": "rate",
        "numbers0": "quantity",
        "dropdown": "account_number",
        "date": "transaction_date",
        "link": "file_link",
        "date_1__1": "due_date",
        # Add other mappings as necessary
    }


def extract_text(event):
    """
    Extracts the text value based on columnType.
    Returns None if the value is empty or None.
    """
    column_type = event.get('columnType')
    value_field = event.get('value', {})

    if column_type == 'color':
        # For color type columns
        label = value_field.get('label', {}) if value_field else {}
        return label.get('text', '') if label else ''
    elif column_type == 'text':
        # For text type columns
        return value_field.get('value', '') if value_field else ''
    elif column_type == 'numbers':
        # For number type columns
        return value_field.get('value', '') if value_field else ''
    elif column_type == 'link':
        # For link type columns
        return extract_url(value_field.get('value')) if value_field else None
    # Add more columnType handlers as needed
    else:
        logger.warning(f"Unhandled columnType: {column_type}")
        return ''


def validate_numeric_field(value, field_name):
    try:
        if value is None or value == '':
            return 1  # Convert empty strings or None to NULL

        # Remove non-numeric characters like '$' and ','
        clean_value = value.replace('$', '').replace(',', '').strip()

        return float(clean_value)  # Attempt to convert the cleaned value to a float
    except ValueError:
        logger.error(f"Invalid value for numeric field '{field_name}': {value}. Defaulting to NULL.")
        return None  # Default invalid values to NULL


def parse_transaction_date(date_str):
    logger.debug(f"Parsing transaction_date: '{date_str}'")
    try:
        if not date_str or date_str.strip() == "":
            #logger.error(f"Empty date : {date_str}")
            return None  # Return None instead of empty string

        # Handle both date-only and datetime formats
        if " " in date_str:
            parsed_date = datetime.strptime(date_str, "%Y-%m-%d %H:%M")
        else:
            parsed_date = datetime.strptime(date_str, "%Y-%m-%d")

        return parsed_date.date()  # Returns a date object
    except ValueError:
        logger.error(f"Invalid date format: {date_str}")
        return None


def extract_detail_item_id(raw_id):
    """
    Extracts the last two digits from a detail_item_id string and strips leading zeros.

    Parameters:
        raw_id (str): The raw detail_item_id string (e.g., '2516_03_02').

    Returns:
        int: The processed detail_item_id (e.g., 2).
    """
    if not raw_id:
        logger.warning("Empty detail_item_id received. Defaulting to 1.")
        return 1  # Default value if raw_id is empty or None

    try:
        # Split the raw_id by underscores and take the last segment
        last_segment = raw_id.split('_')[-1]
        logger.debug(f"Extracted last segment from detail_item_id '{raw_id}': '{last_segment}'")

        # Strip leading zeros
        stripped_segment = last_segment.lstrip('0')
        logger.debug(f"Stripped leading zeros from '{last_segment}': '{stripped_segment}'")

        # If stripping results in an empty string, default to '0'
        if stripped_segment == '':
            stripped_segment = '0'

        # Convert to integer
        detail_item_id = int(stripped_segment)
        logger.debug(f"Converted '{stripped_segment}' to integer: {detail_item_id}")

        return detail_item_id
    except (ValueError, AttributeError) as e:
        logger.error(f"Error processing detail_item_id '{raw_id}': {e}. Defaulting to 1.")
        return 1  # Default value in case of error