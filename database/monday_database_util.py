# database/monday_database_util.py
import json
from sqlalchemy.exc import SQLAlchemyError
from database.db_util import get_db_session
from database.models import PurchaseOrder, DetailItem, Contact, POState

# Configure logging
from logger import logger
from monday_util import SUBITEM_RATE_COLUMN_ID, SUBITEM_QUANTITY_COLUMN_ID
from utils import get_po_state
# Set SQLAlchemy log levels independently
import logging
logging.getLogger("sqlalchemy.engine.Engine").setLevel(logging.ERROR)
logging.getLogger("sqlalchemy.pool").setLevel(logging.ERROR)
logging.basicConfig(level=logging.ERROR)


def insert_main_item(item_data):
    # Filter the data
    filtered_data = map_monday_data_to_main_item(item_data)
    print("LOGGING FILTERED DATA", filtered_data)
    with get_db_session() as session:
        existing_item = session.query(PurchaseOrder).filter_by(pulse_id=filtered_data['pulse_id']).first()
        if existing_item:
            for key, value in filtered_data.items():
                setattr(existing_item, key, value)
        else:
            main_item = PurchaseOrder(**filtered_data)
            session.add(main_item)
           # session.commit()  # Already handled in get_db_session context manager


def insert_DetailItems(DetailItem_data):
    """
    Inserts a new DetailItem or updates an existing one based on DetailItem_id.
    Only updates fields that have changed.
    """
    for DetailItem in DetailItem_data:
        try:
            # Step 1: Map the data and validate required fields
            filtered_data = map_monday_data_to_sub_item(DetailItem)

            if not filtered_data.get("main_item_id"):
                logger.warning(f"DetailItem {filtered_data['DetailItem_id']} has no parent_item. Skipping.")
                continue  # Skip DetailItems without a parent_item

            # Validate numeric fields
            filtered_data['amount'] = validate_numeric_field(filtered_data.get('amount', ''), 'amount')
            filtered_data['quantity'] = validate_numeric_field(filtered_data.get('quantity', ''), 'quantity')

            with get_db_session() as session:
                existing_DetailItem = session.query(DetailItem).filter_by(DetailItem_id=filtered_data['DetailItem_id']).first()

                if existing_DetailItem:
                    changes = {}
                    for key, value in filtered_data.items():
                        existing_value = getattr(existing_DetailItem, key, None)
                        if existing_value != value:
                            changes[key] = value
                            setattr(existing_DetailItem, key, value)

                    if changes:
                        logger.info(f"Updated DetailItem ID {filtered_data['DetailItem_id']} with changes: {changes}")
                else:
                    new_DetailItem = DetailItem(**filtered_data)
                    session.add(new_DetailItem)
                    logger.info(f"Inserted new DetailItem with ID {filtered_data['DetailItem_id']}")

                session.commit()
        except KeyError as e:
            logger.error(f"KeyError: Missing key {e} in filtered_data for DetailItem: {DetailItem}. Skipping DetailItem.")
        except ValueError as e:
            logger.error(f"ValueError: {e} DetailItem data: {DetailItem}")
        except SQLAlchemyError as e:
            logger.error(f"Database error while processing DetailItem: {e}. DetailItem data: {DetailItem}")
        except Exception as e:
            logger.error(f"Unexpected error while processing DetailItem: {e}. DetailItem data: {DetailItem}")



def fetch_all_main_items():
    with get_db_session() as session:
        return session.query(PurchaseOrder).all()


def fetch_DetailItems_for_main_item(main_item_id):
    with get_db_session() as session:
        return session.query(DetailItem).filter_by(main_item_id=main_item_id).all()


def fetch_main_items_by_status(status):
    with get_db_session() as session:
        return session.query(PurchaseOrder).filter_by(po_status=status).all()


def fetch_DetailItems_by_main_item_and_status(main_item_id, status):
    with get_db_session() as session:
        return session.query(DetailItem).filter_by(main_item_id=main_item_id, status=status).all()


def item_exists_by_monday_id(monday_id, is_DetailItem=False):
    """
    Checks if an item exists in the database by its monday.com ID.

    Parameters:
    - monday_id (str or int): The monday.com ID of the item to check.
    - is_DetailItem (bool): Whether to check in the DetailItem table (default: False).

    Returns:
    - bool: True if the item exists, False otherwise.
    """
    with get_db_session() as session:
        if is_DetailItem:
            return session.query(DetailItem).filter_by(pulse_id=monday_id).first() is not None
        else:
            return session.query(PurchaseOrder).filter_by(pulse_id=monday_id).first() is not None


def patch_DetailItem(DetailItem_id, update_data):
    """
    Patches an existing DetailItem in the local database with the provided update_data.
    """
    try:
        logger.debug(f"Patching DetailItem ID {DetailItem_id} with data: {update_data}")

        with get_db_session() as session:
            existing_DetailItem = session.query(DetailItem).filter_by(DetailItem_id=str(DetailItem_id)).first()

            if existing_DetailItem:
                # Update the specified fields
                for key, value in update_data.items():
                    setattr(existing_DetailItem, key, value)
                logger.info(f"Updated DetailItem with ID {DetailItem_id}")
            else:
                logger.error(f"DetailItem with ID {DetailItem_id} does not exist in the database.")
                return False, f"DetailItem with ID {DetailItem_id} does not exist."
        print(f"Succesful patch of {DetailItem_id} to with {key} to {value}")
        return True, "DetailItem patched successfully."

    except SQLAlchemyError as e:
        logger.error(f"Database error while patching DetailItem: {e}")
        return False, "Database error."
    except Exception as e:
        logger.error(f"Unexpected error while patching DetailItem: {e}")
        return False, "Unexpected error."


def update_main_item_from_monday(main_item_data):
    """
    Updates a PurchaseOrder record based on data from Monday.com.

    Parameters:
    - main_item_data (dict): Data for the PurchaseOrder table.
    """

    try:
        with get_db_session() as session:
            # Find the PurchaseOrder by item_id
            main_item = session.query(PurchaseOrder).filter_by(item_id=main_item_data['item_id']).first()
            if main_item:
                logger.debug(f"Updating PurchaseOrder {main_item_data['item_id']} from Monday.com data")
                # Update fields with the provided data
                for key, value in main_item_data.items():
                    setattr(main_item, key, value)
                session.commit()
                logger.info(f"Updated PurchaseOrder {main_item_data['item_id']} successfully.")
                return main_item
            else:
                logger.warning(f"PurchaseOrder {main_item_data['item_id']} not found in the database.")
                insert_main_item(main_item_data)
                return None
    except SQLAlchemyError as e:
        logger.error(f"Error updating PurchaseOrder {main_item_data['item_id']}: {e}")
        raise e


def update_monday_po_status(item_id, status):
    """
    Updates the PO status based on Monday.com data.
    """
    """
    Updates the status of a PO.
    """
    print("LOGGING FILTERED DATA", item_id)

    try:
        with get_db_session() as session:
            po = session.query(PurchaseOrder).filter_by(pulse_id=item_id).first()
            if po:
                po.po_status = status
                session.commit()
                logger.debug(f"Updated PO {item_id} status to {status}")
            else:
                logger.warning(f"PO {item_id} not found")
    except SQLAlchemyError as e:
        logger.error(f"Error updating PO status: {e}")
        raise e


def link_contact_to_po(po_number, contact_data):
    """
    Links a contact to a PO.
    """
    try:
        with get_db_session() as session:
            po = session.query(PurchaseOrder).filter_by(po_number=po_number).first()
            if not po:
                logger.warning(f"PO {po_number} not found")
                return

            # Find or create the contact
            contact = session.query(Contact).filter_by(contact_id=contact_data['contact_id']).first()
            if not contact:
                contact = Contact(**contact_data)
                session.add(contact)
                logger.debug(f"Created new contact {contact_data['contact_id']}")

            # Link contact to vendor
            if not po.vendor:
                vendor = Contact(name=contact_data.get('name', 'Unknown Vendor'), contact=contact)
                session.add(vendor)
                po.vendor = vendor
            else:
                po.vendor.contact = contact

            session.commit()
            logger.debug(f"Linked contact {contact.contact_id} to PO {po_number}")
    except SQLAlchemyError as e:
        logger.error(f"Error linking contact to PO: {e}")
        raise e


def get_monday_po_state(po_number):
    """
    Retrieves the state of a PO as per Monday.com data.
    """
    return get_po_state(po_number)  # Use the shared function from utils.py


def map_monday_data_to_main_item(monday_data):
    """
    Maps Monday.com data to the PurchaseOrder schema.

    Parameters:
    - monday_data (dict): Raw data from Monday.com.

    Returns:
    - dict: A dictionary with keys matching the PurchaseOrder schema.
    """
    # Extract column_values into a dictionary for easier access
    column_values = {col['id']: col for col in monday_data.get('column_values', [])}

    if column_values.get("status", {}).get("text") == "CC / PC":
        po_type = 'CC / PC'
    else:
        po_type = 'Vendor'

    # Map Monday data to PurchaseOrder schema fields
    main_item_data = {
        "pulse_id": monday_data.get("id"),
        "project_id": column_values.get("project_id", {}).get("text"),
        "po_number": column_values.get("numbers08", {}).get("text"),
        "description": column_values.get("text6", {}).get("text"),
        "tax_form_link": extract_url(column_values, "dup__of_invoice"),  # Use helper function
        "state": column_values.get("status", {}).get("text"),
        "producer": column_values.get("people", {}).get("text"),
        "folder_link": extract_url(column_values, "dup__of_tax_form__1"),
        "po_type":  po_type
    }

    return main_item_data


def map_monday_data_to_sub_item(monday_DetailItem_data):
    """
    Maps Monday.com DetailItem data to the DetailItem schema.

    Parameters:
    - monday_DetailItem_data (dict): Raw DetailItem data from Monday.com.

    Returns:
    - dict: A dictionary with keys matching the DetailItem schema.
    """
    # Extract column_values into a dictionary for easier access
    column_values = {col['id']: col for col in monday_DetailItem_data.get('column_values', [])}

    # Map Monday data to DetailItem schema fields
    sub_item_data = {
        "DetailItem_id": monday_DetailItem_data.get("id"),
        "main_item_id": (monday_DetailItem_data.get("parent_item") or {}).get("id"),  # Safely handle None
        "status": column_values.get("status4", {}).get("text"),
        "invoice_number": column_values.get("text0", {}).get("text"),
        "description": column_values.get("text98", {}).get("text"),
        "amount": column_values.get("numbers9", {}).get("text"),
        "quantity": column_values.get("numbers0", {}).get("text"),
        "account_number": column_values.get("dropdown", {}).get("text"),
        "invoice_date": column_values.get("date", {}).get("text"),
        "link": extract_url(column_values, "link"),
        "due_date": column_values.get("date_1__1", {}).get("text"),
        "creation_log": column_values.get("creation_log__1", {}).get("text"),
    }

    # Validate required fields (e.g., DetailItem_id)
    if not sub_item_data["DetailItem_id"]:
        raise ValueError(f"DetailItem data is missing required 'DetailItem_id'. Data: {monday_DetailItem_data}")

    return sub_item_data


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
        "status4": "status",
        "text0": "invoice_number",
        "text98": "description",
        "numbers9": "amount",
        "numbers0": "quantity",
        "dropdown": "account_number",
        "date": "invoice_date",
        "link": "link",
        "date_1__1": "due_date",
        "creation_log__1": "creation_log",
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
            return None  # Convert empty strings or None to NULL
        return float(value)  # Attempt to convert to a float
    except ValueError:
        logger.error(f"Invalid value for numeric field '{field_name}': {value}. Defaulting to NULL.")
        return None  # Default invalid values to NULL