# database/monday_database_util.py
import json
import logging

from sqlalchemy import inspect
from sqlalchemy.exc import SQLAlchemyError

from database.db_util import get_db_session
from database.models import MainItem, SubItem, PO, Contact, Vendor, POState

# Configure logging
from logger import logger
from utils import get_po_state

logging.basicConfig(level=logging.DEBUG)


def insert_main_item(item_data):
    # Filter the data
    filtered_data = map_monday_data_to_main_item(item_data)
    with get_db_session() as session:
        existing_item = session.query(MainItem).filter_by(item_id=filtered_data['item_id']).first()
        if existing_item:
            for key, value in filtered_data.items():
                setattr(existing_item, key, value)
        else:
            main_item = MainItem(**filtered_data)
            session.add(main_item)
        # session.commit()  # Already handled in get_db_session context manager


def insert_subitem(subitem_data):

    filtered_data = map_monday_data_to_sub_item(subitem_data)
    try:
        with get_db_session() as session:
            existing_subitem = session.query(SubItem).filter_by(subitem_id=filtered_data['subitem_id']).first()
            if existing_subitem:
                for key, value in filtered_data.items():
                    setattr(existing_subitem, key, value)
            else:
                subitem = SubItem(**filtered_data)
                session.add(subitem)
    except Exception as e:
        logger.error(f"Error Inserting SubItem to DB: {e}")
        # session.commit()  # Already handled in get_db_session context manager


def fetch_all_main_items():
    with get_db_session() as session:
        return session.query(MainItem).all()


def fetch_subitems_for_main_item(main_item_id):
    with get_db_session() as session:
        return session.query(SubItem).filter_by(main_item_id=main_item_id).all()


def fetch_main_items_by_status(status):
    with get_db_session() as session:
        return session.query(MainItem).filter_by(po_status=status).all()


def fetch_subitems_by_main_item_and_status(main_item_id, status):
    with get_db_session() as session:
        return session.query(SubItem).filter_by(main_item_id=main_item_id, status=status).all()


def item_exists_by_monday_id(monday_id, is_subitem=False):
    """
    Checks if an item exists in the database by its monday.com ID.

    Parameters:
    - monday_id (str or int): The monday.com ID of the item to check.
    - is_subitem (bool): Whether to check in the SubItem table (default: False).

    Returns:
    - bool: True if the item exists, False otherwise.
    """
    with get_db_session() as session:
        if is_subitem:
            return session.query(SubItem).filter_by(subitem_id=monday_id).first() is not None
        else:
            return session.query(MainItem).filter_by(item_id=monday_id).first() is not None


def update_main_item_from_monday(main_item_data):
    """
    Updates a MainItem record based on data from Monday.com.

    Parameters:
    - main_item_data (dict): Data for the MainItem table.
    """
    try:
        with get_db_session() as session:
            # Find the MainItem by item_id
            main_item = session.query(main_item_data).filter_by(item_id=main_item_data['item_id']).first()
            if main_item:
                logger.debug(f"Updating MainItem {main_item_data['item_id']} from Monday.com data")
                # Update fields with the provided data
                for key, value in main_item_data.items():
                    setattr(main_item, key, value)
                session.commit()
                logger.info(f"Updated MainItem {main_item_data['item_id']} successfully.")
                return main_item
            else:
                logger.warning(f"MainItem {main_item_data['item_id']} not found in the database.")
                insert_main_item(main_item_data)
                return None
    except SQLAlchemyError as e:
        logger.error(f"Error updating MainItem {main_item_data['item_id']}: {e}")
        raise e


def update_monday_po_status(item_id, status):
    """
    Updates the PO status based on Monday.com data.
    """
    """
    Updates the status of a PO.
    """
    try:
        with get_db_session() as session:
            po = session.query(MainItem).filter_by(item_id=item_id).first()
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
            po = session.query(PO).filter_by(po_number=po_number).first()
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
                vendor = Vendor(vendor_name=contact_data.get('name', 'Unknown Vendor'), contact=contact)
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
    Maps Monday.com data to the MainItem schema.

    Parameters:
    - monday_data (dict): Raw data from Monday.com.

    Returns:
    - dict: A dictionary with keys matching the MainItem schema.
    """
    # Extract column_values into a dictionary for easier access
    column_values = {col['id']: col for col in monday_data.get('column_values', [])}
    # Map Monday data to MainItem schema fields
    main_item_data = {
        "item_id": monday_data.get("id"),
        "name": monday_data.get("name"),
        "project_id": column_values.get("project_id", {}).get("text"),
        "numbers": column_values.get("numbers08", {}).get("text"),
        "description": column_values.get("text6", {}).get("text"),
        "tax_form": extract_url(column_values, "dup__of_invoice"),  # Use helper function
        "amount": column_values.get("subitems_sub_total", {}).get("text"),
        "po_status": column_values.get("status", {}).get("text"),
        "producer_pm": column_values.get("people", {}).get("text"),
        "updated_date": column_values.get("date01", {}).get("text"),
        "folder": extract_url(column_values, "dup__of_tax_form__1")
    }

    return main_item_data


def map_monday_data_to_sub_item(monday_subitem_data):
    """
    Maps Monday.com subitem data to the SubItem schema.

    Parameters:
    - monday_subitem_data (dict): Raw subitem data from Monday.com.
    - main_item_id (str): The ID of the main item to which this subitem belongs.

    Returns:
    - dict: A dictionary with keys matching the SubItem schema.
    """
    # Extract column_values into a dictionary for easier access
    column_values = {col['id']: col for col in monday_subitem_data.get('column_values', [])}

    # Map Monday data to SubItem schema fields
    sub_item_data = {
        "subitem_id": monday_subitem_data.get("id"),
        "main_item_id": monday_subitem_data.get("parent_item", {}).get("id"),
        "status": column_values.get("status4", {}).get("text"),
        "invoice_number": column_values.get("text0", {}).get("text"),
        "description": column_values.get("text98", {}).get("text"),
        "amount": parse_float(column_values.get("numbers9", {}).get("text")),
        "quantity": parse_float(column_values.get("numbers0", {}).get("text")),
        "account_number": column_values.get("dropdown", {}).get("text"),
        "invoice_date": column_values.get("date", {}).get("text"),
        "link": extract_url(column_values, "link"),
        "due_date": column_values.get("date_1__1", {}).get("text"),
        "creation_log": column_values.get("creation_log__1", {}).get("text"),
    }

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
            print(f"Parsing value for {target_id}: {value}")  # Debugging log
            parsed_value = json.loads(value)
            print(parsed_value.get("url"))
            return parsed_value.get("url")
        except (json.JSONDecodeError, TypeError) as e:
            print(f"Error parsing JSON for {target_id}: {e}")
            return None
    else:
        print(f"No value found for {target_id}")
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
        print(f"Error converting value to float: {value}")
        return None