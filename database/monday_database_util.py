# database/monday_database_util.py
import json
from datetime import datetime

from sqlalchemy.exc import SQLAlchemyError
from database.db_util import get_db_session
from database.models import PurchaseOrder, DetailItem, Contact, POState

# Configure logging
from logger import logger
from monday_util import SUBITEM_RATE_COLUMN_ID, SUBITEM_QUANTITY_COLUMN_ID
from utils import get_po_state, extract_detail_item_id, parse_transaction_date, extract_url, validate_numeric_field
# Set SQLAlchemy log levels independently
import logging
logging.getLogger("sqlalchemy.engine.Engine").setLevel(logging.ERROR)
logging.getLogger("sqlalchemy.pool").setLevel(logging.ERROR)
logging.basicConfig(level=logging.ERROR)


def insert_main_item(item_data):
    # Filter the data
    filtered_data = map_monday_data_to_main_item(item_data)
    with get_db_session() as session:
        existing_item = session.query(PurchaseOrder).filter_by(pulse_id=filtered_data['pulse_id']).first()
        if existing_item:
            for key, value in filtered_data.items():
                setattr(existing_item, key, value)
        else:
            main_item = PurchaseOrder(**filtered_data)
            session.add(main_item)
           # session.commit()  # Already handled in get_db_session context manager


def insert_detail_item(detail_item_data):
    """
    Inserts a new DetailItem or updates an existing one based on DetailItem_id.
    Only updates fields that have changed, but always updates 'state'.
    """
    for detail_item in detail_item_data:
        try:
            # Step 1: Map the data and validate required fields
            filtered_data = map_monday_data_to_sub_item(detail_item)

            if not filtered_data.get("pulse_id"):
                logger.warning(f"DetailItem has no pulse_id. Skipping.")
                continue  # Skip DetailItems without a pulse_id

            # Validate numeric fields
            filtered_data['rate'] = validate_numeric_field(filtered_data.get('rate', ''), 'rate')
            filtered_data['quantity'] = validate_numeric_field(filtered_data.get('quantity', ''), 'quantity')

            with get_db_session() as session:
                # Step 2: Check if the parent exists in the purchase_orders table
                parent_id = filtered_data.get("parent_id")
                if not session.query(PurchaseOrder).filter_by(pulse_id=parent_id).first():
                    logger.warning(f"Parent ID {parent_id} does not exist. Skipping DetailItem with pulse_id {filtered_data['pulse_id']}.")
                    continue

                # Step 3: Check if the DetailItem already exists
                existing_DetailItem = session.query(DetailItem).filter_by(pulse_id=filtered_data['pulse_id']).first()

                if existing_DetailItem:
                    changes = {}
                    # Update fields that have changed
                    for key, value in filtered_data.items():
                        existing_value = getattr(existing_DetailItem, key, None)
                        if existing_value != value:
                            changes[key] = value
                            setattr(existing_DetailItem, key, value)

                    if changes:
                        logger.info(f"Updated DetailItem ID {filtered_data['pulse_id']} with changes: {changes}")
                else:
                    # Insert new DetailItem
                    new_DetailItem = DetailItem(**filtered_data)
                    session.add(new_DetailItem)
                    logger.info(f"Inserted new DetailItem with ID {filtered_data['pulse_id']}")

                # Commit changes
                session.commit()
        except KeyError as e:
            logger.error(f"KeyError: Missing key {e} in filtered_data for DetailItem. Skipping DetailItem.")
        except ValueError as e:
            logger.error(f"ValueError: {e} DetailItem data: {detail_item}")
        except SQLAlchemyError as e:
            logger.error(f"Database error while processing DetailItem: {e}. DetailItem data: {detail_item}")
        except Exception as e:
            logger.error(f"Unexpected error while processing DetailItem: {e}. DetailItem data: {detail_item}")


def fetch_all_main_items():
    with get_db_session() as session:
        return session.query(PurchaseOrder).all()


def fetch_detail_item_for_main_item(main_item_id):
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


def patch_detail_item(detail_item, update_data):
    """
    Patches an existing DetailItem in the local database with the provided update_data.
    """
    try:
        print("Patching Detail Item")
        logger.debug(f"Patching DetailItem ID {detail_item} with data: {update_data}")

        with get_db_session() as session:
            existing_DetailItem = session.query(DetailItem).filter_by(pulse_id=str(detail_item)).first()

            if existing_DetailItem:
                changes = {}
                # Update other fields
                for key, value in update_data.items():
                    existing_value = getattr(existing_DetailItem, key, None)
                    if existing_value != value:
                        changes[key] = value
                        setattr(existing_DetailItem, key, value)

                if changes:
                    logger.info(f"Updated DetailItem with ID {detail_item} with changes: {changes}")
            else:
                logger.error(f"DetailItem with ID {detail_item} does not exist in the database.")
                return False, f"DetailItem with ID {detail_item} does not exist."

        logger.debug(f"Successful patch of {detail_item} with changes: {update_data}")
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


def delete_sub_item_from_db(pulse_id):
    """
    Deletes a sub-item (DetailItem) from the database using its pulse_id.

    Parameters:
    - pulse_id (str or int): The pulse_id of the sub-item to be deleted.

    Returns:
    - bool: True if the deletion was successful, False otherwise.
    - str: Message indicating the result of the operation.
    """
    try:
        with get_db_session() as session:
            # Find the sub-item in the database
            sub_item = session.query(DetailItem).filter_by(pulse_id=pulse_id).first()

            if sub_item:
                # Delete the sub-item
                session.delete(sub_item)
                session.commit()
                logger.info(f"Successfully deleted DetailItem with pulse_id: {pulse_id}")
                return True, f"DetailItem with pulse_id {pulse_id} has been deleted."
            else:
                logger.warning(f"No DetailItem found with pulse_id: {pulse_id}")
                return False, f"No DetailItem found with pulse_id {pulse_id}."
    except SQLAlchemyError as e:
        logger.error(f"Database error while deleting DetailItem with pulse_id {pulse_id}: {e}")
        return False, f"Database error occurred: {e}"
    except Exception as e:
        logger.error(f"Unexpected error while deleting DetailItem with pulse_id {pulse_id}: {e}")
        return False, f"Unexpected error occurred: {e}"

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
        monday_DetailItem_data (dict): Raw DetailItem data from Monday.com.

    Returns:
        dict: A dictionary with keys matching the DetailItem schema.
    """
    # Extract column_values into a dictionary for easier access
    column_values = {col['id']: col for col in monday_DetailItem_data.get('column_values', [])}


    # Map Monday data to DetailItem schema fields
    sub_item_data = {
        "pulse_id": monday_DetailItem_data.get("id"),
        "parent_id": (monday_DetailItem_data.get("parent_item") or {}).get("id"),
        "state": column_values.get("status4", {}).get("text"),
        "detail_item_id": extract_detail_item_id(column_values.get("text0", {}).get("text", "0").lstrip("0")),
        "description": column_values.get("text98", {}).get("text"),
        "rate": column_values.get("numbers9", {}).get("text") or 1,
        "quantity": column_values.get("numbers0", {}).get("text"),
        "account_number": column_values.get("dropdown", {}).get("text") or None,
        "transaction_date": parse_transaction_date(column_values.get("date", {}).get("text")) or None,
        "file_link": extract_url(column_values, "link"),
    }

    return sub_item_data


