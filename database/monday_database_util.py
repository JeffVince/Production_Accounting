# database/monday_database_util.py

import json
from datetime import datetime

from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine

from database.models import (
    PurchaseOrder,
    DetailItem,
    Contact,
    Project,
    AicpCode,
    TaxAccount,
    # other models as needed
)

from database.db_util import get_db_session

from utilities.monday_util import (
    create_item,
    update_item_columns,
    create_subitem,
    update_subitem_columns,
    get_group_id_by_project_id,
    find_item_by_project_and_po,
    find_subitem_by_invoice_or_receipt_number,
    find_all_po_subitems,
    find_contact_item_by_name,
    # Column IDs for POs
    PO_BOARD_ID,
    PO_PROJECT_ID_COLUMN,
    PO_NUMBER_COLUMN,
    PO_DESCRIPTION_COLUMN_ID,
    PO_FOLDER_LINK_COLUMN_ID,
    PO_TAX_COLUMN_ID,
    PO_STATUS_COLUMN_ID,
    PO_CONNECTION_COLUMN_ID,
    # Column IDs for Subitems
    SUBITEM_NOTES_COLUMN_ID,
    SUBITEM_STATUS_COLUMN_ID,
    SUBITEM_ID_COLUMN_ID,
    SUBITEM_DESCRIPTION_COLUMN_ID,
    SUBITEM_QUANTITY_COLUMN_ID,
    SUBITEM_RATE_COLUMN_ID,
    SUBITEM_DATE_COLUMN_ID,
    SUBITEM_DUE_DATE_COLUMN_ID,
    SUBITEM_ACCOUNT_NUMBER_COLUMN_ID,
    SUBITEM_LINK_COLUMN_ID,
    SUBITEM_BOARD_ID,
)

import logging

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# Ensure the Monday API Token is loaded
import os
from dotenv import load_dotenv

load_dotenv()
MONDAY_API_TOKEN = os.getenv("MONDAY_API_TOKEN")
if not MONDAY_API_TOKEN:
    logger.error("Monday API Token not found. Please set it in the environment variables.")
    exit(1)


# Helper functions to validate and parse data
def parse_decimal(value):
    try:
        return float(value)
    except (ValueError, TypeError):
        return None


def parse_date(value):
    try:
        return datetime.strptime(value, "%Y-%m-%d")
    except (ValueError, TypeError):
        return None


# Mapping functions to map Monday.com data to database models
def map_monday_data_to_purchase_order(monday_data):
    """
    Map Monday.com data to PurchaseOrder model fields.
    """
    column_values = {col['id']: col for col in monday_data.get('column_values', [])}

    # Extract and parse data
    project_id = column_values.get(PO_PROJECT_ID_COLUMN, {}).get('text')
    po_number = column_values.get(PO_NUMBER_COLUMN, {}).get('text')
    description = column_values.get(PO_DESCRIPTION_COLUMN_ID, {}).get('text')
    folder_link = column_values.get(PO_FOLDER_LINK_COLUMN_ID, {}).get('value')
    if folder_link:
        folder_link = json.loads(folder_link).get('url')
    tax_form_link = column_values.get(PO_TAX_COLUMN_ID, {}).get('value')
    if tax_form_link:
        tax_form_link = json.loads(tax_form_link).get('url')
    status = column_values.get(PO_STATUS_COLUMN_ID, {}).get('text')
    pulse_id = monday_data.get('id')

    # Map the data
    po_data = {
        'po_surrogate_id': int(pulse_id),
        'pulse_id': int(pulse_id),
        'project_id': int(project_id) if project_id else None,
        'po_number': int(po_number) if po_number else None,
        'description': description,
        'folder_link': folder_link,
        'tax_form_link': tax_form_link,
        'state': status,
        'po_type': 'Vendor' if status != 'CC / PC' else 'CC / PC',
    }
    return po_data


def map_monday_data_to_detail_item(monday_data, parent_pulse_id):
    """
    Map Monday.com subitem data to DetailItem model fields.
    """
    column_values = {col['id']: col for col in monday_data.get('column_values', [])}

    # Extract and parse data
    detail_item_id = column_values.get(SUBITEM_ID_COLUMN_ID, {}).get('text')
    description = column_values.get(SUBITEM_DESCRIPTION_COLUMN_ID, {}).get('text')
    rate = parse_decimal(column_values.get(SUBITEM_RATE_COLUMN_ID, {}).get('text'))
    quantity = parse_decimal(column_values.get(SUBITEM_QUANTITY_COLUMN_ID, {}).get('text'))
    date_text = column_values.get(SUBITEM_DATE_COLUMN_ID, {}).get('text')
    transaction_date = parse_date(date_text)
    due_date_text = column_values.get(SUBITEM_DUE_DATE_COLUMN_ID, {}).get('text')
    due_date = parse_date(due_date_text)
    account_number_text = column_values.get(SUBITEM_ACCOUNT_NUMBER_COLUMN_ID, {}).get('text')
    account_number_id = get_account_number_id(account_number_text)
    file_link_value = column_values.get(SUBITEM_LINK_COLUMN_ID, {}).get('value')
    file_link = None
    if file_link_value:
        file_link = json.loads(file_link_value).get('url')
    status = column_values.get(SUBITEM_STATUS_COLUMN_ID, {}).get('text')
    pulse_id = monday_data.get('id')

    # Map the data
    detail_item_data = {
        'detail_item_surrogate_id': int(pulse_id),
        'pulse_id': int(pulse_id),
        'po_surrogate_id': int(parent_pulse_id),
        'detail_item_id': int(detail_item_id) if detail_item_id else None,
        'description': description,
        'rate': rate or 0.0,
        'quantity': quantity or 1.0,
        'transaction_date': transaction_date,
        'due_date': due_date,
        'account_number_id': account_number_id,
        'file_link': file_link,
        'state': status,
        'is_receipt': False,  # Adjust based on your logic
    }
    return detail_item_data


def get_account_number_id(account_number_text):
    """
    Retrieve the account_code_id from the account number text.
    """
    with get_db_session() as session:
        account = session.query(AicpCode).filter_by(line_number=account_number_text).first()
        if account:
            return account.account_code_id
        else:
            # Optionally create a new account code
            return None


# CRUD Operations

# CREATE
def create_purchase_order_in_db(monday_item_data):
    po_data = map_monday_data_to_purchase_order(monday_item_data)
    if not po_data['po_surrogate_id']:
        logger.error("Cannot create PurchaseOrder without a pulse_id.")
        return None

    with get_db_session() as session:
        existing_po = session.query(PurchaseOrder).filter_by(po_surrogate_id=po_data['po_surrogate_id']).first()
        if existing_po:
            logger.info(f"PurchaseOrder with ID {po_data['po_surrogate_id']} already exists. Updating instead.")
            return update_purchase_order_in_db(monday_item_data)

        new_po = PurchaseOrder(**po_data)
        session.add(new_po)
        session.commit()
        logger.info(f"Created new PurchaseOrder with ID {new_po.po_surrogate_id}.")
        return new_po


def create_detail_item_in_db(monday_subitem_data, parent_pulse_id):
    detail_item_data = map_monday_data_to_detail_item(monday_subitem_data, parent_pulse_id)
    if not detail_item_data['detail_item_surrogate_id']:
        logger.error("Cannot create DetailItem without a pulse_id.")
        return None

    with get_db_session() as session:
        existing_detail_item = session.query(DetailItem).filter_by(
            detail_item_surrogate_id=detail_item_data['detail_item_surrogate_id']).first()
        if existing_detail_item:
            logger.info(
                f"DetailItem with ID {detail_item_data['detail_item_surrogate_id']} already exists. Updating instead.")
            return update_detail_item_in_db(monday_subitem_data, parent_pulse_id)

        new_detail_item = DetailItem(**detail_item_data)
        session.add(new_detail_item)
        session.commit()
        logger.info(f"Created new DetailItem with ID {new_detail_item.detail_item_surrogate_id}.")
        return new_detail_item


# READ
def get_purchase_order_by_pulse_id(pulse_id):
    with get_db_session() as session:
        po = session.query(PurchaseOrder).filter_by(po_surrogate_id=pulse_id).first()
        return po


def get_detail_item_by_pulse_id(pulse_id):
    with get_db_session() as session:
        detail_item = session.query(DetailItem).filter_by(detail_item_surrogate_id=pulse_id).first()
        return detail_item


# UPDATE
def update_purchase_order_in_db(monday_item_data):
    po_data = map_monday_data_to_purchase_order(monday_item_data)
    if not po_data['po_surrogate_id']:
        logger.error("Cannot update PurchaseOrder without a pulse_id.")
        return None

    with get_db_session() as session:
        po = session.query(PurchaseOrder).filter_by(po_surrogate_id=po_data['po_surrogate_id']).first()
        if not po:
            logger.error(f"PurchaseOrder with ID {po_data['po_surrogate_id']} does not exist.")
            return None

        for key, value in po_data.items():
            setattr(po, key, value)
        session.commit()
        logger.info(f"Updated PurchaseOrder with ID {po.po_surrogate_id}.")
        return po


def update_detail_item_in_db(monday_subitem_data, parent_pulse_id):
    detail_item_data = map_monday_data_to_detail_item(monday_subitem_data, parent_pulse_id)
    if not detail_item_data['detail_item_surrogate_id']:
        logger.error("Cannot update DetailItem without a pulse_id.")
        return None

    with get_db_session() as session:
        detail_item = session.query(DetailItem).filter_by(
            detail_item_surrogate_id=detail_item_data['detail_item_surrogate_id']).first()
        if not detail_item:
            logger.error(f"DetailItem with ID {detail_item_data['detail_item_surrogate_id']} does not exist.")
            return None

        for key, value in detail_item_data.items():
            setattr(detail_item, key, value)
        session.commit()
        logger.info(f"Updated DetailItem with ID {detail_item.detail_item_surrogate_id}.")
        return detail_item


# DELETE
def delete_purchase_order_in_db(pulse_id):
    with get_db_session() as session:
        po = session.query(PurchaseOrder).filter_by(po_surrogate_id=pulse_id).first()
        if not po:
            logger.error(f"PurchaseOrder with ID {pulse_id} does not exist.")
            return False
        session.delete(po)
        session.commit()
        logger.info(f"Deleted PurchaseOrder with ID {pulse_id}.")
        return True


def delete_detail_item_in_db(pulse_id):
    with get_db_session() as session:
        detail_item = session.query(DetailItem).filter_by(detail_item_surrogate_id=pulse_id).first()
        if not detail_item:
            logger.error(f"DetailItem with ID {pulse_id} does not exist.")
            return False
        session.delete(detail_item)
        session.commit()
        logger.info(f"Deleted DetailItem with ID {pulse_id}.")
        return True


# Synchronization functions
def sync_purchase_order_from_monday(pulse_id):
    # Fetch item data from Monday.com
    item_data = get_item_from_monday(pulse_id)
    if not item_data:
        logger.error(f"Failed to retrieve PurchaseOrder with ID {pulse_id} from Monday.com.")
        return None

    # Check if the PO exists in the database
    existing_po = get_purchase_order_by_pulse_id(pulse_id)
    if existing_po:
        return update_purchase_order_in_db(item_data)
    else:
        return create_purchase_order_in_db(item_data)


def sync_detail_items_from_monday(parent_pulse_id):
    # Fetch subitems from Monday.com
    subitems = find_all_po_subitems(parent_pulse_id)
    if not subitems:
        logger.info(f"No subitems found for parent item ID {parent_pulse_id}.")
        return []

    synced_items = []
    for subitem in subitems:
        subitem_pulse_id = subitem.get('id')
        existing_detail_item = get_detail_item_by_pulse_id(subitem_pulse_id)
        if existing_detail_item:
            updated_item = update_detail_item_in_db(subitem, parent_pulse_id)
            synced_items.append(updated_item)
        else:
            new_item = create_detail_item_in_db(subitem, parent_pulse_id)
            synced_items.append(new_item)
    return synced_items


def get_item_from_monday(pulse_id):
    """
    Retrieve an item from Monday.com by its pulse_id.
    """
    # Implement the function using Monday.com API
    # Since the code for fetching an item is not provided, here's a placeholder
    # You should implement this using the appropriate function from monday_util.py
    pass

# Additional functions as needed
