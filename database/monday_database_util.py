# monday_database_util.py

import json
from datetime import datetime

from sqlalchemy.exc import SQLAlchemyError, IntegrityError
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine, func

from database.models import (
    PurchaseOrder,
    DetailItem,
    Contact,
    Project,
    AicpCode,
    TaxAccount,
    # other models as needed
)

from monday_api import MondayAPI

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
    PO_CONTACT_CONNECTION_COLUMN_ID,
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
logging.basicConfig(level=logging.ERROR)

# Ensure the Monday API Token is loaded
import os
from dotenv import load_dotenv

load_dotenv()
MONDAY_API_TOKEN = os.getenv("MONDAY_API_TOKEN")

if not MONDAY_API_TOKEN:
    logger.error("Monday API Token not found. Please set it in the environment variables.")
    exit(1)

MONDAY_API_URL = 'https://api.monday.com/v2'  # Ensure this is defined

MondayAPI = MondayAPI()


# CRUD Operations

# CREATE
def create_or_update_main_item_in_db(item_data):
    """
    Creates or updates a main item record in the database.

    Args:
        item_data (dict): The prepared database creation item containing details.

    Returns:
        str: Status message indicating success, creation, or failure.
    """
    pulse_id = item_data.get("pulse_id")
    with get_db_session() as session:
        try:
            # Check if the item already exists in the database
            po_item = session.query(PurchaseOrder).filter_by(pulse_id=pulse_id).one_or_none()

            if po_item:
                # Update existing record
                for db_field, value in item_data.items():
                    if db_field != "pulse_id":  # Skip pulse_id since it's the key
                        setattr(po_item, db_field, value)
                logger.info(f"Updated existing Main with pulse_id: {pulse_id}")
                status = "Updated"
            else:  # Create a new record
                new_item = PurchaseOrder(**item_data)
                session.add(new_item)
                logger.info(f"Created new Main with pulse_id: {pulse_id}")
                status = "Created"
            # Commit the transaction
            session.commit()
            return status
        except Exception as e:
            session.rollback()
            logger.error(f"Error processing main item in DB: {e}")
            return None


def create_or_update_sub_item_in_db(item_data):
    """
    Creates or updates a subitem record in the database.

    Args:
        item_data (dict): The prepared database creation item containing subitem details.

    Returns:
        str: Status message indicating success, creation, or failure.
    """
    pulse_id = item_data.get("pulse_id")
    parent_id = item_data.get("parent_id")
    account_number = item_data.get("account_number_id")
    if not account_number:
        account_number = "5000"
    with get_db_session() as session:
        try:
            # Fetch the parent PurchaseOrder using the pulse_id
            po_surrogate_id = session.query(PurchaseOrder).filter_by(pulse_id=parent_id).one_or_none()
            aicp_code_surrogate_id = session.query(AicpCode).filter_by(code=account_number).one_or_none()
            if not po_surrogate_id:
                raise ValueError(f"No PurchaseOrder found with pulse_id: {parent_id}")
            if not aicp_code_surrogate_id:
                raise ValueError(f"No AICP Line found with surrogate id: {aicp_code_surrogate_id}")
            # set the account number to account surrogate id
            item_data['account_number_id'] = aicp_code_surrogate_id.aicp_code_surrogate_id
            # Set the parent_id to the po_surrogate_id
            item_data['parent_id'] = po_surrogate_id.po_surrogate_id
            # Create the new DetailItem
            new_detail_item = DetailItem(**item_data)
            session.add(new_detail_item)
            session.commit()

            logger.info(
                f"Created new DetailItem with pulse_id: {pulse_id}, surrogate_id: {new_detail_item.detail_item_surrogate_id}")
            return {
                "status": "Created",
                "detail_item_id": new_detail_item.detail_item_surrogate_id
            }
        except IntegrityError as ie:
            session.rollback()
            logger.error(f"IntegrityError processing subitem in DB: {ie}")
            return {
                "status": "Fail",
                "error": str(ie)
            }
        except Exception as e:
            session.rollback()
            logger.error(f"Error processing subitem in DB: {e}")
            return {
                "status": "Fail",
                "error": str(e)
            }


def create_or_update_contact_item_in_db(item_data):
    """
    Creates or updates a contact record in the database.

    Args:
        item_data (dict): The prepared database creation item containing contact details.

    Returns:
        str: Status message indicating success, creation, or failure.
    """
    pulse_id = item_data.get("pulse_id")

    with get_db_session() as session:
        try:
            # Check if the item already exists in the database
            contact_item = session.query(Contact).filter_by(pulse_id=pulse_id).one_or_none()

            if contact_item:
                # Update existing record
                for db_field, value in item_data.items():
                    if db_field != "pulse_id":  # Skip pulse_id since it's the key
                        setattr(contact_item, db_field, value)
                logger.info(f"Updated existing Contact with pulse_id: {pulse_id}")
                status = "Updated"
            else:  # Create a new record
                new_contact = Contact(**item_data)
                session.add(new_contact)
                logger.info(f"Created new DetailItem with pulse_id: {pulse_id}")
                status = "Created"
            # Commit the transaction
            session.commit()
            return status
        except Exception as e:
            session.rollback()
            logger.error(f"Error processing contact in DB: {e}")
            return "Fail"


# READ

def get_purchase_order_by_pulse_id(pulse_id):
    with get_db_session() as session:
        po = session.query(PurchaseOrder).filter_by(po_surrogate_id=pulse_id).first()
        return po


def get_detail_item_by_pulse_id(pulse_id):
    with get_db_session() as session:
        detail_item = session.query(DetailItem).filter_by(detail_item_surrogate_id=pulse_id).first()
        return detail_item


def get_account_number_id(account_number_text):
    """
    Retrieve the account_code_id from the account number text.

    Args:
        account_number_text (str): The text representing the account number.

    Returns:
        int or None: The corresponding account_code_id, or None if not found.
    """
    if not account_number_text:
        logger.error("Account number text is empty.")
        return None

    with get_db_session() as session:
        account = session.query(AicpCode).filter_by(line_number=account_number_text).first()
        if account:
            return account.account_code_id
        else:
            logger.error(f"AicpCode with line_number '{account_number_text}' not found.")
            return None


# UPDATE


def update_db_with_sub_item_change(change_item):
    """
    Applies the prepared change to the database.

    Args:
        session (Session): SQLAlchemy session for the database.
        change_item (dict): The prepared database change item.

    Returns:
        bool: True if update was successful, False otherwise.
    """
    pulse_id = change_item["pulse_id"]
    db_field = change_item["db_field"]
    new_value = change_item["new_value"]
    with get_db_session() as session:
        try:
            # Locate the Detail Item using the pulse_id
            detail_item = session.query(DetailItem).filter_by(pulse_id=pulse_id).one_or_none()
            if not detail_item:
                logger.info(f"No record found with pulse_id {pulse_id}.  Creating One.")
                return "Not Found"

            # Update the appropriate field
            setattr(detail_item, db_field, new_value)

            # Commit the change
            session.commit()
            return "Success"
        except Exception as e:
            session.rollback()
            print(f"Error updating DB: {e}")
            return "Fail"


# DELETE
def delete_purchase_order_in_db(pulse_id):
    with get_db_session() as session:
        po = session.query(PurchaseOrder).filter_by(po_surrogate_id=pulse_id).first()
        if not po:
            logger.error(f"PurchaseOrder with ID {pulse_id} does not exist.")
            return False
        session.delete(po)
        try:
            session.commit()
            logger.info(f"Deleted PurchaseOrder with ID {pulse_id}.")
            return True
        except SQLAlchemyError as e:
            session.rollback()
            logger.error(f"Error deleting PurchaseOrder: {e}")
            return False


def delete_detail_item_in_db(pulse_id):
    with get_db_session() as session:
        detail_item = session.query(DetailItem).filter_by(detail_item_surrogate_id=pulse_id).first()
        if not detail_item:
            logger.error(f"DetailItem with ID {pulse_id} does not exist.")
            return False
        session.delete(detail_item)
        try:
            session.commit()
            logger.info(f"Deleted DetailItem with ID {pulse_id}.")
            return True
        except SQLAlchemyError as e:
            session.rollback()
            logger.error(f"Error deleting DetailItem: {e}")
            return False


def get_aicp_code_surrogate_id(aicp_code):
    with get_db_session() as session:
        aicp_code_entry = session.query(AicpCode).filter_by(code=aicp_code).one_or_none()
        if aicp_code_entry:
            return aicp_code_entry.aicp_code_surrogate_id
        else:
            return None
