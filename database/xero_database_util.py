# database/xero_database_util.py

from database.xero_repository import (
    add_or_update_bill,
    update_bill_status,
    get_bill_by_po,
    get_bill_state,
    add_or_update_spend_money_transaction, add_or_update_spend_money_transaction
)
from database.po_repository import get_po_by_number
from database.models import BillState, SpendMoneyState, SpendMoneyTransaction
from sqlalchemy.exc import SQLAlchemyError
import logging

from db_util import get_db_session

logger = logging.getLogger(__name__)


def create_draft_bill(po_number, bill_data):
    """
    Creates a draft bill in the database associated with a PO.
    """
    try:
        po = get_po_by_number(po_number)
        if not po:
            logger.warning(f"PO {po_number} not found")
            return None

        bill_data['po_id'] = po.id
        bill_data['state'] = BillState.DRAFT
        bill = add_or_update_bill(bill_data)
        logger.debug(f"Created draft bill {bill.bill_id} for PO {po_number}")
        return bill
    except SQLAlchemyError as e:
        logger.error(f"Error creating draft bill for PO {po_number}: {e}")
        raise e


def submit_bill_for_approval(bill_id):
    """
    Updates the bill status to 'Submitted'.
    """
    try:
        update_bill_status(bill_id, BillState.SUBMITTED)  # Pass enum member
        logger.debug(f"Bill {bill_id} submitted for approval successfully.")
    except Exception as e:
        logger.error(f"Failed to submit bill {bill_id} for approval: {e}")
        raise e


def approve_bill(bill_id):
    """
    Updates the bill status to 'Approved'.
    """
    try:
        update_bill_status(bill_id, BillState.APPROVED)  # Pass enum member
        logger.debug(f"Bill {bill_id} approved successfully.")
    except Exception as e:
        logger.error(f"Failed to approve bill {bill_id}: {e}")
        raise e


def get_bill_status_by_po(po_number):
    """
    Retrieves the bill status associated with a PO.
    """
    bill = get_bill_by_po(po_number)
    if bill:
        return bill.state
    else:
        logger.warning(f"No bill found for PO {po_number}")
        return None


def create_spend_money_transaction(po_number, transaction_data):
    """
    Creates a spend money transaction associated with a PO.
    """
    try:
        po = get_po_by_number(po_number)
        if not po:
            logger.warning(f"PO {po_number} not found")
            return None

        transaction_data['po_id'] = po.id
        transaction_data['state'] = SpendMoneyState.DRAFT  # Assigning enum member
        transaction = add_or_update_spend_money_transaction(transaction_data)
        logger.debug(f"Created spend money transaction {transaction.transaction_id} for PO {po_number}")
        return transaction
    except SQLAlchemyError as e:
        logger.error(f"Error creating spend money transaction for PO {po_number}: {e}")
        raise e


def update_spend_money_status(transaction_id, status):
    """
    Updates the status of a Spend Money transaction.
    """
    try:
        with get_db_session() as session:
            transaction = session.query(SpendMoneyTransaction).filter_by(
                transaction_id=transaction_id).first()
            if transaction:
                transaction.state = SpendMoneyState(status)
                session.commit()
                logger.debug(f"Updated Spend Money transaction {transaction_id} status to {status}")
            else:
                logger.warning(f"Spend Money transaction {transaction_id} not found")
    except SQLAlchemyError as e:
        logger.error(f"Error updating Spend Money transaction status: {e}")
        raise e