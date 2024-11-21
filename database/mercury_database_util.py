# database/mercury_database_util.py

from database.mercury_repository import (
    add_or_update_transaction,
    update_transaction_status,
    get_transaction_by_po,
    get_transaction_state
)
from database.po_repository import get_po_by_number
from database.models import TransactionState
from sqlalchemy.exc import SQLAlchemyError
import logging

logger = logging.getLogger(__name__)

def initiate_payment(po_number, transaction_data):
    """
    Initiates a payment transaction in the database associated with a PO.
    """
    try:
        po = get_po_by_number(po_number)
        if not po:
            logger.warning(f"PO {po_number} not found")
            return None

        transaction_data['po_id'] = po.id
        transaction_data['state'] = TransactionState.PENDING
        transaction = add_or_update_transaction(transaction_data)
        logger.debug(f"Initiated payment transaction {transaction.transaction_id} for PO {po_number}")
        return transaction
    except SQLAlchemyError as e:
        logger.error(f"Error initiating payment for PO {po_number}: {e}")
        raise e

def confirm_payment_execution(transaction_id):
    """
    Updates the transaction status to 'PAID'.
    """
    update_transaction_status(transaction_id, 'PAID')
    logger.debug(f"Transaction {transaction_id} marked as PAID")

def confirm_payment_confirmation(transaction_id):
    """
    Updates the transaction status to 'CONFIRMED'.
    """
    update_transaction_status(transaction_id, 'CONFIRMED')
    logger.debug(f"Transaction {transaction_id} confirmed by bank")

def get_transaction_status_by_po(po_number):
    """
    Retrieves the transaction status associated with a PO.
    """
    transaction = get_transaction_by_po(po_number)
    if transaction:
        return transaction.state
    else:
        logger.warning(f"No transaction found for PO {po_number}")
        return None