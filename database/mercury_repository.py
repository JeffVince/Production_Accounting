# database/mercury_repository.py

from sqlalchemy.exc import SQLAlchemyError
from database.models import Transaction, TransactionState, PO
from database.db_util import get_db_session
import logging

logger = logging.getLogger(__name__)


def add_or_update_transaction(transaction_data):
    """
    Adds or updates a transaction associated with a PO.
    """
    try:
        with get_db_session() as session:
            transaction = session.query(Transaction).filter_by(
                transaction_id=transaction_data['transaction_id']).first()
            if transaction:
                logger.debug(f"Updating transaction {transaction_data['transaction_id']}")
                for key, value in transaction_data.items():
                    setattr(transaction, key, value)
            else:
                logger.debug(f"Adding new transaction {transaction_data['transaction_id']}")
                transaction = Transaction(**transaction_data)
                session.add(transaction)
            session.commit()
            return transaction
    except SQLAlchemyError as e:
        logger.error(f"Error adding or updating transaction: {e}")
        raise e


def update_transaction_status(transaction_id, status):
    """
    Updates the status of a transaction.
    """
    try:
        with get_db_session() as session:
            transaction = session.query(Transaction).filter_by(transaction_id=transaction_id).first()
            if transaction:
                transaction.state = TransactionState(status)
                session.commit()
                logger.debug(f"Updated transaction {transaction_id} status to {status}")
            else:
                logger.warning(f"Transaction {transaction_id} not found")
    except SQLAlchemyError as e:
        logger.error(f"Error updating transaction status: {e}")
        raise e


def get_transaction_by_po(po_number):
    """
    Retrieves a transaction associated with a PO.
    """
    try:
        with get_db_session() as session:
            po = session.query(PO).filter_by(po_number=po_number).first()
            if po:
                transaction = session.query(Transaction).filter_by(po_id=po.id).first()
                return transaction
            else:
                logger.warning(f"PO {po_number} not found")
                return None
    except SQLAlchemyError as e:
        logger.error(f"Error retrieving transaction for PO {po_number}: {e}")
        raise e


def get_transaction_state(transaction_id):
    """
    Gets the current state of a transaction.
    """
    try:
        with get_db_session() as session:
            transaction = session.query(Transaction).filter_by(transaction_id=transaction_id).first()
            if transaction:
                return transaction.state
            else:
                logger.warning(f"Transaction {transaction_id} not found")
                return None
    except SQLAlchemyError as e:
        logger.error(f"Error retrieving transaction state: {e}")
        raise e