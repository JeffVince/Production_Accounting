# database/xero_repository.py

from sqlalchemy.exc import SQLAlchemyError
from database.models import Bill, SpendMoney, XeroBillState, XeroSpendMoneyState, PurchaseOrder
from database.db_util import get_db_session
import logging

logger = logging.getLogger(__name__)


def add_or_update_bill(bill_data):
    """
    Adds or updates a bill associated with a PO.
    """
    try:
        with get_db_session() as session:
            bill = session.query(Bill).filter_by(bill_id=bill_data['bill_id']).first()
            if bill:
                logger.debug(f"Updating bill {bill_data['bill_id']}")
                for key, value in bill_data.items():
                    setattr(bill, key, value)
            else:
                logger.debug(f"Adding new bill {bill_data['bill_id']}")
                bill = Bill(**bill_data)
                session.add(bill)
            session.commit()
            return bill
    except SQLAlchemyError as e:
        logger.error(f"Error adding or updating bill: {e}")
        raise e


def update_bill_status(bill_id, status):
    """
    Updates the status of a bill.

    Args:
        bill_id (str): The unique identifier of the bill.
        status (BillState): The new state to assign to the bill.

    Raises:
        ValueError: If the provided status is not an instance of BillState.
        SQLAlchemyError: If a database error occurs.
    """
    if not isinstance(status, XeroBillState):
        raise ValueError(f"'{status}' is not a valid BillState")

    try:
        with get_db_session() as session:
            bill = session.query(Bill).filter_by(bill_id=bill_id).first()
            if bill:
                bill.state = status  # Assign enum member directly
                session.commit()
                logger.debug(f"Updated bill {bill_id} status to {status.value}")
            else:
                logger.warning(f"Bill {bill_id} not found.")
    except SQLAlchemyError as e:
        logger.error(f"Error updating bill status: {e}")
        raise e


def get_bill_by_po(po_number):
    """
    Retrieves a bill associated with a PO.
    """
    try:
        with get_db_session() as session:
            po = session.query(PurchaseOrder).filter_by(po_number=po_number).first()
            if po:
                bill = session.query(Bill).filter_by(po_id=po.id).first()
                return bill
            else:
                logger.warning(f"PO {po_number} not found")
                return None
    except SQLAlchemyError as e:
        logger.error(f"Error retrieving bill for PO {po_number}: {e}")
        raise e


def get_bill_state(bill_id):
    """
    Gets the current state of a bill.
    """
    try:
        with get_db_session() as session:
            bill = session.query(Bill).filter_by(bill_id=bill_id).first()
            if bill:
                return bill.state
            else:
                logger.warning(f"Bill {bill_id} not found")
                return None
    except SQLAlchemyError as e:
        logger.error(f"Error retrieving bill state: {e}")
        raise e


def add_or_update_spend_money_transaction(transaction_data):
    """
    Adds a new spend money transaction or updates an existing one based on transaction_id.
    """
    try:
        with get_db_session() as session:
            transaction = session.query(XeroSpendMoneyState).filter_by(transaction_id=transaction_data['transaction_id']).first()
            if transaction:
                # Update existing transaction
                transaction.amount = transaction_data.get('amount', transaction.amount)
                transaction.description = transaction_data.get('description', transaction.description)
                transaction.state = transaction_data.get('state', transaction.state)
                logger.info(f"Updated SpendMoneyTransaction {transaction.transaction_id}")
            else:
                # Create new transaction
                transaction = XeroSpendMoneyState(**transaction_data)
                session.add(transaction)
                logger.info(f"Added SpendMoneyTransaction {transaction.transaction_id}")
            session.commit()
            return transaction
    except SQLAlchemyError as e:
        logger.error(f"Error adding/updating SpendMoneyTransaction {transaction_data.get('transaction_id')}: {e}")
        raise e