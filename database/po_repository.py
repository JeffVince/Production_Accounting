# database/po_repository.py

from sqlalchemy.exc import SQLAlchemyError
from database.models import PurchaseOrder
from database.db_util import get_db_session
import logging

logger = logging.getLogger(__name__)


def add_or_update_po(po_data):
    """
    Adds a new PurchaseOrder or updates an existing one.
    """
    try:
        with get_db_session() as session:
            po = session.query(PurchaseOrder).filter_by(po_number=po_data['po_number']).first()
            if po:
                logger.debug(f"Updating existing PurchaseOrder: {po_data['po_number']}")
                for key, value in po_data.items():
                    setattr(po, key, value)
            else:
                logger.debug(f"Adding new PurchaseOrder: {po_data['po_number']}")
                po = PurchaseOrder(**po_data)
                session.add(po)
            session.commit()
            return po
    except SQLAlchemyError as e:
        logger.error(f"Error adding or updating PurchaseOrder: {e}")
        raise e


def get_po_by_number(po_number):
    """
    Retrieves a PurchaseOrder by its number.
    """
    try:
        with get_db_session() as session:
            po = session.query(PurchaseOrder).filter_by(po_number=po_number).first()
            return po
    except SQLAlchemyError as e:
        logger.error(f"Error retrieving PurchaseOrder: {e}")
        raise e


def get_pos_by_status(state):
    """Fetch all PurchaseOrders with the given status."""
    try:
        with get_db_session() as session:
            return session.query(PurchaseOrder).filter_by(state=state).all()
    except SQLAlchemyError as e:
        logger.error(f"Error retrieving PO: {e}")
        raise e