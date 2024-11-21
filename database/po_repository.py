# database/po_repository.py

from sqlalchemy.exc import SQLAlchemyError
from database.models import PO
from database.db_util import get_db_session
import logging

logger = logging.getLogger(__name__)

def add_or_update_po(po_data):
    """
    Adds a new PO or updates an existing one.
    """
    try:
        with get_db_session() as session:
            po = session.query(PO).filter_by(po_number=po_data['po_number']).first()
            if po:
                logger.debug(f"Updating existing PO: {po_data['po_number']}")
                for key, value in po_data.items():
                    setattr(po, key, value)
            else:
                logger.debug(f"Adding new PO: {po_data['po_number']}")
                po = PO(**po_data)
                session.add(po)
            session.commit()
            return po
    except SQLAlchemyError as e:
        logger.error(f"Error adding or updating PO: {e}")
        raise e

def get_po_by_number(po_number):
    """
    Retrieves a PO by its number.
    """
    try:
        with get_db_session() as session:
            po = session.query(PO).filter_by(po_number=po_number).first()
            return po
    except SQLAlchemyError as e:
        logger.error(f"Error retrieving PO: {e}")
        raise e