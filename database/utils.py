# database/utils.py

from sqlalchemy.exc import SQLAlchemyError
from database.models import PO, POState
from database.db_util import get_db_session
import logging

logger = logging.getLogger(__name__)


def update_po_status(po_number, status):
    """
    Updates the status of a PO.
    """
    try:
        with get_db_session() as session:
            po = session.query(PO).filter_by(po_number=po_number).first()
            if po:
                po.state = POState(status)
                session.commit()
                logger.debug(f"Updated PO {po_number} status to {status}")
            else:
                logger.warning(f"PO {po_number} not found")
    except SQLAlchemyError as e:
        logger.error(f"Error updating PO status: {e}")
        raise e


def get_po_state(po_number):
    """
    Gets the current state of a PO.
    """
    try:
        with get_db_session() as session:
            po = session.query(PO).filter_by(po_number=po_number).first()
            if po:
                return po.state
            else:
                logger.warning(f"PO {po_number} not found")
                return None
    except SQLAlchemyError as e:
        logger.error(f"Error retrieving PO state: {e}")
        raise e