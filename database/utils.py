# database/utils.py

from sqlalchemy.exc import SQLAlchemyError
from database.models import PO, POState, MainItem, SubItem
from database.db_util import get_db_session
import logging

logger = logging.getLogger(__name__)





def get_po_state(item_id):
    """
    Gets the current state of a PO.
    """
    try:
        with get_db_session() as session:
            po = session.query(PO).filter_by(item_id=item_id).first()
            if po:
                return po.state
            else:
                logger.warning(f"PO {item_id} not found")
                return None
    except SQLAlchemyError as e:
        logger.error(f"Error retrieving PO state: {e}")
        raise e