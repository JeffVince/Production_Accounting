# database/actuals_database_util.py

from database.models import Actual, PO
from database.db_util import get_db_session
from database.po_repository import get_po_by_number
from sqlalchemy.exc import SQLAlchemyError
import logging

logger = logging.getLogger(__name__)

# First, we need to add the Actual model to models.py

# Add this to your models.py
"""
class Actual(Base):
    __tablename__ = 'actuals'

    id = Column(Integer, primary_key=True)
    po_id = Column(Integer, ForeignKey('pos.id'))
    amount = Column(Float)
    description = Column(Text)
    date = Column(DateTime)
    source = Column(String)  # e.g., 'Xero', 'Mercury', 'Manual'

    po = relationship('PO', back_populates='actuals')
"""

# Don't forget to add the relationship in the PO model
# Add 'actuals = relationship('Actual', back_populates='po')' to the PO class in models.py

def add_actual_entry(po_number, actual_data):
    """
    Adds an actual expense entry associated with a PO.
    """
    try:
        po = get_po_by_number(po_number)
        if not po:
            logger.warning(f"PO {po_number} not found")
            return None

        with get_db_session() as session:
            actual = Actual(
                po_id=po.id,
                amount=actual_data['amount'],
                description=actual_data.get('description', ''),
                date=actual_data.get('date'),
                source=actual_data.get('source', 'Manual')
            )
            session.add(actual)
            session.commit()
            logger.debug(f"Added actual entry for PO {po_number}")
            return actual
    except SQLAlchemyError as e:
        logger.error(f"Error adding actual entry for PO {po_number}: {e}")
        raise e

def get_actuals_by_po(po_number):
    """
    Retrieves all actual entries associated with a PO.
    """
    try:
        po = get_po_by_number(po_number)
        if not po:
            logger.warning(f"PO {po_number} not found")
            return []

        with get_db_session() as session:
            actuals = session.query(Actual).filter_by(po_id=po.id).all()
            return actuals
    except SQLAlchemyError as e:
        logger.error(f"Error retrieving actuals for PO {po_number}: {e}")
        raise e

def reconcile_actuals(po_number):
    """
    Performs reconciliation of actuals for a PO.
    """
    try:
        actuals = get_actuals_by_po(po_number)
        if not actuals:
            logger.warning(f"No actuals to reconcile for PO {po_number}")
            return False

        # Implement your reconciliation logic here
        # For example, sum actual amounts and compare with PO amount
        total_actuals = sum([actual.amount for actual in actuals])
        po = get_po_by_number(po_number)
        if po.amount == total_actuals:
            logger.debug(f"PO {po_number} reconciled successfully")
            # Update PO state to 'RECONCILED'
            from database.utils import update_po_status
            update_po_status(po_number, 'RECONCILED')
            return True
        else:
            logger.warning(f"Discrepancy in actuals for PO {po_number}")
            return False
    except SQLAlchemyError as e:
        logger.error(f"Error reconciling actuals for PO {po_number}: {e}")
        raise e