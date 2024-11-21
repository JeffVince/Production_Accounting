# database/monday_repository.py

from sqlalchemy.exc import SQLAlchemyError
from database.models import PO, Contact, Vendor
from database.db_util import get_db_session
from database.utils import update_po_status, get_po_state
import logging

logger = logging.getLogger(__name__)

def add_or_update_monday_po(po_data):
    """
    Adds or updates a PO record based on data from Monday.com.
    """
    try:
        with get_db_session() as session:
            po = session.query(PO).filter_by(po_number=po_data['po_number']).first()
            if po:
                logger.debug(f"Updating PO {po_data['po_number']} from Monday.com data")
                for key, value in po_data.items():
                    setattr(po, key, value)
            else:
                logger.debug(f"Adding new PO {po_data['po_number']} from Monday.com data")
                po = PO(**po_data)
                session.add(po)
            session.commit()
            return po
    except SQLAlchemyError as e:
        logger.error(f"Error adding or updating Monday.com PO: {e}")
        raise e

def update_monday_po_status(po_number, status):
    """
    Updates the PO status based on Monday.com data.
    """
    update_po_status(po_number, status)  # Use the shared function from utils.py

def link_contact_to_po(po_number, contact_data):
    """
    Links a contact to a PO.
    """
    try:
        with get_db_session() as session:
            po = session.query(PO).filter_by(po_number=po_number).first()
            if not po:
                logger.warning(f"PO {po_number} not found")
                return

            # Find or create the contact
            contact = session.query(Contact).filter_by(contact_id=contact_data['contact_id']).first()
            if not contact:
                contact = Contact(**contact_data)
                session.add(contact)
                logger.debug(f"Created new contact {contact_data['contact_id']}")

            # Link contact to vendor
            if not po.vendor:
                vendor = Vendor(vendor_name=contact_data.get('name', 'Unknown Vendor'), contact=contact)
                session.add(vendor)
                po.vendor = vendor
            else:
                po.vendor.contact = contact

            session.commit()
            logger.debug(f"Linked contact {contact.contact_id} to PO {po_number}")
    except SQLAlchemyError as e:
        logger.error(f"Error linking contact to PO: {e}")
        raise e

def get_monday_po_state(po_number):
    """
    Retrieves the state of a PO as per Monday.com data.
    """
    return get_po_state(po_number)  # Use the shared function from utils.py