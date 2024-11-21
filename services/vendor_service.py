# services/vendor_service.py

from database.models import Vendor, Contact
from database.db_util import get_db_session
import logging

logger = logging.getLogger(__name__)

class VendorService:
    def match_vendor_with_contacts(self, vendor_name: str) -> int:
        """Match a vendor with existing contacts."""
        with get_db_session() as session:
            vendor = session.query(Vendor).filter_by(vendor_name=vendor_name).first()
            if vendor and vendor.contact:
                logger.debug(f"Found existing contact for vendor {vendor_name}")
                return vendor.contact.id
            else:
                logger.debug(f"No contact found for vendor {vendor_name}, creating new contact")
                contact_id = self.create_new_contact({'name': vendor_name})
                if not vendor:
                    vendor = Vendor(vendor_name=vendor_name)
                    session.add(vendor)
                vendor.contact_id = contact_id
                session.commit()
                return contact_id

    def create_new_contact(self, vendor_data: dict) -> int:
        """Create a new contact for a vendor."""
        with get_db_session() as session:
            contact = Contact(
                contact_id=vendor_data.get('contact_id', None),
                name=vendor_data.get('name', ''),
                email=vendor_data.get('email', ''),
                phone=vendor_data.get('phone', ''),
            )
            session.add(contact)
            session.commit()
            logger.debug(f"Created new contact with ID {contact.id}")
            return contact.id

    def link_contact_to_po(self, po_number: str, contact_id: int):
        """Link a contact to a PO."""
        with get_db_session() as session:
            po = session.query(PO).filter_by(po_number=po_number).first()
            if not po:
                logger.warning(f"PO {po_number} not found")
                return
            po.vendor.contact_id = contact_id
            session.commit()
            logger.debug(f"Linked contact {contact_id} to PO {po_number}")