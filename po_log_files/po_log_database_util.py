import logging

from database.db_util import get_db_session
from database.models import (
    Contact,
    PurchaseOrder
)
from utilities.singleton import SingletonMeta


class PoLogDatabaseUtil(metaclass=SingletonMeta):
    def __init__(self):
        if not hasattr(self, '_initialized'):
            # Set up logging
            self.logger = logging.getLogger("app_logger")
            self.logger.info("PO Log Database Util initialized")
            self._initialized = True

    # ---------------------- PREPROCESSING ----------------------
    def get_contact_surrogate_ids(self, contacts_list):
        """
        Looks for matching contacts and creates new ones if necessary.

        Args:
            contacts_list (list): List of dictionaries containing contact details.

        Returns:
            list: Updated list with contact surrogate IDs attached.
        """
        new_contact_list = []
        for contact in contacts_list:
            try:
                with get_db_session() as session:
                    # Check if contact exists in the database
                    db_contact = session.query(Contact).filter_by(name=contact.get("name")).one_or_none()
                    if db_contact:
                        # Add existing contact with surrogate ID to the list
                        new_contact_list.append({
                            "name": contact.get("name"),
                            "PO": contact.get("PO"),
                            "contact_surrogate_id": db_contact.contact_surrogate_id
                        })
                        self.logger.debug(f"Found in database: {contact.get('name')}")

                    else:
                        self.logger.debug(f"Not in database: {contact.get('name')}")

            except Exception as e:
                self.logger.error(f"Error processing contact '{contact.get('name', 'Unknown')}': {e}")

        return new_contact_list

    def link_contact_to_po(self, contacts, project_id):
        try:
            with get_db_session() as session:
                for contact in contacts:
                    # Query POs matching the project_id and contact's PO
                    po_records = session.query(PurchaseOrder).filter_by(
                        project_id=project_id,
                        po_number=contact["PO"]
                    ).all()

                    if not po_records:
                        self.logger.warning(
                            f"No PO records found for Project ID '{project_id}' and PO '{contact['PO']}'")
                        continue

                    for po in po_records:
                        original_contact_id = po.contact_id
                        po.contact_id = contact["contact_surrogate_id"]
                        self.logger.info(
                            f"Updated PO ID {contact.get('name')}: contact_id {original_contact_id} -> {po.contact_id}"
                        )

                session.commit()
                self.logger.info("Successfully updated PO records with contact surrogate IDs.")

        except Exception as e:
            self.logger.error(f"Error updating PO records: {e}")
            session.rollback()
            raise  # Re-raise exception after rollback

    def get_contact_pulse_id(self, contact_surrogate_id):
        try:
            with get_db_session() as session:
                # Check if contact exists in the database
                db_contact = session.query(Contact).filter_by(contact_surrogate_id=contact_surrogate_id).one_or_none()
                if db_contact:
                    # Add existing contact with surrogate ID to the list
                    pulse_id =  db_contact.pulse_id
                    self.logger.info(f"Contact Pulse ID found in database: {db_contact.name}")
                    return pulse_id
                else:
                    self.logger.warning(f"Contact Pulse ID not found in database: {db_contact.name}")
        except Exception as e:
            self.logger.error(f"Error retrieving contact pulse ID '{db_contact.name}': {e}")

    def get_pos_by_project_id(self, project_id):
        """
        Looks for POs in a project

        Args:
            project_id (str): Project Id used for querying items

        Returns:
            list: Updated list with po_items attached.
        """
        po_items = []
        try:
            with get_db_session() as session:
                # Check if contact exists in the database
                db_pos = session.query(PurchaseOrder).filter_by(project_id=project_id).all()
                for db in db_pos:
                    po_items.append({
                        "po_surrogate_id": db.po_surrogate_id,
                        "pulse_id": db.pulse_id,
                        "po_number": db.po_number,
                        "'contact_surrogate_id": db.contact_id
                    })
        except Exception as e:
            self.logger.error(f"Error querying POs: {e}")
        return po_items


    # --------------------- CREATE OR UPDATE METHODS ---------------------

    # ---------------------- MAIN EXECUTION ----------------------


po_log_database_util = PoLogDatabaseUtil()
