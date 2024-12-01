import logging

from database.db_util import get_db_session
from database.models import (
    Contact,
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
                        self.logger.info(f"Found in database: {contact.get('name')}")

                    else:
                        self.logger.info(f"Not in database: {contact.get('name')}")

            except Exception as e:
                self.logger.error(f"Error processing contact '{contact.get('name', 'Unknown')}': {e}")

        return new_contact_list

    # --------------------- CREATE OR UPDATE METHODS ---------------------

    # ---------------------- MAIN EXECUTION ----------------------


po_log_database_util = PoLogDatabaseUtil()
