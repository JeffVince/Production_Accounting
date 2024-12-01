import json
import logging
from decimal import Decimal
from unicodedata import decimal

from sqlalchemy.exc import IntegrityError

from database.db_util import get_db_session
from database.models import (
    Contact,
    PurchaseOrder, DetailItem
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

    def prep_po_log_item_for_db(self, main_item, project_id):
        """
        Prepares the po log  payload into a database creation item.

        Args:
            event (dict): The po log item payload.

        Returns:
            dict: A dictionary representing the database creation item.
            :param project_id:
            :param main_item:
        """
        item = main_item

        # Prepare the database creation item
        creation_item = {}

        for key, value in item.items():
            if key == 'PO':
                creation_item["po_number"] = value
            if key == 'Actualized $':
                creation_item['amount_total'] = Decimal(value.replace("$", "").replace(",", ""))
            if key == 'St/Type':
                creation_item['state'] = value
            if key == 'Vendor':
                creation_item['name'] = value

        creation_item['project_id'] = project_id

        self.logger.debug(f'Prepared creation item: {creation_item}')

        return creation_item

    def prep_po_log_detail_for_db(self, detail_item):
        print("test")

    # --------------------- CREATE / UPDATE -------------------
    def create_or_update_main_item_in_db(self, item_data):
        """
        Creates or updates a main item record in the database.

        Args:
            item_data (dict): The prepared database creation item containing details.

        Returns:
            str: Status message indicating success, creation, or failure.
        """
        project_id = item_data['project_id']
        po_number = item_data['po_number']
        name = item_data['name']

        with get_db_session() as session:
            try:
                # Check if the item already exists in the database
                po_item = session.query(PurchaseOrder).filter_by(po_number=po_number, project_id=project_id).one_or_none()
                if po_item:
                    # Update existing record
                    for db_field, value in item_data.items():
                        setattr(po_item, db_field, value)
                    self.logger.info(f"Updated existing Purchase Order: {project_id}_{po_number} {name}")
                    result = {
                        "stats": "Updated",
                        "item": po_item
                    }
                else:  # Create a new record
                    new_item = PurchaseOrder(**item_data)
                    session.add(new_item)
                    self.logger.info(f"Created new Purchase Order: {project_id}_{po_number} {name}")
                    po_item = session.query(PurchaseOrder).filter_by(po_number=po_number,
                                                                     project_id=project_id).one_or_none()

                    result = {
                        "stats": "Updated",
                        "item": po_item
                    }
                    # Commit the transaction
                session.commit()
                return result
            except Exception as e:
                session.rollback()
                self.logger.error(f"Error processing PurchaseOrder in DB: {e}")
                return "Fail"

    def create_or_update_sub_item_in_db(self, item_data):
        """
        Creates or updates a subitem record in the database.

        Args:
            item_data (dict): The prepared database creation item containing subitem details.

        Returns:
            dict: Status message indicating success, creation, or failure.
        """
        pulse_id = item_data.get("pulse_id")

        with get_db_session() as session:
            try:
                # Check if detail item exists
                detail_item = session.query(DetailItem).filter_by(pulse_id=pulse_id).one_or_none()
                # If exists, update it
                if detail_item:
                    for db_field, value in item_data.items():
                        if value is not None:
                            setattr(detail_item, db_field, value)
                    session.commit()
                    self.logger.debug(f"Updated existing DetailItem with pulse_id: {pulse_id}")
                    return {
                        "status": "Updated",
                    }
                else:
                    # Create a new DetailItem
                    new_detail_item = DetailItem(**item_data)
                    session.add(new_detail_item)
                    session.commit()
                    self.logger.info(
                        f"Created new DetailItem with pulse_id: {pulse_id}, surrogate_id: {new_detail_item.detail_item_surrogate_id}")
                    return {
                        "status": "Created",
                    }
            except IntegrityError as ie:
                session.rollback()
                self.logger.error(f"IntegrityError processing DetailItem in DB: {ie}")
                return {
                    "status": "Fail",
                    "error": str(ie)
                }
            except Exception as e:
                session.rollback()
                self.logger.error(f"Error processing DetailItem in DB: {e}")
                return {
                    "status": "Fail",
                    "error": str(e)
                }

    def create_or_update_contact_item_in_db(self, item_data):
        """
        Creates or updates a contact record in the database.

        Args:
            item_data (dict): The prepared database creation item containing contact details.

        Returns:
            str: Status message indicating success, creation, or failure.
        """
        pulse_id = item_data.get("pulse_id")

        with get_db_session() as session:
            try:
                # Check if the contact already exists in the database
                contact_item = session.query(Contact).filter_by(pulse_id=pulse_id).one_or_none()

                if contact_item:
                    # Update existing record
                    for db_field, value in item_data.items():
                        if db_field != "pulse_id":  # Skip pulse_id since it's the key
                            setattr(contact_item, db_field, value)
                    self.logger.info(f"Updated existing Contact with pulse_id: {pulse_id}")
                    status = "Updated"
                else:  # Create a new record
                    new_contact = Contact(**item_data)
                    session.add(new_contact)
                    self.logger.info(f"Created new Contact with pulse_id: {pulse_id}")
                    status = "Created"
                # Commit the transaction
                session.commit()
                return status
            except Exception as e:
                session.rollback()
                self.logger.error(f"Error processing Contact in DB: {e}")
                return "Fail"

    # --------------------- READ  -------------------

    def fetch_po_by_id(self, po_surrogate_id):
        print("test")

    def fetch_detail_by_id(self, detail_surrogate_id):
        print("test")


po_log_database_util = PoLogDatabaseUtil()
