import logging
import re
from decimal import Decimal, InvalidOperation

from dateutil.parser import parser
from sqlalchemy.exc import IntegrityError

from database.db_util import get_db_session
from database.models import (
    Contact,
    PurchaseOrder, DetailItem
)
from utilities.singleton import SingletonMeta
from dateutil import parser
from datetime import datetime, timedelta

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
        # Prepare the database creation item
        creation_item = {}

        for key, value in main_item.items():
            if key == 'PO':
                creation_item["po_number"] = value

        creation_item['project_id'] = project_id

        self.logger.debug(f'Prepared creation item: {creation_item}')

        return creation_item

    def prep_po_log_detail_for_db(self, detail_item):
        print("test")

    # --------------------- CREATE / UPDATE -------------------
    def create_or_update_main_item_in_db(self, item):
        """
        Creates or updates a main item record in the database.

        Args:
            item (dict): The prepared database creation item containing details.

        Returns:
            str: Status message indicating success, creation, or failure.
            :param item:
        """
        db_item = {}
        db_item["project_id"] = item['project_id']
        db_item["po_number"] = item['PO']
        db_item["contact_id"] = item['contact_surrogate_id']
        db_item["description"] = item["description"]
        db_item["po_type"] = item["po_type"]
        db_item["pulse_id"] = item["item_pulse_id"]
        db_item["state"] = item["contact_status"]
        if db_item["po_type"] == "CC":
            db_item["description"] = "Credit Card Purchases"
        if db_item["po_type"] == "PC":
            db_item["description"] = "Petty Cash Purchases"
        with get_db_session() as session:
            try:
                # Check if the item already exists in the database - update contact.
                po_item = session.query(PurchaseOrder).filter_by(po_number=db_item["po_number"], project_id=db_item["project_id"]).one_or_none()
                if po_item:
                    self.logger.info(f"Purchase Order Exists: {db_item['project_id']}_{db_item['po_number']}")
                    po_item.contact_id = db_item["contact_id"]
                    po_item.state = db_item["state"]
                    po_item.contact_id = item['contact_surrogate_id']
                    if not db_item["description"] == "":
                        po_item.description = db_item["description"]
                    session.commit()
                    item["po_surrogate_id"] = po_item.po_surrogate_id
                    return item
                else:  # Create a new record
                    new_item = PurchaseOrder(**db_item)
                    session.add(new_item)
                    session.commit()
                    self.logger.info(f"Created new Purchase Order: {db_item['project_id']}_{ db_item['po_number']}")
                    po_item = session.query(PurchaseOrder).filter_by(po_number=db_item['po_number'],
                                                                     project_id=db_item['project_id']).one_or_none()
                    item["po_surrogate_id"] = po_item.po_surrogate_id
                    return item
            except Exception as e:
                session.rollback()
                self.logger.error(f"Error processing PurchaseOrder in DB: {e}")
                return None

    def create_or_update_sub_item_in_db(self, item):
        """
        Creates or updates a subitem record in the database.

        Args:
            item_data (dict): The prepared database creation item containing subitem details.

        Returns:
            dict: Status message indicating success, creation, or failure.
        """
        db_item = {}
        db_item["quantity"] = 1
        db_item["payment_type"] = item["payment_type"]
        db_item["description"] = item["description"]
        db_item["parent_pulse_id"] = item["parent_pulse_id"]
        db_item["detail_item_number"] = item["item_id"]
        db_item["pulse_id"] = item["pulse_id"]
        db_item["parent_surrogate_id"] = item["po_surrogate_id"]
        db_item["ot"] = item["OT"]
        db_item["fringes"] = item["fringes"]
        db_item["vendor"] = item["vendor"]
        if item["parent_status"] == "RTP":
            db_item["state"] = "RTP"
        else:
            db_item["state"] = "PENDING"

        rate = item["rate"]
        #region RATE CLEANER
        try:
            # Clean the rate: remove commas, strip whitespace, and convert to Decimal
            cleaned_rate = Decimal(str(rate).replace(',', '').strip())
            db_item["rate"] = float(cleaned_rate)  # Convert to float if required
        except (ValueError, InvalidOperation) as e:
            self.logger.error(f"Invalid rate value '{rate}': {e}")
            db_item["rate"]  = None
        #endregion

        date =  item["date"]
        #region DATE CLEANER
        try:
            # Check if `date` is a valid non-empty string
            if isinstance(date, str) and date.strip():
                # Parse and format the `date` to ensure it's in 'YYYY-MM-DD'
                parsed_date = parser.parse(date.strip())
                formatted_date = parsed_date.strftime('%Y-%m-%d')
                db_item["transaction_date"] = formatted_date
                if item["payment_type"] == "CRD":
                    db_item["due_date"] = formatted_date
                else:
                    # Ensure due_date is 30 days after the parsed and formatted date
                    due_date = (parsed_date + timedelta(days=30)).strftime('%Y-%m-%d')
                    db_item["due_date"] = due_date
            else:
                raise ValueError(f"Invalid date value: {date}")
        except Exception as e:
            self.logger.error(f"Error parsing and formatting date '{date}': {e}")
        #endregion

        account_number = item["account"]
        #region ACCOUNT CLEANER
        try:
            # Clean and extract only numeric parts from the account number
            cleaned_account_number = re.sub(r'[^\d]', '', str(account_number).strip())
            # Convert the cleaned value to an integer
            if cleaned_account_number:
                db_item["account_number"] = int(cleaned_account_number)
            else:
                raise ValueError(f"Account number '{account_number}' resulted in an empty value after cleaning.")
        except (ValueError, TypeError) as e:
            self.logger.error(f"Invalid account number value '{account_number}': {e}")
            db_item["account_number"] = None
        #endregion

        with get_db_session() as session:
            try:
                # Check if detail item exists
                detail_item = session.query(DetailItem).filter_by(parent_surrogate_id=item["po_surrogate_id"], detail_item_number = item["item_id"]).one_or_none()
                # If exists, update it
                if detail_item:
                    for db_field, value in item.items():
                        if value is not None:
                            setattr(detail_item, db_field, value)
                    session.commit()
                    self.logger.debug(f"Updated existing DetailItem: {item['vendor']}")
                else:
                    # Create a new DetailItem
                    new_detail_item = DetailItem(**db_item)
                    session.add(new_detail_item)
                    session.commit()
                    self.logger.info(
                        f"Created new DetailItem: {item['vendor']}, surrogate_id: {new_detail_item.detail_item_surrogate_id}")

                detail_item = session.query(DetailItem).filter_by(parent_surrogate_id=item["po_surrogate_id"], detail_item_number = item["item_id"]).one_or_none()
                item["detail_item_surrogate_id"] = detail_item.detail_item_surrogate_id
                return item

            except IntegrityError as ie:
                session.rollback()
                self.logger.exception(f"IntegrityError processing DetailItem in DB: {ie}")
                return {
                    "status": "Fail",
                    "error": str(ie)
                }
            except Exception as e:
                session.rollback()
                self.logger.exception(f"Error processing DetailItem in DB: {e}")
                return {
                    "status": "Fail",
                    "error": str(e)
                }

    def find_or_create_contact_item_in_db(self, item):
        """
        Creates or updates a contact record in the database.

        Args:
            item (dict): The prepared database creation item containing contact details.

        Returns:
            str: Status message indicating success, creation, or failure.
        """
        db_item = {}
        db_item["name"] = item["name"]
        db_item["vendor_status"] = item["contact_status"]
        db_item["payment_details"] = item["contact_payment_details"]
        db_item["pulse_id"] = item["contact_pulse_id"]
        db_item["vendor_type"] = item["po_type"]
        db_item["email"] = item["contact_email"]
        db_item["address_line_1"] = item["address_line_1"]
        db_item["city"] = item["city"]
        db_item["zip"] = item["zip"]
        db_item["tax_ID"] = item["tax_id"]
        db_item["tax_form_link"] = item["tax_form_link"]
        db_item["country"] = item["contact_country"]
        db_item["tax_type"] = item["contact_tax_type"]
        db_item["phone"] = item["contact_phone"]

        with get_db_session() as session:
            try:
                if item["po_type"] == "CC":
                    cc_item = session.query(Contact).filter_by(name="Company Credit Card").one_or_none()
                    item["contact_surrogate_id"] = cc_item.contact_surrogate_id
                    return item
                elif item["po_type"] == "PC":
                    pc_item = session.query(Contact).filter_by(name="PETTY CASH").one_or_none()
                    item["contact_surrogate_id"] = pc_item.contact_surrogate_id
                    return item

                # Check if the contact already exists in the database
                contact_item = session.query(Contact).filter_by(pulse_id=db_item["pulse_id"]).one_or_none()
                if contact_item:
                    for key, value in db_item.items():
                        setattr(contact_item, key, value)
                    session.commit()
                    self.logger.info(f"Existing Contact with name: {db_item['name']}")
                    item["contact_surrogate_id"] = contact_item.contact_surrogate_id
                    return item
                else:  # Create a new record
                    new_contact = Contact(**db_item)
                    session.add(new_contact)
                    self.logger.info(f"Created new Contact: {db_item['name']}")
                    # Commit the transaction
                    session.commit()
                    contact_item = session.query(Contact).filter_by(name=db_item['name']).one_or_none()
                    item["contact_surrogate_id"] =contact_item.contact_surrogate_id
                    return item
            except Exception as e:
                session.rollback()
                self.logger.error(f"Error processing Contact in DB: {e}")
                return "Fail"

    # --------------------- READ  -------------------

    def fetch_po_by_id(self, project_id):
        from sqlalchemy.sql import text
        from flask import jsonify

        with get_db_session() as session:
            try:
                # Execute a raw SQL query against the view
                result = session.execute(text("""
                    SELECT 
                        `Contact Name`,
                        `Project ID`,
                        `PO #`,
                        `Description`,
                        `Tax Form Link Exists`,
                        `Total Amount`,
                        `PO Status`,
                        `Payment Details`,
                        `Folder Link Exists`,
                        `Contact Email`,
                        `Contact Phone`,
                        `Contact SSN`
                    FROM `virtual_pm`.`vw_purchase_order_summary`
                    WHERE `Project ID` = :project_id
                """), {"project_id": project_id})

                rows = result.fetchall()

                objects_as_dict = []
                for row in rows:
                    # row is a Row object from SQLAlchemy
                    # Use row._mapping which returns a dictionary-like object
                    row_dict = dict(row._mapping)
                    objects_as_dict.append(row_dict)

                return jsonify(objects_as_dict)

            except Exception as e:
                self.logger.error(f"Error retrieving objects from view for project_id {project_id}: {e}")
                resp = jsonify({"error": "Could not retrieve objects"})
                resp.status_code = 500
                return resp

    def fetch_detail_by_id(self, detail_surrogate_id):
        print("test")


po_log_database_util = PoLogDatabaseUtil()
