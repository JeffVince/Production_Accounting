# po_log_database_util.py
# 📚✨ PO Log Database Utility: Provides database operations for PO logs, contacts, items, etc. ✨📚
# Jeff, here's your po_log_database_util file with:
# - The get_contact_by_name function
# - Regions, emojis, comments, docstrings, try/catch blocks
# - Ensured class is initialized the same way and all existing functionality remains intact
# - Conditional pulse_id setting logic in main and sub items
# - Default 'PENDING' state if contact_status is None, preventing IntegrityError for NULL state

import logging
import re
from decimal import Decimal, InvalidOperation

from dateutil.parser import parser
from sqlalchemy.exc import IntegrityError

from database.db_util import get_db_session
from database.models import (
    Contact,
    PurchaseOrder,
    DetailItem
)
from utilities.singleton import SingletonMeta
from dateutil import parser
from datetime import datetime, timedelta


# region 🏢 Class Definition
class PoLogDatabaseUtil(metaclass=SingletonMeta):
    """
    📚 This utility class provides various database operations for handling PO logs,
    contacts, items, and other related entities. It uses a SingletonMeta to ensure
    only one instance is active, and interacts with the database via SQLAlchemy sessions.
    """

    def __init__(self):
        # region 🔧 Initialization
        if not hasattr(self, '_initialized'):
            # Set up logging
            self.logger = logging.getLogger("app_logger")
            self.logger.info("PO Log Database Util initialized")
            self._initialized = True
        # endregion

    # region 🏗 PREPROCESSING METHODS
    def get_contact_surrogate_ids(self, contacts_list):
        """
        🗂 Looks for matching contacts in the DB and returns updated list with their surrogate IDs.

        Args:
            contacts_list (list): List of dictionaries containing contact details.

        Returns:
            list: Updated list with contact surrogate IDs attached.
        """
        new_contact_list = []
        for contact in contacts_list:
            try:
                with get_db_session() as session:
                    db_contact = session.query(Contact).filter_by(name=contact.get("name")).one_or_none()
                    if db_contact:
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
        """
        🔗 Link contacts to Purchase Orders in the database.

        Args:
            contacts (list): List of contact dicts or identifiers.
            project_id (str): Project ID to link these contacts to.
        """
        try:
            with get_db_session() as session:
                for contact in contacts:
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
            with get_db_session() as session:
                session.rollback()
            raise

    def get_contact_pulse_id(self, contact_surrogate_id):
        """
        💡 Given a contact surrogate ID, retrieve its Monday.com pulse_id from the DB.

        Args:
            contact_surrogate_id (int): The surrogate ID of the contact in the DB.

        Returns:
            int or None: The pulse_id if exists, else None.
        """
        try:
            with get_db_session() as session:
                db_contact = session.query(Contact).filter_by(contact_surrogate_id=contact_surrogate_id).one_or_none()
                if db_contact:
                    pulse_id = db_contact.pulse_id
                    self.logger.info(f"Contact Pulse ID found in database: {db_contact.name}")
                    return pulse_id
                else:
                    self.logger.warning("Contact Pulse ID not found in database.")
        except Exception as e:
            self.logger.error(f"Error retrieving contact pulse ID: {e}")
        return None

    def get_pos_by_project_id(self, project_id):
        """
        🔍 Looks for POs in a given project and returns their details.

        Args:
            project_id (str): Project ID used for querying items

        Returns:
            list: List of PO items (dictionaries).
        """
        po_items = []
        try:
            with get_db_session() as session:
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
        🏗 Prepares the po log payload into a database creation item.

        Args:
            main_item (dict): The PO log item payload.
            project_id (str): The project ID.

        Returns:
            dict: A dictionary representing the database creation item.
        """
        creation_item = {}
        for key, value in main_item.items():
            if key == 'PO':
                creation_item["po_number"] = value
        creation_item['project_id'] = project_id
        self.logger.debug(f'Prepared creation item: {creation_item}')
        return creation_item

    def prep_po_log_detail_for_db(self, detail_item):
        """
        🏗 Prepares a PO log detail item for the database.
        """
        print("test")

    def is_unchanged(self, detail_item, main_item):
        """
        🤔 Determines if a detail_item has unchanged values compared to the DB record.

        Args:
            detail_item (dict): The detail item dictionary.
            main_item (dict): The main item dictionary.

        Returns:
            bool: True if unchanged, False otherwise.
        """
        with get_db_session() as session:
            try:
                main_item_record = session.query(PurchaseOrder).filter_by(
                    po_number=main_item["PO"],
                    project_id=main_item["project_id"]
                ).one_or_none()

                if not main_item_record:
                    self.logger.warning(f"Main item not found: {main_item}")
                    return False

                detail_item_record = session.query(DetailItem).filter_by(
                    parent_surrogate_id=main_item_record.po_surrogate_id,
                    detail_item_number=detail_item["item_id"]
                ).one_or_none()

                if not detail_item_record:
                    self.logger.warning(f"Detail item not found: {detail_item}")
                    return False

                key_mapping = {
                    "date": "transaction_date",
                    "due date": "due_date",
                    "rate": "rate",
                    "quantity": "quantity",
                    "payment_type": "payment_type",
                    "OT": "ot",
                    "fringes": "fringes",
                    "description": "description",
                    "state": "state",
                    "account": "account_number",
                }

                for key, model_field in key_mapping.items():
                    provided_value = detail_item.get(key)

                    # Type conversions
                    if model_field in ["transaction_date", "due_date"] and provided_value:
                        provided_value = datetime.strptime(provided_value, "%Y-%m-%d")
                    elif model_field in ["rate", "quantity", "ot", "fringes", "sub_total"] and provided_value is not None:
                        provided_value = Decimal(provided_value)
                    elif isinstance(provided_value, str):
                        provided_value = provided_value.strip().lower()

                    db_value = getattr(detail_item_record, model_field)
                    if isinstance(db_value, str):
                        db_value = db_value.strip().lower()

                    if provided_value != db_value:
                        self.logger.debug(f"Field '{key}' differs: {db_value} != {provided_value}")
                        return False
                self.logger.info("No Changes DETECTED")
                return True
            except Exception as e:
                self.logger.exception(f"Error checking for unchanged detail item: {e}")
                return False
    # endregion

    # region 🏗 CREATE / UPDATE METHODS
    def create_or_update_main_item_in_db(self, item):
        """
        🏗 Creates or updates a main item record in the database.
        Ensures 'state' is never None by providing a default ('PENDING') if contact_status is None.

        Args:
            item (dict): The prepared database creation item with details.

        Returns:
            dict or None: Updated item with `po_surrogate_id` or None on failure.
        """
        db_item = {}
        db_item["project_id"] = item['project_id']
        db_item["po_number"] = item['PO']
        db_item["contact_id"] = item['contact_surrogate_id']
        db_item["description"] = item["description"]
        db_item["po_type"] = item["po_type"]
        # Only set pulse_id if it exists in item
        if "item_pulse_id" in item:
            db_item["pulse_id"] = item["item_pulse_id"]

        # Default state to 'PENDING' if contact_status is None or empty
        db_item["state"] = item.get("contact_status") or "PENDING"

        if db_item["po_type"] == "CC":
            db_item["description"] = "Credit Card Purchases"
        if db_item["po_type"] == "PC":
            db_item["description"] = "Petty Cash Purchases"

        with get_db_session() as session:
            try:
                po_item = session.query(PurchaseOrder).filter_by(po_number=db_item["po_number"],
                                                                 project_id=db_item["project_id"]).one_or_none()
                if po_item:
                    self.logger.info(f"Purchase Order Exists: {db_item['project_id']}_{db_item['po_number']}")
                    po_item.contact_id = db_item["contact_id"]
                    po_item.state = db_item["state"]
                    po_item.contact_id = item['contact_surrogate_id']
                    if db_item["description"] != "":
                        po_item.description = db_item["description"]
                    # Update pulse_id if provided
                    if "pulse_id" in db_item:
                        po_item.pulse_id = db_item["pulse_id"]
                    session.commit()
                    item["po_surrogate_id"] = po_item.po_surrogate_id
                    return item
                else:
                    new_item = PurchaseOrder(**db_item)
                    session.add(new_item)
                    session.commit()
                    self.logger.info(f"Created new Purchase Order: {db_item['project_id']}_{db_item['po_number']}")
                    po_item = session.query(PurchaseOrder).filter_by(po_number=db_item['po_number'],
                                                                     project_id=db_item['project_id']).one_or_none()
                    item["po_surrogate_id"] = po_item.po_surrogate_id
                    return item
            except Exception as e:
                session.rollback()
                self.logger.error(f"Error processing PurchaseOrder in DB: {e}", exc_info=True)
                return None

    def create_or_update_sub_item_in_db(self, item):
        """
        🏗 Creates or updates a subitem record in the database using a surrogate ID.
        If `pulse_id` is present, it will be stored in the DB.
        Ensures that after this call, `item` has `detail_item_surrogate_id`.
        """
        db_item = {}
        db_item["quantity"] = 1
        db_item["payment_type"] = item["payment_type"]
        db_item["description"] = item["description"]
        if "parent_pulse_id" in item:
            db_item["parent_pulse_id"] = item["parent_pulse_id"]

        db_item["detail_item_number"] = item["item_id"]
        if "pulse_id" in item:
            db_item["pulse_id"] = item["pulse_id"]

        db_item["parent_surrogate_id"] = item["po_surrogate_id"]
        db_item["ot"] = item["OT"]
        db_item["fringes"] = item["fringes"]
        db_item["vendor"] = item["vendor"]
        db_item["state"] = "RTP" if item["parent_status"] == "RTP" else "PENDING"

        rate = item["rate"]
        # region 🧹 RATE CLEANER
        try:
            cleaned_rate = Decimal(str(rate).replace(',', '').strip())
            db_item["rate"] = float(cleaned_rate)
        except (ValueError, InvalidOperation) as e:
            self.logger.error(f"Invalid rate value '{rate}': {e}")
            db_item["rate"] = None
        # endregion

        date = item["date"]
        # region 🗓 DATE CLEANER
        try:
            if isinstance(date, str) and date.strip():
                parsed_date = parser.parse(date.strip())
                formatted_date = parsed_date.strftime('%Y-%m-%d')
                db_item["transaction_date"] = formatted_date
                if item["payment_type"] == "CRD":
                    db_item["due_date"] = formatted_date
                else:
                    due_date = (parsed_date + timedelta(days=30)).strftime('%Y-%m-%d')
                    db_item["due_date"] = due_date
            else:
                raise ValueError(f"Invalid date value: {date}")
        except Exception as e:
            self.logger.error(f"Error parsing and formatting date '{date}': {e}")
        # endregion

        account_number = item["account"]
        # region 💳 ACCOUNT CLEANER
        try:
            cleaned_account_number = re.sub(r'[^\d]', '', str(account_number).strip())
            if cleaned_account_number:
                db_item["account_number"] = int(cleaned_account_number)
            else:
                raise ValueError(f"Account number '{account_number}' resulted in an empty value.")
        except (ValueError, TypeError) as e:
            self.logger.error(f"Invalid account number value '{account_number}': {e}")
            db_item["account_number"] = None
        # endregion

        with get_db_session() as session:
            try:
                detail_item = session.query(DetailItem).filter_by(
                    parent_surrogate_id=item["po_surrogate_id"],
                    detail_item_number=item["item_id"]
                ).one_or_none()

                if detail_item:
                    # Update existing record
                    for db_field, value in db_item.items():
                        if value is not None:
                            setattr(detail_item, db_field, value)
                    session.commit()
                    self.logger.debug(f"Updated existing DetailItem: {item['vendor']}")
                else:
                    # Create a new record
                    new_detail_item = DetailItem(**db_item)
                    session.add(new_detail_item)
                    session.commit()
                    detail_item = new_detail_item
                    self.logger.info(
                        f"Created new DetailItem: {item['vendor']}, surrogate_id: {detail_item.detail_item_surrogate_id}"
                    )

                # Retrieve and store the surrogate_id
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
        Creates or updates a contact record in the database without creating duplicates.
        Assumes 'tax_id' or 'pulse_id' can identify an existing contact uniquely.
        """
        db_item = {
            "name": item.get("name"),
            "vendor_type": item.get("po_type"),
            "payment_details": item.get("contact_payment_details") or "PENDING",
            "pulse_id": item.get("contact_pulse_id"),
            "email": item.get("contact_email"),
            "phone": item.get("contact_phone"),
            "address_line_1": item.get("address_line_1"),
            "city": item.get("city"),
            "zip": item.get("zip"),
            "tax_ID": item.get("tax_id"),
            "tax_form_link": item.get("tax_form_link"),
            "tax_type": item.get("contact_tax_type") or "SSN",
            "vendor_status": item.get("contact_status") or "PENDING",
            "country": item.get("contact_country")
        }

        with get_db_session() as session:
            try:
                # 1. Attempt to find by tax_ID if present
                contact_query = None
                if db_item["tax_ID"]:
                    contact_query = session.query(Contact).filter_by(tax_ID=db_item["tax_ID"]).one_or_none()

                # 2. If not found by tax_ID, try name
                if not contact_query and db_item["name"]:
                    contact_query = session.query(Contact).filter_by(name=db_item["name"]).one_or_none()

                # If found, update; else create new
                if contact_query:
                    self.logger.info(f"Existing Contact found in DB: {db_item['name']}")
                    for key, value in db_item.items():
                        if value is not None:
                            setattr(contact_query, key, value)
                    session.commit()
                    item["contact_surrogate_id"] = contact_query.contact_surrogate_id
                    return item
                else:
                    new_contact = Contact(**db_item)
                    session.add(new_contact)
                    session.commit()

                    created_contact = session.query(Contact).filter_by(name=db_item['name']).one_or_none()
                    if created_contact:
                        self.logger.info(f"Created new Contact: {db_item['name']}")
                        item["contact_surrogate_id"] = created_contact.contact_surrogate_id
                        return item
                    else:
                        self.logger.error("Contact creation failed unexpectedly.")
                        return "Fail"

            except Exception as e:
                session.rollback()
                self.logger.error(f"Error processing Contact in DB: {e}", exc_info=True)
                return "Fail"
    # endregion

    # region 🗂 CONTACT RETRIEVAL
    def get_contact_by_name(self, name: str):
        """
        🗂 Retrieve a contact record from the DB by their name and return it as a dictionary.

        Args:
            name (str): The name of the contact.

        Returns:
            dict or None: A dictionary of contact attributes if found, else None.
        """
        try:
            with get_db_session() as session:
                self.logger.debug(f"🔍 Searching for contact by name: {name}")
                contact = session.query(Contact).filter_by(name=name).one_or_none()
                if contact:
                    self.logger.info(f"Found contact: {contact.name}")
                    contact_dict = {
                        'payment_details': contact.payment_details,
                        'email': contact.email,
                        'phone': contact.phone,
                        'address_line_1': contact.address_line_1,
                        'city': contact.city,
                        'zip': contact.zip,
                        'tax_id': contact.tax_ID,
                        'tax_form_link': contact.tax_form_link,
                        'contact_status': contact.vendor_status,
                        'country': contact.country,
                        'tax_type': contact.tax_type
                    }
                    return contact_dict
                else:
                    self.logger.info(f"No contact found with name: {name}")
                    return None
        except Exception as e:
            self.logger.error(f"💥 Database error in get_contact_by_name: {e}", exc_info=True)
            return None
    # endregion

    def fetch_po_by_id(self, project_id):
        """
        🔍 Retrieve PO and Detail Items for a given project_id.
        Returns a JSON structure including both PO summary and associated detail items.
        """
        from sqlalchemy.sql import text
        from flask import jsonify

        with get_db_session() as session:
            try:
                # Fetch all POs for the given project_id
                po_result = session.execute(text("""
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

                po_rows = po_result.fetchall()

                objects_as_dict = []

                for row in po_rows:
                    po_dict = dict(row._mapping)
                    po_number = po_dict.get('PO #')

                    # Fetch detail items associated with this PO
                    # Adjust the join and column names based on your actual schema.
                    # This assumes:
                    #   - detail_items.po_surrogate_id references purchase_orders.po_surrogate_id
                    #   - purchase_orders.po_number = :po_number
                    detail_result = session.execute(text("""
                        SELECT 
                            detail_items.detail_item_surrogate_id,
                            detail_items.transaction_date,
                            detail_items.due_date,
                            detail_items.pulse_id,
                            detail_items.rate,
                            detail_items.quantity,
                            detail_items.sub_total,
                            detail_items.payment_type,
                            detail_items.ot,
                            detail_items.fringes,
                            detail_items.vendor,
                            detail_items.description AS detail_description,
                            detail_items.file_link,
                            detail_items.state,
                            detail_items.account_number
                        FROM `virtual_pm`.`detail_items`
                        JOIN `virtual_pm`.`purchase_orders` po 
                            ON po.po_surrogate_id = detail_items.parent_surrogate_id
                        WHERE po.po_number = :po_number
                    """), {"po_number": po_number})

                    detail_rows = detail_result.fetchall()
                    detail_items = []
                    for d_row in detail_rows:
                        d_dict = dict(d_row._mapping)
                        detail_items.append(d_dict)

                    # Add detail items to the PO dictionary
                    po_dict['detail_items'] = detail_items
                    objects_as_dict.append(po_dict)

                return jsonify(objects_as_dict)

            except Exception as e:
                self.logger.error(f"Error retrieving objects from view for project_id {project_id}: {e}")
                resp = jsonify({"error": "Could not retrieve objects"})
                resp.status_code = 500
                return resp

    def fetch_detail_by_id(self, detail_surrogate_id):
        """
        🔍 Placeholder method to fetch detail by ID.
        Currently prints 'test'.
        """
        print("test")
    # endregion


# endregion

# 🎉 Instantiate the PoLogDatabaseUtil Singleton
po_log_database_util = PoLogDatabaseUtil()