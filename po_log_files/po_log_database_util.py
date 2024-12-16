# po_log_database_util.py
# 📚✨ PO Log Database Utility: Provides database operations for PO logs, contacts, items, etc. ✨📚
# - The get_contact_by_name function
# - Regions, emojis, comments, docstrings, try/catch blocks
# - Ensured class is initialized the same way and all existing functionality remains intact
# - Conditional pulse_id setting logic in main and sub items
# - Default 'PENDING' state if contact_status is None, preventing IntegrityError for NULL state
#
# ✨ Additional Implementations ✨
# 🏗 Implemented the following functions as requested:
# - get_po_with_details(po_surrogate_id)
# - update_po_folder_link(po_surrogate_id, folder_link)
# - update_po_tax_form_link(po_surrogate_id, tax_form_link)
# - update_detail_item_file_link(detail_item_surrogate_id, file_link)
# - get_project_folder_name(project_id)
#
# 📚 All functions include extensive logging, comments, try/except blocks, and tons of emojis for easy scanning!
# 🔍 Carefully check each region for the implemented logic and error handling.

import logging
import re
from decimal import Decimal, InvalidOperation

from dateutil.parser import parser
from sqlalchemy.exc import IntegrityError
from database.db_util import get_db_session
from database.models import (
    Contact,
    PurchaseOrder,
    DetailItem,
    Project
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

    ✨ Features:
    - Lots of emojis for quick scanning!
    - Detailed docstrings and exceptions
    - Safe DB operations with try/catch
    - Preprocessing methods, Create/Update methods, and retrieval methods
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
                        self.logger.debug(f"🤝 Found in database: {contact.get('name')}")
                    else:
                        self.logger.debug(f"🙅 Not in database: {contact.get('name')}")
            except Exception as e:
                self.logger.error(f"💥 Error processing contact '{contact.get('name', 'Unknown')}': {e}", exc_info=True)

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
                            f"⚠️ No PO records found for Project ID '{project_id}' and PO '{contact['PO']}'")
                        continue

                    for po in po_records:
                        original_contact_id = po.contact_id
                        po.contact_id = contact["contact_surrogate_id"]
                        self.logger.info(
                            f"🔄 Updated PO ID {contact.get('name')}: contact_id {original_contact_id} -> {po.contact_id}"
                        )

                session.commit()
                self.logger.info("✅ Successfully updated PO records with contact surrogate IDs.")

        except Exception as e:
            self.logger.error(f"💥 Error updating PO records: {e}", exc_info=True)
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
                    self.logger.info(f"🔎 Contact Pulse ID found in database: {db_contact.name} (pulse_id={pulse_id})")
                    return pulse_id
                else:
                    self.logger.warning("⚠️ Contact Pulse ID not found in database.")
        except Exception as e:
            self.logger.error(f"💥 Error retrieving contact pulse ID: {e}", exc_info=True)
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
            self.logger.error(f"💥 Error querying POs: {e}", exc_info=True)
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
        self.logger.debug(f'🛠 Prepared creation item: {creation_item}')
        return creation_item

    def prep_po_log_detail_for_db(self, detail_item):
        """
        🏗 Prepares a PO log detail item for the database. (Currently just a placeholder)
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
                    self.logger.warning(f"⚠️ Main item not found: {main_item}")
                    return False

                detail_item_record = session.query(DetailItem).filter_by(
                    parent_surrogate_id=main_item_record.po_surrogate_id,
                    detail_item_number=detail_item["detail_item_id"],
                    line_id=detail_item["line_id"]
                ).one_or_none()

                if not detail_item_record:
                    self.logger.warning(f"⚠️ Detail item not found: {detail_item}")
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
                        self.logger.debug(f"✏️ Field '{key}' differs: {db_value} != {provided_value}")
                        return False
                self.logger.debug("✅ No Changes DETECTED")
                return True
            except Exception as e:
                self.logger.exception(f"💥 Error checking for unchanged detail item: {e}", exc_info=True)
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
        db_item["contact_name"] = item["contact_name"]
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
                    self.logger.info(f"📝 Purchase Order Exists: {db_item['project_id']}_{db_item['po_number']}")
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
                    self.logger.info(f"🎉 Created new Purchase Order: {db_item['project_id']}_{db_item['po_number']}")
                    po_item = session.query(PurchaseOrder).filter_by(po_number=db_item['po_number'],
                                                                     project_id=db_item['project_id']).one_or_none()
                    item["po_surrogate_id"] = po_item.po_surrogate_id
                    return item
            except Exception as e:
                session.rollback()
                self.logger.error(f"💥 Error processing PurchaseOrder in DB: {e}", exc_info=True)
                return None

    def create_or_update_sub_item_in_db(self, item):
        """
        🏗 Creates or updates a subitem record in the database using a surrogate ID.
        If `pulse_id` is present, it will be stored in the DB.
        Ensures that after this call, `item` has `detail_item_surrogate_id`.
        """
        db_item = {}
        db_item["quantity"] = item["quantity"]
        db_item["payment_type"] = item["payment_type"]
        db_item["description"] = item["description"]
        if "parent_pulse_id" in item:
            db_item["parent_pulse_id"] = item["parent_pulse_id"]

        db_item["detail_item_number"] = item["detail_item_id"]
        db_item["line_id"] = item["line_id"]  # <-- Added the new line_id field

        if "pulse_id" in item:
            db_item["pulse_id"] = item["pulse_id"]

        db_item["parent_surrogate_id"] = item["po_surrogate_id"]
        db_item["ot"] = item["OT"]
        db_item["fringes"] = item["fringes"]
        db_item["vendor"] = item["vendor"]
        db_item["state"] = "RTP" if item["parent_status"] == "RTP" else "PENDING"
        db_item["po_number"] = item["po_number"]
        db_item["project_id"] = item["project_id"]

        rate = item["rate"]
        # region 🧹 RATE CLEANER
        try:
            cleaned_rate = Decimal(str(rate).replace(',', '').strip())
            db_item["rate"] = float(cleaned_rate)
        except (ValueError, InvalidOperation) as e:
            self.logger.error(f"💥 Invalid rate value '{rate}': {e}", exc_info=True)
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
            self.logger.error(f"💥 Error parsing and formatting date '{date}': {e}", exc_info=True)
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
            self.logger.error(f"💥 Invalid account number value '{account_number}': {e}", exc_info=True)
            db_item["account_number"] = None
        # endregion

        with get_db_session() as session:
            try:
                detail_item = session.query(DetailItem).filter_by(
                    po_number=item["po_number"],
                    detail_item_number=item["detail_item_id"],
                    line_id=item["line_id"]
                ).one_or_none()

                if detail_item:
                    # Update existing record
                    for db_field, value in db_item.items():
                        if value is not None:
                            setattr(detail_item, db_field, value)
                    session.commit()
                    self.logger.debug(f"🔄 Updated existing DetailItem: {item['vendor']} (line_id: {item['line_id']})")
                else:
                    # Create a new record
                    new_detail_item = DetailItem(**db_item)
                    session.add(new_detail_item)
                    session.commit()
                    detail_item = new_detail_item
                    self.logger.info(
                        f"🎉 Created new DetailItem: {item['vendor']}, line_id: {item['line_id']}, surrogate_id: {detail_item.detail_item_surrogate_id}"
                    )

                # Retrieve and store the surrogate_id
                item["detail_item_surrogate_id"] = detail_item.detail_item_surrogate_id
                return item

            except IntegrityError as ie:
                session.rollback()
                self.logger.exception(f"💥 IntegrityError processing DetailItem in DB: {ie}", exc_info=True)
                return {
                    "status": "Fail",
                    "error": str(ie)
                }
            except Exception as e:
                session.rollback()
                self.logger.exception(f"💥 Error processing DetailItem in DB: {e}", exc_info=True)
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
            "name": item.get("contact_name"),
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
                    self.logger.info(f"👥 Existing Contact found in DB: {db_item['name']}")
                    for key, value in db_item.items():
                        if value is not None:
                            setattr(contact_query, key, value)
                    session.commit()
                    item["contact_surrogate_id"] = contact_query.contact_surrogate_id
                    item["contact_pulse_id"] = contact_query.pulse_id
                    return item
                else:
                    new_contact = Contact(**db_item)
                    session.add(new_contact)
                    session.commit()

                    created_contact = session.query(Contact).filter_by(name=db_item['name']).one_or_none()
                    if created_contact:
                        self.logger.info(f"🎉 Created new Contact: {db_item['name']}")
                        item["contact_surrogate_id"] = created_contact.contact_surrogate_id
                        item["contact_pulse_id"] = created_contact.pulse_id
                        return item
                    else:
                        self.logger.error("💥 Contact creation failed unexpectedly.")
                        return "Fail"

            except Exception as e:
                session.rollback()
                self.logger.error(f"💥 Error processing Contact in DB: {e}", exc_info=True)
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
                    self.logger.info(f"🔎 Found contact: {contact.name}")
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
                    self.logger.info(f"⚠️ No contact found with name: {name}")
                    return None
        except Exception as e:
            self.logger.error(f"💥 Database error in get_contact_by_name: {e}", exc_info=True)
            return None
    # endregion

    def get_subitems(self, project_id, po_number=None, detail_item_number=None, line_id=None):
        """
        📚 Retrieve subitems (DetailItems) from the database based on various filters.

        Parameters:
            project_id (str): The project ID to filter detail items.
            po_number (str, optional): The PO number to filter. If not provided, returns all subitems in the project.
            detail_item_number (int or str, optional): The detail item number to filter. If not provided, returns all matching subitems.
            line_id (int or str, optional): The line_id to filter. If provided along with all other parameters,
                                            returns only that specific subitem.

        Returns:
            list or dict:
                - If only project_id is provided: Returns a list of all subitems in that project.
                - If project_id and po_number are provided: Returns subitems filtered by that PO.
                - If project_id, po_number, and detail_item_number are provided: Returns subitems filtered to that detail_item_number.
                - If project_id, po_number, detail_item_number, and line_id are provided: Returns a single subitem dict or None if not found.
        """
        from sqlalchemy import and_
        try:
            with get_db_session() as session:
                # Base query: Join DetailItem with PurchaseOrder since we need project_id and po_number from PurchaseOrder
                query = session.query(DetailItem, PurchaseOrder).join(
                    PurchaseOrder, DetailItem.parent_surrogate_id == PurchaseOrder.po_surrogate_id
                ).filter(PurchaseOrder.project_id == project_id)

                if po_number:
                    query = query.filter(PurchaseOrder.po_number == po_number)
                if detail_item_number is not None:
                    # Ensure detail_item_number is compared as the correct type
                    # detail_item_number on model is likely an int or Decimal; adjust as needed
                    query = query.filter(DetailItem.detail_item_number == detail_item_number)
                if line_id is not None:
                    query = query.filter(DetailItem.line_id == line_id)

                results = query.all()

                # Convert results to a list of dicts
                subitems_list = []
                for detail_item, po in results:
                    subitems_list.append({
                        "detail_item_surrogate_id": detail_item.detail_item_surrogate_id,
                        "project_id": po.project_id,
                        "po_number": po.po_number,
                        "detail_item_number": detail_item.detail_item_number,
                        "line_id": detail_item.line_id,
                        "transaction_date": detail_item.transaction_date.strftime(
                            "%Y-%m-%d") if detail_item.transaction_date else None,
                        "due_date": detail_item.due_date.strftime("%Y-%m-%d") if detail_item.due_date else None,
                        "pulse_id": detail_item.pulse_id,
                        "rate": float(detail_item.rate) if detail_item.rate is not None else None,
                        "quantity": float(detail_item.quantity) if detail_item.quantity is not None else None,
                        "sub_total": float(detail_item.sub_total) if detail_item.sub_total is not None else None,
                        "payment_type": detail_item.payment_type,
                        "ot": float(detail_item.ot) if detail_item.ot is not None else None,
                        "fringes": float(detail_item.fringes) if detail_item.fringes is not None else None,
                        "vendor": detail_item.vendor,
                        "description": detail_item.description,
                        "file_link": detail_item.file_link,
                        "state": detail_item.state,
                        "account_number": detail_item.account_number
                    })

                # If we have all four parameters and expect one subitem, return a single dict or None
                if po_number and detail_item_number is not None and line_id is not None:
                    return subitems_list[0] if len(subitems_list) == 1 else None

                return subitems_list

        except Exception as e:
            self.logger.error(f"💥 Error retrieving subitems: {e}", exc_info=True)
            return None

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

                    po_dict['detail_items'] = detail_items
                    objects_as_dict.append(po_dict)

                return jsonify(objects_as_dict)

            except Exception as e:
                self.logger.error(f"💥 Error retrieving objects from view for project_id {project_id}: {e}", exc_info=True)
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

    # region ✨ LINK FUNCTIONS  ✨

    def get_po_with_details(self, po_surrogate_id):
        """
        🕵️‍♂️ get_po_with_details:
        Returns a dictionary with PO and detail item info:
        {
            "project_id": ...,
            "po_number": ...,
            "pulse_id": ...,
            "vendor_name": ...,
            "detail_items": [
                {
                    "detail_item_surrogate_id": ...,
                    "pulse_id": ...,
                    "detail_item_number": ...,
                },
                ...
            ]
        }
        """
        with get_db_session() as session:
            try:
                po = session.query(PurchaseOrder).filter_by(po_surrogate_id=po_surrogate_id).one_or_none()
                if not po:
                    self.logger.warning(f"⚠️ No purchase order found for po_surrogate_id={po_surrogate_id}")
                    return None

                # Retrieve vendor name from associated contact if available
                vendor_name = "Unknown Vendor"
                if po.contact_id:
                    contact = session.query(Contact).filter_by(contact_surrogate_id=po.contact_id).one_or_none()
                    if contact and contact.name:
                        vendor_name = contact.name

                # Build detail items list
                detail_items_list = []
                detail_items_from_db=session.query(DetailItem).filter_by(parent_surrogate_id=po.po_surrogate_id).all()
                for d in detail_items_from_db:
                    detail_items_list.append({
                        "detail_item_surrogate_id": d.detail_item_surrogate_id,
                        "pulse_id": d.pulse_id,
                        "detail_item_number": float(d.detail_item_number) if d.detail_item_number else None
                    })

                result = {
                    "project_id": po.project_id,
                    "po_number": po.po_number,
                    "pulse_id": po.pulse_id,
                    "vendor_name": vendor_name,
                    "detail_items": detail_items_list
                }

                self.logger.info(f"✅ Retrieved PO with details for po_surrogate_id={po_surrogate_id}")
                return result

            except Exception as e:
                self.logger.error(f"💥 Error in get_po_with_details: {e}", exc_info=True)
                return None

    def update_po_folder_link(self, project_id, po_number, folder_link):
        """
        🗄 Update the folder_link for the PO with the given po_surrogate_id.
        """
        with get_db_session() as session:
            try:
                po = session.query(PurchaseOrder).filter_by(project_id=project_id, po_number=po_number).one_or_none()
                if not po:
                    self.logger.warning(f"⚠️ No PO found for PO# {po_number}, cannot update folder_link.")
                    return False

                po.folder_link = folder_link
                session.commit()
                self.logger.info(f"🔗 Updated folder_link for PO {po_number} to {folder_link}")
                return True
            except Exception as e:
                session.rollback()
                self.logger.error(f"💥 Error updating PO folder_link: {e}", exc_info=True)
                return False

    def update_po_tax_form_link(self, po_surrogate_id, tax_form_link):
        """
        🗃 Update the tax_form_link for the PO with the given po_surrogate_id.
        """
        with get_db_session() as session:
            try:
                po = session.query(PurchaseOrder).filter_by(po_surrogate_id=po_surrogate_id).one_or_none()
                if not po:
                    self.logger.warning(f"⚠️ No PO found for po_surrogate_id={po_surrogate_id}, cannot update tax_form_link.")
                    return False

                po.tax_form_link = tax_form_link
                session.commit()
                self.logger.info(f"🔗 Updated tax_form_link for PO {po_surrogate_id} to {tax_form_link}")
                return True
            except Exception as e:
                session.rollback()
                self.logger.error(f"💥 Error updating PO tax_form_link: {e}", exc_info=True)
                return False

    def update_detail_item_file_link(self, detail_item_surrogate_id, file_link):
        """
        🗂 Update the file_link for the detail item identified by detail_item_surrogate_id.
        """
        with get_db_session() as session:
            try:
                detail_item = session.query(DetailItem).filter_by(detail_item_surrogate_id=detail_item_surrogate_id).one_or_none()
                if not detail_item:
                    self.logger.warning(f"⚠️ No DetailItem found for detail_item_surrogate_id={detail_item_surrogate_id}")
                    return False

                detail_item.file_link = file_link
                session.commit()
                self.logger.info(f"🔗 Updated file_link for DetailItem {detail_item_surrogate_id} to {file_link}")
                return True
            except Exception as e:
                session.rollback()
                self.logger.error(f"💥 Error updating DetailItem file_link: {e}", exc_info=True)
                return False

    def get_project_folder_name(self, project_id):
        """
        🌐 Given a project_id, return the project folder name, e.g. "2416 - Whop Keynote"
        """
        with get_db_session() as session:
            try:
                project = session.query(Project).filter_by(project_id=project_id).one_or_none()
                if project:
                    folder_name = f"{project_id} - {project.name}"
                    self.logger.info(f"✅ Retrieved project folder name for project_id={project_id}: {folder_name}")
                    return folder_name
                else:
                    self.logger.warning(f"⚠️ No Project found for project_id={project_id}")
                    return None
            except Exception as e:
                self.logger.error(f"💥 Error fetching project folder name for project_id={project_id}: {e}", exc_info=True)
                return None

    # endregion

    # region ✨ NEW PULSE ID UPDATE METHODS ✨
    def update_main_item_pulse_id(self, project_id: str, po_number: str, pulse_id: int):
        """
        🔧 Update the main item's pulse_id in the database by searching with project_id and po_number.

        Args:
            project_id (str): The project ID.
            po_number (str): The PO number.
            pulse_id (int): The Monday.com pulse_id to store.

        Returns:
            bool: True if updated successfully, False otherwise.
        """
        try:
            with get_db_session() as session:
                po = session.query(PurchaseOrder).filter_by(project_id=project_id, po_number=po_number).one_or_none()
                if not po:
                    self.logger.warning(
                        f"⚠️ No PurchaseOrder found for project_id={project_id}, po_number={po_number}. Cannot update pulse_id."
                    )
                    return False
                po.pulse_id = pulse_id
                session.commit()
                self.logger.info(
                    f"✅ Updated main item pulse_id for project_id={project_id}, po_number={po_number} to {pulse_id}"
                )
                return True
        except Exception as e:
            self.logger.error(f"💥 Error updating main item pulse_id: {e}", exc_info=True)
            return False

    def update_detail_item_pulse_ids(self, project_id: str, po_number: str, detail_item_number: str, line_id: str,
                                     pulse_id: int, parent_pulse_id: int):
        """
        🔧 Update the detail (sub) item's pulse_id and parent_pulse_id in the database.

        Args:
            project_id (str): The project ID.
            po_number (str): The PO number.
            detail_item_number (str): The detail item's number identifier.
            line_id (str): The line_id of the detail item.
            pulse_id (int): The Monday.com pulse_id of the sub-item.
            parent_pulse_id (int): The Monday.com pulse_id of the parent item.

        Returns:
            bool: True if updated successfully, False otherwise.
        """
        try:
            with get_db_session() as session:
                detail_item = session.query(DetailItem).join(PurchaseOrder,
                                                             PurchaseOrder.po_surrogate_id == DetailItem.parent_surrogate_id).filter(
                    PurchaseOrder.project_id == project_id,
                    PurchaseOrder.po_number == po_number,
                    DetailItem.detail_item_number == detail_item_number,
                    DetailItem.line_id == line_id
                ).one_or_none()

                if not detail_item:
                    self.logger.warning(
                        f"⚠️ No DetailItem found for project_id={project_id}, po_number={po_number}, detail_item_number={detail_item_number}, line_id={line_id}"
                    )
                    return False

                detail_item.pulse_id = pulse_id
                detail_item.parent_pulse_id = parent_pulse_id
                session.commit()
                self.logger.info(
                    f"✅ Updated detail item pulse_id for project_id={project_id}, po_number={po_number}, detail_item_number={detail_item_number}, line_id={line_id} to pulse_id={pulse_id}, parent_pulse_id={parent_pulse_id}"
                )
                return True
        except Exception as e:
            self.logger.error(f"💥 Error updating detail item pulse_ids: {e}", exc_info=True)
            return False
    # endregion

    def get_all_processed_items(self):
        """
        🔍 Retrieves all purchase orders from the database that have a pulse_id
        (indicating they've been matched with Monday items) but have no folder_link.
        This assumes that having a pulse_id means they've been "processed" enough
        to appear on Monday and are ready for Dropbox link updates.

        Returns:
            list of dict: A list of dictionaries containing basic PO info needed
                          for Dropbox link retrieval and Monday updates.
        """
        try:
            with get_db_session() as session:
                # Fetch all POs that have pulse_id assigned but no folder_link yet
                # Adjust the filters as needed to fit the definition of "processed"
                pos = session.query(PurchaseOrder).filter(
                    PurchaseOrder.pulse_id.isnot(None),
                    (PurchaseOrder.folder_link == None) | (PurchaseOrder.folder_link == "")
                ).all()

                processed_items = []
                for po in pos:
                    processed_items.append({
                        "po_surrogate_id": po.po_surrogate_id,
                        "project_id": po.project_id,
                        "PO": po.po_number,
                        "pulse_id": po.pulse_id
                    })

                self.logger.info(f"✅ Retrieved {len(processed_items)} processed items that need Dropbox links.")
                return processed_items

        except Exception as e:
            self.logger.error(f"💥 Error retrieving processed items: {e}", exc_info=True)
            return []

    def get_purchase_orders(self, project_id=None, po_number=None, detail_item_number=None, line_id=None):
        """
        Retrieve Purchase Orders from the database with optional filters and include their associated detail items.

        Filtering logic:
        - If no parameters are provided, return all POs.
        - If project_id is provided, filter POs by project_id.
        - If po_number is provided, filter POs by po_number.
        - If detail_item_number is provided, return only POs having detail items matching that detail_item_number.
        - If line_id is provided, return only POs having detail items matching that line_id.

        Returns:
            list: A list of dictionaries, each representing a PurchaseOrder and its associated detail items.
        """
        from sqlalchemy import and_

        with get_db_session() as session:
            # Start building the PO query
            query = session.query(PurchaseOrder)

            # If we need to filter by detail_item_number or line_id, we must join the DetailItem table
            detail_filtering = (detail_item_number is not None or line_id is not None)
            if detail_filtering:
                query = query.join(DetailItem, PurchaseOrder.po_surrogate_id == DetailItem.parent_surrogate_id)

            # Apply filters to the PO query
            if project_id is not None:
                query = query.filter(PurchaseOrder.project_id == project_id)
            if po_number is not None:
                query = query.filter(PurchaseOrder.po_number == po_number)
            if detail_item_number is not None:
                query = query.filter(DetailItem.detail_item_number == detail_item_number)
            if line_id is not None:
                query = query.filter(DetailItem.line_id == line_id)

            # Execute the query to get POs
            purchase_orders = query.all()

            # If no POs found, return empty
            if not purchase_orders:
                return []

            # Extract surrogate IDs of the found POs
            po_surrogate_ids = [po.po_surrogate_id for po in purchase_orders]

            # Now we get the associated detail items
            detail_query = session.query(DetailItem).filter(DetailItem.parent_surrogate_id.in_(po_surrogate_ids))
            # If we had filtering by detail item number or line_id, apply it here too (redundant but ensures correctness)
            if detail_item_number is not None:
                detail_query = detail_query.filter(DetailItem.detail_item_number == detail_item_number)
            if line_id is not None:
                detail_query = detail_query.filter(DetailItem.line_id == line_id)

            detail_items = detail_query.all()

            # Group detail items by their parent_surrogate_id (PO)
            detail_map = {}
            for d in detail_items:
                detail_map.setdefault(d.parent_surrogate_id, []).append({
                    "detail_item_surrogate_id": d.detail_item_surrogate_id,
                    "detail_item_number": d.detail_item_number,
                    "line_id": d.line_id,
                    "description": d.description,
                    "transaction_date": d.transaction_date.isoformat() if d.transaction_date else None,
                    "due_date": d.due_date.isoformat() if d.due_date else None,
                    "quantity": float(d.quantity) if d.quantity is not None else None,
                    "rate": float(d.rate) if d.rate is not None else None,
                    "ot": float(d.ot) if d.ot is not None else None,
                    "fringes": float(d.fringes) if d.fringes is not None else None,
                    "vendor": d.vendor,
                    "state": d.state,
                    "account_number": d.account_number,
                    "payment_type": d.payment_type,
                    "pulse_id": d.pulse_id,
                    "file_link": d.file_link,
                })

            # Build the final result set, embedding detail items into their respective PO
            results = []
            for po in purchase_orders:
                results.append({
                    "po_surrogate_id": po.po_surrogate_id,
                    "project_id": po.project_id,
                    "po_number": po.po_number,
                    "contact_id": po.contact_id,
                    "pulse_id": po.pulse_id,
                    "state": po.state,
                    "description": po.description,
                    "po_type": po.po_type,
                    "folder_link": po.folder_link,
                    "tax_form_link": po.tax_form_link,
                    "subitems": detail_map.get(po.po_surrogate_id, [])
                })

            return results

# endregion

# 🎉 Instantiate the PoLogDatabaseUtil Singleton
po_log_database_util = PoLogDatabaseUtil()