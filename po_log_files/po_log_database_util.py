# po_log_database_util.py
# -*- coding: utf-8 -*-
"""
📚✨ PO Log Database Utility
===========================
Provides database operations for PO logs, contacts, items, etc., but now uses
the unified `DatabaseOperations` class from `database_util.py`.
"""

#region Imports
import logging
import re
from decimal import Decimal, InvalidOperation
from dateutil.parser import parser
from sqlalchemy.exc import IntegrityError
from datetime import datetime, timedelta

# Our new DB Ops
from database.database_util import DatabaseOperations

# If you still need direct references to your session or to models, you can import them:
from database.db_util import get_db_session
from database.models import Contact, PurchaseOrder, DetailItem, Project
from utilities.singleton import SingletonMeta
#endregion


class PoLogDatabaseUtil(metaclass=SingletonMeta):
    """
    📚 This utility class now leverages DatabaseOperations to handle most
    create/update/retrieve logic. It focuses on any specialized logic or
    preprocessing specific to PO logs.
    """

    #region Initialization
    def __init__(self):
        if not hasattr(self, '_initialized'):
            self.logger = logging.getLogger("app_logger")
            self.logger.info("PO Log Database Util initialized")

            # Instantiate our new DatabaseOperations
            self.db_ops = DatabaseOperations()

            self._initialized = True
    #endregion

    #region Preprocessing & Utility
    def get_contact_surrogate_ids(self, contacts_list):
        """
        🗂 Example usage showing searching the Contact model with DatabaseOperations.
        """
        new_contact_list = []
        for contact in contacts_list:
            try:
                # Instead of manual queries, we can do:
                found = self.db_ops.search_contacts(["name"], [contact.get("name")])
                if found:
                    self.logger.debug(f"🤝 Found in database: {contact.get('name')}")
                    if isinstance(found, list):
                        found = found[0]
                    new_contact_list.append({
                        "name": contact.get("name"),
                        "po_number": contact.get("po_number"),
                        "contact_surrogate_id": "Unknown (You could store or map ID here)"
                    })
                else:
                    self.logger.debug(f"🙅 Not in database: {contact.get('name')}")
            except Exception as e:
                self.logger.error(f"💥 Error processing contact '{contact.get('name', 'Unknown')}': {e}", exc_info=True)

        return new_contact_list

    def link_contact_to_po(self, contacts, project_id):
        """
        🔗 Example method to show linking logic. This might now be replaced by
        direct usage of create/update from DatabaseOperations if you prefer.
        """
        self.logger.info("🔗 link_contact_to_po is not fully migrated; use db_ops if needed.")
    #endregion


    #region DB Create/Update Wrappers
    def find_or_create_contact_item_in_db(self, item):
        """
        🏗 Example wrapper around db_ops for contact creation.
        """
        # We'll do a search by name or tax_id, etc.
        name = item.get("contact_name")
        tax_id = item.get("tax_id")

        found = None
        if tax_id:
            found = self.db_ops.search_contacts(["tax_ID"], [tax_id])
        if not found and name:
            found = self.db_ops.search_contacts(["name"], [name])

        if found and not isinstance(found, list):
            # Single record
            contact_id = found.get("id")  # If your DB model has an 'id' PK
            # Update if needed:
            updated = self.db_ops.update_contact(contact_id, **{
                "payment_details": item.get("contact_payment_details", "PENDING"),
                "email": item.get("contact_email"),
                "phone": item.get("contact_phone"),
                "address_line_1": item.get("address_line_1"),
                "city": item.get("city"),
                "zip": item.get("zip"),
                "tax_ID": item.get("tax_id"),
                "tax_form_link": item.get("tax_form_link"),
                "vendor_status": item.get("contact_status", "PENDING"),
                "country": item.get("contact_country"),
                "tax_type": item.get("contact_tax_type", "SSN"),
                "pulse_id": item.get("contact_pulse_id")
            })
            if updated:
                item["contact_surrogate_id"] = updated["id"]
                item["contact_pulse_id"] = updated["pulse_id"]
            return item
        elif found and isinstance(found, list) and len(found) > 0:
            # Take first record
            first = found[0]
            contact_id = first.get("id")
            updated = self.db_ops.update_contact(contact_id, **{
                "payment_details": item.get("contact_payment_details", "PENDING"),
                # ... and so forth
            })
            if updated:
                item["contact_surrogate_id"] = updated["id"]
                item["contact_pulse_id"] = updated["pulse_id"]
            return item
        else:
            # Create new contact
            created = self.db_ops.create_contact(
                name=name,
                payment_details=item.get("contact_payment_details", "PENDING"),
                email=item.get("contact_email"),
                phone=item.get("contact_phone"),
                address_line_1=item.get("address_line_1"),
                city=item.get("city"),
                zip=item.get("zip"),
                tax_ID=item.get("tax_id"),
                tax_form_link=item.get("tax_form_link"),
                vendor_status=item.get("contact_status", "PENDING"),
                country=item.get("contact_country"),
                tax_type=item.get("contact_tax_type", "SSN"),
                pulse_id=item.get("contact_pulse_id")
            )
            if created:
                item["contact_surrogate_id"] = created["id"]
                item["contact_pulse_id"] = created["pulse_id"]
            return item

    def create_or_update_main_item_in_db(self, item):
        """
        🏗 Create or update a PurchaseOrder.
        """
        # Search to see if it already exists:
        existing_po = self.db_ops.search_purchase_orders(
            ["project_id", "po_number"],
            [item["project_id"], item["po_number"]]
        )
        if existing_po:
            # If it's a list, get the first
            if isinstance(existing_po, list):
                existing_po = existing_po[0]
            po_id = existing_po["id"]  # The PK in your DB (assuming 'id')
            updated = self.db_ops.update_purchase_order(po_id, **{
                "contact_id": item.get("contact_surrogate_id"),
                "description": item.get("description"),
                "pulse_id": item.get("pulse_id"),
                "po_type": item.get("po_type"),
                "state": item.get("status"),
                "folder_link": item.get("folder_link"),
                "tax_form_link": item.get("tax_form_link")
            })
            if updated:
                item["po_surrogate_id"] = updated["id"]
        else:
            # Create a new PO
            created = self.db_ops.create_purchase_order(
                project_id=item["project_id"],
                po_number=item["po_number"],
                contact_id=item.get("contact_surrogate_id"),
                description=item.get("description"),
                pulse_id=item.get("pulse_id"),
                po_type=item.get("po_type"),
                state=item.get("status"),
                folder_link=item.get("folder_link"),
                tax_form_link=item.get("tax_form_link")
            )
            if created:
                item["po_surrogate_id"] = created["id"]
        return item

    def create_or_update_sub_item_in_db(self, sub_item):
        """
        🏗 Create or update a DetailItem.
        """
        # Search
        existing_detail = self.db_ops.search_detail_items(
            ["project_id", "po_number", "detail_item_number", "line_id"],
            [
                sub_item["project_id"],
                sub_item["po_number"],
                sub_item["detail_item_id"],
                sub_item["line_id"]
            ]
        )
        if existing_detail:
            if isinstance(existing_detail, list):
                existing_detail = existing_detail[0]
            detail_id = existing_detail["id"]
            self.db_ops.update_detail_item(detail_id, **{
                "parent_surrogate_id": sub_item.get("po_surrogate_id"),
                "vendor": sub_item.get("vendor"),
                "payment_type": sub_item.get("payment_type"),
                "description": sub_item.get("description"),
                "pulse_id": sub_item.get("pulse_id"),
                "parent_pulse_id": sub_item.get("parent_pulse_id"),
                "state": "RTP" if sub_item.get("parent_status") == "RTP" else "PENDING",
                "rate": sub_item.get("rate"),
                "quantity": sub_item.get("quantity"),
                "ot": sub_item.get("OT"),
                "fringes": sub_item.get("fringes"),
                "transaction_date": sub_item.get("date"),
                "due_date": None,  # or compute it
                "account_number": sub_item.get("account")
            })
        else:
            self.db_ops.create_detail_item(
                project_id=sub_item["project_id"],
                po_number=sub_item["po_number"],
                detail_item_number=sub_item["detail_item_id"],
                line_id=sub_item["line_id"],
                parent_surrogate_id=sub_item.get("po_surrogate_id"),
                vendor=sub_item.get("vendor"),
                payment_type=sub_item.get("payment_type"),
                description=sub_item.get("description"),
                pulse_id=sub_item.get("pulse_id"),
                parent_pulse_id=sub_item.get("parent_pulse_id"),
                state="RTP" if sub_item.get("parent_status") == "RTP" else "PENDING",
                rate=sub_item.get("rate"),
                quantity=sub_item.get("quantity"),
                ot=sub_item.get("OT"),
                fringes=sub_item.get("fringes"),
                transaction_date=sub_item.get("date"),
                due_date=None,  # or compute it
                account_number=sub_item.get("account")
            )
        return sub_item
    #endregion


    #region Retrieval Wrappers
    def get_contact_by_name(self, name: str):
        """
        🗂 Simplified retrieval via db_ops.search_contacts
        """
        results = self.db_ops.search_contacts(["name"], [name])
        if not results:
            self.logger.info(f"⚠️ No contact found with name: {name}")
            return None
        if isinstance(results, list):
            return results[0]
        return results

    def get_subitems(self, project_id, po_number=None, detail_item_number=None, line_id=None):
        """
        📚 Example retrieval using db_ops.search_detail_items with possible filters.
        """
        # Build dynamic filters
        column_names = []
        values = []
        if project_id is not None:
            column_names.append("project_id")
            values.append(project_id)
        if po_number is not None:
            column_names.append("po_number")
            values.append(po_number)
        if detail_item_number is not None:
            column_names.append("detail_item_number")
            values.append(detail_item_number)
        if line_id is not None:
            column_names.append("line_id")
            values.append(line_id)

        # Use the search
        subitems = self.db_ops.search_detail_items(column_names, values)
        if not subitems:
            return []
        if isinstance(subitems, dict):
            return [subitems]
        return subitems

    def get_purchase_orders(self, project_id=None, po_number=None, po_type=None):
        """
        Retrieve POs with optional filters. For example, pass in po_type="INV".
        """
        column_names = []
        values = []
        if project_id is not None:
            column_names.append("project_id")
            values.append(project_id)
        if po_number is not None:
            column_names.append("po_number")
            values.append(po_number)
        if po_type is not None:
            column_names.append("po_type")
            values.append(po_type)

        results = self.db_ops.search_purchase_orders(column_names, values)
        if not results:
            return []
        if isinstance(results, dict):
            return [results]
        return results

    def update_po_folder_link(self, project_id, po_number, folder_link):
        """
        🗄 Example: find the PO and update the folder_link
        """
        pos = self.db_ops.search_purchase_orders(["project_id", "po_number"], [project_id, po_number])
        if not pos:
            self.logger.warning(f"⚠️ No PO found for {project_id}_{po_number}, cannot update folder_link.")
            return False

        if isinstance(pos, list):
            # Take the first result
            pos = pos[0]
        updated = self.db_ops.update_purchase_order(pos["id"], folder_link=folder_link)
        return True if updated else False
    #endregion


# 🎉 Singleton instance
po_log_database_util = PoLogDatabaseUtil()