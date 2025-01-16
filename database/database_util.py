# -*- coding: utf-8 -*-
"""
database/database_util.py

💻 Database Operations Module
=============================
This module provides flexible, DRY (Don't Repeat Yourself) functions for searching,
creating, and updating records in various database tables, using SQLAlchemy ORM
and a common session pattern.

Additional concurrency changes:
- We now catch IntegrityError when creating a record and attempt a fallback re-query
  if a 'unique_lookup' dict is provided or if you want to handle a known unique column.
"""

# region 🔗 Imports
from contextlib import contextmanager
from typing import Optional, Dict, Any, List, Union
import logging

from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from models import (
    Contact,
    Project,
    PurchaseOrder,
    DetailItem,
    BankTransaction,
    BillLineItem,
    Invoice,
    AccountCode,
    Receipt,
    SpendMoney,
    TaxAccount,
    XeroBill
)
from database.db_util import get_db_session
# endregion

# region 🏢 Class Definition
class DatabaseOperations:
    """
    🗂 Database Operations Class
    ============================
    Provides flexible, DRY methods to search, create, and update records
    in your new schema, using project_number-based lookups rather than project_id.
    Also handles 'account_code_id' logic in detail_item and avoids DetachedInstanceError
    by performing all relevant operations in the same session scope.

    Concurrency-safe creation logic included:
    - If we encounter an IntegrityError (e.g., duplicate key),
      we re-query to see if another thread/process just created the same record
      (when a 'unique_lookup' dict is provided).
    """

    # region 🏗 Constructor
    def __init__(self):
        self.logger = logging.getLogger("db_logger")
        self.logger.debug("🌟 Hello from DatabaseOperations constructor! Ready to keep the DB in check!")
    # endregion

    # region 📝 Helper Method: Serialize Record
    def _serialize_record(self, record):
        """
        🗄 Serialize a record (SQLAlchemy model) into a dictionary of column names -> values.
        Returns None if the record is None.
        """
        if not record:
            return None
        # 🚀 Convert SQLAlchemy object to a dict of {column_name: value}
        record_values = {c.name: getattr(record, c.name) for c in record.__table__.columns}
        self.logger.debug(f"🤓 Pulling record: {record_values['id']} from table {record.__table__}")
        return record_values
    # endregion

    # region 🔍 Generic Search Method
    def _search_records(
            self,
            model,
            column_names: Optional[List[str]] = None,
            values: Optional[List[Any]] = None
    ) -> Union[None, Dict[str, Any], List[Dict[str, Any]]]:
        """
        🔎 Search for records of a given model based on multiple column filters.
        If no column_names or values are provided, retrieves all records from the table.

        Returns:
            - None if no records
            - A single dict if exactly one found
            - A list if multiple found
            - [] if mismatch in columns/values or on error

        This is an *atomic* operation: we open a fresh session, do the query,
        commit/rollback, and close the session each time.
        """
        column_names = column_names or []
        values = values or []

        # Basic validations
        if column_names and values:
            self.logger.debug(f"🕵️ Searching {model.__name__} with filters: {list(zip(column_names, values))}")
            self.logger.info(
                f"🚦 Checking if there are any matches in {model.__name__} for columns & values: {list(zip(column_names, values))}"
            )

            if len(column_names) != len(values):
                self.logger.warning(
                    "⚠️ Oops, mismatch: The number of column names and values do not match. Returning empty list."
                )
                return []
        else:
            self.logger.debug(f"🕵️ No filters provided. Retrieving all records from {model.__name__}.")
            self.logger.info(f"🚦 Fetching the entire {model.__name__} table without any filters.")

        with get_db_session() as session:
            try:
                query = session.query(model)
                if column_names and values:
                    for col_name, val in zip(column_names, values):
                        column_attr = getattr(model, col_name, None)
                        if column_attr is None:
                            self.logger.warning(
                                f"😬 '{col_name}' is not a valid column in {model.__name__}. Returning empty list."
                            )
                            return []
                        query = query.filter(column_attr == val)

                records = query.all()
                if not records:
                    self.logger.info("🙅 No records found in the DB for these filters. Maybe next time!")
                    return None
                elif len(records) == 1:
                    self.logger.info("✅ Found exactly ONE record. Bingo!")
                    return self._serialize_record(records[0])
                else:
                    self.logger.info(f"✅ Located {len(records)} records! Bundling them all up.")
                    return [self._serialize_record(r) for r in records]

            except Exception as e:
                session.rollback()
                self.logger.error(f"💥 Error searching {model.__name__}: {e}", exc_info=True)
                return []
    # endregion

    # region 🆕 Concurrency-Safe Create
    def _create_record(
            self,
            model,
            unique_lookup: dict = None,
            **kwargs
    ) -> Optional[Dict[str, Any]]:
        """
        🆕 Create a new record in the database, returning its serialized form or None on error.
        Includes session.flush() before session.commit() to ensure ID is generated & data is visible.

        Concurrency-Safe Logic:
          - If an IntegrityError occurs (e.g. duplicate key), we do a fallback re-query
            if 'unique_lookup' is provided.
          - If found, return the existing record. Otherwise, raise or return None.
        """
        self.logger.debug(f"🧑‍💻 Creating new {model.__name__} using data: {kwargs}")
        self.logger.info(f"🌱 About to insert a fresh record into {model.__name__} with {kwargs}")

        with get_db_session() as session:
            try:
                record = model(**kwargs)
                session.add(record)

                # Flush so the DB assigns PK, catches any constraints before commit
                session.flush()
                self.logger.debug(f"🪄 Flushed new {model.__name__}. ID now: {record.id}")

                session.commit()
                self.logger.info("🎉 Creation successful! Record is now in the DB.")
                return self._serialize_record(record)

            except IntegrityError:
                # Possibly a concurrency conflict or unique constraint violation
                self.logger.debug(f"❗ IntegrityError creating {model.__name__}")
                session.rollback()  # revert this session

                # Attempt fallback re-query if we know how
                if unique_lookup:
                    self.logger.warning("Attempting concurrency fallback re-query using 'unique_lookup'...")
                    found = self._search_records(
                        model,
                        list(unique_lookup.keys()),
                        list(unique_lookup.values())
                    )
                    if found:
                        if isinstance(found, list):
                            self.logger.info(
                                f"Found {len(found)} record(s) matching unique_lookup; returning the first."
                            )
                            return found[0]
                        else:
                            self.logger.info("Found exactly one existing record after concurrency fallback.")
                            return found
                    else:
                        self.logger.error(
                            f"No record found after concurrency fallback for {model.__name__} with {unique_lookup}"
                        )
                        return None
                else:
                    self.logger.error(
                        "No 'unique_lookup' provided, cannot re-query for concurrency fallback. Returning None."
                    )
                return None

            except Exception as e:
                session.rollback()
                self.logger.error(f"💥 Trouble creating {model.__name__}: {e}", exc_info=True)
                return None
    # endregion

    # region 🔄 Concurrency-Safe Update
    def _update_record(
            self,
            model,
            record_id: int,
            unique_lookup: dict = None,
            **kwargs
    ) -> Optional[Dict[str, Any]]:
        """
        🔄 Update an existing record by its primary key (ID).
        Returns the serialized updated record, or None if not found or on error.
        Includes session.flush() before session.commit() so changes are visible.

        Concurrency-Safe Logic:
          - If an IntegrityError occurs (duplicate key on some column), we can attempt
            a fallback re-query if 'unique_lookup' is provided.

        :param model: The SQLAlchemy model
        :param record_id: The primary key ID for the record to update
        :param unique_lookup: optional concurrency fallback
        :param kwargs: fields to update
        :return: updated record (serialized) or None
        """
        self.logger.debug(f"🔧 Attempting to update {model.__name__}(id={record_id}). Fields: {kwargs}")
        self.logger.info(f"🤝 Checking if {model.__name__}(id={record_id}) exists, then updating with {kwargs}.")

        with get_db_session() as session:
            try:
                # 🏷 Using session.query to find our target record
                record = session.query(model).get(record_id)
                if not record:
                    self.logger.info(f"🙅 No {model.__name__} with id={record_id} found.")
                    return None

                # 🚀 Apply updates only for valid model attributes
                for key, value in kwargs.items():
                    if hasattr(record, key):
                        setattr(record, key, value)
                    else:
                        self.logger.warning(
                            f"⚠️ The attribute '{key}' doesn't exist on {model.__name__}. Skipping."
                        )

                session.flush()
                self.logger.debug(f"🪄 Flushed updated {model.__name__}(id={record_id}).")

                session.commit()
                self.logger.info("✅ Done updating! The record is all set.")
                return self._serialize_record(record)

            except IntegrityError:
                self.logger.warning(f"❗ IntegrityError updating {model.__name__}(id={record_id})")
                session.rollback()

                if unique_lookup:
                    self.logger.warning("Attempting concurrency fallback re-query with 'unique_lookup' after update...")
                    found = self._search_records(
                        model,
                        list(unique_lookup.keys()),
                        list(unique_lookup.values())
                    )
                    if found:
                        if isinstance(found, list):
                            self.logger.info(
                                f"Found {len(found)} record(s) matching unique_lookup; returning the first."
                            )
                            return found[0]
                        else:
                            self.logger.info("Found exactly one existing record after concurrency fallback.")
                            return found
                    else:
                        self.logger.error(
                            f"No record found after concurrency fallback for {model.__name__} with {unique_lookup}"
                        )
                        return None
                else:
                    self.logger.error("No 'unique_lookup' provided for fallback; returning None.")
                return None

            except Exception as e:
                session.rollback()
                self.logger.error(f"💥 Had an issue updating {model.__name__}: {e}", exc_info=True)
                return None
    # endregion

    # region 🧩 AccountCodes
    def search_account_codes(self, column_names, values):
        self.logger.debug("🔎 Searching for Account Code  entries...")
        return self._search_records(AccountCode, column_names, values)

    def create_account_code(self, **kwargs):
        """
        If 'account_code' is unique, pass unique_lookup={'account_code': kwargs.get('account_code')}
        """
        self.logger.debug(f"🌈 Creating an AccountCode with data={kwargs}")
        unique_lookup = {}
        if 'account_code' in kwargs:
            unique_lookup['code'] = kwargs['code']
        return self._create_record(AccountCode, unique_lookup=unique_lookup, **kwargs)
    # endregion

    # region 📝 PurchaseOrder
    def search_purchase_orders(self, column_names, values):
        self.logger.debug(f"📝 search_purchase_orders: columns={column_names}, values={values}")
        return self._search_records(PurchaseOrder, column_names, values)

    def create_purchase_order(self, **kwargs):
        """
        If you have a unique constraint like (project_id, po_number), define:
          unique_lookup = {'project_id': ..., 'po_number': ...}
        """
        self.logger.debug(f"📝 create_purchase_order with {kwargs}")
        unique_lookup = {}
        if 'project_id' in kwargs and 'po_number' in kwargs:
            unique_lookup = {
                'project_id': kwargs['project_id'],
                'po_number': kwargs['po_number']
            }
        return self._create_record(PurchaseOrder, unique_lookup=unique_lookup, **kwargs)

    def update_purchase_order(self, po_id, **kwargs):
        """
        If there's a unique constraint (project_id, po_number) that can be updated, pass a unique_lookup.
        Otherwise normal update.
        """
        self.logger.debug(f"📝 update_purchase_order -> PurchaseOrder(id={po_id}), data={kwargs}")
        return self._update_record(PurchaseOrder, po_id, **kwargs)

    # region 🔎 PurchaseOrder by Keys
    def search_purchase_order_by_keys(
        self,
        project_number: Optional[int] = None,
        po_number: Optional[int] = None
    ) -> Union[None, Dict[str, Any], List[Dict[str, Any]]]:
        """
        Return PurchaseOrder(s) based on optional project_number & po_number.
          - If neither is provided, returns ALL PurchaseOrders.
          - If only project_number is given, returns all POs for that project_number.
          - If project_number + po_number are given, returns just that subset.
        """
        # If nothing provided, search all
        if not project_number and not po_number:
            return self.search_purchase_orders([], [])

        col_filters = []
        val_filters = []

        if project_number is not None:
            col_filters.append("project_number")
            val_filters.append(project_number)

        if po_number is not None:
            col_filters.append("po_number")
            val_filters.append(po_number)

        return self.search_purchase_orders(col_filters, val_filters)

    def create_purchase_order_by_keys(self, project_number: int, po_number: int, **kwargs):
        """
        Shortcut to create a PurchaseOrder by directly specifying project_number & po_number.
        We still need the project_id though, so consider how you want to handle that.
        """
        # If you want to look up a project row by project_number, you can do so here:
        project_record = self.search_projects(["project_number"], [project_number])
        if not project_record:
            self.logger.warning(
                f"Cannot create PurchaseOrder because Project with project_number={project_number} not found."
            )
            return None
        if isinstance(project_record, list):
            project_record = project_record[0]

        project_id = project_record["id"]
        kwargs.update({"project_id": project_id, "project_number": project_number, "po_number": po_number})
        return self.create_purchase_order(**kwargs)

    def update_purchase_order_by_keys(self, project_number: int, po_number: int, **kwargs):
        """
        Shortcut to update a PurchaseOrder, given project_number + po_number.
        If multiple POs match, we only update the first one.
        """
        pos = self.search_purchase_order_by_keys(project_number, po_number)
        if not pos:
            return None
        if isinstance(pos, list):
            first_po = pos[0]
        else:
            first_po = pos

        return self.update_purchase_order(first_po["id"], **kwargs)
    # endregion

    # endregion

    # region 🔎 DetailItem
    def search_detail_items(self, column_names, values):
        self.logger.debug(f"🔎 search_detail_items: columns={column_names}, values={values}")
        return self._search_records(DetailItem, column_names, values)

    def create_detail_item(self, **kwargs):
        self.logger.debug(f"🧱 Creating a detail item with {kwargs}")
        # If (po_id, detail_number, line_number) is unique, define unique_lookup
        unique_lookup = {}
        if 'po_number' in kwargs and 'project_number' in kwargs and 'detail_number' in kwargs and 'line_number' in kwargs:
            unique_lookup = {
                'project_number': kwargs['project_number'],
                'po_number': kwargs['po_number'],
                'detail_number': kwargs['detail_number'],
                'line_number': kwargs['line_number']
            }
        return self._create_record(DetailItem, unique_lookup=unique_lookup, **kwargs)

    def update_detail_item(self, detail_item_id, **kwargs):
        self.logger.debug(f"🔧 update_detail_item -> DetailItem(id={detail_item_id}), data={kwargs}")
        return self._update_record(DetailItem, detail_item_id, **kwargs)

    # region 🔎 DetailItem by Keys
    def search_detail_item_by_keys(
        self,
        project_number: Optional[int] = None,
        po_number: Optional[int] = None,
        detail_number: Optional[int] = None,
        line_number: Optional[int] = None
    ) -> Union[None, Dict[str, Any], List[Dict[str, Any]]]:
        """
        Returns DetailItem(s) based on any subset of the four optional keys.
          - If none provided, returns ALL DetailItems.
          - If only project_number, returns all items for that project (across all POs).
          - If project_number + po_number, returns all items for that PO, etc.
        """
        if not project_number and not po_number and not detail_number and not line_number:
            return self.search_detail_items([], [])

        col_filters = []
        val_filters = []

        if project_number is not None:
            col_filters.append("project_number")
            val_filters.append(project_number)
        if po_number is not None:
            col_filters.append("po_number")
            val_filters.append(po_number)
        if detail_number is not None:
            col_filters.append("detail_number")
            val_filters.append(detail_number)
        if line_number is not None:
            col_filters.append("line_number")
            val_filters.append(line_number)

        return self.search_detail_items(col_filters, val_filters)

    def create_detail_item_by_keys(
        self,
        project_number: int,
        po_number: int,
        detail_number: int,
        line_number: int,
        **kwargs
    ):
        """
        Shortcut to create a DetailItem with explicit project_number, po_number, detail_number, line_number.
        We also need po_id from purchase_order. We'll try to find it by (project_number, po_number).
        """

        # 2) Combine data
        kwargs.update({
            "project_number": project_number,
            "po_number": po_number,
            "detail_number": detail_number,
            "line_number": line_number
        })

        # 3) Possibly define concurrency fallback
        unique_lookup = {
            "project_number": project_number,
            "po_number": po_number,
            "detail_number": detail_number,
            "line_number": line_number
        }
        return self._create_record(DetailItem, unique_lookup=unique_lookup, **kwargs)

    def update_detail_item_by_keys(
        self,
        project_number: int,
        po_number: int,
        detail_number: int,
        line_number: int,
        **kwargs
    ):
        """
        Shortcut to update the first matching DetailItem found by (project_number, po_number, detail_number, line_number).
        """
        matches = self.search_detail_item_by_keys(project_number, po_number, detail_number, line_number)
        if not matches:
            return None

        if isinstance(matches, list):
            match = matches[0]
        else:
            match = matches

        detail_item_id = match["id"]
        return self.update_detail_item(detail_item_id, **kwargs)
    # endregion

    # endregion

    # region 💼 Contact
    def search_contacts(self, column_names=None, values=None):
        self.logger.debug(f"💼 Searching Contacts with column_names={column_names}, values={values}")
        return self._search_records(Contact, column_names, values)

    def create_contact(self, **kwargs):
        self.logger.debug(f"🙋 create_contact with {kwargs}")
        # If 'name' is unique or 'email' is unique, define unique_lookup
        unique_lookup = {}
        if 'name' in kwargs:
            unique_lookup['name'] = kwargs['name']
        return self._create_record(Contact, unique_lookup=unique_lookup, **kwargs)

    def update_contact(self, contact_id, **kwargs):
        self.logger.debug(f"💁‍♀️ update_contact -> Contact(id={contact_id}), data={kwargs}")
        return self._update_record(Contact, contact_id, **kwargs)

    def find_contact_close_match(self, contact_name: str, all_db_contacts: List[Dict[str, Any]], cutoff=0.7):
        """
        Attempt a fuzzy name match among all_db_contacts.
        Returns a list of dicts or None.
        """
        from difflib import get_close_matches

        if not all_db_contacts:
            self.logger.debug("No existing contacts provided to match against.")
            return None

        name_map = {c['name']: c for c in all_db_contacts if c.get('name')}
        best_matches = get_close_matches(contact_name, name_map.keys(), n=5, cutoff=cutoff)
        if best_matches:
            return [name_map[m] for m in best_matches]
        else:
            return None

    def create_minimal_contact(self, contact_name: str):
        """
        Creates a minimal Contact record with just name=contact_name, vendor_type="Vendor".
        """
        return self.create_contact(name=contact_name, vendor_type="Vendor")
    # endregion

    # region 🏗 Projects
    def search_projects(self, column_names, values):
        self.logger.debug(f"🏗 Searching Projects with columns={column_names}, values={values}")
        return self._search_records(Project, column_names, values)

    def create_project(self, **kwargs):
        self.logger.debug(f"🏗 create_project with {kwargs}")
        # If 'project_number' is unique
        unique_lookup = {}
        if 'project_number' in kwargs:
            unique_lookup['project_number'] = kwargs['project_number']
        return self._create_record(Project, unique_lookup=unique_lookup, **kwargs)

    def update_project(self, project_id, **kwargs):
        self.logger.debug(f"🤖 update_project -> Project(id={project_id}), data={kwargs}")
        return self._update_record(Project, project_id, **kwargs)
    # endregion

    # region 💰 BankTransaction
    def search_bank_transactions(self, column_names, values):
        self.logger.debug(f"💰 search_bank_transactions: columns={column_names}, values={values}")
        return self._search_records(BankTransaction, column_names, values)

    def create_bank_transaction(self, **kwargs):
        self.logger.debug(f"💸 create_bank_transaction with {kwargs}")
        return self._create_record(BankTransaction, **kwargs)

    def update_bank_transaction(self, transaction_id, **kwargs):
        self.logger.debug(f"💸 update_bank_transaction -> BankTransaction(id={transaction_id}), data={kwargs}")
        return self._update_record(BankTransaction, transaction_id, **kwargs)
    # endregion

    # region 📜 BillLineItem
    def search_bill_line_items(self, column_names, values):
        self.logger.debug(f"📜 search_bill_line_items: columns={column_names}, values={values}")
        return self._search_records(BillLineItem, column_names, values)

    def create_bill_line_item(self, **kwargs):
        self.logger.debug(f"📜 create_bill_line_item with {kwargs}")
        unique_lookup = {}
        # If you consider (parent_id, detail_number, line_number) unique, define it here
        if 'parent_id' in kwargs and 'detail_number' in kwargs and 'line_number' in kwargs:
            unique_lookup = {
                'parent_id': kwargs['parent_id'],
                'detail_number': kwargs['detail_number'],
                'line_number': kwargs['line_number']
            }
        return self._create_record(BillLineItem, unique_lookup=unique_lookup, **kwargs)

    def update_bill_line_item(self, bill_line_item_id, **kwargs):
        self.logger.debug(f"📜 update_bill_line_item -> BillLineItem(id={bill_line_item_id}), data={kwargs}")
        return self._update_record(BillLineItem, bill_line_item_id, **kwargs)

    # region 📜 BillLineItem by Keys
    def search_bill_line_item_by_keys(
        self,
        project_number: Optional[int] = None,
        po_number: Optional[int] = None,
        detail_number: Optional[int] = None,
        line_number: Optional[int] = None
    ) -> Union[None, Dict[str, Any], List[Dict[str, Any]]]:
        """
        Returns BillLineItem(s) for any subset of project_number, po_number, detail_number, line_number.
        """
        if not project_number and not po_number and not detail_number and not line_number:
            return self.search_bill_line_items([], [])

        col_filters = []
        val_filters = []

        if project_number is not None:
            col_filters.append("project_number")
            val_filters.append(project_number)
        if po_number is not None:
            col_filters.append("po_number")
            val_filters.append(po_number)
        if detail_number is not None:
            col_filters.append("detail_number")
            val_filters.append(detail_number)
        if line_number is not None:
            col_filters.append("line_number")
            val_filters.append(line_number)

        return self.search_bill_line_items(col_filters, val_filters)

    def create_bill_line_item_by_keys(
        self,
        parent_id: int,
        project_number: int,
        po_number: int,
        detail_number: int,
        line_number: int,
        **kwargs
    ):
        kwargs.update({
            "parent_id": parent_id,
            "project_number": project_number,
            "po_number": po_number,
            "detail_number": detail_number,
            "line_number": line_number,
        })
        unique_lookup = {
            "parent_id": parent_id,
            "detail_number": detail_number,
            "line_number": line_number,
        }
        return self._create_record(BillLineItem, unique_lookup=unique_lookup, **kwargs)

    def update_bill_line_item_by_keys(
        self,
        project_number: int,
        po_number: int,
        detail_number: int,
        line_number: int,
        **kwargs
    ):
        matches = self.search_bill_line_item_by_keys(project_number, po_number, detail_number, line_number)
        if not matches:
            return None
        if isinstance(matches, list):
            match = matches[0]
        else:
            match = matches
        return self.update_bill_line_item(match["id"], **kwargs)
    # endregion
    # endregion

    # region 🧾 Invoice
    def search_invoices(self, column_names, values):
        self.logger.debug(f"🧾 search_invoices: columns={column_names}, values={values}")
        return self._search_records(Invoice, column_names, values)

    def create_invoice(self, **kwargs):
        self.logger.debug(f"🧾 create_invoice with {kwargs}")
        unique_lookup = {}
        if 'invoice_number' in kwargs:
            unique_lookup['invoice_number'] = kwargs['invoice_number']
        return self._create_record(Invoice, unique_lookup=unique_lookup, **kwargs)

    def update_invoice(self, invoice_id, **kwargs):
        self.logger.debug(f"🧾 update_invoice -> Invoice(id={invoice_id}), data={kwargs}")
        return self._update_record(Invoice, invoice_id, **kwargs)

    # region 🧾 Invoice by Keys
    def search_invoice_by_keys(
        self,
        project_number: Optional[int] = None,
        po_number: Optional[int] = None,
        invoice_number: Optional[int] = None
    ) -> Union[None, Dict[str, Any], List[Dict[str, Any]]]:
        """
        Return Invoices(s) based on any subset of project_number, po_number, invoice_number.
        If none are provided, returns ALL.
        """
        if not project_number and not po_number and not invoice_number:
            return self.search_invoices([], [])

        col_filters = []
        val_filters = []

        if project_number is not None:
            col_filters.append("project_number")
            val_filters.append(project_number)
        if po_number is not None:
            col_filters.append("po_number")
            val_filters.append(po_number)
        if invoice_number is not None:
            col_filters.append("invoice_number")
            val_filters.append(invoice_number)

        return self.search_invoices(col_filters, val_filters)
    # endregion
    # endregion

    # region 🧾 Receipt
    def search_receipts(self, column_names, values):
        self.logger.debug(f"🧾 search_receipts: columns={column_names}, values={values}")
        return self._search_records(Receipt, column_names, values)

    def create_receipt(self, **kwargs):
        self.logger.debug(f"🧾 create_receipt with {kwargs}")
        return self._create_record(Receipt, **kwargs)

    def update_receipt_by_id(self, receipt_id, **kwargs):
        self.logger.debug(f"🧾 update_receipt_by_id -> Receipt(id={receipt_id}), data={kwargs}")
        return self._update_record(Receipt, receipt_id, **kwargs)

    # region 🧾 Receipt by Keys
    def search_receipt_by_keys(
        self,
        project_number: Optional[int] = None,
        po_number: Optional[int] = None,
        detail_number: Optional[int] = None,
        line_number: Optional[int] = None
    ):
        if not project_number and not po_number and not detail_number and not line_number:
            return self.search_receipts([], [])

        col_filters = []
        val_filters = []

        if project_number is not None:
            col_filters.append("project_number")
            val_filters.append(project_number)
        if po_number is not None:
            col_filters.append("po_number")
            val_filters.append(po_number)
        if detail_number is not None:
            col_filters.append("detail_number")
            val_filters.append(detail_number)
        if line_number is not None:
            col_filters.append("line_number")
            val_filters.append(line_number)

        return self.search_receipts(col_filters, val_filters)

    def create_receipt_by_keys(
        self,
        project_number: int,
        po_number: int,
        detail_number: int,
        line_number: int,
        **kwargs
    ):
        kwargs.update({
            "project_number": project_number,
            "po_number": po_number,
            "detail_number": detail_number,
            "line_number": line_number
        })
        return self.create_receipt(**kwargs)

    def update_receipt_by_keys(
        self,
        project_number: int,
        po_number: int,
        detail_number: int,
        line_number: int,
        **kwargs
    ):
        recs = self.search_receipt_by_keys(project_number, po_number, detail_number, line_number)
        if not recs:
            return None
        if isinstance(recs, list):
            recs = recs[0]
        return self.update_receipt_by_id(recs["id"], **kwargs)
    # endregion
    # endregion

    # region 💵 SpendMoney
    def search_spend_money(self, column_names, values, deleted=False):
        self.logger.debug(
            f"💵 search_spend_money: columns={column_names}, values={values}, deleted={deleted}"
        )
        records = self._search_records(SpendMoney, column_names, values)
        if not records:
            return records

        # If your schema uses something like 'status' = "DELETED", filter it out unless deleted=True
        # (Adjust below if your schema uses 'state' or another column.)
        if not deleted:
            if isinstance(records, dict):
                # For a single record
                if records.get('state') == "DELETED":
                    return None
            elif isinstance(records, list):
                # For multiple
                records = [rec for rec in records if rec.get('state') != "DELETED"]
        return records

    def create_spend_money(self, **kwargs):
        self.logger.debug(f"💸 create_spend_money with {kwargs}")
        return self._create_record(SpendMoney, **kwargs)

    def update_spend_money(self, spend_money_id, **kwargs):
        self.logger.debug(f"💸 update_spend_money -> SpendMoney(id={spend_money_id}), data={kwargs}")
        return self._update_record(SpendMoney, spend_money_id, **kwargs)

    # region 💵 SpendMoney by Keys
    def search_spend_money_by_keys(
        self,
        project_number: Optional[int] = None,
        po_number: Optional[int] = None,
        detail_number: Optional[int] = None,
        line_number: Optional[int] = None,
        deleted: bool = False
    ):
        if not project_number and not po_number and not detail_number and not line_number:
            return self.search_spend_money([], [], deleted=deleted)

        col_filters = []
        val_filters = []

        if project_number is not None:
            col_filters.append("project_number")
            val_filters.append(project_number)
        if po_number is not None:
            col_filters.append("po_number")
            val_filters.append(po_number)
        if detail_number is not None:
            col_filters.append("detail_number")
            val_filters.append(detail_number)
        if line_number is not None:
            col_filters.append("line_number")
            val_filters.append(line_number)

        return self.search_spend_money(col_filters, val_filters, deleted=deleted)

    def create_spend_money_by_keys(
        self,
        project_number: int,
        po_number: int,
        detail_number: int,
        line_number: int,
        **kwargs
    ):
        kwargs.update({
            "project_number": project_number,
            "po_number": po_number,
            "detail_number": detail_number,
            "line_number": line_number
        })
        # If you want concurrency fallback on (project_number, po_number, detail_number, line_number):
        unique_lookup = {
            "project_number": project_number,
            "po_number": po_number,
            "detail_number": detail_number,
            "line_number": line_number
        }
        return self._create_record(SpendMoney, unique_lookup=unique_lookup, **kwargs)

    def update_spend_money_by_keys(
        self,
        project_number: int,
        po_number: int,
        detail_number: int,
        line_number: int,
        **kwargs
    ):
        recs = self.search_spend_money_by_keys(project_number, po_number, detail_number, line_number)
        if not recs:
            return None
        if isinstance(recs, list):
            recs = recs[0]
        return self.update_spend_money(recs["id"], **kwargs)
    # endregion
    # endregion

    # region 🏦 TaxAccount
    def search_tax_accounts(self, column_names, values):
        self.logger.debug(f"🏦 search_tax_accounts with columns={column_names}, values={values}")
        return self._search_records(TaxAccount, column_names, values)

    def create_tax_account(self, **kwargs):
        self.logger.debug(f"🏦 create_tax_account with {kwargs}")
        return self._create_record(TaxAccount, **kwargs)

    def update_tax_account(self, tax_account_id, **kwargs):
        self.logger.debug(f"🏦 update_tax_account -> TaxAccount(id={tax_account_id}), data={kwargs}")
        return self._update_record(TaxAccount, tax_account_id, **kwargs)
    # endregion

    # region 🏷 XeroBill
    def search_xero_bills(self, column_names, values):
        self.logger.debug(f"🏷 search_xero_bills with columns={column_names}, values={values}")
        return self._search_records(XeroBill, column_names, values)

    def create_xero_bill(self, **kwargs):
        """
        If 'xero_reference_number' is unique, pass that as unique_lookup:
        """
        self.logger.debug(f"🏷 create_xero_bill with {kwargs}")
        unique_lookup = {}
        if 'xero_reference_number' in kwargs:
            unique_lookup['xero_reference_number'] = kwargs['xero_reference_number']
        return self._create_record(XeroBill, unique_lookup=unique_lookup, **kwargs)

    def update_xero_bill(self, parent_id, **kwargs):
        self.logger.debug(f"🏷 update_xero_bill -> XeroBill(id={parent_id}), data={kwargs}")
        return self._update_record(XeroBill, parent_id, **kwargs)

    # region 🏷 XeroBill by Keys
    def search_xero_bill_by_keys(
        self,
        project_number: Optional[int] = None,
        po_number: Optional[int] = None,
        detail_number: Optional[int] = None
    ) -> Union[None, Dict[str, Any], List[Dict[str, Any]]]:
        """
        Returns XeroBill(s) based on any subset of project_number, po_number, detail_number.
        """
        if not project_number and not po_number and not detail_number:
            return self.search_xero_bills([], [])

        col_filters = []
        val_filters = []
        if project_number is not None:
            col_filters.append("project_number")
            val_filters.append(project_number)
        if po_number is not None:
            col_filters.append("po_number")
            val_filters.append(po_number)
        if detail_number is not None:
            col_filters.append("detail_number")
            val_filters.append(detail_number)

        return self.search_xero_bills(col_filters, val_filters)

    def create_xero_bill_by_keys(
        self,
        project_number: int,
        po_number: int,
        detail_number: int,
        **kwargs
    ):
        kwargs.update({
            "project_number": project_number,
            "po_number": po_number,
            "detail_number": detail_number
        })
        # If you consider (project_number, po_number, detail_number) effectively unique:
        unique_lookup = {
            "project_number": project_number,
            "po_number": po_number,
            "detail_number": detail_number
        }
        return self._create_record(XeroBill, unique_lookup=unique_lookup, **kwargs)

    def update_xero_bill_by_keys(
        self,
        project_number: int,
        po_number: int,
        detail_number: int,
        **kwargs
    ):
        bills = self.search_xero_bill_by_keys(project_number, po_number, detail_number)
        if not bills:
            return None
        if isinstance(bills, list):
            bills = bills[0]
        return self.update_xero_bill(bills["id"], **kwargs)
    # endregion
    #endregion

    #region HAS CHANGED FUNCTIONS
  # --------------------------------------------------------
    # 1) Revised Private Helper: _has_changes_for_record
    # --------------------------------------------------------
    def _has_changes_for_record(
        self,
        model,
        record_id: Optional[int] = None,
        unique_filters: Dict[str, Any] = None,
        **kwargs
    ) -> bool:
        """
        Checks whether a record of a given model has changed.
        1) If record_id is provided, we fetch by ID.
        2) Else if unique_filters is provided, we attempt a lookup with those filters.
        3) Compare each kwarg to the record’s fields.
        4) Return True if any difference is found, otherwise False.

        Special logic:
          - For 'state', do a case-insensitive comparison. Adjust as needed.
        """
        if not record_id and not unique_filters:
            self.logger.warning(
                f"Cannot check changes for {model.__name__}, neither record_id nor unique_filters given."
            )
            return False

        # Step A: Locate the record
        record_dict = None

        if record_id:
            # Fetch by primary key
            with get_db_session() as session:
                record = session.query(model).get(record_id)
                if record:
                    record_dict = self._serialize_record(record)
        else:
            # Fetch by unique filters
            filters_list = list(unique_filters.keys())
            values_list = list(unique_filters.values())
            found = self._search_records(model, filters_list, values_list)

            # If exactly one match, store its dict
            if isinstance(found, dict):
                record_dict = found
            elif isinstance(found, list) and len(found) == 1:
                record_dict = found[0]

        if not record_dict:
            self.logger.debug(
                f"No single {model.__name__} record found to compare. Returning False."
            )
            return False

        # Step B: Compare each kwarg to what is in record_dict
        for field, new_val in kwargs.items():
            old_val = record_dict.get(field)

            # Optional case-insensitive compare for "state"
            if field == "state":
                old_val = (old_val or "").upper()
                new_val = (new_val or "").upper()

            if old_val != new_val:
                return True

        return False

    # --------------------------------------------------------
    # 2) Example “Has Changed” Methods per Table
    # --------------------------------------------------------

    # -- PurchaseOrder
    def purchase_order_has_changes(
        self,
        record_id: Optional[int] = None,
        project_number: Optional[int] = None,
        po_number: Optional[int] = None,
        **kwargs
    ) -> bool:
        """
        Check if a PurchaseOrder has changed. Provide either:
          - record_id, OR
          - (project_number, po_number) as unique fields.
        Additional fields to compare are passed via kwargs (e.g. description='xyz', state='Draft', etc.).
        """
        unique_filters = {}
        if project_number is not None:
            unique_filters["project_number"] = project_number
        if po_number is not None:
            unique_filters["po_number"] = po_number

        return self._has_changes_for_record(
            PurchaseOrder,
            record_id=record_id,
            unique_filters=unique_filters if not record_id else None,
            **kwargs
        )

    # -- DetailItem
    def detail_item_has_changes(
        self,
        record_id: Optional[int] = None,
        project_number: Optional[int] = None,
        po_number: Optional[int] = None,
        detail_number: Optional[int] = None,
        line_number: Optional[int] = None,
        **kwargs
    ) -> bool:
        """
        Check if a DetailItem has changed. Provide either:
          - record_id, OR
          - (project_number, po_number, detail_number, line_number).
        """
        unique_filters = {}
        if project_number is not None:
            unique_filters["project_number"] = project_number
        if po_number is not None:
            unique_filters["po_number"] = po_number
        if detail_number is not None:
            unique_filters["detail_number"] = detail_number
        if line_number is not None:
            unique_filters["line_number"] = line_number

        return self._has_changes_for_record(
            DetailItem,
            record_id=record_id,
            unique_filters=unique_filters if not record_id else None,
            **kwargs
        )

    # -- Contact
    def contact_has_changes(
        self,
        record_id: Optional[int] = None,
        name: Optional[str] = None,
        email: Optional[str] = None,
        **kwargs
    ) -> bool:
        """
        If Contact is uniquely identified by 'name', or by 'email', or by record_id.
        Adjust as needed for your schema.
        """
        unique_filters = {}
        if name is not None:
            unique_filters["name"] = name
        if email is not None:
            unique_filters["email"] = email

        return self._has_changes_for_record(
            Contact,
            record_id=record_id,
            unique_filters=unique_filters if not record_id else None,
            **kwargs
        )

    # -- Project
    def project_has_changes(
        self,
        record_id: Optional[int] = None,
        project_number: Optional[int] = None,
        **kwargs
    ) -> bool:
        unique_filters = {}
        if project_number is not None:
            unique_filters["project_number"] = project_number

        return self._has_changes_for_record(
            Project,
            record_id=record_id,
            unique_filters=unique_filters if not record_id else None,
            **kwargs
        )

    # -- BankTransaction
    def bank_transaction_has_changes(
        self,
        record_id: Optional[int] = None,
        transaction_id_xero: Optional[str] = None,
        **kwargs
    ) -> bool:
        """
        Example: If your BankTransaction is uniquely identified by some external ID,
        pass it in, else use the record_id.
        """
        unique_filters = {}
        if transaction_id_xero is not None:
            unique_filters["transaction_id_xero"] = transaction_id_xero

        return self._has_changes_for_record(
            BankTransaction,
            record_id=record_id,
            unique_filters=unique_filters if not record_id else None,
            **kwargs
        )

    # -- BillLineItem
    def bill_line_item_has_changes(
        self,
        record_id: Optional[int] = None,
        parent_id: Optional[int] = None,
        detail_number: Optional[int] = None,
        line_number: Optional[int] = None,
        **kwargs
    ) -> bool:
        unique_filters = {}
        if parent_id is not None:
            unique_filters["parent_id"] = parent_id
        if detail_number is not None:
            unique_filters["detail_number"] = detail_number
        if line_number is not None:
            unique_filters["line_number"] = line_number

        return self._has_changes_for_record(
            BillLineItem,
            record_id=record_id,
            unique_filters=unique_filters if not record_id else None,
            **kwargs
        )

    # -- Invoice
    def invoice_has_changes(
        self,
        record_id: Optional[int] = None,
        invoice_number: Optional[str] = None,
        **kwargs
    ) -> bool:
        unique_filters = {}
        if invoice_number is not None:
            unique_filters["invoice_number"] = invoice_number

        return self._has_changes_for_record(
            Invoice,
            record_id=record_id,
            unique_filters=unique_filters if not record_id else None,
            **kwargs
        )

    # -- Receipt
    def receipt_has_changes(
        self,
        record_id: Optional[int] = None,
        project_number: Optional[int] = None,
        po_number: Optional[int] = None,
        detail_number: Optional[int] = None,
        line_number: Optional[int] = None,
        **kwargs
    ) -> bool:
        unique_filters = {}
        if project_number is not None:
            unique_filters["project_number"] = project_number
        if po_number is not None:
            unique_filters["po_number"] = po_number
        if detail_number is not None:
            unique_filters["detail_number"] = detail_number
        if line_number is not None:
            unique_filters["line_number"] = line_number

        return self._has_changes_for_record(
            Receipt,
            record_id=record_id,
            unique_filters=unique_filters if not record_id else None,
            **kwargs
        )

    # -- SpendMoney
    def spend_money_has_changes(
        self,
        record_id: Optional[int] = None,
        project_number: Optional[int] = None,
        po_number: Optional[int] = None,
        detail_number: Optional[int] = None,
        line_number: Optional[int] = None,
        **kwargs
    ) -> bool:
        unique_filters = {}
        if project_number is not None:
            unique_filters["project_number"] = project_number
        if po_number is not None:
            unique_filters["po_number"] = po_number
        if detail_number is not None:
            unique_filters["detail_number"] = detail_number
        if line_number is not None:
            unique_filters["line_number"] = line_number

        return self._has_changes_for_record(
            SpendMoney,
            record_id=record_id,
            unique_filters=unique_filters if not record_id else None,
            **kwargs
        )

    # -- TaxAccount
    def tax_account_has_changes(
        self,
        record_id: Optional[int] = None,
        tax_code: Optional[str] = None,
        **kwargs
    ) -> bool:
        unique_filters = {}
        if tax_code is not None:
            unique_filters["tax_code"] = tax_code

        return self._has_changes_for_record(
            TaxAccount,
            record_id=record_id,
            unique_filters=unique_filters if not record_id else None,
            **kwargs
        )

    # -- XeroBill
    def xero_bill_has_changes(
        self,
        record_id: Optional[int] = None,
        xero_reference_number: Optional[str] = None,
        **kwargs
    ) -> bool:
        unique_filters = {}
        if xero_reference_number is not None:
            unique_filters["xero_reference_number"] = xero_reference_number

        return self._has_changes_for_record(
            XeroBill,
            record_id=record_id,
            unique_filters=unique_filters if not record_id else None,
            **kwargs
        )

    #endregion

# endregion

