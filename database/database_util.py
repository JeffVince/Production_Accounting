# -*- coding: utf-8 -*-
"""
database.database_util.py

💻 Database Operations Module
=============================
This module provides flexible, DRY (Don't Repeat Yourself) functions for searching,
creating, and updating records in various database tables, using SQLAlchemy ORM
and a common session pattern.

Additional concurrency changes:
- We now catch IntegrityError when creating a record and attempt a fallback re-query
  if a 'unique_lookup' dict is provided or if you want to handle a known unique column.

"""

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
    AicpCode,
    Receipt,
    SpendMoney,
    TaxAccount,
    XeroBill
)
from database.db_util import get_db_session


class DatabaseOperations:
    """
    🗂 Database Operations Class
    ============================
    Provides flexible, DRY methods to search, create, and update records
    in your new schema, using project_number-based lookups rather than project_id.
    Also handles 'aicp_code_id' logic in detail_item and avoids DetachedInstanceError
    by performing all relevant operations in the same session scope.

    Now includes concurrency-safe creation logic:
    - If we encounter an IntegrityError (e.g., duplicate key), we can re-query
      to see if another thread/process just created the same record.
    """

    def __init__(self):
        self.logger = logging.getLogger("app_logger")
        self.logger.debug("🌟 Hello from DatabaseOperations constructor! Ready to keep the DB in check!")

    # -------------------------------------------------------------------------
    # Helper Method to Turn Model Instance -> dict
    # -------------------------------------------------------------------------
    def _serialize_record(self, record):
        """
        🗄 Serialize a record (SQLAlchemy model) into a dictionary of column names -> values.
        Returns None if the record is None.
        """
        if not record:
            return None
        record_values = {c.name: getattr(record, c.name) for c in record.__table__.columns}
        self.logger.debug(f"🤓 Pulling record: {record_values['id']} from table {record.__table__}")
        return record_values

    # -------------------------------------------------------------------------
    # Generic Search Method
    # -------------------------------------------------------------------------
    def _search_records(
            self,
            model,
            column_names: Optional[List[str]] = None,
            values: Optional[List[Any]] = None
    ) -> Union[None, Dict[str, Any], List[Dict[str, Any]]]:
        """
        🔍 Search for records of a given model based on multiple column filters.
        If no column_names or values are provided, retrieves all records from the table.

        Returns:
            - None if no records
            - A single dict if exactly one found
            - A list if multiple found
            - [] if an error occurred or if mismatch in columns/values

        This is an *atomic* operation: we open a fresh session, do the query,
        commit/rollback, and close (or remove) the session each time.
        """
        column_names = column_names or []
        values = values or []

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

    # -------------------------------------------------------------------------
    # Concurrency-Safe Create
    # -------------------------------------------------------------------------
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
          - If an IntegrityError occurs (e.g., duplicate key), we do a fallback re-query
            if 'unique_lookup' is provided (i.e., the columns that are guaranteed unique).
          - If found, return the existing record. Otherwise, raise or return None.

        :param model: The SQLAlchemy model class.
        :param unique_lookup: A dict of {col_name: value} used to re-query after concurrency conflict.
                              Example: {"xero_reference_number": "2416_20_1"}
        :param kwargs: The data to create the new record with.
        :return: Serialized dict of the newly created (or existing) record, or None if error.
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

            except IntegrityError as ie:
                # Possibly a concurrency conflict or unique constraint violation
                self.logger.debug(f"❗ IntegrityError creating {model.__name__} ")
                session.rollback()  # revert this session

                # Attempt fallback re-query if we know how to look up
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

    # -------------------------------------------------------------------------
    # Concurrency-Safe Update (Optional)
    # -------------------------------------------------------------------------
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

        :param model: The SQLAlchemy model class
        :param record_id: The primary key ID for the record to update
        :param unique_lookup: Optional dict of {col_name: value} to re-query if unique violation occurs
        :param kwargs: fields to update
        :return: The updated record (serialized) or None
        """
        self.logger.debug(f"🔧 Attempting to update {model.__name__}(id={record_id}). Fields: {kwargs}")
        self.logger.info(f"🤝 Checking if {model.__name__}(id={record_id}) exists, then updating with {kwargs}.")

        with get_db_session() as session:
            try:
                record = session.query(model).get(record_id)
                if not record:
                    self.logger.info(f"🙅 No {model.__name__} with id={record_id} found.")
                    return None

                for key, value in kwargs.items():
                    if hasattr(record, key):
                        setattr(record, key, value)
                    else:
                        self.logger.warning(
                            f"⚠️ The attribute '{key}' doesn't exist on {model.__name__}. Skipping."
                        )

                # Flush so changes are applied, catches constraint issues
                session.flush()
                self.logger.debug(f"🪄 Flushed updated {model.__name__}(id={record_id}).")

                session.commit()
                self.logger.info("✅ Done updating! The record is all set.")
                return self._serialize_record(record)

            except IntegrityError as ie:
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

    # -------------------------------------------------------------------------
    # From here on, we use the concurrency-safe _create_record / _update_record
    # in all the model-specific methods
    # -------------------------------------------------------------------------
    #
    # e.g. create_contact -> self._create_record(Contact, unique_lookup=..., name=..., etc.)
    # where 'unique_lookup' might be {'name': name} if you have a unique index on 'name'.

    # Example: XeroBill concurrency
    # If you have UNIQUE (xero_reference_number), you can do:
    # create_xero_bill(xero_reference_number="2416_20_1", unique_lookup={"xero_reference_number": "2416_20_1"}, ...)

    # -------------- AicpCodes --------------
    def search_aicp_codes(self, column_names, values):
        self.logger.debug("🔎 Searching for AicpCode entries...")
        return self._search_records(AicpCode, column_names, values)

    def create_aicp_code(self, **kwargs):
        """
        If 'aicp_code' is unique in the DB, pass unique_lookup={'aicp_code': kwargs.get('aicp_code')}
        """
        self.logger.debug(f"🌈 Creating an AicpCode with data={kwargs}")
        unique_lookup = {}
        if 'aicp_code' in kwargs:
            unique_lookup['aicp_code'] = kwargs['aicp_code']
        return self._create_record(AicpCode, unique_lookup=unique_lookup, **kwargs)

    # -------------- PurchaseOrder --------------
    def search_purchase_orders(self, column_names, values):
        self.logger.debug(f"📝 search_purchase_orders: columns={column_names}, values={values}")
        return self._search_records(PurchaseOrder, column_names, values)

    def create_purchase_order(self, **kwargs):
        """
        If you have a unique constraint like (project_id, po_number), you can do:
          unique_lookup = {
              'project_id': kwargs.get('project_id'),
              'po_number': kwargs.get('po_number')
          }
        """
        self.logger.debug(f"📝 create_purchase_order with {kwargs}")
        unique_lookup = {}
        if 'project_id' in kwargs and 'po_number' in kwargs:
            unique_lookup = {'project_id': kwargs['project_id'], 'po_number': kwargs['po_number']}

        return self._create_record(PurchaseOrder, unique_lookup=unique_lookup, **kwargs)

    def update_purchase_order(self, po_id, **kwargs):
        """
        If there's a unique constraint (project_id, po_number) that can be updated, pass a unique_lookup.
        Otherwise, a normal update.
        """
        self.logger.debug(f"📝 update_purchase_order -> PurchaseOrder(id={po_id}), data={kwargs}")
        # e.g. unique_lookup = {'project_id': new_proj_id, 'po_number': new_po_number}
        return self._update_record(PurchaseOrder, po_id, **kwargs)

    # -------------- DetailItem --------------
    def search_detail_items(self, column_names, values):
        self.logger.debug(f"🔎 search_detail_items: columns={column_names}, values={values}")
        return self._search_records(DetailItem, column_names, values)

    def create_detail_item(self, **kwargs):
        self.logger.debug(f"🧱 create_detail_item with {kwargs}")
        # If (po_id, detail_number, line_id) is unique, do:
        unique_lookup = {}
        if 'po_id' in kwargs and 'detail_number' in kwargs and 'line_id' in kwargs:
            unique_lookup = {
                'po_id': kwargs['po_id'],
                'detail_number': kwargs['detail_number'],
                'line_id': kwargs['line_id']
            }
        return self._create_record(DetailItem, unique_lookup=unique_lookup, **kwargs)

    def update_detail_item(self, detail_item_id, **kwargs):
        self.logger.debug(f"🔧 update_detail_item -> DetailItem(id={detail_item_id}), data={kwargs}")
        return self._update_record(DetailItem, detail_item_id, **kwargs)

    # -------------- Contact --------------
    def search_contacts(self, column_names=None, values=None):
        self.logger.debug(f"💼 Searching Contacts with column_names={column_names}, values={values}")
        return self._search_records(Contact, column_names, values)

    def create_contact(self, **kwargs):
        self.logger.debug(f"🙋 create_contact with {kwargs}")
        # If 'email' is unique or 'name' is unique, define a unique_lookup:
        unique_lookup = {}
        if 'name' in kwargs:
            unique_lookup['name'] = kwargs['name']
        return self._create_record(Contact, unique_lookup=unique_lookup, **kwargs)

    def update_contact(self, contact_id, **kwargs):
        self.logger.debug(f"💁‍♀️ update_contact -> Contact(id={contact_id}), data={kwargs}")
        return self._update_record(Contact, contact_id, **kwargs)

    # -------------- Projects --------------
    def search_projects(self, column_names, values):
        self.logger.debug(f"🏗 Searching Projects with columns={column_names}, values={values}")
        return self._search_records(Project, column_names, values)

    def create_project(self, **kwargs):
        self.logger.debug(f"🏗 create_project with {kwargs}")
        # If 'project_number' is unique, do:
        unique_lookup = {}
        if 'project_number' in kwargs:
            unique_lookup['project_number'] = kwargs['project_number']
        return self._create_record(Project, unique_lookup=unique_lookup, **kwargs)

    def update_project(self, project_id, **kwargs):
        self.logger.debug(f"🤖 update_project -> Project(id={project_id}), data={kwargs}")
        return self._update_record(Project, project_id, **kwargs)

    # -------------- BankTransaction --------------
    def search_bank_transactions(self, column_names, values):
        self.logger.debug(f"💰 search_bank_transactions: columns={column_names}, values={values}")
        return self._search_records(BankTransaction, column_names, values)

    def create_bank_transaction(self, **kwargs):
        self.logger.debug(f"💸 create_bank_transaction with {kwargs}")
        return self._create_record(BankTransaction, **kwargs)

    def update_bank_transaction(self, transaction_id, **kwargs):
        self.logger.debug(f"💸 update_bank_transaction -> BankTransaction(id={transaction_id}), data={kwargs}")
        return self._update_record(BankTransaction, transaction_id, **kwargs)

    # -------------- BillLineItem --------------
    def search_bill_line_items(self, column_names, values):
        self.logger.debug(f"📜 search_bill_line_items: columns={column_names}, values={values}")
        return self._search_records(BillLineItem, column_names, values)

    def create_bill_line_item(self, **kwargs):
        self.logger.debug(f"📜 create_bill_line_item with {kwargs}")
        # If you have a unique constraint on (xero_bill_id, detail_item_id), define unique_lookup:
        unique_lookup = {}
        if 'xero_bill_id' in kwargs and 'detail_item_id' in kwargs:
            unique_lookup = {
                'xero_bill_id': kwargs['xero_bill_id'],
                'detail_item_id': kwargs['detail_item_id']
            }
        return self._create_record(BillLineItem, unique_lookup=unique_lookup, **kwargs)

    def update_bill_line_item(self, bill_line_item_id, **kwargs):
        self.logger.debug(f"📜 update_bill_line_item -> BillLineItem(id={bill_line_item_id}), data={kwargs}")
        return self._update_record(BillLineItem, bill_line_item_id, **kwargs)

    # -------------- Invoice --------------
    def search_invoices(self, column_names, values):
        self.logger.debug(f"🧾 search_invoices: columns={column_names}, values={values}")
        return self._search_records(Invoice, column_names, values)

    def create_invoice(self, **kwargs):
        self.logger.debug(f"🧾 create_invoice with {kwargs}")
        # E.g. if 'invoice_number' is unique:
        unique_lookup = {}
        if 'invoice_number' in kwargs:
            unique_lookup['invoice_number'] = kwargs['invoice_number']
        return self._create_record(Invoice, unique_lookup=unique_lookup, **kwargs)

    def update_invoice(self, invoice_id, **kwargs):
        self.logger.debug(f"🧾 update_invoice -> Invoice(id={invoice_id}), data={kwargs}")
        return self._update_record(Invoice, invoice_id, **kwargs)

    # -------------- Receipt --------------
    def search_receipts(self, column_names, values):
        self.logger.debug(f"🧾 search_receipts: columns={column_names}, values={values}")
        return self._search_records(Receipt, column_names, values)

    def create_receipt(self, **kwargs):
        self.logger.debug(f"🧾 create_receipt with {kwargs}")
        return self._create_record(Receipt, **kwargs)

    def update_receipt_by_id(self, receipt_id, **kwargs):
        self.logger.debug(f"🧾 update_receipt_by_id -> Receipt(id={receipt_id}), data={kwargs}")
        return self._update_record(Receipt, receipt_id, **kwargs)

    # -------------- SpendMoney --------------
    def search_spend_money(self, column_names, values, deleted=False):
        self.logger.debug(
            f"💵 search_spend_money: columns={column_names}, values={values}, deleted={deleted}"
        )
        records = self._search_records(SpendMoney, column_names, values)
        if not records:
            return records

        # Filter out DELETED if not asked for
        if not deleted:
            if isinstance(records, dict):
                if records.get('status') == "DELETED":
                    return None
            elif isinstance(records, list):
                records = [rec for rec in records if rec.get('status') != "DELETED"]
        return records

    def create_spend_money(self, **kwargs):
        self.logger.debug(f"💸 create_spend_money with {kwargs}")
        return self._create_record(SpendMoney, **kwargs)

    def update_spend_money(self, spend_money_id, **kwargs):
        self.logger.debug(f"💸 update_spend_money -> SpendMoney(id={spend_money_id}), data={kwargs}")
        return self._update_record(SpendMoney, spend_money_id, **kwargs)

    # -------------- TaxAccount --------------
    def search_tax_accounts(self, column_names, values):
        self.logger.debug(f"🏦 search_tax_accounts with columns={column_names}, values={values}")
        return self._search_records(TaxAccount, column_names, values)

    def create_tax_account(self, **kwargs):
        self.logger.debug(f"🏦 create_tax_account with {kwargs}")
        return self._create_record(TaxAccount, **kwargs)

    def update_tax_account(self, tax_account_id, **kwargs):
        self.logger.debug(f"🏦 update_tax_account -> TaxAccount(id={tax_account_id}), data={kwargs}")
        return self._update_record(TaxAccount, tax_account_id, **kwargs)

    # -------------- XeroBill --------------
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

    def update_xero_bill(self, xero_bill_id, **kwargs):
        self.logger.debug(f"🏷 update_xero_bill -> XeroBill(id={xero_bill_id}), data={kwargs}")
        # If changing the reference number (rare?), pass unique_lookup
        return self._update_record(XeroBill, xero_bill_id, **kwargs)