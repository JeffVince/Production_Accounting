# -*- coding: utf-8 -*-
"""
database.database_util.py

💻 Database Operations Module
=============================
This module provides flexible, DRY (Don't Repeat Yourself) functions for searching,
creating, and updating records in various database tables, using SQLAlchemy ORM
and a common session pattern.

Key modifications:
- `detail_item` table now has `aicp_code_id` instead of `aicp_code`.
- `create_detail_item_by_keys(...)` fully handles any `aicp_code` argument (int or str).
- We avoid DetachedInstanceError by always accessing `po.id` in the same session
  that loads the PurchaseOrder, storing the numeric ID, and then creating the DetailItem.

ADDITIONAL changes:
- Added `session.flush()` before `session.commit()` in _create_record(...) and _update_record(...).
"""

from contextlib import contextmanager
from typing import Optional, Dict, Any

import logging
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
    """

    def __init__(self):
        self.logger = logging.getLogger("app_logger")
        self.logger.debug("🌟 Hello from DatabaseOperations constructor! Ready to keep the DB in check!")

    # -------------------------------------------------------------------------
    # Helper Methods
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

    def _search_records(self, model, column_names=None, values=None):
        """
        🔍 Search for records of a given model based on multiple column filters.
        If no column_names or values are provided, retrieves all records from the table.

        Returns:
            - None if no records
            - A single dict if exactly one found
            - A list if multiple found
            - [] if an error occurred or if we had a mismatch in columns/values

        This is an *atomic* operation: we open a fresh session, do the query, commit/rollback,
        and close (or remove) the session.
        """
        column_names = column_names or []
        values = values or []

        if column_names and values:
            self.logger.debug(
                f"🕵️ Searching {model.__name__} with filters: {list(zip(column_names, values))}")
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
                self.logger.error(
                    f"💥 Error searching {model.__name__}: {e}",
                    exc_info=True
                )
                return []

    def _create_record(self, model, **kwargs):
        """
        🆕 Create a new record in the database, returning its serialized form or None on error.
        Includes session.flush() before session.commit() to ensure ID is generated & data visible.
        """
        self.logger.debug(f"🧑‍💻 Creating new {model.__name__} using data: {kwargs}")
        self.logger.info(f"🌱 About to insert a fresh record into {model.__name__} with {kwargs}")
        with get_db_session() as session:
            try:
                record = model(**kwargs)
                session.add(record)

                # <-- FLUSH to ensure the insert is sent to DB, ID is available
                session.flush()
                self.logger.debug(f"🪄 Flushed new {model.__name__}. ID now: {record.id}")

                session.commit()
                self.logger.info("🎉 Creation successful! Record is now in the DB.")
                return self._serialize_record(record)
            except Exception as e:
                session.rollback()
                self.logger.error(f"💥 Trouble creating {model.__name__}: {e}", exc_info=True)
                return None

    def _update_record(self, model, record_id, **kwargs):
        """
        🔄 Update an existing record by its primary key (ID).
        Returns the serialized updated record, or None if not found or on error.
        Includes session.flush() before session.commit() so changes are visible immediately.
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

                # <-- FLUSH so that the UPDATE hits the DB, ensuring new values are accessible
                session.flush()
                self.logger.debug(f"🪄 Flushed updated {model.__name__}(id={record_id}).")

                session.commit()
                self.logger.info("✅ Done updating! The record is all set.")
                return self._serialize_record(record)
            except Exception as e:
                session.rollback()
                self.logger.error(f"💥 Had an issue updating {model.__name__}: {e}", exc_info=True)
                return None

    # ---------------------------------------------------------------------
    #  Levenshtein + find_contact_close_match
    # ---------------------------------------------------------------------
    def _levenshtein_distance(self, s1: str, s2: str) -> int:
        """
        Optimized Levenshtein distance calculation with early rejection if
        the first letters differ.
        """
        original_s1, original_s2 = s1, s2
        s1, s2 = s1.lower(), s2.lower()

        # Early exit if first letters differ
        if s1 and s2 and s1[0] != s2[0]:
            dist = len(s1) + len(s2)
            self.logger.debug(
                f"🚫 First-letter mismatch: '{original_s1}' vs '{original_s2}', distance={dist}"
            )
            return dist

        # If either is empty, distance is the length of the other
        if not s1:
            return len(s2)
        if not s2:
            return len(s1)

        # Iterative dynamic programming
        m, n = len(s1), len(s2)
        dp = [[0] * (n + 1) for _ in range(m + 1)]
        for i in range(m + 1):
            dp[i][0] = i
        for j in range(n + 1):
            dp[0][j] = j

        for i in range(1, m + 1):
            for j in range(1, n + 1):
                cost = 0 if s1[i - 1] == s2[j - 1] else 1
                dp[i][j] = min(
                    dp[i - 1][j] + 1,
                    dp[i][j - 1] + 1,
                    dp[i - 1][j - 1] + cost
                )
        return dp[m][n]

    def find_contact_close_match(self, contact_name: str, all_contacts, max_distance: int = 2):
        """
        Finds an exact or "close" match for 'contact_name' among 'all_contacts'.
        Returns the matched contact (dict) if found, else None.
        """
        contact_name_lower = contact_name.lower()
        # 1) Exact matches
        for c in all_contacts:
            if c['name'].strip().lower() == contact_name_lower:
                self.logger.info(f"✅ Exact match found: '{c['name']}' for '{contact_name}'.")
                return c

        # 2) Close matches
        best_candidate = None
        best_distance = max_distance + 1
        for i, c in enumerate(all_contacts):
            if i % 50 == 0:
                self.logger.debug(
                    f"Comparing '{contact_name}' to '{c['name']}', index={i}"
                )
            current_distance = self._levenshtein_distance(contact_name_lower, c['name'].strip().lower())
            if current_distance < best_distance:
                best_distance = current_distance
                best_candidate = c

        if best_candidate and best_distance <= max_distance:
            self.logger.info(
                f"⚠️ Close match for '{contact_name}' → '{best_candidate['name']}', distance={best_distance}"
            )
            return best_candidate
        return None

    # -------------------------------------------------------------------------
    #  AicpCodes
    # -------------------------------------------------------------------------
    def search_aicp_codes(self, column_names, values):
        self.logger.debug("🔎 Searching for AicpCode entries...")
        return self._search_records(AicpCode, column_names, values)

    def create_aicp_code(self, **kwargs):
        self.logger.debug(f"🌈 Creating an AicpCode with data={kwargs}")
        return self._create_record(AicpCode, **kwargs)

    # -------------------------------------------------------------------------
    #  PurchaseOrder
    # -------------------------------------------------------------------------
    def search_purchase_order_by_keys(self, project_number, po_number=None):
        """
        🔍 Search for PurchaseOrders based on project_number & optional po_number.
        Returns None or a single dict or a list of dicts.
        """
        search_criteria = f"project_number='{project_number}'"
        if po_number:
            search_criteria += f", po_number='{po_number}'"
        self.logger.debug(f"🤔 Checking for PurchaseOrders with ({search_criteria}).")
        self.logger.info(f"🚦 Searching PurchaseOrders with {search_criteria}")

        with get_db_session() as session:
            try:
                query = (
                    session.query(PurchaseOrder)
                        .join(Project, PurchaseOrder.project_id == Project.id)
                        .filter(Project.project_number == project_number)
                )
                if po_number:
                    query = query.filter(PurchaseOrder.po_number == po_number)

                results = query.all()
                if not results:
                    self.logger.info("🙅 No PurchaseOrders matched.")
                    return None
                elif len(results) == 1:
                    self.logger.info("🎯 Exactly ONE PurchaseOrder matched.")
                    return self._serialize_record(results[0])
                else:
                    self.logger.info(f"🎯 Found {len(results)} PurchaseOrders. Returning list.")
                    return [self._serialize_record(r) for r in results]
            except Exception as e:
                session.rollback()
                self.logger.error(f"💥 Error searching PurchaseOrders: {e}", exc_info=True)
                return None

    def create_purchase_order_by_keys(self, project_number, po_number, **kwargs):
        self.logger.debug(
            f"🚧 Building a new PurchaseOrder for project_number='{project_number}', po_number='{po_number}'. Extra: {kwargs}"
        )
        with get_db_session() as session:
            try:
                proj = (
                    session.query(Project)
                        .filter(Project.project_number == project_number)
                        .one_or_none()
                )
                if not proj:
                    self.logger.warning(
                        f"💔 Couldn't find Project for project_number={project_number}. No PO creation."
                    )
                    session.rollback()
                    return None

                new_po = PurchaseOrder(project_id=proj.id, po_number=po_number, **kwargs)
                session.add(new_po)

                session.flush()  # optional flush if you want PO.id immediately
                self.logger.debug(f"🪄 Flushed new PurchaseOrder. ID now: {new_po.id}")

                session.commit()
                self.logger.info("🎉 A new PurchaseOrder is now saved!")
                return self._serialize_record(new_po)
            except Exception as e:
                session.rollback()
                self.logger.error(f"💥 Error creating PurchaseOrder: {e}", exc_info=True)
                return None

    def update_purchase_order_by_keys(self, project_number, po_number, **kwargs):
        """
        🔄 Update a PurchaseOrder by (project_number, po_number).
        """
        self.logger.debug(
            f"🔧 Updating PurchaseOrder for project_number='{project_number}', po_number='{po_number}'. Data={kwargs}"
        )
        self.logger.info(
            f"🛠 Checking if PurchaseOrder with project_number={project_number}, po_number={po_number} exists.")
        with get_db_session() as session:
            try:
                query = (
                    session.query(PurchaseOrder)
                        .join(Project, PurchaseOrder.project_id == Project.id)
                        .filter(Project.project_number == project_number)
                        .filter(PurchaseOrder.po_number == po_number)
                )
                po = query.one_or_none()
                if not po:
                    self.logger.info("🙅 No PurchaseOrder found for those keys.")
                    return None

                po_id = po.id
            except Exception as e:
                session.rollback()
                self.logger.error(
                    f"💥 Trouble looking up PurchaseOrder for update: {e}",
                    exc_info=True
                )
                return None

        return self._update_record(PurchaseOrder, record_id=po_id, **kwargs)

    # -------------------------------------------------------------------------
    #  DetailItems
    # -------------------------------------------------------------------------
    def search_detail_item_by_keys(self, project_number, po_number=None, detail_number=None, line_id=None):
        """
        🔍 Search DetailItems by project_number & optional po_number, detail_number, line_id.
        Returns None, single dict, or list of dicts.
        """
        search_criteria = f"project_number='{project_number}'"
        if po_number:
            search_criteria += f", po_number='{po_number}'"
        if detail_number:
            search_criteria += f", detail_number='{detail_number}'"
        if line_id:
            search_criteria += f", line_id='{line_id}'"

        self.logger.debug(f"❓ Checking DetailItems with ({search_criteria}).")
        self.logger.info(f"🔍 Searching DetailItems with {search_criteria}")

        with get_db_session() as session:
            try:
                query = (
                    session.query(DetailItem)
                        .join(PurchaseOrder, DetailItem.po_id == PurchaseOrder.id)
                        .join(Project, PurchaseOrder.project_id == Project.id)
                        .filter(Project.project_number == project_number)
                )

                if po_number:
                    query = query.filter(PurchaseOrder.po_number == po_number)
                if detail_number:
                    query = query.filter(DetailItem.detail_number == detail_number)
                if line_id:
                    query = query.filter(DetailItem.line_id == line_id)

                results = query.all()
                if not results:
                    self.logger.info("🙅 No DetailItems matched.")
                    return None
                elif len(results) == 1:
                    self.logger.info("✅ Found exactly 1 matching DetailItem.")
                    return self._serialize_record(results[0])
                else:
                    self.logger.info(f"✅ Found {len(results)} matching DetailItems. Returning list.")
                    return [self._serialize_record(r) for r in results]
            except Exception as e:
                session.rollback()
                self.logger.error(f"💥 Error searching DetailItems: {e}", exc_info=True)
                return None

    def search_detail_items_by_project_po_qty_rate(self, project_number: int, po_number: int, quantity: float,
                                                   rate: float):
        """
        Example method searching for detail items by sub_total = quantity*rate, under a project/PO.
        """
        self.logger.debug(
            f"search_detail_items_by_project_po_qty_rate with project_number={project_number}, po_number={po_number}, qty={quantity}, rate={rate}"
        )
        with get_db_session() as session:
            try:
                query = (
                    session.query(DetailItem)
                        .join(PurchaseOrder, DetailItem.po_id == PurchaseOrder.id)
                        .join(Project, PurchaseOrder.project_id == Project.id)
                        .filter(Project.project_number == project_number)
                        .filter(PurchaseOrder.po_number == po_number)
                        .filter(DetailItem.sub_total == quantity * rate)
                )

                results = query.all()
                if not results:
                    self.logger.info("No matching DetailItems found for that qty/rate.")
                    return None

                self.logger.info(f"Found {len(results)} matching DetailItems.")
                return [self._serialize_record(r) for r in results]
            except Exception as e:
                session.rollback()
                self.logger.error(f"Error searching DetailItems by qty/rate: {e}", exc_info=True)
                return None

    def create_detail_item_by_keys(
            self,
            project_number,
            po_number,
            detail_number,
            line_id,
            rate,
            quantity=1.0,
            ot=0.0,
            fringes=0.0,
            vendor=None,
            description=None,
            transaction_date=None,
            due_date=None,
            state=None,
            aicp_code=None,
            payment_type=None
    ):
        """
        Create a DetailItem by first looking up the PO in the same session, then committing.
        Avoids DetachedInstanceError.
        """
        self.logger.debug(
            f"🧱 create_detail_item_by_keys project_number='{project_number}', po_number='{po_number}', "
            f"detail_number='{detail_number}', line_id='{line_id}', rate={rate}, aicp_code={aicp_code}"
        )
        self.logger.info(
            f"🆕 Creating DetailItem for project_number={project_number}, po_number={po_number}, "
            f"detail_number={detail_number}, line_id={line_id}."
        )

        with get_db_session() as session:
            try:
                po = (
                    session.query(PurchaseOrder)
                        .join(Project, PurchaseOrder.project_id == Project.id)
                        .filter(Project.project_number == project_number)
                        .filter(PurchaseOrder.po_number == po_number)
                        .one_or_none()
                )
                if not po:
                    self.logger.warning(
                        f"💔 No PurchaseOrder for project_number={project_number}, po_number={po_number}."
                    )
                    session.rollback()
                    return None

                purchase_order_id = po.id

                # Possibly resolve aicp_code -> aicp_code_id
                aicp_code_id = None
                if aicp_code:
                    if isinstance(aicp_code, int):
                        aicp_code_id = aicp_code
                    elif isinstance(aicp_code, str):
                        self.logger.info(f"🔎 Searching or creating AicpCode '{aicp_code}'.")
                        found_code = self.search_aicp_codes(["aicp_code"], [aicp_code])
                        if not found_code:
                            self.logger.warning(
                                f"🦄 Not found, creating new AicpCode for '{aicp_code}'."
                            )
                            new_code = self.create_aicp_code(aicp_code=aicp_code, tax_id=1)
                            if new_code:
                                aicp_code_id = new_code["id"]
                            else:
                                self.logger.warning(
                                    f"⚠️ Could not create AicpCode for '{aicp_code}'. Aborting detail creation."
                                )
                                session.rollback()
                                return None
                        elif isinstance(found_code, list):
                            aicp_code_id = found_code[0]["id"]
                        else:
                            aicp_code_id = found_code["id"]
                    else:
                        self.logger.warning(
                            f"⚠️ 'aicp_code' param wasn't int or str. Value={aicp_code}"
                        )
                        aicp_code_id = None

                new_detail_item = DetailItem(
                    po_id=purchase_order_id,
                    detail_number=detail_number,
                    line_id=line_id,
                    rate=rate,
                    quantity=quantity,
                    ot=ot,
                    fringes=fringes,
                    vendor=vendor,
                    description=description,
                    transaction_date=transaction_date,
                    due_date=due_date,
                    payment_type=payment_type
                )
                if state:
                    new_detail_item.state = state
                if aicp_code_id:
                    new_detail_item.aicp_code_id = aicp_code_id

                session.add(new_detail_item)
                session.flush()
                self.logger.debug(f"🪄 Flushed new DetailItem. ID now: {new_detail_item.id}")

                session.commit()
                self.logger.info("🍀 Successfully created a new DetailItem!")
                return self._serialize_record(new_detail_item)
            except Exception as e:
                session.rollback()
                self.logger.error(f"💥 Error creating DetailItem: {e}", exc_info=True)
                return None

    def update_detail_item_by_keys(
            self,
            project_number,
            po_number,
            detail_number,
            line_id,
            **kwargs
    ):
        """
        🔄 Update a DetailItem by (project_number, po_number, detail_number, line_id).
        """
        self.logger.debug(
            f"🔧 update_detail_item_by_keys for project_number='{project_number}', po_number='{po_number}', "
            f"detail_number='{detail_number}', line_id='{line_id}'. Extra={kwargs}"
        )
        self.logger.info(
            f"🤝 Updating an existing DetailItem for project_number={project_number}, "
            f"po_number={po_number}, detail_number={detail_number}, line_id={line_id}"
        )

        with get_db_session() as session:
            try:
                di = (
                    session.query(DetailItem)
                        .join(PurchaseOrder, DetailItem.po_id == PurchaseOrder.id)
                        .join(Project, PurchaseOrder.project_id == Project.id)
                        .filter(Project.project_number == project_number)
                        .filter(PurchaseOrder.po_number == po_number)
                        .filter(DetailItem.detail_number == detail_number)
                        .filter(DetailItem.line_id == line_id)
                        .one_or_none()
                )
                if not di:
                    self.logger.info("🙅 That DetailItem doesn't exist. No update done.")
                    return None

                # If the user passes 'aicp_code', handle it
                if "aicp_code" in kwargs:
                    aicp_val = kwargs.pop("aicp_code")
                    if isinstance(aicp_val, int):
                        di.aicp_code_id = aicp_val
                    elif isinstance(aicp_val, str):
                        self.logger.info(f"🔎 Searching/creating AicpCode '{aicp_val}' during update.")
                        found_code = self.search_aicp_codes(["aicp_code"], [aicp_val])
                        if not found_code:
                            new_code = self.create_aicp_code(aicp_code=aicp_val, tax_id=1)
                            if new_code:
                                di.aicp_code_id = new_code["id"]
                            else:
                                self.logger.warning(
                                    f"⚠️ Could not create new AicpCode '{aicp_val}'. Rolling back."
                                )
                                session.rollback()
                                return None
                        elif isinstance(found_code, list):
                            di.aicp_code_id = found_code[0]["id"]
                        else:
                            di.aicp_code_id = found_code["id"]
                    else:
                        di.aicp_code_id = None
                        self.logger.warning(f"⚠️ 'aicp_code' param wasn't int or str, ignoring: {aicp_val}")

                for key, value in kwargs.items():
                    if hasattr(di, key):
                        setattr(di, key, value)
                    else:
                        self.logger.warning(f"⚠️ 'DetailItem' has no attribute '{key}'. Skipping.")

                session.flush()
                self.logger.debug(f"🪄 Flushed updated DetailItem(id={di.id}).")

                session.commit()
                self.logger.info("✅ The DetailItem has been updated successfully.")
                return self._serialize_record(di)
            except Exception as e:
                session.rollback()
                self.logger.error(f"💥 Updating the DetailItem error: {e}", exc_info=True)
                return None

    # -------------------------------------------------------------------------
    #  Basic CRUD for Contacts, Projects, etc.
    # -------------------------------------------------------------------------
    def find_contact_by_name(self, contact_name: str):
        self.logger.debug(f"🙋 Searching for contact by name='{contact_name}' in DB.")
        with get_db_session() as session:
            contact = session.query(Contact).filter_by(name=contact_name).one_or_none()
            return self._serialize_record(contact) if contact else None

    def create_minimal_contact(self, contact_name: str):
        self.logger.debug(f"🚀 Creating a minimal Contact with name='{contact_name}'")
        self.logger.info(f"🆕 Attempting to create a new Contact record for name='{contact_name}'.")
        with get_db_session() as session:
            try:
                new_contact = Contact(name=contact_name)
                session.add(new_contact)

                session.flush()
                self.logger.debug(f"🪄 Flushed new Contact. ID now: {new_contact.id}")

                session.commit()
                self.logger.info(f"💾 New Contact (id={new_contact.id}) saved.")
                return self._serialize_record(new_contact)
            except Exception as e:
                session.rollback()
                self.logger.error(f"💥 Couldn't create that Contact: {e}", exc_info=True)
                return None

    def update_contact_with_monday_data(
            self,
            contact_id: int,
            pulse_id: str = None,
            phone: str = None,
            email: str = None,
            address_line_1: str = None,
            address_line_2: str = None,
            city: str = None,
            zip_code: str = None,
            country: str = None,
            tax_type: str = None,
            tax_number: int = None,
            payment_details: str = None,
            vendor_status: str = None,
            tax_form_link: str = None,
    ) -> Optional[Dict[str, Any]]:
        """
        Update a Contact with fields from Monday. Only overwrites if a new (non-None) value is given.
        Returns the updated contact (dict) or None if not found.
        """
        self.logger.debug(f"🤝 update_contact_with_monday_data -> Contact id={contact_id}")
        with get_db_session() as session:
            contact = session.query(Contact).get(contact_id)
            if not contact:
                self.logger.warning(f"⚠️ No Contact with id={contact_id}. Nothing to update.")
                return None

            changed = False
            if pulse_id is not None:
                contact.pulse_id = pulse_id
                changed = True
            if phone is not None:
                contact.phone = phone
                changed = True
            if email is not None:
                contact.email = email
                changed = True
            if address_line_1 is not None:
                contact.address_line_1 = address_line_1
                changed = True
            if address_line_2 is not None:
                contact.address_line_2 = address_line_2
                changed = True
            if city is not None:
                contact.city = city
                changed = True
            if zip_code is not None:
                contact.zip = zip_code
                changed = True
            if country is not None:
                contact.country = country
                changed = True
            if tax_type is not None:
                contact.tax_type = tax_type
                changed = True
            if tax_number is not None:
                contact.tax_number = tax_number
                changed = True
            if payment_details is not None:
                contact.payment_details = payment_details
                changed = True
            if vendor_status is not None:
                contact.vendor_status = vendor_status
                changed = True
            if tax_form_link is not None:
                contact.tax_form_link = tax_form_link
                changed = True

            if changed:
                self.logger.info(f"🎨 Updating Contact (id={contact.id}) with new values from Monday.")
                session.flush()
                self.logger.debug(f"🪄 Flushed updated Contact(id={contact.id}).")

                session.commit()
                return self._serialize_record(contact)
            else:
                self.logger.debug(f"ℹ️ No changes provided, returning existing contact.")
                return self._serialize_record(contact)

    def parse_tax_number(self, tax_str: str):
        """
        Remove hyphens from SSN/EIN and parse as str. Return None if invalid.
        """
        self.logger.debug(f"🔢 Parsing tax number from '{tax_str}' by removing hyphens.")
        if not tax_str:
            return None
        cleaned = tax_str.replace("-", "")
        return cleaned or None

    def search_contacts(self, column_names=None, values=None):
        self.logger.debug(f"💼 Searching Contacts with column_names={column_names}, values={values}")
        return self._search_records(Contact, column_names, values)

    def create_contact(self, **kwargs):
        self.logger.debug(f"🙋 create_contact with {kwargs}")
        return self._create_record(Contact, **kwargs)

    def update_contact(self, contact_id, **kwargs):
        self.logger.debug(f"💁‍♀️ update_contact -> Contact(id={contact_id}), data={kwargs}")
        return self._update_record(Contact, contact_id, **kwargs)

    def search_projects(self, column_names, values):
        self.logger.debug(f"🏗 Searching Projects with columns={column_names}, values={values}")
        return self._search_records(Project, column_names, values)

    def create_project(self, **kwargs):
        self.logger.debug(f"🏗 create_project with {kwargs}")
        return self._create_record(Project, **kwargs)

    def update_project(self, project_id, **kwargs):
        self.logger.debug(f"🤖 update_project -> Project(id={project_id}), data={kwargs}")
        return self._update_record(Project, project_id, **kwargs)

    def search_purchase_orders(self, column_names, values):
        self.logger.debug(f"📝 search_purchase_orders: columns={column_names}, values={values}")
        return self._search_records(PurchaseOrder, column_names, values)

    def create_purchase_order(self, **kwargs):
        self.logger.debug(f"📝 create_purchase_order with {kwargs}")
        return self._create_record(PurchaseOrder, **kwargs)

    def update_purchase_order(self, po_id, **kwargs):
        self.logger.debug(f"📝 update_purchase_order -> PurchaseOrder(id={po_id}), data={kwargs}")
        return self._update_record(PurchaseOrder, po_id, **kwargs)

    def search_detail_items(self, column_names, values):
        self.logger.debug(f"🔎 search_detail_items: columns={column_names}, values={values}")
        return self._search_records(DetailItem, column_names, values)

    def create_detail_item(self, **kwargs):
        self.logger.debug(f"🧱 create_detail_item with {kwargs}")
        return self._create_record(DetailItem, **kwargs)

    def update_detail_item(self, detail_item_id, **kwargs):
        self.logger.debug(f"🔧 update_detail_item -> DetailItem(id={detail_item_id}), data={kwargs}")
        return self._update_record(DetailItem, detail_item_id, **kwargs)

    def search_bank_transactions(self, column_names, values):
        self.logger.debug(f"💰 search_bank_transactions: columns={column_names}, values={values}")
        return self._search_records(BankTransaction, column_names, values)

    def create_bank_transaction(self, **kwargs):
        self.logger.debug(f"💸 create_bank_transaction with {kwargs}")
        return self._create_record(BankTransaction, **kwargs)

    def update_bank_transaction(self, transaction_id, **kwargs):
        self.logger.debug(f"💸 update_bank_transaction -> BankTransaction(id={transaction_id}), data={kwargs}")
        return self._update_record(BankTransaction, transaction_id, **kwargs)

    def search_bill_line_items(self, column_names, values):
        self.logger.debug(f"📜 search_bill_line_items: columns={column_names}, values={values}")
        return self._search_records(BillLineItem, column_names, values)

    def create_bill_line_item(self, **kwargs):
        self.logger.debug(f"📜 create_bill_line_item with {kwargs}")
        return self._create_record(BillLineItem, **kwargs)

    def update_bill_line_item(self, bill_line_item_id, **kwargs):
        self.logger.debug(f"📜 update_bill_line_item -> BillLineItem(id={bill_line_item_id}), data={kwargs}")
        return self._update_record(BillLineItem, bill_line_item_id, **kwargs)

    def search_invoices(self, column_names, values):
        self.logger.debug(f"🧾 search_invoices: columns={column_names}, values={values}")
        return self._search_records(Invoice, column_names, values)

    def create_invoice(self, **kwargs):
        self.logger.debug(f"🧾 create_invoice with {kwargs}")
        return self._create_record(Invoice, **kwargs)

    def update_invoice(self, invoice_id, **kwargs):
        self.logger.debug(f"🧾 update_invoice -> Invoice(id={invoice_id}), data={kwargs}")
        return self._update_record(Invoice, invoice_id, **kwargs)

    def search_invoice_by_keys(self, project_number, po_number=None, invoice_number=None):
        """
        🔍 Search for Invoices by project_number & optional po_number/invoice_number.
        Returns None, single dict, or list of dicts.
        """
        search_criteria = f"project_number='{project_number}'"
        if po_number:
            search_criteria += f", po_number='{po_number}'"
        if invoice_number:
            search_criteria += f", invoice_number='{invoice_number}'"

        self.logger.debug(f"❓ Checking for Invoices with ({search_criteria}).")
        self.logger.info(f"🔎 Searching Invoices with {search_criteria}")

        with get_db_session() as session:
            try:
                query = (
                    session.query(Invoice)
                        .join(PurchaseOrder, Invoice.po_number == PurchaseOrder.po_number)
                        .join(Project, PurchaseOrder.project_id == Project.id)
                        .filter(Project.project_number == project_number)
                )
                if po_number:
                    query = query.filter(PurchaseOrder.po_number == po_number)
                if invoice_number:
                    query = query.filter(Invoice.invoice_number == invoice_number)

                results = query.all()
                if not results:
                    self.logger.info("🙅 No Invoices matched.")
                    return None
                elif len(results) == 1:
                    self.logger.info("✅ Found exactly 1 matching Invoice.")
                    return self._serialize_record(results[0])
                else:
                    self.logger.info(f"✅ Found {len(results)} matching Invoices. Returning list.")
                    return [self._serialize_record(r) for r in results]
            except Exception as e:
                session.rollback()
                self.logger.error(f"💥 Error searching Invoices: {e}", exc_info=True)
                return None

    def search_receipts(self, column_names, values):
        self.logger.debug(f"🧾 search_receipts: columns={column_names}, values={values}")
        return self._search_records(Receipt, column_names, values)

    def create_receipt(self, **kwargs):
        self.logger.debug(f"🧾 create_receipt with {kwargs}")
        return self._create_record(Receipt, **kwargs)

    def update_receipt_by_id(self, receipt_id, **kwargs):
        self.logger.debug(f"🧾 update_receipt_by_id -> Receipt(id={receipt_id}), data={kwargs}")
        return self._update_record(Receipt, receipt_id, **kwargs)

    def update_receipt_by_keys(self, project_number, po_number, detail_number, line_id, **kwargs):
        """
        Update the first matching Receipt via (project_number, po_number, detail_item_number, line_id).
        """
        self.logger.debug(
            f"🧾 update_receipt_by_keys (proj={project_number}, po={po_number}, detail={detail_number}, line={line_id}) => {kwargs}"
        )
        found = self.search_receipts(
            ["project_number", "po_number", "detail_number", "line_id"],
            [project_number, po_number, detail_number, line_id]
        )
        if not found:
            self.logger.warning("❌ No matching receipt found. Cannot update.")
            return None

        if isinstance(found, list):
            found = found[0]  # update the first match
        receipt_id = found["id"]

        return self._update_record(Receipt, receipt_id, **kwargs)

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
        self.logger.debug(
            f"💸 update_spend_money -> SpendMoney(id={spend_money_id}), data={kwargs}"
        )
        return self._update_record(SpendMoney, spend_money_id, **kwargs)

    def search_tax_accounts(self, column_names, values):
        self.logger.debug(f"🏦 search_tax_accounts with columns={column_names}, values={values}")
        return self._search_records(TaxAccount, column_names, values)

    def create_tax_account(self, **kwargs):
        self.logger.debug(f"🏦 create_tax_account with {kwargs}")
        return self._create_record(TaxAccount, **kwargs)

    def update_tax_account(self, tax_account_id, **kwargs):
        self.logger.debug(
            f"🏦 update_tax_account -> TaxAccount(id={tax_account_id}), data={kwargs}"
        )
        return self._update_record(TaxAccount, tax_account_id, **kwargs)

    def search_xero_bills(self, column_names, values):
        self.logger.debug(f"🏷 search_xero_bills with columns={column_names}, values={values}")
        return self._search_records(XeroBill, column_names, values)

    def create_xero_bill(self, **kwargs):
        self.logger.debug(f"🏷 create_xero_bill with {kwargs}")
        return self._create_record(XeroBill, **kwargs)

    def update_xero_bill(self, xero_bill_id, **kwargs):
        self.logger.debug(f"🏷 update_xero_bill -> XeroBill(id={xero_bill_id}), data={kwargs}")
        return self._update_record(XeroBill, xero_bill_id, **kwargs)