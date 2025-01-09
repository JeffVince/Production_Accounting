# -*- coding: utf-8 -*-
"""
database.database_uti.py

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


"""

from contextlib import contextmanager
from typing import Optional, Dict, Any

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
import logging

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
        """🗄 Serialize a record (SQLAlchemy model) into a dictionary of column names -> values."""
        if not record:
            return None
        record_values =  {c.name: getattr(record, c.name) for c in record.__table__.columns}
        self.logger.debug(f"🤓 Pulling record: {record_values['id']} from table {record.__table__}")
        return record_values

    def _search_records(self, model, column_names=None, values=None):
        """
        🔍 Search for records of a given model based on multiple column filters.
        If no column_names or values are provided, retrieves all records from the table.
        Returns:
            None if no records,
            a single dict if exactly one found,
            or a list if multiple found.
        Rolls back if an error occurs.
        """
        # Initialize column_names and values to empty lists if None
        column_names = column_names or []
        values = values or []

        if column_names and values:
            self.logger.debug(
                f"🕵️ We're looking into {model.__name__} with these filters: {list(zip(column_names, values))}")
            self.logger.info(
                f"🚦 Checking if there are any matches in {model.__name__} for columns & values: {list(zip(column_names, values))}")

            if len(column_names) != len(values):
                self.logger.warning(
                    "⚠️ Oops, mismatch: The number of column names and values do not match. Let's bail out.")
                return []
        else:
            self.logger.debug(f"🕵️ No filters provided. Retrieving all records from {model.__name__}.")
            self.logger.info(f"🚦 Fetching the entire {model.__name__} table without any filters.")

        with get_db_session() as session:
            try:
                query = session.query(model)

                # Dynamically build the query filters if any filters are provided
                if column_names and values:
                    for col_name, val in zip(column_names, values):
                        column_attr = getattr(model, col_name, None)
                        if column_attr is None:
                            self.logger.warning(
                                f"😬 Hmm, '{col_name}' is not a valid column in {model.__name__}. No searching possible.")
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
                    self.logger.info(f"✅ Located {len(records)} records! Let's bundle them all up.")
                    return [self._serialize_record(r) for r in records]

            except Exception as e:
                session.rollback()
                self.logger.error(f"💥 Well, that didn't go smoothly. Error searching {model.__name__}: {e}",
                                  exc_info=True)
                return []

    def _create_record(self, model, **kwargs):
        """
        🆕 Create a new record in the database.
        Returns the serialized new record, or None on error.
        Rolls back if an exception occurs.
        """
        self.logger.debug(f"🧑‍💻 Let's create a brand-new {model.__name__} using the following data: {kwargs}")
        self.logger.info(f"🌱 About to plant a fresh record in {model.__name__} with {kwargs}")
        with get_db_session() as session:
            try:
                record = model(**kwargs)
                session.add(record)
                session.commit()
                self.logger.info("🎉 Creation successful! Record is now alive in the DB.")
                return self._serialize_record(record)
            except Exception as e:
                session.rollback()
                self.logger.error(f"💥 Trouble creating {model.__name__}: {e}", exc_info=True)
                return None

    def _update_record(self, model: object, record_id: object, **kwargs: object) -> object:
        """
        🔄 Update an existing record by ID.
        Returns the serialized updated record, or None if not found or error.
        Rolls back if an exception occurs.
        """
        self.logger.debug(f"🔧 Attempting to update {model.__name__}(id={record_id}). Fields to change: {kwargs}")
        self.logger.info(f"🤝 Let's see if {model.__name__}(id={record_id}) exists. If so, we'll tweak it with {kwargs}.")
        with get_db_session() as session:
            try:
                record = session.query(model).get(record_id)
                if not record:
                    self.logger.info(f"🙅 Sorry, no {model.__name__} with id={record_id} was found in the DB.")
                    return None

                for key, value in kwargs.items():
                    if hasattr(record, key):
                        setattr(record, key, value)
                    else:
                        self.logger.warning(f"⚠️ The attribute '{key}' doesn't exist on {model.__name__}. We'll skip it.")

                session.commit()
                self.logger.info("✅ Done updating! The record is all set.")
                return self._serialize_record(record)
            except Exception as e:
                session.rollback()
                self.logger.error(f"💥 Had an issue updating {model.__name__}: {e}", exc_info=True)
                return None

    def _levenshtein_distance(self, s1: str, s2: str) -> int:
        """
        Optimized Levenshtein distance calculation with early rejection if
        the first letters differ. Returns the edit distance (number of single-character edits).

        Requirements / Assumptions:
          - If the first characters do not match, we immediately return
            len(s1) + len(s2) (a large distance), effectively filtering out
            strings that don't share the same first letter.
          - Otherwise, use an iterative DP approach for speed.
        """
        original_s1, original_s2 = s1, s2  # Keep for logging
        # Convert to lowercase for case-insensitive comparison
        s1 = s1.lower()
        s2 = s2.lower()

        # Early exit if first letters differ
        if s1 and s2 and s1[0] != s2[0]:
            dist = len(s1) + len(s2)
            self.logger.debug(
                f"🚫 First-letter mismatch: '{original_s1}' vs '{original_s2}' "
                f"(distance={dist})"
            )
            return dist

        # If either is empty, distance is the length of the other
        if not s1:
            dist = len(s2)
            # If both empty, it's distance=0 => exact match
            if dist == 0:
                self.logger.debug(
                    f"🟢 EXACT MATCH: '{original_s1}' vs '{original_s2}' -> distance=0"
                )
            else:
                self.logger.debug(
                    f"🔎 '{original_s1}' vs '{original_s2}' -> distance={dist} (one empty)"
                )
            return dist
        if not s2:
            dist = len(s1)
            # If both empty, it's distance=0 => exact match
            if dist == 0:
                self.logger.debug(
                    f"🟢 EXACT MATCH: '{original_s1}' vs '{original_s2}' -> distance=0"
                )
            else:
                self.logger.debug(
                    f"🔎 '{original_s1}' vs '{original_s2}' -> distance={dist} (one empty)"
                )
            return dist

        # Iterative dynamic programming
        m, n = len(s1), len(s2)
        # dp[i][j] = edit distance between s1[:i] and s2[:j]
        dp = [[0] * (n + 1) for _ in range(m + 1)]

        # Base cases: distance to transform from empty string
        for i in range(m + 1):
            dp[i][0] = i
        for j in range(n + 1):
            dp[0][j] = j

        for i in range(1, m + 1):
            for j in range(1, n + 1):
                cost = 0 if s1[i - 1] == s2[j - 1] else 1
                dp[i][j] = min(
                    dp[i - 1][j] + 1,  # deletion
                    dp[i][j - 1] + 1,  # insertion
                    dp[i - 1][j - 1] + cost  # substitution (if needed)
                )

        dist = dp[m][n]
        if dist == 0:
            # Perfect match
            self.logger.debug(
                f"🟢 EXACT MATCH: '{original_s1}' vs '{original_s2}' -> distance=0"
            )
        else:
            self.logger.debug(
                f"🔎 '{original_s1}' vs '{original_s2}' -> distance={dist}"
            )

        return dist

    def find_contact_close_match(
            self, contact_name: str, all_contacts, max_distance: int = 2
    ) -> dict:
        """
        Find an exact or "close" match for contact_name among the list of
        contacts. Returns the matched contact dict if found, else None.
        Logs if match is 'close'.
        """
        contact_name_lower = contact_name.lower()
        # 1) Try exact matches first
        for c in all_contacts:
            if c['name'].strip().lower() == contact_name_lower:
                self.logger.info(f"✅ Exact match found: '{c['name']}' for '{contact_name}'.")
                return c

        # 2) If no exact match, look for "close matches" within allowable distance
        best_candidate = None
        best_distance = max_distance + 1  # Initialize beyond the threshold

        for i, c in enumerate(all_contacts):
            if i % 50 == 0:
                self.logger.debug(
                    f"Comparing '{contact_name}' to '{c['name']}', index {i} of {len(all_contacts)}"
                )
            # Perform distance check
            current_distance = self._levenshtein_distance(contact_name_lower, c['name'].strip().lower())
            if current_distance < best_distance:
                best_distance = current_distance
                best_candidate = c

        if best_candidate and best_distance <= max_distance:
            self.logger.info(
                f"⚠️ Close match found for '{contact_name}' → '{best_candidate['name']}', "
                f"distance={best_distance}. Accepting as match."
            )
            return best_candidate

        # No match or close match found
        return None

    # -------------------------------------------------------------------------
    # AicpCodes
    # -------------------------------------------------------------------------
    def search_aicp_codes(self, column_names, values):
        """🔍 Search for AicpCode records by arbitrary columns (e.g. aicp_code)."""
        self.logger.debug("🔎 Searching for AicpCode entries. Let's see what we find!")
        return self._search_records(AicpCode, column_names, values)

    def create_aicp_code(self, **kwargs):
        """🆕 Create an AicpCode record."""
        self.logger.debug(f"🌈 Creating an AicpCode with these goodies: {kwargs}")
        return self._create_record(AicpCode, **kwargs)

    # -------------------------------------------------------------------------
    # PurchaseOrders by (project_number, po_number)
    # -------------------------------------------------------------------------
    def search_purchase_order_by_keys(self, project_number, po_number=None):
        """
        🔍 Search for PurchaseOrders based on provided keys.

        - If only project_number is provided, return all PurchaseOrders under that project.
        - If both project_number and po_number are provided, return PurchaseOrders matching both.

        Args:
            project_number (str): The project number to filter by.
            po_number (str, optional): The purchase order number to filter by.

        Returns:
            Serialized PurchaseOrder(s) or None if no matches found.
        """
        # Build the search criteria string for logging
        search_criteria = f"project_number='{project_number}'"
        if po_number:
            search_criteria += f", po_number='{po_number}'"

        self.logger.debug(f"🤔 Checking for PurchaseOrders with ({search_criteria}).")
        self.logger.info(f"🚦 Searching PurchaseOrders with {search_criteria}")

        with get_db_session() as session:
            try:
                # Start building the query with necessary joins
                query = (
                    session.query(PurchaseOrder)
                        .join(Project, PurchaseOrder.project_id == Project.id)
                        .filter(Project.project_number == project_number)
                )

                # Add filter for po_number if provided
                if po_number:
                    query = query.filter(PurchaseOrder.po_number == po_number)

                results = query.all()

                if not results:
                    self.logger.info("🙅 No PurchaseOrders matched the provided keys.")
                    return None
                elif len(results) == 1:
                    self.logger.info("🎯 Exactly ONE PurchaseOrder matched. Perfect!")
                    return self._serialize_record(results[0])
                else:
                    self.logger.info(f"🎯 Found {len(results)} matching PurchaseOrders. Returning list.")
                    return [self._serialize_record(r) for r in results]

            except Exception as e:
                session.rollback()
                self.logger.error(f"💥 Error searching for PurchaseOrders: {e}", exc_info=True)
                return None

    def create_purchase_order_by_keys(self, project_number, po_number, **kwargs):
        """
        🆕 Create a PurchaseOrder by specifying project_number, po_number, etc.
        We look up the `Project` row by project_number, then use project.id.
        """
        self.logger.debug(
            f"🚧 Let's build a new PurchaseOrder for project_number='{project_number}', po_number='{po_number}'. Extra data: {kwargs}"
        )
        self.logger.info(f"🌱 Creating a new PurchaseOrder for project_number={project_number}, po_number={po_number}")
        with get_db_session() as session:
            try:
                proj = (
                    session.query(Project)
                    .filter(Project.project_number == project_number)
                    .one_or_none()
                )
                if not proj:
                    self.logger.warning(
                        f"💔 Couldn't find a matching Project for project_number={project_number}. No PO creation possible."
                    )
                    session.rollback()
                    return None

                new_po = PurchaseOrder(project_id=proj.id, po_number=po_number, **kwargs)
                session.add(new_po)
                session.commit()
                self.logger.info("🎉 A brand-new PurchaseOrder is now saved!")
                return self._serialize_record(new_po)
            except Exception as e:
                session.rollback()
                self.logger.error(f"💥 Whoops, error while creating PurchaseOrder: {e}", exc_info=True)
                return None

    def update_purchase_order_by_keys(self, project_number, po_number, **kwargs):
        """
        🔄 Update a PurchaseOrder by (project_number, po_number).
        """
        self.logger.debug(f"🔧 Attempting to update PurchaseOrder for project_number='{project_number}', po_number='{po_number}'. Data: {kwargs}")
        self.logger.info(f"🛠 Checking if PurchaseOrder with project_number={project_number} and po_number={po_number} is around.")
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
                    self.logger.info("🙅 No PurchaseOrder found for those keys, so we can't update it.")
                    return None

                po_id = po.id
            except Exception as e:
                session.rollback()
                self.logger.error(f"💥 Had trouble looking up the PurchaseOrder for update: {e}", exc_info=True)
                return None

        return self._update_record(PurchaseOrder, record_id=po_id, **kwargs)

    # -------------------------------------------------------------------------
    # DetailItems by (project_number, po_number, detail_number, line_id)
    # -------------------------------------------------------------------------
    def search_detail_item_by_keys(self, project_number, po_number=None, detail_number=None, line_id=None):
        """
        🔍 Search for DetailItems based on provided keys.

        - If only project_number is provided, return all DetailItems under that project.
        - If project_number and po_number are provided, return all DetailItems under that Purchase Order.
        - If project_number, po_number, and detail_number are provided, return all DetailItems matching those.
        - If all four parameters are provided, return the specific DetailItem.

        Args:
            project_number (str): The project number to filter by.
            po_number (str, optional): The purchase order number to filter by.
            detail_number (str, optional): The detail number to filter by.
            line_id (str, optional): The line ID to filter by.

        Returns:
            Serialized DetailItem(s) or None if no matches found.
        """
        # Build the search criteria string for logging
        search_criteria = f"project_number='{project_number}'"
        if po_number:
            search_criteria += f", po_number='{po_number}'"
        if detail_number:
            search_criteria += f", detail_number='{detail_number}'"
        if line_id:
            search_criteria += f", line_id='{line_id}'"

        self.logger.debug(f"❓ Checking for DetailItems with ({search_criteria}).")
        self.logger.info(f"🔍 Searching DetailItems with {search_criteria}")

        with get_db_session() as session:
            try:
                # Start building the query with necessary joins
                query = (
                    session.query(DetailItem)
                        .join(PurchaseOrder, DetailItem.po_id == PurchaseOrder.id)
                        .join(Project, PurchaseOrder.project_id == Project.id)
                        .filter(Project.project_number == project_number)
                )

                # Add filters based on provided parameters
                if po_number:
                    query = query.filter(PurchaseOrder.po_number == po_number)
                if detail_number:
                    query = query.filter(DetailItem.detail_number == detail_number)
                if line_id:
                    query = query.filter(DetailItem.line_id == line_id)

                results = query.all()

                if not results:
                    self.logger.info("🙅 No DetailItems matched the provided keys.")
                    return None
                elif len(results) == 1:
                    self.logger.info("✅ Found exactly 1 matching DetailItem.")
                    return self._serialize_record(results[0])
                else:
                    self.logger.info(f"✅ Found {len(results)} matching DetailItems. Returning list.")
                    return [self._serialize_record(r) for r in results]

            except Exception as e:
                session.rollback()
                self.logger.error(f"💥 Error searching for DetailItems: {e}", exc_info=True)
                return None

    # In database.database_uti.py (within the DatabaseOperations class)

    def search_detail_items_by_project_po_qty_rate(self, project_number: int, po_number: int, quantity: float,
                                                   rate: float):
        """
        Search for DetailItems by matching the same project_number, po_number,
        quantity, and rate. Returns a list of matching DetailItems (serialized as dicts)
        or None if no matches.
        """
        self.logger.debug(
            f"search_detail_items_by_project_po_qty_rate called with "
            f"project_number={project_number}, po_number={po_number}, quantity={quantity}, rate={rate}"
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
                    self.logger.info(
                        f"No DetailItems found matching qty={quantity}, rate={rate} "
                        f"for project_number={project_number}, po_number={po_number}"
                    )
                    return None

                self.logger.info(
                    f"Found {len(results)} DetailItem(s) matching qty={quantity}, rate={rate}, "
                    f"project_number={project_number}, po_number={po_number}."
                )
                # Return a list of serialized records
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
        aicp_code=None
    ):
        """
        🆕 Create a DetailItem by looking up PurchaseOrder in the same session,
        storing po.id in a local variable, and then creating the DetailItem.
        This avoids DetachedInstanceError.

        `aicp_code` can be:
         1) int => actual AicpCode.id
         2) str => the `aicp_code` field in `AicpCode`. We'll find/create it.
         3) None => no aicp_code_id assigned.
        """
        self.logger.debug(
            f"🧱 create_detail_item_by_keys called with project_number='{project_number}', "
            f"po_number='{po_number}', detail_number='{detail_number}', line_id='{line_id}', "
            f"rate={rate}, aicp_code={aicp_code}"
        )
        self.logger.info(
            f"🆕 Creating a brand-new DetailItem for project_number={project_number}, po_number={po_number}, "
            f"detail_number={detail_number}, line_id={line_id}, with possible aicp_code={aicp_code}."
        )

        with get_db_session() as session:
            try:
                # 1) Lookup the PurchaseOrder in the SAME session
                po = (
                    session.query(PurchaseOrder)
                    .join(Project, PurchaseOrder.project_id == Project.id)
                    .filter(Project.project_number == project_number)
                    .filter(PurchaseOrder.po_number == po_number)
                    .one_or_none()
                )
                if not po:
                    self.logger.warning(
                        f"💔 No PurchaseOrder found for project_number={project_number}, po_number={po_number}. Can't proceed."
                    )
                    return None

                purchase_order_id = po.id

                # 2) Resolve aicp_code -> aicp_code_id
                aicp_code_id = None
                if aicp_code:
                    if isinstance(aicp_code, int):
                        aicp_code_id = aicp_code
                    elif isinstance(aicp_code, str):
                        self.logger.info(f"🔎 Let's find or create an AicpCode for '{aicp_code}'.")
                        found_code = self.search_aicp_codes(["aicp_code"], [aicp_code])
                        if not found_code:
                            self.logger.warning(f"🦄 Not found, so let's create a new AicpCode for '{aicp_code}'.")
                            new_code = self.create_aicp_code(aicp_code=aicp_code, tax_id=1)
                            if new_code:
                                aicp_code_id = new_code["id"]
                            else:
                                self.logger.warning(
                                    f"⚠️ Could not create AicpCode for '{aicp_code}'. Stopping detail creation."
                                )
                                return None
                        elif isinstance(found_code, list):
                            aicp_code_id = found_code[0]["id"]
                        else:
                            aicp_code_id = found_code["id"]
                    else:
                        self.logger.warning(
                            f"⚠️ The aicp_code param wasn't int or str, so ignoring. Value was: {aicp_code}"
                        )
                        aicp_code_id = None

                # 3) Create the DetailItem
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
                )
                if state:
                    new_detail_item.state = state
                if aicp_code_id:
                    new_detail_item.aicp_code_id = aicp_code_id

                session.add(new_detail_item)
                session.commit()
                self.logger.info("🍀 We successfully created a new DetailItem!")
                return self._serialize_record(new_detail_item)
            except Exception as e:
                session.rollback()
                self.logger.error(f"💥 Problem occurred while creating DetailItem: {e}", exc_info=True)
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
        Also uses a single session to avoid DetachedInstanceError.
        """
        self.logger.debug(
            f"🔧 update_detail_item_by_keys called for project_number='{project_number}', "
            f"po_number='{po_number}', detail_number='{detail_number}', line_id='{line_id}'. Extra: {kwargs}"
        )
        self.logger.info(
            f"🤝 We'll update an existing DetailItem for project_number={project_number}, "
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
                    self.logger.info("🙅 That DetailItem doesn't seem to exist. No update performed.")
                    return None

                # If the user passes 'aicp_code' in kwargs, do the resolution logic
                if "aicp_code" in kwargs:
                    aicp_val = kwargs.pop("aicp_code")
                    if isinstance(aicp_val, int):
                        di.aicp_code_id = aicp_val
                    elif isinstance(aicp_val, str):
                        self.logger.info(f"🔎 Searching for or creating AicpCode '{aicp_val}' during DetailItem update.")
                        found_code = self.search_aicp_codes(["aicp_code"], [aicp_val])
                        if not found_code:
                            new_code = self.create_aicp_code(aicp_code=aicp_val, tax_id=1)
                            if new_code:
                                di.aicp_code_id = new_code["id"]
                            else:
                                self.logger.warning(
                                    f"⚠️ Could not create new AicpCode '{aicp_val}' for this update. Rolling back."
                                )
                                session.rollback()
                                return None
                        elif isinstance(found_code, list):
                            di.aicp_code_id = found_code[0]["id"]
                        else:
                            di.aicp_code_id = found_code["id"]
                    else:
                        di.aicp_code_id = None
                        self.logger.warning(f"⚠️ 'aicp_code' param was neither int nor str, ignoring: {aicp_val}")

                # Apply other fields
                for key, value in kwargs.items():
                    if hasattr(di, key):
                        setattr(di, key, value)
                    else:
                        self.logger.warning(f"⚠️ 'DetailItem' has no attribute '{key}'. We'll ignore it.")

                session.commit()
                self.logger.info("✅ The DetailItem has been successfully updated.")
                return self._serialize_record(di)
            except Exception as e:
                session.rollback()
                self.logger.error(f"💥 Updating the DetailItem encountered an error: {e}", exc_info=True)
                return None

    # -------------------------------------------------------------------------
    # Basic CRUD for Contacts, Projects, POs, DetailItems, BankTransactions, etc.
    # -------------------------------------------------------------------------

    # -- Contacts
    def find_contact_by_name(self, contact_name: str):
        """
        Look up a Contact by name. Returns a dict of serialized fields or None.
        """
        self.logger.debug(f"🙋 Searching for contact by name='{contact_name}' in our DB.")
        with get_db_session() as session:
            contact = session.query(Contact).filter_by(name=contact_name).one_or_none()
            return self._serialize_record(contact) if contact else None

    def create_minimal_contact(self, contact_name: str):
        """
        Create a minimal Contact entry (just name). Returns a dict of serialized fields.
        """
        self.logger.debug(f"🚀 Let's create a minimal Contact with name='{contact_name}'")
        self.logger.info(f"🆕 Attempting to create a new Contact record for name='{contact_name}'.")
        with get_db_session() as session:
            try:
                new_contact = Contact(name=contact_name)
                session.add(new_contact)
                session.commit()
                self.logger.info(f"💾 A new Contact (id={new_contact.id}) was stored in the DB.")
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
        Update a Contact record with fields pulled from Monday’s API.
        Only overwrite fields if a new (non-None) value is provided.

        Returns:
            dict: Serialized Contact object after update.
            None: If no Contact is found or no changes were made.
        """
        self.logger.debug(
            f"🤝 We want to update Contact (id={contact_id}) with Monday data, let's do it carefully."
        )
        with get_db_session() as session:
            contact = session.query(Contact).get(contact_id)
            if not contact:
                self.logger.warning(
                    f"⚠️ We don't have a Contact record with id={contact_id}. Nothing to update."
                )
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
                self.logger.info(
                    f"🎨 Updating DB Contact (id={contact.id}) with these new values from Monday."
                )
                session.commit()
                self.logger.info(
                    f"✅ The Contact (id={contact.id}) is now refreshed with Monday's info!"
                )
                # Serialize and return the updated contact
                serialized_contact = self._serialize_record(contact)
                return serialized_contact
            else:
                self.logger.debug(
                    f"ℹ️ No changes were provided, so the Contact (id={contact.id}) remains as-is."
                )
                # Optionally, return the existing contact's serialized form
                serialized_contact = self._serialize_record(contact)
                return serialized_contact

    def parse_tax_number(self, tax_str: str):
        """
        Remove hyphens from SSN/EIN and parse as int.
        Returns None if parsing fails.
        """
        self.logger.debug(f"🔢 Let's parse tax number from '{tax_str}' by removing hyphens and converting to int.")
        if not tax_str:
            return None

        cleaned = tax_str.replace("-", "")
        try:
            return str(cleaned)
        except ValueError:
            self.logger.warning(f"⚠️ We couldn't parse '{tax_str}' into an integer. Possibly invalid format.")
            return None

    def search_contacts(self, column_names=None, values=None):
        self.logger.debug(f"💼 Searching for Contacts with column_names={column_names}, values={values}")
        return self._search_records(Contact, column_names, values)

    def create_contact(self, **kwargs):
        self.logger.debug(f"🙋 create_contact called with fields={kwargs}")
        return self._create_record(Contact, **kwargs)

    def update_contact(self, contact_id, **kwargs):
        self.logger.debug(f"💁‍♀️ update_contact: Let's update Contact(id={contact_id}) with {kwargs}")
        return self._update_record(Contact, contact_id, **kwargs)

    # -- Projects
    def search_projects(self, column_names, values):
        self.logger.debug(f"🏗 Searching for Projects with column_names={column_names}, values={values}")
        return self._search_records(Project, column_names, values)

    def create_project(self, **kwargs):
        self.logger.debug(f"🏗 create_project: Let's open a new Project with these details: {kwargs}")
        return self._create_record(Project, **kwargs)

    def update_project(self, project_id, **kwargs):
        self.logger.debug(f"🤖 update_project: Attempting to update Project(id={project_id}) with {kwargs}")
        return self._update_record(Project, project_id, **kwargs)

    # -- PurchaseOrders
    def search_purchase_orders(self, column_names, values):
        self.logger.debug(f"📝 search_purchase_orders: columns={column_names}, values={values}")
        return self._search_records(PurchaseOrder, column_names, values)

    def create_purchase_order(self, **kwargs):
        self.logger.debug(f"📝 create_purchase_order with {kwargs}")
        return self._create_record(PurchaseOrder, **kwargs)

    def update_purchase_order(self, po_id: object, **kwargs: object) -> object:
        self.logger.debug(f"📝 update_purchase_order called for id={po_id} with {kwargs}")
        return self._update_record(PurchaseOrder, po_id, **kwargs)

    # -- DetailItems
    def search_detail_items(self, column_names, values):
        self.logger.debug(f"🔎 search_detail_items with columns={column_names} and values={values}")
        return self._search_records(DetailItem, column_names, values)

    def create_detail_item(self, **kwargs):
        self.logger.debug(f"🧱 create_detail_item with {kwargs}")
        return self._create_record(DetailItem, **kwargs)

    def update_detail_item(self, detail_item_id, **kwargs):
        self.logger.debug(f"🔧 update_detail_item for id={detail_item_id}, data={kwargs}")
        return self._update_record(DetailItem, detail_item_id, **kwargs)

    # -- BankTransactions
    def search_bank_transactions(self, column_names, values):
        self.logger.debug(f"💰 search_bank_transactions: columns={column_names}, values={values}")
        return self._search_records(BankTransaction, column_names, values)

    def create_bank_transaction(self, **kwargs):
        self.logger.debug(f"💸 create_bank_transaction with {kwargs}")
        return self._create_record(BankTransaction, **kwargs)

    def update_bank_transaction(self, transaction_id, **kwargs):
        self.logger.debug(f"💸 update_bank_transaction for id={transaction_id} with {kwargs}")
        return self._update_record(BankTransaction, transaction_id, **kwargs)

    # -- BillLineItems
    def search_bill_line_items(self, column_names, values):
        self.logger.debug(f"📜 search_bill_line_items: columns={column_names}, values={values}")
        return self._search_records(BillLineItem, column_names, values)

    def create_bill_line_item(self, **kwargs):
        self.logger.debug(f"📜 create_bill_line_item with {kwargs}")
        return self._create_record(BillLineItem, **kwargs)

    def update_bill_line_item(self, bill_line_item_id, **kwargs):
        self.logger.debug(f"📜 update_bill_line_item for id={bill_line_item_id}, data={kwargs}")
        return self._update_record(BillLineItem, bill_line_item_id, **kwargs)

    # -- Invoices
    def search_invoices(self, column_names, values):
        self.logger.debug(f"🧾 search_invoices with columns={column_names} and values={values}")
        return self._search_records(Invoice, column_names, values)

    def create_invoice(self, **kwargs):
        self.logger.debug(f"🧾 create_invoice with {kwargs}")
        return self._create_record(Invoice, **kwargs)

    def update_invoice(self, invoice_id, **kwargs):
        self.logger.debug(f"🧾 update_invoice for id={invoice_id} with {kwargs}")
        return self._update_record(Invoice, invoice_id, **kwargs)

    def search_invoice_by_keys(self, project_number, po_number=None, invoice_number=None):
        """
        🔍 Search for Invoices based on provided keys.

        - If only project_number is provided, return all Invoices under that project.
        - If project_number and po_number are provided, return all Invoices under that PO.
        - If project_number, po_number, and invoice_number are provided, return just that Invoice.

        Args:
            project_number (str): The project number to filter by.
            po_number (str, optional): The purchase order number to filter by.
            invoice_number (int, optional): The invoice number to filter by.

        Returns:
            Serialized Invoice(s) or None if no matches found.
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
                    self.logger.info("🙅 No Invoices matched the provided keys.")
                    return None
                elif len(results) == 1:
                    self.logger.info("✅ Found exactly 1 matching Invoice.")
                    return self._serialize_record(results[0])
                else:
                    self.logger.info(f"✅ Found {len(results)} matching Invoices. Returning list.")
                    return [self._serialize_record(r) for r in results]

            except Exception as e:
                session.rollback()
                self.logger.error(f"💥 Error searching for Invoices: {e}", exc_info=True)
                return None

    # -- Receipts
    def search_receipts(self, column_names, values):
        self.logger.debug(f"🧾 search_receipts with columns={column_names}, values={values}")
        return self._search_records(Receipt, column_names, values)

    def create_receipt(self, **kwargs):
        self.logger.debug(f"🧾 create_receipt with {kwargs}")
        return self._create_record(Receipt, **kwargs)

    def update_receipt_by_id(self, receipt_id, **kwargs):
        self.logger.debug(f"🧾 update_receipt_by_id for id={receipt_id} with {kwargs}")
        return self._update_record(Receipt, receipt_id, **kwargs)

    def update_receipt_by_keys(self, project_number, po_number, detail_number, line_id, **kwargs):
        """
        Update the first matching Receipt via (project_number, po_number, detail_item_number),
        then pass its `id` to _update_record for the actual update.
        """
        self.logger.debug(
            f"🧾 update_receipt_by_keys for (project_number={project_number}, "
            f"po_number={po_number}, detail_item_number={detail_number}) with {kwargs}"
        )

        # 1) Find the matching receipt
        found = self.search_receipts(
            ["project_number", "po_number", "detail_number", "line_id"],
            [project_number, po_number, detail_number, line_id]
        )
        if not found:
            self.logger.warning("❌ No matching receipt found. Cannot update.")
            return None

        # Handle the possibility of multiple matches
        if isinstance(found, list):
            found = found[0]  # just update the first match

        receipt_id = found["id"]

        # 2) Perform the update using the exact same approach as update_receipt_by_id
        return self._update_record(Receipt, receipt_id, **kwargs)

    # -- SpendMoney
    def search_spend_money(self, column_names, values, deleted=False):
        self.logger.debug(f"💵 search_spend_money: columns={column_names}, values={values}, deleted={deleted}")

        records = self._search_records(SpendMoney, column_names, values)

        # If we received nothing back, just return it as is (None, empty dict, or empty list)
        if not records:
            return records

        # If the user wants to exclude deleted items
        if not deleted:
            # If it's a single dictionary
            if isinstance(records, dict):
                if records.get('status') == "DELETED":
                    return None  # or return {} if you prefer
            # If it's a list of dictionaries
            elif isinstance(records, list):
                records = [rec for rec in records if rec.get('status') != "DELETED"]

        return records

    def create_spend_money(self, **kwargs):
        self.logger.debug(f"💸 create_spend_money with {kwargs}")
        return self._create_record(SpendMoney, **kwargs)

    def update_spend_money(self, spend_money_id, **kwargs):
        self.logger.debug(f"💸 update_spend_money for id={spend_money_id}, data={kwargs}")
        return self._update_record(SpendMoney, spend_money_id, **kwargs)

    # -- TaxAccounts
    def search_tax_accounts(self, column_names, values):
        self.logger.debug(f"🏦 search_tax_accounts with columns={column_names}, values={values}")
        return self._search_records(TaxAccount, column_names, values)

    def create_tax_account(self, **kwargs):
        self.logger.debug(f"🏦 create_tax_account with {kwargs}")
        return self._create_record(TaxAccount, **kwargs)

    def update_tax_account(self, tax_account_id, **kwargs):
        self.logger.debug(f"🏦 update_tax_account for id={tax_account_id}, data={kwargs}")
        return self._update_record(TaxAccount, tax_account_id, **kwargs)

    # -- XeroBills
    def search_xero_bills(self, column_names, values):
        self.logger.debug(f"🏷 search_xero_bills with columns={column_names}, values={values}")
        return self._search_records(XeroBill, column_names, values)

    def create_xero_bill(self, **kwargs):
        self.logger.debug(f"🏷 create_xero_bill with {kwargs}")
        return self._create_record(XeroBill, **kwargs)

    def update_xero_bill(self, xero_bill_id, **kwargs):
        self.logger.debug(f"🏷 update_xero_bill for id={xero_bill_id} with {kwargs}")
        return self._update_record(XeroBill, xero_bill_id, **kwargs)