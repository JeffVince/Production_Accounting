"""
database/database_util.py

This module provides flexible, DRY (Don't Repeat Yourself) functions for searching,
creating, updating, deleting, and checking changes for various database records
using SQLAlchemy ORM with an optional session parameter. If no session is provided,
it uses `get_db_session()` from db_util to open/close a new session.
"""

from difflib import SequenceMatcher
from typing import Optional, Dict, Any, List, Union
import logging

from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

# Use the unified session pattern (get_db_session) instead of make_local_session
from database.db_util import get_db_session
from database.models import (
    Contact, Project, PurchaseOrder, DetailItem, BankTransaction,
    XeroBillLineItem, Invoice, AccountCode, Receipt, SpendMoney, TaxAccount,
    XeroBill, User, TaxLedger, BudgetMap, PoLog
)

class DatabaseOperations:
    """
    Provides methods to create, read, update, delete, and check changes
    in database records, handling concurrency safely via unique lookups.
    """

    def __init__(self):
        self.logger = logging.getLogger('database_logger')
        self.logger.debug("🌟 DatabaseOperations initialized.")

    # region 🛠️ Utility Functions
    def _serialize_record(self, record):
        """
        Converts a SQLAlchemy model record into a dict of column_name -> value.
        Returns None if record is None.
        """
        if not record:
            return None
        return {c.name: getattr(record, c.name) for c in record.__table__.columns}
    # endregion

    # region 🔎 Search Records
    def _search_records(
        self,
        model,
        column_names: Optional[List[str]] = None,
        values: Optional[List[Any]] = None,
        session: Session = None
    ) -> Union[None, Dict[str, Any], List[Dict[str, Any]]]:
        """
        Searches for records of a given model based on multiple column filters.
        Returns:
          - None if no records
          - A single dict if exactly one found
          - A list if multiple found
          - [] if mismatch or error
        """
        prefix = "[BATCH OPERATION] " if session else ""
        column_names = column_names or []
        values = values or []

        if column_names and values:
            self.logger.debug(f"{prefix}🕵️ Searching {model.__name__} with filters: {list(zip(column_names, values))}")
            self.logger.info(f"{prefix}🔎 Checking {model.__name__} for columns/values: {list(zip(column_names, values))}")
            if len(column_names) != len(values):
                self.logger.warning(f"{prefix}⚠️ Mismatch: columns vs. values. Returning empty list.")
                return []
        else:
            self.logger.debug(f"{prefix}🕵️ Searching all {model.__name__} records (no filters).")
            self.logger.info(f"{prefix}🔎 Fetching entire {model.__name__} table without filters.")

        # If a session was provided, use it, else open a new one with get_db_session().
        if session is not None:
            try:
                query = session.query(model)
                if column_names and values:
                    for col_name, val in zip(column_names, values):
                        column_attr = getattr(model, col_name, None)
                        if column_attr is None:
                            self.logger.warning(f"{prefix}😬 '{col_name}' invalid for {model.__name__}.")
                            return []
                        query = query.filter(column_attr == val)

                records = query.all()
                if not records:
                    self.logger.info(f"{prefix}🙅 No {model.__name__} records found.")
                    return None
                if len(records) == 1:
                    self.logger.info(f"{prefix}✅ Found a matching {model.__name__}.")
                    return self._serialize_record(records[0])
                else:
                    self.logger.info(f"{prefix}✅ Located {len(records)} {model.__name__} records.")
                    return [self._serialize_record(r) for r in records]

            except Exception as e:
                self.logger.error(f"{prefix}❌ Error searching {model.__name__}: {e}", exc_info=True)
                return []

        else:
            # No session => open a new one
            with get_db_session() as new_session:
                try:
                    query = new_session.query(model)
                    if column_names and values:
                        for col_name, val in zip(column_names, values):
                            column_attr = getattr(model, col_name, None)
                            if column_attr is None:
                                self.logger.warning(f"😬 '{col_name}' invalid for {model.__name__}.")
                                return []
                            query = query.filter(column_attr == val)

                    records = query.all()
                    if not records:
                        self.logger.info(f"🙅 No {model.__name__} records found.")
                        return None
                    if len(records) == 1:
                        self.logger.info("✅ Found exactly one match.")
                        return self._serialize_record(records[0])
                    else:
                        self.logger.info(f"✅ Found {len(records)} matches for {model.__name__}.")
                        return [self._serialize_record(r) for r in records]

                except Exception as e:
                    new_session.rollback()
                    self.logger.error(f"❌ Error searching {model.__name__}: {e}", exc_info=True)
                    return []
    # endregion

    # region 🆕 Create Records
    def _create_record(
        self,
        model,
        unique_lookup: dict = None,
        session: Session = None,
        **kwargs
    ) -> Optional[Dict[str, Any]]:
        """
        Creates a new record. Returns its dict form or None on error.
        Flushes to get the ID. If IntegrityError and unique_lookup is set,
        attempts concurrency fallback by re-querying.
        """
        prefix = "[BATCH OPERATION] " if session else ""
        self.logger.debug(f"{prefix}🧑‍💻 Creating {model.__name__} with: {kwargs}")
        self.logger.info(f"{prefix}🌱 Insert => {model.__name__} with {kwargs}")

        if session is not None:
            try:
                record = model(**kwargs)
                session.add(record)
                session.flush()
                self.logger.debug(f"{prefix}🎉 Flushed new {model.__name__}, ID={getattr(record, 'id', 'N/A')}")
                return self._serialize_record(record)

            except IntegrityError:
                self.logger.debug(f"{prefix}❗ IntegrityError creating {model.__name__}")
                session.rollback()
                if unique_lookup:
                    self.logger.warning(f"{prefix}🔎 Trying concurrency fallback re-query...")
                    found = self._search_records(model, list(unique_lookup.keys()), list(unique_lookup.values()), session=session)
                    if found:
                        if isinstance(found, list):
                            self.logger.info(f"{prefix}⚠️ Found {len(found)}; returning first.")
                            return found[0]
                        else:
                            self.logger.info(f"{prefix}⚠️ Found existing record after fallback.")
                            return found
                    else:
                        self.logger.error(f"{prefix}❌ No record found after fallback. Returning None.")
                        return None
                else:
                    self.logger.error(f"{prefix}❌ No unique_lookup => cannot fallback. Returning None.")
                return None

            except Exception as e:
                session.rollback()
                self.logger.error(f"{prefix}💥 Trouble creating {model.__name__}: {e}", exc_info=True)
                return None

        else:
            # No session => open a new one
            with get_db_session() as new_session:
                try:
                    record = model(**kwargs)
                    new_session.add(record)
                    new_session.flush()
                    new_session.commit()
                    self.logger.debug(f"🎉 Created {model.__name__}, ID={getattr(record, 'id', 'N/A')}")
                    return self._serialize_record(record)

                except IntegrityError:
                    new_session.rollback()
                    self.logger.debug("❗ IntegrityError on create.")
                    if unique_lookup:
                        self.logger.warning("🔎 Attempting concurrency fallback re-query...")
                        found = self._search_records(model, list(unique_lookup.keys()), list(unique_lookup.values()), session=new_session)
                        if found:
                            if isinstance(found, list):
                                self.logger.info(f"⚠️ Found {len(found)} matching; returning first.")
                                return found[0]
                            else:
                                self.logger.info("⚠️ Found exactly one after fallback.")
                                return found
                        else:
                            self.logger.error("❌ Nothing found after fallback.")
                            return None
                    else:
                        self.logger.error("❌ No unique_lookup => cannot fallback. Returning None.")
                    return None

                except Exception as e:
                    new_session.rollback()
                    self.logger.error(f"💥 Trouble creating {model.__name__}: {e}", exc_info=True)
                    return None
    # endregion

    # region ♻️ Update Records
    def _update_record(
        self,
        model,
        record_id: int,
        unique_lookup: dict = None,
        session: Session = None,
        **kwargs
    ) -> Optional[Dict[str, Any]]:
        """
        Updates an existing record by primary key. Returns updated record or None on error.
        If IntegrityError and unique_lookup is set, tries concurrency fallback.
        """
        prefix = "[BATCH OPERATION] " if session else ""
        self.logger.debug(f"{prefix}🔧 Updating {model.__name__}(id={record_id}) with {kwargs}")
        self.logger.info(f"{prefix}🤝 Checking & updating {model.__name__}(id={record_id}).")

        if session is not None:
            try:
                record = session.query(model).get(record_id)
                if not record:
                    self.logger.info(f"{prefix}🙅 No {model.__name__}(id={record_id}) found.")
                    return None

                for key, value in kwargs.items():
                    if hasattr(record, key):
                        setattr(record, key, value)
                    else:
                        self.logger.warning(f"{prefix}⚠️ '{key}' not on {model.__name__}. Skipping.")

                session.flush()
                self.logger.debug(f"{prefix}🎉 Flushed updated {model.__name__}(id={record_id}).")
                return self._serialize_record(record)

            except IntegrityError:
                self.logger.warning(f"{prefix}❗ IntegrityError on update {model.__name__}(id={record_id})")
                session.rollback()
                if unique_lookup:
                    self.logger.warning(f"{prefix}🔎 Concurrency fallback re-query...")
                    found = self._search_records(model, list(unique_lookup.keys()), list(unique_lookup.values()), session=session)
                    if found:
                        if isinstance(found, list):
                            self.logger.info(f"{prefix}⚠️ Found {len(found)}; returning first.")
                            return found[0]
                        else:
                            self.logger.info(f"{prefix}⚠️ Found exactly one after fallback.")
                            return found
                    else:
                        self.logger.error(f"{prefix}❌ No record found after fallback.")
                        return None
                else:
                    self.logger.error(f"{prefix}❌ No unique_lookup => cannot fallback.")
                return None

            except Exception as e:
                session.rollback()
                self.logger.error(f"{prefix}💥 Error updating {model.__name__}(id={record_id}): {e}", exc_info=True)
                return None

        else:
            with get_db_session() as new_session:
                try:
                    record = new_session.query(model).get(record_id)
                    if not record:
                        self.logger.info(f"🙅 No {model.__name__}(id={record_id}) found.")
                        return None

                    for key, value in kwargs.items():
                        if hasattr(record, key):
                            setattr(record, key, value)
                        else:
                            self.logger.warning(f"⚠️ '{key}' not on {model.__name__}. Skipping.")

                    new_session.flush()
                    new_session.commit()
                    self.logger.debug(f"🎉 Updated {model.__name__}(id={record_id}).")
                    return self._serialize_record(record)

                except IntegrityError:
                    new_session.rollback()
                    self.logger.warning("❗ IntegrityError on update.")
                    if unique_lookup:
                        self.logger.warning("🔎 Attempting fallback re-query after update fail...")
                        found = self._search_records(model, list(unique_lookup.keys()), list(unique_lookup.values()), session=new_session)
                        if found:
                            if isinstance(found, list):
                                self.logger.info(f"⚠️ Found {len(found)} matches; returning first.")
                                return found[0]
                            else:
                                self.logger.info("⚠️ Found exactly one record after fallback.")
                                return found
                        else:
                            self.logger.error("❌ No record found after fallback.")
                            return None
                    else:
                        self.logger.error("❌ No unique_lookup => cannot fallback. Returning None.")
                    return None

                except Exception as e:
                    new_session.rollback()
                    self.logger.error(f"💥 Error updating {model.__name__}(id={record_id}): {e}", exc_info=True)
                    return None
    # endregion

    # region 🗑️ Delete Records
    def _delete_record(
        self,
        model,
        record_id: int,
        unique_lookup: dict = None,
        session: Session = None
    ) -> bool:
        """
        Deletes an existing record by primary key. Returns True if deleted, False otherwise.
        If IntegrityError with unique_lookup, tries concurrency fallback.
        """
        prefix = "[BATCH OPERATION] " if session else ""
        self.logger.debug(f"{prefix}🗑️ Deleting {model.__name__}(id={record_id}).")

        if session is not None:
            try:
                record = session.query(model).get(record_id)
                if not record:
                    self.logger.info(f"{prefix}🙅 No {model.__name__}(id={record_id}) found to delete.")
                    return False

                session.delete(record)
                session.flush()
                self.logger.debug(f"{prefix}🗑️ {model.__name__}(id={record_id}) removed (pending commit).")
                return True  # caller can commit

            except IntegrityError:
                self.logger.warning(f"{prefix}❗ IntegrityError deleting {model.__name__}(id={record_id})")
                session.rollback()

                if unique_lookup:
                    self.logger.warning(f"{prefix}🔎 Attempting concurrency fallback re-query (post-delete).")
                    found = self._search_records(model, list(unique_lookup.keys()), list(unique_lookup.values()), session=session)
                    if found:
                        self.logger.info(f"{prefix}⚠️ Record still exists after fallback. Cannot delete.")
                        return False
                    else:
                        self.logger.error(f"{prefix}❌ Not found after fallback. Possibly already deleted.")
                        return False
                else:
                    self.logger.error(f"{prefix}❌ No unique_lookup => can't re-check.")
                return False

            except Exception as e:
                session.rollback()
                self.logger.error(f"{prefix}💥 Error deleting {model.__name__}(id={record_id}): {e}", exc_info=True)
                return False

        else:
            with get_db_session() as new_session:
                try:
                    record = new_session.query(model).get(record_id)
                    if not record:
                        self.logger.info(f"🙅 No {model.__name__}(id={record_id}) found to delete.")
                        return False

                    new_session.delete(record)
                    new_session.flush()
                    new_session.commit()
                    self.logger.debug(f"🗑️ Deleted {model.__name__}(id={record_id}).")
                    return True

                except IntegrityError:
                    new_session.rollback()
                    self.logger.warning("❗ IntegrityError on delete.")
                    if unique_lookup:
                        self.logger.warning("🔎 Attempting fallback re-query (post-delete error).")
                        found = self._search_records(model, list(unique_lookup.keys()), list(unique_lookup.values()), session=new_session)
                        if found:
                            self.logger.info(f"⚠️ Record still present => cannot delete.")
                            return False
                        else:
                            self.logger.error("❌ Not found after fallback => likely already deleted.")
                            return False
                    else:
                        self.logger.error("❌ No unique_lookup => no re-check. Returning False.")
                    return False

                except Exception as e:
                    new_session.rollback()
                    self.logger.error(f"💥 Error deleting {model.__name__}(id={record_id}): {e}", exc_info=True)
                    return False
    # endregion

    # region 🏷️ HAS-CHANGES UTILITY
    def _has_changes_for_record(
        self,
        model,
        record_id: Optional[int] = None,
        unique_filters: Dict[str, Any] = None,
        session: Session = None,
        **kwargs
    ) -> bool:
        """
        Checks if a record has different values than provided. If record_id is given,
        fetch by ID. Otherwise uses unique_filters. Compares each kwarg to the record.
        Returns True if differences are found, else False.
        """
        prefix = "[BATCH OPERATION] " if session else ""
        if not record_id and not unique_filters:
            self.logger.warning(f"{prefix}⚠️ No record_id or unique_filters => cannot check changes.")
            return False

        record_dict = None

        def fetch_record(s: Session):
            if record_id:
                rec_obj = s.query(model).get(record_id)
                return self._serialize_record(rec_obj) if rec_obj else None
            else:
                filters_list = list(unique_filters.keys())
                values_list = list(unique_filters.values())
                found = self._search_records(model, filters_list, values_list, session=s)
                if isinstance(found, dict):
                    return found
                elif isinstance(found, list) and len(found) == 1:
                    return found[0]
                return None

        if session:
            record_dict = fetch_record(session)
        else:
            with get_db_session() as new_session:
                record_dict = fetch_record(new_session)

        if not record_dict:
            self.logger.debug(f"{prefix}🙅 No single {model.__name__} found => no changes.")
            return False

        for field, new_val in kwargs.items():
            old_val = record_dict.get(field)
            # if you want to unify state comparisons:
            if field == 'state':
                old_val = (old_val or '').upper()
                new_val = (new_val or '').upper()

            if old_val != new_val:
                return True
        return False
    # endregion

    ########################################################################
    # BELOW: Region-based CRUD for each model
    ########################################################################

    # region 📁 ACCOUNT CODE
    def create_account_code(self, session: Session = None, **kwargs):
        unique_lookup = {}
        if 'code' in kwargs:
            unique_lookup['code'] = kwargs['code']
        return self._create_record(AccountCode, unique_lookup=unique_lookup, session=session, **kwargs)

    def search_account_codes(self, column_names, values, session: Session = None):
        return self._search_records(AccountCode, column_names, values, session=session)

    def update_account_code(self, account_code_id, session: Session = None, **kwargs):
        return self._update_record(AccountCode, account_code_id, session=session, **kwargs)

    def delete_account_code(self, account_code_id, session: Session = None, **kwargs) -> bool:
        unique_lookup = {}
        if 'code' in kwargs:
            unique_lookup['code'] = kwargs['code']
        return self._delete_record(AccountCode, account_code_id, unique_lookup=unique_lookup, session=session)

    def account_code_has_changes(self, record_id=None, code=None, session=None, **kwargs) -> bool:
        unique_filters = {}
        if code is not None:
            unique_filters['code'] = code
        return self._has_changes_for_record(
            AccountCode,
            record_id=record_id,
            unique_filters=unique_filters if not record_id else None,
            session=session,
            **kwargs
        )
    # endregion

    # region 📝 PURCHASE ORDER
    def create_purchase_order(self, session: Session = None, **kwargs):
        unique_lookup = {}
        if 'project_id' in kwargs and 'po_number' in kwargs:
            unique_lookup = {'project_id': kwargs['project_id'], 'po_number': kwargs['po_number']}
        return self._create_record(PurchaseOrder, unique_lookup=unique_lookup, session=session, **kwargs)

    def search_purchase_orders(self, column_names, values, session: Session = None):
        return self._search_records(PurchaseOrder, column_names, values, session=session)

    def update_purchase_order(self, po_id, session: Session = None, **kwargs):
        return self._update_record(PurchaseOrder, po_id, session=session, **kwargs)

    def delete_purchase_order(self, po_id, session: Session = None, **kwargs) -> bool:
        unique_lookup = {}
        if 'project_id' in kwargs and 'po_number' in kwargs:
            unique_lookup['project_id'] = kwargs['project_id']
            unique_lookup['po_number'] = kwargs['po_number']
        return self._delete_record(PurchaseOrder, po_id, unique_lookup=unique_lookup, session=session)

    def purchase_order_has_changes(self, record_id=None, project_number=None, po_number=None, session=None, **kwargs) -> bool:
        unique_filters = {}
        if project_number is not None:
            unique_filters['project_number'] = project_number
        if po_number is not None:
            unique_filters['po_number'] = po_number
        return self._has_changes_for_record(
            PurchaseOrder,
            record_id=record_id,
            unique_filters=unique_filters if not record_id else None,
            session=session,
            **kwargs
        )

    def search_purchase_order_by_keys(
        self,
        project_number: Optional[int] = None,
        po_number: Optional[int] = None,
        session: Session = None
    ) -> Union[None, Dict[str, Any], List[Dict[str, Any]]]:
        if not project_number and not po_number:
            return self.search_purchase_orders([], [], session=session)
        col_filters = []
        val_filters = []
        if project_number is not None:
            col_filters.append('project_number')
            val_filters.append(project_number)
        if po_number is not None:
            col_filters.append('po_number')
            val_filters.append(po_number)
        return self.search_purchase_orders(col_filters, val_filters, session=session)

    def create_purchase_order_by_keys(self, project_number: int, po_number: int, session: Session = None, **kwargs):
        project_record = self.search_projects(['project_number'], [project_number], session=session)
        if not project_record:
            return None
        if isinstance(project_record, list):
            project_record = project_record[0]
        project_id = project_record['id']
        kwargs.update({
            'project_id': project_id,
            'project_number': project_number,
            'po_number': po_number
        })
        return self.create_purchase_order(session=session, **kwargs)

    def update_purchase_order_by_keys(self, project_number: int, po_number: int, session: Session = None, **kwargs):
        pos = self.search_purchase_order_by_keys(project_number, po_number, session=session)
        if not pos:
            return None
        if isinstance(pos, list):
            first_po = pos[0]
        else:
            first_po = pos
        return self.update_purchase_order(first_po['id'], session=session, **kwargs)
    # endregion

    # region 🧱 DETAIL ITEM
    def create_detail_item(self, session: Session = None, **kwargs):
        unique_lookup = {}
        if 'po_number' in kwargs and 'project_number' in kwargs and 'detail_number' in kwargs and 'line_number' in kwargs:
            unique_lookup = {
                'project_number': kwargs['project_number'],
                'po_number': kwargs['po_number'],
                'detail_number': kwargs['detail_number'],
                'line_number': kwargs['line_number']
            }
        return self._create_record(DetailItem, unique_lookup=unique_lookup, session=session, **kwargs)

    def search_detail_items(self, column_names, values, session: Session = None):
        return self._search_records(DetailItem, column_names, values, session=session)

    def update_detail_item(self, detail_item_id, session: Session = None, **kwargs):
        return self._update_record(DetailItem, detail_item_id, session=session, **kwargs)

    def delete_detail_item(self, detail_item_id, session: Session = None, **kwargs) -> bool:
        unique_lookup = {}
        if 'project_number' in kwargs and 'po_number' in kwargs and 'detail_number' in kwargs and 'line_number' in kwargs:
            unique_lookup = {
                'project_number': kwargs['project_number'],
                'po_number': kwargs['po_number'],
                'detail_number': kwargs['detail_number'],
                'line_number': kwargs['line_number']
            }
        return self._delete_record(DetailItem, detail_item_id, unique_lookup=unique_lookup, session=session)

    def detail_item_has_changes(
        self,
        record_id: Optional[int] = None,
        project_number: Optional[int] = None,
        po_number: Optional[int] = None,
        detail_number: Optional[int] = None,
        line_number: Optional[int] = None,
        session: Session = None,
        **kwargs
    ) -> bool:
        unique_filters = {}
        if project_number is not None:
            unique_filters['project_number'] = project_number
        if po_number is not None:
            unique_filters['po_number'] = po_number
        if detail_number is not None:
            unique_filters['detail_number'] = detail_number
        if line_number is not None:
            unique_filters['line_number'] = line_number
        return self._has_changes_for_record(
            DetailItem,
            record_id=record_id,
            unique_filters=unique_filters if not record_id else None,
            session=session,
            **kwargs
        )

    def search_detail_item_by_keys(
        self,
        project_number: Optional[int] = None,
        po_number: Optional[int] = None,
        detail_number: Optional[int] = None,
        line_number: Optional[int] = None,
        session: Session = None
    ) -> Union[None, Dict[str, Any], List[Dict[str, Any]]]:
        if not project_number and not po_number and not detail_number and not line_number:
            return self.search_detail_items([], [], session=session)
        col_filters = []
        val_filters = []
        if project_number is not None:
            col_filters.append('project_number')
            val_filters.append(project_number)
        if po_number is not None:
            col_filters.append('po_number')
            val_filters.append(po_number)
        if detail_number is not None:
            col_filters.append('detail_number')
            val_filters.append(detail_number)
        if line_number is not None:
            col_filters.append('line_number')
            val_filters.append(line_number)
        return self.search_detail_items(col_filters, val_filters, session=session)

    def create_detail_item_by_keys(
        self,
        project_number: int,
        po_number: int,
        detail_number: int,
        line_number: int,
        session: Session = None,
        **kwargs
    ):
        kwargs.update({
            'project_number': project_number,
            'po_number': po_number,
            'detail_number': detail_number,
            'line_number': line_number
        })
        unique_lookup = {
            'project_number': project_number,
            'po_number': po_number,
            'detail_number': detail_number,
            'line_number': line_number
        }
        return self._create_record(DetailItem, unique_lookup=unique_lookup, session=session, **kwargs)

    def update_detail_item_by_keys(
        self,
        project_number: int,
        po_number: int,
        detail_number: int,
        line_number: int,
        session: Session = None,
        **kwargs
    ):
        matches = self.search_detail_item_by_keys(project_number, po_number, detail_number, line_number, session=session)
        if not matches:
            return None
        if isinstance(matches, list):
            match = matches[0]
        else:
            match = matches
        return self.update_detail_item(match['id'], session=session, **kwargs)
    # endregion

    # region 💼 CONTACT
    def create_contact(self, session: Session = None, **kwargs):
        unique_lookup = {}
        if 'name' in kwargs:
            unique_lookup['name'] = kwargs['name']
        return self._create_record(Contact, unique_lookup=unique_lookup, session=session, **kwargs)

    def search_contacts(self, column_names=None, values=None, session: Session = None):
        return self._search_records(Contact, column_names, values, session=session)

    def update_contact(self, contact_id, session: Session = None, **kwargs):
        return self._update_record(Contact, contact_id, session=session, **kwargs)

    def delete_contact(self, contact_id, session: Session = None, **kwargs) -> bool:
        unique_lookup = {}
        if 'name' in kwargs:
            unique_lookup['name'] = kwargs['name']
        return self._delete_record(Contact, contact_id, unique_lookup=unique_lookup, session=session)

    def contact_has_changes(self, record_id=None, name=None, email=None, session=None, **kwargs) -> bool:
        unique_filters = {}
        if name is not None:
            unique_filters['name'] = name
        if email is not None:
            unique_filters['email'] = email
        return self._has_changes_for_record(
            Contact,
            record_id=record_id,
            unique_filters=unique_filters if not record_id else None,
            session=session,
            **kwargs
        )

    def find_contact_close_match(self, contact_name: str, all_db_contacts: List[Dict[str, Any]]) -> Optional[
        List[Dict[str, Any]]]:
        """
        Finds contacts in all_db_contacts that have the same first character as contact_name and are at most one edit away.

        Args:
            contact_name (str): The name of the contact to match.
            all_db_contacts (List[Dict[str, Any]]): A list of contact dictionaries to search within.

        Returns:
            Optional[List[Dict[str, Any]]]: A list of matching contact dictionaries or None if no matches found.
        """
        if not all_db_contacts:
            self.logger.debug("🙅 No existing contacts to match.")
            return None

        matches = []
        contact_name_lower = contact_name.lower()
        first_char = contact_name_lower[0]

        for contact in all_db_contacts:
            existing_name = contact.get('name', '').strip()
            if not existing_name:
                continue

            existing_name_lower = existing_name.lower()

            # Enforce that the first character matches exactly
            if existing_name_lower[0] != first_char:
                continue

            if self._is_one_edit_away(contact_name_lower, existing_name_lower):
                self.logger.info(f"🤏 Found close match for contact: '{contact_name}'.")
                matches.append(contact)

        if matches:
            self.logger.info(f"✅ Found {len(matches)} matching contact(s) for '{contact_name}'.")
            return matches
        else:
            self.logger.info(f"🤷 No close matches found for '{contact_name}'.")
            return None

    def _is_one_edit_away(self, s1: str, s2: str) -> bool:
        """
        Determines if two strings are at most one edit away from each other.
        An edit is an insertion, deletion, or substitution of a single character.

        Args:
            s1 (str): The first string.
            s2 (str): The second string.

        Returns:
            bool: True if the strings are at most one edit away, False otherwise.
        """
        len1, len2 = len(s1), len(s2)

        # If the length difference is more than 1, they can't be one edit away
        if abs(len1 - len2) > 1:
            return False

        # Identify the shorter and longer string
        if len1 > len2:
            s1, s2 = s2, s1  # Ensure that s1 is the shorter string
            len1, len2 = len2, len1

        index1 = index2 = 0
        found_difference = False

        while index1 < len1 and index2 < len2:
            if s1[index1] != s2[index2]:
                if found_difference:
                    return False
                found_difference = True

                if len1 == len2:
                    # Move both pointers if lengths are equal (substitution)
                    index1 += 1
            else:
                index1 += 1  # Move shorter string pointer if characters match

            # Always move pointer for longer string
            index2 += 1

        return True

    def create_minimal_contact(self, contact_name: str, session: Session = None):
        return self.create_contact(name=contact_name, vendor_type='Vendor', session=session)
    # endregion

    # region 🏗️ PROJECT
    def create_project(self, session: Session = None, **kwargs):
        unique_lookup = {}
        if 'project_number' in kwargs:
            unique_lookup['project_number'] = kwargs['project_number']
        return self._create_record(Project, unique_lookup=unique_lookup, session=session, **kwargs)

    def search_projects(self, column_names: List[str], values: List[str], session: Session = None):
        return self._search_records(Project, column_names, values, session=session)

    def update_project(self, project_id, session: Session = None, **kwargs):
        return self._update_record(Project, project_id, session=session, **kwargs)

    def delete_project(self, project_id, session: Session = None, **kwargs) -> bool:
        unique_lookup = {}
        if 'project_number' in kwargs:
            unique_lookup['project_number'] = kwargs['project_number']
        return self._delete_record(Project, project_id, unique_lookup=unique_lookup, session=session)

    def project_has_changes(self, record_id=None, project_number=None, session=None, **kwargs) -> bool:
        unique_filters = {}
        if project_number is not None:
            unique_filters['project_number'] = project_number
        return self._has_changes_for_record(
            Project,
            record_id=record_id,
            unique_filters=unique_filters if not record_id else None,
            session=session,
            **kwargs
        )
    # endregion

    # region 💰 BANK TRANSACTION
    def create_bank_transaction(self, session: Session = None, **kwargs):
        return self._create_record(BankTransaction, session=session, **kwargs)

    def search_bank_transactions(self, column_names, values, session: Session = None):
        return self._search_records(BankTransaction, column_names, values, session=session)

    def update_bank_transaction(self, transaction_id, session: Session = None, **kwargs):
        return self._update_record(BankTransaction, transaction_id, session=session, **kwargs)

    def delete_bank_transaction(self, transaction_id, session: Session = None, **kwargs) -> bool:
        return self._delete_record(BankTransaction, transaction_id, unique_lookup=None, session=session)

    def bank_transaction_has_changes(self, record_id=None, transaction_id_xero=None, session=None, **kwargs) -> bool:
        unique_filters = {}
        if transaction_id_xero is not None:
            unique_filters['transaction_id_xero'] = transaction_id_xero
        return self._has_changes_for_record(
            BankTransaction,
            record_id=record_id,
            unique_filters=unique_filters if not record_id else None,
            session=session,
            **kwargs
        )
    # endregion

    # region 📜 BILL LINE ITEM
    def create_xero_bill_line_item(self, session: Session = None, **kwargs):
        unique_lookup = {}
        if 'parent_id' in kwargs and 'detail_number' in kwargs and 'line_number' in kwargs:
            unique_lookup = {
                'parent_id': kwargs['parent_id'],
                'detail_number': kwargs['detail_number'],
                'line_number': kwargs['line_number']
            }
        return self._create_record(XeroBillLineItem, unique_lookup=unique_lookup, session=session, **kwargs)

    def search_xero_bill_line_items(self, column_names, values, session: Session = None):
        return self._search_records(XeroBillLineItem, column_names, values, session=session)

    def update_xero_bill_line_item(self, xero_bill_line_item_id, session: Session = None, **kwargs):
        return self._update_record(XeroBillLineItem, xero_bill_line_item_id, session=session, **kwargs)

    def delete_xero_bill_line_item(self, xero_bill_line_item_id, session: Session = None, **kwargs) -> bool:
        unique_lookup = {}
        if 'parent_id' in kwargs and 'detail_number' in kwargs and 'line_number' in kwargs:
            unique_lookup = {
                'parent_id': kwargs['parent_id'],
                'detail_number': kwargs['detail_number'],
                'line_number': kwargs['line_number']
            }
        return self._delete_record(XeroBillLineItem, xero_bill_line_item_id, unique_lookup=unique_lookup, session=session)

    def xero_bill_line_item_has_changes(
        self,
        record_id: Optional[int] = None,
        parent_id: Optional[int] = None,
        detail_number: Optional[int] = None,
        line_number: Optional[int] = None,
        session: Session = None,
        **kwargs
    ) -> bool:
        unique_filters = {}
        if parent_id is not None:
            unique_filters['parent_id'] = parent_id
        if detail_number is not None:
            unique_filters['detail_number'] = detail_number
        if line_number is not None:
            unique_filters['line_number'] = line_number
        return self._has_changes_for_record(
            XeroBillLineItem,
            record_id=record_id,
            unique_filters=unique_filters if not record_id else None,
            session=session,
            **kwargs
        )

    def search_xero_bill_line_item_by_keys(
        self,
        project_number: Optional[int] = None,
        po_number: Optional[int] = None,
        detail_number: Optional[int] = None,
        line_number: Optional[int] = None,
        session: Session = None
    ) -> Union[None, Dict[str, Any], List[Dict[str, Any]]]:
        if not project_number and not po_number and not detail_number and not line_number:
            return self.search_xero_bill_line_items([], [], session=session)
        col_filters = []
        val_filters = []
        if project_number is not None:
            col_filters.append('project_number')
            val_filters.append(project_number)
        if po_number is not None:
            col_filters.append('po_number')
            val_filters.append(po_number)
        if detail_number is not None:
            col_filters.append('detail_number')
            val_filters.append(detail_number)
        if line_number is not None:
            col_filters.append('line_number')
            val_filters.append(line_number)
        return self.search_xero_bill_line_items(col_filters, val_filters, session=session)

    def create_xero_bill_line_item_by_keys(
        self,
        parent_id: int,
        project_number: int,
        po_number: int,
        detail_number: int,
        line_number: int,
        session: Session = None,
        **kwargs
    ):
        kwargs.update({
            'parent_id': parent_id,
            'project_number': project_number,
            'po_number': po_number,
            'detail_number': detail_number,
            'line_number': line_number
        })
        unique_lookup = {
            'parent_id': parent_id,
            'detail_number': detail_number,
            'line_number': line_number
        }
        return self._create_record(XeroBillLineItem, unique_lookup=unique_lookup, session=session, **kwargs)

    def update_xero_bill_line_item_by_keys(
        self,
        project_number: int,
        po_number: int,
        detail_number: int,
        line_number: int,
        session: Session = None,
        **kwargs
    ):
        matches = self.search_xero_bill_line_item_by_keys(project_number, po_number, detail_number, line_number, session=session)
        if not matches:
            return None
        if isinstance(matches, list):
            match = matches[0]
        else:
            match = matches
        return self.update_xero_bill_line_item(match['id'], session=session, **kwargs)
    # endregion

    # region 🧾 INVOICE
    def create_invoice(self, session: Session = None, **kwargs):
        unique_lookup = {}
        if 'invoice_number' in kwargs:
            unique_lookup['invoice_number'] = kwargs['invoice_number']
        return self._create_record(Invoice, unique_lookup=unique_lookup, session=session, **kwargs)

    def search_invoices(self, column_names, values, session: Session = None):
        return self._search_records(Invoice, column_names, values, session=session)

    def update_invoice(self, invoice_id, session: Session = None, **kwargs):
        return self._update_record(Invoice, invoice_id, session=session, **kwargs)

    def delete_invoice(self, invoice_id, session: Session = None, **kwargs) -> bool:
        unique_lookup = {}
        if 'invoice_number' in kwargs:
            unique_lookup['invoice_number'] = kwargs['invoice_number']
        return self._delete_record(Invoice, invoice_id, unique_lookup=unique_lookup, session=session)

    def invoice_has_changes(self, record_id=None, invoice_number=None, session=None, **kwargs) -> bool:
        unique_filters = {}
        if invoice_number is not None:
            unique_filters['invoice_number'] = invoice_number
        return self._has_changes_for_record(
            Invoice,
            record_id=record_id,
            unique_filters=unique_filters if not record_id else None,
            session=session,
            **kwargs
        )

    def search_invoice_by_keys(
        self,
        project_number: Optional[int] = None,
        po_number: Optional[int] = None,
        invoice_number: Optional[int] = None,
        session: Session = None
    ) -> Union[None, Dict[str, Any], List[Dict[str, Any]]]:
        if not project_number and not po_number and not invoice_number:
            return self.search_invoices([], [], session=session)
        col_filters = []
        val_filters = []
        if project_number is not None:
            col_filters.append('project_number')
            val_filters.append(project_number)
        if po_number is not None:
            col_filters.append('po_number')
            val_filters.append(po_number)
        if invoice_number is not None:
            col_filters.append('invoice_number')
            val_filters.append(invoice_number)
        return self.search_invoices(col_filters, val_filters, session=session)
    # endregion

    # region 🧾 RECEIPT
    def create_receipt(self, session: Session = None, **kwargs):
        return self._create_record(Receipt, session=session, **kwargs)

    def search_receipts(self, column_names, values, session: Session = None):
        return self._search_records(Receipt, column_names, values, session=session)

    def update_receipt_by_id(self, receipt_id, session: Session = None, **kwargs):
        return self._update_record(Receipt, receipt_id, session=session, **kwargs)

    def delete_receipt_by_id(self, receipt_id, session: Session = None, **kwargs) -> bool:
        return self._delete_record(Receipt, receipt_id, unique_lookup=None, session=session)

    def receipt_has_changes(
        self,
        record_id: Optional[int] = None,
        project_number: Optional[int] = None,
        po_number: Optional[int] = None,
        detail_number: Optional[int] = None,
        line_number: Optional[int] = None,
        session: Session = None,
        **kwargs
    ) -> bool:
        unique_filters = {}
        if project_number is not None:
            unique_filters['project_number'] = project_number
        if po_number is not None:
            unique_filters['po_number'] = po_number
        if detail_number is not None:
            unique_filters['detail_number'] = detail_number
        if line_number is not None:
            unique_filters['line_number'] = line_number
        return self._has_changes_for_record(
            Receipt,
            record_id=record_id,
            unique_filters=unique_filters if not record_id else None,
            session=session,
            **kwargs
        )

    def search_receipt_by_keys(
        self,
        project_number: Optional[int] = None,
        po_number: Optional[int] = None,
        detail_number: Optional[int] = None,
        line_number: Optional[int] = None,
        session: Session = None
    ):
        if not project_number and not po_number and not detail_number and not line_number:
            return self.search_receipts([], [], session=session)
        col_filters = []
        val_filters = []
        if project_number is not None:
            col_filters.append('project_number')
            val_filters.append(project_number)
        if po_number is not None:
            col_filters.append('po_number')
            val_filters.append(po_number)
        if detail_number is not None:
            col_filters.append('detail_number')
            val_filters.append(detail_number)
        if line_number is not None:
            col_filters.append('line_number')
            val_filters.append(line_number)
        return self.search_receipts(col_filters, val_filters, session=session)

    def create_receipt_by_keys(
        self,
        project_number: int,
        po_number: int,
        detail_number: int,
        line_number: int,
        session: Session = None,
        **kwargs
    ):
        kwargs.update({
            'project_number': project_number,
            'po_number': po_number,
            'detail_number': detail_number,
            'line_number': line_number
        })
        return self.create_receipt(session=session, **kwargs)

    def update_receipt_by_keys(
        self,
        project_number: int,
        po_number: int,
        detail_number: int,
        line_number: int,
        session: Session = None,
        **kwargs
    ):
        recs = self.search_receipt_by_keys(project_number, po_number, detail_number, line_number, session=session)
        if not recs:
            return None
        if isinstance(recs, list):
            recs = recs[0]
        return self.update_receipt_by_id(recs['id'], session=session, **kwargs)
    # endregion

    # region 💵 SPEND MONEY
    def create_spend_money(self, session: Session = None, **kwargs):
        return self._create_record(SpendMoney, session=session, **kwargs)

    def search_spend_money(self, column_names, values, deleted=False, session: Session = None):
        prefix = "[BATCH OPERATION] " if session else ""
        self.logger.debug(f"{prefix}💵 Searching SpendMoney columns={column_names}, vals={values}, deleted={deleted}")
        records = self._search_records(SpendMoney, column_names, values, session=session)
        if not records:
            return records
        # filter out state=DELETED if not asked for deleted
        if not deleted:
            if isinstance(records, dict):
                if records.get('state') == 'DELETED':
                    return None
            elif isinstance(records, list):
                records = [rec for rec in records if rec.get('state') != 'DELETED']
        return records

    def update_spend_money(self, spend_money_id, session: Session = None, **kwargs):
        return self._update_record(SpendMoney, spend_money_id, session=session, **kwargs)

    def delete_spend_money(self, spend_money_id, session: Session = None, **kwargs) -> bool:
        return self._delete_record(SpendMoney, spend_money_id, unique_lookup=None, session=session)

    def spend_money_has_changes(
        self,
        record_id: Optional[int] = None,
        project_number: Optional[int] = None,
        po_number: Optional[int] = None,
        detail_number: Optional[int] = None,
        line_number: Optional[int] = None,
        session: Session = None,
        **kwargs
    ) -> bool:
        unique_filters = {}
        if project_number is not None:
            unique_filters['project_number'] = project_number
        if po_number is not None:
            unique_filters['po_number'] = po_number
        if detail_number is not None:
            unique_filters['detail_number'] = detail_number
        if line_number is not None:
            unique_filters['line_number'] = line_number
        return self._has_changes_for_record(
            SpendMoney,
            record_id=record_id,
            unique_filters=unique_filters if not record_id else None,
            session=session,
            **kwargs
        )

    def search_spend_money_by_keys(
        self,
        project_number: Optional[int] = None,
        po_number: Optional[int] = None,
        detail_number: Optional[int] = None,
        line_number: Optional[int] = None,
        deleted: bool = False,
        session: Session = None
    ):
        if not project_number and not po_number and not detail_number and not line_number:
            return self.search_spend_money([], [], deleted=deleted, session=session)
        col_filters = []
        val_filters = []
        if project_number is not None:
            col_filters.append('project_number')
            val_filters.append(project_number)
        if po_number is not None:
            col_filters.append('po_number')
            val_filters.append(po_number)
        if detail_number is not None:
            col_filters.append('detail_number')
            val_filters.append(detail_number)
        if line_number is not None:
            col_filters.append('line_number')
            val_filters.append(line_number)
        return self.search_spend_money(col_filters, val_filters, deleted=deleted, session=session)

    def create_spend_money_by_keys(
        self,
        project_number: int,
        po_number: int,
        detail_number: int,
        line_number: int,
        session: Session = None,
        **kwargs
    ):
        kwargs.update({
            'project_number': project_number,
            'po_number': po_number,
            'detail_number': detail_number,
            'line_number': line_number
        })
        unique_lookup = {
            'project_number': project_number,
            'po_number': po_number,
            'detail_number': detail_number,
            'line_number': line_number
        }
        return self._create_record(SpendMoney, unique_lookup=unique_lookup, session=session, **kwargs)

    def update_spend_money_by_keys(
        self,
        project_number: int,
        po_number: int,
        detail_number: int,
        line_number: int,
        session: Session = None,
        **kwargs
    ):
        recs = self.search_spend_money_by_keys(project_number, po_number, detail_number, line_number, session=session)
        if not recs:
            return None
        if isinstance(recs, list):
            recs = recs[0]
        return self.update_spend_money(recs['id'], session=session, **kwargs)
    # endregion

    # region 🏦 TAX ACCOUNT
    def create_tax_account(self, session: Session = None, **kwargs):
        return self._create_record(TaxAccount, session=session, **kwargs)

    def search_tax_accounts(self, column_names, values, session: Session = None):
        return self._search_records(TaxAccount, column_names, values, session=session)

    def update_tax_account(self, tax_account_id, session: Session = None, **kwargs):
        return self._update_record(TaxAccount, tax_account_id, session=session, **kwargs)

    def delete_tax_account(self, tax_account_id, session: Session = None, **kwargs) -> bool:
        return self._delete_record(TaxAccount, tax_account_id, unique_lookup=None, session=session)

    def tax_account_has_changes(self, record_id=None, tax_code=None, session=None, **kwargs) -> bool:
        unique_filters = {}
        if tax_code is not None:
            unique_filters['tax_code'] = tax_code
        return self._has_changes_for_record(
            TaxAccount,
            record_id=record_id,
            unique_filters=unique_filters if not record_id else None,
            session=session,
            **kwargs
        )
    # endregion

    # region 🏷 XERO BILL

    def create_xero_bill(self, project_number, po_number, detail_number, session: Session = None, **kwargs):
        kwargs['project_number'] = project_number
        kwargs['po_number'] = po_number
        kwargs['detail_number'] = detail_number
        unique_lookup = {
            'project_number': project_number,
            'po_number': po_number,
            'detail_number': detail_number
        }
        return self._create_record(XeroBill, unique_lookup=unique_lookup, session=session, **kwargs)

    def search_xero_bills(self, column_names, values, session: Session = None):
        return self._search_records(XeroBill, column_names, values, session=session)

    def update_xero_bill(self, xero_bill_id, session: Session = None, **kwargs):
        return self._update_record(XeroBill, xero_bill_id, session=session, **kwargs)

    def delete_xero_bill(self, xero_bill_id, session: Session = None, **kwargs) -> bool:
        return self._delete_record(XeroBill, xero_bill_id, unique_lookup=None, session=session)

    def xero_bill_has_changes(
            self,
            record_id=None,
            project_number=None,
            po_number=None,
            detail_number=None,
            session=None,
            **kwargs
    ) -> bool:
        unique_filters = {}
        if project_number is not None:
            unique_filters['project_number'] = project_number
        if po_number is not None:
            unique_filters['po_number'] = po_number
        if detail_number is not None:
            unique_filters['detail_number'] = detail_number
        return self._has_changes_for_record(
            XeroBill,
            record_id=record_id,
            unique_filters=unique_filters if not record_id else None,
            session=session,
            **kwargs
        )

    def search_xero_bill_by_keys(self, project_number=None, po_number=None, detail_number=None, session=None):
        if not project_number and not po_number and not detail_number:
            return self.search_xero_bills([], [], session=session)
        col_filters = []
        val_filters = []
        if project_number is not None:
            col_filters.append('project_number')
            val_filters.append(project_number)
        if po_number is not None:
            col_filters.append('po_number')
            val_filters.append(po_number)
        if detail_number is not None:
            col_filters.append('detail_number')
            val_filters.append(detail_number)
        return self.search_xero_bills(col_filters, val_filters, session=session)

    def create_xero_bill_by_keys(self, project_number, po_number, detail_number, session=None, **kwargs):
        kwargs['project_number'] = project_number
        kwargs['po_number'] = po_number
        kwargs['detail_number'] = detail_number
        unique_lookup = {
            'project_number': project_number,
            'po_number': po_number,
            'detail_number': detail_number
        }
        return self._create_record(XeroBill, unique_lookup=unique_lookup, session=session, **kwargs)

    def update_xero_bill_by_keys(self, project_number, po_number, detail_number, session=None, **kwargs):
        bills = self.search_xero_bill_by_keys(project_number, po_number, detail_number, session=session)
        if not bills:
            return None
        if isinstance(bills, list):
            bills = bills[0]
        return self.update_xero_bill(bills['id'], session=session, **kwargs)




    # endregion

    # region 👤 USER
    def create_user(self, session: Session = None, **kwargs):
        unique_lookup = {}
        return self._create_record(User, unique_lookup=unique_lookup, session=session, **kwargs)

    def search_users(self, column_names=None, values=None, session: Session = None):
        return self._search_records(User, column_names, values, session=session)

    def update_user(self, user_id, session: Session = None, **kwargs):
        return self._update_record(User, user_id, session=session, **kwargs)

    def delete_user(self, user_id, session: Session = None, **kwargs) -> bool:
        return self._delete_record(User, user_id, unique_lookup=None, session=session)

    def user_has_changes(self, record_id=None, username=None, session=None, **kwargs) -> bool:
        unique_filters = {}
        if username is not None:
            unique_filters['username'] = username
        return self._has_changes_for_record(
            User,
            record_id=record_id,
            unique_filters=unique_filters if not record_id else None,
            session=session,
            **kwargs
        )
    # endregion

    # region 📒 TAX LEDGER
    def create_tax_ledger(self, session: Session = None, **kwargs):
        return self._create_record(TaxLedger, unique_lookup={}, session=session, **kwargs)

    def search_tax_ledgers(self, column_names=None, values=None, session: Session = None):
        return self._search_records(TaxLedger, column_names, values, session=session)

    def update_tax_ledger(self, ledger_id, session: Session = None, **kwargs):
        return self._update_record(TaxLedger, ledger_id, unique_lookup=None, session=session, **kwargs)

    def delete_tax_ledger(self, ledger_id, session: Session = None, **kwargs) -> bool:
        return self._delete_record(TaxLedger, ledger_id, unique_lookup=None, session=session)

    def tax_ledger_has_changes(self, record_id=None, name=None, session=None, **kwargs) -> bool:
        unique_filters = {}
        if name is not None:
            unique_filters['name'] = name
        return self._has_changes_for_record(
            TaxLedger,
            record_id=record_id,
            unique_filters=unique_filters if not record_id else None,
            session=session,
            **kwargs
        )
    # endregion

    # region 📊 BUDGET MAP
    def create_budget_map(self, session: Session = None, **kwargs):
        return self._create_record(BudgetMap, unique_lookup={}, session=session, **kwargs)

    def search_budget_maps(self, column_names=None, values=None, session: Session = None):
        return self._search_records(BudgetMap, column_names, values, session=session)

    def update_budget_map(self, map_id, session: Session = None, **kwargs):
        return self._update_record(BudgetMap, map_id, unique_lookup=None, session=session, **kwargs)

    def delete_budget_map(self, map_id, session: Session = None, **kwargs) -> bool:
        return self._delete_record(BudgetMap, map_id, unique_lookup=None, session=session)

    def budget_map_has_changes(self, record_id=None, map_name=None, session=None, **kwargs) -> bool:
        unique_filters = {}
        if map_name is not None:
            unique_filters['map_name'] = map_name
        return self._has_changes_for_record(
            BudgetMap,
            record_id=record_id,
            unique_filters=unique_filters if not record_id else None,
            session=session,
            **kwargs
        )
    # endregion

    # region 📝 PO LOG
    def create_po_log(self, session: Session = None, **kwargs):
        """
        Creates a new PoLog record in the DB.
        Returns the created record as a dict or None on failure.
        """
        unique_lookup = {}
        if 'db_path' in kwargs:
            unique_lookup['db_path'] = kwargs['db_path']
        return self._create_record(PoLog, unique_lookup=unique_lookup, session=session, **kwargs)

    def search_po_logs(self, column_names=None, values=None, session: Session = None):
        """
        Searches PoLog records with optional filters.
        Returns None, a dict, or a list of dicts.
        """
        return self._search_records(PoLog, column_names, values, session=session)

    def update_po_log(self, po_log_id, session: Session = None, **kwargs):
        """
        Updates an existing PoLog by ID. Returns updated dict or None on failure.
        """
        return self._update_record(PoLog, po_log_id, session=session, **kwargs)

    def delete_po_log(self, po_log_id, session: Session = None, **kwargs) -> bool:
        """
        Deletes a PoLog by ID. Returns True if deleted, False otherwise.
        """
        unique_lookup = {}
        if 'db_path' in kwargs:
            unique_lookup['db_path'] = kwargs['db_path']
        return self._delete_record(PoLog, po_log_id, unique_lookup=unique_lookup, session=session)

    def po_log_has_changes(
        self,
        record_id: Optional[int] = None,
        project_number: Optional[int] = None,
        filename: Optional[str] = None,
        db_path: Optional[str] = None,
        status: Optional[str] = None,
        session: Session = None,
        **kwargs
    ) -> bool:
        """
        Checks if a PoLog record has changed.
        """
        unique_filters = {}
        if db_path is not None:
            unique_filters['db_path'] = db_path
        return self._has_changes_for_record(
            PoLog,
            record_id=record_id,
            unique_filters=unique_filters if not record_id else None,
            session=session,
            project_number=project_number,
            filename=filename,
            db_path=db_path,
            status=status,
            **kwargs
        )

    def search_po_log_by_keys(
        self,
        project_number: Optional[int] = None,
        filename: Optional[str] = None,
        db_path: Optional[str] = None,
        status: Optional[str] = None,
        session: Session = None
    ) -> Union[None, Dict[str, Any], List[Dict[str, Any]]]:
        """
        Searches for PoLog records based on unique keys.
        """
        if not project_number and not filename and not db_path and not status:
            return self.search_po_logs([], [], session=session)

        col_filters = []
        val_filters = []
        if project_number is not None:
            col_filters.append('project_number')
            val_filters.append(project_number)
        if filename is not None:
            col_filters.append('filename')
            val_filters.append(filename)
        if db_path is not None:
            col_filters.append('db_path')
            val_filters.append(db_path)
        if status is not None:
            col_filters.append('status')
            val_filters.append(status)

        return self.search_po_logs(col_filters, val_filters, session=session)

    def create_po_log_by_keys(
        self,
        project_number: int,
        filename: str,
        db_path: str,
        status: str = 'PENDING',
        session: Session = None,
        **kwargs
    ):
        kwargs.update({
            'project_number': project_number,
            'filename': filename,
            'db_path': db_path,
            'status': status
        })
        unique_lookup = {'db_path': db_path}
        return self._create_record(PoLog, unique_lookup=unique_lookup, session=session, **kwargs)

    def update_po_log_by_keys(
        self,
        project_number: int,
        filename: str,
        db_path: str,
        status: Optional[str] = None,
        session: Session = None,
        **kwargs
    ):
        matches = self.search_po_log_by_keys(project_number, filename, db_path, session=session)
        if not matches:
            return None
        if isinstance(matches, list):
            match = matches[0]
        else:
            match = matches
        return self.update_po_log(match['id'], session=session, status=status, **kwargs)
    # endregion