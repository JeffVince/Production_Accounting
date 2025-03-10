"""
database/database_util.py

This module provides flexible, DRY (Don't Repeat Yourself) functions for searching,
creating, updating, deleting, and checking changes for various database records
using SQLAlchemy ORM with an optional session parameter. If no session is provided,
it uses `get_db_session()` from db_util to open/close a new session.
"""

from typing import Optional, Dict, Any, List, Union
import logging

from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session
from sqlalchemy import or_, and_

# Use the unified session pattern (get_db_session) instead of make_local_session
from database.db_util import get_db_session
from database_pg.models_pg import (
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

    # region UTILITY & HELPER METHODS

    def _serialize_record(self, record):
        """
        Converts a SQLAlchemy model record into a dict of column_name -> value.
        Returns None if record is None.
        """
        if not record:
            return None
        return {c.name: getattr(record, c.name) for c in record.__table__.columns}

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
          - None if no records,
          - A single dict if exactly one found,
          - A list if multiple found,
          - [] if mismatch or error.
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

        if session is not None:
            try:
                query = session.query(model)
                if column_names and values:
                    for col_name, val in zip(column_names, values):
                        column_attr = getattr(model, col_name, None)
                        if column_attr is None:
                            self.logger.warning(f"{prefix}😬 '{col_name}' invalid for {model.__name__}.")
                            return []
                        if isinstance(val, (list, tuple)):
                            query = query.filter(column_attr.in_(val))
                        else:
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
            with get_db_session() as new_session:
                try:
                    query = new_session.query(model)
                    if column_names and values:
                        for col_name, val in zip(column_names, values):
                            column_attr = getattr(model, col_name, None)
                            if column_attr is None:
                                self.logger.warning(f"😬 '{col_name}' invalid for {model.__name__}.")
                                return []
                            if isinstance(val, (list, tuple)):
                                query = query.filter(column_attr.in_(val))
                            else:
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
                    self.logger.error(f"💥 Error searching {model.__name__}: {e}", exc_info=True)
                    return []

    def _create_record(
            self,
            model,
            unique_lookup: dict = None,
            session: Session = None,
            **kwargs
    ) -> Optional[Dict[str, Any]]:
        """
        Creates a new record in the DB. Returns its dict form or None on error.
        If an IntegrityError occurs and unique_lookup is set, attempts concurrency
        fallback by re-querying within a SAVEPOINT (nested transaction).
        """
        prefix = "[BATCH OPERATION] " if session else ""
        self.logger.debug(f"{prefix}🧑‍💻 Creating {model.__name__} with: {kwargs}")
        self.logger.info(f"{prefix}🌱 Insert => {model.__name__} with {kwargs}")

        if session is not None:
            try:
                with session.begin_nested():
                    record = model(**kwargs)
                    session.add(record)
                    session.flush()  # May raise IntegrityError
                self.logger.debug(f"{prefix}🎉 Flushed new {model.__name__}, ID={getattr(record, 'id', 'N/A')}")
                return self._serialize_record(record)
            except IntegrityError as ie:
                self.logger.debug(f"{prefix}❗ IntegrityError creating {model.__name__}: {ie}")
                session.expire_all()  # Clear any stale state
                if unique_lookup:
                    self.logger.warning(f"{prefix}🔎 Trying concurrency fallback re-query...")
                    found = self._search_records(model, list(unique_lookup.keys()), list(unique_lookup.values()), session=session)
                    if found:
                        if isinstance(found, list) and len(found) > 0:
                            self.logger.info(f"{prefix}⚠️ Found {len(found)}; returning first fallback match.")
                            return found[0]
                        elif isinstance(found, dict):
                            self.logger.info(f"{prefix}⚠️ Found existing record after fallback.")
                            return found
                        else:
                            self.logger.error(f"{prefix}❌ No record found after fallback. Returning None.")
                            return None
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
            with get_db_session() as new_session:
                try:
                    record = model(**kwargs)
                    new_session.add(record)
                    new_session.flush()
                    new_session.commit()
                    self.logger.debug(f"🎉 Created {model.__name__}, ID={getattr(record, 'id', 'N/A')}")
                    return self._serialize_record(record)
                except IntegrityError as ie:
                    new_session.rollback()
                    self.logger.debug(f"❗ IntegrityError on create: {ie}")
                    if unique_lookup:
                        self.logger.warning("🔎 Attempting concurrency fallback re-query...")
                        found = self._search_records(model, list(unique_lookup.keys()), list(unique_lookup.values()), session=new_session)
                        if found:
                            if isinstance(found, list) and len(found) > 0:
                                self.logger.info(f"⚠️ Found {len(found)} matching; returning first.")
                                return found[0]
                            elif isinstance(found, dict):
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
                    self.logger.error(f"💥 Error updating {model.__name__}(id={record_id}): {e}", exc_info=True)
                    return None

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
                return True
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
                    self.logger.warning("❗ IntegrityError on delete.")
                    if unique_lookup:
                        self.logger.warning("🔎 Attempting fallback re-query (post-delete error).")
                        found = self._search_records(model, list(unique_lookup.keys()), list(unique_lookup.values()), session=new_session)
                        if found:
                            self.logger.info("⚠️ Record still present => cannot delete.")
                            return False
                        else:
                            self.logger.error("❌ Not found after fallback => likely already deleted.")
                            return False
                    else:
                        self.logger.error("❌ No unique_lookup => no re-check. Returning False.")
                        return False
                except Exception as e:
                    self.logger.error(f"💥 Error deleting {model.__name__}(id={record_id}): {e}", exc_info=True)
                    return False

    def bulk_delete_records(self, model: object, record_ids: List[int], session: Session = None) -> int:
        """
        Deletes records in bulk given a list of record IDs.
        Returns the number of records deleted.
        """
        if session is None:
            with get_db_session() as session:
                deleted_count = session.query(model) \
                    .filter(model.id.in_(record_ids)) \
                    .delete(synchronize_session=False)
                session.commit()
        else:
            deleted_count = session.query(model) \
                .filter(model.id.in_(record_ids)) \
                .delete(synchronize_session=False)
        self.logger.debug(f"Bulk delete: {deleted_count} records deleted from {model.__name__}.")
        return deleted_count

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
            self.logger.debug(f"{prefix}🙅 No single {model.__name__} found => has changes.")
            return True

        for field, new_val in kwargs.items():
            old_val = record_dict.get(field)
            # For state comparisons:
            if field == 'state':
                old_val = (old_val or '').upper()
                new_val = (new_val or '').upper()
            if old_val != new_val:
                return True
        return False

    # endregion (UTILITY & HELPER METHODS)

    # region GENERIC BULK/BATCH OPERATIONS

    def bulk_create_records(self, model, items: List[Dict[str, Any]], session: Session = None) -> List[Dict[str, Any]]:
        """
        Creates multiple records in the DB in bulk.
        Returns a list of dict representations for the created records.
        """
        if session is not None:
            try:
                records = [model(**item) for item in items]
                session.add_all(records)
                session.flush()
                return [self._serialize_record(record) for record in records]
            except Exception as e:
                self.logger.error(f"Error in bulk create for {model.__name__}: {e}", exc_info=True)
                session.rollback()
                return []
        else:
            with get_db_session() as new_session:
                try:
                    records = [model(**item) for item in items]
                    new_session.add_all(records)
                    new_session.flush()
                    new_session.commit()
                    return [self._serialize_record(record) for record in records]
                except Exception as e:
                    new_session.rollback()
                    self.logger.error(f"Error in bulk create for {model.__name__}: {e}", exc_info=True)
                    return []

    def bulk_update_records(self, model, updates: List[Dict[str, Any]], session: Session = None) -> List[Dict[str, Any]]:
        """
        Updates multiple records in bulk via a direct SQL update.
        Each dict in `updates` must have: {"id": <primary_key>, "field": <value>, ...}
        Returns a list of updated record dicts (re-fetched from the DB).
        """
        updated_records = []
        if session is None:
            from database.db_util import get_db_session
            with get_db_session() as new_session:
                return self.bulk_update_records(model, updates, session=new_session)

        try:
            for item in updates:
                record_id = item.get("id")
                if not record_id:
                    continue
                data_to_update = {k: v for k, v in item.items() if k != "id"}
                if not data_to_update:
                    continue

                result = (
                    session.query(model)
                    .filter(model.id == record_id)
                    .update(data_to_update, synchronize_session=False)
                )

                if result == 0:
                    self.logger.warning(
                        f"bulk_update_records: No '{model.__name__}' found with id={record_id}"
                    )
                else:
                    updated_obj = session.query(model).get(record_id)
                    updated_records.append(self._serialize_record(updated_obj))

            session.flush()
            session.commit()
            return updated_records

        except Exception as e:
            self.logger.error(
                f"Error in bulk_update_records for {model.__name__}: {e}",
                exc_info=True
            )
            session.rollback()
            return []

    def bulk_has_changes(self, model, checks: List[Dict[str, Any]], session: Session = None) -> List[bool]:
        """
        Checks for changes in multiple records.
        Each check is a dict that may contain 'record_id' or 'unique_filters' plus fields to compare.
        Returns a list of booleans indicating if changes were detected for each.
        """
        results = []
        if session is not None:
            for check in checks:
                record_id = check.get("record_id")
                unique_filters = check.get("unique_filters")
                expected = {k: v for k, v in check.items() if k not in ("record_id", "unique_filters")}
                result = self._has_changes_for_record(model, record_id=record_id, unique_filters=unique_filters, session=session, **expected)
                results.append(result)
            return results
        else:
            with get_db_session() as new_session:
                for check in checks:
                    record_id = check.get("record_id")
                    unique_filters = check.get("unique_filters")
                    expected = {k: v for k, v in check.items() if k not in ("record_id", "unique_filters")}
                    result = self._has_changes_for_record(model, record_id=record_id, unique_filters=unique_filters, session=new_session, **expected)
                    results.append(result)
                return results

    # endregion (GENERIC BULK/BATCH OPERATIONS)

    # region ACCOUNT CODE

    # region INDIVIDUAL CRUD
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

    # region BULK OPERATIONS
    def bulk_create_account_codes(self, items: List[Dict[str, Any]], session: Session = None):
        return self.bulk_create_records(AccountCode, items, session=session)

    def bulk_update_account_codes(self, updates: List[Dict[str, Any]], session: Session = None):
        return self.bulk_update_records(AccountCode, updates, session=session)

    def bulk_delete_account_codes(self, record_ids: List[int], session: Session = None) -> bool:
        return self.bulk_delete_records(AccountCode, record_ids, session=session)

    def bulk_account_code_has_changes(self, checks: List[Dict[str, Any]], session: Session = None) -> List[bool]:
        return self.bulk_has_changes(AccountCode, checks, session=session)
    # endregion

    # endregion (ACCOUNT CODE)

    # region BANK TRANSACTION

    # region INDIVIDUAL CRUD
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

    # region BULK OPERATIONS
    def bulk_create_bank_transactions(self, items: List[Dict[str, Any]], session: Session = None):
        return self.bulk_create_records(BankTransaction, items, session=session)

    def bulk_update_bank_transactions(self, updates: List[Dict[str, Any]], session: Session = None):
        return self.bulk_update_records(BankTransaction, updates, session=session)

    def bulk_delete_bank_transactions(self, record_ids: List[int], session: Session = None) -> bool:
        return self.bulk_delete_records(BankTransaction, record_ids, session=session)

    def bulk_bank_transaction_has_changes(self, checks: List[Dict[str, Any]], session: Session = None) -> List[bool]:
        return self.bulk_has_changes(BankTransaction, checks, session=session)
    # endregion

    # endregion (BANK TRANSACTION)

    # region BUDGET MAP

    # region INDIVIDUAL CRUD
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

    # region BULK OPERATIONS
    def bulk_create_budget_maps(self, items: List[Dict[str, Any]], session: Session = None):
        return self.bulk_create_records(BudgetMap, items, session=session)

    def bulk_update_budget_maps(self, updates: List[Dict[str, Any]], session: Session = None):
        return self.bulk_update_records(BudgetMap, updates, session=session)

    def bulk_delete_budget_maps(self, record_ids: List[int], session: Session = None) -> bool:
        return self.bulk_delete_records(BudgetMap, record_ids, session=session)

    def bulk_budget_map_has_changes(self, checks: List[Dict[str, Any]], session: Session = None) -> List[bool]:
        return self.bulk_has_changes(BudgetMap, checks, session=session)
    # endregion

    # endregion (BUDGET MAP)

    # region CONTACT

    # region INDIVIDUAL CRUD
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

    def create_minimal_contact(self, contact_name: str, session: Session = None):
        return self.create_contact(name=contact_name, vendor_type='Vendor', session=session)

    def find_contact_close_match(self, contact_name: str, all_db_contacts: List[Dict[str, Any]]) -> Optional[List[Dict[str, Any]]]:
        """
        Finds contacts in all_db_contacts that have the same first character as contact_name
        and are at most one edit away.
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
        """
        len1, len2 = len(s1), len(s2)
        if abs(len1 - len2) > 1:
            return False

        if len1 > len2:
            s1, s2 = s2, s1
            len1, len2 = len2, len1

        index1 = index2 = 0
        found_difference = False

        while index1 < len1 and index2 < len2:
            if s1[index1] != s2[index2]:
                if found_difference:
                    return False
                found_difference = True
                if len1 == len2:
                    index1 += 1
            else:
                index1 += 1
            index2 += 1

        return True
    # endregion

    # region BULK OPERATIONS
    def bulk_create_contacts(self, items: List[Dict[str, Any]], session: Session = None):
        return self.bulk_create_records(Contact, items, session=session)

    def bulk_update_contacts(self, updates: List[Dict[str, Any]], session: Session = None):
        return self.bulk_update_records(Contact, updates, session=session)

    def bulk_delete_contacts(self, record_ids: List[int], session: Session = None) -> bool:
        return self.bulk_delete_records(Contact, record_ids, session=session)

    def bulk_contact_has_changes(self, checks: List[Dict[str, Any]], session: Session = None) -> List[bool]:
        return self.bulk_has_changes(Contact, checks, session=session)
    # endregion

    # endregion (CONTACT)

    # region DETAIL ITEM

    # region INDIVIDUAL CRUD
    def create_detail_item(self, session: Session = None, **kwargs):
        unique_lookup = {}
        if (
            'po_number' in kwargs and
            'project_number' in kwargs and
            'detail_number' in kwargs and
            'line_number' in kwargs
        ):
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
        if (
            'project_number' in kwargs and
            'po_number' in kwargs and
            'detail_number' in kwargs and
            'line_number' in kwargs
        ):
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

    # region BULK OPERATIONS
    def bulk_create_detail_items(self, items: List[Dict[str, Any]], session: Session = None):
        return self.bulk_create_records(DetailItem, items, session=session)

    def bulk_update_detail_items(self, updates: List[Dict[str, Any]], session: Session = None):
        return self.bulk_update_records(DetailItem, updates, session=session)

    def bulk_delete_detail_items(self, record_ids: List[int], session: Session = None) -> bool:
        return self.bulk_delete_records(DetailItem, record_ids, session=session)

    def bulk_detail_item_has_changes(self, checks: List[Dict[str, Any]], session: Session = None) -> List[bool]:
        return self.bulk_has_changes(DetailItem, checks, session=session)
    # endregion

    # endregion (DETAIL ITEM)

    # region INVOICE

    # region INDIVIDUAL CRUD
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

    # region BULK OPERATIONS
    def bulk_create_invoices(self, items: List[Dict[str, Any]], session: Session = None):
        return self.bulk_create_records(Invoice, items, session=session)

    def bulk_update_invoices(self, updates: List[Dict[str, Any]], session: Session = None):
        return self.bulk_update_records(Invoice, updates, session=session)

    def bulk_delete_invoices(self, record_ids: List[int], session: Session = None) -> bool:
        return self.bulk_delete_records(Invoice, record_ids, session=session)

    def bulk_invoice_has_changes(self, checks: List[Dict[str, Any]], session: Session = None) -> List[bool]:
        return self.bulk_has_changes(Invoice, checks, session=session)
    # endregion

    # endregion (INVOICE)

    # region PO LOG

    # region INDIVIDUAL CRUD
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

    # region BULK OPERATIONS
    def bulk_create_po_logs(self, items: List[Dict[str, Any]], session: Session = None):
        return self.bulk_create_records(PoLog, items, session=session)

    def bulk_update_po_logs(self, updates: List[Dict[str, Any]], session: Session = None):
        return self.bulk_update_records(PoLog, updates, session=session)

    def bulk_delete_po_logs(self, record_ids: List[int], session: Session = None) -> bool:
        return self.bulk_delete_records(PoLog, record_ids, session=session)

    def bulk_po_log_has_changes(self, checks: List[Dict[str, Any]], session: Session = None) -> List[bool]:
        return self.bulk_has_changes(PoLog, checks, session=session)
    # endregion

    # endregion (PO LOG)

    # region PROJECT

    # region INDIVIDUAL CRUD
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

    # region BULK OPERATIONS
    def bulk_create_projects(self, items: List[Dict[str, Any]], session: Session = None):
        return self.bulk_create_records(Project, items, session=session)

    def bulk_update_projects(self, updates: List[Dict[str, Any]], session: Session = None):
        return self.bulk_update_records(Project, updates, session=session)

    def bulk_delete_projects(self, record_ids: List[int], session: Session = None) -> bool:
        return self.bulk_delete_records(Project, record_ids, session=session)

    def bulk_project_has_changes(self, checks: List[Dict[str, Any]], session: Session = None) -> List[bool]:
        return self.bulk_has_changes(Project, checks, session=session)
    # endregion

    # endregion (PROJECT)

    # region PURCHASE ORDER

    # region INDIVIDUAL CRUD
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
        project_record = self.search_projects(['project_number'], [str(project_number)], session=session)
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

    # region BULK OPERATIONS
    def bulk_create_purchase_orders(self, items: List[Dict[str, Any]], session: Session = None):
        return self.bulk_create_records(PurchaseOrder, items, session=session)

    def bulk_update_purchase_orders(self, updates: List[Dict[str, Any]], session: Session = None):
        return self.bulk_update_records(PurchaseOrder, updates, session=session)

    def bulk_delete_purchase_orders(self, record_ids: List[int], session: Session = None) -> bool:
        return self.bulk_delete_records(PurchaseOrder, record_ids, session=session)

    def bulk_purchase_order_has_changes(self, checks: List[Dict[str, Any]], session: Session = None) -> List[bool]:
        return self.bulk_has_changes(PurchaseOrder, checks, session=session)
    # endregion

    # endregion (PURCHASE ORDER)

    # region RECEIPT

    # region INDIVIDUAL CRUD
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

    def bulk_update_receipts(self, items: List[dict], session: Session = None) -> List[Dict[str, Any]]:
        """
        Bulk update Receipt records.
        Each item dict must contain an 'id' field along with the fields to update.
        """
        updated_objects = []
        for item in items:
            record = session.query(Receipt).get(item['id'])
            if record:
                for key, value in item.items():
                    if key != 'id':
                        setattr(record, key, value)
                updated_objects.append(record)
        session.flush()
        return [self._serialize_record(obj) for obj in updated_objects]
    # endregion

    # region BULK OPERATIONS
    def bulk_create_receipts(self, items: List[Dict[str, Any]], session: Session = None):
        return self.bulk_create_records(Receipt, items, session=session)

    def bulk_delete_receipts(self, record_ids: List[int], session: Session = None) -> bool:
        return self.bulk_delete_records(Receipt, record_ids, session=session)

    def bulk_receipt_has_changes(self, checks: List[Dict[str, Any]], session: Session = None) -> List[bool]:
        return self.bulk_has_changes(Receipt, checks, session=session)
    # endregion

    # endregion (RECEIPT)

    # region SPEND MONEY

    # region INDIVIDUAL CRUD
    def create_spend_money(self, session: Session = None, **kwargs):
        return self._create_record(SpendMoney, session=session, **kwargs)

    def search_spend_money(self, column_names, values, deleted=False, session: Session = None):
        prefix = "[BATCH OPERATION] " if session else ""
        self.logger.debug(f"{prefix}💵 Searching SpendMoney columns={column_names}, vals={values}, deleted={deleted}")
        records = self._search_records(SpendMoney, column_names, values, session=session)
        if not records:
            return records
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

    def batch_search_spend_money_by_keys(self, keys: List[tuple], deleted: bool = False, session: Session = None) -> List[Dict[str, Any]]:
        """
        Batch search for SpendMoney records.
        Each key is a tuple: (project_number, po_number, detail_number).
        Filters out records marked as DELETED if deleted=False.
        """
        from sqlalchemy import or_, and_
        if not keys:
            return []
        conditions = []
        for key in keys:
            conditions.append(
                and_(
                    SpendMoney.project_number == key[0],
                    SpendMoney.po_number == key[1],
                    SpendMoney.detail_number == key[2]
                )
            )
        query = session.query(SpendMoney).filter(or_(*conditions))
        records = query.all()
        result = []
        for rec in records:
            rec_dict = self._serialize_record(rec)
            if not deleted and rec_dict.get("state") == "DELETED":
                continue
            result.append(rec_dict)
        return result
    # endregion

    # region BULK OPERATIONS
    def bulk_create_spend_money(self, items: List[Dict[str, Any]], session: Session = None):
        return self.bulk_create_records(SpendMoney, items, session=session)

    def bulk_update_spend_money(self, items: List[dict], session: Session = None) -> List[Dict[str, Any]]:
        """
        Bulk update SpendMoney records.
        Each item dict must include an 'id' field along with the fields to update.
        """
        updated_objects = []
        for item in items:
            record = session.query(SpendMoney).get(item['id'])
            if record:
                for key, value in item.items():
                    if key != 'id':
                        setattr(record, key, value)
                updated_objects.append(record)
        session.flush()
        return [self._serialize_record(obj) for obj in updated_objects]

    def bulk_delete_spend_money(self, record_ids: List[int], session: Session = None) -> bool:
        return self.bulk_delete_records(SpendMoney, record_ids, session=session)

    def bulk_spend_money_has_changes(self, checks: List[Dict[str, Any]], session: Session = None) -> List[bool]:
        return self.bulk_has_changes(SpendMoney, checks, session=session)
    # endregion

    # endregion (SPEND MONEY)

    # region TAX ACCOUNT

    # region INDIVIDUAL CRUD
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

    # region BULK OPERATIONS
    def bulk_create_tax_accounts(self, items: List[Dict[str, Any]], session: Session = None):
        return self.bulk_create_records(TaxAccount, items, session=session)

    def bulk_update_tax_accounts(self, updates: List[Dict[str, Any]], session: Session = None):
        return self.bulk_update_records(TaxAccount, updates, session=session)

    def bulk_delete_tax_accounts(self, record_ids: List[int], session: Session = None) -> bool:
        return self.bulk_delete_records(TaxAccount, record_ids, session=session)

    def bulk_tax_account_has_changes(self, checks: List[Dict[str, Any]], session: Session = None) -> List[bool]:
        return self.bulk_has_changes(TaxAccount, checks, session=session)
    # endregion

    # endregion (TAX ACCOUNT)

    # region TAX LEDGER

    # region INDIVIDUAL CRUD
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

    # region BULK OPERATIONS
    def bulk_create_tax_ledgers(self, items: List[Dict[str, Any]], session: Session = None):
        return self.bulk_create_records(TaxLedger, items, session=session)

    def bulk_update_tax_ledgers(self, updates: List[Dict[str, Any]], session: Session = None):
        return self.bulk_update_records(TaxLedger, updates, session=session)

    def bulk_delete_tax_ledgers(self, record_ids: List[int], session: Session = None) -> bool:
        return self.bulk_delete_records(TaxLedger, record_ids, session=session)

    def bulk_tax_ledger_has_changes(self, checks: List[Dict[str, Any]], session: Session = None) -> List[bool]:
        return self.bulk_has_changes(TaxLedger, checks, session=session)
    # endregion

    # endregion (TAX LEDGER)

    # region USER

    # region INDIVIDUAL CRUD
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

    # region BULK OPERATIONS
    def bulk_create_users(self, items: List[Dict[str, Any]], session: Session = None):
        return self.bulk_create_records(User, items, session=session)

    def bulk_update_users(self, updates: List[Dict[str, Any]], session: Session = None):
        return self.bulk_update_records(User, updates, session=session)

    def bulk_delete_users(self, record_ids: List[int], session: Session = None) -> bool:
        return self.bulk_delete_records(User, record_ids, session=session)

    def bulk_user_has_changes(self, checks: List[Dict[str, Any]], session: Session = None) -> List[bool]:
        return self.bulk_has_changes(User, checks, session=session)
    # endregion

    # endregion (USER)

    # region XERO BILL

    # region INDIVIDUAL CRUD
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

    def batch_search_xero_bills_by_keys(self, keys: List[tuple], session: Session = None) -> List[Dict[str, Any]]:
        """
        Batch search for XeroBill records.
        Each key is a tuple: (project_number, po_number, detail_number).
        """
        from sqlalchemy import or_, and_
        if not keys:
            return []
        conditions = []
        for key in keys:
            conditions.append(
                and_(
                    XeroBill.project_number == key[0],
                    XeroBill.po_number == key[1],
                    XeroBill.detail_number == key[2]
                )
            )
        query = session.query(XeroBill).filter(or_(*conditions))
        records = query.all()
        return [self._serialize_record(r) for r in records]

    def bulk_create_xero_bills(self, items: List[dict], session: Session = None) -> List[Dict[str, Any]]:
        """
        Bulk create XeroBill records.
        """
        new_objects = [XeroBill(**item) for item in items]
        session.add_all(new_objects)
        session.flush()
        return [self._serialize_record(obj) for obj in new_objects]

    def bulk_update_xero_bills(self, items: List[dict], session: Session = None) -> List[Dict[str, Any]]:
        """
        Bulk update XeroBill records.
        Each item dict must contain an 'id' field along with the fields to update.
        """
        updated_objects = []
        for item in items:
            record = session.query(XeroBill).get(item['id'])
            if record:
                for key, value in item.items():
                    if key != 'id':
                        setattr(record, key, value)
                updated_objects.append(record)
        session.flush()
        return [self._serialize_record(obj) for obj in updated_objects]
    # endregion

    # region BULK OPERATIONS
    def bulk_delete_xero_bills(self, record_ids: List[int], session: Session = None) -> bool:
        return self.bulk_delete_records(XeroBill, record_ids, session=session)

    def bulk_xero_bill_has_changes(self, checks: List[Dict[str, Any]], session: Session = None) -> List[bool]:
        return self.bulk_has_changes(XeroBill, checks, session=session)
    # endregion

    # endregion (XERO BILL)

    # region XERO BILL LINE ITEM

    # region INDIVIDUAL CRUD
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

    def batch_search_xero_bill_line_items_by_xero_bill_ids(self, xero_bill_ids: List[int], session: Session = None) -> List[Dict[str, Any]]:
        """
        Batch search for XeroBillLineItem records by a list of XeroBill IDs.
        """
        if not xero_bill_ids:
            return []
        result = self._search_records(XeroBillLineItem, ["parent_id"], [xero_bill_ids], session=session)
        return result
    # endregion

    # region BULK OPERATIONS
    def bulk_create_xero_bill_line_items(self, items: List[Dict[str, Any]], session: Session = None):
        return self.bulk_create_records(XeroBillLineItem, items, session=session)

    def bulk_update_xero_bill_line_items(self, updates: List[Dict[str, Any]], session: Session = None):
        return self.bulk_update_records(XeroBillLineItem, updates, session=session)

    def bulk_delete_xero_bill_line_items(self, record_ids: List[int], session: Session = None) -> bool:
        return self.bulk_delete_records(XeroBillLineItem, record_ids, session=session)

    def bulk_xero_bill_line_item_has_changes(self, checks: List[Dict[str, Any]], session: Session = None) -> List[bool]:
        return self.bulk_has_changes(XeroBillLineItem, checks, session=session)
    # endregion

    # endregion (XERO BILL LINE ITEM)

    # region ADDITIONAL BATCH SEARCH METHODS (MODEL-SPECIFIC)

    def batch_search_invoices_by_keys(self, keys: List[tuple], session: Session = None) -> List[Dict[str, Any]]:
        """
        Batch search for Invoice records using a list of keys.
        Each key is a tuple: (project_number, po_number, invoice_number)
        Returns a list of matching Invoice records as dicts.
        """
        from sqlalchemy import or_, and_
        if not keys:
            return []
        conditions = []
        for key in keys:
            project_number, po_number, invoice_number = key
            conditions.append(
                and_(
                    Invoice.project_number == project_number,
                    Invoice.po_number == po_number,
                    Invoice.invoice_number == invoice_number
                )
            )
        if session:
            query = session.query(Invoice).filter(or_(*conditions))
            records = query.all()
            return [self._serialize_record(r) for r in records]
        else:
            with get_db_session() as session_local:
                query = session_local.query(Invoice).filter(or_(*conditions))
                records = query.all()
                return [self._serialize_record(r) for r in records]

    def batch_search_receipts_by_keys(self, keys: List[tuple], session: Session = None) -> List[Dict[str, Any]]:
        """
        Batch search for Receipt records using a list of keys.
        Each key is a tuple: (project_number, po_number, detail_number)
        Returns a list of matching Receipt records as dicts.
        """
        from sqlalchemy import or_, and_
        if not keys:
            return []
        conditions = []
        for key in keys:
            project_number, po_number, detail_number = key
            conditions.append(
                and_(
                    Receipt.project_number == project_number,
                    Receipt.po_number == po_number,
                    Receipt.detail_number == detail_number
                )
            )
        if isinstance(session, Session):
            query = session.query(Receipt).filter(or_(*conditions))
            records = query.all()
            return [self._serialize_record(r) for r in records]
        else:
            with get_db_session() as session_local:
                query = session_local.query(Receipt).filter(or_(*conditions))
                records = query.all()
                return [self._serialize_record(r) for r in records]

    def batch_search_detail_items_by_keys(self, keys: List[dict], session: Session = None) -> List[Dict[str, Any]]:
        """
        Batch search for DetailItem records using a list of key dictionaries.
        Each key dict should have: project_number, po_number, detail_number, line_number.
        Returns a list of matching DetailItem records as dicts.
        """
        from sqlalchemy import or_, and_
        if not keys:
            return []
        conditions = []
        for key in keys:
            cond = and_(
                DetailItem.project_number == key.get('project_number'),
                DetailItem.po_number == key.get('po_number'),
                DetailItem.detail_number == key.get('detail_number'),
                DetailItem.line_number == key.get('line_number')
            )
            conditions.append(cond)
        if session is not None:
            query = session.query(DetailItem).filter(or_(*conditions))
            records = query.all()
            return [self._serialize_record(r) for r in records]
        else:
            with get_db_session() as session_local:
                query = session_local.query(DetailItem).filter(or_(*conditions))
                records = query.all()
                return [self._serialize_record(r) for r in records]

    def batch_search_purchase_orders_by_keys(self, keys: List[tuple], session: Session = None) -> List[Dict[str, Any]]:
        """
        Batch search for PurchaseOrder records using a list of keys.
        Each key is a tuple: (project_number, po_number).
        Returns a list of matching PurchaseOrder records as dicts.
        """
        from sqlalchemy import or_, and_
        if not keys:
            return []
        conditions = []
        for project_number, po_number in keys:
            conditions.append(
                and_(
                    PurchaseOrder.project_number == project_number,
                    PurchaseOrder.po_number == po_number
                )
            )
        if session is not None:
            query = session.query(PurchaseOrder).filter(or_(*conditions))
            records = query.all()
            return [self._serialize_record(r) for r in records]
        else:
            with get_db_session() as session_local:
                query = session_local.query(PurchaseOrder).filter(or_(*conditions))
                records = query.all()
                return [self._serialize_record(r) for r in records]

    # endregion