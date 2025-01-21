"""
database/database_util.py

This module provides flexible, DRY (Don't Repeat Yourself) functions for searching,
creating, updating, deleting, and checking changes for various database records
using SQLAlchemy ORM with optional session batching.
"""
from typing import Optional, Dict, Any, List, Union
import logging

from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

# region 🛠️ UTILITY IMPORTS
from database.models import (
    Contact, Project, PurchaseOrder, DetailItem, BankTransaction,
    BillLineItem, Invoice, AccountCode, Receipt, SpendMoney, TaxAccount,
    XeroBill, User, TaxLedger, BudgetMap
)
from database.db_util import make_local_session
# endregion


#region 🏢 CLASS DEFINITION
class DatabaseOperations:
    """
    Provides methods to create, read, update, delete, and check changes
    in database records, handling concurrency safely via unique lookups.
    """

    # region 🔧 Initialization
    def __init__(self):
        self.logger = logging.getLogger('database_logger')
        self.logger.debug("🌟 DatabaseOperations initialized.")
    # endregion

    # region 🛠️ Utility Functions

    # region 📦 Serialize
    def _serialize_record(self, record):
        """
        Converts a SQLAlchemy model record into a dict of column_name -> value.
        Returns None if record is None.
        """
        if not record:
            return None
        record_values = {c.name: getattr(record, c.name) for c in record.__table__.columns}
        return record_values
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
        prefix = "[ BATCH OPERATION ] " if session else ""
        column_names = column_names or []
        values = values or []

        if column_names and values:
            self.logger.debug(f"{prefix}🕵️ Searching {model.__name__} with filters: {list(zip(column_names, values))}")
            self.logger.info(f"{prefix}🔎 Checking for matches in {model.__name__} for columns/values: {list(zip(column_names, values))}")
            if len(column_names) != len(values):
                self.logger.warning(f"{prefix}⚠️ Mismatch: number of columns vs. values. Returning empty list.")
                return []
        else:
            self.logger.debug(f"{prefix}🕵️ Searching all records from {model.__name__}, no filters.")
            self.logger.info(f"{prefix}🔎 Fetching entire {model.__name__} table without filters.")

        if session is not None:
            try:
                query = session.query(model)
                if column_names and values:
                    for (col_name, val) in zip(column_names, values):
                        column_attr = getattr(model, col_name, None)
                        if column_attr is None:
                            self.logger.warning(f"{prefix}😬 '{col_name}' is not valid for {model.__name__}. Returning empty list.")
                            return []
                        query = query.filter(column_attr == val)

                records = query.all()
                if not records:
                    self.logger.info(f"{prefix}🙅 No records found in {model.__name__}. Maybe next time!")
                    return None
                elif len(records) == 1:
                    # Specifically changed per prior request:
                    self.logger.info(f"[_search_records] - ✅ Found a matching {model.__name__}. Bingo!")
                    return self._serialize_record(records[0])
                else:
                    self.logger.info(f"{prefix}✅ Located {len(records)} records in {model.__name__}. Bundling them up.")
                    return [self._serialize_record(r) for r in records]
            except Exception as e:
                self.logger.error(f"{prefix}❌ Error searching {model.__name__}: {e}", exc_info=True)
                return []
        else:
            with make_local_session() as new_session:
                try:
                    query = new_session.query(model)
                    if column_names and values:
                        for (col_name, val) in zip(column_names, values):
                            column_attr = getattr(model, col_name, None)
                            if column_attr is None:
                                self.logger.warning(f"😬 '{col_name}' is not valid for {model.__name__}. Returning empty list.")
                                return []
                            query = query.filter(column_attr == val)

                    records = query.all()
                    if not records:
                        self.logger.info(f"🙅 No records found in {model.__name__}. Maybe next time!")
                        return None
                    elif len(records) == 1:
                        self.logger.info(f"[_search_records] - ✅ Found a matching {model.__name__}. Bingo!")
                        return self._serialize_record(records[0])
                    else:
                        self.logger.info(f"✅ Located {len(records)} records in {model.__name__}. Bundling them up.")
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
        Creates a new record in the database. Returns its dict form or None on error.
        Flushes before commit to ensure the ID is generated.
        Handles concurrency by re-querying if IntegrityError occurs and unique_lookup is provided.
        """
        prefix = "[ BATCH OPERATION ] " if session else ""
        self.logger.debug(f"{prefix}🧑‍💻 Creating new {model.__name__} with data: {kwargs}")
        self.logger.info(f"{prefix}🌱 Inserting record into {model.__name__} with {kwargs}")

        if session is not None:
            try:
                record = model(**kwargs)
                session.add(record)
                session.flush()
                self.logger.debug(f"{prefix}🎉 Flushed new {model.__name__}, ID now: {getattr(record, 'id', 'N/A')}")
                self.logger.info(f"{prefix}🌟 Creation successful (pending commit).")
                return self._serialize_record(record)

            except IntegrityError:
                self.logger.debug(f"{prefix}❗ IntegrityError creating {model.__name__}")
                session.rollback()

                if unique_lookup:
                    self.logger.warning(f"{prefix}🔎 Attempting concurrency fallback re-query...")
                    found = self._search_records(model, list(unique_lookup.keys()), list(unique_lookup.values()), session=session)
                    if found:
                        if isinstance(found, list):
                            self.logger.info(f"{prefix}⚠️ Found {len(found)} matching records; returning the first.")
                            return found[0]
                        else:
                            self.logger.info(f"{prefix}⚠️ Found exactly one existing record after concurrency fallback.")
                            return found
                    else:
                        self.logger.error(f"{prefix}❌ No record found after concurrency fallback for {model.__name__}.")
                        return None
                else:
                    self.logger.error(f"{prefix}❌ No unique_lookup, cannot re-query. Returning None.")
                return None
            except Exception as e:
                session.rollback()
                self.logger.error(f"{prefix}💥 Trouble creating {model.__name__}: {e}", exc_info=True)
                return None

        else:
            with make_local_session() as new_session:
                try:
                    record = model(**kwargs)
                    new_session.add(record)
                    new_session.flush()
                    self.logger.debug(f"🎉 Flushed new {model.__name__}, ID now: {getattr(record, 'id', 'N/A')}")
                    new_session.commit()
                    self.logger.info(f"🌟 Creation successful! Record is now in the DB.")
                    return self._serialize_record(record)

                except IntegrityError:
                    self.logger.debug(f"❗ IntegrityError creating {model.__name__}")
                    new_session.rollback()

                    if unique_lookup:
                        self.logger.warning("🔎 Attempting concurrency fallback re-query...")
                        found = self._search_records(model, list(unique_lookup.keys()), list(unique_lookup.values()))
                        if found:
                            if isinstance(found, list):
                                self.logger.info(f"⚠️ Found {len(found)} matching records; returning the first.")
                                return found[0]
                            else:
                                self.logger.info("⚠️ Found exactly one existing record after concurrency fallback.")
                                return found
                        else:
                            self.logger.error(f"❌ No record found after concurrency fallback for {model.__name__}.")
                            return None
                    else:
                        self.logger.error("❌ No unique_lookup, cannot re-query. Returning None.")
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
        Updates an existing record by primary key. Returns the updated record or None on error.
        Flushes before commit so changes are visible. Handles concurrency fallback if IntegrityError occurs.
        """
        prefix = "[ BATCH OPERATION ] " if session else ""
        self.logger.debug(f"{prefix}🔧 Updating {model.__name__}(id={record_id}) with {kwargs}")
        self.logger.info(f"{prefix}🤝 Checking and then updating {model.__name__}(id={record_id}).")

        if session is not None:
            try:
                record = session.query(model).get(record_id)
                if not record:
                    self.logger.info(f"{prefix}🙅 No {model.__name__} with id={record_id} found.")
                    return None

                for (key, value) in kwargs.items():
                    if hasattr(record, key):
                        setattr(record, key, value)
                    else:
                        self.logger.warning(f"{prefix}⚠️ '{key}' does not exist on {model.__name__}. Skipping.")

                session.flush()
                self.logger.debug(f"{prefix}🎉 Flushed updated {model.__name__}(id={record_id}).")
                self.logger.info(f"{prefix}✅ Done updating {model.__name__}.")
                return self._serialize_record(record)

            except IntegrityError:
                self.logger.warning(f"{prefix}❗ IntegrityError updating {model.__name__}(id={record_id})")
                session.rollback()

                if unique_lookup:
                    self.logger.warning(f"{prefix}🔎 Attempting concurrency fallback re-query...")
                    found = self._search_records(model, list(unique_lookup.keys()), list(unique_lookup.values()), session=session)
                    if found:
                        if isinstance(found, list):
                            self.logger.info(f"{prefix}⚠️ Found {len(found)} matching records; returning the first.")
                            return found[0]
                        else:
                            self.logger.info(f"{prefix}⚠️ Found exactly one existing record after concurrency fallback.")
                            return found
                    else:
                        self.logger.error(f"{prefix}❌ No record found after concurrency fallback for {model.__name__}.")
                        return None
                else:
                    self.logger.error(f"{prefix}❌ No unique_lookup for fallback. Returning None.")
                return None

            except Exception as e:
                session.rollback()
                self.logger.error(f"{prefix}💥 Issue updating {model.__name__}: {e}", exc_info=True)
                return None

        else:
            with make_local_session() as new_session:
                try:
                    record = new_session.query(model).get(record_id)
                    if not record:
                        self.logger.info(f"🙅 No {model.__name__} with id={record_id} found.")
                        return None

                    for (key, value) in kwargs.items():
                        if hasattr(record, key):
                            setattr(record, key, value)
                        else:
                            self.logger.warning(f"⚠️ '{key}' does not exist on {model.__name__}. Skipping.")

                    new_session.flush()
                    self.logger.debug(f"🎉 Flushed updated {model.__name__}(id={record_id}).")
                    new_session.commit()
                    self.logger.info(f"✅ Done updating {model.__name__}.")
                    return self._serialize_record(record)

                except IntegrityError:
                    self.logger.warning(f"❗ IntegrityError updating {model.__name__}(id={record_id})")
                    new_session.rollback()

                    if unique_lookup:
                        self.logger.warning("🔎 Attempting concurrency fallback re-query...")
                        found = self._search_records(model, list(unique_lookup.keys()), list(unique_lookup.values()))
                        if found:
                            if isinstance(found, list):
                                self.logger.info(f"⚠️ Found {len(found)} matching records; returning the first.")
                                return found[0]
                            else:
                                self.logger.info("⚠️ Found exactly one existing record after concurrency fallback.")
                                return found
                        else:
                            self.logger.error(f"❌ No record found after concurrency fallback for {model.__name__}.")
                            return None
                    else:
                        self.logger.error("❌ No unique_lookup for fallback. Returning None.")
                    return None

                except Exception as e:
                    new_session.rollback()
                    self.logger.error(f"💥 Issue updating {model.__name__}: {e}", exc_info=True)
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
        Handles concurrency fallback if an IntegrityError occurs.
        """
        prefix = "[ BATCH OPERATION ] " if session else ""
        self.logger.debug(f"{prefix}🗑️ Deleting {model.__name__}(id={record_id}).")

        if session is not None:
            try:
                record = session.query(model).get(record_id)
                if not record:
                    self.logger.info(f"{prefix}🙅 No {model.__name__} with id={record_id} found for deletion.")
                    return False

                session.delete(record)
                session.flush()
                self.logger.debug(f"{prefix}🗑️ Successfully deleted {model.__name__}(id={record_id}). (pending commit)")
                return True  # caller can commit

            except IntegrityError:
                self.logger.warning(f"{prefix}❗ IntegrityError deleting {model.__name__}(id={record_id})")
                session.rollback()

                if unique_lookup:
                    self.logger.warning(f"{prefix}🔎 Attempting concurrency fallback re-query post-delete error...")
                    found = self._search_records(model, list(unique_lookup.keys()), list(unique_lookup.values()), session=session)
                    if found:
                        self.logger.info(f"{prefix}⚠️ Record still exists after fallback. Cannot delete. Found: {found}")
                        return False
                    else:
                        self.logger.error(f"{prefix}❌ No record found after concurrency fallback. Possibly was already deleted.")
                        return False
                else:
                    self.logger.error(f"{prefix}❌ No unique_lookup, cannot re-check. Returning False.")
                return False
            except Exception as e:
                session.rollback()
                self.logger.error(f"{prefix}💥 Issue deleting {model.__name__}(id={record_id}): {e}", exc_info=True)
                return False

        else:
            with make_local_session() as new_session:
                try:
                    record = new_session.query(model).get(record_id)
                    if not record:
                        self.logger.info(f"🙅 No {model.__name__} with id={record_id} found for deletion.")
                        return False

                    new_session.delete(record)
                    new_session.flush()
                    new_session.commit()
                    self.logger.debug(f"🗑️ Successfully deleted {model.__name__}(id={record_id}).")
                    return True

                except IntegrityError:
                    self.logger.warning(f"❗ IntegrityError deleting {model.__name__}(id={record_id})")
                    new_session.rollback()

                    if unique_lookup:
                        self.logger.warning("🔎 Attempting concurrency fallback re-query post-delete error...")
                        found = self._search_records(model, list(unique_lookup.keys()), list(unique_lookup.values()))
                        if found:
                            self.logger.info(f"⚠️ Record still exists after fallback. Cannot delete. Found: {found}")
                            return False
                        else:
                            self.logger.error("❌ No record found after concurrency fallback. Possibly was already deleted.")
                            return False
                    else:
                        self.logger.error("❌ No unique_lookup to re-check. Returning False.")
                    return False

                except Exception as e:
                    new_session.rollback()
                    self.logger.error(f"💥 Issue deleting {model.__name__}(id={record_id}): {e}", exc_info=True)
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
        fetch by ID. Otherwise, tries unique_filters. Compares each kwarg to the record’s fields.
        Returns True if differences are found, else False.
        """
        prefix = "[ BATCH OPERATION ] " if session else ""
        if not record_id and not unique_filters:
            self.logger.warning(f"{prefix}⚠️ Cannot check changes for {model.__name__}; no record_id or unique_filters.")
            return False

        record_dict = None

        if record_id:
            if session is not None:
                record = session.query(model).get(record_id)
                if record:
                    record_dict = self._serialize_record(record)
            else:
                with make_local_session() as new_session:
                    rec = new_session.query(model).get(record_id)
                    if rec:
                        record_dict = self._serialize_record(rec)
        else:
            filters_list = list(unique_filters.keys())
            values_list = list(unique_filters.values())
            found = self._search_records(model, filters_list, values_list, session=session)
            if isinstance(found, dict):
                record_dict = found
            elif isinstance(found, list) and len(found) == 1:
                record_dict = found[0]

        if not record_dict:
            self.logger.debug(f"{prefix}🙅 No single {model.__name__} found to compare. Returning False.")
            return False

        for (field, new_val) in kwargs.items():
            old_val = record_dict.get(field)
            if field == 'state':
                old_val = (old_val or '').upper()
                new_val = (new_val or '').upper()
            if old_val != new_val:
                return True
        return False
    # endregion

    #endregion

    ########################################################################
    # REGIONS FOR EACH MODEL
    # Each region now follows the order:
    #   CREATE -> READ -> UPDATE -> DELETE -> HAS CHANGES
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
        # Optionally pass unique_lookup if you want concurrency fallback:
        unique_lookup = {}
        if 'code' in kwargs:
            unique_lookup['code'] = kwargs['code']
        return self._delete_record(AccountCode, account_code_id, unique_lookup=unique_lookup, session=session)

    def account_code_has_changes(
        self,
        record_id: Optional[int] = None,
        code: Optional[str] = None,
        session: Session = None,
        **kwargs
    ) -> bool:
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

    def purchase_order_has_changes(
        self,
        record_id: Optional[int] = None,
        project_number: Optional[int] = None,
        po_number: Optional[int] = None,
        session: Session = None,
        **kwargs
    ) -> bool:
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
        if not project_number and (not po_number):
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
        if (
            'po_number' in kwargs and
            'project_number' in kwargs and
            ('detail_number' in kwargs) and
            ('line_number' in kwargs)
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

    def contact_has_changes(
        self,
        record_id: Optional[int] = None,
        name: Optional[str] = None,
        email: Optional[str] = None,
        session: Session = None,
        **kwargs
    ) -> bool:
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

    def find_contact_close_match(self, contact_name: str, all_db_contacts: List[Dict[str, Any]], cutoff=0.7):
        """
        Attempts a fuzzy name match among all_db_contacts.
        Returns a list of possible matches or None.
        """
        from difflib import get_close_matches
        if not all_db_contacts:
            self.logger.debug("🙅 No existing contacts to match against.")
            return None

        name_map = {c['name']: c for c in all_db_contacts if c.get('name')}
        best_matches = get_close_matches(contact_name, name_map.keys(), n=5, cutoff=cutoff)
        if best_matches:
            return [name_map[m] for m in best_matches]
        else:
            return None

    def create_minimal_contact(self, contact_name: str, session: Session = None):
        return self.create_contact(name=contact_name, vendor_type='Vendor', session=session)
    # endregion

    # region 🏗️ PROJECT
    def create_project(self, session: Session = None, **kwargs):
        unique_lookup = {}
        if 'project_number' in kwargs:
            unique_lookup['project_number'] = kwargs['project_number']
        return self._create_record(Project, unique_lookup=unique_lookup, session=session, **kwargs)

    def search_projects(self, column_names, values, session: Session = None):
        return self._search_records(Project, column_names, values, session=session)

    def update_project(self, project_id, session: Session = None, **kwargs):
        return self._update_record(Project, project_id, session=session, **kwargs)

    def delete_project(self, project_id, session: Session = None, **kwargs) -> bool:
        unique_lookup = {}
        if 'project_number' in kwargs:
            unique_lookup['project_number'] = kwargs['project_number']
        return self._delete_record(Project, project_id, unique_lookup=unique_lookup, session=session)

    def project_has_changes(
        self,
        record_id: Optional[int] = None,
        project_number: Optional[int] = None,
        session: Session = None,
        **kwargs
    ) -> bool:
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

    def bank_transaction_has_changes(
        self,
        record_id: Optional[int] = None,
        transaction_id_xero: Optional[str] = None,
        session: Session = None,
        **kwargs
    ) -> bool:
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
    def create_bill_line_item(self, session: Session = None, **kwargs):
        unique_lookup = {}
        if 'parent_id' in kwargs and 'detail_number' in kwargs and ('line_number' in kwargs):
            unique_lookup = {
                'parent_id': kwargs['parent_id'],
                'detail_number': kwargs['detail_number'],
                'line_number': kwargs['line_number']
            }
        return self._create_record(BillLineItem, unique_lookup=unique_lookup, session=session, **kwargs)

    def search_bill_line_items(self, column_names, values, session: Session = None):
        return self._search_records(BillLineItem, column_names, values, session=session)

    def update_bill_line_item(self, bill_line_item_id, session: Session = None, **kwargs):
        return self._update_record(BillLineItem, bill_line_item_id, session=session, **kwargs)

    def delete_bill_line_item(self, bill_line_item_id, session: Session = None, **kwargs) -> bool:
        unique_lookup = {}
        if 'parent_id' in kwargs and 'detail_number' in kwargs and 'line_number' in kwargs:
            unique_lookup = {
                'parent_id': kwargs['parent_id'],
                'detail_number': kwargs['detail_number'],
                'line_number': kwargs['line_number']
            }
        return self._delete_record(BillLineItem, bill_line_item_id, unique_lookup=unique_lookup, session=session)

    def bill_line_item_has_changes(
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
            BillLineItem,
            record_id=record_id,
            unique_filters=unique_filters if not record_id else None,
            session=session,
            **kwargs
        )

    def search_bill_line_item_by_keys(
        self,
        project_number: Optional[int] = None,
        po_number: Optional[int] = None,
        detail_number: Optional[int] = None,
        line_number: Optional[int] = None,
        session: Session = None
    ) -> Union[None, Dict[str, Any], List[Dict[str, Any]]]:
        if not project_number and (not po_number) and (not detail_number) and (not line_number):
            return self.search_bill_line_items([], [], session=session)
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
        return self.search_bill_line_items(col_filters, val_filters, session=session)

    def create_bill_line_item_by_keys(
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
        return self._create_record(BillLineItem, unique_lookup=unique_lookup, session=session, **kwargs)

    def update_bill_line_item_by_keys(
        self,
        project_number: int,
        po_number: int,
        detail_number: int,
        line_number: int,
        session: Session = None,
        **kwargs
    ):
        matches = self.search_bill_line_item_by_keys(project_number, po_number, detail_number, line_number, session=session)
        if not matches:
            return None
        if isinstance(matches, list):
            match = matches[0]
        else:
            match = matches
        return self.update_bill_line_item(match['id'], session=session, **kwargs)
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

    def invoice_has_changes(
        self,
        record_id: Optional[int] = None,
        invoice_number: Optional[str] = None,
        session: Session = None,
        **kwargs
    ) -> bool:
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
        if not project_number and (not po_number) and (not invoice_number):
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
        # If you have any unique fields for concurrency fallback, populate unique_lookup
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
        if not project_number and (not po_number) and (not detail_number) and (not line_number):
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
        prefix = "[ BATCH OPERATION ] " if session else ""
        self.logger.debug(
            f"{prefix}💵 Searching SpendMoney with columns={column_names}, values={values}, deleted={deleted}")
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
        if not project_number and (not po_number) and (not detail_number) and (not line_number):
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

    def tax_account_has_changes(
        self,
        record_id: Optional[int] = None,
        tax_code: Optional[str] = None,
        session: Session = None,
        **kwargs
    ) -> bool:
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
    def create_xero_bill(self, session: Session = None, **kwargs):
        unique_lookup = {}
        if 'xero_reference_number' in kwargs:
            unique_lookup['xero_reference_number'] = kwargs['xero_reference_number']
        return self._create_record(XeroBill, unique_lookup=unique_lookup, session=session, **kwargs)

    def search_xero_bills(self, column_names, values, session: Session = None):
        return self._search_records(XeroBill, column_names, values, session=session)

    def update_xero_bill(self, xero_bill_id, session: Session = None, **kwargs):
        return self._update_record(XeroBill, xero_bill_id, session=session, **kwargs)

    def delete_xero_bill(self, xero_bill_id, session: Session = None, **kwargs) -> bool:
        unique_lookup = {}
        if 'xero_reference_number' in kwargs:
            unique_lookup['xero_reference_number'] = kwargs['xero_reference_number']
        return self._delete_record(XeroBill, xero_bill_id, unique_lookup=unique_lookup, session=session)

    def xero_bill_has_changes(
        self,
        record_id: Optional[int] = None,
        xero_reference_number: Optional[str] = None,
        session: Session = None,
        **kwargs
    ) -> bool:
        unique_filters = {}
        if xero_reference_number is not None:
            unique_filters['xero_reference_number'] = xero_reference_number
        return self._has_changes_for_record(
            XeroBill,
            record_id=record_id,
            unique_filters=unique_filters if not record_id else None,
            session=session,
            **kwargs
        )

    def search_xero_bill_by_keys(
        self,
        project_number: Optional[int] = None,
        po_number: Optional[int] = None,
        detail_number: Optional[int] = None,
        session: Session = None
    ) -> Union[None, Dict[str, Any], List[Dict[str, Any]]]:
        if not project_number and (not po_number) and (not detail_number):
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

    def create_xero_bill_by_keys(
        self,
        project_number: int,
        po_number: int,
        detail_number: int,
        session: Session = None,
        **kwargs
    ):
        kwargs.update({
            'project_number': project_number,
            'po_number': po_number,
            'detail_number': detail_number
        })
        unique_lookup = {
            'project_number': project_number,
            'po_number': po_number,
            'detail_number': detail_number
        }
        return self._create_record(XeroBill, unique_lookup=unique_lookup, session=session, **kwargs)

    def update_xero_bill_by_keys(
        self,
        project_number: int,
        po_number: int,
        detail_number: int,
        session: Session = None,
        **kwargs
    ):
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

    def user_has_changes(
        self,
        record_id: Optional[int] = None,
        username: Optional[str] = None,
        session: Session = None,
        **kwargs
    ) -> bool:
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
        unique_lookup = {}
        return self._create_record(TaxLedger, unique_lookup=unique_lookup, session=session, **kwargs)

    def search_tax_ledgers(self, column_names=None, values=None, session: Session = None):
        return self._search_records(TaxLedger, column_names, values, session=session)

    def update_tax_ledger(self, ledger_id, session: Session = None, **kwargs):
        return self._update_record(TaxLedger, ledger_id, session=session, **kwargs)

    def delete_tax_ledger(self, ledger_id, session: Session = None, **kwargs) -> bool:
        return self._delete_record(TaxLedger, ledger_id, unique_lookup=None, session=session)

    def tax_ledger_has_changes(
        self,
        record_id: Optional[int] = None,
        name: Optional[str] = None,
        session: Session = None,
        **kwargs
    ) -> bool:
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
        unique_lookup = {}
        return self._create_record(BudgetMap, unique_lookup=unique_lookup, session=session, **kwargs)

    def search_budget_maps(self, column_names=None, values=None, session: Session = None):
        return self._search_records(BudgetMap, column_names, values, session=session)

    def update_budget_map(self, map_id, session: Session = None, **kwargs):
        return self._update_record(BudgetMap, map_id, session=session, **kwargs)

    def delete_budget_map(self, map_id, session: Session = None, **kwargs) -> bool:
        return self._delete_record(BudgetMap, map_id, unique_lookup=None, session=session)

    def budget_map_has_changes(
        self,
        record_id: Optional[int] = None,
        map_name: Optional[str] = None,
        session: Session = None,
        **kwargs
    ) -> bool:
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

#endregion  # End Class Definition