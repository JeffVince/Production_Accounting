# region 1: Imports
import copy
import logging
import os
from typing import Any

from database.database_util import DatabaseOperations
from files_dropbox.dropbox_service import DropboxService
from files_monday.monday_service import monday_service
from files_xero.xero_services import xero_services
from utilities.singleton import SingletonMeta
# endregion


#TODO get xero bills to sync and make sure to check DB for changes first
#TODO make sure we don't sync contacts with APIs if they aren't different from DB
#TODO test full Showbiz pipeline
#TODO move to postgres
#TODO populate PO items in monday with Dropbox Folders
#TODO add webhook CRUD from Xero and Monday
#TODO connect AI Agent to Monday / POSTGRES
#TODO add tokens to database instead of .env





# region 2: BudgetService Class Definition
# noinspection PyBroadException
def chunk_list(items, chunk_size=500):
    for i in range(0, len(items), chunk_size):
        yield items[i:i + chunk_size]


# noinspection PyTypedDict
def transform_detail_item(item):
    """
    Transform a dictionary (format A) to match the DB version (dict B).

    Mapping:
      - "project_number", "po_number", "detail_number", and "line_number" are converted to integers.
      - "account" is renamed to "account_code".
      - "date" is renamed and parsed as "transaction_date".
      - "due date" is renamed and parsed as "due_date".
      - Other keys ("vendor", "payment_type", "state", "description", "rate", "quantity", "ot", "fringes")
        are retained as provided.
      - If present, "pulse_id" is also preserved.
    Unwanted keys ("detail_item_id", "total", "ot") are removed if present.
    """
    # Remove unwanted fields if they exist
    for key in ("detail_item_id", "total"):
        item.pop(key, None)

    transformed = {}

    # Convert and map numeric fields
    try:
        transformed["id"] = int(item.get("id"))
    except (ValueError, TypeError):
        transformed["id"] = None

    try:
        transformed["project_number"] = int(item.get("project_number"))
    except (ValueError, TypeError):
        transformed["project_number"] = None

    try:
        transformed["po_number"] = int(item.get("po_number"))
    except (ValueError, TypeError):
        transformed["po_number"] = None

    try:
        transformed["detail_number"] = int(item.get("detail_number"))
    except (ValueError, TypeError):
        transformed["detail_number"] = None

    try:
        transformed["line_number"] = int(item.get("line_number"))
    except (ValueError, TypeError):
        transformed["line_number"] = None

    # Rename "account" to "account_code"
    transformed["account_code"] = item.get("account")

    # Map remaining string fields
    transformed["vendor"] = item.get("vendor")
    transformed["payment_type"] = item.get("payment_type")
    transformed["state"] = item.get("state")
    transformed["description"] = item.get("description")

    # Use the date string directly for transaction_date
    date_str = item.get("date")
    transformed["transaction_date"] = date_str if date_str else None

    # Use the due date string directly for due_date
    due_date_str = item.get("due date")
    transformed["due_date"] = due_date_str if due_date_str else None

    # Map numeric values for rate, quantity, ot, and fringes
    transformed["rate"] = item.get("rate")
    transformed["quantity"] = item.get("quantity")
    transformed["ot"] = item.get("ot")
    transformed["fringes"] = item.get("fringes")

    # Preserve pulse_id if present so that subsequent updates know this record exists in Monday
    if "pulse_id" in item:
        transformed["pulse_id"] = item["pulse_id"]

    return transformed


################################################################
#  1) Relevant contact fields from your Contact DB model
################################################################
CONTACT_FIELDS_STRING = [
    "name", "vendor_status", "payment_details", "vendor_type",
    "email", "phone", "address_line_1", "address_line_2",
    "city", "zip", "region", "country", "tax_type", "tax_number",
]
# For numeric fields, we want to allow int comparison if new_data has an int
# or int-like string (pulse_id, tax_form_id). xero_id can be treated as string only.
CONTACT_FIELDS_INT = ["tax_form_id", "pulse_id"]
CONTACT_FIELD_XERO_ID = "xero_id"  # We'll treat as a string, ignoring empties.


################################################################
#  2) Compare an existing DB contact vs. new data
################################################################
def contact_has_diff(db_contact: dict, new_data: dict) -> bool:
    """
    Return True if there's a difference for any relevant field, ignoring
    empty fields in `new_data`.

    - If new_data[field] is None, empty string, or blank => we skip it (no difference).
    - Otherwise, we compare to db_contact[field], case-insensitive for strings
      and exact for numeric fields. If different => return True.

    We skip created_at/updated_at comparisons, focusing on relevant contact columns.
    """
    # 2.1) Compare string fields
    for field in CONTACT_FIELDS_STRING:
        new_val = (new_data.get(field) or "").strip()
        if not new_val:
            # empty => skip
            continue
        db_val = (db_contact.get(field) or "").strip()
        # For case-insensitive compares on certain fields (like name, email, city,...):
        if new_val.lower() != db_val.lower():
            return True

    # 2.2) Compare integer fields
    for field in CONTACT_FIELDS_INT:
        new_val_raw = new_data.get(field)
        if new_val_raw in (None, ""):
            # skip empty
            continue
        try:
            new_val_int = int(new_val_raw)
        except (ValueError, TypeError):
            # new data isn't a valid int => treat as difference
            return True

        db_val_raw = db_contact.get(field)
        if db_val_raw is None:
            db_val_int = None
        else:
            try:
                db_val_int = int(db_val_raw)
            except (ValueError, TypeError):
                db_val_int = None

        if new_val_int != db_val_int:
            return True

    # 2.3) Compare xero_id as a string
    if CONTACT_FIELD_XERO_ID in new_data:
        new_xid = (new_data.get(CONTACT_FIELD_XERO_ID) or "").strip()
        if new_xid:
            db_xid = (db_contact.get(CONTACT_FIELD_XERO_ID) or "").strip()
            if new_xid != db_xid:
                return True

    return False


################################################################
#  3) Build a dict of updates from new_data, ignoring empties
################################################################
def prepare_contact_update_dict(db_contact: dict, new_data: dict) -> dict:
    """
    Build a dict of fields to update in DB from new_data (ignoring empty fields),
    only including those that differ from what's in db_contact.
    """
    updates = {}

    # 3.1) Handle string fields
    for field in CONTACT_FIELDS_STRING:
        new_val = (new_data.get(field) or "").strip()
        if not new_val:
            # empty => skip
            continue
        db_val = (db_contact.get(field) or "").strip()
        if str(new_val.lower()) != str(db_val.lower()):
            updates[field] = str(new_val)

    # 3.2) Handle integer fields
    for field in CONTACT_FIELDS_INT:
        new_val_raw = new_data.get(field)
        if new_val_raw in (None, ""):
            continue
        try:
            new_val_int = int(new_val_raw)
        except (ValueError, TypeError):
            continue
        db_val_raw = db_contact.get(field)
        if db_val_raw is None:
            db_val_int = None
        else:
            try:
                db_val_int = int(db_val_raw)
            except (ValueError, TypeError):
                db_val_int = None
        if new_val_int != db_val_int:
            updates[field] = new_val_int

    # 3.3) xero_id
    if CONTACT_FIELD_XERO_ID in new_data:
        new_xid = (new_data.get(CONTACT_FIELD_XERO_ID) or "").strip()
        if new_xid:
            db_xid = (db_contact.get(CONTACT_FIELD_XERO_ID) or "").strip()
            if new_xid != db_xid:
                updates[CONTACT_FIELD_XERO_ID] = new_xid

    return updates


# noinspection PyBroadException
class BudgetService(metaclass=SingletonMeta):
    """
    Aggregator logic (previously in budget_service), now called BudgetService.

    Responsibilities:
      - Checking aggregator status (STARTED/COMPLETED)
      - Summation logic for detail items vs. invoice
      - Setting detail items state to RTP
      - Updating XeroBill date ranges
      - Searching aggregator logs (po_logs)
    """

    # region 2.1: Constructor
    def __init__(self):
        self.PROJECT_NUMBER = None
        try:
            self.logger = logging.getLogger('budget_logger')
            self.db_ops = DatabaseOperations()
            self.xero_services = xero_services
            self.dropbox_service = DropboxService()
            self.monday_service = monday_service
            self.logger.info("🧩 BudgetService (aggregator logic) initialized!")
        except Exception:
            logging.exception("Error initializing BudgetService.", exc_info=True)
            raise

    # endregion

    # region 2.2: Process Contact Aggregator
    def process_contact_aggregator(self, contacts_data: list[dict], session):
        """
        Aggregator for CONTACTS with a single commit at the end.
        Only upsert contacts to Xero & Monday if we detect differences from
        the DB record, ignoring empty new fields.
        """
        try:
            self.logger.info("[Contact Aggregator] START => Processing contact data.")
            if not contacts_data:
                self.logger.info("[Contact Aggregator] No contacts provided; nothing to do.")
                return

            try:
                self.logger.info("🔎 Creating/updating DB contacts and sending to Xero & Monday...")

                # region 2.2.1: Fetch Existing Contacts
                try:
                    all_db_contacts = self.db_ops.search_contacts(session=session)
                except Exception:
                    self.logger.exception("Exception searching contacts.", exc_info=True)
                    all_db_contacts = []
                if not all_db_contacts:
                    self.logger.debug("📝 No existing contacts found in DB.")
                else:
                    self.logger.debug(f"📝 Found {len(all_db_contacts)} existing contacts.")
                # endregion

                # region 2.2.2: Process Each Contact
                for contact_item in contacts_data:
                    try:
                        # We at least need a name for fuzzy matching
                        in_name = (contact_item.get('name') or '').strip()
                        if not in_name:
                            self.logger.warning("🚫 'name' missing in contact_item; skipping record.")
                            continue

                        contact_id = None
                        matched_db_contact = None

                        if all_db_contacts:
                            try:
                                fuzzy_matches = self.db_ops.find_contact_close_match(in_name, all_db_contacts)
                                if fuzzy_matches:
                                    matched_db_contact = fuzzy_matches[0]
                                    contact_id = matched_db_contact['id']
                                    self.logger.debug(
                                        f"✅ Fuzzy matched contact ID={contact_id} for name='{in_name}'"
                                    )
                            except Exception:
                                self.logger.exception("Exception during fuzzy matching.", exc_info=True)

                        # If not found in DB => create new
                        if not contact_id:
                            self.logger.info(f"🆕 Creating new contact for '{in_name}'")
                            new_ct = self.db_ops.create_contact(session=session, **contact_item)
                            if not new_ct:
                                self.logger.error(f"❌ Could not create contact for '{in_name}'.")
                                continue
                            contact_id = new_ct['id']
                            matched_db_contact = new_ct
                            self.logger.info(f"🎉 Created contact ID={contact_id}")

                            # Since it's new, definitely push to Xero & Monday
                            try:
                                self.xero_services.buffered_upsert_contact(matched_db_contact)
                            except Exception:
                                self.logger.exception("Exception buffering Xero upsert.", exc_info=True)

                            try:
                                self.monday_service.buffered_upsert_contact(matched_db_contact)
                            except Exception:
                                self.logger.exception("Exception buffering Monday upsert.", exc_info=True)

                        else:
                            # We have an existing DB contact. Check for differences ignoring empty new fields.
                            if not matched_db_contact:
                                # In case fuzzy matched but didn't store record
                                matched_db_contact = self.db_ops.search_contacts(
                                    ["id"], [contact_id], session=session
                                )
                                if isinstance(matched_db_contact, list) and matched_db_contact:
                                    matched_db_contact = matched_db_contact[0]

                            # If there's no difference, skip upserts
                            if not contact_has_diff(matched_db_contact, contact_item):
                                self.logger.debug(
                                    f"🟰 No changes for '{matched_db_contact['name']}' => skip Xero/Monday."
                                )
                                continue

                            # If differences exist, update DB contact
                            updated_fields = prepare_contact_update_dict(matched_db_contact, contact_item)
                            if updated_fields:
                                self.db_ops.update_contact(contact_id, session=session, **updated_fields)
                                # Re-fetch so we have updated record
                                db_contact = self.db_ops.search_contacts(["id"], [contact_id], session=session)
                                if isinstance(db_contact, list) and db_contact:
                                    db_contact = db_contact[0]
                                else:
                                    db_contact = matched_db_contact
                            else:
                                db_contact = matched_db_contact

                            # Then push to Xero & Monday
                            try:
                                self.xero_services.buffered_upsert_contact(db_contact)
                            except Exception:
                                self.logger.exception("Exception buffering Xero upsert.", exc_info=True)
                            try:
                                self.monday_service.buffered_upsert_contact(db_contact)
                            except Exception:
                                self.logger.exception("Exception buffering Monday upsert.", exc_info=True)

                    except Exception:
                        self.logger.exception("Error processing a contact record.", exc_info=True)
                # endregion

                # region 2.2.3: Final Batch Upsert
                try:
                    self.logger.info("📤 Executing batch upsert to Xero & Monday.")
                    self.xero_services.execute_batch_upsert_contacts(self.xero_services.contact_upsert_queue)
                    # TODO self.monday_service.execute_batch_upsert_contacts()
                except Exception:
                    self.logger.exception("Exception during final batch upsert.", exc_info=True)
                # endregion

            except Exception:
                self.logger.exception("Exception in contact aggregator.", exc_info=True)
                raise

        except Exception:
            self.logger.exception("General exception in process_contact_aggregator.", exc_info=True)
            raise

    # endregion

    # region 2.3: Process Purchase Orders Aggregator
    def process_aggregator_pos(self, po_data: dict, session):
        """
        Aggregator for PURCHASE ORDERS with a single commit at the end.
        """
        try:
            self.logger.info("🚀 START => Processing PO aggregator data.")
            if not po_data or not po_data.get("main_items"):
                self.logger.info("🤷 No main_items provided; nothing to do.")
                return

            po_records_info = []
            self.logger.info("[PO Aggregator] Creating/updating POs in DB.")

            # region 2.3.1: Fetch Contacts for Fuzzy Matching
            all_contacts = self.db_ops.search_contacts(session=session)
            # endregion

            # region 2.3.2: Process Each PO Item
            for item in po_data["main_items"]:
                if not item:
                    continue

                project_number = item.get("project_number")
                po_number = item.get("po_number")
                raw_po_type = item.get("po type", "INV")
                description = item.get("description", "")
                vendor_name = item.get("contact_name")

                if not po_number:
                    self.logger.warning("🤔 Missing po_number; skipping item.")
                    continue

                po_type = "INV" if raw_po_type == "PROJ" else raw_po_type

                # region 2.3.2.1: Ensure Project Exists
                project_record = self.db_ops.search_projects(["project_number"], [project_number], session=session)
                if not project_record:
                    self.logger.warning(f"⚠️ Project {project_number} not found; creating new project.")
                    project_record = self.db_ops.create_project(
                        session=session,
                        project_number=project_number,
                        name=f"{project_number}_untitled",
                        status="Active",
                        user_id=1,
                        tax_ledger=14,
                        budget_map_id=1
                    )
                    if not project_record:
                        self.logger.warning("❌ Could not create project; skipping PO.")
                        continue
                    else:
                        self.logger.info(f"🌱 Created Project ID={project_record['id']}")
                if isinstance(project_record, list) and project_record:
                    project_record = project_record[0]
                project_id = project_record["id"]
                # endregion

                # region 2.3.2.2: Lookup or Fuzzy Match Contact
                contact_id = None
                if vendor_name:
                    found_contact = self.db_ops.search_contacts(["name"], [vendor_name], session=session)
                    if found_contact:
                        if isinstance(found_contact, list) and found_contact:
                            found_contact = found_contact[0]
                        contact_id = found_contact.get("id")
                    else:
                        fuzzy_matches = self.db_ops.find_contact_close_match(vendor_name, all_contacts)
                        if fuzzy_matches:
                            best_match = fuzzy_matches[0]
                            self.logger.warning(
                                f"⚠️ Fuzzy matched contact '{vendor_name}' to '{best_match.get('name')}'")
                            contact_id = best_match.get("id")
                else:
                    self.logger.warning("⚠️ No contact_name provided; using default naming.")
                    vendor_name = "PO LOG Naming Error"
                # endregion

                # region 2.3.2.3: Create or Update PO
                existing = self.db_ops.search_purchase_order_by_keys(project_number, po_number, session=session)
                if not existing:
                    self.logger.info("🌱 Creating new PO in DB.")
                    new_po = self.db_ops.create_purchase_order_by_keys(
                        project_number=project_number,
                        po_number=po_number,
                        session=session,
                        description=description,
                        po_type=po_type,
                        contact_id=contact_id,
                        project_id=project_id,
                        vendor_name=vendor_name
                    )
                    if new_po:
                        self.logger.info(f"✅ Created PO ID={new_po['id']}")
                        po_records_info.append(new_po)
                    else:
                        self.logger.warning("❌ Failed to create PO.")
                else:
                    if isinstance(existing, list) and existing:
                        existing = existing[0]
                    po_id = existing["id"]
                    if self.db_ops.purchase_order_has_changes(
                            project_number=project_number,
                            po_number=po_number,
                            session=session,
                            description=description,
                            po_type=po_type,
                            contact_id=contact_id,
                            project_id=project_id,
                            vendor_name=vendor_name
                    ):
                        self.logger.info(f"🔄 Updating PO ID={po_id}.")
                        updated_po = self.db_ops.update_purchase_order(
                            po_id,
                            session=session,
                            description=description,
                            po_type=po_type,
                            contact_id=contact_id,
                            project_id=project_id,
                            vendor_name=vendor_name
                        )
                        if updated_po:
                            self.logger.info(f"🔄 Updated PO ID={updated_po['id']}")
                            po_records_info.append(updated_po)
                        else:
                            self.logger.warning("❌ Failed to update PO.")
                    else:
                        if not existing.get("pulse_id"):
                            self.logger.info(f"🆕 PO ID={po_id} missing pulse_id; upserting to Monday.")
                            po_records_info.append(existing)
                        else:
                            self.logger.info(f"🏳️‍🌈 No changes to PO ID={po_id}; already in Monday.")
                session.flush()
                # endregion
            # endregion

            self.logger.info("[PO Aggregator] All PO items processed.")

            # region 2.3.3: Monday Upsert for POs
            for po_record in po_records_info:
                self.monday_service.buffered_upsert_po(po_record)

            created_POs = self.monday_service.execute_batch_upsert_pos()

            if created_POs:
                for po_obj in created_POs:
                    if 'column_values' in po_obj and isinstance(po_obj['column_values'], list):
                        po_obj['column_values'] = {col['id']: col for col in po_obj['column_values']}
                        project_number = po_obj["column_values"]["project_id"]["text"]
                        po_number = po_obj["column_values"]["numeric__1"]["text"]
                        pulse_id = po_obj["id"]
                        self.db_ops.update_purchase_order_by_keys(project_number=project_number, po_number=po_number,
                                                                  pulse_id=pulse_id, session=session)
            # endregion
            session.commit()
            self.logger.info("[PO Aggregator] PO processing complete.")
        except Exception as e:
            self.logger.error(f"❌ Error in PO aggregator: {str(e)}")
            session.rollback()
            raise

    # endregion

    # region 2.4: Process Detail Item Aggregator
    def process_aggregator_detail_items(self, po_log_data: dict, session, chunk_size: int = 500):
        """
        Aggregator for DETAIL ITEMS with minimal DB queries and in-memory logic.
        Ensures integer casting for detail_number and line_number to avoid duplicates.
        """
        try:
            self.logger.info("[Detail Aggregator] START => Processing detail items.")

            # region 2.4.1: Input Gathering
            if not po_log_data or not po_log_data.get("detail_items"):
                self.logger.info("[Detail Aggregator] No detail_items provided; returning.")
                return

            detail_items_input = []
            detail_item_keys = []
            receipt_keys = set()  # For CC/PC items
            invoice_keys = set()  # For INV/PROF items
            for d_item in po_log_data["detail_items"]:
                if not d_item:
                    continue

                project_number = d_item.get("project_number")
                po_number = d_item.get("po_number")
                raw_detail_number_id = d_item.get("detail_item_id")
                if not raw_detail_number_id:
                    raw_detail_number_id = d_item.get("detail_number")
                if not raw_detail_number_id:
                    self.logger.warning("❌ detail_item_id missing; skipping item.")
                    continue
                detail_number = int(raw_detail_number_id)
                raw_line_number = d_item.get("line_number", 0)
                line_number = int(raw_line_number)
                d_item["payment_type"] = (d_item.get("payment_type") or "").upper()

                if d_item["payment_type"] in ["CC", "PC"]:
                    receipt_keys.add((project_number, po_number, detail_number))
                if d_item["payment_type"] in ["INV", "PROF"]:
                    invoice_keys.add((project_number, po_number, detail_number))

                d_item["detail_number"] = detail_number
                d_item["line_number"] = line_number
                d_item["ot"] = d_item["ot"]
                detail_items_input.append(d_item)
                detail_item_keys.append({
                    "project_number": project_number,
                    "po_number": po_number,
                    "detail_number": detail_number,
                    "line_number": line_number
                })

            self.logger.info(f"🛠️ Input Gathering complete: {len(detail_items_input)} detail items collected.")
            # endregion

            # region 2.4.2: Bulk Fetch Existing Data

            # Compute additional keys for Spend Money and Xero Bills
            spend_money_keys = set()
            xero_bill_keys = set()
            for d_item in detail_items_input:
                if d_item.get("payment_type") in ["CC", "PC"]:
                    key = (
                        int(d_item["project_number"]),
                        int(d_item["po_number"]),
                        int(d_item["detail_number"]),
                        int(d_item["line_number"])
                    )
                    spend_money_keys.add(key)
                if d_item.get("payment_type") in ["INV", "PROF", "PROJ"]:
                    key = (
                        d_item.get("project_number"),
                        d_item.get("po_number"),
                        d_item.get("detail_number")
                    )
                    xero_bill_keys.add(key)

            # 2.4.2.1: Existing Detail Items
            existing_items = self.db_ops.batch_search_detail_items_by_keys(detail_item_keys)
            existing_map_OG = {}
            for item in existing_items:
                key = (
                    item.get("project_number"),
                    item.get("po_number"),
                    item.get("detail_number"),
                    item.get("line_number")
                )
                existing_map_OG[key] = item
            self.logger.info(f"🔍 Found {len(existing_map_OG)} existing detail items in DB.")

            # 2.4.2.2: Fetch Receipts for CC/PC Items
            receipt_map_OG = {}
            if receipt_keys:
                self.logger.info(f"💳 Bulk fetching receipts for {len(receipt_keys)} keys.")
                receipt_list = self.db_ops.batch_search_receipts_by_keys(list(receipt_keys), session=session)
                for r in receipt_list:
                    rk = (r.get("project_number"), r.get("po_number"), r.get("detail_number"), r.get("line_number"))
                    receipt_map_OG[rk] = r
            else:
                self.logger.info("💳 No receipt keys to fetch.")

            # 2.4.2.3: Fetch Invoices for INV/PROF Items
            invoice_map_OG = {}
            if invoice_keys:
                self.logger.info(f"📑 Bulk fetching invoices for {len(invoice_keys)} keys.")
                invoice_list = self.db_ops.batch_search_invoices_by_keys(list(invoice_keys), session=session)
                for inv in invoice_list:
                    k = (
                        int(inv.get("project_number")),
                        int(inv.get("po_number")),
                        int(inv.get("invoice_number"))
                    )
                    invoice_map_OG[k] = inv
            else:
                self.logger.info("📑 No invoice keys to fetch.")

            # 2.4.2.4: Fetch POs for Pulse IDs
            unique_po_keys = {
                (di["project_number"], di["po_number"])
                for di in detail_items_input if di.get("project_number") and di.get("po_number")
            }
            po_map_OG = {}
            if unique_po_keys:
                unique_po_keys_list = list(unique_po_keys)
                project_number = int(unique_po_keys_list[0][0])
                po_list = self.db_ops.search_purchase_order_by_keys(project_number=project_number, session=session)
                if po_list:
                    for p in po_list:
                        po_map_OG[(p["project_number"], p["po_number"])] = p
                self.logger.info(f"✅ Bulk-fetched {len(po_map_OG)} POs for pulse IDs.")

            # 2.4.2.5: Fetch Spend Money, Xero Bills, and Xero Bill Line Items
            spend_money_map_OG = {}
            if spend_money_keys:
                self.logger.info(f"💰 Bulk fetching Spend Money records for {len(spend_money_keys)} keys.")
                spend_money_list = self.db_ops.batch_search_spend_money_by_keys(list(spend_money_keys), session=session)
                for sm in spend_money_list:
                    key = (
                        int(sm.get("project_number")),
                        int(sm.get("po_number")),
                        int(sm.get("detail_number")),
                        int(sm.get("line_number"))
                    )
                    spend_money_map_OG[key] = sm
            else:
                self.logger.info("💰 No Spend Money keys to fetch.")

            xero_bill_map_OG = {}
            if xero_bill_keys:
                self.logger.info(f"📄 Bulk fetching Xero Bills for {len(xero_bill_keys)} keys.")
                xero_bill_list = self.db_ops.batch_search_xero_bills_by_keys(list(xero_bill_keys), session=session)
                for xb in xero_bill_list:
                    key = (
                        int(xb.get("project_number")),
                        int(xb.get("po_number")),
                        int(xb.get("detail_number"))
                    )
                    xero_bill_map_OG[key] = xb
            else:
                self.logger.info("📄 No Xero Bill keys to fetch.")

            xero_bill_line_items_map_OG = {}
            xero_bill_ids = [xb["id"] for xb in xero_bill_map_OG.values() if xb.get("id")]
            if xero_bill_ids:
                self.logger.info(f"📑 Bulk fetching Xero Bill Line Items for {len(xero_bill_ids)} Xero Bills.")
                xero_bill_line_items_list = self.db_ops.batch_search_xero_bill_line_items_by_xero_bill_ids(
                    xero_bill_ids, session=session
                )

                for xbl in xero_bill_line_items_list:
                    xb_id = xbl.get("xero_bill_id")
                    if xb_id:
                        if xb_id not in xero_bill_line_items_map_OG:
                            xero_bill_line_items_map_OG[xb_id] = []
                        xero_bill_line_items_map_OG[xb_id].append(xbl)
            else:
                self.logger.info("📑 No Xero Bills found; skipping Xero Bill Line Items fetch.")

            # 2.4.2.6: Fetch Project-specific Accounts and Tax Accounts
            project_accounts_map_OG = {}
            project_tax_accounts_map_OG = {}
            unique_project_numbers = {
                d_item.get("project_number") for d_item in detail_items_input if d_item.get("project_number")
            }
            for project_number in unique_project_numbers:
                try:
                    project_record = self.db_ops.search_projects(["project_number"], [project_number], session=session)
                    if not project_record:
                        self.logger.warning(f"No project record found for project number: {project_number}")
                        continue
                    if isinstance(project_record, list):
                        project_record = project_record[0]
                    budget_map_id = project_record.get("budget_map_id")
                    tax_ledger_id = project_record.get("tax_ledger")
                    tax_accounts = (
                        self.db_ops.search_tax_accounts(["tax_ledger_id"], [tax_ledger_id], session=session)
                        if tax_ledger_id
                        else []
                    )
                    tax_account_ids = [ta.get("id") for ta in tax_accounts]
                    if budget_map_id:
                        accounts = self.db_ops.search_account_codes(
                            ["budget_map_id", 'tax_id'],
                            [budget_map_id, tax_account_ids],
                            session=session
                        )
                    else:
                        accounts = []
                    project_accounts_map_OG[project_number] = accounts
                    project_tax_accounts_map_OG[project_number] = tax_accounts
                    self.logger.info(
                        f"Fetched {len(accounts)} accounts and {len(tax_accounts)} tax accounts "
                        f"for project {project_number}"
                    )
                except Exception:
                    self.logger.exception(
                        f"Error fetching project-specific accounts for project {project_number}",
                        exc_info=True
                    )

            # 2.4.2.7: Fetch Contacts for Detail Item Linking
            contact_map_OG = {}
            vendor_names = {d_item.get("vendor") for d_item in detail_items_input if d_item.get("vendor")}
            if vendor_names:
                try:
                    contacts_result = self.db_ops.search_contacts(["name"], [list(vendor_names)], session=session)
                    if contacts_result:
                        if not isinstance(contacts_result, list):
                            contacts_result = [contacts_result]
                        for contact in contacts_result:
                            for po in po_map_OG.values():
                                if po.get("contact_id") == contact.get("id"):
                                    contact["project_number"] = po.get("project_number")
                                    contact["po_number"] = po.get("po_number")
                                    break
                            key = (contact.get("project_number"), contact.get("po_number"))
                            if None not in key:
                                contact_map_OG[key] = contact
                            else:
                                self.logger.warning(
                                    f"Contact {contact.get('id')} does not have project or PO number."
                                )
                        self.logger.info(
                            f"Fetched and processed {len(contact_map_OG)} contacts for detail item linking."
                        )
                    else:
                        self.logger.info("No contacts found for detail item linking.")
                except Exception:
                    self.logger.exception("Error fetching contacts for detail item linking.", exc_info=True)
            # endregion

            # region 2.4.3: In-Memory Processing for CC/PC and INV/PROF items
            copy.deepcopy(existing_map_OG)
            receipt_map_updated = copy.deepcopy(receipt_map_OG)
            invoice_map_updated = copy.deepcopy(invoice_map_OG)
            po_map_updated = copy.deepcopy(po_map_OG)
            spend_money_map_updated = copy.deepcopy(spend_money_map_OG)
            xero_bill_map_updated = copy.deepcopy(xero_bill_map_OG)
            new_xero_bill_line_items = []
            contact_map_updated = copy.deepcopy(contact_map_OG)
            xero_bill_line_items_map_updated = copy.deepcopy(xero_bill_line_items_map_OG)

            # 2.4.3.1: Handle CC/PC Receipt Matching 💳🔍
            for d_item in detail_items_input:
                payment_type = d_item.get("payment_type")
                if payment_type in ["CC", "PC"]:
                    key = (
                        int(d_item["project_number"]),
                        int(d_item["po_number"]),
                        int(d_item["detail_number"]),
                        int(d_item["line_number"]),
                    )
                    sub_total = float(d_item.get("total") or 0.0)
                    if key in receipt_map_updated:
                        receipt_status = (receipt_map_updated[key].get("status") or "PENDING").upper()
                        receipt_total = float(receipt_map_updated[key].get("total") or 0.0)
                        if receipt_status == "PENDING":
                            if abs(receipt_total - sub_total) < 0.0001:
                                receipt_map_updated[key]["status"] = "VERIFIED"
                                d_item["state"] = "REVIEWED"
                                self.logger.info(f"[Receipt: PENDING->VERIFIED] Detail state -> REVIEWED: {key}")
                            else:
                                d_item["state"] = "PO MISMATCH"
                                self.logger.info(f"[Receipt: PENDING mismatch] Detail state -> PO MISMATCH: {key}")
                        elif receipt_status == "VERIFIED":
                            if abs(receipt_total - sub_total) < 0.0001:
                                d_item["state"] = "REVIEWED"
                                self.logger.info(f"[Receipt: VERIFIED match] Detail state -> REVIEWED: {key}")
                            else:
                                d_item["state"] = "PO MISMATCH"
                                self.logger.info(f"[Receipt: VERIFIED mismatch] Detail state -> PO MISMATCH: {key}")
                        elif receipt_status == "REJECTED":
                            self.logger.info(f"[Receipt: REJECTED] No action for detail {key}.")
                        else:
                            self.logger.debug(f"[Receipt: {receipt_status}] Not recognized. Skipping detail {key}.")
                    else:
                        self.logger.debug(f"[Receipt Not Found] for detail {key}. No action.")

            # 2.4.3.2: Handle Spend Money for Reviewed CC/PC Items 💰✅
            for d_item in detail_items_input:
                payment_type = d_item.get("payment_type")
                detail_state = d_item.get("state")
                approved_states = ["REVIEWED", "VERIFIED", "APPROVED"]
                if payment_type in ["CC", "PC"] and detail_state in approved_states:
                    key = (
                        int(d_item["project_number"]),
                        int(d_item["po_number"]),
                        int(d_item["detail_number"]),
                        int(d_item["line_number"])
                    )
                    sub_total = float(d_item.get("total") or 0.0)
                    if key not in spend_money_map_updated:
                        sm_record = {
                            "project_number": int(d_item["project_number"]),
                            "po_number": int(d_item["po_number"]),
                            "detail_number": int(d_item["detail_number"]),
                            "line_number": int(d_item["line_number"]),
                            "state": "AUTHORISED",
                            "amount": sub_total,
                            "description": d_item.get("description", ""),
                            "date": d_item.get("date", ""),
                        }
                        parent_po = po_map_updated.get((int(d_item["project_number"]), int(d_item["po_number"])))
                        if parent_po and parent_po.get("contact_id"):
                            sm_record["contact_id"] = parent_po["contact_id"]
                        account_code = d_item.get("account_code")
                        if account_code:
                            sm_record["tax_code"] = self.get_tax_code_from_account_code(account_code)
                        spend_money_map_updated[key] = sm_record
                        self.logger.info(f"[SpendMoney: CREATE] Created new spend money for detail {key}")
                    else:
                        existing_sm = spend_money_map_updated[key]
                        sm_status = (existing_sm.get("status", "DRAFT")).upper()
                        existing_amount = float(existing_sm.get("amount") or 0.0)
                        if sm_status == "RECONCILED":
                            if abs(existing_amount - sub_total) < 0.0001:
                                d_item["state"] = "RECONCILED"
                                self.logger.info(f"[SpendMoney: RECONCILED match] Detail state->RECONCILED: {key}")
                            else:
                                d_item["state"] = "ISSUE"
                                self.logger.info(f"[SpendMoney: RECONCILED mismatch] Detail state->ISSUE: {key}")
                        elif sm_status in ["DRAFT", "AUTHORIZED", "PAID", "SUBMITTED FOR APPROVAL"]:
                            contact_id = None
                            parent_po = po_map_updated.get((int(d_item["project_number"]), int(d_item["po_number"])))
                            if parent_po and parent_po.get("contact_id"):
                                contact_id = parent_po["contact_id"]
                            account_code = d_item.get("account_code")
                            tax_code = None
                            if account_code:
                                tax_code = self.get_tax_code_from_account_code(account_code)
                            differences_found = False
                            if abs(existing_amount - sub_total) > 0.0001:
                                differences_found = True
                            if existing_sm.get("tax_code") != tax_code:
                                differences_found = True
                            if contact_id and existing_sm.get("contact_id") != contact_id:
                                differences_found = True
                            if existing_sm.get("description", "") != d_item.get("description", ""):
                                differences_found = True
                            if differences_found:
                                self.logger.info(f"[SpendMoney: UPDATE] Differences found; updating record for {key}.")
                                existing_sm["amount"] = sub_total
                                existing_sm["tax_code"] = tax_code
                                if contact_id:
                                    existing_sm["contact_id"] = contact_id
                                existing_sm["description"] = d_item.get("description", "")
                            else:
                                self.logger.debug(f"[SpendMoney: NO-UPDATE] No differences for detail {key}.")

            # 2.4.3.3: Handle Invoice Matching & Status Updates 📑🧾
            invoice_sums_map = {}
            for d_item in detail_items_input:
                payment_type = d_item.get("payment_type")
                if payment_type in ["INV", "PROF", "PROJ"]:
                    key = (
                        int(d_item["project_number"]),
                        int(d_item["po_number"]),
                        int(d_item["detail_number"])
                    )
                    sub_total = float(d_item.get("total") or 0.0)
                    invoice_sums_map.setdefault(key, 0.0)
                    invoice_sums_map[key] += sub_total
            for key, total_of_details in invoice_sums_map.items():
                if key in invoice_map_updated:
                    invoice_obj = invoice_map_updated[key]
                    invoice_status = (invoice_obj.get("status") or "PENDING").upper()
                    invoice_total = float(invoice_obj.get("total") or 0.0)
                    siblings = [
                        d for d in detail_items_input
                        if (int(d["project_number"]), int(d["po_number"]), int(d["detail_number"])) == key
                    ]
                    if invoice_status == "PENDING":
                        if abs(invoice_total - total_of_details) < 0.0001:
                            invoice_obj["status"] = "VERIFIED"
                            for s in siblings:
                                s["state"] = "RTP"
                            self.logger.info(f"[Invoice: PENDING->VERIFIED] siblings => RTP: {key}")
                        else:
                            for s in siblings:
                                s["state"] = "PO MISMATCH"
                            self.logger.info(f"[Invoice: PENDING mismatch] siblings => PO MISMATCH: {key}")
                    elif invoice_status == "REJECTED":
                        self.logger.info(f"[Invoice: REJECTED] No action on detail items for {key}.")
                    elif invoice_status == "VERIFIED":
                        if abs(invoice_total - total_of_details) < 0.0001:
                            for s in siblings:
                                s["state"] = "RTP"
                            self.logger.info(f"[Invoice: VERIFIED match] siblings => RTP: {key}")
                        else:
                            for s in siblings:
                                s["state"] = "PO MISMATCH"
                            self.logger.info(f"[Invoice: VERIFIED mismatch] siblings => PO MISMATCH: {key}")
                    else:
                        self.logger.debug(f"[Invoice: {invoice_status}] Not recognized. No action for {key}.")
                else:
                    self.logger.debug(f"[Invoice: NOT FOUND] for {key}. No action.")

            # 2.4.3.4: Handle Xero Bills for RTP Detail Items 📄💡
            for d_item in detail_items_input:
                payment_type = d_item.get("payment_type")
                detail_state = d_item.get("state")
                if payment_type in ["INV", "PROF", "PROJ"] and detail_state == "RTP":
                    key = (
                        int(d_item["project_number"]),
                        int(d_item["po_number"]),
                        int(d_item["detail_number"])
                    )
                    siblings = [
                        x for x in detail_items_input
                        if (int(x.get("project_number")), int(x.get("po_number")), int(x.get("detail_number"))) == key
                    ]
                    from datetime import datetime, date

                    def to_date(v):
                        if isinstance(v, datetime):
                            return v.date()
                        elif isinstance(v, date):
                            return v
                        elif isinstance(v, str):
                            try:
                                return datetime.fromisoformat(v).date()
                            except:
                                return None
                        return None

                    all_dates = [to_date(x.get("date")) for x in siblings if to_date(x.get("date"))]
                    all_dues = [to_date(x.get("due date")) for x in siblings if to_date(x.get("due date"))]
                    earliest_date = min(all_dates) if all_dates else None
                    latest_due = max(all_dues) if all_dues else None
                    if key not in xero_bill_map_updated:
                        self.logger.info(f"[XeroBill: CREATE] Creating new Xero Bill for key {key}.")
                        new_bill = {
                            "project_number": key[0],
                            "po_number": key[1],
                            "detail_number": key[2],
                            "state": "DRAFT",
                            "transaction_date": earliest_date,
                            "due_date": latest_due,
                            "contact_xero_id": None,
                        }
                        contact_ = contact_map_updated.get((key[0], key[1]))
                        if contact_ and contact_.get("xero_id"):
                            new_bill["contact_xero_id"] = contact_["xero_id"]

                        line_items = []
                        for s in siblings:
                            s = transform_detail_item(s)
                            account_code = s.get("account_code")
                            tax_code = self.get_tax_code_from_account_code(account_code)

                            sub_total = float(s.get("total") or 0.0)
                            line_item = {
                                "description": s.get("description", ""),
                                "quantity": s.get("quantity", 1),
                                "unit_amount": s.get("rate", 0.0),
                                "tax_code": tax_code,
                                "line_amount": sub_total,
                                "project_number": key[0],
                                "po_number": key[1],
                                "detail_number": key[2],
                                "line_number": s.get("line_number"),
                                "transaction_date": earliest_date,
                                "due_date": latest_due,
                            }
                            line_items.append(line_item)

                        xero_bill_map_updated[key] = new_bill
                        new_xero_bill_line_items.extend(line_items)
                    else:
                        existing_bill = xero_bill_map_updated[key]
                        bill_status = (existing_bill.get("state") or "DRAFT").upper()
                        differences_found = False
                        if earliest_date and existing_bill.get("transaction_date") != earliest_date:
                            differences_found = True
                        if latest_due and existing_bill.get("due_date") != latest_due:
                            differences_found = True
                        parent_po = po_map_updated.get((int(d_item["project_number"]), int(d_item["po_number"])))
                        contact_xero_id = existing_bill.get("contact_xero_id")
                        existing_contact_record = None
                        if parent_po and parent_po.get("contact_id"):
                            existing_contact_record = self.db_ops.search_contacts(["id"], [parent_po["contact_id"]], session=session)
                            if isinstance(existing_contact_record, list):
                                existing_contact_record = existing_contact_record[0]
                            if contact_xero_id != existing_contact_record["xero_id"]:
                                differences_found = True
                        if bill_status in ["DRAFT", "SUBMITTED FOR APPROVAL", "PAID"]:
                            if differences_found:
                                self.logger.info(f"[XeroBill: UPDATE] Updating Xero Bill for key {key}.")
                                existing_bill["transaction_date"] = earliest_date
                                existing_bill["due_date"] = latest_due
                                if existing_contact_record and existing_contact_record.get("xero_id"):
                                    existing_bill["contact_xero_id"] = existing_contact_record["xero_id"]
                            else:
                                self.logger.debug(f"[XeroBill: NO-UPDATE] No changes for key {key}.")
                        elif bill_status in ["RECONCILED", "APPROVED", "AUTHORIZED"]:
                            if differences_found:
                                self.logger.info(
                                    f"[XeroBill: RECONCILED or APPROVED mismatch] "
                                    f"Setting details to ISSUE for key {key}."
                                )
                                for s in siblings:
                                    s["state"] = "ISSUE"
                            else:
                                self.logger.debug(
                                    f"[XeroBill: RECONCILED or APPROVED match] Marking details RECONCILED for key {key}."
                                )
                                for s in siblings:
                                    s["state"] = "RECONCILED"
                        else:
                            self.logger.debug(f"[XeroBill: {bill_status}] Not recognized. No action for {key}.")

            # 2.4.3.4.5: Handle PO Pulse ID --> Detail.Parent_pulse_id
            for d_item in detail_items_input:
                project_number = int(d_item.get("project_number"))
                po_number = int(d_item.get("po_number"))
                matching_po = po_map_updated.get((project_number, po_number))
                if matching_po:
                    d_item["parent_pulse_id"] = matching_po.get("pulse_id")

            # 2.4.3.5: Prepare Data for Detail Item List Update
            updated_detail_items = detail_items_input

            # 2.4.3.6: Prepare Data for Xero Bill List Update
            updated_xero_bills = list(xero_bill_map_updated.values())

            # 2.4.3.7: Prepare Data for Xero Bill Line Item List Update
            updated_xero_bill_line_items = []
            # Include any pre-existing Xero Bill Line Items fetched from DB
            for line_items in xero_bill_line_items_map_updated.values():
                updated_xero_bill_line_items.extend(line_items)
            # Add new Xero Bill Line Items accumulated during processing
            updated_xero_bill_line_items.extend(new_xero_bill_line_items)

            # 2.4.3.7.5: Assign missing line numbers to Xero Bill Line Items from matching detail items
            for d_item in detail_items_input:
                # Only consider invoice-type detail items for this matching
                if d_item.get("payment_type") in ["INV", "PROF", "PROJ"]:
                    composite_key = (
                        int(d_item["project_number"]),
                        int(d_item["po_number"]),
                        int(d_item["detail_number"])
                    )
                    # Proceed only if the detail item has a non-zero line number
                    if d_item.get("line_number"):
                        detail_line_number = int(d_item["line_number"])
                        detail_total = float(d_item.get("total") or 0.0)
                        for xbl in updated_xero_bill_line_items:
                            if (
                                int(xbl.get("project_number", 0)) == composite_key[0] and
                                int(xbl.get("po_number", 0)) == composite_key[1] and
                                int(xbl.get("detail_number", 0)) == composite_key[2]
                            ):
                                # If the Xero Bill Line Item doesn't have a line number yet, try to match by total amount
                                if not xbl.get("line_number"):
                                    xbl_line_amount = float(xbl.get("line_amount") or 0.0)
                                    if abs(detail_total - xbl_line_amount) < 0.0001:
                                        xbl["line_number"] = detail_line_number
                                        self.logger.info(f"[XeroBill Line Item] Assigned line number {detail_line_number} for {composite_key} based on matching total.")

            # 2.4.3.8: Prepare Data for Invoice and Receipt List Update
            updated_invoices = list(invoice_map_updated.values())
            updated_receipts = list(receipt_map_updated.values())

            # 2.4.3.9: Prepare Data for Spend Money List Update
            updated_spend_money = list(spend_money_map_updated.values())
            # endregion

            # region 2.4.4: Bulk Create/Update in DB
            original_detail_map = copy.deepcopy(existing_map_OG)
            original_receipt_map = copy.deepcopy(receipt_map_OG)
            original_invoice_map = copy.deepcopy(invoice_map_OG)
            original_xero_bill_map = copy.deepcopy(xero_bill_map_OG)
            original_spend_money_map = copy.deepcopy(spend_money_map_OG)
            original_xero_bill_line_items_map = {}
            for xb_id, items in xero_bill_line_items_map_OG.items():
                for item in items:
                    key = (xb_id, item.get("line_number"))
                    original_xero_bill_line_items_map[key] = item

            def are_dicts_different(d1, d2):
                # Compare everything except the DB 'id', which is an internal key
                d1_copy = {k_: v for k_, v in d1.items() if k_ != "id"}
                d2_copy = {k_: v for k_, v in d2.items() if k_ != "id"}
                return d1_copy != d2_copy

            detail_items_to_create = []
            detail_items_to_update = []
            detail_items_unchanged = []  # We'll handle these if they have no pulse_id

            for d_item in updated_detail_items:
                # Build a key from (project, po, detail#, line#)
                key = (
                    int(d_item["project_number"]),
                    int(d_item["po_number"]),
                    d_item.get("detail_number"),
                    d_item.get("line_number")
                )
                if key in original_detail_map:
                    # It's an existing DB record => check for differences
                    db_item = original_detail_map[key]
                    if are_dicts_different(d_item, db_item):
                        # We plan to update in DB
                        detail_items_to_update.append(transform_detail_item(d_item))
                    else:
                        # no difference in DB => keep track in detail_items_unchanged
                        # We'll see if it has pulse_id or not
                        # But we also need to transform so we have consistent fields
                        unchanged_transformed = transform_detail_item(d_item)
                        # Make sure we carry over DB 'id' from the existing record
                        unchanged_transformed["id"] = db_item["id"]
                        detail_items_unchanged.append(unchanged_transformed)
                else:
                    # Not in DB => we'll create
                    detail_items_to_create.append(transform_detail_item(d_item))

            # --- Actually insert/update in DB ---
            created_detail_items_db = []
            if detail_items_to_create:
                for chunk in chunk_list(detail_items_to_create, chunk_size):
                    self.logger.debug(f"Creating chunk of {len(chunk)} detail items.")
                    created_sub = self.db_ops.bulk_create_detail_items(chunk, session=session)
                    created_detail_items_db.extend(created_sub)
                    session.flush()

            updated_detail_items_db = []
            if detail_items_to_update:
                for chunk in chunk_list(detail_items_to_update, chunk_size):
                    self.logger.debug(f"Updating chunk of {len(chunk)} detail items.")
                    updated_sub = self.db_ops.bulk_update_detail_items(chunk, session=session)
                    updated_detail_items_db.extend(updated_sub)
                    session.flush()

            # Build a map so we can retrieve the DB 'id' for newly created or updated items
            id_map = {}
            for record in created_detail_items_db:
                key = (
                    int(record["project_number"]),
                    int(record["po_number"]),
                    record["detail_number"],
                    record["line_number"]
                )
                id_map[key] = record["id"]

            for record in updated_detail_items_db:
                key = (
                    int(record["project_number"]),
                    int(record["po_number"]),
                    record["detail_number"],
                    record["line_number"]
                )
                id_map[key] = record["id"]

            # Update the 'id' field in those same dicts
            for record in created_detail_items_db:
                key = (
                    int(record["project_number"]),
                    int(record["po_number"]),
                    record["detail_number"],
                    record["line_number"]
                )
                record["id"] = id_map[key]

            for record in updated_detail_items_db:
                key = (
                    int(record["project_number"]),
                    int(record["po_number"]),
                    record["detail_number"],
                    record["line_number"]
                )
                record["id"] = id_map[key]

            # The items in 'detail_items_unchanged' also need their correct DB 'id'.
            # Because they had no difference, we rely on the original DB item
            for record in detail_items_unchanged:
                key = (
                    int(record["project_number"]),
                    int(record["po_number"]),
                    record["detail_number"],
                    record["line_number"]
                )
                if key in original_detail_map:
                    record["id"] = original_detail_map[key]["id"]

            try:
                session.commit()
                self.logger.info("💾 DB Bulk Create/Update complete for all items. Commit successful.")
            except Exception:
                session.rollback()
                self.logger.exception("Error during DB Bulk Create/Update commit.", exc_info=True)
                raise

            # endregion

            # --- Xero Bills ---
            created_xero_bills_db = []
            updated_xero_bills_db = []

            xero_bills_to_create = []
            xero_bills_to_update = []
            for xb in updated_xero_bills:
                key = (xb["project_number"], xb["po_number"], xb["detail_number"])
                if key in original_xero_bill_map:
                    db_xb = original_xero_bill_map[key]
                    if are_dicts_different(xb, db_xb):
                        xb["id"] = db_xb["id"]
                        xero_bills_to_update.append(xb)
                else:
                    xero_bills_to_create.append(xb)

            try:
                if xero_bills_to_create:
                    created_xero_bills_db = self.db_ops.bulk_create_xero_bills(xero_bills_to_create, session=session)
                    session.flush()
                if xero_bills_to_update:
                    updated_xero_bills_db = self.db_ops.bulk_update_xero_bills(xero_bills_to_update, session=session)
                    session.flush()
            except Exception:
                self.logger.exception("Error during bulk create/update of Xero Bills.", exc_info=True)
                session.rollback()
                raise
            xero_bills_to_upload = []
            if xero_bills_to_create:
                xero_bills_to_upload.extend(xero_bills_to_create)
            if xero_bills_to_update:
                xero_bills_to_upload.extend(xero_bills_to_update)

            # After Xero Bills insertion, map their composite keys to IDs
            bill_id_map = {}
            for bill in created_xero_bills_db:
                identifier = (bill["project_number"], bill["po_number"], bill["detail_number"])
                bill_id_map[identifier] = bill["id"]
            for bill in updated_xero_bills_db:
                identifier = (bill["project_number"], bill["po_number"], bill["detail_number"])
                bill_id_map[identifier] = bill["id"]

            # Now, update each Xero Bill Line Item dict to set the proper parent_id
            for li in updated_xero_bill_line_items:
                key = (li["project_number"], li["po_number"], li["detail_number"])
                parent_id = bill_id_map.get(key)
                if parent_id is None:
                    self.logger.warning(
                        f"No parent Xero Bill found for line item with identifier {key}"
                    )
                else:
                    li["parent_id"] = parent_id

            # --- Xero Bill Line Items ---
            xero_bill_line_items_to_create = {}
            xero_bill_line_items_to_update = {}

            for xbl in updated_xero_bill_line_items:
                bill_id = xbl.get("parent_id")
                key = (bill_id, xbl.get("line_number"))
                if key in original_xero_bill_line_items_map:
                    db_xbl = original_xero_bill_line_items_map[key]
                    if are_dicts_different(xbl, db_xbl):
                        xbl["id"] = db_xbl["id"]
                        if bill_id not in xero_bill_line_items_to_update:
                            xero_bill_line_items_to_update[bill_id] = []
                        xero_bill_line_items_to_update[bill_id].append(xbl)
                else:
                    if bill_id not in xero_bill_line_items_to_create:
                        xero_bill_line_items_to_create[bill_id] = []
                    xero_bill_line_items_to_create[bill_id].append(xbl)

            try:
                for bill_id, items in xero_bill_line_items_to_create.items():
                    if not items:
                        continue
                    self.db_ops.bulk_create_xero_bill_line_items(
                        items, session=session
                    )
                    session.flush()

                for bill_id, items in xero_bill_line_items_to_update.items():
                    if not items:
                        continue
                    self.db_ops.bulk_update_xero_bill_line_items(
                        items, session=session
                    )
                    session.flush()
            except Exception:
                self.logger.exception("Error during bulk create/update of Xero Bill Line Items.", exc_info=True)
                session.rollback()
                raise

            xero_bill_line_items_to_upload = []
            for items in xero_bill_line_items_to_create.values():
                xero_bill_line_items_to_upload.extend(items)
            for items in xero_bill_line_items_to_update.values():
                xero_bill_line_items_to_upload.extend(items)

            # --- Invoices ---
            invoices_to_update = []
            for inv in updated_invoices:
                key = (
                    inv["project_number"],
                    inv["po_number"],
                    inv.get("invoice_number") or inv.get("detail_number")
                )
                if key in original_invoice_map:
                    db_inv = original_invoice_map[key]
                    if are_dicts_different(inv, db_inv):
                        inv["id"] = db_inv["id"]
                        invoices_to_update.append(inv)

            if invoices_to_update:
                self.db_ops.bulk_update_invoices(invoices_to_update, session=session)
                session.flush()

            # --- Receipts ---
            receipts_to_update = []
            for rec in updated_receipts:
                key = (rec["project_number"], rec["po_number"], rec["detail_number"], rec["line_number"])
                if key in original_receipt_map:
                    db_rec = original_receipt_map[key]
                    if are_dicts_different(rec, db_rec):
                        rec["id"] = db_rec["id"]
                        receipts_to_update.append(rec)
                session.flush()

            if receipts_to_update:
                self.db_ops.bulk_update_receipts(receipts_to_update, session=session)
                session.flush()

            # --- Spend Money ---
            spend_money_to_create = []
            spend_money_to_update = []
            for sm in updated_spend_money:
                key = (
                    sm["project_number"],
                    sm["po_number"],
                    sm["detail_number"],
                    sm["line_number"]
                )
                if key in original_spend_money_map:
                    db_sm = original_spend_money_map[key]
                    if are_dicts_different(sm, db_sm):
                        sm["id"] = db_sm["id"]
                        spend_money_to_update.append(sm)
                else:
                    spend_money_to_create.append(sm)

            if spend_money_to_create:
                self.db_ops.bulk_create_spend_money(spend_money_to_create, session=session)
                session.flush()

            if spend_money_to_update:
                self.db_ops.bulk_update_spend_money(spend_money_to_update, session=session)
                session.flush()

            try:
                session.commit()
                self.logger.info("💾 DB Bulk Create/Update complete for all items. Commit successful.")

            except Exception:
                session.rollback()
                self.logger.exception("Error during DB Bulk Create/Update commit.", exc_info=True)
                raise

            # endregion

            # -------------------------------------------------------------
            # region 2.4.5: Prepare "monday_upsert_list" for Subitems
            #
            # We'll unify:
            #   1) newly-created DB items,
            #   2) updated DB items,
            #   3) unchanged items that have no pulse_id yet
            # Any item that already had a pulse_id and is unchanged => we skip.
            #
            # The "updated_detail_items_db" and "created_detail_items_db" lists contain
            # the final DB records after create/update. For unchanged items, we have them in
            # detail_items_unchanged.
            # -------------------------------------------------------------
            monday_upsert_list = []

            # 1) newly-created DB items => always at least create in Monday
            for rec in created_detail_items_db:
                monday_upsert_list.append(rec)

            # 2) updated DB items => we want to push to Monday. If they have pulse_id => update
            #    if they do not => create
            for rec in updated_detail_items_db:
                monday_upsert_list.append(rec)

            # 3) unchanged items that have no pulse_id => still need to create in Monday
            for rec in detail_items_unchanged:
                if not rec.get("pulse_id"):
                    monday_upsert_list.append(rec)

            # If there's absolutely no difference and has a pulse_id,
            # we skip. That means it's already in Monday and unchanged.

            # region 2.4.6: Monday Upsert for Detail Items
            try:
                self.logger.info("[Detail Aggregator] Starting Monday upsert for detail items.")
                self.logger.debug(f"Total items to upsert in Monday: {len(monday_upsert_list)}")

                # Build final "monday_items" from each record
                monday_items = []
                for di in monday_upsert_list:
                    # Rebuild external links logic here
                    file_link = None
                    xero_link = None
                    pay_type = di.get("payment_type")
                    if pay_type in ["CC", "PC"]:
                        key = (
                            int(di.get("project_number")),
                            int(di.get("po_number")),
                            int(di.get("detail_number")),
                            int(di.get("line_number"))
                        )
                        if key in receipt_map_OG:
                            file_link = receipt_map_OG[key].get("file_link")
                        if key in spend_money_map_updated:
                            xero_link = spend_money_map_updated[key].get("xero_link")

                    elif pay_type in ["INV", "PROF"]:
                        key = (
                            int(di.get("project_number")),
                            int(di.get("po_number")),
                            int(di.get("detail_number"))
                        )
                        if key in invoice_map_OG:
                            file_link = invoice_map_OG[key].get("file_link")
                        if key in xero_bill_map_updated:
                            xero_link = xero_bill_map_updated[key].get("xero_link")

                    # Construct the dict for Monday
                    detail_dict = {
                        'id': di.get('id'),
                        'parent_pulse_id': di.get('parent_pulse_id'),
                        'pulse_id': di.get('pulse_id'),
                        'project_number': di.get('project_number'),
                        'po_number': di.get('po_number'),
                        'detail_number': di.get('detail_number'),
                        'line_number': di.get('line_number'),
                        'description': di.get('description'),
                        'quantity': di.get('quantity'),
                        'vendor': di.get("vendor"),
                        'rate': di.get('rate'),
                        'transaction_date': di.get('transaction_date'),  # DB field
                        'due_date': di.get('due_date'),  # DB field
                        'account_code': di.get('account_code'),
                        'file_link': file_link,
                        'xero_link': xero_link,
                        'ot': di.get('ot'),
                        'fringes': di.get('fringes'),
                        'state': di.get('state')
                    }
                    monday_items.append(detail_dict)

                self.logger.info(f"📤 Sending {len(monday_items)} items to Monday upsert in chunks...")
                for chunk in chunk_list(monday_items, 500):
                    self.logger.debug(f"Buffering chunk of {len(chunk)} detail items for Monday upsert.")
                    for detail_dict in chunk:
                        self.monday_service.buffered_upsert_detail_item(detail_dict)

                    # Execute batch upsert for subitems
                    results = self.monday_service.execute_batch_upsert_detail_items()
                    if not results:
                        continue

                    # Collect new pulse_ids from the results
                    pulse_updates = []
                    for subitem_obj in results:
                        db_sub_item = subitem_obj.get("db_sub_item")
                        _monday_item = subitem_obj.get("monday_item")
                        if db_sub_item and db_sub_item.get("id") and _monday_item:
                            self.logger.debug(
                                f"Processing Monday created/updated subitem: DB ID={db_sub_item['id']}, Monday ID={_monday_item['id']}"
                            )
                            pulse_updates.append(
                                {
                                    "id": db_sub_item.get("id"),
                                    "pulse_id": _monday_item["id"],
                                    "parent_pulse_id": db_sub_item["parent_pulse_id"],
                                }
                            )
                        else:
                            self.logger.warning(
                                f"No DB Sub Item or Monday Item found for subitem: {subitem_obj.get('db_sub_item')}"
                            )

                    # Update the DB with the new pulse_ids
                    if pulse_updates:
                        self.db_ops.bulk_update_detail_items(updates=pulse_updates, session=session)
                        session.commit()

            except Exception:
                self.logger.exception("Exception during Monday upsert for detail items.", exc_info=True)
            # endregion
            # -------------------------------------------------------------

            # region 2.4.7: Xero Upsert for Xero Bill, Xero Bill Line Item, Spend Money Item
            try:
                self.logger.info("🔄 Starting Xero Upsert for Xero Bills and associated line items.")
                # Sync Xero Bills (and implicitly their line items via the bill creation process)
                xero_bill_results = xero_services.handle_xero_bill_create_bulk(xero_bills_to_upload, xero_bill_line_items_to_upload, session)
                self.logger.info(f"✅ Synced {len(xero_bill_results)} Xero Bills.")
                self.logger.info("🔄 Starting Xero Upsert for Spend Money items.")
                spend_money_results = xero_services.handle_spend_money_create_bulk(updated_spend_money, session)
                self.logger.info(f"✅ Synced {len(spend_money_results)} Spend Money items.")
            except Exception:
                self.logger.exception("Error during Xero Upsert for bills and spend money items.", exc_info=True)
                session.rollback()
                raise
            # endregion

        except Exception:
            self.logger.exception("Error in process_aggregator_detail_items.", exc_info=True)
            if session:
                session.rollback()
            raise

    # endregion

    # region 2.8: Helper Methods
    def parse_po_log_data(self, po_log: dict) -> list[Any] | dict[str, Any]:
        try:
            #po_log_db_path = po_log["db_path"]
            po_log_filename = po_log["filename"]
            project_number = po_log["project_number"]
            temp_file_path = f'../temp_files/{os.path.basename(po_log_filename)}'
            self.PROJECT_NUMBER = project_number
            # if not os.path.exists(temp_file_path):
            #     self.logger.info('🛠 Not using local temp files? Attempting Dropbox download...')
            #     if not self.dropbox_service.download_file_from_dropbox((po_log_db_path + po_log_filename),
            #                                                            temp_file_path):
            #         return []
            #    self.logger.info(f'📝 Received PO Log file from Dropbox: {po_log_filename}')
            self.logger.info('🔧 Passing parsed PO log data to DB aggregator...')
            main_items, detail_items, contacts = self.dropbox_service.extract_data_from_po_log(temp_file_path,
                                                                                               project_number)
            return {
                "main_items": main_items,
                "detail_items": detail_items,
                "contacts": contacts
            }
        except Exception:
            self.logger.exception("Exception in parse_po_log_data.", exc_info=True)
            raise

    def get_tax_code_from_account_code(self, param):
        try:
            # Fetch the budget map ID for the project
            project_record = self.db_ops.search_projects(["project_number"], [self.PROJECT_NUMBER])
            if isinstance(project_record, list):
                project_record = project_record[0]
            budget_map_id = project_record.get("budget_map_id")

            budget_account = self.db_ops.search_account_codes(["budget_map_id", "code"], [budget_map_id, param])
            if isinstance(budget_account, list) and len(budget_account) > 0:
                budget_account = budget_account[0]
            else:
                return None
            tax_id = budget_account["tax_id"]
            tax_account = self.db_ops.search_tax_accounts(["id"], [tax_id])

            if not tax_account:
                self.logger.warning(f"No tax account found for account code: {param}")
                return None


            if isinstance(tax_account, list):
                tax_account = tax_account[0]


            return tax_account["tax_code"]
        except Exception:
            self.logger.exception("Exception in get_tax_code_from_account_code.", exc_info=True)
            return None
    # endregion

# endregion

budget_service = BudgetService()
