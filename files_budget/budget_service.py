# region 1: Imports
import copy
import logging
import os

from typing import Any, List

from database.database_util import DatabaseOperations
from files_dropbox.dropbox_service import DropboxService
from files_monday.monday_service import monday_service
from files_xero.xero_services import xero_services
from utilities.singleton import SingletonMeta


# endregion

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

    Unwanted keys ("detail_item_id", "total", "OT") are removed if present.
    """
    # Remove unwanted fields if they exist
    for key in ("detail_item_id", "total", "OT"):
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

    return transformed


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
                # endregion

                if not all_db_contacts:
                    self.logger.debug("📝 No existing contacts found in DB.")
                else:
                    self.logger.debug(f"📝 Found {len(all_db_contacts)} existing contacts.")

                # region 2.2.2: Process Each Contact
                for contact_item in contacts_data:
                    try:
                        name = (contact_item.get('name') or '').strip()
                        if not name:
                            self.logger.warning("🚫 'name' missing in contact_item; skipping record.")
                            continue

                        contact_id = None
                        if all_db_contacts:
                            try:
                                fuzzy_matches = self.db_ops.find_contact_close_match(name, all_db_contacts)
                                if fuzzy_matches:
                                    contact_id = fuzzy_matches[0]['id']
                                    self.logger.debug(f"✅ Fuzzy matched contact ID={contact_id} for name='{name}'")
                            except Exception:
                                self.logger.exception("Exception during fuzzy matching.", exc_info=True)

                        if not contact_id:
                            self.logger.info(f"🆕 Creating new contact for '{name}'")
                            new_ct = self.db_ops.create_contact(session=session, name=name)
                            if not new_ct:
                                self.logger.error(f"❌ Could not create contact for '{name}'.")
                                continue
                            contact_id = new_ct['id']
                            self.logger.info(f"🎉 Created contact ID={contact_id}")

                        db_contact = self.db_ops.search_contacts(["id"], [contact_id], session=session)
                        if isinstance(db_contact, list) and db_contact:
                            db_contact = db_contact[0]

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
                    #self.monday_service.execute_batch_upsert_contacts()
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
                raw_detail_number = d_item.get("detail_item_id")
                if raw_detail_number is None:
                    self.logger.warning("❌ detail_item_id missing; skipping item.")
                    continue
                detail_number = int(raw_detail_number)
                raw_line_number = d_item.get("line_number", 0)
                line_number = int(raw_line_number)
                d_item["payment_type"] = (d_item.get("payment_type") or "").upper()

                if d_item["payment_type"] in ["CC", "PC"]:
                    receipt_keys.add((project_number, po_number, detail_number))
                if d_item["payment_type"] in ["INV", "PROF"]:
                    invoice_keys.add((project_number, po_number, detail_number))

                d_item["detail_number"] = detail_number
                d_item["line_number"] = line_number
                d_item["ot"] = d_item["OT"]
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
            # region 2.4.2.1: Existing Detail Items
            existing_items = self.db_ops.batch_search_detail_items_by_keys(detail_item_keys, session=session)
            existing_map = {}
            for item in existing_items:
                key = (
                item.get("project_number"), item.get("po_number"), item.get("detail_number"), item.get("line_number"))
                existing_map[key] = item
            self.logger.info(f"🔍 Found {len(existing_map)} existing detail items in DB.")
            # endregion

            # region 2.4.2.2: Fetch Receipts for CC/PC Items
            receipt_map = {}
            if receipt_keys:
                self.logger.info(f"💳 Bulk fetching receipts for {len(receipt_keys)} keys.")
                receipt_list = self.db_ops.batch_search_receipts_by_keys(list(receipt_keys), session=session)
                for r in receipt_list:
                    rk = (r.get("project_number"), r.get("po_number"), r.get("detail_number"))
                    receipt_map[rk] = r
            # endregion

            # region 2.4.2.3: Fetch Invoices for INV/PROF Items
            invoice_map = {}
            if invoice_keys:
                self.logger.info(f"📑 Bulk fetching invoices for {len(invoice_keys)} keys.")
                invoice_list = self.db_ops.batch_search_invoices_by_keys(list(invoice_keys), session=session)
                for inv in invoice_list:
                    k = (int(inv.get("project_number")), int(inv.get("po_number")), int(inv.get("invoice_number")))
                    invoice_map[k] = inv
            # endregion

            # region 2.4.2.4: Fetch POs for Pulse IDs
            unique_po_keys = {
                (di["project_number"], di["po_number"])
                for di in detail_items_input if di.get("project_number") and di.get("po_number")
            }
            po_map = {}
            if unique_po_keys:
                unique_po_keys_list = list(unique_po_keys)
                project_number = int(unique_po_keys_list[0][0])
                po_list = self.db_ops.search_purchase_order_by_keys(project_number=project_number, session=session)
                if po_list:
                    for p in po_list:
                        po_map[(p["project_number"], p["po_number"])] = p
                self.logger.info(f"✅ Bulk-fetched {len(po_map)} POs for pulse IDs.")
            # endregion

            # region 2.4.2.5: Fetch Spend Money, Xero Bills, and Xero Bill Line Items
            spend_money_map = {}
            xero_bill_map = {}
            xero_bill_line_items_map = {}

            # Fetch Spend Money records for CC/PC items
            spend_money_keys = set()
            for d_item in detail_items_input:
                if d_item.get("payment_type", "").upper() in ["CC", "PC"]:
                    try:
                        # Note: the key here uses detail_item_id from the input.
                        key = (
                            int(d_item.get("project_number")),
                            int(d_item.get("po_number")),
                            int(d_item.get("detail_number"))
                        )
                        spend_money_keys.add(key)
                    except Exception:
                        self.logger.warning(f"Invalid key for Spend Money record: {d_item}")
            if spend_money_keys:
                self.logger.info(f"💰 Bulk fetching Spend Money records for {len(spend_money_keys)} keys.")
                spend_money_list = self.db_ops.batch_search_spend_money_by_keys(list(spend_money_keys), session=session)
                for sm in spend_money_list:
                    # Assuming the database record uses 'detail_number' to match the input 'detail_item_id'
                    key = (
                        int(sm.get("project_number")),
                        int(sm.get("po_number")),
                        int(sm.get("detail_number"))
                    )
                    spend_money_map[key] = sm
            else:
                self.logger.info("💰 No Spend Money keys to fetch.")

            # Fetch Xero Bills for INV/PROF items
            xero_bill_keys = set()
            for d_item in detail_items_input:
                if d_item.get("payment_type", "").upper() in ["INV", "PROF"]:
                    try:
                        key = (
                            int(d_item.get("project_number")),
                            int(d_item.get("po_number")),
                            int(d_item.get("detail_number"))
                        )
                        xero_bill_keys.add(key)
                    except Exception:
                        self.logger.warning(f"Invalid key for Xero Bill: {d_item}")
            if xero_bill_keys:
                self.logger.info(f"📄 Bulk fetching Xero Bills for {len(xero_bill_keys)} keys.")
                xero_bill_list = self.db_ops.batch_search_xero_bills_by_keys(list(xero_bill_keys), session=session)
                for xb in xero_bill_list:
                    key = (
                        int(xb.get("project_number")),
                        int(xb.get("po_number")),
                        int(xb.get("detail_number"))
                    )
                    xero_bill_map[key] = xb
            else:
                self.logger.info("📄 No Xero Bill keys to fetch.")

            # Fetch Xero Bill Line Items for the fetched Xero Bills
            xero_bill_ids = [xb["id"] for xb in xero_bill_map.values() if xb.get("id")]
            if xero_bill_ids:
                self.logger.info(f"📑 Bulk fetching Xero Bill Line Items for {len(xero_bill_ids)} Xero Bills.")
                xero_bill_line_items_list = self.db_ops.batch_search_xero_bill_line_items_by_xero_bill_ids(xero_bill_ids, session=session)
                for xbl in xero_bill_line_items_list:
                    xb_id = xbl.get("xero_bill_id")
                    if xb_id:
                        if xb_id not in xero_bill_line_items_map:
                            xero_bill_line_items_map[xb_id] = []
                        xero_bill_line_items_map[xb_id].append(xbl)
            else:
                self.logger.info("📑 No Xero Bills found; skipping Xero Bill Line Items fetch.")

            self.logger.info("✅ Fetched Spend Money, Xero Bills, and Xero Bill Line Items.")
            # endregion

            # region 2.4.2.6: Fetch Project-specific Accounts and Tax Accounts
            project_accounts_map = {}
            project_tax_accounts_map = {}
            unique_project_numbers = {d_item.get("project_number") for d_item in detail_items_input if
                                      d_item.get("project_number")}
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
                    # First, query tax accounts based on the tax ledger
                    tax_accounts = self.db_ops.search_tax_accounts(["tax_ledger_id"], [tax_ledger_id],
                                                                   session=session) if tax_ledger_id else []
                    # Extract tax account IDs for filtering
                    tax_account_ids = {ta.get("id") for ta in tax_accounts}
                    # Next, query budget accounts with the same budget map id and filter them
                    # so that only accounts whose tax account ID is in the previously fetched tax_account_ids are returned.
                    if budget_map_id:
                        all_budget_accounts = self.db_ops.search_account_codes(["budget_map_id"], [budget_map_id],
                                                                               session=session)
                        accounts = [acc for acc in all_budget_accounts if acc.get("tax_id") in tax_account_ids]
                    else:
                        accounts = []
                    project_accounts_map[project_number] = accounts
                    project_tax_accounts_map[project_number] = tax_accounts
                    self.logger.info(
                        f"Fetched {len(accounts)} accounts and {len(tax_accounts)} tax accounts for project {project_number}")
                except Exception:
                    self.logger.exception(f"Error fetching project-specific accounts for project {project_number}",
                                          exc_info=True)
            project_account_info = {}
            for project_number in unique_project_numbers:
                project_account_info[project_number] = {
                    "accounts": project_accounts_map.get(project_number, []),
                    "tax_accounts": project_tax_accounts_map.get(project_number, [])
                }

            budget_accounts = project_account_info["2416"]["accounts"]
            tax_accounts = project_account_info["2416"]["tax_accounts"]

            # endregion

            # region 2.4.2.7: Fetch Contacts for Detail Item Linking
            contact_map = {}
            # Extract unique vendor names from detail_items_input
            vendor_names = {d_item.get("vendor") for d_item in detail_items_input if d_item.get("vendor")}
            if vendor_names:
                try:
                    # Pass the vendor names directly as a list
                    contacts_result = self.db_ops.search_contacts(["name"], [list(vendor_names)], session=session)
                    if contacts_result:
                        if not isinstance(contacts_result, list):
                            contacts_result = [contacts_result]
                        for contact in contacts_result:
                            # Look up the corresponding PO to get project_number and po_number based on contact_id
                            for po in po_map.values():
                                if po.get("contact_id") == contact.get("id"):
                                    contact["project_number"] = po.get("project_number")
                                    contact["po_number"] = po.get("po_number")
                                    break
                            # Use tuple (project_number, po_number) as key if both values are present
                            key = (contact.get("project_number"), contact.get("po_number"))
                            if None not in key:
                                contact_map[key] = contact
                            else:
                                self.logger.warning(f"Contact {contact.get('id')} does not have project or PO number.")
                        self.logger.info(f"Fetched and processed {len(contact_map)} contacts for detail item linking.")
                    else:
                        self.logger.info("No contacts found for detail item linking.")
                except Exception:
                    self.logger.exception("Error fetching contacts for detail item linking.", exc_info=True)
            # endregion

            self.logger.info("🔍 Bulk Fetch complete.")
            # endregion

            # region 2.4.3: In-Memory Processing for CC/PC and INV/PROF items

            # region 2.4.3.1: Handle CC/PC Receipt Matching 💳🔍
            for d_item in detail_items_input:
                payment_type = d_item.get("payment_type")
                if payment_type in ["CC", "PC"]:
                    key = (
                        int(d_item["project_number"]),
                        int(d_item["po_number"]),
                        int(d_item["detail_number"])
                    )
                    sub_total = float(d_item.get("total") or 0.0)

                    # region 2.4.3.1.1: Check for Receipt Existence 🔍
                    if key in receipt_map:
                        # endregion 2.4.3.1.1

                        # region 2.4.3.1.2: Process Receipt Status Values (PENDING/VERIFIED/REJECTED) ⚖️
                        receipt_status = (receipt_map[key].get("status") or "PENDING").upper()
                        receipt_total = float(receipt_map[key].get("total") or 0.0)

                        if receipt_status == "PENDING":
                            # region 2.4.3.1.2.1: PENDING – Compare Totals & Update to VERIFIED 🟢
                            if abs(receipt_total - sub_total) < 0.0001:
                                receipt_map[key]["status"] = "VERIFIED"
                                d_item["state"] = "REVIEWED"
                                self.logger.info(f"[Receipt: PENDING->VERIFIED] Detail state -> REVIEWED: {key}")
                            else:
                                d_item["state"] = "PO MISMATCH"
                                self.logger.info(f"[Receipt: PENDING mismatch] Detail state -> PO MISMATCH: {key}")
                            # endregion 2.4.3.1.2.1
                        elif receipt_status == "VERIFIED":
                            # region 2.4.3.1.2.2: VERIFIED – Confirm Totals or Flag Mismatch 🔍
                            if abs(receipt_total - sub_total) < 0.0001:
                                d_item["state"] = "REVIEWED"
                                self.logger.info(f"[Receipt: VERIFIED match] Detail state -> REVIEWED: {key}")
                            else:
                                d_item["state"] = "PO MISMATCH"
                                self.logger.info(f"[Receipt: VERIFIED mismatch] Detail state -> PO MISMATCH: {key}")
                            # endregion 2.4.3.1.2.2
                        elif receipt_status == "REJECTED":
                            # region 2.4.3.1.2.3: REJECTED – No Action Needed ❌
                            self.logger.info(f"[Receipt: REJECTED] No action for detail {key}.")
                            # endregion 2.4.3.1.2.3
                        else:
                            # region 2.4.3.1.2.4: Unknown Status – Log and Skip ⚠️
                            self.logger.debug(f"[Receipt: {receipt_status}] Not recognized. Skipping detail {key}.")
                            # endregion 2.4.3.1.2.4
                        # endregion 2.4.3.1.2
                    else:
                        # region 2.4.3.1.3: Receipt Not Found – Log Skipping Detail 🛑
                        self.logger.debug(f"[Receipt Not Found] for detail {key}. No action.")
                        # endregion 2.4.3.1.3
            # endregion 2.4.3.1

            # region 2.4.3.2: Handle Spend Money for Reviewed CC/PC Items 💰✅
            for d_item in detail_items_input:
                payment_type = d_item.get("payment_type")
                detail_state = d_item.get("state")
                if payment_type in ["CC", "PC"] and detail_state == "REVIEWED":
                    key = (
                        int(d_item["project_number"]),
                        int(d_item["po_number"]),
                        int(d_item["detail_number"])
                    )
                    sub_total = float(d_item.get("total") or 0.0)

                    # region 2.4.3.2.1: Check for Existing Spend Money Record 🔎
                    if key not in spend_money_map:
                        # region 2.4.3.2.1.1: Create New Spend Money Record ✨
                        sm_record = {
                            "project_number": int(d_item["project_number"]),
                            "po_number": int(d_item["po_number"]),
                            "detail_number": int(d_item["detail_number"]),
                            "line_number": 1,
                            "status": "DRAFT",  # default
                            "amount": sub_total,
                            "description": d_item.get("description", ""),
                        }
                        # region 2.4.3.2.1.1.1: Attach Contact ID from Parent PO 📇
                        parent_po = po_map.get((int(d_item["project_number"]), int(d_item["po_number"])))
                        if parent_po and parent_po.get("contact_id"):
                            sm_record["contact_id"] = parent_po["contact_id"]
                        # endregion 2.4.3.2.1.1.1


                        # region 2.4.3.2.1.1.2: Retrieve Tax Code from Account Code 🏷️
                        account_code = d_item.get("account_code")
                        tax_code = None
                        if account_code:
                            matching_account = next((acc for acc in budget_accounts if acc.get("code") == account_code), None)
                            if matching_account:
                                tax_id = matching_account.get("tax_id")
                                if tax_id:
                                    matching_tax_account = next((tax for tax in tax_accounts if tax.get("id") == tax_id), None)
                                    if matching_tax_account:
                                        tax_code = matching_tax_account.get("tax_code")
                        sm_record["tax_code"] = tax_code
                        # endregion 2.4.3.2.1.1.2

                        spend_money_map[key] = sm_record
                        self.logger.info(f"[SpendMoney: CREATE] Created new spend money for detail {key}")
                        # endregion 2.4.3.2.1.1
                    else:
                        # region 2.4.3.2.2: Update Existing Spend Money Record 🔄
                        existing_sm = spend_money_map[key]
                        sm_status = existing_sm.get("status", "DRAFT").upper()
                        existing_amount = float(existing_sm.get("amount") or 0.0)

                        if sm_status == "RECONCILED":
                            # region 2.4.3.2.2.1: Reconciled Status – Validate Amount Consistency ✔️
                            if abs(existing_amount - sub_total) < 0.0001:
                                d_item["state"] = "RECONCILED"
                                self.logger.info(f"[SpendMoney: RECONCILED match] Detail state->RECONCILED: {key}")
                            else:
                                d_item["state"] = "ISSUE"
                                self.logger.info(f"[SpendMoney: RECONCILED mismatch] Detail state->ISSUE: {key}")
                            # endregion 2.4.3.2.2.1
                        elif sm_status in ["DRAFT", "AUTHORIZED", "PAID", "SUBMITTED FOR APPROVAL"]:
                            # region 2.4.3.2.2.2: Pending Spend Money – Compare & Update Differences 📝
                            contact_id = None
                            parent_po = po_map.get((int(d_item["project_number"]), int(d_item["po_number"])))
                            if parent_po and parent_po.get("contact_id"):
                                contact_id = parent_po["contact_id"]

                            account_code = d_item.get("account_code")
                            tax_code = None
                            if account_code:
                                matching_account = next(
                                    (acc for acc in budget_accounts if acc.get("code") == account_code), None)
                                if matching_account:
                                    tax_id = matching_account.get("tax_id")
                                    if tax_id:
                                        matching_tax_account = next(
                                            (tax for tax in tax_accounts if tax.get("id") == tax_id), None)
                                        if matching_tax_account:
                                            tax_code = matching_tax_account.get("tax_code")


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
                            # endregion 2.4.3.2.2.2
                        else:
                            # region 2.4.3.2.2.3: Unrecognized Spend Money Status – Log & Skip 🚫
                            self.logger.debug(f"[SpendMoney: {sm_status}] Not recognized. Skipping detail {key}.")
                            # endregion 2.4.3.2.2.3
                        # endregion 2.4.3.2.2
                    # endregion 2.4.3.2.1/2.4.3.2.2

            # endregion 2.4.3.2

            # region 2.4.3.3: Handle Invoice Matching & Status Updates 📑🧾
            # region 2.4.3.3.1: Calculate Invoice Sums from Detail Items ➕
            invoice_sums_map = {}  # keyed by (proj, po, detail_number) => sum of sub_totals
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
            # endregion 2.4.3.3.1

            # region 2.4.3.3.2: Update Invoice & Detail Item States Based on Sums 🔄
            for key, total_of_details in invoice_sums_map.items():
                if key in invoice_map:
                    invoice_obj = invoice_map[key]
                    invoice_status = (invoice_obj.get("status") or "PENDING").upper()
                    invoice_total = float(invoice_obj.get("total") or 0.0)

                    # region 2.4.3.3.2.1: Gather Sibling Detail Items 📋
                    siblings = [
                        d for d in detail_items_input
                        if (int(d["project_number"]), int(d["po_number"]), int(d["detail_number"])) == key
                    ]
                    # endregion 2.4.3.3.2.1

                    if invoice_status == "PENDING":
                        # region 2.4.3.3.2.2: PENDING Invoice – Verify Totals & Update to VERIFIED ✅
                        if abs(invoice_total - total_of_details) < 0.0001:
                            invoice_obj["status"] = "VERIFIED"
                            for s in siblings:
                                s["state"] = "RTP"
                            self.logger.info(f"[Invoice: PENDING->VERIFIED] siblings => RTP: {key}")
                        else:
                            for s in siblings:
                                s["state"] = "PO MISMATCH"
                            self.logger.info(f"[Invoice: PENDING mismatch] siblings => PO MISMATCH: {key}")
                        # endregion 2.4.3.3.2.2
                    elif invoice_status == "REJECTED":
                        # region 2.4.3.3.2.3: REJECTED Invoice – No Action Needed ❌
                        self.logger.info(f"[Invoice: REJECTED] No action on detail items for {key}.")
                        # endregion 2.4.3.3.2.3
                    elif invoice_status == "VERIFIED":
                        # region 2.4.3.3.2.4: VERIFIED Invoice – Confirm Totals or Mark Mismatch 🔍
                        if abs(invoice_total - total_of_details) < 0.0001:
                            for s in siblings:
                                s["state"] = "RTP"
                            self.logger.info(f"[Invoice: VERIFIED match] siblings => RTP: {key}")
                        else:
                            for s in siblings:
                                s["state"] = "PO MISMATCH"
                            self.logger.info(f"[Invoice: VERIFIED mismatch] siblings => PO MISMATCH: {key}")
                        # endregion 2.4.3.3.2.4
                    else:
                        # region 2.4.3.3.2.5: Unknown Invoice Status – Log and Skip ⚠️
                        self.logger.debug(f"[Invoice: {invoice_status}] Not recognized. No action for {key}.")
                        # endregion 2.4.3.3.2.5
                else:
                    # region 2.4.3.3.2.6: Invoice Not Found – Log Information ℹ️
                    self.logger.debug(f"[Invoice: NOT FOUND] for {key}. No action.")
                    # endregion 2.4.3.3.2.6
            # endregion 2.4.3.3.2
            # endregion 2.4.3.3

            # region 2.4.3.4: Handle Xero Bills for RTP Detail Items 📄💡
            for d_item in detail_items_input:
                payment_type = d_item.get("payment_type")
                detail_state = d_item.get("state")
                if payment_type in ["INV", "PROF", "PROJ"] and detail_state == "RTP":
                    key = (
                        int(d_item["project_number"]),
                        int(d_item["po_number"]),
                        int(d_item["detail_number"])
                    )
                    # region 2.4.3.4.1: Gather Sibling Detail Items for Date Analysis 📅
                    siblings = [
                        x for x in detail_items_input
                        if (int(x.get("project_number")), int(x.get("po_number")), int(x.get("detail_number"))) == key
                    ]
                    # endregion 2.4.3.4.1

                    # region 2.4.3.4.2: Determine Earliest Transaction Date & Latest Due Date ⏰
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
                    # endregion 2.4.3.4.2

                    # region 2.4.3.4.3: Create or Update Xero Bill Record 📝
                    if key not in xero_bill_map:
                        # region 2.4.3.4.3.1: Create New Xero Bill 🎉
                        self.logger.info(f"[XeroBill: CREATE] Creating new Xero Bill for key {key}.")
                        new_bill = {
                            "project_number": key[0],
                            "po_number": key[1],
                            "detail_number": key[2],
                            "status": "DRAFT",
                            "transaction_date": earliest_date,
                            "due_date": latest_due,
                            "reference": None,
                            "xero_contact_id": None,
                        }
                        # region 2.4.3.4.3.1.1: Assign Xero Contact ID from Contact Map 📇
                        contact_ = contact_map.get((key[0], key[1]))
                        if contact_:
                            if contact_.get("xero_id"):
                                new_bill["xero_contact_id"] = contact_["xero_id"]
                        # endregion 2.4.3.4.3.1.1

                        # region 2.4.3.4.3.1.2: Compile Line Items for Xero Bill 🛠️
                        line_items = []
                        for s in siblings:

                            account_code = d_item.get("account_code")
                            tax_code = None
                            if account_code:
                                matching_account = next(
                                    (acc for acc in budget_accounts if acc.get("code") == account_code), None)
                                if matching_account:
                                    tax_id = matching_account.get("tax_id")
                                    if tax_id:
                                        matching_tax_account = next(
                                            (tax for tax in tax_accounts if tax.get("id") == tax_id), None)
                                        if matching_tax_account:
                                            tax_code = matching_tax_account.get("tax_code")


                            sub_total = float(s.get("total") or 0.0)
                            line_item = {
                                "description": s.get("description", ""),
                                "quantity": s.get("quantity", 1),
                                "unit_amount": s.get("rate", 0.0),
                                "tax_code": tax_code,
                                "line_total": sub_total,
                            }
                            line_items.append(line_item)
                        new_bill["line_items"] = line_items
                        # endregion 2.4.3.4.3.1.2

                        xero_bill_map[key] = new_bill
                        # endregion 2.4.3.4.3.1
                    else:
                        # region 2.4.3.4.3.2: Update Existing Xero Bill if Needed 🔄
                        existing_bill = xero_bill_map[key]
                        bill_status = (existing_bill.get("status") or "DRAFT").upper()
                        differences_found = False

                        # region 2.4.3.4.3.2.1: Check for Date Discrepancies 🕒
                        if earliest_date and existing_bill.get("transaction_date") != earliest_date:
                            differences_found = True
                        if latest_due and existing_bill.get("due_date") != latest_due:
                            differences_found = True
                        # endregion 2.4.3.4.3.2.1

                        # region 2.4.3.4.3.2.2: Validate Contact Assignment 🔎
                        parent_po = po_map.get((key[0], key[1]))
                        xero_contact_id = existing_bill.get("xero_contact_id")
                        if parent_po and parent_po.get("contact_id") and xero_contact_id != parent_po["contact_id"]:
                            differences_found = True
                        # endregion 2.4.3.4.3.2.2

                        # region 2.4.3.4.3.2.3: Process Updates Based on Bill Status ⚙️
                        if bill_status in ["DRAFT", "SUBMITTED FOR APPROVAL", "PAID"]:
                            if differences_found:
                                self.logger.info(f"[XeroBill: UPDATE] Updating Xero Bill + lines for key {key}.")
                                existing_bill["transaction_date"] = earliest_date
                                existing_bill["due_date"] = latest_due
                                if parent_po and parent_po.get("contact_id"):
                                    existing_bill["xero_contact_id"] = parent_po["contact_id"]
                            else:
                                self.logger.debug(f"[XeroBill: NO-UPDATE] No changes for key {key}.")
                        elif bill_status in ["RECONCILED", "APPROVED", "AUTHORIZED"]:
                            if differences_found:
                                self.logger.info(f"[XeroBill: RECONCILED or APPROVED mismatch] Setting details to ISSUE.")
                                for s in siblings:
                                    s["state"] = "ISSUE"
                            else:
                                self.logger.debug(f"[XeroBill: RECONCILED or APPROVED match] Marking details RECONCILED.")
                                for s in siblings:
                                    s["state"] = "RECONCILED"
                        else:
                            self.logger.debug(f"[XeroBill: {bill_status}] Not recognized. No action for {key}.")
                        # endregion 2.4.3.4.3.2.3
                        # endregion 2.4.3.4.3.2
                    # endregion 2.4.3.4.3
                    # endregion 2.4.3.4
            # endregion 2.4.3.4

            # region 2.4.3.4.5 Handle PO Pulse ID --> Detail.Parent_pulse_id
            for d_item in detail_items_input:
                # Extract project number and PO number from the detail item
                project_number = int(d_item.get("project_number"))
                po_number = int(d_item.get("po_number"))

                # Find a matching PO in po_map using the project and PO numbers
                matching_po = po_map.get((project_number, po_number))

                if matching_po:
                    # Set the detail item's parent_pulse_id to the PO's pulse_id
                    d_item["parent_pulse_id"] = matching_po.get("pulse_id")
            
            #endregion

            # region 2.4.3.5: Prepare Data for Detail Item List Update 📝
            updated_detail_items = detail_items_input
            # endregion 2.4.3.5
            
            # region 2.4.3.6: Prepare Data for Xero Bill List Update 📃
            updated_xero_bills = list(xero_bill_map.values())
            # endregion 2.4.3.6

            # region 2.4.3.7: Prepare Data for Xero Bill Line Item List Update 🧾
            updated_xero_bill_line_items = []
            for line_items in xero_bill_line_items_map.values():
                updated_xero_bill_line_items.extend(line_items)
            # endregion 2.4.3.7

            # region 2.4.3.8: Prepare Data for Invoice and Receipt List Update 🔄
            updated_invoices = list(invoice_map.values())
            updated_receipts = list(receipt_map.values())
            # endregion 2.4.3.8

            # region 2.4.3.9: Prepare Data for Spend Money List Update 💸
            updated_spend_money = list(spend_money_map.values())
            # endregion 2.4.3.9

            # endregion

            # region 2.4.4: Bulk Create/Update in DB
            # To avoid duplicates we compare against the original fetched data.
            # For that purpose, we create copies of the original maps.
            original_detail_map = copy.deepcopy(existing_map)
            original_receipt_map = copy.deepcopy(receipt_map)
            original_invoice_map = copy.deepcopy(invoice_map)
            original_xero_bill_map = copy.deepcopy(xero_bill_map)
            original_spend_money_map = copy.deepcopy(spend_money_map)
            original_xero_bill_line_items_map = {}
            for xb_id, items in xero_bill_line_items_map.items():
                for item in items:
                    key = (xb_id, item.get("line_number"))
                    original_xero_bill_line_items_map[key] = item

            # Helper function to compare dictionaries ignoring the 'id' field.
            def are_dicts_different(d1, d2):
                d1_copy = {k_: v for k_, v in d1.items() if k_ != "id"}
                d2_copy = {k_: v for k_, v in d2.items() if k_ != "id"}
                return d1_copy != d2_copy

            # --- Detail Items ---
            detail_items_to_create = []
            detail_items_to_update = []
            for d_item in updated_detail_items:

                key = (int(d_item.get("project_number")), int(d_item.get("po_number")),
                       d_item.get("detail_number"), d_item.get("line_number"))
                if key in original_detail_map:
                    db_item = original_detail_map[key]
                    if are_dicts_different(d_item, db_item):
                        detail_items_to_update.append(transform_detail_item(d_item))
                else:
                    detail_items_to_create.append(transform_detail_item(d_item))

            if detail_items_to_create:
                created_detail_items = []
                for chunk in chunk_list(detail_items_to_create, chunk_size):
                    self.logger.debug(f"Creating chunk of {len(chunk)} detail items.")
                    created_sub = self.db_ops.bulk_create_detail_items(chunk, session=session)
                    created_detail_items.extend(created_sub)
                    session.flush()
                # Optionally, merge created_detail_items into updated_detail_items.
            if detail_items_to_update:
                updated_detail_items_db = []
                for chunk in chunk_list(detail_items_to_update, chunk_size):
                    self.logger.debug(f"Updating chunk of {len(chunk)} detail items.")
                    updated_sub = self.db_ops.bulk_update_detail_items(chunk, session=session)
                    updated_detail_items_db.extend(updated_sub)
                    session.flush()
                # Optionally, merge updated_detail_items_db into updated_detail_items.

            # --- Xero Bills ---
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
            if xero_bills_to_create:
                created_xero_bills = self.db_ops.bulk_create_xero_bills(xero_bills_to_create, session=session)
                session.flush()
            if xero_bills_to_update:
                updated_xero_bills_db = self.db_ops.bulk_update_xero_bills(xero_bills_to_update, session=session)
                session.flush()

            # --- Xero Bill Line Items ---
            xero_bill_line_items_to_create = {}
            xero_bill_line_items_to_update = {}
            for xbl in updated_xero_bill_line_items:
                bill_id = xbl.get("xero_bill_id")
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
            for bill_id, items in xero_bill_line_items_to_create.items():
                created_xero_bill_line_items = self.db_ops.bulk_create_xero_bill_line_items(bill_id, items, session=session)
                session.flush()
            for bill_id, items in xero_bill_line_items_to_update.items():
                updated_xero_bill_line_items_db = self.db_ops.bulk_update_xero_bill_line_items(items, session=session)
                session.flush()

            # --- Invoices ---
            invoices_to_update = []
            for inv in updated_invoices:
                # Use invoice_number if available; otherwise, use detail_item_id.
                key = (inv["project_number"], inv["po_number"], inv.get("invoice_number") or inv.get("detail_number"))
                if key in original_invoice_map:
                    db_inv = original_invoice_map[key]
                    if are_dicts_different(inv, db_inv):
                        inv["id"] = db_inv["id"]
                        invoices_to_update.append(inv)
            if invoices_to_update:
                updated_invoices_db = self.db_ops.bulk_update_invoices(invoices_to_update, session=session)
                session.flush()

            # --- Receipts ---
            receipts_to_update = []
            for rec in updated_receipts:
                key = (rec["project_number"], rec["po_number"], rec["detail_number"])
                if key in original_receipt_map:
                    db_rec = original_receipt_map[key]
                    if are_dicts_different(rec, db_rec):
                        rec["id"] = db_rec["id"]
                        receipts_to_update.append(rec)
                session.flush()
            if receipts_to_update:
                updated_receipts_db = self.db_ops.bulk_update_receipts(receipts_to_update, session=session)
                session.flush()

            # --- Spend Money ---
            spend_money_to_create = []
            spend_money_to_update = []
            for sm in updated_spend_money:
                key = (sm["project_number"], sm["po_number"], sm["detail_number"])
                if key in original_spend_money_map:
                    db_sm = original_spend_money_map[key]
                    if are_dicts_different(sm, db_sm):
                        sm["id"] = db_sm["id"]
                        spend_money_to_update.append(sm)
                else:
                    spend_money_to_create.append(sm)
            if spend_money_to_create:
                created_spend_money = self.db_ops.bulk_create_spend_money(spend_money_to_create, session=session)
                session.flush()
            if spend_money_to_update:
                updated_spend_money_db = self.db_ops.bulk_update_spend_money(spend_money_to_update, session=session)
                session.flush()

            # Restate the variables for clarity to pass along to the next section:
            updated_detail_items = updated_detail_items  # for the detail items
            updated_xero_bills = updated_xero_bills      # for the Xero bills
            updated_xero_bill_line_items = updated_xero_bill_line_items  # for the Xero bill line items
            updated_invoices = updated_invoices          # for the invoices
            updated_receipts = updated_receipts          # for the receipts
            updated_spend_money = updated_spend_money    # for the Spend Money records

            self.logger.info("💾 Bulk Create/Update complete for all items.")
            # endregion

            # region 2.4.5: Xero Upsert for Spend Money Items, Xero Bills, and Xero Bill Items
            # spend_money_items = []
            # for d_item in detail_items_input:
            #     if d_item.get("payment_type") in ["CC", "PC"] and d_item.get("_spend_money"):
            #         sm_key = (
            #         int(d_item.get("project_number")), int(d_item.get("po_number")), int(d_item.get("detail_number")))
            #         if sm_key in spend_money_map:
            #             formatted_sm = {
            #                 "project_number": int(d_item["project_number"]),
            #                 "po_number": int(d_item["po_number"]),
            #                 "detail_number": int(d_item["detail_number"]),
            #                 "line_number": 1,
            #                 "state": "DRAFT",
            #                 "amount": float(d_item.get("total") or 0.0),
            #                 "contact_Id": receipt_map[sm_key].get("contact_Id"),
            #                 "tax_code": self.get_tax_code_from_account_code(d_item.get("account_code")),
            #                 "description": d_item.get("description", "")
            #             }
            #             spend_money_items.append(formatted_sm)
            # new_spend_money_items = []
            # if spend_money_items:
            #     self.logger.info(f"🎯 Creating {len(spend_money_items)} SpendMoney records.")
            #     for chunk in self.chunk_list(spend_money_items, chunk_size):
            #         self.logger.debug(f"Creating SpendMoney chunk: {chunk}")
            #         new_sm_items = self.db_ops.bulk_create_spend_money(chunk, session=session)
            #         session.flush()
            #         self.logger.info(f"💸 Created {len(new_sm_items)} SpendMoney records in this chunk.")
            #         new_spend_money_items.extend(self.xero_services.handle_spend_money_create_bulk(new_sm_items, session=session))
            # self.logger.info("🎯 Post-Persist Side Effects complete.")
            # endregion

            #region 2.4.6: Monday Upsert for Detail Items
            try:
                self.logger.info("[Detail Aggregator] Starting Monday upsert for detail items.")
                self.logger.debug(
                    f"Created/updated items count: {len(updated_detail_items)}")
                monday_items = []

                #region 2.4.6.1: Add External Links to Detail Items
                for di in updated_detail_items:
                    file_link = None
                    xero_link = None

                    if di.get("payment_type") in ["CC", "PC"]:
                        key = (int(di.get("project_number")), int(di.get("po_number")), int(di.get("detail_number")))
                        #get file link from receipt_map
                        if key in receipt_map:
                            file_link = receipt_map[key].get("file_link")
                        #TODO get spend money links
                    elif di.get("payment_type") in ["INV", "PROF"]:
                        key = (int(di.get("project_number")), int(di.get("po_number")), int(di.get("detail_number")))
                        #get file  link from invoice_map
                        if key in invoice_map:
                            file_link = invoice_map[key].get("file_link")
                        #TODO get xero link from xero_bills
                #endregion

                    #region BUILD DETAIL DICT
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
                        'transaction_date': di.get('date'),
                        'due_date': di.get('due date'),
                        'account_code': di.get('account'),
                        'file_link': file_link,
                        'xero_link': xero_link,
                        'ot': di.get('ot'),
                        'fringes': di.get('fringes'),
                        'state': di.get('state')
                    }
                    self.logger.debug(f"Prepared Monday item (updated): {detail_dict}")
                    monday_items.append(detail_dict)
                    #endregion

                self.logger.info(f"📤 Total Monday items prepared: {len(monday_items)}")
                for chunk in chunk_list(monday_items, 500):
                    self.logger.debug(f"Processing Monday chunk with {len(chunk)} items.")
                    for detail_dict in chunk:
                        self.logger.debug(f"Buffering Monday upsert: {detail_dict}")
                        self.monday_service.buffered_upsert_detail_item(detail_dict)
                    self.logger.debug("Executing batch upsert for current Monday chunk.")
                    created_subitems, updated_subitems = self.monday_service.execute_batch_upsert_detail_items()
                    self.logger.debug(
                        f"Monday batch upsert returned: created_subitems={created_subitems}, updated_subitems={updated_subitems}")
                    if created_subitems:
                        for subitem_obj in created_subitems:
                            self.logger.debug(f"Processing Monday created subitem: {subitem_obj}")
                            db_sub_item = subitem_obj.get("db_sub_item")
                            monday_sub_id = subitem_obj.get("monday_item_id")
                            if db_sub_item and db_sub_item.get("id") and monday_sub_id:
                                self.logger.debug(
                                    f"Updating DB detail item {db_sub_item.get('id')} with pulse_id {monday_sub_id}")
                                self.db_ops.update_detail_item(db_sub_item["id"], pulse_id=monday_sub_id,
                                                               session=session)
                            else:
                                self.logger.warning("Monday created subitem missing required fields for DB update.")
                    else:
                        self.logger.info("No new Monday subitems were created in this chunk.")
                self.logger.info("[Detail Aggregator] Monday upsert complete; will commit once aggregator completes.")
            except Exception:
                self.logger.exception("Error during Monday upsert.", exc_info=True)
            #endregion


        except Exception:
            self.logger.exception("Exception in process_aggregator_detail_items.", exc_info=True)
            raise
    # endregion

    # region 2.5: Aggregator Status Checks
    def is_aggregator_in_progress(self, record: dict) -> bool:
        try:
            project_number = record.get('project_number')
            if not project_number:
                return False
            self.logger.info(f"🔎 Checking if aggregator for project {project_number} is STARTED.")
            po_logs = self.db_ops.search_po_logs(['project_number'], [project_number])
            if not po_logs:
                return False
            if isinstance(po_logs, dict):
                po_logs = [po_logs]
            for pl in po_logs:
                if pl.get('status') == 'STARTED':
                    self.logger.info("🚦 Aggregator in progress (status=STARTED).")
                    return True
            return False
        except Exception:
            self.logger.exception("Exception in is_aggregator_in_progress.", exc_info=True)
            raise

    def is_aggregator_done(self, record: dict) -> bool:
        try:
            project_number = record.get('project_number')
            if not project_number:
                return True
            self.logger.info(f"🔎 Checking if aggregator for project {project_number} is COMPLETED.")
            po_logs = self.db_ops.search_po_logs(['project_number'], [project_number])
            if not po_logs:
                return True
            if isinstance(po_logs, dict):
                po_logs = [po_logs]
            for pl in po_logs:
                if pl.get('status') == 'COMPLETED':
                    self.logger.info("🏁 Aggregator completed (status=COMPLETED).")
                    return True
            return False
        except Exception:
            self.logger.exception("Exception in is_aggregator_done.", exc_info=True)
            raise

    # endregion

    # region 2.6: Summation & State Changes for Invoices & Details
    def set_invoice_details_rtp(self, detail_item: dict, buffer: List[dict]):
        try:
            self.logger.info("🔖 Setting detail items to RTP (invoice sums matched).")
            project_number = detail_item.get('project_number')
            po_number = detail_item.get('po_number')
            invoice_num = detail_item.get('detail_number')
            siblings = [di for di in buffer if di.get('project_number') == project_number and
                        di.get('po_number') == po_number and di.get('detail_number') == invoice_num]
            if not siblings:
                self.logger.warning("🙅 No siblings found; no updates made.")
                return
            for sib in siblings:
                current_state = (sib.get('state') or '').upper()
                if current_state not in {"PAID", "RECONCILED", "APPROVED"}:
                    self.db_ops.update_detail_item(sib['id'], state="RTP")
                    self.logger.info(f"✨ DetailItem(id={sib['id']}) set to RTP.")
        except Exception:
            self.logger.exception("Exception in set_invoice_details_rtp.", exc_info=True)
            raise

    def sum_detail_items_and_compare_invoice(self, detail_item: dict, buffer: List[dict], session) -> bool:
        try:
            project_number = detail_item.get('project_number')
            po_number = detail_item.get('po_number')
            invoice_num = detail_item.get('detail_item_id')
            self.logger.info(f"🧮 Summing detail items vs invoice total for invoice {invoice_num}")
            details = [di for di in buffer if di.get('project_number') == project_number and
                       di.get('po_number') == po_number and di.get('detail_item_id') == invoice_num]
            if not details:
                return False
            total_of_details = sum(float(di.get('sub_total') or 0.0) for di in details)
            invoice = self.db_ops.search_invoice_by_keys(
                project_number=project_number,
                po_number=po_number,
                invoice_number=invoice_num,
                session=session
            )
            if not invoice:
                return False
            if isinstance(invoice, list):
                invoice = invoice[0]
            invoice_total = float(invoice.get('total', 0.0))
            return abs(total_of_details - invoice_total) < 0.0001
        except Exception:
            self.logger.exception("Exception in sum_detail_items_and_compare_invoice.", exc_info=True)
            raise

    def check_siblings_all_rtp(self, detail_item: dict, buffer: List[dict]) -> bool:
        try:
            project_number = detail_item.get('project_number')
            po_number = detail_item.get('po_number')
            detail_number = detail_item.get('detail_item_id')
            siblings = [di for di in buffer if di.get('project_number') == project_number and
                        di.get('po_number') == po_number and di.get('detail_item_id') == detail_number]
            if not siblings:
                return False
            return all((sib.get('state') or '').upper() == 'RTP' for sib in siblings)
        except Exception:
            self.logger.exception("Exception in check_siblings_all_rtp.", exc_info=True)
            raise

    # endregion

    # region 2.7: Xero Bill Date Range Update
    def update_xero_bill_dates_from_detail_item(self, xero_bill: dict):
        try:
            self.logger.info("🤖 Updating XeroBill date range from detail items...")
            project_number = xero_bill.get('project_number')
            po_number = xero_bill.get('po_number')
            detail_number = xero_bill.get('detail_number')
            detail_items = self.db_ops.search_detail_item_by_keys(
                project_number=project_number,
                po_number=po_number,
                detail_number=detail_number
            )
            if not detail_items:
                self.logger.info("🙅 No detail items found; skipping date update.")
                return
            if isinstance(detail_items, dict):
                detail_items = [detail_items]
            existing_parent_date = xero_bill.get('transaction_date')
            existing_parent_due = xero_bill.get('due_date')
            detail_dates = [di['transaction_date'] for di in detail_items if di.get('transaction_date')]
            detail_dues = [di['due_date'] for di in detail_items if di.get('due_date')]
            if existing_parent_date:
                detail_dates.append(existing_parent_date)
            if existing_parent_due:
                detail_dues.append(existing_parent_due)
            from datetime import datetime, date
            def to_date(d):
                if isinstance(d, datetime):
                    return d.date()
                elif isinstance(d, date):
                    return d
                elif isinstance(d, str):
                    try:
                        return datetime.fromisoformat(d).date()
                    except:
                        return None
                return None

            detail_dates = [to_date(d) for d in detail_dates if to_date(d)]
            detail_dues = [to_date(d) for d in detail_dues if to_date(d)]
            earliest_date = min(detail_dates) if detail_dates else existing_parent_date
            latest_due = max(detail_dues) if detail_dues else existing_parent_due
            self.logger.info(f"🔎 Determined earliest_date={earliest_date}, latest_due={latest_due}.")
            if earliest_date != existing_parent_date or latest_due != existing_parent_due:
                self.logger.info("🌀 Updating XeroBill with new date range!")
                self.db_ops.update_xero_bill(xero_bill_id=xero_bill['id'],
                                             transaction_date=earliest_date,
                                             due_date=latest_due)
            else:
                self.logger.info("🙆 Date range is already correct; no update needed.")
        except Exception:
            self.logger.exception("Exception in update_xero_bill_dates_from_detail_item.", exc_info=True)
            raise

    # endregion

    # region 2.8: Helper Methods
    def parse_po_log_data(self, po_log: dict) -> list[Any] | dict[str, Any]:
        try:
            po_log_db_path = po_log["db_path"]
            po_log_filename = po_log["filename"]
            project_number = po_log["project_number"]
            temp_file_path = f'../temp_files/{os.path.basename(po_log_filename)}'
            self.PROJECT_NUMBER = project_number
            po_log_file_path = temp_file_path
            if not os.path.exists(temp_file_path):
                self.logger.info('🛠 Not using local temp files? Attempting Dropbox download...')
                if not self.dropbox_service.download_file_from_dropbox((po_log_db_path + po_log_filename),
                                                                       temp_file_path):
                    return []
                self.logger.info(f'📝 Received PO Log file from Dropbox: {po_log_filename}')
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
            budget_map_id = self.db_ops.search_projects(["project_number"], self.PROJECT_NUMBER)["budget_map_id"]

            # Fetch the tax account ID using the budget map ID and account code
            tax_account = self.db_ops.search_tax_accounts(["budget_map_id", "code"], [budget_map_id, param])

            if not tax_account:
                self.logger.warning(f"No tax account found for account code: {param}")
                return None

            tax_code_id = tax_account["tax_id"]

            # Fetch the tax code using the tax account ID
            tax_code = self.db_ops.search_tax_accounts(['id'], tax_code_id)

            if not tax_code:
                self.logger.warning(f"No tax code found for tax account ID: {tax_code_id}")
                return None

            return tax_code["tax_code"]
        except Exception:
            self.logger.exception("Exception in get_tax_code_from_account_code.", exc_info=True)
            return None
    # endregion

# endregion

# region 3: Instantiate BudgetService
budget_service = BudgetService()
# endregion