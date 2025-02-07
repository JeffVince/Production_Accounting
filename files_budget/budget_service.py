import logging
import os
import sys
from typing import Any, List

from database.database_util import DatabaseOperations
from files_dropbox.dropbox_service import DropboxService
from files_monday.monday_service import monday_service
from files_xero.xero_services import xero_services
from utilities.singleton import SingletonMeta


class BudgetService(metaclass=SingletonMeta):
    """
    Aggregator logic (previously in budget_service), now called BudgetService.

    Responsibilities:
      - Checking aggregator status (is aggregator = STARTED/COMPLETED?)
      - Summation logic for detail items vs. invoice
      - Setting detail items state to RTP
      - Updating XeroBill date ranges
      - Searching aggregator logs (po_logs)
    """
    def __init__(self):
        try:
            self.logger = logging.getLogger('budget_logger')
            self.db_ops = DatabaseOperations()
            self.xero_services = xero_services
            self.dropbox_service = DropboxService()
            self.monday_service = monday_service
            self.logger.info("🧩 BudgetService (aggregator logic) initialized!")
        except Exception as e:
            logging.exception("Error initializing BudgetService.", exc_info=True)
            raise



    #region🌹 CONTACT AGGREGATOR FUNCTIONS
    def process_contact_aggregator(self, contacts_data: list[dict], session):
        """
        Aggregator for CONTACTS with:
          - Single commit at the end, using the session passed in.
        """
        try:
            self.logger.info("[Contact Aggregator] START => Processing contact data in multi-phase style.")
            if not contacts_data:
                self.logger.info("[Contact Aggregator] No contacts provided. Nothing to do.")
                return

            # PHASE 1: Minimal contact creation + Xero/Monday Upsert
            try:
                self.logger.info("🔎 => Creating/Updating DB contacts, then sending to Xero & Monday...")

                # -- 1) Search existing contacts with aggregator's session
                try:
                    all_db_contacts = self.db_ops.search_contacts(session=session)
                except Exception as e:
                    self.logger.exception("Exception searching all contacts in PHASE 1 of contact aggregator.", exc_info=True)
                    all_db_contacts = []

                if not all_db_contacts:
                    self.logger.debug("📝 => Found no existing contacts in DB. Starting fresh.")
                else:
                    self.logger.debug(
                        f"📝 => Found {len(all_db_contacts)} existing contacts in DB for potential fuzzy matches.")

                # -- 2) Process each contact item
                for contact_item in contacts_data:
                    try:
                        name = (contact_item.get('name') or '').strip()
                        if not name:
                            self.logger.warning("🚫 => No 'name' in contact_item. Skipping this record.")
                            continue

                        # Attempt fuzzy match
                        contact_id = None
                        if all_db_contacts:
                            try:
                                fuzzy_matches = self.db_ops.find_contact_close_match(name, all_db_contacts)
                                if fuzzy_matches:
                                    contact_id = fuzzy_matches[0]['id']
                                    self.logger.debug(
                                        f"✅ => Fuzzy matched contact => ID={contact_id} for name='{name}'")
                            except Exception as e:
                                self.logger.exception("Exception during fuzzy matching in contact aggregator.", exc_info=True)

                        # If no fuzzy match, create a new contact in DB
                        if not contact_id:
                            self.logger.info(f"🆕 => Creating new contact in DB for '{name}'")
                            new_ct = self.db_ops.create_contact(
                                session=session,
                                name=name,
                            )
                            if not new_ct:
                                self.logger.error(f"❌ => Could not create new contact for '{name}'.")
                                continue
                            contact_id = new_ct['id']
                            self.logger.info(f"🎉 => Successfully created contact => ID={contact_id}")

                        # Re-fetch the updated DB record
                        db_contact = self.db_ops.search_contacts(["id"], [contact_id], session=session)
                        if isinstance(db_contact, list) and db_contact:
                            db_contact = db_contact[0]

                        # Upsert to Xero
                        try:
                            xero_id = db_contact.get('xero_id')
                            self.xero_services.buffered_upsert_contact(db_contact)
                        except Exception as e:
                            self.logger.exception("Exception buffering Xero contact upsert.", exc_info=True)

                        # Upsert to Monday
                        try:
                            pulse_id = db_contact.get('pulse_id')
                            self.monday_service.buffered_upsert_contact(db_contact)
                        except Exception as e:
                            self.logger.exception("Exception buffering Monday contact upsert.", exc_info=True)

                    except Exception as e:
                        self.logger.exception("Exception in loop while processing each contact_item in PHASE 1.", exc_info=True)

                # -- 3) Final batch push to Xero & Monday
                try:
                    self.logger.info("📤 => Executing update/create batches to Xero & Monday now.")
                    self.xero_services.execute_batch_upsert_contacts(self.xero_services.contact_upsert_queue)
                    self.monday_service.execute_batch_upsert_contacts()
                except Exception as e:
                    self.logger.exception("Exception executing batch upsert to Xero/Monday in contact aggregator.", exc_info=True)

            except Exception as e:
                self.logger.exception("Exception in PHASE 1 of contact aggregator.", exc_info=True)
                raise

        except Exception as e:
            self.logger.exception("Exception in contact aggregator logic.", exc_info=True)
            raise
    #endregion

    # region 🌺 PURCHASE ORDERS AGGREGATOR
    def process_aggregator_pos(self, po_data: dict, session):
        """
        Aggregator for PURCHASE ORDERS, single commit at the end.
        """
        try:
            self.logger.info("🚀START => Processing PO aggregator data.")

            if not po_data or not po_data.get("main_items"):
                self.logger.info("🤷No main_items provided. Nothing to do.")
                return

            # PHASE 1: CREATE / UPDATE each PO in DB
            po_records_info = []
            self.logger.info("[PO Aggregator, PHASE 1] => Creating/Updating POs in DB.")

            # We'll also gather contacts once here, if needed for fuzzy matching
            all_contacts = self.db_ops.search_contacts(session=session)

            for item in po_data["main_items"]:
                if not item:
                    continue

                project_number = item.get("project_number")
                po_number = item.get("po_number")
                raw_po_type = item.get("po type", "INV")
                description = item.get("description", "")
                vendor_name = item.get("contact_name")

                if not po_number:
                    self.logger.warning("🤔 Missing po_number, skipping this item.")
                    continue

                # Normalize
                if raw_po_type == "PROJ":
                    po_type = "INV"
                else:
                    po_type = raw_po_type

                # Ensure Project exists or create
                project_record = self.db_ops.search_projects(["project_number"], [project_number], session=session)
                if not project_record:
                    self.logger.warning(f"⚠️=> Project {project_number} not found, creating a new one.")
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
                        self.logger.warning("❌ Could not create project. Skipping this PO.")
                        continue
                    else:
                        self.logger.info(f"🌱 Created Project => ID={project_record['id']}")

                if isinstance(project_record, list) and project_record:
                    project_record = project_record[0]
                project_id = project_record["id"]

                # Lookup contact
                contact_id = None
                if vendor_name:
                    found_contact = self.db_ops.search_contacts(["name"], [vendor_name], session=session)
                    if found_contact:
                        if isinstance(found_contact, list) and found_contact:
                            found_contact = found_contact[0]
                        contact_id = found_contact.get("id")
                    else:
                        # fuzzy match
                        fuzzy_matches = self.db_ops.find_contact_close_match(vendor_name, all_contacts)
                        best_match = None
                        if fuzzy_matches:
                            best_match = fuzzy_matches[0]
                            self.logger.warning(f"⚠️ Contact '{vendor_name}' fuzzy matched to => {best_match.get('name')}")
                            contact_id = best_match.get("id")
                else:
                    self.logger.warning(f"⚠️ No contact_name => leaving contact data null.")
                    vendor_name = "PO LOG Naming Error"

                # Check if PO exists
                existing = self.db_ops.search_purchase_order_by_keys(project_number, po_number, session=session)
                if not existing:
                    # CREATE
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
                        self.logger.info(f"✅ Created new PO => ID={new_po['id']}")
                        po_records_info.append(new_po)
                    else:
                        self.logger.warning("❌ Failed to create new PO.")
                else:
                    if isinstance(existing, list) and existing:
                        existing = existing[0]
                    po_id = existing["id"]

                    # Check if anything changed
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
                        # UPDATE
                        self.logger.info(f"🔄 Updating existing PO => ID={po_id}.")
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
                            self.logger.info(f"🔄 Updated PO => ID={updated_po['id']}")
                            po_records_info.append(updated_po)
                        else:
                            self.logger.warning("❌ Failed to update existing PO.")
                    else:
                        # If it has no pulse_id, we still want to upsert to Monday
                        if not existing.get("pulse_id"):
                            self.logger.info(
                                f"🆕 Existing PO ID={po_id} has no pulse_id => we still upsert to Monday.")
                            po_records_info.append(existing)
                        else:
                            self.logger.info(
                                f"🏳️‍🌈 No changes to existing PO => ID={po_id}. Already in Monday, skipping.")

            self.logger.info("[PHASE 1] => DONE => POs loaded into DB")

            # PHASE 2: Update Monday (batched)
            self.logger.info("[PHASE 2] => Batching POs to Monday.")
            for po_record in po_records_info:
                self.monday_service.buffered_upsert_po(po_record)

            created_POs = self.monday_service.execute_batch_upsert_pos()

            # Update the pulse_id in DB for newly created Monday items
            if created_POs:
                for po_obj in created_POs:
                    po_id = po_obj["db_item"]["id"]
                    pulse_id = po_obj["monday_item_id"]
                    self.db_ops.update_purchase_order(po_id=po_id, pulse_id=pulse_id, session=session)

            self.logger.info("[PHASE 2] DONE => Batching POs to Monday complete.")
            self.logger.info("[COMPLETED]")

        except Exception as e:
            self.logger.error(f"❌=> Error => {str(e)}")
            session.rollback()
            raise
    #endregion

    # region 🌺 DETAIL ITEM AGGREGATOR
    def process_aggregator_detail_items(self, po_log_data: dict, session, chunk_size: int = 500):
        """
        Aggregator for DETAIL ITEMS, with minimal DB queries and in-memory logic.
        Still commits once at the end (assuming `session` is managed externally).

        This version ensures `detail_number` and `line_number` are cast to int,
        so we don't mistakenly create duplicates instead of updates.
        """
        try:
            self.logger.info("[Detail Aggregator] START => Processing detail items data.")

            # =========================
            # PHASE 1: GATHER INPUT
            # =========================
            if not po_log_data or not po_log_data.get("detail_items"):
                self.logger.info("[Detail Aggregator] No detail_items to process, returning.")
                return

            detail_items_input = []
            detail_item_keys = []

            # Also track sets of keys for receipts + invoices
            receipt_keys = set()  # for CC/PC
            invoice_keys = set()  # for INV/PROF

            for d_item in po_log_data["detail_items"]:
                if not d_item:
                    continue

                # Convert detail_number & line_number to integers if they are present
                project_number = d_item.get("project_number")
                po_number = d_item.get("po_number")

                # If detail_item_id is not None, cast to int (or skip if you always expect it)
                raw_detail_number = d_item.get("detail_item_id")
                if raw_detail_number is None:
                    self.logger.warning("❌ detail_item_id missing, skipping this item.")
                    continue
                detail_number = int(raw_detail_number)  # assume DB uses int

                raw_line_number = d_item.get("line_number", 0)
                line_number = int(raw_line_number)  # default to 0 if None or blank

                # Normalize payment type
                d_item["payment_type"] = (d_item.get("payment_type") or "").upper()

                # If CC/PC => track keys to fetch receipts
                if d_item["payment_type"] in ["CC", "PC"]:
                    receipt_keys.add((project_number, po_number, detail_number))

                # If INV/PROF => track keys to fetch invoices
                if d_item["payment_type"] in ["INV", "PROF"]:
                    invoice_keys.add((project_number, po_number, detail_number))

                # Update the aggregator item to reflect these integer conversions
                d_item["detail_item_id"] = detail_number
                d_item["line_number"] = line_number

                detail_items_input.append(d_item)
                detail_item_keys.append({
                    "project_number": project_number,
                    "po_number": po_number,
                    "detail_number": detail_number,
                    "line_number": line_number
                })

            self.logger.info(f"[Detail Aggregator] Gathered {len(detail_items_input)} input items.")

            # =========================================
            # PHASE 2: BULK FETCH EXISTING DATA
            # =========================================

            # 2a) Lookup existing detail items
            existing_items = self.db_ops.batch_search_detail_items_by_keys(
                detail_item_keys,
                session=session
            )
            existing_map = {}
            for item in existing_items:
                key = (
                    item.get("project_number"),
                    item.get("po_number"),
                    item.get("detail_number"),
                    item.get("line_number")
                )
                existing_map[key] = item
            self.logger.info(f"[Detail Aggregator] Found {len(existing_map)} existing detail items in DB.")

            # 2b) If we have any CC/PC items => fetch receipts in one call
            receipt_map = {}
            if receipt_keys:
                self.logger.info(f"💳 Bulk fetching receipts for {len(receipt_keys)} CC/PC keys.")
                receipt_list = self.db_ops.batch_search_receipts_by_keys(list(receipt_keys), session=session)
                for r in receipt_list:
                    rk = (r.get("project_number"), r.get("po_number"), r.get("detail_number"))
                    receipt_map[rk] = r

            # 2c) If we have any INV/PROF => fetch invoices in one call
            invoice_map = {}
            if invoice_keys:
                self.logger.info(f"📑 Bulk fetching invoices for {len(invoice_keys)} INV/PROF keys.")
                invoice_list = self.db_ops.batch_search_invoices_by_keys(list(invoice_keys), session=session)
                for inv in invoice_list:
                    # unify by (project_number, po_number, invoice_number)
                    k = (int(inv.get("project_number")), int(inv.get("po_number")), int(inv.get("invoice_number")))
                    invoice_map[k] = inv

            # =========================
            # PHASE 3: IN-MEMORY LOGIC
            # =========================

            # 3a) CC/PC => compare receipts
            for d_item in detail_items_input:
                if d_item["payment_type"] not in ["CC", "PC"]:
                    continue
                key = (
                    int(d_item.get("project_number")),
                    int(d_item.get("po_number")),
                    int(d_item.get("detail_item_id"))
                )
                sub_total = float(d_item.get("total") or 0.0)
                if key in receipt_map:
                    receipt_total = float(receipt_map[key].get("total") or 0.0)
                    if abs(receipt_total - sub_total) < 0.0001:
                        self.logger.info(f"✅ Receipt matches detail => {key}, marking as REVIEWED.")
                        d_item["state"] = "REVIEWED"
                        d_item["_spend_money"] = {
                            "project_number": int(d_item["project_number"]),
                            "po_number": int(d_item["po_number"]),
                            "detail_number": int(d_item["detail_item_id"]),
                            "line_number": 1,
                            "state": "DRAFT",
                            "amount": receipt_total
                        }
                    else:
                        self.logger.info(
                            f"🔻 Mismatch for detail => {key}, sub_total={sub_total}, receipt={receipt_total}")
                else:
                    self.logger.info(f"ℹ️ No receipt found for detail => {key}.")

            # 3b) INV/PROF => sum up detail sub_totals vs. invoice

            xero_bill_groups = {}
            for d_item in detail_items_input:
                if d_item["payment_type"] not in ["INV", "PROF"]:
                    continue

                key = (
                    d_item.get("project_number"),
                    d_item.get("po_number"),
                    d_item.get("detail_item_id")
                )
                project_number, po_number, invoice_num = key

                # find siblings in memory
                siblings = [
                    x for x in detail_items_input
                    if x.get("project_number") == project_number
                       and x.get("po_number") == po_number
                       and x.get("detail_item_id") == invoice_num
                ]
                total_of_siblings = sum(float(x.get("total") or 0.0) for x in siblings)

                inv_key = (int(project_number), int(po_number), invoice_num)
                invoice_row = invoice_map.get(inv_key)
                if not invoice_row:
                    # no invoice found => skip
                    continue

                invoice_total = float(invoice_row.get("total") or 0.0)
                if abs(total_of_siblings - invoice_total) < 0.0001:

                    self.logger.info(f"✅ Sums match => setting all detail lines={invoice_num} to RTP.")
                    for sibling in siblings:
                        sibling["state"] = "RTP"
                    xero_bill_groups.setdefault(inv_key, []).extend(siblings)

            # =============================
            # PHASE 4: BULK CREATE / UPDATE
            # =============================
            items_to_create = []
            items_to_update = []

            for d_item in detail_items_input:
                key = (
                    int(d_item.get("project_number")),
                    int(d_item.get("po_number")),
                    d_item.get("detail_item_id"),
                    d_item.get("line_number")
                )
                common_data = {
                    "project_number": d_item.get("project_number"),
                    "po_number": d_item.get("po_number"),
                    "detail_number": d_item.get("detail_item_id"),
                    "line_number": d_item.get("line_number"),
                    "vendor": d_item.get("vendor"),
                    "transaction_date": d_item.get("date"),
                    "due_date": d_item.get("due date"),
                    "quantity": d_item.get("quantity"),
                    "rate": d_item.get("rate"),
                    "description": d_item.get("description"),
                    "state": d_item.get("state"),
                    "account_code": d_item.get("account"),
                    "payment_type": d_item.get("payment_type"),
                    "ot": d_item.get("ot"),
                    "fringes": d_item.get("fringes")
                }
                if key in existing_map:
                    # It's an update
                    common_data["id"] = existing_map[key]["id"]
                    items_to_update.append(common_data)
                else:
                    # It's a new record
                    items_to_create.append(common_data)

            self.logger.info(
                f"[Detail Aggregator] {len(items_to_create)} items to create, {len(items_to_update)} to update.")

            # 4a) Bulk Create
            created_items = []
            if items_to_create:
                for chunk in self.chunk_list(items_to_create, chunk_size):
                    created_sub = self.db_ops.bulk_create_detail_items(chunk, session=session)
                    created_items.extend(created_sub)
                    session.flush()

            # 4b) Bulk Update
            updated_items = []
            if items_to_update:
                for chunk in self.chunk_list(items_to_update, chunk_size):
                    updated_sub = self.db_ops.bulk_update_detail_items(chunk, session=session)
                    updated_items.extend(updated_sub)
                    session.flush()

            # Build a map of (proj, po, detail_number) -> new DB record (for referencing in side effects)
            detail_item_id_map = {}
            for di in (created_items + updated_items):
                k = (di.get("project_number"), di.get("po_number"), di.get("detail_number"))
                detail_item_id_map[k] = di

            # =============================
            # PHASE 5: POST-PERSIST SIDE EFFECTS
            # =============================

            # 5a) SPEND MONEY for CC/PC
            spend_money_items = []
            for d_item in detail_items_input:
                if d_item.get("payment_type") in ["CC", "PC"] and d_item.get("_spend_money"):
                    sm_key = (
                        d_item.get("project_number"),
                        d_item.get("po_number"),
                        d_item.get("detail_item_id")
                    )
                    if sm_key in detail_item_id_map:
                        spend_money_items.append(d_item["_spend_money"])

            if spend_money_items:
                self.logger.info(f"💳 Creating {len(spend_money_items)} SpendMoney records.")
                for chunk in self.chunk_list(spend_money_items, chunk_size):
                    new_sm_items = self.db_ops.bulk_create_spend_money(chunk, session=session)
                    session.flush()
                    for sm in new_sm_items:
                        self.logger.info(f"💸 Created SpendMoney => ID={sm['id']}, calling Xero handle.")
                        self.xero_services.handle_spend_money_create(sm["id"])

            # 5b) Xero Bill creation for INV/PROF items that are all RTP
            xero_bill_todo = []
            for inv_key, siblings in xero_bill_groups.items():
                # If all siblings are RTP, create a XeroBill
                all_rtp = all((sib.get("state") or "").upper() == "RTP" for sib in siblings)
                if all_rtp:
                    xero_bill_todo.append(inv_key)

            if xero_bill_todo:
                self.logger.info(f"🧾 Creating XeroBills for {len(xero_bill_todo)} invoice groups.")
                for chunk in self.chunk_list(xero_bill_todo, chunk_size):
                    for (prj, po, dt) in chunk:
                        new_bill = self.db_ops.create_xero_bill_by_keys(
                            project_number=prj,
                            po_number=po,
                            detail_number=dt,
                            state="DRAFT",
                            session=session
                        )
                        session.flush()
                        if new_bill:
                            self.logger.info(f"🆕 Created XeroBill => ID={new_bill['id']}, pushing to Xero.")
                            self.xero_services.create_xero_bill_in_xero(new_bill)

            # =============================
            # PHASE 6: UPDATE MONDAY (OPTIONAL)
            # =============================
            self.logger.info("[Detail Aggregator, PHASE 6] => Upserting changes to Monday (SKIPPED).")
            # if needed, do chunked upserts to Monday here

            self.logger.info("[Detail Aggregator] DONE => Will commit once aggregator completes.")
            # The final commit happens outside (with the `session` context).

        except Exception as e:
            self.logger.exception("Exception in process_aggregator_detail_items.")
            raise

    #endregion

    # region 🪄 Aggregator Status Checks
    def is_aggregator_in_progress(self, record: dict) -> bool:
        """
        Determine if aggregator is still 'STARTED' for the project_number
        associated with this record. If so => partial skip logic in triggers.

        :param record: Typically a PO or DetailItem dict with 'project_number'.
        :return: True if aggregator=STARTED, else False
        """
        try:
            project_number = record.get('project_number')
            if not project_number:
                return False  # no aggregator concept if missing project_number

            self.logger.info(f"🔎 Checking aggregator logs for project_number={project_number} to see if status=STARTED.")
            po_logs = self.db_ops.search_po_logs(['project_number'], [project_number])
            if not po_logs:
                return False

            # If single dict, unify to list
            if isinstance(po_logs, dict):
                po_logs = [po_logs]

            for pl in po_logs:
                if pl.get('status') == 'STARTED':
                    self.logger.info("🚦 Found aggregator log with status=STARTED => aggregator in progress!")
                    return True
            return False
        except Exception as e:
            self.logger.exception("Exception in is_aggregator_in_progress.", exc_info=True)
            raise

    def is_aggregator_done(self, record: dict) -> bool:
        """
        Optional method if you want to specifically confirm aggregator=COMPLETED.
        :param record: the DB record (PO, detail item, etc.)
        :return: True if aggregator found with status=COMPLETED, else False
        """
        try:
            project_number = record.get('project_number')
            if not project_number:
                return True  # if no aggregator concept => assume done

            self.logger.info(f"🔎 Checking aggregator logs for project_number={project_number} to see if status=COMPLETED.")
            po_logs = self.db_ops.search_po_logs(['project_number'], [project_number])
            if not po_logs:
                return True

            if isinstance(po_logs, dict):
                po_logs = [po_logs]

            for pl in po_logs:
                if pl.get('status') == 'COMPLETED':
                    self.logger.info("🏁 Aggregator found with status=COMPLETED => aggregator done!")
                    return True
            return False
        except Exception as e:
            self.logger.exception("Exception in is_aggregator_done.", exc_info=True)
            raise
    # endregion

    # region 📝 Summation and State Changes for Invoices & Details
    def set_invoice_details_rtp(self, detail_item: dict, buffer: List[dict]):
        """
        Marks all detail items (from the provided buffer) for the same invoice as 'RTP'.
        Typically used after sum_detail_items_and_compare_invoice returns True.
        """
        try:
            self.logger.info("🔖 Setting detail items => RTP because sums matched invoice total.")

            project_number = detail_item.get('project_number')
            po_number = detail_item.get('po_number')
            invoice_num = detail_item.get('detail_number')  # or use a separate key if preferred

            siblings = [
                di for di in buffer
                if di.get('project_number') == project_number
                   and di.get('po_number') == po_number
                   and di.get('detail_number') == invoice_num
            ]
            if not siblings:
                self.logger.warning("🙅 No siblings found in buffer => no updates.")
                return
            if isinstance(siblings, dict):
                siblings = [siblings]

            for sib in siblings:
                current_state = (sib.get('state') or '').upper()
                # Skip if the state is already final
                if current_state not in {"PAID", "RECONCILED", "APPROVED"}:
                    # Update detail item in the DB (or buffer update logic if needed)
                    self.db_ops.update_detail_item(sib['id'], state="RTP")
                    self.logger.info(f"✨ DetailItem(id={sib['id']}) => RTP (invoice sums matched).")
        except Exception as e:
            self.logger.exception("Exception in set_invoice_details_rtp.", exc_info=True)
            raise

    def sum_detail_items_and_compare_invoice(self, detail_item: dict, buffer: List[dict], session) -> bool:
        """
        Gathers all detail items matching (proj, po, detail_number),
        sums their sub_total, then compares with the matching invoice total in DB.
        Returns True if they match within threshold, else False.
        Now it always uses the aggregator's session.
        """
        try:
            project_number = detail_item.get('project_number')
            po_number = detail_item.get('po_number')
            invoice_num = detail_item.get('detail_item_id')

            self.logger.info(f"🧮 Summation => Checking detail items vs invoice totals for inv={invoice_num}")

            details = [
                di for di in buffer
                if di.get('project_number') == project_number
                   and di.get('po_number') == po_number
                   and di.get('detail_item_id') == invoice_num
            ]
            if not details:
                return False

            total_of_details = sum(float(di.get('sub_total') or 0.0) for di in details)

            # Fetch invoice from DB
            invoice = self.db_ops.search_invoice_by_keys(
                project_number=project_number,
                po_number=po_number,
                invoice_number=invoice_num,
                session=session  # always aggregator session
            )
            if not invoice:
                return False
            if isinstance(invoice, list):
                invoice = invoice[0]

            invoice_total = float(invoice.get('total', 0.0))
            return (abs(total_of_details - invoice_total) < 0.0001)

        except Exception as e:
            self.logger.exception("Exception in sum_detail_items_and_compare_invoice.", exc_info=True)
            raise

    def check_siblings_all_rtp(self, detail_item: dict, buffer: List[dict]) -> bool:
        """
        Returns True if all detail items in 'buffer' for the same invoice are in state='RTP'.
        """
        try:
            project_number = detail_item.get('project_number')
            po_number = detail_item.get('po_number')
            detail_number = detail_item.get('detail_item_id')

            siblings = [
                di for di in buffer
                if di.get('project_number') == project_number
                   and di.get('po_number') == po_number
                   and di.get('detail_item_id') == detail_number
            ]
            if not siblings:
                return False
            for sib in siblings:
                if (sib.get('state') or '').upper() != 'RTP':
                    return False
            return True

        except Exception as e:
            self.logger.exception("Exception in check_siblings_all_rtp.", exc_info=True)
            raise

    # endregion

    # endregion

    # region 🌐 Xero Bill Date Range Update
    def update_xero_bill_dates_from_detail_item(self, xero_bill: dict):
        """
        Update the parent XeroBill's transaction_date => earliest among all detail items,
        and due_date => latest among them. If aggregator=STARTED, you might skip or do partial.

        :param xero_bill: dict with project_number, po_number, detail_number, etc.
        """
        try:
            self.logger.info("🤖 Updating XeroBill date range from detail items...")

            parent_bill_id = xero_bill['id']
            project_number = xero_bill.get('project_number')
            po_number = xero_bill.get('po_number')
            detail_number = xero_bill.get('detail_number')

            # region ⚙️ Gather relevant detail items
            detail_items = self.db_ops.search_detail_item_by_keys(
                project_number=project_number,
                po_number=po_number,
                detail_number=detail_number
            )
            if not detail_items:
                self.logger.info("🙅 No detail items => skipping date update.")
                return
            if isinstance(detail_items, dict):
                detail_items = [detail_items]
            # endregion

            # region 🗓 Find earliest transaction_date, latest due_date
            existing_parent_date = xero_bill.get('transaction_date')
            existing_parent_due = xero_bill.get('due_date')

            detail_dates = []
            detail_dues = []
            for di in detail_items:
                if di.get('transaction_date'):
                    detail_dates.append(di['transaction_date'])
                if di.get('due_date'):
                    detail_dues.append(di['due_date'])

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
                    # try parse
                    try:
                        return datetime.fromisoformat(d).date()
                    except:
                        return None
                return None

            # convert everything
            detail_dates = [to_date(d) for d in detail_dates if to_date(d)]
            detail_dues = [to_date(d) for d in detail_dues if to_date(d)]

            if detail_dates:
                earliest_date = min(detail_dates)
            else:
                earliest_date = existing_parent_date
            if detail_dues:
                latest_due = max(detail_dues)
            else:
                latest_due = existing_parent_due

            self.logger.info(f"🔎 Determined => earliest_date={earliest_date}, latest_due={latest_due}.")
            # endregion

            # region 🛠 Update bill if changed
            if earliest_date != existing_parent_date or latest_due != existing_parent_due:
                self.logger.info("🌀 Updating XeroBill with new date range!")
                self.db_ops.update_xero_bill(
                    xero_bill_id=xero_bill['id'],
                    transaction_date=earliest_date,
                    due_date=latest_due
                )
            else:
                self.logger.info("🙆 No changes needed => date range is already correct.")
            # endregion
        except Exception as e:
            self.logger.exception("Exception in update_xero_bill_dates_from_detail_item.", exc_info=True)
            raise
    # endregion

    # region 🏗️ HELPER METHODS
    def parse_po_log_data(self, po_log: dict) -> list[Any] | dict[str, Any]:
        try:
            po_log_db_path = po_log["db_path"]
            po_log_filename = po_log["filename"]
            project_number = po_log["project_number"]

            temp_file_path = f'../temp_files/{os.path.basename(po_log_filename)}'
            self.PROJECT_NUMBER = project_number

            po_log_file_path = temp_file_path

            # Attempt to download if not local
            if not os.path.exists(temp_file_path):
                self.logger.info('🛠 Not using local temp files? Attempting direct download from dropbox...')
                if not self.dropbox_service.download_file_from_dropbox(
                        (po_log_db_path + po_log_filename), temp_file_path):
                    return []
                self.logger.info(f'📝 Received PO Log file from Dropbox: {po_log_filename}')

            self.logger.info('🔧 Passing parsed PO log data (main, detail, contacts) to DB aggregator...')
            main_items, detail_items, contacts = \
                self.dropbox_service.extract_data_from_po_log(temp_file_path, project_number)

            return {
                "main_items": main_items,
                "detail_items": detail_items,
                "contacts": contacts
            }
        except Exception as e:
            self.logger.exception("Exception in parse_po_log_data.", exc_info=True)
            raise

    def chunk_list(self, items, chunk_size=500):
        """
        Generator that yields successive `chunk_size`-sized chunks
        from the given list.
        """
        for i in range(0, len(items), chunk_size):
            yield items[i:i + chunk_size]
    #endregion


budget_service = BudgetService()




