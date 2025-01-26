"""
files_budget/budget_service.py

Handles aggregator logic for PO logs (renamed from budget_service to budget_service),
along with functions for summing detail items vs. invoice totals, checking aggregator
status, setting items to RTP, and adjusting Xero bill dates.

Integration Points:
- db_ops (DatabaseOperations) from database.database_util
- Possibly calls or other domain services (if needed).
"""

import logging
import os
from typing import Any

from database.database_util import DatabaseOperations
from database.db_util import get_db_session
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
            self.logger.info("🧩 BudgetService (aggregator logic) initialized!")
        except Exception as e:
            logging.exception("Error initializing BudgetService.", exc_info=True)
            raise



    #region🌹 CONTACT AGGREGATOR FUNCTIONS
    def process_contact_aggregator(self, contacts_data: list[dict], session):
        """
        Aggregator for CONTACTS with:
          - Phase 1: Minimal DB creation, plus batch upsert to Xero & Monday
          - Phase 2: Final linking of tax forms, verifying data, final Xero & Monday updates

        'contacts_data' might look like:
          [
            {
              "name": "Vendor Name",
            },
            ...
          ]
        """
        try:
            self.logger.info("🤝 [Contact Aggregator] START => We are about to process contact data in multi-phase style.")

            if not contacts_data:
                self.logger.info("🤷‍♂️ [Contact Aggregator] No contacts provided. Nothing to do.")
                return

            #region 🤠 PHASE 1: Minimal contact creation + Xero/Monday Upsert
            ###########################################################
            try:
                self.logger.info("🔎 [Contact Aggregator, PHASE 1] => Creating or updating DB contacts, then sending to Xero & Monday...")

                with get_db_session() as session:
                    self.logger.debug("🗄️ [Contact Aggregator, PHASE 1] => Opened a new DB session for batch commits.")
                    try:
                        all_db_contacts = self.db_ops.search_contacts()
                    except Exception as e:
                        self.logger.exception("Exception searching all contacts in PHASE 1 of contact aggregator.", exc_info=True)
                        all_db_contacts = []

                    if not all_db_contacts:
                        self.logger.debug("📝 [Contact Aggregator, PHASE 1] => Found no existing contacts in DB. Starting fresh.")
                    else:
                        self.logger.debug(
                            f"📝 [Contact Aggregator, PHASE 1] => Found {len(all_db_contacts)} existing contacts in DB for potential fuzzy matches.")

                    for contact_item in contacts_data:
                        try:
                            name = (contact_item.get('name') or '').strip()
                            self.logger.debug(f"🌀 [Contact Aggregator, PHASE 1] => Processing contact => '{name}' data={contact_item}")
                            if not name:
                                self.logger.warning("🚫 [Contact Aggregator, PHASE 1] => No 'name' in contact_item. Skipping this record.")
                                continue

                            # region Fuzzy match or create
                            contact_id = None
                            if all_db_contacts:
                                try:
                                    fuzzy_matches = self.db_ops.find_contact_close_match(name, all_db_contacts)
                                    if fuzzy_matches:
                                        contact_id = fuzzy_matches[0]['id']
                                        self.logger.debug(
                                            f"✅ [Contact Aggregator, PHASE 1] => Fuzzy matched contact => ID={contact_id} for name='{name}'")
                                except Exception as e:
                                    self.logger.exception("Exception during fuzzy matching in contact aggregator.", exc_info=True)

                            if not contact_id:
                                self.logger.info(f"🆕 [Contact Aggregator, PHASE 1] => Creating a new contact in DB for '{name}'")
                                try:
                                    new_ct = self.db_ops.create_contact(
                                        session=session,
                                        name=name,
                                    )
                                    if not new_ct:
                                        self.logger.error(f"❌ [Contact Aggregator, PHASE 1] => Could not create new contact for '{name}'.")
                                        continue
                                    contact_id = new_ct['id']
                                    self.logger.info(f"🎉 [Contact Aggregator, PHASE 1] => Successfully created contact => ID={contact_id}")
                                except Exception as e:
                                    self.logger.exception("Exception creating new contact in contact aggregator.", exc_info=True)
                                    continue
                            # endregion

                            # region DB lookup to get fresh data
                            try:
                                db_contact = self.db_ops.search_contacts(["id"], [contact_id], session=session)
                                if isinstance(db_contact, list):
                                    db_contact = db_contact[0]
                                self.logger.debug(f"🔍 [Contact Aggregator, PHASE 1] => DB contact => {db_contact}")
                            except Exception as e:
                                self.logger.exception("Exception retrieving fresh contact from DB in contact aggregator PHASE 1.", exc_info=True)
                                continue
                            # endregion

                            # region Load XERO upserts into Batch Queue
                            try:
                                xero_id = db_contact.get('xero_id')
                                if xero_id:
                                    self.logger.debug(
                                        f"🔗 [Contact Aggregator, PHASE 1] => Contact ID={contact_id} already has xero_id={xero_id}. Will do a batch update.")
                                    self.xero_services.buffered_upsert_contact(db_contact)
                                else:
                                    self.logger.info(
                                        f"🆕 [Contact Aggregator, PHASE 1] => No xero_id for contact ID={contact_id}, buffering upsert as NEW in Xero.")
                                    self.xero_services.buffered_upsert_contact(db_contact)
                            except Exception as e:
                                self.logger.exception("Exception buffering Xero contact upsert.", exc_info=True)
                            # endregion

                            # region Load MONDAY upserts into Batch Queue
                            try:
                                pulse_id = db_contact.get('pulse_id')
                                if pulse_id:
                                    self.logger.debug(
                                        f"🔗 [Contact Aggregator, PHASE 1] => Contact ID={contact_id} already has pulse_id={pulse_id}. Buffering update in Monday.")
                                    monday_service.buffered_upsert_contact(db_contact)
                                else:
                                    self.logger.info(
                                        f"🆕 [Contact Aggregator, PHASE 1] => No pulse_id for contact ID={contact_id}, buffering upsert as NEW in Monday.")
                                    monday_service.buffered_upsert_contact(db_contact)
                            except Exception as e:
                                self.logger.exception("Exception buffering Monday contact upsert.", exc_info=True)
                            # endregion

                        except Exception as e:
                            self.logger.exception("Exception in loop while processing each contact_item in PHASE 1.", exc_info=True)

                    # region Final batch push to Xero & Monday
                    try:
                        self.logger.info("📤 [Contact Aggregator, PHASE 1] => Executing update and create batches to Xero & Monday now.")
                        self.xero_services.execute_batch_upsert_contacts(self.xero_services.contact_upsert_queue)
                        #onday_service.execute_batch_upsert_contacts(self.xero_services.contact_upsert_queue)
                        self.logger.debug("📥 [Contact Aggregator, PHASE 1] => DONE => Xero & Monday upsert done. DB session commit on exit.")
                    except Exception as e:
                        self.logger.exception("Exception executing batch upsert to Xero/Monday in contact aggregator.", exc_info=True)
                    # endregion

            except Exception as e:
                self.logger.exception("Exception in PHASE 1 of contact aggregator.", exc_info=True)
                raise
            #endregion

            #region 🗺️ PHASE 2: Link TaxForms, finalize statuses
            # ###########################################################
            # try:
            #     self.logger.info("📝 [Contact Aggregator, PHASE 2] => Linking tax forms, verifying data, final Xero/Monday updates...")
            #
            #     with get_db_session() as session:
            #         self.logger.debug("🗄️ [Contact Aggregator, PHASE 2] => Opened a new DB session for final updates.")
            #         try:
            #             all_db_contacts_again = self.db_ops.search_contacts(session=session)
            #         except Exception as e:
            #             self.logger.exception("Exception searching contacts in PHASE 2 of contact aggregator.", exc_info=True)
            #             all_db_contacts_again = []
            #
            #         self.logger.debug(
            #             f"📝 [Contact Aggregator, PHASE 2] => Fetched {len(all_db_contacts_again) if all_db_contacts_again else 0} contacts from DB to cross-check tax forms.")
            #
            #         for contact_item in contacts_data:
            #             try:
            #                 name = (contact_item.get('name') or '').strip()
            #                 self.logger.debug(
            #                     f"🌀 [Contact Aggregator, PHASE 2] => Checking final tax form for '{name}' with item data: {contact_item}")
            #                 if not name:
            #                     self.logger.warning("🚫 [Contact Aggregator, PHASE 2] => Missing contact name in item, skipping.")
            #                     continue
            #
            #                 # region Fuzzy match again
            #                 contact_id = None
            #                 if all_db_contacts_again:
            #                     try:
            #                         fuzzy2 = self.db_ops.find_contact_close_match(name, all_db_contacts_again)
            #                         if fuzzy2:
            #                             contact_id = fuzzy2[0]['id']
            #                             self.logger.debug(
            #                                 f"✅ [Contact Aggregator, PHASE 2] => Found contact ID={contact_id} for name='{name}' (fuzzy).")
            #                     except Exception as e:
            #                         self.logger.exception("Exception during second fuzzy match in PHASE 2 of contact aggregator.", exc_info=True)
            #
            #                 if not contact_id:
            #                     self.logger.debug(
            #                         f"🤷‍♂️ [Contact Aggregator, PHASE 2] => No contact found for '{name}'; skipping final tax form link.")
            #                     continue
            #                 # endregion
            #
            #                 # region Link or check tax form
            #                 candidate_file = contact_item.get('tax_file_candidate')
            #                 if candidate_file:
            #                     self.logger.info(
            #                         f"🗂 [Contact Aggregator, PHASE 2] => Attempting to attach tax form '{candidate_file}' to contact ID={contact_id}.")
            #                     if "W9" in candidate_file.upper():
            #                         form_type = "W9"
            #                     else:
            #                         form_type = "UNKNOWN"
            #
            #                     try:
            #                         db_contact2 = self.db_ops.update_contact(
            #                             contact_id, session=session,
            #                             tax_file_link=candidate_file
            #                         )
            #                         self.logger.info(f"📎 [Contact Aggregator, PHASE 2] => Set contact's tax_file_link={candidate_file}")
            #                     except Exception as e:
            #                         self.logger.exception("Exception updating contact with tax file link in PHASE 2 of contact aggregator.", exc_info=True)
            #                 # endregion
            #
            #                 # region finalize status
            #                 try:
            #                     db_contact3 = self.db_ops.search_contacts(["id"], [contact_id], session=session)
            #                     if isinstance(db_contact3, list):
            #                         db_contact3 = db_contact3[0]
            #
            #                     tf_link = db_contact3.get('tax_file_link')
            #                     email_ok = bool(db_contact3.get('email'))
            #                     phone_ok = bool(db_contact3.get('phone'))
            #
            #                     new_status = "PENDING"
            #                     if tf_link:
            #                         if email_ok and phone_ok:
            #                             new_status = "Verified"
            #                         else:
            #                             new_status = "To Review"
            #
            #                     if db_contact3.get('vendor_status') != new_status:
            #                         self.logger.debug(
            #                             f"🖍 [Contact Aggregator, PHASE 2] => Updating vendor_status from '{db_contact3.get('vendor_status')}' to '{new_status}' for contact ID={contact_id}")
            #                         self.db_ops.update_contact(contact_id, session=session, vendor_status=new_status)
            #                 except Exception as e:
            #                     self.logger.exception("Exception finalizing contact status in PHASE 2 of contact aggregator.", exc_info=True)
            #                 # endregion
            #
            #             except Exception as e:
            #                 self.logger.exception("Exception while looping over contact_item in PHASE 2 of contact aggregator.", exc_info=True)
            #
            #         # region Final batch push
            #         try:
            #             self.logger.info("📤 [Contact Aggregator, PHASE 2] => Doing final batch upsert to Xero & Monday for any updated contact records.")
            #             self.xero_services.execute_batch_upsert_contacts()
            #             monday_service.execute_batch_upsert_contacts()
            #             self.logger.debug("📥 [Contact Aggregator, PHASE 2] => Final upsert done; DB commit upon exit.")
            #         except Exception as e:
            #             self.logger.exception("Exception executing final batch upsert to Xero/Monday in contact aggregator PHASE 2.", exc_info=True)
            #         # endregion
            #
            #
            #     self.logger.info("🏁 [Contact Aggregator] ALL DONE => contacts processed in multi-phase with extra logging.")
            # except Exception as e:
            #     self.logger.exception("Exception in PHASE 2 of contact aggregator.", exc_info=True)
            #     raise
            #endregion

        except Exception as e:
            self.logger.exception("Exception in process_contact_aggregator (entire method).", exc_info=True)
            raise
    #endregion

    # region 🌺 PURCHASE ORDERS AGGREGATOR
    ############################################################
    def process_aggregator_pos(self, po_data: dict, session):
        """
        Aggregator for PURCHASE ORDERS, where 'po_data' is typically:
          {
            "main_items": [
                {
                    "project_number": 2416,
                    "po_number": 83,
                    "po type": "INV",
                    "description": "Some desc",
                    "contact_name": "DIRECTOR PLACEHOLDER",
                    ...
                },
                ...
            ]
          }
        We do:
          PHASE 1: Create/Update each PO in DB
          PHASE 2: Update Monday / Xero
        """
        try:
            self.logger.info("🚀 [PO Aggregator] START => Processing PO aggregator data.")

            if not po_data or not po_data.get("main_items"):
                self.logger.info("🤷 [PO Aggregator] No main_items provided. Nothing to do.")
                return

            ###################################################################
            # PHASE 1: CREATE / UPDATE each PO in the DB
            ###################################################################
            po_records_info = []
            self.logger.info("📝 [PO Aggregator, PHASE 1] => Creating/Updating POs in DB.")
            with get_db_session() as session:
                for item in po_data["main_items"]:
                    # Skip if empty
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

                    # region Normalize / find correct po_type
                    if raw_po_type == "PROJ":
                        po_type = "INV"
                    else:
                        po_type = raw_po_type
                    # endregion

                    self.logger.debug(
                        f"🔎 [PO Aggregator, PHASE 1] => project_number={project_number}, po_number={po_number}, "
                        f"po_type={po_type}, contact_name={vendor_name}"
                    )

                    # region Ensure Project exists or create
                    project_record = self.db_ops.search_projects(["project_number"], [project_number], session=session)
                    if not project_record:
                        # create a project if none found
                        self.logger.warning(
                            f"⚠️ [PO Aggregator] => Project {project_number} not found, creating a new one.")
                        project_record = self.db_ops.create_project(
                            session=session,
                            project_number=project_number,
                            name=f"{project_number}_untitled",
                            status="Active",
                            user_id = 1,
                            tax_ledger=14,  # example default
                            budget_map_id=1  # example default
                        )
                        if not project_record:
                            self.logger.warning("❌ Could not create project. Skipping this PO.")
                            continue
                        else:
                            self.logger.info(
                                f"🌱 Created Project => ID={project_record['id']} for project_number={project_number}")

                    # If project_record is a list, unify
                    if isinstance(project_record, list):
                        project_record = project_record[0]
                    project_id = project_record["id"]
                    # endregion

                    # region Lookup contact if the column is "name"
                    contact_id = None
                    if vendor_name:
                        # If your actual Contact table column is "name", do this:
                        found_contact = self.db_ops.search_contacts(["name"], [vendor_name], session=session)
                        if found_contact:
                            if isinstance(found_contact, list):
                                found_contact = found_contact[0]
                            contact_id = found_contact.get("id")
                        else:
                            self.logger.warning(f"⚠️ Contact '{vendor_name}' not found => leaving contact_id=None.")
                    else:
                        self.logger.warning(f"⚠️ Contact '{vendor_name}' not provided => leaving contact data null")
                        vendor_name = "PO LOG Naming Error"

                    # endregion

                    # region Check if PO exists
                    existing = self.db_ops.search_purchase_order_by_keys(project_number, po_number, session=session)
                    # endregion

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
                            vendor_name = vendor_name
                        )
                        if new_po:
                            self.logger.info(f"✅ Created new PO => ID={new_po['id']}")
                            po_records_info.append(new_po)
                        else:
                            self.logger.warning("❌ Failed to create new PO.")
                    else:
                        # If existing is a list, unify
                        if isinstance(existing, list):
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
                            # UPDATE
                            self.logger.info(f"🔄 Updating existing PO => ID={po_id}.")
                            updated_po = self.db_ops.update_purchase_order(
                                po_id,
                                session=session,
                                description=description,
                                po_type=po_type,
                                contact_id=contact_id,
                                project_id=project_id,
                                vendor_name = vendor_name
                            )
                            if updated_po:
                                self.logger.info(f"🔄 Updated PO => ID={updated_po['id']}")
                                po_records_info.append(updated_po)
                            else:
                                self.logger.warning("❌ Failed to update existing PO.")
                        else:
                            self.logger.info(f"🏳️‍🌈 No changes to existing PO => ID={po_id}.")
            self.logger.info("🏁 [PO Aggregator] [PHASE 1] => DONE => POs loaded into DB")
            ###################################################################
            # PHASE 2: UPDATE MONDAY
            ###################################################################
            self.logger.info("🔄 [PO Aggregator] [PHASE 2] => START => POs to Monday.")
            # for po_record in po_records_info:
            #     proj_num = po_record.get("project_number")
            #     the_po_number = po_record.get("po_number")
            #     found_po = self.db_ops.search_purchase_order_by_keys(proj_num, the_po_number, session=session)
            #     if found_po and not isinstance(found_po, list):
            #         monday_service.upsert_po_in_monday(found_po)
            self.logger.info("🏁 [PO Aggregator] [PHASE 2] DONE => POs to Monday")
            self.logger.info("🏁 [PO Aggregator] [COMPLETED]")

        except Exception as e:
            self.logger.exception(f"Exception in process_aggregator_pos: {e}")
            raise
    #endregion

    # region 🌻 DETAIL ITEMS AGGREGATOR
    ############################################################
    def process_aggregator_detail_items(self, po_log_data: dict):
        """
        Aggregator for DETAIL ITEMS, expecting something like:
          {
            "detail_items": [
               {
                 "project_number": 2416,
                 "po_number": 83,
                 "detail_item_id": 101,
                 "payment_type": "CC",
                 "account": "6001",
                 "due date": "2025-01-31",
                 ...
               },
               ...
            ]
          }
        Multi-phase:
          PHASE 1: Create/Update detail items
          PHASE 2: Receipts/Invoices
          PHASE 3: Update Monday
        """
        try:
            self.logger.info("🚀 [Detail Aggregator] START => Processing detail items data.")

            if not po_log_data or not po_log_data.get("detail_items"):
                self.logger.info("🤷 [Detail Aggregator] No detail_items to process, returning.")
                return

            detail_items_info = []

            ###################################################################
            # PHASE 1: CREATE / UPDATE detail items
            ###################################################################
            self.logger.info("📝 [Detail Aggregator, PHASE 1] => Create/Update detail items in DB.")
            with get_db_session() as session:
                for d_item in po_log_data["detail_items"]:
                    if not d_item:
                        continue

                    project_number = d_item.get("project_number")
                    po_number = d_item.get("po_number")
                    detail_number = d_item.get("detail_item_id")
                    payment_type = (d_item.get("payment_type") or "").upper()

                    if not detail_number:
                        self.logger.warning("❌ detail_item_id missing, skipping.")
                        continue

                    existing = self.db_ops.search_detail_item_by_keys(
                        project_number=project_number,
                        po_number=po_number,
                        detail_number=detail_number,
                        line_number=1,  # or pass a real line_number if aggregator has that
                        session=session
                    )
                    if not existing:
                        # CREATE
                        self.logger.info("🌱 Creating new DetailItem in DB.")
                        new_di = self.db_ops.create_detail_item_by_keys(
                            project_number=project_number,
                            po_number=po_number,
                            detail_number=detail_number,
                            line_number=1,
                            session=session,
                            vendor=d_item.get("vendor"),
                            transaction_date=d_item.get("date"),
                            due_date=d_item.get("due date"),
                            quantity=d_item.get("quantity"),
                            rate=d_item.get("rate"),
                            description=d_item.get("description"),
                            state=d_item.get("state"),
                            account_code=d_item.get("account"),  # DB column is account_code
                            payment_type=payment_type,
                            ot=d_item.get("ot"),
                            fringes=d_item.get("fringes")
                        )
                        if new_di:
                            self.logger.info(f"✅ Created DetailItem => ID={new_di['id']}")
                            detail_items_info.append(new_di)
                    else:
                        # UPDATE
                        if isinstance(existing, list):
                            existing = existing[0]
                        detail_id = existing["id"]
                        updated_di = self.db_ops.update_detail_item(
                            detail_id,
                            session=session,
                            vendor=d_item.get("vendor"),
                            transaction_date=d_item.get("date"),
                            due_date=d_item.get("due date"),
                            quantity=d_item.get("quantity"),
                            rate=d_item.get("rate"),
                            description=d_item.get("description"),
                            state=d_item.get("state"),
                            account_code=d_item.get("account"),
                            payment_type=payment_type,
                            ot=d_item.get("ot"),
                            fringes=d_item.get("fringes")
                        )
                        if updated_di:
                            self.logger.info(f"🔄 Updated DetailItem => ID={updated_di['id']}")
                            detail_items_info.append(updated_di)

            ###################################################################
            # PHASE 2: Logic for CC/PC receipts vs. INV/PROF invoices
            ###################################################################
            self.logger.info("🚧 [Detail Aggregator, PHASE 2] => Checking receipts or invoice sums.")
            final_detail_items = []
            with get_db_session() as session:
                for di in detail_items_info:
                    detail_id = di["id"]
                    current_state = (di.get("state") or "").upper()
                    ptype = (di.get("payment_type") or "").upper()
                    project_number = di.get("project_number")
                    po_number = di.get("po_number")
                    dnumber = di.get("detail_number")
                    sub_total = float(di.get("sub_total") or 0.0)

                    # skip final states
                    if current_state in {"PAID", "RECONCILED", "APPROVED"}:
                        self.logger.info(f"🛑 DetailItem ID={detail_id} is final => skipping aggregator logic.")
                        final_detail_items.append(di)
                        continue

                    # region CC/PC => check receipt match => set REVIEWED => create spend money
                    if ptype in ["CC", "PC"]:
                        self.logger.info(f"💳 Checking receipts for detail_item ID={detail_id}.")
                        found_receipt = self.db_ops.search_receipt_by_keys(
                            project_number=project_number,
                            po_number=po_number,
                            detail_number=dnumber,
                            line_number=1,  # adjust if real line_number
                            session=session
                        )
                        if found_receipt:
                            if isinstance(found_receipt, list):
                                found_receipt = found_receipt[0]
                            receipt_total = float(found_receipt.get("total") or 0.0)
                            if abs(receipt_total - sub_total) < 0.0001:
                                self.logger.info(
                                    f"✅ Receipt matches sub_total => setting detail_item ID={detail_id} to REVIEWED.")
                                self.db_ops.update_detail_item(detail_id, session=session, state="REVIEWED")
                                # Also create spend_money
                                new_sm = self.db_ops.create_spend_money_by_keys(
                                    project_number=project_number,
                                    po_number=po_number,
                                    detail_number=dnumber,
                                    line_number=1,
                                    state="DRAFT",
                                    amount=receipt_total,
                                    session=session
                                )
                                if new_sm:
                                    sm_id = new_sm["id"]
                                    self.logger.info(f"💸 Created SpendMoney => ID={sm_id}, calling Xero.")
                                    self.xero_services.handle_spend_money_create(sm_id)
                            else:
                                self.logger.info("🔻 Mismatch between receipt & sub_total => no changes.")
                        else:
                            self.logger.info("😶 No receipt found => cannot set REVIEWED.")
                    # endregion

                    # region INV/PROF => sum detail => if match => set RTP => create Xero bill
                    elif ptype in ["INV", "PROF"]:
                        self.logger.info(f"📑 Summation vs. invoice for detail_item ID={detail_id}.")
                        # Suppose we have methods: sum_detail_items_and_compare_invoice, set_invoice_details_rtp, etc.
                        if self.sum_detail_items_and_compare_invoice(di):
                            self.logger.info(f"✅ Sums match => setting siblings to RTP for detail_item ID={detail_id}.")
                            self.set_invoice_details_rtp(di)

                            if self.check_siblings_all_rtp(di):
                                self.logger.info("🟢 All siblings are RTP => create XeroBill.")
                                new_bill = self.db_ops.create_xero_bill_by_keys(
                                    project_number=project_number,
                                    po_number=po_number,
                                    detail_number=dnumber,
                                    state="DRAFT",
                                    session=session
                                )
                                if new_bill:
                                    self.logger.info(f"🆕 Created xero_bill => ID={new_bill['id']}, pushing to Xero.")
                                    self.xero_services.create_xero_bill_in_xero(new_bill)
                    # endregion

                    # Re-fetch final state
                    refetched = self.db_ops.search_detail_item_by_keys(
                        project_number=project_number,
                        po_number=po_number,
                        detail_number=dnumber,
                        line_number=1,
                        session=session
                    )
                    if refetched and not isinstance(refetched, list):
                        final_detail_items.append(refetched)
                    else:
                        final_detail_items.append(di)

            ###################################################################
            # PHASE 3: Update Monday
            ###################################################################
            self.logger.info("🔄 [Detail Aggregator, PHASE 3] => Upserting changes to Monday. CURRENTLY SKIPPED")
            # with get_db_session() as session:
            #     for fdi in final_detail_items:
            #         pn = fdi.get("project_number")
            #         pno = fdi.get("po_number")
            #         dno = fdi.get("detail_number")
            #
            #         # Upsert main PO
            #         po_rec = self.db_ops.search_purchase_order_by_keys(pn, pno, session=session)
            #         if po_rec and not isinstance(po_rec, list):
            #             monday_service.upsert_po_in_monday(po_rec)
            #
            #         # Upsert subitem
            #         di_rec = self.db_ops.search_detail_item_by_keys(pn, pno, dno, line_number=1, session=session)
            #         if di_rec and not isinstance(di_rec, list):
            #             monday_service.upsert_detail_subitem_in_monday(di_rec)

            self.logger.info("🏁 [Detail Aggregator] DONE => detail items processed in multi-phase.")
        except Exception as e:
            self.logger.exception("Exception in process_aggregator_detail_items.")
            raise

    # endregion 🌻 DETAIL ITEMS AGGREGATOR


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
    def sum_detail_items_and_compare_invoice(self, detail_item: dict) -> bool:
        """
        Gathers all detail items matching (project_number, po_number, detail_number),
        sums their sub_total, then compares with the matching invoice total.
        Returns True if they match within small threshold; else False.

        :param detail_item: a dictionary with project_number, po_number, detail_number, etc.
        """
        try:
            project_number = detail_item.get('project_number')
            po_number = detail_item.get('po_number')
            invoice_num = detail_item.get('detail_number')  # or separate key if you prefer

            self.logger.info(f"🧮 Summation logic => Checking detail items for invoice vs. sub_totals (proj={project_number}, po={po_number}, inv={invoice_num}).")

            # region 🏗️ Gather all detail items
            try:
                details = self.db_ops.search_detail_item_by_keys(
                    project_number=project_number,
                    po_number=po_number,
                    detail_number=invoice_num
                )
                if not details:
                    self.logger.info("😶 No detail items found => returning False.")
                    return False
                if isinstance(details, dict):
                    details = [details]
            except Exception as e:
                self.logger.exception("Error searching detail items in sum_detail_items_and_compare_invoice.", exc_info=True)
                raise

            total_of_details = sum(float(di.get('sub_total') or 0.0) for di in details)
            self.logger.info(f"🔢 Summation of detail sub_totals => {total_of_details}")
            # endregion

            # region 📑 Fetch matching invoice
            try:
                invoice = self.db_ops.search_invoice_by_keys(
                    project_number=project_number,
                    po_number=po_number,
                    invoice_number=invoice_num
                )
            except Exception as e:
                self.logger.exception("Error searching invoice in sum_detail_items_and_compare_invoice.", exc_info=True)
                raise

            if not invoice:
                self.logger.info("🤷 Invoice not found => returning False.")
                return False
            if isinstance(invoice, list):
                invoice = invoice[0]
            invoice_total = float(invoice.get('total', 0.0))
            self.logger.info(f"📄 Found invoice => total={invoice_total}")
            # endregion

            # region 🤏 Compare totals
            if abs(total_of_details - invoice_total) < 0.0001:
                self.logger.info("✅ Sums match the invoice total!")
                return True
            else:
                self.logger.info("❌ Sums do NOT match invoice => returning False.")
                return False
            # endregion
        except Exception as e:
            self.logger.exception("Exception in sum_detail_items_and_compare_invoice.", exc_info=True)
            raise

    def set_invoice_details_rtp(self, detail_item: dict):
        """
        Mark all detail items for the same invoice => state='RTP'
        Typically used after sum_detail_items_and_compare_invoice returns True.
        """
        try:
            self.logger.info("🔖 Setting detail items => RTP because sums matched invoice total.")

            project_number = detail_item.get('project_number')
            po_number = detail_item.get('po_number')
            invoice_num = detail_item.get('detail_number')  # or separate

            siblings = self.db_ops.search_detail_item_by_keys(
                project_number=project_number,
                po_number=po_number,
                detail_number=invoice_num
            )
            if not siblings:
                self.logger.warning("🙅 No siblings found => no updates.")
                return
            if isinstance(siblings, dict):
                siblings = [siblings]

            for sib in siblings:
                current_state = (sib.get('state') or '').upper()
                # skip if it's final
                if current_state not in {"PAID", "RECONCILED", "APPROVED"}:
                    self.db_ops.update_detail_item(sib['id'], state="RTP")
                    self.logger.info(f"✨ DetailItem(id={sib['id']}) => RTP (invoice sums matched).")
        except Exception as e:
            self.logger.exception("Exception in set_invoice_details_rtp.", exc_info=True)
            raise

    def check_siblings_all_rtp(self, detail_item: dict) -> bool:
        """
        Returns True if all detail items for the same (project_number, po_number, detail_number)
        have state='RTP'.
        """
        try:
            self.logger.info("🕵️ Checking if ALL siblings are 'RTP'...")

            project_number = detail_item.get('project_number')
            po_number = detail_item.get('po_number')
            detail_number = detail_item.get('detail_number')

            siblings = self.db_ops.search_detail_item_by_keys(
                project_number=project_number,
                po_number=po_number,
                detail_number=detail_number
            )
            if not siblings:
                self.logger.info("🙅 No siblings => returning False.")
                return False
            if isinstance(siblings, dict):
                siblings = [siblings]

            for sib in siblings:
                s_state = (sib.get('state') or '').upper()
                if s_state != 'RTP':
                    self.logger.info(f"🚫 Found sibling detail_item id={sib['id']} state={s_state} => not all RTP.")
                    return False
            self.logger.info("✅ All siblings are RTP!")
            return True
        except Exception as e:
            self.logger.exception("Exception in check_siblings_all_rtp.", exc_info=True)
            raise
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
    #endregion


budget_service = BudgetService()