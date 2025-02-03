import logging
import re

from database.database_util import DatabaseOperations
from files_xero.xero_api import xero_api
from utilities.singleton import SingletonMeta

class XeroServices(metaclass=SingletonMeta):
    """
    Orchestrates DB <-> Xero:
      - create / update / delete Xero bills
      - update local DB with returned IDs
      - handle spend money logic
    """
    def __init__(self):
        self.logger = logging.getLogger('xero_logger')
        self.logger.setLevel(logging.DEBUG)
        self.xero_api = xero_api

        # We'll store staged contacts here until we do a batch upsert.
        # Each item is a local DB dict (has at least 'id', 'name', optional 'xero_id').
        self.contact_upsert_queue = []
        self.db_ops = DatabaseOperations()  # if that's how you reference DB ops
        self.logger.info("XeroServices initialized.")

    # ─────────────────────────────────────────────────────────────
    #                SPEND MONEY METHODS
    # ─────────────────────────────────────────────────────────────
    def handle_spend_money_create(self, spend_money_id:int):
        self.logger.info(f'handle_spend_money_create => spend_money_id={spend_money_id}')
        sm = self.db_ops.search_spend_money(["id"], [spend_money_id])
        if not sm:
            self.logger.warning("No SpendMoney record found to create in Xero.")
            return
        if isinstance(sm, list):
            sm = sm[0]
        if (sm.get('state') or '').upper() == "RECONCILED":
            self.logger.info("Already RECONCILED => no Xero update needed.")
            return
        # 1) Pull data from SpendMoney record
        detail_item_id = sm.get('detail_item_id')
        if not detail_item_id:
            self.logger.info("SpendMoney row lacks detail_item_id; we’ll do a minimal create.")
            created = self.xero_api.create_spend_money_in_xero(sm)
        else:
            # If you store a reference to detail_item_id, pass that to xero_api
            self.logger.info(f"Creating SPEND money in Xero referencing detail_item_id={detail_item_id}...")
            created = self.xero_api.create_spend_money_via_detail_id(detail_item_id)
        if created and isinstance(created, list):
            created = created[0]
        if created and created.get('xero_spend_money_id'):
            new_xero_spend_money_id = created['xero_spend_money_id']
            self.db_ops.update_spend_money(spend_money_id, xero_spend_money_id=new_xero_spend_money_id)
            self.logger.info(f"Successfully created SPEND transaction in Xero => xero_id={new_xero_spend_money_id}")
        else:
            self.logger.warning("No valid response from Xero after spend money creation.")

    def handle_spend_money_update(self, spend_money_id:int, new_state:str):
        """
        Example method that tries to update an existing Xero SPEND transaction
        to a new status (e.g., 'AUTHORISED', 'VOIDED', or 'RECONCILED').
        """
        self.logger.info(f'handle_spend_money_update => spend_money_id={spend_money_id}, new_state={new_state}')
        sm = self.db_ops.search_spend_money(["id"], [spend_money_id])
        if not sm:
            self.logger.warning("No SpendMoney record found for update.")
            return
        if isinstance(sm, list):
            sm = sm[0]
        existing_xero_id = sm.get('xero_id')
        if not existing_xero_id:
            self.logger.info("This SpendMoney has no xero_id => calling handle_spend_money_create first.")
            self.handle_spend_money_create(spend_money_id)
            return
        # 2) Actually call xero_api to update the status
        updated = self.xero_api.update_spend_transaction_status(existing_xero_id, new_state)
        if updated and isinstance(updated, list):
            updated = updated[0]
        if updated and updated.get('Status'):
            self.logger.info(f"SpendMoney status updated in Xero => {updated['Status']}")
            # Mirror new status in local DB
            self.db_ops.update_spend_money(spend_money_id, state=updated['Status'])
        else:
            self.logger.warning("Could not update SPEND transaction in Xero.")

    # ─────────────────────────────────────────────────────────────
    #                XERO BILLS (CREATE/UPDATE/DELETE)
    # ─────────────────────────────────────────────────────────────
    def create_xero_bill_in_xero(self, xero_bill: dict):
        bill_id = xero_bill["id"]
        self.logger.info(f'[create_xero_bill_in_xero] => BillID={bill_id}')
        if xero_bill.get('xero_id'):
            self.logger.info(f'Already has xero_id={xero_bill["xero_id"]}, skipping creation.')
            return
        reference = xero_bill["xero_reference_number"]
        self.logger.info(f'Checking Xero for existing invoice with InvoiceNumber={reference}...')
        existing_invoices = self.xero_api.get_bills_by_reference(reference)
        self.logger.debug(f'Found existing_invoices={existing_invoices}')
        if existing_invoices:
            existing_xero_id = existing_invoices[0].get('InvoiceID')
            existing_link = f'https://go.xero.com/AccountsPayable/View.aspx?invoiceId={existing_xero_id}'
            self.db_ops.update_xero_bill(bill_id, xero_id=existing_xero_id, xero_link=existing_link)
            self.logger.info("Linked local Bill to existing Xero invoice.")
            return
        po_number = xero_bill.get('po_number')
        project_number = xero_bill.get('project_number')
        purchase_orders = self.db_ops.search_purchase_orders(['project_number','po_number'], [project_number, po_number])
        self.logger.debug(f'purchase_orders={purchase_orders}')
        if not purchase_orders:
            self.logger.warning("No PurchaseOrder for the Bill. Missing contact_id.")
            return
        if isinstance(purchase_orders, list):
            purchase_order = purchase_orders[0]
        else:
            purchase_order = purchase_orders
        contact_id = purchase_order.get('contact_id')
        if not contact_id:
            self.logger.warning("PurchaseOrder has no contact_id => cannot create in Xero.")
            return
        contact_record = self.db_ops.search_contacts(['id'], [contact_id])
        self.logger.debug(f'contact_record={contact_record}')
        if not contact_record:
            self.logger.warning("No Contact found in DB => skipping Xero Bill creation.")
            return
        if isinstance(contact_record, list):
            contact_record = contact_record[0]
        xero_contact_id = contact_record.get('xero_id')
        if not xero_contact_id:
            self.logger.warning("Contact missing xero_id => cannot create Xero Bill.")
            return
        creation_payload = {
            'Type': 'ACCPAY',
            'InvoiceNumber': reference,
            'Contact': {'ContactID': xero_contact_id}
        }
        if xero_bill.get('transaction_date'):
            creation_payload['Date'] = xero_bill['transaction_date']
        if xero_bill.get('due_date'):
            creation_payload['DueDate'] = xero_bill['due_date']
        self.logger.info(f'Sending invoice creation payload to Xero => {creation_payload}')
        result = self.xero_api.create_invoice(creation_payload)
        self.logger.debug(f'create_invoice => {result}')
        if not result:
            self.logger.error("Invoice creation in Xero failed.")
            return
        try:
            new_inv = result[0]
            new_xero_id = new_inv.get('InvoiceID')
            link = f'https://go.xero.com/AccountsPayable/View.aspx?invoiceId={new_xero_id}'
            self.db_ops.update_xero_bill(bill_id, xero_id=new_xero_id, xero_link=link)
            self.logger.info(f'Created new Xero invoice => ID={new_xero_id}')
        except Exception as e:
            self.logger.error(f'Error parsing invoice response => {e}')

    def update_xero_bill(self, bill_id:int):
        self.logger.info(f'[update_xero_bill] => BillID={bill_id}')
        xero_bill = self.db_ops.search_xero_bills(['id'], [bill_id])
        self.logger.debug(f'xero_bill={xero_bill}')
        if not xero_bill:
            self.logger.warning("No XeroBill found => skipping.")
            return
        if isinstance(xero_bill, list):
            xero_bill = xero_bill[0]
        if not xero_bill.get('xero_id'):
            self.logger.info("No xero_id => calling create_xero_bill_in_xero.")
            self.create_xero_bill_in_xero(xero_bill)
            self.logger.info("Done creating new xero bill => returning.")
            project_number = xero_bill.get('project_number')
            po_number = xero_bill.get('po_number')
            detail_number = xero_bill.get('detail_number')
            if not(project_number and po_number and detail_number):
                self.logger.warning("Missing keys => cannot update detail items.")
                return
            detail_items = self.db_ops.search_detail_item_by_keys(project_number, po_number, detail_number)
            self.logger.debug(f'Found detail_items => {detail_items}')
            if isinstance(detail_items, dict):
                detail_items = [detail_items]
            if detail_items:
                for di in detail_items:
                    if not di.get('parent_xero_id'):
                        xero_bill_line_items = self.db_ops.search_xero_bill_line_items(
                            ['description','parent_id','xero_bill_line_id'],
                            [di['description'],bill_id,None]
                        )
                        if xero_bill_line_items:
                            if isinstance(xero_bill_line_items, list):
                                xero_bill_line_item = xero_bill_line_items[0]
                            else:
                                xero_bill_line_item = xero_bill_line_items
                            xero_line_id = xero_bill_line_item.get('xero_bill_line_id')
                            if xero_line_id:
                                self.logger.info(f'Updating DetailItem={di["id"]} => parent_xero_id={xero_line_id}')
                                self.db_ops.update_detail_item(di['id'], parent_xero_id=xero_line_id)
                        else:
                            self.logger.warning(f'No XeroBillLineItem found matching detail_item={di["id"]}')
            return
        changes = {}
        self.logger.debug(f'Potential changes => {changes}')
        if not changes:
            self.logger.info("No changes to push => done.")
            return
        self.logger.info(f'Pushing changes => {changes}')
        updated = self.xero_api.update_invoice(xero_bill['xero_id'], changes)
        self.logger.debug(f'update_invoice => {updated}')
        if updated:
            self.logger.info("Updated invoice in Xero.")
        else:
            self.logger.warning("Failed to update in Xero.")

    def delete_xero_bill(self, bill_id:int):
        self.logger.info(f'[delete_xero_bill] => BillID={bill_id}')
        xero_bill = self.db_ops.search_xero_bills(['id'], [bill_id])
        self.logger.debug(f'Fetched xero_bill => {xero_bill}')
        if xero_bill:
            if isinstance(xero_bill, list):
                xero_bill = xero_bill[0]
            xero_id = xero_bill.get('xero_id')
        else:
            self.logger.warning("No local record => skipping.")
            return
        if not xero_id:
            self.logger.warning("Bill has no xero_id => cannot delete in Xero.")
            return
        self.logger.info(f'Setting invoice {xero_id} => DELETED in Xero.')
        delete_resp = self.xero_api.delete_invoice(xero_id)
        self.logger.debug(f'delete_invoice => {delete_resp}')
        if delete_resp:
            self.logger.info("Invoice set to DELETED in Xero.")
        else:
            self.logger.warning("Could not set to DELETED.")

    # ─────────────────────────────────────────────────────────────
    #                  SPEND MONEY LOADING
    # ─────────────────────────────────────────────────────────────
    def load_spend_money_transactions(self, project_id:int=None, po_number:int=None, detail_number:int=None):
        self.logger.info(f'load_spend_money_transactions => project={project_id}, po={po_number}, detail={detail_number}')
        self.logger.info('Retrieving SPEND transactions from Xero...')
        xero_spend_transactions = self.xero_api.get_spend_money_by_reference(
            project_id=project_id,
            po_number=po_number,
            detail_number=detail_number
        )
        self.logger.debug(f'get_spend_money_by_reference => {xero_spend_transactions}')
        if not xero_spend_transactions:
            self.logger.info('No SPEND transactions returned from Xero.')
            return
        for tx in xero_spend_transactions:
            current_state = 'RECONCILED' if tx.get('IsReconciled',False) else tx.get('Status','DRAFT')
            reference_number = tx.get('InvoiceNumber')
            bank_transaction_id = tx.get('BankTransactionID')
            xero_link = f'https://go.xero.com/Bank/ViewTransaction.aspx?bankTransactionID={bank_transaction_id}'
            existing_spend = self.db_ops.search_spend_money(['xero_spend_money_reference_number'], [reference_number])
            self.logger.debug(f'existing_spend => {existing_spend}')
            # ... logic to create or update local spend record

    # ─────────────────────────────────────────────────────────────
    #             POPULATE LOCAL CONTACTS WITH XERO IDS
    # ─────────────────────────────────────────────────────────────
    def populate_xero_contacts(self):
        self.logger.info('populate_xero_contacts => retrieving local DB and Xero contacts...')
        db_contacts = self.db_ops.search_contacts()
        self.logger.info(f'Found {len(db_contacts)} contacts locally.')
        self.logger.info('Retrieving all contacts from Xero...')
        try:
            all_xero_contacts = self.xero_api.get_all_contacts()
            self.logger.debug(f'get_all_contacts => {len(all_xero_contacts)} contacts')
        except Exception as xe:
            self.logger.error(f'Failed to retrieve contacts from Xero => {xe}')
            return
        xero_contacts_dict = {
            c['Name'].strip().lower(): c for c in all_xero_contacts if c.get('Name')
        }
        for db_contact in db_contacts:
            contact_name = db_contact.get('name','')
            if not contact_name:
                continue
            xero_match = xero_contacts_dict.get(contact_name.strip().lower())
            if xero_match:
                xero_id = xero_match['ContactID']
                self.db_ops.update_contact(db_contact['id'], xero_id=xero_id)
                self.logger.info(f"Linked local contact '{contact_name}' => XeroID={xero_id}")

    # ─────────────────────────────────────────────────────────────
    #                CONTACT VALIDATION
    # ─────────────────────────────────────────────────────────────
    def validate_xero_data(self, db_contact):
        self.logger.debug(f'Validating db_contact => {db_contact["name"]}')
        errors = []

        # 1) Validate Name (still considered "required" in your system)
        name = db_contact.get('name', '').strip()
        if not name:
            errors.append('❗ Missing or empty name.')

        # 2) Validate Address Line 1 length
        address_line_1 = db_contact.get('address_line_1', '')
        if address_line_1 and len(address_line_1) > 255:
            errors.append('❗ address_line_1 exceeds char limit.')

        # 3) Validate Email format
        email = db_contact.get('email', '')
        if email and '@' not in email:
            errors.append('❗ Invalid email format.')

        # 4) Validate Phone length
        phone = db_contact.get('phone', '')
        if phone and len(phone) > 50:
            errors.append('❗ Phone number exceeds character limit.')

        # 5) Validate xero_id as a proper GUID
        xero_id = (db_contact.get('xero_id') or '').strip()
        if xero_id:
            guid_no_dashes = xero_id.replace('-', '')
            if len(guid_no_dashes) == 32 and re.match(r'^[0-9A-Fa-f]{32}$', guid_no_dashes):
                # If original xero_id was missing dashes, auto-correct
                if '-' not in xero_id:
                    corrected = (
                        guid_no_dashes[0:8] + '-' +
                        guid_no_dashes[8:12] + '-' +
                        guid_no_dashes[12:16] + '-' +
                        guid_no_dashes[16:20] + '-' +
                        guid_no_dashes[20:]
                    )
                    db_contact['xero_id'] = corrected
                    self.logger.warning(
                        f"Auto-corrected XeroID from '{xero_id}' to '{corrected}' (missing dashes)."
                    )
            else:
                errors.append(
                    f"❗ Xero ID '{xero_id}' is invalid; must be 32 hex digits "
                    "with 4 dashes (xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx)."
                )

        return errors

    # ─────────────────────────────────────────────────────────────
    #            BUFFER + EXECUTE CONTACT UPSERTS
    # ─────────────────────────────────────────────────────────────
    def buffered_upsert_contact(self, contact_record: dict):
        """
        Stage a single local 'contact_record' for eventual batch upsert in Xero.
        We'll do the actual creation/update in 'execute_batch_upsert_contacts'
        to minimize repeated calls.
        """
        self.logger.info("🌀 [START] Attempting to stage a contact for batch upsert in Xero.")
        contact_val_errors = self.validate_xero_data(contact_record)
        if contact_val_errors:
            self.logger.warning("🌀 Contact errors. Cannot enqueue.")
            self.logger.warning(f"🌀 {contact_val_errors}")
            self.logger.info("🌀 [COMPLETED] [STATUS=Fail]")
        else:
            self.logger.debug(f"🫸 - {contact_record['name']} with Xero ID - {contact_record['xero_id']}")
            self.contact_upsert_queue.append(contact_record)
            self.logger.debug(
                f"🌀 Current queue size => {len(self.contact_upsert_queue)}. Added contact => {contact_record}"
            )
            self.logger.info("🌀 [COMPLETED] [STATUS=Success] Staged contact for upsert.")

    def execute_batch_upsert_contacts(self, contacts: list[dict], chunk_size: int = 50) -> None:
        """
        Executes a batched 'upsert' (create or update) of contacts in Xero.
        - Splits into create vs. update lists
        - Optionally processes in chunks if lists are large
        - Logs success/failure counts
        """
        self.logger.info("🌀 [START] Performing batched Xero contact upserts...")

        total_contacts = len(contacts)
        if total_contacts == 0:
            self.logger.info("🌀 No contacts provided => nothing to process.")
            self.logger.info("🌀 [COMPLETED] [STATUS=Success] No contacts upserted.")
            return

        # Separate into create vs. update
        create_list = []
        update_list = []
        for c in contacts:
            try:
                if c.get("xero_id"):
                    update_list.append(c)
                else:
                    create_list.append(c)
            except Exception as e:
                self.logger.error(f"⛔ Error sorting contact => {c}, Error: {e}")

        self.logger.info(
            f"🌀 Split {total_contacts} staged contacts => create_list={len(create_list)}, update_list={len(update_list)}."
        )

        success_count = 0
        fail_count = 0

        # CREATE
        if create_list:
            for i in range(0, len(create_list), chunk_size):
                subset = create_list[i : i + chunk_size]
                chunk_success = self.process_chunk("create", subset)
                success_count += chunk_success
                fail_count += (len(subset) - chunk_success)
        else:
            self.logger.info("🌀 No contacts to create in Xero.")

        # UPDATE
        if update_list:
            for i in range(0, len(update_list), chunk_size):
                subset = update_list[i : i + chunk_size]
                chunk_success = self.process_chunk("update", subset)
                success_count += chunk_success
                fail_count += (len(subset) - chunk_success)
        else:
            self.logger.info("🌀 No contacts to update in Xero.")

        # Summary
        self.logger.info(f"🌀 Upsert summary => success={success_count}, fails={fail_count}, total={total_contacts}")

        status_str = "Success"
        if fail_count == total_contacts:
            status_str = "Fail"
        elif fail_count > 0:
            status_str = "PartialFail"

        self.logger.info(
            f"🌀 [COMPLETED] [STATUS={status_str}] Done with batched contact upserts."
        )

    # ─────────────────────────────────────────────────────────────
    #       Convert DB Contact -> Xero Contact (Partial Logic)
    # ─────────────────────────────────────────────────────────────
    def _convert_contact_to_xero_schema(self, db_contact: dict) -> dict:
        """
        For new contacts (no xero_id): We supply a non-empty Name + a unique AccountNumber
        For existing contacts (has xero_id): We omit Name + AccountNumber so we don't collide or rename.
        """
        xero_contact = {}

        # Pull existing or missing Xero ID
        x_id = (db_contact.get("xero_id") or "").strip()
        if x_id:
            xero_contact["ContactID"] = x_id

        # If no xero_id => This is a brand-new contact => must send Name (non-empty).
        # Also ensure AccountNumber is unique if used.
        if not x_id:
            raw_name = (db_contact.get("name") or "").strip()
            if not raw_name:
                raw_name = "Unnamed Contact"
            xero_contact["Name"] = raw_name

            # If you still want to store something in AccountNumber, ensure it's unique
            # so "Account number already exists" doesn't blow up your batch.
            vendor_status = (db_contact.get("vendor_status") or "").strip()
            vendor_type = (db_contact.get("vendor_type") or "").strip()
            local_id = str(db_contact.get("id") or "")  # or "pulse_id"
            if vendor_status or vendor_type or local_id:
                # e.g. "VENDOR-PENDING-4611" => ensures uniqueness
                xero_contact["AccountNumber"] = f"{vendor_type}-{vendor_status}-{local_id}"

        # If xero_id is present => partial update => omit "Name" & "AccountNumber"
        # So we avoid duplicate name or "Account number already exists."

        # Fields that are safe to update whether new or existing:
        email = (db_contact.get("email") or "").strip()
        if email:
            xero_contact["EmailAddress"] = email

        phone = (db_contact.get("phone") or "").strip()
        if phone:
            xero_contact["Phones"] = [
                {"PhoneType": "DEFAULT", "PhoneNumber": phone}
            ]

        # Minimal "Addresses" structure
        xero_contact["Addresses"] = [{"AddressType": "STREET"}]
        if db_contact.get("address_line_1"):
            xero_contact["Addresses"][0]["AddressLine1"] = db_contact["address_line_1"].strip()
        if db_contact.get("address_line_2"):
            xero_contact["Addresses"][0]["AddressLine2"] = db_contact["address_line_2"].strip()
        if db_contact.get("city"):
            xero_contact["Addresses"][0]["City"] = db_contact["city"].strip()
        if db_contact.get("region"):
            xero_contact["Addresses"][0]["Region"] = db_contact["region"].strip()
        if db_contact.get("zip"):
            xero_contact["Addresses"][0]["PostalCode"] = db_contact["zip"].strip()
        if db_contact.get("country"):
            xero_contact["Addresses"][0]["Country"] = db_contact["country"].strip()

        # If needed, store a tax_number
        tax_num = (db_contact.get("tax_number") or "").strip()
        if tax_num:
            xero_contact["TaxNumber"] = tax_num

        return xero_contact

    def process_chunk(self, mode: str, data_chunk: list[dict]) -> int:
        """
        mode='create' => xero.contacts.put(...) (requires Name, unique AccountNumber)
        mode='update' => xero.contacts.save(...) partial update, skipping Name + AccountNumber
        """
        # Transform each contact from your DB format -> Xero format
        xero_contacts = [self._convert_contact_to_xero_schema(c) for c in data_chunk]

        chunk_success = 0
        self.logger.info(
            f"🌀 Sending {len(xero_contacts)} contacts to Xero in one '{mode}' batch call..."
        )

        try:
            if mode == "create":
                result = self.xero_api._retry_on_unauthorized(
                    self.xero_api.xero.contacts.put,
                    xero_contacts
                )
            else:  # mode == 'update'
                result = self.xero_api._retry_on_unauthorized(
                    self.xero_api.xero.contacts.save,
                    xero_contacts
                )

            if result:
                chunk_success = len(result)
                self.logger.info(
                    f"🌀 Successfully completed '{mode}' => {chunk_success} upserted."
                )
        except Exception as e:
            self.logger.error(f"⛔ Exception during '{mode}' batch => {e}")

        return chunk_success

# Singleton instance
xero_services = XeroServices()