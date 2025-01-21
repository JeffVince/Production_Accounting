import logging
from datetime import datetime
from typing import Optional

from xero.exceptions import XeroException

from database.database_util import DatabaseOperations
from files_xero.xero_api import xero_api  # Adjust import path as needed
from utilities.singleton import SingletonMeta

class XeroServices(metaclass=SingletonMeta):
    """
    Orchestrator: does the 'heavy lifting' DB <-> Xero:
      - create / update / delete Xero bills
      - fetch purchase orders / contacts from DB
      - build Xero payload
      - handle local DB updates (like storing xero_id)
    """

    def __init__(self):
        self.logger = logging.getLogger('xero_logger')
        self.logger.setLevel(logging.DEBUG)
        self.db_ops = DatabaseOperations()
        self.xero_api = xero_api  # Singleton from xero_api

    def _format_date(self, dt: datetime) -> str:
        """Utility to format Python datetime to 'YYYY-MM-DD' for Xero."""
        return dt.strftime('%Y-%m-%d')

    # --------------------------------------------------------------------------
    # New function: create_bill_line_item_in_xero
    # --------------------------------------------------------------------------
    def create_bill_line_item_in_xero(self, bill_line_item_id: int):
        """
        Called when a new Xero_Bill_Line_Item is created locally.
         1) Find the local BillLineItem (by bill_line_item_id).
         2) Identify the parent Bill's Xero ID (the ACCPAY Invoice ID in Xero).
         3) If it doesn't already exist in Xero, we can't add the line item.
         4) Build a line-item payload using the tax code, description, line amount, etc.
         5) Call add_line_item_to_invoice(...) on the Xero side.
         6) From the returned invoice data, find the newly added line item and store
            its Xero LineItemID in the local DB (xero_bill_line_id).
        """

        self.logger.info(f'START => line_item_id={bill_line_item_id}')

        # 1) Fetch the local line item
        line_item = self.db_ops.search_bill_line_items(['id'], [bill_line_item_id])
        if not line_item:
            self.logger.warning(f'No local BillLineItem found (ID={bill_line_item_id}).')
            return
        if isinstance(line_item, list):
            line_item = line_item[0]

        parent_id = line_item.get('parent_id')
        if not parent_id:
            self.logger.warning(f'BillLineItem={bill_line_item_id} has no parent_id.')
            return

        # 2) Identify the parent Bill => check if parent has a Xero ID
        parent_bill = self.db_ops.search_xero_bills(['id'], [parent_id])
        if not parent_bill:
            self.logger.warning(f'No parent Bill found (ID={parent_id}).')
            return
        if isinstance(parent_bill, list):
            parent_bill = parent_bill[0]

        xero_invoice_id = parent_bill.get('xero_id')
        if not xero_invoice_id:
            self.logger.warning(f'Parent Bill={parent_id} has no xero_id. Cannot add line item in Xero.')
            return

        # 3) (Optional) Confirm it doesn't already exist in Xero by checking line_item['xero_bill_line_id']
        existing_xero_line_id = line_item.get('xero_bill_line_id')
        if existing_xero_line_id:
            self.logger.info(
                f'BillLineItem={bill_line_item_id} already has xero_bill_line_id={existing_xero_line_id}. '
                'Skipping creation.'
            )
            return

        # 4) Build the payload to send to Xero
        description = line_item.get('description', '')
        line_amount = line_item.get('line_amount', 0)
        # If you store quantity, you can specify it. Or default to 1:
        quantity = line_item.get('quantity', 1)
        # If you store an account code or tax code, pass them along:
        tax_code = line_item.get('tax_code')  # e.g. "TAX001" or "NONE"
        # For Xero's line item, typically we must provide UnitAmount, Quantity, and TaxType or AccountCode
        # If 'line_amount' is the total, you can do some logic to figure out UnitAmount.
        # For simplicity, if your 'line_amount' is the total = UnitAmount * Quantity, then:
        #    UnitAmount = line_amount / quantity  (assuming quantity != 0).
        # The user specifically said: "we use tax code, description and line amount" -- so let's do:

        # Guard for zero quantity:
        if quantity == 0:
            quantity = 1
        unit_amount = float(line_amount) / float(quantity)

        line_item_payload = {
            'Description': description,
            'Quantity': quantity,
            'UnitAmount': unit_amount
        }
        # If you want to specify the tax code:
        if tax_code:
            line_item_payload['TaxType'] = tax_code
        # or if you need "AccountCode":
        if line_item.get('account_code'):
            line_item_payload['AccountCode'] = str(line_item['account_code'])

        self.logger.info(
            f'Adding line item to invoice={xero_invoice_id}, payload={line_item_payload}'
        )

        # 5) Add line item in Xero
        try:
            updated_invoice = self.xero_api.add_line_item_to_invoice(
                invoice_id=xero_invoice_id,
                line_item_data=line_item_payload
            )
            if not updated_invoice:
                self.logger.warning(
                    '[create_bill_line_item_in_xero] No updated invoice data returned from Xero.'
                )
                return

            # 6) Find the newly added line item in the returned invoice
            new_line_id = None
            line_items = updated_invoice.get('LineItems', [])
            # We can guess the newly added one is the last in the list, or search for an exact match:
            # This approach looks for a line item matching 'Description', 'Quantity', and 'UnitAmount'
            # so that we can handle duplicates carefully if needed.
            for li in reversed(line_items):  # check from the end
                if (li.get('Description') == description and
                    float(li.get('Quantity') or 0) == float(quantity) and
                    float(li.get('UnitAmount') or 0) == float(unit_amount)):
                    new_line_id = li.get('LineItemID')
                    break

            if new_line_id:
                self.logger.info(
                    f'Found newly created Xero LineItemID={new_line_id} for BillLineItem={bill_line_item_id}. '
                    'Updating DB...'
                )
                # Update local DB
                self.db_ops.update_bill_line_item(
                    bill_line_item_id,
                    xero_bill_line_id=new_line_id
                )
            else:
                self.logger.warning(
                    '[create_bill_line_item_in_xero] Could not find newly added line item among returned LineItems.'
                )

        except XeroException as xe:
            self.logger.error(
                f'XeroException while creating line item in Xero: {xe}'
            )

    # --------------------------------------------------------------------------
    #             CREATE Xero Bill in Xero if Xero ID is missing
    # --------------------------------------------------------------------------
    def create_xero_bill_in_xero(self, xero_bill):
        """
        1) Look up the local XeroBill record in DB.
        2) If missing xero_id => attempt to create in Xero.
        3) Build payload with contact from PurchaseOrder, plus date/dueDate if present.
        4) Update local DB with new xero_id & link.
        """

        bill_id = xero_bill["id"]

        self.logger.info(f'[BillID={bill_id}] 🏁 START function.')

        self.logger.debug(f'[BillID={bill_id}] 🔎 Fetched local xero_bill={xero_bill}')



        if xero_bill.get('xero_id'):
            self.logger.info(
                f'[BillID={bill_id}] 🚫 Already has xero_id={xero_bill["xero_id"]}. Skipping creation.'
            )
            self.logger.info(f'[BillID={bill_id}] 🏁 END function (bill already linked).')
            return

        reference = xero_bill["xero_reference_number"]
        self.logger.info(
            f'[BillID={bill_id}] 🔎 Checking Xero for existing invoices with InvoiceNumber={reference} ...'
        )

        # --- [Duplicate Check] ---
        existing_invoices = self.xero_api.get_bills_by_reference(reference)
        self.logger.debug(
            f'[BillID={bill_id}] 🧐 "existing_invoices" result = {existing_invoices}'
        )

        if existing_invoices:
            self.logger.info(
                f'[BillID={bill_id}] ⚠️ FOUND an existing Xero invoice with InvoiceNumber={reference}. '
                'Linking local bill to this existing invoice and skipping creation.'
            )
            existing_xero_id = existing_invoices[0].get('InvoiceID')
            existing_link = f'https://go.xero.com/AccountsPayable/View.aspx?invoiceId={existing_xero_id}'
            self.logger.debug(
                f'[BillID={bill_id}] Existing xero_id={existing_xero_id}, link={existing_link}.'
            )
            self.db_ops.update_xero_bill(bill_id, xero_id=existing_xero_id, xero_link=existing_link)
            self.logger.info(f'[BillID={bill_id}] 🏁 END function (linked to existing).')
            return
        else:
            self.logger.info(
                f'[BillID={bill_id}] ✅ No existing Xero invoice found with that reference. Proceeding with creation.'
            )
        # --- [End Duplicate Check] ---

        # Grab the associated PO => contact
        po_number = xero_bill.get('po_number')
        project_number = xero_bill.get('project_number')
        purchase_orders = self.db_ops.search_purchase_orders(['project_number','po_number'], [project_number, po_number])
        self.logger.debug(
            f'[BillID={bill_id}] 🔎 "purchase_orders" result = {purchase_orders}'
        )

        if not purchase_orders:
            self.logger.warning(f'[BillID={bill_id}] ⚠️ - No PurchaseOrder for (proj={project_number}, po={po_number}).')
            self.logger.info(f'[BillID={bill_id}] 🏁 END function (missing PO).')
            return

        if isinstance(purchase_orders, list):
            purchase_order = purchase_orders[0]
        else:
            purchase_order = purchase_orders

        contact_id = purchase_order.get('contact_id')
        if not contact_id:
            self.logger.warning(f'[BillID={bill_id}] ⚠️ - PurchaseOrder has no contact_id.')
            self.logger.info(f'[BillID={bill_id}] 🏁 END function (missing contact_id).')
            return

        contact_record = self.db_ops.search_contacts(['id'], [contact_id])
        self.logger.debug(
            f'[BillID={bill_id}] 🔎 "contact_record" result = {contact_record}'
        )

        if not contact_record:
            self.logger.warning(f'[BillID={bill_id}] ⚠️ - No Contact found with id={contact_id}.')
            self.logger.info(f'[BillID={bill_id}] 🏁 END function (no contact).')
            return
        if isinstance(contact_record, list):
            contact_record = contact_record[0]

        xero_contact_id = contact_record.get('xero_id')
        if not xero_contact_id:
            self.logger.warning(f'[BillID={bill_id}] ⚠️ - Contact has no xero_id.')
            self.logger.info(f'[BillID={bill_id}] 🏁 END function (missing xero_contact_id).')
            return

        # Build invoice creation payload
        creation_payload = {
            'Type': 'ACCPAY',
            'InvoiceNumber': reference,
            'Contact': {'ContactID': xero_contact_id}
        }
        if xero_bill.get('transaction_date'):
            creation_payload['Date'] = (xero_bill['transaction_date'])
        if xero_bill.get('due_date'):
            creation_payload['DueDate'] = (xero_bill['due_date'])

        self.logger.info(
            f'[BillID={bill_id}] 📄 - Sending invoice creation payload to Xero: {creation_payload}'
        )
        result = self.xero_api.create_invoice(creation_payload)
        self.logger.debug(
            f'[BillID={bill_id}] 🧐 "create_invoice" result = {result}'
        )

        if not result:
            self.logger.error(
                f'[BillID={bill_id}] ❌ - Invoice creation in Xero failed.'
            )
            self.logger.info(f'[BillID={bill_id}] 🏁 END function (creation failed).')
            return

        try:
            new_inv = result[0]
            new_xero_id = new_inv.get('InvoiceID')
            link = f'https://go.xero.com/AccountsPayable/View.aspx?invoiceId={new_xero_id}'
            self.db_ops.update_xero_bill(bill_id, xero_id=new_xero_id, xero_link=link)
            self.logger.info(
                f'[BillID={bill_id}] ✅ - Created new Xero invoice with ID={new_xero_id}.'
            )
        except Exception as e:
            self.logger.error(
                f'[BillID={bill_id}] ❌ - Error parsing invoice response: {e}'
            )

        self.logger.info(f'[BillID={bill_id}] 🏁 END function (success).')

    # --------------------------------------------------------------------------
    #             UPDATE Xero Bill
    # --------------------------------------------------------------------------
    def update_xero_bill(self, bill_id: int):
        """
        1) Check local XeroBill for xero_id; if none, call create_xero_bill.
        2) If final => maybe skip or pull data. Otherwise, upsert changes, etc.
        (You can adapt based on your own logic.)
        """
        self.logger.info(f'[BillID={bill_id}] 🏁 START function.')
        xero_bill = self.db_ops.search_xero_bills(['id'], [bill_id])
        self.logger.debug(f'[BillID={bill_id}] 🔎 Fetched xero_bill={xero_bill}')

        if not xero_bill:
            self.logger.warning(
                f'[BillID={bill_id}] ⚠️ - No XeroBill found. Aborting update.'
            )
            self.logger.info(f'[BillID={bill_id}] 🏁 END function (no local bill).')
            return
        if isinstance(xero_bill, list):
            xero_bill = xero_bill[0]

        # If no xero_id, let's create it first
        if not xero_bill.get('xero_id'):
            self.logger.info(
                f'[BillID={bill_id}] 🤔 No xero_id => calling create_xero_bill_in_xero.'
            )
            self.create_xero_bill_in_xero(xero_bill)
            self.logger.info(f'[BillID={bill_id}] 🏁 END function (created new xero bill).')
            
            #region update detail items
            project_number = xero_bill.get('project_number')
            po_number = xero_bill.get('po_number')
            detail_number = xero_bill.get('detail_number')

            if not (project_number and po_number and detail_number):
                self.logger.warning(
                    f'[BillID={bill_id}] Missing project_number, po_number, or detail_number. Cannot update detail items.'
                )
                self.logger.info(
                    f'[BillID={bill_id}] 🏁 END function (missing fields for detail items).')
                return

            # Fetch all detail items associated with this bill
            detail_items = self.db_ops.search_detail_item_by_keys(
                project_number=project_number,
                po_number=po_number,
                detail_number=detail_number
            )
            self.logger.debug(f'Found {(detail_items)} detail items for BillID={bill_id}.')

            for detail_item in detail_items:
                if not detail_item.get('parent_xero_id'):
                    # Find corresponding BillLineItem based on description and line amount
                    bill_line_items = self.db_ops.search_bill_line_items(
                        column_names=['description', 'parent_id', 'xero_bill_line_id'],
                        values=[detail_item['description'], bill_id, None]
                    )

                    if bill_line_items:
                        if isinstance(bill_line_items, list):
                            bill_line_item = bill_line_items[0]
                        else:
                            bill_line_item = bill_line_items
                        xero_line_id = bill_line_item.get('xero_bill_line_id')

                        if xero_line_id:
                            self.logger.info(
                                f'Updating DetailItem={detail_item["id"]} with parent_xero_id={xero_line_id}.'
                            )
                            self.db_ops.update_detail_item(
                                detail_item['id'],
                                parent_xero_id=xero_line_id
                            )
                        else:
                            self.logger.warning(
                                f'BillLineItem={bill_line_item["id"]} has no xero_bill_line_id. Cannot update DetailItem={detail_item["id"]}.'
                            )
                    else:
                        self.logger.warning(
                            f'No BillLineItem found matching DetailItem={detail_item["id"]}.'
                        )
            #endregion

            self.logger.info(f'[BillID={bill_id}] 🏁 END function (created new xero bill).')
            return

        # If we do want to actually "update" the invoice in Xero, we can do so by:
        changes = {}
        # e.g. changes['DueDate'] = '2025-05-01'
        # or changes['Status'] = 'AUTHORISED'

        self.logger.debug(
            f'[BillID={bill_id}] Potential changes to push => {changes}'
        )

        if not changes:
            self.logger.info(f'[BillID={bill_id}] No changes to push. Done.')
            self.logger.info(f'[BillID={bill_id}] 🏁 END function (nothing updated).')
            return

        self.logger.info(
            f'[BillID={bill_id}] 📤 Pushing changes to Xero: {changes}'
        )
        updated = self.xero_api.update_invoice(xero_bill['xero_id'], changes)
        self.logger.debug(
            f'[BillID={bill_id}] 🧐 "update_invoice" result = {updated}'
        )

        if updated:
            self.logger.info(f'[BillID={bill_id}] ✅ - Updated invoice in Xero.')
        else:
            self.logger.warning(f'[BillID={bill_id}] ⚠️ - Failed to update in Xero.')

        self.logger.info(f'[BillID={bill_id}] 🏁 END function.')

    # --------------------------------------------------------------------------
    #             DELETE Xero Bill
    # --------------------------------------------------------------------------
    def delete_xero_bill(self, bill_id: int):
        """
        1) If XeroBill was deleted in DB, we set the Xero invoice status=DELETED if possible.
        2) Only possible if the invoice is in DRAFT or SUBMITTED status in Xero.
        """
        self.logger.info(f'[BillID={bill_id}] 🏁 START function.')
        # We can still fetch the local record (it might be "soft" deleted or just removed).
        xero_bill = self.db_ops.search_xero_bills(['id'], [bill_id])
        self.logger.debug(f'[BillID={bill_id}] 🔎 Fetched xero_bill={xero_bill}')

        if xero_bill:
            if isinstance(xero_bill, list):
                xero_bill = xero_bill[0]
            xero_id = xero_bill.get('xero_id')
        else:
            self.logger.warning(
                f'[BillID={bill_id}] ⚠️ - No local record. No xero_id known.'
            )
            self.logger.info(f'[BillID={bill_id}] 🏁 END function (missing local record).')
            return

        if not xero_id:
            self.logger.warning(
                f'[BillID={bill_id}] ⚠️ - Bill has no xero_id. Cannot delete in Xero.'
            )
            self.logger.info(f'[BillID={bill_id}] 🏁 END function (missing xero_id).')
            return

        self.logger.info(
            f'[BillID={bill_id}] ❌ - Setting invoice {xero_id} to DELETED in Xero.'
        )
        delete_resp = self.xero_api.delete_invoice(xero_id)
        self.logger.debug(
            f'[BillID={bill_id}] 🧐 "delete_invoice" result = {delete_resp}'
        )

        if delete_resp:
            self.logger.info(
                f'[BillID={bill_id}] ✅ - Invoice set to DELETED in Xero.'
            )
        else:
            self.logger.warning(
                f'[BillID={bill_id}] ⚠️ - Could not set to DELETED.'
            )

        self.logger.info(f'[BillID={bill_id}] 🏁 END function.')

    # OLDER
    def load_spend_money_transactions(self, project_id: int=None, po_number: int=None, detail_number: int=None):
        """
        Loads Xero SPEND transactions (bankTransactions) into the 'spend_money' table.
        """
        self.logger.info(f'[Proj={project_id}, PO={po_number}, Detail={detail_number}] 🏁 START function.')
        self.logger.info('- Retrieving SPEND transactions from Xero...')
        xero_spend_transactions = self.xero_api.get_spend_money_by_reference(
            project_id=project_id,
            po_number=po_number,
            detail_number=detail_number
        )
        self.logger.debug(
            f'[Proj={project_id}, PO={po_number}, Detail={detail_number}] 🧐 "get_spend_money_by_reference" returned {len(xero_spend_transactions) if xero_spend_transactions else 0} transactions'
        )

        if not xero_spend_transactions:
            self.logger.info('- No SPEND transactions returned from Xero for the provided filters.')
            self.logger.info(f'[Proj={project_id}, PO={po_number}, Detail={detail_number}] 🏁 END function (no data).')
            return

        for tx in xero_spend_transactions:
            self.logger.debug(f'- Processing transaction => {tx}')
            if tx.get('IsReconciled', False) is True:
                current_state = 'RECONCILED'
            else:
                current_state = tx.get('Status', 'DRAFT')

            reference_number = tx.get('InvoiceNumber')
            bank_transaction_id = tx.get('BankTransactionID')
            xero_link = f'https://go.xero.com/Bank/ViewTransaction.aspx?bankTransactionID={bank_transaction_id}'

            existing_spend = self.database_util.search_spend_money(
                column_names=['xero_spend_money_reference_number'],
                values=[reference_number]
            )
            self.logger.debug(f'- existing_spend query => {existing_spend}')

            if not existing_spend:
                created = self.database_util.create_spend_money(
                    xero_spend_money_reference_number=reference_number,
                    xero_link=xero_link,
                    state=current_state
                )
                if created:
                    self.logger.info(
                        f"- Created new SpendMoney record for reference={reference_number}, ID={created['id']}."
                    )
                else:
                    self.logger.error(
                        f'- Failed to create SpendMoney for reference={reference_number}.'
                    )
            else:
                if isinstance(existing_spend, list):
                    existing_spend = existing_spend[0]
                spend_money_id = existing_spend['id']
                updated = self.database_util.update_spend_money(
                    spend_money_id,
                    state=current_state,
                    xero_link=xero_link
                )
                if updated:
                    self.logger.info(
                        f'- Updated SpendMoney (id={spend_money_id}) for reference={reference_number}.'
                    )
                else:
                    self.logger.error(
                        f'- Failed to update SpendMoney (id={spend_money_id}) for reference={reference_number}.'
                    )

        self.logger.info(f'[Proj={project_id}, PO={po_number}, Detail={detail_number}] 🏁 END function.')

    def populate_xero_contacts(self):
        """
        Retrieve all contacts from the local DB, retrieve all contacts from Xero,
        compare them, and then perform a single batch update (only for those that need changes).
        """
        self.logger.info('🏁 START function.')
        db_contacts = self.database_util.search_contacts()
        self.logger.info(f'- Found {len(db_contacts)} contacts in the local DB to process.')
        self.logger.info('- Retrieving all contacts from Xero...')

        try:
            all_xero_contacts = self.xero_api.get_all_contacts()
            self.logger.debug(f'🧐 "get_all_contacts" returned {len(all_xero_contacts)} contacts')
        except XeroException as xe:
            self.logger.error(f'- Failed to retrieve contacts from Xero: {xe}')
            self.logger.info('🏁 END function (XeroException).')
            return

        xero_contacts_dict = {
            contact['Name'].strip().lower(): contact
            for contact in all_xero_contacts
            if isinstance(contact.get('Name'), str) and contact.get('Name').strip()
        }

        contacts_to_update = []
        for db_contact in db_contacts:
            errors = self.validate_xero_data(db_contact)
            if errors:
                self.logger.error(
                    f"- Skipping contact '{db_contact.get('name', 'Unnamed')}' "
                    f"due to validation errors: {errors}"
                )
                continue

            contact_name = db_contact['name']
            self.logger.info(
                f"- 🔎 Checking if there's a matching Xero contact for '{contact_name}'"
            )
            xero_match = xero_contacts_dict.get(contact_name.strip().lower())

            if not xero_match:
                msg = f"No matching Xero contact found for: '{contact_name}' ❌"
                self.logger.warning('- ' + msg)
                continue

            xero_id = xero_match['ContactID']

            # --- Store the Xero ContactID in the local 'xero_id' column ---
            self.logger.debug(
                f"- Linking local DB contact '{contact_name}' to XeroID={xero_id}."
            )
            self.database_util.update_contact(contact_id=db_contact["id"], xero_id=xero_id)

            xero_tax_number = xero_match.get('TaxNumber', '') or ''
            xero_addresses = xero_match.get('Addresses', [])
            xero_email = xero_match.get('EmailAddress') or ''

            tax_number = str(db_contact.get('tax_number')) if db_contact.get('tax_number') else ''
            email = db_contact['email']

            if tax_number and len(tax_number) == 9 and tax_number.isdigit():
                formatted_ssn = f'{tax_number[0:3]}-{tax_number[3:5]}-{tax_number[5:]}'
                self.logger.debug(
                    f"- Formatting SSN from '{tax_number}' to '{formatted_ssn}' "
                    f"for '{contact_name}'."
                )
                tax_number = formatted_ssn

            address_data = [
                {
                    'AddressType': 'STREET',
                    'AddressLine1': db_contact.get('address_line_1', '') or '',
                    'AddressLine2': db_contact.get('address_line_2', '') or '',
                    'City': db_contact.get('city', '') or '',
                    'PostalCode': db_contact.get('zip', '') or '',
                    'Region': db_contact.get('region', '') or '',
                    'Country': db_contact.get('country', '') or ''
                },
                {
                    'AddressType': 'POBOX',
                    'AddressLine1': db_contact.get('address_line_1', '') or '',
                    'AddressLine2': db_contact.get('address_line_2', '') or '',
                    'City': db_contact.get('city', '') or '',
                    'PostalCode': db_contact.get('zip', '') or '',
                    'Region': db_contact.get('region', '') or '',
                    'Country': db_contact.get('country', '') or ''
                }
            ]

            need_update = False
            if xero_tax_number != tax_number:
                need_update = True
                self.logger.debug(
                    f"- Tax number changed for '{contact_name}' "
                    f"from '{xero_tax_number}' to '{tax_number}'."
                )

            if email != xero_email:
                need_update = True
                self.logger.debug(
                    f"- Email changed for '{contact_name}' "
                    f"from '{xero_email}' to '{email}'."
                )

            if len(xero_addresses) < 2:
                self.logger.debug(
                    f"- Xero contact '{contact_name}' has fewer than 2 addresses stored. "
                    "Triggering update."
                )
                need_update = True
            else:
                for idx in range(2):
                    old = xero_addresses[idx]
                    new = address_data[idx]
                    for field in ['AddressLine1', 'AddressLine2', 'City', 'PostalCode', 'Country', 'Region']:
                        if old.get(field, '') != new.get(field, ''):
                            self.logger.debug(
                                f"- Address {idx} field '{field}' changed for '{contact_name}' "
                                f"from '{old.get(field, '')}' to '{new.get(field, '')}'."
                            )
                            need_update = True
                            break

            if need_update:
                updated_contact_data = {
                    'ContactID': xero_id,
                    'Name': db_contact['name'],
                    'Email': email,
                    'TaxNumber': tax_number,
                    'Addresses': address_data
                }
                contacts_to_update.append(updated_contact_data)
            else:
                self.logger.info(f"- 🎉  No change needed for '{contact_name}'.")

        if contacts_to_update:
            self.logger.info(
                f'- 💾 Sending a batch update for {len(contacts_to_update)} Xero contacts...'
            )
            try:
                self.xero_api.update_contacts_with_retry(contacts_to_update)
                self.logger.info(
                    f'- 🎉 Successfully updated {len(contacts_to_update)} Xero contacts in a single batch.'
                )
            except XeroException as xe:
                self.logger.error(
                    f'- XeroException while updating contacts in batch: {xe}'
                )
            except Exception as e:
                self.logger.debug(
                    f'- Debugging the exception object: '
                    f'type={type(e)}, repr={repr(e)}'
                )
                error_message = f'⚠️ Error in batch update: {e}'
                self.logger.error('- ' + error_message)
        else:
            self.logger.info('- No contacts required updating in Xero.')

        self.logger.info('- 🏁 Finished populating Xero contacts from the local DB in a single batch.')
        self.logger.info('🏁 END function.')

    def validate_xero_data(self, db_contact):
        """
        Validate the DB contact for required fields, address formats, etc.
        Returns a list of error messages, empty if no errors.
        """
        self.logger.debug(f'Checking db_contact => {db_contact}')
        errors = []
        if not db_contact.get('name'):
            errors.append('❗ Missing or empty name.')
        address = {
            'AddressLine1': db_contact.get('address_line_1', ''),
            'AddressLine2': db_contact.get('address_line_2', ''),
            'City': db_contact.get('city', ''),
            'PostalCode': db_contact.get('zip', '')
        }
        if len(address['AddressLine1']) > 255:
            errors.append('❗ AddressLine1 exceeds character limit.')
        if len(address['City']) > 255:
            errors.append('❗ City exceeds character limit.')
        if len(address['PostalCode']) > 50:
            errors.append('❗ PostalCode exceeds character limit.')
        tax_number = str(db_contact.get('tax_number', ''))
        if tax_number and (not tax_number.isalnum()):
            errors.append('❗ TaxNumber contains invalid characters.')
        return errors

    def load_bills(self, project_number: str):
        """
        1) Retrieve summary of ACCPAY invoices matching project_number in InvoiceNumber.
        2) For each invoice, retrieve full details (line items).
        3) For each line item, match or create a local BillLineItem with parent_xero_id
           (and project_number, po_number, detail_number, etc.), then link to XeroBill.
        """
        self.logger.info(f"[load_bills] [Project={project_number}] 🏁 START function.")
        self.logger.info(f"[Retrieving ACCPAY invoices from Xero where InvoiceNumber contains '{project_number}'...")
        summaries = self.xero_api.get_acpay_invoices_summary_by_ref(project_number)
        self.logger.debug(
            f"[load_bills] [Project={project_number}] 🧐 'get_acpay_invoices_summary_by_ref' returned {len(summaries) if summaries else 0} invoices"
        )

        if not summaries:
            self.logger.info(f"[No ACCPAY invoices found with InvoiceNumber containing '{project_number}'.")
            self.logger.info(f"[load_bills] [Project={project_number}] 🏁 END function (no data).")
            return

        for summary_inv in summaries:
            invoice_id = summary_inv.get('InvoiceID')
            invoice_number = summary_inv.get('InvoiceNumber', '')
            status = summary_inv.get('Status', 'DRAFT')
            self.logger.info(f'[Fetching full details for InvoiceNumber={invoice_number} (ID={invoice_id})...')
            full_inv = self.xero_api.get_invoice_details(invoice_id)
            if not full_inv:
                self.logger.warning(f'[Skipping InvoiceID={invoice_id}, no line items returned.')
                continue

            line_items = full_inv.get('LineItems', [])
            self.logger.debug(f'[Invoice {invoice_number} has {len(line_items)} line item(s).')

            parts = invoice_number.split('_')
            if len(parts) >= 2:
                try:
                    project_num = int(parts[0])
                    po_num = int(parts[1])
                    if len(parts) >= 3:
                        detail_num = int(parts[2])
                    else:
                        detail_num = 1
                except ValueError:
                    self.logger.warning(f"[InvoiceNumber='{invoice_number}' not in numeric format. Skipping line item match.")
                    continue
            else:
                self.logger.warning(f"[InvoiceNumber='{invoice_number}' doesn't have at least two parts. Skipping line item match.")
                continue

            existing_bill = self.database_util.search_xero_bills(
                column_names=['xero_reference_number'],
                values=[invoice_number]
            )
            self.logger.debug(f"[existing_bill query => {existing_bill}")

            if not existing_bill:
                self.logger.debug(f'[Creating new xero_bill for invoice_number={invoice_number}.')
                created_bill = self.database_util.create_xero_bill_in_xero(
                    xero_reference_number=invoice_number,
                    xero_id=invoice_id,
                    state=status,
                    project_number=project_num,
                    po_number=po_num,
                    detail_number=detail_num,
                    xero_link=f'https://go.xero.com/AccountsPayable/View.aspx?invoiceId={invoice_id}'
                )
                if not created_bill:
                    self.logger.error(f'[Failed to create xero_bill for {invoice_number}. Skipping line items.')
                    continue
                xero_bill_id = created_bill['id']
            else:
                if isinstance(existing_bill, list):
                    existing_bill = existing_bill[0]
                xero_bill_id = existing_bill['id']
                self.logger.info(f'[Updating existing xero_bill (ID={xero_bill_id}) to status={status}.')
                self.database_util.update_xero_bill(
                    xero_bill_id,
                    xero_id=invoice_id,
                    project_number=project_num,
                    po_number=po_num,
                    detail_number=detail_num,
                    state=status,
                    xero_link=f'https://go.xero.com/AccountsPayable/View.aspx?invoiceId={invoice_id}'
                )

            for (idx, li) in enumerate(line_items):
                xero_line_number = li.get('LineItemID')
                description = li.get('Description')
                quantity = li.get('Quantity')
                unit_amount = li.get('UnitAmount')
                line_amount = li.get('LineAmount')
                account_code_str = li.get('AccountCode')
                account_code = None

                if account_code_str:
                    try:
                        account_code = int(account_code_str)
                    except ValueError:
                        self.logger.warning(f"[AccountCode '{account_code_str}' is not an integer. Using None.")

                existing_line = self.database_util.search_bill_line_items(
                    column_names=['parent_xero_id'],
                    values=[xero_line_number]
                )
                self.logger.debug(f'[existing_line query => {existing_line}')

                if existing_line:
                    if isinstance(existing_line, list):
                        existing_line = existing_line[0]
                    bill_line_item_id = existing_line['id']
                    self.logger.info(f'[Updating existing BillLineItem (ID={bill_line_item_id}) with Xero line data.')
                    updated_line = self.database_util.update_bill_line_item(
                        bill_line_item_id,
                        description=description,
                        quantity=quantity,
                        unit_amount=unit_amount,
                        line_amount=line_amount,
                        account_code=account_code
                    )
                    if updated_line:
                        self.logger.debug(f'[BillLineItem (ID={bill_line_item_id}) successfully updated.')
                    else:
                        self.logger.error(f'[Failed to update BillLineItem (ID={bill_line_item_id}).')
                else:
                    self.logger.info(f'[No BillLineItem found for parent_xero_id={xero_line_number}. Creating a new one.')
                    new_line = self.database_util.create_bill_line_item(
                        parent_id=xero_bill_id,
                        parent_xero_id=xero_line_number,
                        project_number=project_num,
                        po_number=po_num,
                        detail_number=detail_num,
                        line_number=idx + 1,
                        description=description,
                        quantity=quantity,
                        unit_amount=unit_amount,
                        line_amount=line_amount,
                        account_code=account_code
                    )
                    if new_line:
                        self.logger.debug(
                            f"[Created BillLineItem with ID={new_line['id']} and parent_xero_id={xero_line_number}."
                        )
                    else:
                        self.logger.error(f'[Failed to create BillLineItem for parent_xero_id={xero_line_number}.')

        self.logger.info(f"[Finished loading ACCPAY invoices matching '{project_number}', with line items mapped to local BillLineItems.")
        self.logger.info(f"[load_bills] [Project={project_number}] 🏁 END function.")

    # ------------------------------------------------------------------------------
    # HELPER #1: Adjust Bill's Dates to Create the Largest Range
    # ------------------------------------------------------------------------------
    def set_largest_date_range(self, parent_bill_id: int) -> None:
        """
        Ensures the parent's 'transaction_date' is the earliest date found
        among all detail items sharing the same project_number, po_number,
        and detail_number as the parent bill, and ensures the parent's
        'due_date' is the latest date among them. If any of those dates are
        currently set on the parent, they will also be taken into account.
        """

        self.logger.info(f'[ParentBillID={parent_bill_id}] 🏁 START function.')

        # 1) Fetch the parent bill
        xero_bill = self.db_ops.search_xero_bills(['id'], [parent_bill_id])
        if not xero_bill:
            self.logger.warning(
                f'No parent bill found for ID={parent_bill_id}.'
            )
            self.logger.info(
                f'[ParentBillID={parent_bill_id}] 🏁 END function (no parent bill).'
            )
            return

        if isinstance(xero_bill, list):
            xero_bill = xero_bill[0]

        self.logger.debug(f'Fetched parent bill => {xero_bill}')

        # 2) Get the parent's key fields (project_number, po_number, detail_number)
        project_number = xero_bill.get('project_number')
        po_number = xero_bill.get('po_number')
        detail_number = xero_bill.get('detail_number')

        if not (project_number and po_number and detail_number):
            self.logger.warning(
                'Parent bill is missing project_number, po_number, or detail_number.'
            )
            self.logger.info(
                f'[ParentBillID={parent_bill_id}] 🏁 END function (missing fields).'
            )
            return

        # 3) Find *all* detail items with the same (project_number, po_number, detail_number)
        detail_items = self.db_ops.search_detail_item_by_keys(
            project_number=project_number,
            po_number=po_number,
            detail_number=detail_number
        )
        if not detail_items:
            self.logger.info(
                f'No detail items found for (proj={project_number}, po={po_number}, detail={detail_number}).'
            )
            self.logger.info(
                f'[ParentBillID={parent_bill_id}] 🏁 END function (no detail items).'
            )
            return

        # If the DB operation returns a single dict for a single record, normalize to a list
        if isinstance(detail_items, dict):
            detail_items = [detail_items]

        self.logger.debug(
            f'Found {len(detail_items)} detail item(s) matching parent bill.'
        )

        # 4) Determine earliest transaction_date and latest due_date among all detail items
        existing_parent_date = xero_bill.get('transaction_date')
        existing_parent_due = xero_bill.get('due_date')

        # Collect all detail item dates (ignoring None)
        detail_dates = [
            di['transaction_date'] for di in detail_items
            if di.get('transaction_date') is not None
        ]
        detail_dues = [
            di['due_date'] for di in detail_items
            if di.get('due_date') is not None
        ]

        # Include parent's existing date and due date in the comparison (if present)
        if existing_parent_date is not None:
            detail_dates.append(existing_parent_date)
        if existing_parent_due is not None:
            detail_dues.append(existing_parent_due)

        self.logger.debug(
            f'Existing parent dates => transaction_date={existing_parent_date}, '
            f'due_date={existing_parent_due}'
        )

        from datetime import datetime, date

        # Helper function to convert datetime to date
        def to_date(d):
            if isinstance(d, datetime):
                return d.date()
            elif isinstance(d, date):
                return d
            else:
                raise TypeError(f"Unsupported type: {type(d)}")

        # Apply the conversion to all dates in detail_dates and detail_dues
        detail_dates = [to_date(d) for d in detail_dates if d is not None]
        detail_dues = [to_date(d) for d in detail_dues if d is not None]

        # Include existing parent dates if they are present
        if existing_parent_date:
            existing_parent_date = to_date(existing_parent_date)
        if existing_parent_due:
            existing_parent_due = to_date(existing_parent_due)

        # Calculate the new earliest transaction_date and latest due_date
        if detail_dates:
            earliest_date = min(detail_dates)
        else:
            earliest_date = existing_parent_date

        if detail_dues:
            latest_due = max(detail_dues)
        else:
            latest_due = existing_parent_due

        self.logger.debug(
            f'Computed new Bill dates => transaction_date={earliest_date}, due_date={latest_due}'
        )

        # 5) Update the parent bill if there's a difference
        if (earliest_date != existing_parent_date) or (latest_due != existing_parent_due):
            self.logger.info(
                f'Updating Bill={parent_bill_id} with '
                f'earliest transaction_date={earliest_date} and latest due_date={latest_due}.'
            )
            self.db_ops.update_xero_bill(
                xero_bill_id=xero_bill['id'],
                xero_bill=xero_bill.get('xero_id'),
                transaction_date=earliest_date,
                due_date=latest_due
            )
        else:
            self.logger.info(
                f'No changes needed to Bill={parent_bill_id} date range.'
            )

        self.logger.info(
            f'[ParentBillID={parent_bill_id}] 🏁 END function.'
        )

    # ------------------------------------------------------------------------------
    # HELPER #3: Sync Bill Line Item to Xero (unless Bill is final)
    # ------------------------------------------------------------------------------
    def sync_line_item_to_xero(
        self,
        parent_bill_id: int,
        bill_line_item_id: int,
        is_create: bool = False
    ) -> None:
        """
        If there's a parent_xero_id, then update or add the BillLineItem in Xero
        unless the Bill is in a final state. If the Bill is final, fetch from Xero
        and ensure it is truly final in Xero (update it if different).
        """
        self.logger.info(f'[ParentBillID={parent_bill_id}, LineItemID={bill_line_item_id}, Create={is_create}] 🏁 START function.')

        # Grab parent bill
        parent_bill = self.db_ops.search_xero_bills(['id'], [parent_bill_id])
        self.logger.debug(f'parent_bill => {parent_bill}')

        if not parent_bill:
            self.logger.warning(
                f'No parent bill found for ID={parent_bill_id}.'
            )
            self.logger.info(f'[ParentBillID={parent_bill_id}, LineItemID={bill_line_item_id}] 🏁 END function (no parent).')
            return
        if isinstance(parent_bill, list):
            parent_bill = parent_bill[0]

        # Grab line item
        line_item = self.db_ops.search_bill_line_items(['id'], [bill_line_item_id])
        self.logger.debug(f'line_item => {line_item}')

        if not line_item:
            self.logger.warning(
                f'No line item found for ID={bill_line_item_id}.'
            )
            self.logger.info(f'[ParentBillID={parent_bill_id}, LineItemID={bill_line_item_id}] 🏁 END function (no line item).')
            return
        if isinstance(line_item, list):
            line_item = line_item[0]

        parent_xero_id = line_item.get('parent_xero_id')
        if not parent_xero_id:
            self.logger.info(
                f'No parent_xero_id on line item={bill_line_item_id}, skipping Xero sync.'
            )
            self.logger.info(f'[ParentBillID={parent_bill_id}, LineItemID={bill_line_item_id}] 🏁 END function (missing parent_xero_id).')
            return

        bill_state = parent_bill.get('state', '').upper()
        if bill_state == 'FINAL':
            # Bill is final => check Xero to ensure invoice is also final
            self.logger.info(
                f'Bill={parent_bill_id} is FINAL. Checking Xero invoice state...'
            )
            try:
                invoice_data = self.xero_api.get_invoice_details(parent_bill['xero_id'])
                self.logger.debug(f'get_invoice_details => {invoice_data}')

                if invoice_data:
                    xero_status = invoice_data.get('Status', '').upper()
                    if xero_status not in ['AUTHORISED', 'PAID']:
                        self.logger.info(
                            f'Xero invoice status is {xero_status}, switching to AUTHORISED.'
                        )
                        self.xero_api.update_invoice(parent_bill['xero_id'], {'Status': 'AUTHORISED'})
                    else:
                        self.logger.info('Xero invoice already final or paid.')
                else:
                    self.logger.warning(
                        f'Could not retrieve invoice data from Xero for ID={parent_bill["xero_id"]}.'
                    )
            except XeroException as xe:
                self.logger.error(
                    f'XeroException: {xe}'
                )
            self.logger.info(f'[ParentBillID={parent_bill_id}, LineItemID={bill_line_item_id}] 🏁 END function (bill final).')
            return

        # Not final => we can safely create or update in Xero
        if is_create:
            self.logger.info(
                f'Creating line item in Xero for line={bill_line_item_id}.'
            )
            # Possibly call your xero_api.add_line_item_to_invoice(...)
            # or self.create_bill_line_item_in_xero(bill_line_item_id)
            # Example:
            self.create_bill_line_item_in_xero(bill_line_item_id)

        else:
            self.logger.info(
                f'Updating line item in Xero for line={bill_line_item_id}.'
            )
            # Possibly call your xero_api.update_line_item_in_invoice(...)
            # This depends on your actual Xero implementation.

        self.logger.info(
            f'Finished Xero sync for line item={bill_line_item_id}.'
        )
        self.logger.info(f'[ParentBillID={parent_bill_id}, LineItemID={bill_line_item_id}] 🏁 END function.')

    def set_parent_xero_id_on_line_item(
        self,
        parent_bill_id: int,
        bill_line_item_id: int
    ) -> None:
        """
        Always store the local parent's xero_bill_id (parent_bill_id) on the line item,
        and if the parent bill has a 'xero_id', store it as parent_xero_id.
        """
        self.logger.info(
            f'[ParentBillID={parent_bill_id}, LineItemID={bill_line_item_id}] 🏁 START function.'
        )

        # Fetch parent bill
        parent_bill = self.db_ops.search_xero_bills(['id'], [parent_bill_id])
        self.logger.debug(f'parent_bill => {parent_bill}')

        if not parent_bill:
            self.logger.warning(
                f'No parent bill found for ID={parent_bill_id}.'
            )
            self.logger.info(
                f'[ParentBillID={parent_bill_id}, LineItemID={bill_line_item_id}] 🏁 END function (no parent).'
            )
            return

        if isinstance(parent_bill, list):
            parent_bill = parent_bill[0]

        # We'll always store the local parent ID (xero_bill_id) on the line item
        update_data = {
            'parent_id': parent_bill_id
        }

        # If the parent bill has a Xero ID, store it on the line item
        parent_xero_id = parent_bill.get('xero_id')
        if parent_xero_id:
            update_data['parent_xero_id'] = parent_xero_id
            self.logger.debug(
                f'Parent Xero ID found: {parent_xero_id}.'
            )
        else:
            self.logger.info(
                f'Bill {parent_bill_id} has no xero_id. Storing just the local parent_id.'
            )

        self.logger.debug(
            f'Updating line item={bill_line_item_id} with {update_data}.'
        )
        self.db_ops.update_bill_line_item(
            bill_line_item_id,
            **update_data
        )

        self.logger.info(
            f'Stored parent_id={parent_bill_id} and parent_xero_id={parent_xero_id} on line item={bill_line_item_id}.'
        )
        self.logger.info(
            f'[ParentBillID={parent_bill_id}, LineItemID={bill_line_item_id}] 🏁 END function.'
        )

    def set_xero_id_on_line_items(self, xero_bill: dict) -> None:
        """
        Given a full xero_bill object, find all associated child line items
        by project_number, po_number, and detail_number, and set their parent_xero_id
        to the xero_id from the xero_bill.

        Args:
            xero_bill (dict): The full xero_bill record containing project_number, po_number, detail_number, and xero_id.
        """
        parent_bill_id = xero_bill.get('id')  # Assuming 'id' is the local database ID
        self.logger.info(
            f'[ParentBillID={parent_bill_id}] 🏁 START function.'
        )

        # Extract necessary fields from xero_bill
        project_number = xero_bill.get('project_number')
        po_number = xero_bill.get('po_number')
        detail_number = xero_bill.get('detail_number')
        xero_id = xero_bill.get('xero_id')

        if not all([project_number, po_number, detail_number, xero_id]):
            self.logger.warning(
                f'xero_bill missing required fields (project_number, po_number, detail_number, xero_id).'
            )
            self.logger.info(
                f'[ParentBillID={parent_bill_id}] 🏁 END function (missing fields).'
            )
            return

        self.logger.debug(
            f'Extracted project_number={project_number}, po_number={po_number}, detail_number={detail_number}, xero_id={xero_id}.'
        )

        # Search for all child line items matching project_number, po_number, detail_number
        child_line_items = self.db_ops.search_bill_line_item_by_keys(
            project_number=project_number,
            po_number=po_number,
            detail_number=detail_number
        )

        if not child_line_items:
            self.logger.info(
                f'No child line items found for ParentBillID={parent_bill_id}.'
            )
            self.logger.info(
                f'[ParentBillID={parent_bill_id}] 🏁 END function (no child line items).'
            )
            return
        self.logger.debug(
            f'Found {len(child_line_items)} child line item(s) for ParentBillID={parent_bill_id}.'
        )

        # Iterate over each child line item
        for line_item in child_line_items:
            bill_line_item_id = line_item.get('id')
            current_parent_xero_id = line_item.get('parent_xero_id')

            if current_parent_xero_id:
                self.logger.info(
                    f'BillLineItem={bill_line_item_id} already has parent_xero_id={current_parent_xero_id}. Skipping.'
                )
                continue  # Skip if parent_xero_id is already set

            self.logger.info(
                f'Updating BillLineItem={bill_line_item_id} with parent_xero_id={xero_id}.'
            )

            # Update the child line item with the parent's xero_id
            update_success = self.db_ops.update_bill_line_item(
                bill_line_item_id,
                parent_xero_id=xero_id
            )

            if update_success:
                self.logger.info(
                    f'Successfully updated BillLineItem={bill_line_item_id} with parent_xero_id={xero_id}.'
                )
            else:
                self.logger.error(
                    f'Failed to update BillLineItem={bill_line_item_id} with parent_xero_id={xero_id}.'
                )

        self.logger.info(
            f'[ParentBillID={parent_bill_id}] 🏁 END function.'
        )

# Instantiate the singleton for use
xero_services = XeroServices()