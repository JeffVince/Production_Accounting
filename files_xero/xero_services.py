import datetime
import logging
import re
from decimal import Decimal, ROUND_HALF_UP

from sqlalchemy.exc import IntegrityError

from database.database_util import DatabaseOperations
from files_xero.xero_api import xero_api
from utilities.singleton import SingletonMeta

#TODO new bill line items need to be full dicts not just IDs
#TODO add tax code, date, and description to Spend Item sync

def parse_reference(reference):
    parts = reference.split("_")
    if len(parts) == 3:
        parts.append("1")
    elif len(parts) != 4:
        raise ValueError(
            f"Expected 3 or 4 segments separated by underscores, got {len(parts)} segments: {parts}")

    project_str, po_str, detail_str, line_number = parts
    return project_str, po_str, detail_str, line_number


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
    def handle_spend_money_create_bulk(self, spend_money_items: list, session):
        # Retrieve spend money records for the provided IDs.
        spend_money_ids = [item.get('id') for item in spend_money_items]
        spend_money_records = self.db_ops.search_spend_money(["id"], [spend_money_ids], session=session)
        if not spend_money_records:
            self.logger.warning("No SpendMoney records found for the provided IDs.")
            return []

        # Separate records into creation vs. update groups.
        records_to_create = []
        records_to_update = []
        for record in spend_money_records:
            # Skip records that are already in a final state.
            if (record.get('state') or '').upper() == "RECONCILED":
                self.logger.info(f"SpendMoney record with id {record.get('id')} is already reconciled. Skipping.")
                continue

            # Check if the record already has a Xero spend money ID.
            # If it does, it means a Xero transaction exists and should be updated instead.
            if record.get('xero_spend_money_id'):
                self.logger.info(
                    f"SpendMoney record with id {record.get('id')} already has Xero ID {record.get('xero_spend_money_id')}. Marking for update."
                )
                records_to_update.append(record)
            else:
                records_to_create.append(record)

        updated_spend_money = []

        # Process bulk creation for new spend money records.
        if records_to_create:
            # Optionally format records to create into Spend Money format for Xero API
            # records_to_create = self.xero_api.format_spend_money_bulk(records_to_create)
            self.logger.info(f"Attempting bulk creation for {len(records_to_create)} new SpendMoney records in Xero.")
            bulk_create_response = self.xero_api.create_spend_money_bulk(records_to_create)
            if not bulk_create_response:
                self.logger.warning("No valid response from Xero after bulk spend money creation.")
            else:
                for record, response in zip(records_to_create, bulk_create_response):
                    spend_money_id = record.get('id')
                    if response and response.get('xero_spend_money_id'):
                        new_xero_spend_money_id = response['xero_spend_money_id']
                        self.db_ops.update_spend_money(
                            spend_money_id,
                            xero_spend_money_id=new_xero_spend_money_id,
                            session=session
                        )
                        self.logger.info(
                            f"Successfully created SpendMoney record {spend_money_id} in Xero => xero_spend_money_id={new_xero_spend_money_id}"
                        )
                        updated_spend_money.append({
                            "id": spend_money_id,
                            "xero_spend_money_id": new_xero_spend_money_id,
                            "xero_link": f"https://go.xero.com/Bank/ViewTransaction.aspx?bankTransactionID={new_xero_spend_money_id}"
                        })
                    else:
                        self.logger.warning(
                            f"Failed to create SpendMoney record {spend_money_id} in Xero. Response: {response}"
                        )
        else:
            self.logger.info("No new SpendMoney records to create in Xero.")

        # Process updates for spend money records that already have a Xero ID.
        if records_to_update:
            self.logger.info(
                f"Attempting bulk update for {len(records_to_update)} existing SpendMoney records in Xero.")
            bulk_update_response = self.xero_api.update_spend_money_bulk(records_to_update)
            if not bulk_update_response:
                self.logger.warning("No valid response from Xero after bulk spend money update.")
            else:
                for record, response in zip(records_to_update, bulk_update_response):
                    spend_money_id = record.get('id')
                    if response and response.get('xero_spend_money_id'):
                        updated_xero_spend_money_id = response['xero_spend_money_id']
                        self.db_ops.update_spend_money(
                            spend_money_id,
                            xero_spend_money_id=updated_xero_spend_money_id,
                            session=session
                        )
                        self.logger.info(
                            f"Successfully updated SpendMoney record {spend_money_id} in Xero => xero_spend_money_id={updated_xero_spend_money_id}"
                        )
                        updated_spend_money.append({
                            "id": spend_money_id,
                            "xero_spend_money_id": updated_xero_spend_money_id,
                            "xero_link": f"https://go.xero.com/Bank/ViewTransaction.aspx?bankTransactionID={updated_xero_spend_money_id}"
                        })
                    else:
                        self.logger.warning(
                            f"Failed to update SpendMoney record {spend_money_id} in Xero. Response: {response}"
                        )
        else:
            self.logger.info("No existing SpendMoney records require an update in Xero.")

        return updated_spend_money

    def handle_spend_money_create(self, spend_money_id: int):
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

    def handle_spend_money_update(self, spend_money_id: int, new_state: str):
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
    def handle_xero_bill_create_bulk(self, new_bills: list, new_bill_line_items: list, session):
        self.logger.info(f"Pushing {len(new_bills)} bills to Xero.")
        payloads = []
        for bill in new_bills:
            # Build basic payload
            project_number = bill.get("project_number")
            po_number = bill.get("po_number")
            detail_number = bill.get("detail_number")
            if bill.get("contact_xero_id"):
                contact_xero_id = bill.get("contact_xero_id")
            else:
                po_record = self.db_ops.search_purchase_order_by_keys(project_number=project_number, po_number=po_number)
                if isinstance(po_record, list):
                    po_record = po_record[0]
                if not po_record:
                    self.logger.warning(
                        f"Failed to find Purchase Order record for project_number={project_number} and po_number={po_number}"
                    )
                    continue

                contact_id = po_record.get("contact_id")
                contact_record = self.db_ops.search_contacts(["id"], [contact_id])
                if isinstance(contact_record, list):
                    contact_record = contact_record[0]
                if not contact_record:
                    self.logger.warning(f"Failed to find Contact record for contact_id={contact_id}")
                    continue

                contact_xero_id = contact_record.get("xero_id")
                if not contact_xero_id:
                    self.logger.warning(f"Contact record lacks Xero ID for contact_id={contact_id}")
                    self.logger.info(f"Creating Contact in Xero for contact_id={contact_id}")
                    contact_xero_id = self.xero_api.create_contact(contact_record)
                    if not contact_xero_id:
                        self.logger.warning("Failed to create Contact in Xero.")
                        continue

            # Convert the stored strings/dates to date objects
            due_date_raw = bill.get("due_date")
            if due_date_raw:
                if isinstance(due_date_raw, datetime.date):
                    parsed_due_date = due_date_raw
                else:
                    parsed_due_date = datetime.datetime.strptime(due_date_raw, "%Y-%m-%d").date()
            else:
                parsed_due_date = None

            transaction_date_raw = bill.get("transaction_date")
            if transaction_date_raw:
                if isinstance(transaction_date_raw, datetime.date):
                    parsed_transaction_date = transaction_date_raw
                else:
                    parsed_transaction_date = datetime.datetime.strptime(transaction_date_raw, "%Y-%m-%d").date()
            else:
                parsed_transaction_date = None

            #build reference number
            bill_reference_number = bill.get("xero_reference_number")
            if not bill_reference_number:
                bill_reference_number = f"{project_number}_{po_number}_{detail_number}"
                self.logger.debug(f"Bill ID {bill['id']}: Setting Xero reference number to {bill_reference_number}")

            # Basic invoice payload
            payload = {
                "Type": "ACCPAY",
                "InvoiceNumber": bill_reference_number,
                "Date": parsed_transaction_date,
                "DueDate": parsed_due_date,
                "Contact": {"ContactID": contact_xero_id},
            }

            # Fetch raw line items and transform them for Xero
            line_items_raw = new_bill_line_items
            if line_items_raw:
                if isinstance(line_items_raw, dict):
                    line_items_raw = [line_items_raw]
                xero_line_items = []
                for li in line_items_raw:
                    #make sure line item matches bill key
                    if (li["project_number"], li["po_number"], li["detail_number"]) == (project_number, po_number, detail_number):
                        xero_line_items.append({
                            'Description': li.get('description'),
                            'Quantity': li.get('quantity', Decimal('1')),
                            'UnitAmount': li.get('unit_amount', Decimal('0')),
                            'AccountCode': str(li.get('tax_code', '0000')),
                            'LineAmount': li.get('line_amount', Decimal('0')),
                        })
                payload["LineItems"] = xero_line_items
                if len(xero_line_items) == 0:
                    self.logger.debug(f"No LineItems to add to bill item {project_number}_{po_number}_{detail_number}.")
                self.logger.debug(f"Bill ID {bill_reference_number}: Including {len(xero_line_items)} line items in payload.")
            else:
                self.logger.warning(f"Bill ID {bill_reference_number}: No line items found in DB.")

            payloads.append(payload)

        self.logger.info(f"Sending bulk payload for {len(payloads)} bills.")
        result = self.xero_api.create_invoice_bulk(payloads)
        self.logger.debug(f"Bulk create invoice response: {result}")
        if not result:
            self.logger.error("Bulk invoice creation in Xero failed.")
            return []

        updated_bills = []
        # Update local DB with new Xero IDs
        for bill, inv in zip(new_bills, result):
            try:
                new_xero_id = inv.get("InvoiceID")
                link = f"https://go.xero.com/AccountsPayable/View.aspx?invoiceId={new_xero_id}"
                self.db_ops.update_xero_bill(bill["id"], xero_id=new_xero_id, xero_link=link, session=session)
                self.logger.info(f"Bill ID {bill['id']}: Created new Xero invoice => ID={new_xero_id}")
                updated_bills.append({
                    "id": bill["id"],
                    "xero_id": new_xero_id,
                    "xero_link": link
                })
            except Exception as e:
                self.logger.error(f"Bill ID {bill['id']}: Error parsing invoice response: {e}")
        return updated_bills

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
        purchase_orders = self.db_ops.search_purchase_orders(['project_number', 'po_number'],
                                                             [project_number, po_number])
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

    def update_xero_bill(self, bill_id: int):
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
            if not (project_number and po_number and detail_number):
                self.logger.warning("Missing keys => cannot update detail items.")
                return
            detail_items = self.db_ops.search_detail_item_by_keys(project_number, po_number, detail_number)
            self.logger.debug(f'Found detail_items => {detail_items}')

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

    def delete_xero_bill(self, bill_id: int):
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

    def load_xero_bills(self, project_number: int, po_number: int = None):
        """
        Loads Xero bills (ACCPAY invoices) and their line items for a given project_number
        and (optionally) a po_number into the local database.

        Assumes that each bill's 'InvoiceNumber' is formatted as "projectNumber_poNumber_detailNumber".
        Uses the local database operations (self.db_ops) to search, create, or update records.
        """
        self.logger.info(f"load_xero_bills => project_number={project_number}, po_number={po_number}")
        self.logger.info("Retrieving Xero bills from Xero...")

        xero_bills = self.xero_api.get_bills_by_reference(project_number)
        if not xero_bills:
            self.logger.info("No Xero bills retrieved.")
            return []

        for bill in xero_bills:
            reference = bill.get("InvoiceNumber", "")
            parts = reference.split("_")
            if len(parts) == 2:
                parts.append("1")
            local_project_number = None
            local_po_number = None
            detail_number = None
            if len(parts) >= 3:
                try:
                    local_project_number = int(parts[0])
                    local_po_number = int(parts[1])
                    detail_number = int(parts[2])
                except ValueError:
                    self.logger.warning(f"Invalid detail_number in reference: {reference}")

            xero_invoice_id = bill.get("InvoiceID")
            xero_link = (f"https://go.xero.com/AccountsPayable/View.aspx?invoiceId={xero_invoice_id}"
                         if xero_invoice_id else None)

            # Extract additional fields from the Xero bill
            bill_date = bill.get("Date") or bill.get("transaction_date")
            bill_due_date = bill.get("DueDate") or bill.get("due_date")
            contact_data = bill.get("Contact", {})
            xero_contact_id = contact_data.get("ContactID")
            bill_status = bill.get("state", "DRAFT")
            if bill.get("IsReconciled") or \
                    (bill.get("state") == "PAID" and bill.get("AmountDue", 0) == 0) or \
                    bill.get("FullyPaidOnDate"):
                bill_status = "RECONCILED"

            # Upsert the Xero bill record in the local database.
            local_bill = self.db_ops.search_xero_bill_by_keys(local_project_number, local_po_number, detail_number)
            if local_bill:
                if isinstance(local_bill, list):
                    local_bill = local_bill[0]
                local_bill_id = local_bill['id']
                self.db_ops.update_xero_bill(
                    local_bill_id,
                    xero_id=xero_invoice_id,
                    xero_link=xero_link,
                    transaction_date=bill_date,
                    due_date=bill_due_date,
                    contact_xero_id=xero_contact_id,
                    state=bill_status
                )
                self.logger.info(f"Updated local bill ID {local_bill_id} with Xero invoice {xero_invoice_id}")
            else:
                local_bill = self.db_ops.create_xero_bill_by_keys(
                    local_project_number,
                    local_po_number,
                    detail_number,
                    xero_id=xero_invoice_id,
                    xero_link=xero_link,
                    transaction_date=bill_date,
                    due_date=bill_due_date,
                    contact_xero_id=xero_contact_id,
                    state=bill_status
                )
                local_bill_id = local_bill['id']
                self.logger.info(f"Created new local bill for Reference {reference} with ID {local_bill_id}")

            # Process each line item in the bill.
            line_items = bill.get("LineItems", [])
            for idx, li in enumerate(line_items, start=1):
                # Extract fields from the Xero line item.
                line_item_id = li.get("LineItemID", idx)
                description = li.get("Description", "")
                quantity = li.get("Quantity", 1)
                unit_amount = Decimal(li.get("UnitAmount", 0)).quantize(Decimal("0.00"), rounding=ROUND_HALF_UP)
                tax_code = li.get("AccountCode", "")

                # Attempt to find a matching detail item using project, PO, detail number, unit amount, and description.
                matching_detail_item = self.db_ops.search_detail_items(
                    ["project_number", "po_number", "detail_number", "sub_total"],
                    [local_project_number, local_po_number, detail_number, unit_amount]
                )
                if matching_detail_item:
                    if isinstance(matching_detail_item, list):
                        matching_detail_item = matching_detail_item[0]
                    # Update the detail item with the Xero line item ID and parent Xero bill ID.
                    self.db_ops.update_detail_item(
                        matching_detail_item['id'],
                        xero_id=line_item_id,
                    )
                    self.logger.debug(
                        f"Updated detail item ID {matching_detail_item['id']} with xero_id {line_item_id} "
                    )
                    line_number = matching_detail_item["line_number"]

                else:
                    self.logger.debug(
                        f"No matching detail item found for project {local_project_number}, PO {local_po_number}, "
                        f"detail {detail_number}, unit_amount {unit_amount}"
                    )
                    line_number = None


                # Process the corresponding Xero bill line item.
                local_li = self.db_ops.search_xero_bill_line_item_by_keys(
                    local_project_number,
                    local_po_number,
                    detail_number,
                    line_number
                )
                if local_li:
                    if not isinstance(local_li, list):
                        local_li = [local_li]
                    for li_ in local_li:
                        self.db_ops.update_xero_bill_line_item(
                            li_['id'],
                            description=description,
                            quantity=quantity,
                            unit_amount=unit_amount,
                            tax_code=tax_code,
                            xero_bill_line_id=line_item_id,
                            parent_xero_id=xero_invoice_id,
                            line_number=line_number
                        )
                        self.logger.info(
                            f"Updated local bill line item ID {li_['id']} with xero_bill_line_id {line_item_id} "
                            f"and parent_xero_id {xero_invoice_id} for bill Reference {reference}"
                        )
                else:
                    self.db_ops.create_xero_bill_line_item_by_keys(
                        parent_id=local_bill_id,
                        project_number=local_project_number,
                        po_number=local_po_number,
                        detail_number=detail_number,
                        line_number=line_number,
                        description=description,
                        quantity=quantity,
                        unit_amount=unit_amount,
                        tax_code=tax_code,
                        xero_bill_line_id=line_item_id,
                        parent_xero_id=xero_invoice_id
                    )
                    self.logger.info(
                        f"Created new line item for bill Reference {reference} with xero_bill_line_id {line_item_id} "
                        f"and parent_xero_id {xero_invoice_id}"
                    )

        return "Success"

    # ─────────────────────────────────────────────────────────────
    #                  SPEND MONEY LOADING
    # ─────────────────────────────────────────────────────────────
    def load_spend_money_transactions(self, project_id: int = None, po_number: int = None, detail_number: int = None):
        self.logger.info(
            f'load_spend_money_transactions => project={project_id}, po={po_number}, detail={detail_number}')
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
            current_state = 'RECONCILED' if tx.get('IsReconciled', False) else tx.get('Status', 'DRAFT')
            if current_state == 'DELETED':
                self.logger.info(f'SKIPPING DELETED transaction => {tx.get("InvoiceNumber")}')
                continue
            reference_number = tx.get('InvoiceNumber')
            bank_transaction_id = tx.get('BankTransactionID')
            xero_link = f'https://go.xero.com/Bank/ViewTransaction.aspx?bankTransactionID={bank_transaction_id}'
            existing_spend = self.db_ops.search_spend_money(['xero_spend_money_reference_number'], [reference_number])
            self.logger.debug(f'existing_spend => {existing_spend}')
            # Build the kwargs for record creation or update.
            # These keys should match the SpendMoney model fields.
            # Extract the reference string from the tx variable (e.g., "2416_02_01_01")
            reference = tx.get("Reference", "")
            try:
                # Split the reference by underscores into three parts
                project_str, po_str, detail_str, line_number = parse_reference(reference)

                # Convert the parts to integers (this removes any leading zeros)
                project_number = int(project_str)
                po_number = int(po_str)
                detail_number = int(detail_str)
                line_number = int(line_number)

            except ValueError:
                # In case the reference is missing or not properly formatted,
                # you might want to set defaults or handle the error appropriately.
                #skip this spend money receipt
                continue



            # Attempt to find the contact based on the Xero contact ID
            contact_record = self.db_ops.search_contacts(["xero_id"], [tx["Contact"]["ContactID"]])

            if not contact_record:
                self.logger.info(f"No contact found for Xero ID {tx['Contact']['ContactID']}. Creating a new contact.")
                # Prepare the new contact data using the provided name or a fallback if the name is missing
                name = tx["Contact"].get("Name", "Unnamed Contact")
                xero_id = tx["Contact"]["ContactID"]
                # Create the new contact record in the DB using keyword arguments
                contact_record = self.db_ops.create_contact(name=name, xero_id=xero_id)

            # Extract the local contact ID from the returned contact record
            if isinstance(contact_record, list):
                contact_id = contact_record[0]["id"]
            else:
                contact_id = contact_record["id"]

            spend_kwargs = {
                "project_number": project_number,
                "po_number": po_number,
                "detail_number": detail_number,
                "line_number": line_number,
                "state": current_state,
                "xero_spend_money_id": bank_transaction_id,
                "xero_link": xero_link,
                "contact_id": contact_id,
                "amount": tx.get("Total", 0),
            }

            # Attempt to locate an existing SpendMoney record using the unique keys.
            existing_record = self.db_ops.search_spend_money_by_keys(
                project_number=project_number,
                po_number=po_number,
                detail_number=detail_number,
                line_number=line_number
            )

            if not existing_record:
                # No record exists, so create a new one.
                new_record = self.db_ops.create_spend_money(**spend_kwargs)
                self.logger.info(f"Created new SpendMoney record with ID {new_record.get('id')}.")
            else:
                # If there are multiple records, assume the first is the one to update.
                record = existing_record if isinstance(existing_record, dict) else existing_record[0]

                # Use spend_money_has_changes to check if any fields have changed.
                if self.db_ops.spend_money_has_changes(
                        record_id=record.get("id"),
                        **spend_kwargs
                ):
                    updated_record = self.db_ops.update_spend_money(record.get("id"), **spend_kwargs)
                    self.logger.info(f"Updated SpendMoney record with ID {updated_record.get('id')}.")
                else:
                    self.logger.info(
                        f"SpendMoney record with ID {record.get('id')} exists and no changes were detected.")
    # ─────────────────────────────────────────────────────────────
    #             POPULATE LOCAL CONTACTS WITH XERO IDS
    # ─────────────────────────────────────────────────────────────
    def populate_xero_contacts(self):
        self.logger.info('populate_xero_contacts => retrieving local DB and Xero contacts...')
        # Retrieve all contacts from the local DB.
        db_contacts = self.db_ops.search_contacts()
        self.logger.info(f'Found {len(db_contacts)} contacts locally.')

        # Build a set of local contact names (in lower-case) for easy lookup.
        db_contact_names = {contact.get('name', '').strip().lower() for contact in db_contacts if contact.get('name')}

        self.logger.info('Retrieving all contacts from Xero...')
        try:
            all_xero_contacts = self.xero_api.get_all_contacts()
            self.logger.debug(f'get_all_contacts => {len(all_xero_contacts)} contacts')
        except Exception as xe:
            self.logger.error(f'Failed to retrieve contacts from Xero => {xe}')
            return

        # Build a dictionary of Xero contacts mapping lower-case name to the contact object.
        xero_contacts_dict = {
            c['Name'].strip().lower(): c for c in all_xero_contacts if c.get('Name')
        }

        # Prepare a list to accumulate local contacts that need to be created in Xero.
        contacts_to_create = []

        # Process each local DB contact.
        for db_contact in db_contacts:
            contact_name = db_contact.get('name', '').strip()
            if not contact_name:
                continue
            lower_name = contact_name.lower()

            if lower_name in xero_contacts_dict:
                # Match found in Xero: update the local record with the Xero_ID.
                xero_contact = xero_contacts_dict[lower_name]
                xero_id = xero_contact['ContactID']
                if db_contact.get('xero_id') != xero_id:
                    try:
                        self.db_ops.update_contact(db_contact['id'], xero_id=xero_id)
                        self.logger.info(f"Linked local contact '{contact_name}' => XeroID={xero_id}")
                    except IntegrityError as ie:
                        self.logger.warning(
                            f"IntegrityError while updating contact '{contact_name}' with XeroID {xero_id}: {ie}. Skipping update.")
            else:
                # No match found in Xero: stage this contact for bulk creation.
                self.logger.info(f"Local contact '{contact_name}' not found in Xero. Staging for bulk creation...")
                contacts_to_create.append(db_contact)

        # Bulk create new contacts in chunks.
        if contacts_to_create:
            chunk_size = 50  # Adjust chunk size as needed.
            self.logger.info(f"Bulk creating {len(contacts_to_create)} contacts in chunks of {chunk_size}...")
            for i in range(0, len(contacts_to_create), chunk_size):
                chunk = contacts_to_create[i:i + chunk_size]
                # Convert each local DB contact to a Xero-friendly payload.
                payloads = [self._convert_contact_to_xero_schema(contact) for contact in chunk]
                try:
                    # Bulk create contacts via the Xero API.
                    api_result = self.xero_api._retry_on_unauthorized(
                        self.xero_api.xero.contacts.put,
                        payloads
                    )
                    if api_result:
                        # Iterate over the chunk and corresponding API result.
                        for db_contact, created_contact in zip(chunk, api_result):
                            new_xero_id = created_contact.get('ContactID')
                            if new_xero_id:
                                try:
                                    self.db_ops.update_contact(db_contact['id'], xero_id=new_xero_id)
                                    self.logger.info(
                                        f"Created and linked new Xero contact for local contact '{db_contact.get('name', '').strip()}' => XeroID={new_xero_id}"
                                    )
                                except IntegrityError as ie:
                                    self.logger.warning(
                                        f"IntegrityError while updating contact '{db_contact.get('name', '').strip()}' with new XeroID {new_xero_id}: {ie}. Skipping update.")
                                # Update our Xero contacts dictionary so subsequent lookups work as expected.
                                xero_contacts_dict[db_contact.get('name', '').strip().lower()] = {
                                    'Name': db_contact.get('name', '').strip(),
                                    'ContactID': new_xero_id
                                }
                            else:
                                self.logger.error(
                                    f"Failed to create Xero contact for local contact '{db_contact.get('name', '').strip()}'.")
                    else:
                        self.logger.error("Bulk creation API call returned no result.")
                except Exception as e:
                    self.logger.error(f"⛔ Exception during bulk creation of contacts: {e}")
        else:
            self.logger.info("No new contacts need to be created in Xero.")

        # Now check for any Xero contacts that are missing from the local DB.
        for name_lower, xero_contact in xero_contacts_dict.items():
            if name_lower not in db_contact_names:
                self.logger.info(
                    f"Xero contact '{xero_contact.get('Name')}' not found in local DB. Creating new local contact record..."
                )
                created_contact = self.db_ops.create_contact(name=xero_contact.get('Name'),
                                                             xero_id=xero_contact.get('ContactID'))
                if created_contact:
                    self.logger.info(
                        f"Added new local contact '{xero_contact.get('Name')}' with XeroID={xero_contact.get('ContactID')}"
                    )
                else:
                    self.logger.error(f"Failed to create local contact for Xero contact '{xero_contact.get('Name')}'.")
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
                # If original xero_id was missing dashes, autocorrect
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
                subset = create_list[i: i + chunk_size]
                chunk_success = self.process_chunk("create", subset)
                success_count += chunk_success
                fail_count += (len(subset) - chunk_success)
        else:
            self.logger.info("🌀 No contacts to create in Xero.")

        # UPDATE
        if update_list:
            for i in range(0, len(update_list), chunk_size):
                subset = update_list[i: i + chunk_size]
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