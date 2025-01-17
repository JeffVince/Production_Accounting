import logging
import time
from xero.exceptions import XeroNotFound, XeroRateLimitExceeded, XeroException
from database_util import DatabaseOperations
from singleton import SingletonMeta
from files_xero.xero_api import xero_api

class XeroServices(metaclass=SingletonMeta):
    """
    XeroServices
    ============
    Provides higher-level operations for working with the XeroAPI and storing
    data in the database via DatabaseOperations.
    """

    def __init__(self):
        """
        Initialize the XeroServices with references to DatabaseOperations
        and XeroAPI instances.
        """
        self.database_util = DatabaseOperations()
        self.xero_api = xero_api
        self.logger = logging.getLogger('xero_logger')
        self.logger.debug('[__init__] - Initialized XeroServices.')
        self._initialized = True

    def load_spend_money_transactions(self, project_id: int=None, po_number: int=None, detail_number: int=None):
        """
        Loads Xero SPEND transactions (bankTransactions) into the 'spend_money' table.

        Args:
            project_id (int, optional): The project ID portion of the reference filter.
            po_number (int, optional): The PO number portion of the reference filter.
            detail_number (int, optional): The detail number portion of the reference filter.

        Usage example:
            database_util = DatabaseOperations()
            xero_api_instance = XeroAPI()
            xero_services = XeroServices(database_util, xero_api_instance)
            xero_services.load_spend_money_transactions(project_id=1234, po_number=101, detail_number=2)
        """
        self.logger.info('[load_spend_money_transactions] - Retrieving SPEND transactions from Xero...')
        xero_spend_transactions = self.xero_api.get_spend_money_by_reference(project_id=project_id, po_number=po_number, detail_number=detail_number)
        if not xero_spend_transactions:
            self.logger.info('[load_spend_money_transactions] - No SPEND transactions returned from Xero for the provided filters.')
            return
        for tx in xero_spend_transactions:
            if tx.get('IsReconciled', False) is True:
                current_state = 'RECONCILED'
            else:
                current_state = tx.get('Status', 'DRAFT')
            reference_number = tx.get('Reference')
            bank_transaction_id = tx.get('BankTransactionID')
            xero_link = f'https://go.xero.com/Bank/ViewTransaction.aspx?bankTransactionID={bank_transaction_id}'
            existing_spend = self.database_util.search_spend_money(column_names=['xero_spend_money_reference_number'], values=[reference_number])
            if not existing_spend:
                created = self.database_util.create_spend_money(xero_spend_money_reference_number=reference_number, xero_link=xero_link, state=current_state)
                if created:
                    self.logger.info(f"[load_spend_money_transactions] - Created new SpendMoney record for reference={reference_number}, ID={created['id']}.")
                else:
                    self.logger.error(f'[load_spend_money_transactions] - Failed to create SpendMoney for reference={reference_number}.')
            else:
                if isinstance(existing_spend, list):
                    existing_spend = existing_spend[0]
                spend_money_id = existing_spend['id']
                updated = self.database_util.update_spend_money(spend_money_id, state=current_state, xero_link=xero_link)
                if updated:
                    self.logger.info(f'[load_spend_money_transactions] - Updated SpendMoney (id={spend_money_id}) for reference={reference_number}.')
                else:
                    self.logger.error(f'[load_spend_money_transactions] - Failed to update SpendMoney (id={spend_money_id}) for reference={reference_number}.')

    def populate_xero_contacts(self):
        """
        Retrieve all contacts from the local DB, retrieve all contacts from Xero,
        compare them, and then perform a single batch update (only for those that need changes).
        """
        self.logger.info('[populate_xero_contacts] - 🚀 Starting to populate Xero contacts from the local DB in a single batch...')
        db_contacts = self.database_util.search_contacts()
        self.logger.info(f'[populate_xero_contacts] - Found {len(db_contacts)} contacts in the local DB to process.')
        self.logger.info('[populate_xero_contacts] - Retrieving all contacts from Xero...')
        try:
            all_xero_contacts = self.xero_api.get_all_contacts()
        except XeroException as xe:
            self.logger.error(f'[populate_xero_contacts] - Failed to retrieve contacts from Xero: {xe}')
            return
        xero_contacts_dict = {contact['Name'].strip().lower(): contact for contact in all_xero_contacts if isinstance(contact.get('Name'), str) and contact.get('Name').strip()}
        contacts_to_update = []
        for db_contact in db_contacts:
            errors = self.validate_xero_data(db_contact)
            if errors:
                self.logger.error(f"[populate_xero_contacts] - Skipping contact '{db_contact.get('name', 'Unnamed')}' due to validation errors: {errors}")
                continue
            contact_name = db_contact['name']
            self.logger.info(f"[populate_xero_contacts] - 🔎 Checking if there's a matching Xero contact for '{contact_name}'")
            xero_match = xero_contacts_dict.get(contact_name.strip().lower())
            if not xero_match:
                msg = f"No matching Xero contact found for: '{contact_name}' ❌"
                self.logger.warning('[populate_xero_contacts] - ' + msg)
                continue
            contact_id = xero_match['ContactID']
            xero_tax_number = xero_match.get('TaxNumber', '') or ''
            xero_addresses = xero_match.get('Addresses', [])
            xero_email = xero_match.get('EmailAddress') or ''
            tax_number = str(db_contact.get('tax_number')) if db_contact.get('tax_number') else ''
            email = db_contact['email']
            if tax_number and len(tax_number) == 9 and tax_number.isdigit():
                formatted_ssn = f'{tax_number[0:3]}-{tax_number[3:5]}-{tax_number[5:]}'
                self.logger.debug(f"[populate_xero_contacts] - Formatting SSN from '{tax_number}' to '{formatted_ssn}' for '{contact_name}'.")
                tax_number = formatted_ssn
            address_data = [{'AddressType': 'STREET', 'AddressLine1': db_contact.get('address_line_1', '') or '', 'AddressLine2': db_contact.get('address_line_2', '') or '', 'City': db_contact.get('city', '') or '', 'PostalCode': db_contact.get('zip', '') or '', 'Region': db_contact.get('region', '') or '', 'Country': db_contact.get('country', '') or ''}, {'AddressType': 'POBOX', 'AddressLine1': db_contact.get('address_line_1', '') or '', 'AddressLine2': db_contact.get('address_line_2', '') or '', 'City': db_contact.get('city', '') or '', 'PostalCode': db_contact.get('zip', '') or '', 'Region': db_contact.get('region', '') or '', 'Country': db_contact.get('country', '') or ''}]
            need_update = False
            if xero_tax_number != tax_number:
                need_update = True
                self.logger.debug(f"[populate_xero_contacts] - Tax number changed for '{contact_name}' from '{xero_tax_number}' to '{tax_number}'.")
            if email != xero_email:
                need_update = True
                self.logger.debug(f"[populate_xero_contacts] - Email changed for '{contact_name}' from '{xero_email}' to '{email}'.")
            if len(xero_addresses) < 2:
                self.logger.debug(f"[populate_xero_contacts] - Xero contact '{contact_name}' has fewer than 2 addresses stored. Triggering update.")
                need_update = True
            else:
                for idx in range(2):
                    old = xero_addresses[idx]
                    new = address_data[idx]
                    for field in ['AddressLine1', 'AddressLine2', 'City', 'PostalCode', 'Country', 'Region']:
                        if old.get(field, '') != new.get(field, ''):
                            self.logger.debug(f"[populate_xero_contacts] - Address {idx} field '{field}' changed for '{contact_name}' from '{old.get(field, '')}' to '{new.get(field, '')}'.")
                            need_update = True
                            break
            if need_update:
                updated_contact_data = {'ContactID': contact_id, 'Name': db_contact['name'], 'Email': email, 'TaxNumber': tax_number, 'Addresses': address_data}
                contacts_to_update.append(updated_contact_data)
            else:
                self.logger.info(f"[populate_xero_contacts] - 🎉  No change needed for '{contact_name}'.")
        if contacts_to_update:
            self.logger.info(f'[populate_xero_contacts] - 💾 Sending a batch update for {len(contacts_to_update)} Xero contacts...')
            try:
                self.xero_api.update_contacts_with_retry(contacts_to_update)
                self.logger.info(f'[populate_xero_contacts] - 🎉 Successfully updated {len(contacts_to_update)} Xero contacts in a single batch.')
            except XeroException as xe:
                self.logger.error(f'[populate_xero_contacts] - XeroException while updating contacts in batch: {xe}')
            except Exception as e:
                self.logger.debug(f'[populate_xero_contacts] - Debugging the exception object: type={type(e)}, repr={repr(e)}')
                error_message = f'⚠️ Error in batch update: {e}'
                self.logger.error('[populate_xero_contacts] - ' + error_message)
        else:
            self.logger.info('[populate_xero_contacts] - No contacts required updating in Xero.')
        self.logger.info('[populate_xero_contacts] - 🏁 Finished populating Xero contacts from the local DB in a single batch.')

    def validate_xero_data(self, db_contact):
        """
        Validate the DB contact for required fields, address formats, etc.
        Returns a list of error messages, empty if no errors.
        """
        errors = []
        if not db_contact.get('name'):
            errors.append('❗ Missing or empty name.')
        address = {'AddressLine1': db_contact.get('address_line_1', ''), 'AddressLine2': db_contact.get('address_line_2', ''), 'City': db_contact.get('city', ''), 'PostalCode': db_contact.get('zip', '')}
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
        self.logger.info(f"[load_bills] - Retrieving ACCPAY invoices from Xero where InvoiceNumber contains '{project_number}'...")
        summaries = self.xero_api.get_acpay_invoices_summary_by_ref(project_number)
        if not summaries:
            self.logger.info(f"[load_bills] - No ACCPAY invoices found with InvoiceNumber containing '{project_number}'.")
            return
        for summary_inv in summaries:
            invoice_id = summary_inv.get('InvoiceID')
            invoice_number = summary_inv.get('InvoiceNumber', '')
            status = summary_inv.get('Status', 'DRAFT')
            self.logger.info(f'[load_bills] - Fetching full details for InvoiceNumber={invoice_number} (ID={invoice_id})...')
            full_inv = self.xero_api.get_invoice_details(invoice_id)
            if not full_inv:
                self.logger.warning(f'[load_bills] - Skipping InvoiceID={invoice_id}, no line items returned.')
                continue
            line_items = full_inv.get('LineItems', [])
            self.logger.debug(f'[load_bills] - Invoice {invoice_number} has {len(line_items)} line item(s).')
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
                    self.logger.warning(f"[load_bills] - InvoiceNumber='{invoice_number}' not in numeric format. Skipping line item match.")
                    continue
            else:
                self.logger.warning(f"[load_bills] - InvoiceNumber='{invoice_number}' doesn't have at least two parts. Skipping line item match.")
                continue
            existing_bill = self.database_util.search_xero_bills(column_names=['xero_reference_number'], values=[invoice_number])
            if not existing_bill:
                created_bill = self.database_util.create_xero_bill(xero_reference_number=invoice_number, xero_id=invoice_id, state=status, project_number=project_num, po_number=po_num, detail_number=detail_num, xero_link=f'https://go.xero.com/AccountsPayable/View.aspx?invoiceId={invoice_id}')
                if not created_bill:
                    self.logger.error(f'[load_bills] - Failed to create xero_bill for {invoice_number}. Skipping line items.')
                    continue
                xero_bill_id = created_bill['id']
            else:
                if isinstance(existing_bill, list):
                    existing_bill = existing_bill[0]
                xero_bill_id = existing_bill['id']
                self.logger.info(f'[load_bills] - Updating existing xero_bill (ID={xero_bill_id}) to status={status}.')
                self.database_util.update_xero_bill(xero_bill_id, xero_id=invoice_id, project_number=project_num, po_number=po_num, detail_number=detail_num, state=status, xero_link=f'https://go.xero.com/AccountsPayable/View.aspx?invoiceId={invoice_id}')
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
                        self.logger.warning(f"[load_bills] - AccountCode '{account_code_str}' is not an integer. Using None.")
                existing_line = self.database_util.search_bill_line_items(column_names=['parent_xero_id'], values=[xero_line_number])
                if existing_line:
                    if isinstance(existing_line, list):
                        existing_line = existing_line[0]
                    bill_line_item_id = existing_line['id']
                    self.logger.info(f'[load_bills] - Updating existing BillLineItem (ID={bill_line_item_id}) with Xero line data.')
                    updated_line = self.database_util.update_bill_line_item(bill_line_item_id, description=description, quantity=quantity, unit_amount=unit_amount, line_amount=line_amount, account_code=account_code)
                    if updated_line:
                        self.logger.debug(f'[load_bills] - BillLineItem (ID={bill_line_item_id}) successfully updated.')
                    else:
                        self.logger.error(f'[load_bills] - Failed to update BillLineItem (ID={bill_line_item_id}).')
                else:
                    self.logger.info(f'[load_bills] - No BillLineItem found for parent_xero_id={xero_line_number}. Creating a new one.')
                    new_line = self.database_util.create_bill_line_item(parent_id=xero_bill_id, parent_xero_id=xero_line_number, project_number=project_num, po_number=po_num, detail_number=detail_num, line_number=idx + 1, description=description, quantity=quantity, unit_amount=unit_amount, line_amount=line_amount, account_code=account_code)
                    if new_line:
                        self.logger.debug(f"[load_bills] - Created BillLineItem with ID={new_line['id']} and parent_xero_id={xero_line_number}.")
                    else:
                        self.logger.error(f'[load_bills] - Failed to create BillLineItem for parent_xero_id={xero_line_number}.')
        self.logger.info(f"[load_bills] - Finished loading ACCPAY invoices matching '{project_number}', with line items mapped to local BillLineItems.")
xero_services = XeroServices()