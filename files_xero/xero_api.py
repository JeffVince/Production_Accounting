# xero_api.py

import os
import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

from dotenv import load_dotenv, set_key
from xero import Xero
from xero.auth import OAuth2Credentials
from xero.constants import XeroScopes
from xero.exceptions import (
    XeroException,
    XeroUnauthorized,
    XeroRateLimitExceeded
)

from database.models import (
    SpendMoney,
    XeroBill,
    BillLineItem,
    DetailItem,
    AccountCode,
    TaxAccount
)
from singleton import SingletonMeta
from database.database_util import DatabaseOperations

# region 🔑 XeroAPI Class
class XeroAPI(metaclass=SingletonMeta):
    """
    Encapsulates interactions with Xero using PyXero.
    """

    # region 🏗 Initialization
    def __init__(self):
        """
        Initialize XeroAPI with environment variables and set up the Xero client.
        """
        try:
            env_path = os.path.join(os.path.dirname(__file__), '..', '.env')
            load_dotenv(env_path)
        except Exception as e:
            logging.getLogger('xero_logger').error(
                f'🚨 Could not load .env: {e}'
            )

        self.client_id = os.getenv('XERO_CLIENT_ID')
        self.client_secret = os.getenv('XERO_CLIENT_SECRET')
        self.access_token = os.getenv('XERO_ACCESS_TOKEN')
        self.refresh_token = os.getenv('XERO_REFRESH_TOKEN')
        self.tenant_id = os.getenv('XERO_TENANT_ID')
        self.db_ops = DatabaseOperations()
        self.scope = (
            os.getenv('XERO_SCOPE')
            or 'accounting.contacts accounting.settings '
               'accounting.transactions offline_access'
        )

        # Set up logging
        self.logger = logging.getLogger('xero_logger')
        self.logger.setLevel(logging.DEBUG)

        current_time = time.time()
        default_expires_in = 1800  # 30 minutes
        token_dict = {
            'access_token': self.access_token,
            'refresh_token': self.refresh_token,
            'expires_in': default_expires_in,
            'expires_at': current_time + default_expires_in,
            'token_type': 'Bearer',
            'scope': self.scope.split()
        }

        # Create credentials
        self.credentials = OAuth2Credentials(
            client_id=self.client_id,
            client_secret=self.client_secret,
            scope=self.scope.split(),
            token=token_dict
        )
        if self.tenant_id:
            self.credentials.tenant_id = self.tenant_id

        # Create the Xero client
        self.xero = Xero(self.credentials)

        # Force refresh check
        self._refresh_token_if_needed()

        self.logger.info(
            '[__init__] - 🚀 XeroAPI (PyXero) initialized successfully!'
        )
        self._initialized = True
    # endregion

    # region 🔒 Token Refresh
    def _refresh_token_if_needed(self, force=False):
        """
        Refresh the Xero token if it’s expired or about to expire.
        If 'force=True', we attempt a refresh no matter what.
        """
        if not force and not self.credentials.expired():
            self.logger.debug(
                '[_refresh_token_if_needed] - 🔑 Token still valid, no refresh.'
            )
            return

        try:
            self.logger.debug(
                '[_refresh_token_if_needed] - 🔑 Token expired or '
                'force-refresh requested; refreshing now...'
            )
            self.credentials.refresh()
            self.logger.info(
                '[_refresh_token_if_needed] - 🔄 Successfully refreshed '
                'Xero tokens with PyXero! ✅'
            )

            # If no tenant set, pick the first available
            if not self.credentials.tenant_id:
                tenants = self.credentials.get_tenants()
                if tenants:
                    self.credentials.tenant_id = tenants[0]['tenantId']
                    self.logger.info(
                        f'[_refresh_token_if_needed] - '
                        f'🏢 Tenant set to {self.credentials.tenant_id}'
                    )
                else:
                    self.logger.warning(
                        '[_refresh_token_if_needed] - ⚠️ No tenants found.'
                    )

            new_token = self.credentials.token
            os.environ['XERO_ACCESS_TOKEN'] = new_token['access_token']
            os.environ['XERO_REFRESH_TOKEN'] = new_token['refresh_token']

            env_path = os.path.join(os.path.dirname(__file__), '..', '.env')
            set_key(env_path, 'XERO_ACCESS_TOKEN', new_token['access_token'])
            set_key(env_path, 'XERO_REFRESH_TOKEN', new_token['refresh_token'])

            self.xero = Xero(self.credentials)

        except XeroException as e:
            self.logger.error(
                '[_refresh_token_if_needed] - ❌ Encountered XeroException '
                f'during token refresh. Possibly need re-auth. Error: {str(e)}'
            )
            raise e

    def _retry_on_unauthorized(self, func, *args, **kwargs):
        """
        Call a PyXero function with retries if:
         - XeroUnauthorized occurs (token expired/invalid),
         - XeroRateLimitExceeded occurs (429 rate limit),
         - or other recoverable XeroExceptions.
        We retry up to 3 times, sleeping if rate-limited.
        """
        max_retries = 3
        for attempt in range(1, max_retries + 1):
            try:
                self.logger.debug(
                    f'[_retry_on_unauthorized] - 🔄 Attempt {attempt} => {func.__name__}'
                )
                return func(*args, **kwargs)
            except XeroUnauthorized:
                self.logger.warning(
                    '[_retry_on_unauthorized] - ⚠️ XeroUnauthorized, '
                    'attempting force-refresh...'
                )
                self._refresh_token_if_needed(force=True)
            except XeroRateLimitExceeded:
                self.logger.warning(
                    '[_retry_on_unauthorized] - 🔃 Rate limit hit. '
                    f'Attempt {attempt} of {max_retries}. Sleeping 65s...'
                )
                time.sleep(65)
            except XeroException as e:
                self.logger.error(
                    f'[_retry_on_unauthorized] - ❌ XeroException: {str(e)}'
                )
                raise e

        self.logger.error(
            '[_retry_on_unauthorized] - ❌ Failed Xero API call after retries.'
        )
        return None
    # endregion

    # region 🛠 Utility
    def _get_tax_code_for_detail_item(self, session, detail_item: DetailItem) -> str:
        """
        Retrieve the tax code from the DB (AccountCode -> TaxAccount).
        Falls back to "TAX001" if anything goes wrong or if not found.
        """
        tax_code = 'TAX001'
        try:
            acct_code = session.query(AccountCode).filter_by(
                id=detail_item.account_code_id
            ).first()
            if acct_code and acct_code.tax_account:
                tax_code = acct_code.tax_account.tax_code
        except Exception as e:
            self.logger.warning(
                f'[_get_tax_code_for_detail_item] - ⚠️ Error retrieving tax code '
                f'for DetailItem ID {detail_item.id}: {str(e)}'
            )
        return tax_code

    def _convert_detail_item_to_line_item(self, session, detail_item: DetailItem) -> dict:
        """
        Convert a DetailItem record to a dict suitable for PyXero's
        bankTransactions or invoices.
        """
        try:
            tax_code = self._get_tax_code_for_detail_item(session, detail_item)
            return {
                'Description': detail_item.description or 'No description',
                'Quantity': float(detail_item.quantity),
                'UnitAmount': float(detail_item.rate),
                'TaxType': tax_code
            }
        except Exception as e:
            self.logger.error(
                f'[_convert_detail_item_to_line_item] - 💥 Failed to convert '
                f'DetailItem(ID={detail_item.id}) to line item: {str(e)}'
            )
            return {
                'Description': 'Conversion error',
                'Quantity': 1.0,
                'UnitAmount': 0.0,
                'TaxType': 'TAX001'
            }
    # endregion

    # region 💸 Spend Money
    def create_spend_money(self, session, detail_item_id: int):
        """
        Create a SPEND bank transaction in Xero based on a local DetailItem.
        """
        self._refresh_token_if_needed()
        try:
            detail_item = session.query(DetailItem).get(detail_item_id)
        except Exception as e:
            self.logger.error(
                f'[create_spend_money] - ❌ DB error retrieving DetailItem '
                f'(id={detail_item_id}): {str(e)}'
            )
            detail_item = None

        if not detail_item:
            self.logger.info(
                '[create_spend_money] - 🗒️ Detail item not found, creating '
                'VOIDED spend money record.'
            )
            return self._create_voided_spend_money()

        if detail_item.state == 'SUBMITTED':
            xero_status = 'DRAFT'
        elif detail_item.state == 'REVIEWED':
            xero_status = 'AUTHORISED'
        else:
            xero_status = 'VOIDED'

        try:
            line_dict = self._convert_detail_item_to_line_item(session, detail_item)
            new_tx = {
                'Type': 'SPEND',
                'Contact': {
                    'Name': detail_item.vendor or 'Unknown Vendor'
                },
                'LineItems': [line_dict],
                'Status': xero_status
            }

            self.logger.info(
                '[create_spend_money] - 💸 Creating spend money transaction in Xero...'
            )

            created = self._retry_on_unauthorized(
                self.xero.banktransactions.put, [new_tx]
            )
            self.logger.debug(
                f'[create_spend_money] - 🔍 Xero response: {created}'
            )
            return created

        except XeroException as e:
            self.logger.error(
                f'[create_spend_money] - ❌ Failed to create spend money: {str(e)}'
            )
            return None
        except Exception as e:
            self.logger.error(
                f'[create_spend_money] - 💥 Unexpected error: {str(e)}'
            )
            return None

    def update_spend_money(self, session, xero_spend_money_id: str, new_state: str):
        """
        Update an existing spend money transaction's status in Xero.
        """
        self._refresh_token_if_needed()
        self.logger.info(
            f'[update_spend_money] - 🔄 Updating spend money ID={xero_spend_money_id} '
            f'to {new_state}...'
        )
        try:
            existing_list = self._retry_on_unauthorized(
                self.xero.banktransactions.filter,
                BankTransactionID=xero_spend_money_id
            )
            if not existing_list:
                self.logger.warning(
                    f'[update_spend_money] - ⚠️ No bank transaction found with '
                    f'ID {xero_spend_money_id}'
                )
                return None

            bank_tx = existing_list[0]
            bank_tx['Status'] = new_state

            updated = self._retry_on_unauthorized(
                self.xero.banktransactions.save, bank_tx
            )
            self.logger.debug(
                f'[update_spend_money] - 🔍 Updated spend money: {updated}'
            )
            return updated

        except XeroException as e:
            self.logger.error(
                f'[update_spend_money] - ❌ Failed to update spend money: {str(e)}'
            )
            return None
        except Exception as e:
            self.logger.error(
                f'[update_spend_money] - 💥 Unexpected error: {str(e)}'
            )
            return None

    def _create_voided_spend_money(self):
        """
        Helper to create a 'VOIDED' spend money transaction in Xero
        when the detail item is missing or invalid.
        """
        voided_tx = {
            'Type': 'SPEND',
            'Contact': {'Name': 'Unknown Vendor'},
            'LineItems': [],
            'Status': 'VOIDED'
        }
        try:
            response = self._retry_on_unauthorized(
                self.xero.banktransactions.put, [voided_tx]
            )
            return response
        except XeroException as e:
            self.logger.error(
                f'[_create_voided_spend_money] - ❌ Failed to create voided '
                f'spend money: {str(e)}'
            )
            return None
        except Exception as e:
            self.logger.error(
                f'[_create_voided_spend_money] - 💥 Unexpected error: {str(e)}'
            )
            return None
    # endregion

    # region 💼 Bills
    def create_bill(
            self,
            session,
            project_id: int,
            po_number: int,
            detail_number: int
    ):
        """
        Create a Bill (Invoice type='ACCPAY') in Xero for a specific
        project/PO/detail combo.
        """
        self._refresh_token_if_needed()
        try:
            detail_items = (
                session.query(DetailItem)
                    .filter(DetailItem.line_number == detail_number)
                    .all()
            )
        except Exception as e:
            self.logger.error(
                f'[create_bill] - ❌ DB error loading DetailItems '
                f'(detail_number={detail_number}): {str(e)}'
            )
            detail_items = []

        if not detail_items:
            self.logger.warning(
                '[create_bill] - ⚠️ No detail items found; creating empty Bill.'
            )
            detail_items = []

        # Convert detail items to Xero-compatible line items
        xero_line_items = []
        for di in detail_items:
            try:
                xero_line_items.append(
                    self._convert_detail_item_to_line_item(session, di)
                )
            except Exception as ex:
                self.logger.error(
                    f'[create_bill] - 💥 Error converting detail_item to line_item: '
                    f'{str(ex)}'
                )

        # Determine the Bill status from the detail items' states
        states = {di.state for di in detail_items}
        if len(states) == 1:
            only_state = list(states)[0]
            if only_state == 'SUBMITTED':
                xero_status = 'DRAFT'
            elif only_state == 'RTP':
                xero_status = 'SUBMITTED'
            elif only_state == 'PAID':
                xero_status = 'PAID'
            else:
                xero_status = 'DRAFT'
        else:
            xero_status = 'DRAFT'

        # -------------------------------------------
        # 🏷  Attempt to find contact name from DB
        # -------------------------------------------
        # 1) If the first detail item has a contact_id, try to look up a matching contact in DB.
        contact_name_from_db = None
        contact_xero_id_from_db = None
        if detail_items and hasattr(detail_items[0], "contact_id"):
            contact_id = detail_items[0].contact_id
            if contact_id:
                self.logger.debug(
                    f'[create_bill] - Attempting to find contact by contact_id={contact_id}'
                )

                found_contact = self.db_ops.search_contacts(["id"], [contact_id])
                if found_contact and not isinstance(found_contact, list):
                    contact_name_from_db = found_contact["name"]
                    contact_xero_id_from_db = found_contact["xero_id"]

        # 2) If we didn't find a DB contact name, fallback to vendor in detail_item or "Unknown Vendor."
        if contact_xero_id_from_db:
            vendor_name = contact_name_from_db
        else:
           return "Failed, due to not Xero_ID in Contact"

        reference = f'{project_id}_{po_number}_{detail_number}'

        new_invoice = {
            'Type': 'ACCPAY',
            'Contact': {'Name': vendor_name, 'ContactID': contact_xero_id_from_db},
            'LineItems': xero_line_items,
            'Reference': reference,
            'Status': xero_status
        }

        self.logger.info(
            f'[create_bill] - 💼 Creating Xero bill for reference {reference}...'
        )
        try:
            created_invoice = self._retry_on_unauthorized(
                self.xero.invoices.put, [new_invoice]
            )
            self.logger.debug(
                f'[create_bill] - 🔍 Xero create response: {created_invoice}'
            )
            return created_invoice

        except XeroException as e:
            self.logger.error(
                f'[create_bill] - ❌ Failed to create ACCPAY bill: {str(e)}'
            )
            return None
        except Exception as e:
            self.logger.error(
                f'[create_bill] - 💥 Unexpected error creating Xero bill: {str(e)}'
            )
            return None

    def update_bill_status(self, invoice_id: str, new_status: str):
        """
        Update the status of an existing Bill (ACCPAY Invoice) in Xero.
        """
        self._refresh_token_if_needed()
        self.logger.info(
            f'[update_bill_status] - 🔄 Updating bill (invoice_id={invoice_id}) '
            f'to {new_status}...'
        )

        try:
            existing_list = self._retry_on_unauthorized(
                self.xero.invoices.filter,
                InvoiceID=invoice_id
            )
            if not existing_list:
                self.logger.warning(
                    f'[update_bill_status] - ⚠️ No invoice found with ID={invoice_id}'
                )
                return None

            invoice_obj = existing_list[0]
            invoice_obj['Status'] = new_status
            updated_invoices = self._retry_on_unauthorized(
                self.xero.invoices.save, invoice_obj
            )
            self.logger.debug(
                f'[update_bill_status] - 🔍 Updated invoice: {updated_invoices}'
            )
            return updated_invoices

        except XeroException as e:
            self.logger.error(
                f'[update_bill_status] - ❌ Failed to update bill: {str(e)}'
            )
            return None
        except Exception as e:
            self.logger.error(
                f'[update_bill_status] - 💥 Unexpected error updating bill: {str(e)}'
            )
            return None
    # endregion

    # region 📃 Bill Retrieval
    def get_all_bills(self):
        """
        Retrieve all Invoices from Xero of Type='ACCPAY' (including line items),
        excluding any with Status == "DELETED".
        """
        self._refresh_token_if_needed()
        self.logger.info(
            '[get_all_bills] - 📄 Retrieving all ACCPAY invoices from Xero.'
        )

        all_invoices_summary = []
        page_number = 1
        page_size = 100

        # Step 1: Collect summaries via paging
        while True:
            self.logger.debug(
                f'[get_all_bills] - 🔎 Fetching page {page_number} of ACCPAY...'
            )
            filter_str = 'Type=="ACCPAY"'
            invoices_page = self._retry_on_unauthorized(
                self.xero.invoices.filter,
                raw=filter_str,
                page=page_number
            )
            if not invoices_page:
                self.logger.debug(
                    f'[get_all_bills] - ⏹️ No invoices found on page {page_number}.'
                )
                break

            all_invoices_summary.extend(invoices_page)
            self.logger.debug(
                f'[get_all_bills] - ✅ Retrieved {len(invoices_page)} invoice(s) '
                f'on page {page_number}.'
            )
            if len(invoices_page) < page_size:
                break

            page_number += 1

        if not all_invoices_summary:
            self.logger.info(
                '[get_all_bills] - ℹ️ No ACCPAY invoices found in Xero at all.'
            )
            return []

        self.logger.info(
            f'[get_all_bills] - 🔎 Fetched {len(all_invoices_summary)} summaries. '
            'Retrieving full details...'
        )

        detailed_invoices = []
        for summary_inv in all_invoices_summary:
            if summary_inv.get('Status') == 'DELETED':
                self.logger.debug(
                    f"[get_all_bills] - ⏩ Skipping DELETED invoice "
                    f"{summary_inv.get('InvoiceID')}"
                )
                continue

            invoice_id = summary_inv.get('InvoiceID')
            if not invoice_id:
                self.logger.warning(
                    '[get_all_bills] - ⚠️ Summary is missing InvoiceID; skipping.'
                )
                continue

            full_inv_list = self._retry_on_unauthorized(
                self.xero.invoices.get, invoice_id
            )
            if not full_inv_list:
                self.logger.warning(
                    f'[get_all_bills] - ⚠️ No detailed invoice for ID={invoice_id}.'
                )
                continue

            detailed_inv = full_inv_list[0]
            if detailed_inv.get('Status') == 'DELETED':
                self.logger.debug(
                    f'[get_all_bills] - ⏩ Skipping DELETED invoice {invoice_id}.'
                )
                continue

            detailed_invoices.append(detailed_inv)

        self.logger.info(
            f'[get_all_bills] - ✅ Finished retrieving {len(detailed_invoices)} '
            'detailed ACCPAY invoices (excluding DELETED).'
        )
        return detailed_invoices

    def get_acpay_invoices_summary_by_ref(self, reference_substring: str) -> list:
        """
        Retrieve a *summary* of ACCPAY (bills) from Xero whose InvoiceNumber
        or Reference contains the given substring. Does NOT include line items,
        excludes any with Status == "DELETED".
        """
        self._refresh_token_if_needed()
        raw_filter = (
            'Type=="ACCPAY" AND InvoiceNumber!=null '
            f'&& InvoiceNumber.Contains("{reference_substring}")'
        )
        self.logger.info(
            '[get_acpay_invoices_summary_by_ref] - 🔎 Fetching summary for ACCPAY '
            f'invoices that match {reference_substring} in InvoiceNumber.'
        )

        page_number = 1
        page_size = 100
        all_summaries = []

        while True:
            self.logger.debug(
                f'[get_acpay_invoices_summary_by_ref] - 🔍 Requesting page '
                f'{page_number} with filter: {raw_filter}'
            )
            current_page = self._retry_on_unauthorized(
                self.xero.invoices.filter,
                raw=raw_filter,
                page=page_number
            )
            if not current_page:
                break

            filtered_page = [
                inv for inv in current_page
                if inv.get('Status') != 'DELETED'
            ]
            all_summaries.extend(filtered_page)

            if len(current_page) < page_size:
                break
            page_number += 1

        self.logger.info(
            f'[get_acpay_invoices_summary_by_ref] - ✅ Found {len(all_summaries)} '
            'ACCPAY invoice summaries (excluding DELETED).'
        )
        return all_summaries

    def get_invoice_details(self, invoice_id: str) -> dict:
        """
        Retrieves the *full* invoice (with line items) by InvoiceID.
        Returns a dict if found and not DELETED; otherwise None.
        """
        self._refresh_token_if_needed()
        self.logger.debug(
            f'[get_invoice_details] - 🔎 Fetching invoice for ID={invoice_id}'
        )

        invoice_list = self._retry_on_unauthorized(
            self.xero.invoices.get, invoice_id
        )
        if not invoice_list:
            self.logger.warning(
                f'[get_invoice_details] - ⚠️ No detailed invoice for ID={invoice_id}.'
            )
            return None

        full_invoice = invoice_list[0]
        if full_invoice.get('Status') == 'DELETED':
            self.logger.debug(
                f'[get_invoice_details] - ⏩ Invoice {invoice_id} is DELETED.'
            )
            return None

        return full_invoice
    # endregion

    # region 🆕 GET BILLS BY REFERENCE
    def get_bills_by_reference(self, reference_str: str) -> list:
        """
        Fetch ACCPAY invoices in Xero by matching EXACT 'Reference'
        to 'reference_str'. Excludes DELETED. Returns a list of partial
        invoice objects (not guaranteed to have line items).
        """
        self._refresh_token_if_needed()
        self.logger.info(
            '[get_bills_by_reference] - 🔎 Searching Xero for ACCPAY invoices '
            f'with Reference="{reference_str}"...'
        )

        try:
            raw_filter = (
                'Type=="ACCPAY" AND Reference!=null '
                f'AND Reference=="{reference_str}"'
            )
            invoices = self._retry_on_unauthorized(
                self.xero.invoices.filter, raw=raw_filter
            )
            if not invoices:
                self.logger.info(
                    f'[get_bills_by_reference] - ℹ️ No invoices found for '
                    f'Reference="{reference_str}".'
                )
                return []

            results = [
                inv for inv in invoices
                if inv.get('Status') != 'DELETED'
            ]
            self.logger.debug(
                f'[get_bills_by_reference] - ✅ Found {len(results)} '
                'ACCPAY invoice(s) with that reference.'
            )
            return results

        except XeroException as e:
            self.logger.error(
                f'[get_bills_by_reference] - ❌ Failed to retrieve bills: {str(e)}'
            )
            return []
        except Exception as e:
            self.logger.error(
                f'[get_bills_by_reference] - 💥 Unexpected error: {str(e)}'
            )
            return []
    # endregion

    # region 👥 Contacts
    def get_contact_by_name(self, name: str):
        """
        Retrieve a Xero contact by name. Returns the first match
        if multiple are found, or None if none exist.
        """
        self._refresh_token_if_needed()
        self.logger.info(
            f'[get_contact_by_name] - 🔎 Searching contact by name: {name}'
        )
        try:
            results = self._retry_on_unauthorized(
                self.xero.contacts.filter, Name=name
            )
            if results:
                self.logger.debug(
                    f"[get_contact_by_name] - ✅ Found contact(s) for '{name}': {results}"
                )
                return results[0]
            self.logger.info(
                f"[get_contact_by_name] - ℹ️ No matching contact for '{name}' in Xero."
            )
            return None
        except XeroException as e:
            self.logger.error(
                f"[get_contact_by_name] - ❌ Error retrieving contact '{name}': {str(e)}"
            )
            return None
        except Exception as e:
            self.logger.error(
                f'[get_contact_by_name] - 💥 Unexpected error: {str(e)}'
            )
            return None

    def get_all_contacts(self):
        """
        Fetch all contacts from Xero. Could be large; consider pagination
        or filtering if performance is a concern.
        """
        self._refresh_token_if_needed()
        self.logger.info('[get_all_contacts] - 📇 Fetching all contacts...')
        try:
            contacts = self._retry_on_unauthorized(
                self.xero.contacts.all
            )
            if not contacts:
                self.logger.info(
                    '[get_all_contacts] - ℹ️ No contacts found in Xero.'
                )
                return []
            self.logger.debug(
                f'[get_all_contacts] - ✅ Retrieved {len(contacts)} contacts.'
            )
            return contacts
        except XeroException as e:
            self.logger.error(
                f'[get_all_contacts] - ❌ Error retrieving all contacts: {str(e)}'
            )
            return []
        except Exception as e:
            self.logger.error(
                f'[get_all_contacts] - 💥 Unexpected error: {str(e)}'
            )
            return []

    def create_contact(self, contact_data: dict):
        """
        Create a new contact in Xero using the provided contact_data.
        """
        self._refresh_token_if_needed()
        self.logger.info(
            f'[create_contact] - 👤 Creating new Xero contact: {contact_data}'
        )
        try:
            created = self._retry_on_unauthorized(
                self.xero.contacts.put, [contact_data]
            )
            self.logger.debug(
                f'[create_contact] - ✅ Created contact: {created}'
            )
            return created
        except XeroException as e:
            self.logger.error(
                f'[create_contact] - ❌ Failed to create contact: {str(e)}'
            )
            return None
        except Exception as e:
            self.logger.error(
                f'[create_contact] - 💥 Unexpected error: {str(e)}'
            )
            return None

    def update_contact(self, contact_data: dict):
        """
        Update an existing contact in Xero. Must include 'ContactID'.
        """
        cid = contact_data.get('ContactID')
        self.logger.info(
            f'[update_contact] - 🔄 Updating contact with ID={cid}'
        )
        self._refresh_token_if_needed()
        try:
            updated = self._retry_on_unauthorized(
                self.xero.contacts.save, contact_data
            )
            if not updated:
                self.logger.error(
                    '[update_contact] - ❌ Empty response from Xero.'
                )
                return None
            self.logger.debug(
                f'[update_contact] - 🔍 Updated contact data: {updated}'
            )
            return updated
        except XeroException as e:
            self.logger.error(
                f'[update_contact] - ❌ XeroException updating contact: {str(e)}'
            )
            return None
        except Exception as e:
            self.logger.error(
                f'[update_contact] - 💥 Unexpected error: {str(e)}'
            )
            return None

    def update_contact_with_retry(
        self,
        contact_data,
        max_retries=3
    ):
        """
        Attempts to update a Xero contact, retrying on rate-limit errors.
        """
        cid = contact_data.get('ContactID')
        self.logger.info(
            f'[update_contact_with_retry] - 🔄 Updating contact (ID={cid}) '
            f'with up to {max_retries} retries.'
        )
        self._refresh_token_if_needed()

        for attempt in range(1, max_retries + 1):
            try:
                updated = self.xero.contacts.save(contact_data)
                return updated
            except XeroRateLimitExceeded:
                self.logger.warning(
                    f'[update_contact_with_retry] - 🔃 Rate limit. '
                    f'Attempt {attempt} of {max_retries}. Sleeping 65s...'
                )
                time.sleep(65)
            except XeroException as xe:
                self.logger.error(
                    f'[update_contact_with_retry] - ❌ XeroException: {xe}'
                )
                return None
            except Exception as e:
                self.logger.error(
                    f'[update_contact_with_retry] - 💥 Unexpected error: {str(e)}'
                )
                return None

        self.logger.error(
            '[update_contact_with_retry] - ❌ Failed after multiple retries.'
        )
        return None

    def update_contacts_with_retry(
        self,
        contacts_data: list[dict],
        max_retries=3
    ):
        """
        Attempts to update multiple Xero contacts in batch,
        retrying if rate-limited.
        """
        self.logger.info(
            f'[update_contacts_with_retry] - 🔄 Batch update of '
            f'{len(contacts_data)} contact(s), up to {max_retries} retries.'
        )
        self._refresh_token_if_needed()

        for attempt in range(1, max_retries + 1):
            try:
                updated = self.xero.contacts.save(contacts_data)
                return updated
            except XeroRateLimitExceeded:
                self.logger.warning(
                    f'[update_contacts_with_retry] - 🔃 Rate limit (batch). '
                    f'Attempt {attempt} of {max_retries}. Sleeping 65s...'
                )
                time.sleep(65)
            except XeroException as xe:
                self.logger.error(
                    f'[update_contacts_with_retry] - ❌ XeroException: {xe}'
                )
                return None
            except Exception as e:
                self.logger.error(
                    f'[update_contacts_with_retry] - 💥 Unexpected: {str(e)}'
                )
                return None

        self.logger.error(
            '[update_contacts_with_retry] - ❌ Failed after multiple retries.'
        )
        return None
    # endregion

    # region 🏎 Concurrency Example
    def create_spend_money_in_batch(
        self,
        session,
        detail_item_ids: list[int]
    ):
        """
        Example of concurrency to create multiple spend money items in parallel.
        """
        self.logger.info(
            f'[create_spend_money_in_batch] - 🏎 Creating SPEND money in batch '
            f'for detail_item_ids={detail_item_ids}'
        )
        futures = []
        results = []

        with ThreadPoolExecutor(max_workers=5) as executor:
            for detail_item_id in detail_item_ids:
                futures.append(
                    executor.submit(
                        self.create_spend_money, session, detail_item_id
                    )
                )
            for future in as_completed(futures):
                try:
                    result = future.result()
                    results.append(result)
                except Exception as e:
                    self.logger.error(
                        '[create_spend_money_in_batch] - 💥 Exception in thread '
                        f'for detail_item_id: {str(e)}'
                    )

        self.logger.info(
            f'[create_spend_money_in_batch] - ✅ Batch done. '
            f'Results length={len(results)}'
        )
        return results
    # endregion

# endregion

# Create a global instance
xero_api = XeroAPI()