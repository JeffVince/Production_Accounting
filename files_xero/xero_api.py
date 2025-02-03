# region 🚀 Imports & Setup
import os
import time
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

from dotenv import load_dotenv
from xero.auth import OAuth2Credentials
from xero.exceptions import (
    XeroException,
    XeroUnauthorized,
    XeroRateLimitExceeded
)

from database.models import DetailItem
from utilities.singleton import SingletonMeta

# Configure a logger for Xero operations
logging.getLogger('xero_logger').setLevel(logging.DEBUG)

# endregion

# region 🏦 XeroAPI Singleton Class
class XeroAPI(metaclass=SingletonMeta):
    """
    Minimal Xero API client that handles token refresh, direct calls,
    and new methods to create/update SPEND transactions for improved functionality.
    """

    # region 1️⃣ Initialization & Environment
    def __init__(self):
        """
        Initialize the XeroAPI client:
          - Load environment variables
          - Build OAuth2 credentials
          - Create Xero client
          - Refresh token if needed
        """
        try:
            env_path = os.path.join(os.path.dirname(__file__), '..', '.env')
            load_dotenv(env_path)
        except Exception as e:
            logging.getLogger('xero_logger').warning(
                f'🚨 Could not load .env: {e}'
            )

        # Retrieve config from env
        self.client_id = os.getenv('XERO_CLIENT_ID')
        self.client_secret = os.getenv('XERO_CLIENT_SECRET')
        self.access_token = os.getenv('XERO_ACCESS_TOKEN')
        self.refresh_token = os.getenv('XERO_REFRESH_TOKEN')
        self.tenant_id = os.getenv('XERO_TENANT_ID')
        self.scope = (
            os.getenv('XERO_SCOPE') or
            'accounting.contacts accounting.settings accounting.transactions offline_access'
        )

        # Logger for Xero operations
        self.logger = logging.getLogger('xero_logger')

        # Prepare a default token dict
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

        # Create OAuth2 credentials & Xero client
        self.credentials = OAuth2Credentials(
            client_id=self.client_id,
            client_secret=self.client_secret,
            scope=self.scope.split(),
            token=token_dict
        )
        if self.tenant_id:
            self.credentials.tenant_id = self.tenant_id

        from xero import Xero  # Re-import for clarity
        self.xero = Xero(self.credentials)
        self._refresh_token_if_needed()

        self.logger.info('🚀 - XeroAPI initialized.')
        self._initialized = True

    # endregion

    # region 2️⃣ Token Refresh & Retry Logic
    def _refresh_token_if_needed(self, force=False):
        # If not forced and token isn't expired, skip
        if not force and not self.credentials.expired():
            self.logger.debug('[XeroAPI] Token still valid, no refresh needed.')
            return

        self.logger.debug('[XeroAPI] 🔑 Refreshing token...')
        try:
            self.credentials.refresh()
            self.logger.info('[XeroAPI] 🔄 Token refresh successful!')

            # If your code sets tenant explicitly, do so after refreshing:
            if not self.credentials.tenant_id:
                tenants = self.credentials.get_tenants()
                if tenants:
                    self.credentials.tenant_id = tenants[0]['tenantId']
                    self.logger.debug(f'[XeroAPI] 🏢 Tenant set to {self.credentials.tenant_id}')

            # Re-init the Xero client with the new token
            from xero import Xero
            self.xero = Xero(self.credentials)

            # Optional: store tokens in env (though not strictly needed inside the same process)
            new_token = self.credentials.token
            os.environ['XERO_ACCESS_TOKEN'] = new_token.get('access_token', '')
            os.environ['XERO_REFRESH_TOKEN'] = new_token.get('refresh_token', '')

        except XeroException as e:
            self.logger.error(f'[XeroAPI] ❌ Error refreshing token: {e}')
            raise e

    def _retry_on_unauthorized(self, func, *args, **kwargs):
        """
        Retry logic for certain Xero exceptions (Unauthorized, RateLimit, etc.).
        """
        max_retries = 3
        for attempt in range(1, max_retries + 1):
            try:
                self.logger.debug(f'[XeroAPI] Attempt {attempt} => {func.__name__}')
                return func(*args, **kwargs)
            except XeroUnauthorized:
                self.logger.warning('[XeroAPI] ⚠️ Unauthorized, attempting force-refresh.')
                self._refresh_token_if_needed(force=True)
            except XeroRateLimitExceeded:
                self.logger.warning(f'[XeroAPI] 🔃 Rate limit on attempt {attempt}, sleeping 65s...')
                time.sleep(65)
            except XeroException as e:
                self.logger.error(f'[XeroAPI] ❌ XeroException: {e}')
                raise e

        self.logger.error('[XeroAPI] ❌ Failed Xero API call after max retries.')
        return None

    # endregion

    # region 3️⃣ Utility Methods (TaxCode, Conversion, etc.)
    def _get_tax_code_for_detail_item(self, session, detail_item: DetailItem) -> str:
        tax_code = 'TAX001'
        try:
            from database.models import AccountCode  # local import to avoid cyclical references
            acct_code = session.query(AccountCode).filter_by(
                id=detail_item.account_code_id
            ).first()
            if acct_code and acct_code.tax_account:
                tax_code = acct_code.tax_account.tax_code
        except Exception as e:
            self.logger.warning(
                f'[_get_tax_code_for_detail_item] [XeroAPI - detail_item {detail_item.id}] ⚠️ - Error retrieving tax code: {str(e)}'
            )
        return tax_code

    def _convert_detail_item_to_line_item(self, session, detail_item: DetailItem) -> dict:
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
                f'[_convert_detail_item_to_line_item] [XeroAPI - detail_item {detail_item.id}] 💥 - Conversion failed: {str(e)}'
            )
            return {
                'Description': 'Conversion error',
                'Quantity': 1.0,
                'UnitAmount': 0.0,
                'TaxType': 'TAX001'
            }

    # endregion

    # region 4️⃣ Contacts
    def get_contact_by_name(self, name: str):
        self._refresh_token_if_needed()
        function_name = 'get_contact_by_name'
        self.logger.info(f'[{function_name}] [XeroAPI - contact {name}] 🔎 - Searching by name...')
        try:
            results = self._retry_on_unauthorized(self.xero.contacts.filter, Name=name)
            if results:
                self.logger.debug(f'[{function_name}] [XeroAPI - contact {name}] ✅ - Found contact(s): {results}')
                return results[0]
            self.logger.info(f'[{function_name}] [XeroAPI - contact {name}] ℹ️ - No match in Xero.')
            return None
        except XeroException as e:
            self.logger.error(f'[{function_name}] [XeroAPI - contact {name}] ❌ - XeroException: {str(e)}')
            return None
        except Exception as e:
            self.logger.error(f'[{function_name}] [XeroAPI - contact {name}] 💥 - Unexpected: {str(e)}')
            return None

    def get_all_contacts(self):
        self._refresh_token_if_needed()
        function_name = 'get_all_contacts'
        self.logger.info(f'[{function_name}] [XeroAPI] 📇 - Fetching all contacts...')
        try:
            contacts = self._retry_on_unauthorized(self.xero.contacts.all)
            if not contacts:
                self.logger.info(f'[{function_name}] [XeroAPI] ℹ️ - No contacts found.')
                return []
            self.logger.debug(f'[{function_name}] [XeroAPI] ✅ - Retrieved {len(contacts)} contacts.')
            return contacts
        except XeroException as e:
            self.logger.error(f'[{function_name}] [XeroAPI] ❌ - XeroException retrieving all contacts: {str(e)}')
            return []
        except Exception as e:
            self.logger.error(f'[{function_name}] [XeroAPI] 💥 - Unexpected: {str(e)}')
            return []

    def create_contact(self, contact_data: dict):
        self._refresh_token_if_needed()
        function_name = 'create_contact'
        self.logger.info(f'[{function_name}] [XeroAPI] 👤 - Creating new contact: {contact_data}')
        try:
            created = self._retry_on_unauthorized(self.xero.contacts.put, [contact_data])
            self.logger.debug(f'[{function_name}] [XeroAPI] ✅ - Created contact: {created}')
            return created
        except XeroException as e:
            self.logger.error(f'[{function_name}] [XeroAPI] ❌ - XeroException creating contact: {str(e)}')
            return None
        except Exception as e:
            self.logger.error(f'[{function_name}] [XeroAPI] 💥 - Unexpected: {str(e)}')
            return None

    def update_contact(self, contact_data: dict):
        function_name = 'update_contact'
        cid = contact_data.get('ContactID')
        self.logger.info(f'[{function_name}] [XeroAPI - contactID {cid}] 🔄 - Updating contact...')
        self._refresh_token_if_needed()
        try:
            updated = self._retry_on_unauthorized(self.xero.contacts.save, contact_data)
            if not updated:
                self.logger.error(f'[{function_name}] [XeroAPI - contactID {cid}] ❌ - Empty response.')
                return None
            self.logger.debug(f'[{function_name}] [XeroAPI - contactID {cid}] 🔍 - Updated: {updated}')
            return updated
        except XeroException as e:
            self.logger.error(f'[{function_name}] [XeroAPI - contactID {cid}] ❌ - XeroException: {str(e)}')
            return None
        except Exception as e:
            self.logger.error(f'[{function_name}] [XeroAPI - contactID {cid}] 💥 - Unexpected: {str(e)}')
            return None

    def update_contact_with_retry(self, contact_data, max_retries=3):
        function_name = 'update_contact_with_retry'
        cid = contact_data.get('ContactID')
        self.logger.info(
            f'[{function_name}] [XeroAPI - contactID {cid}] 🔄 - Updating with up to {max_retries} retries.')
        self._refresh_token_if_needed()
        for attempt in range(1, max_retries + 1):
            try:
                updated = self.xero.contacts.save(contact_data)
                return updated
            except XeroRateLimitExceeded:
                self.logger.warning(
                    f'[{function_name}] [XeroAPI - contactID {cid}] 🔃 - Rate limit. Attempt {attempt} of {max_retries}.')
                time.sleep(65)
            except XeroException as xe:
                self.logger.error(f'[{function_name}] [XeroAPI - contactID {cid}] ❌ - XeroException: {xe}')
                return None
            except Exception as e:
                self.logger.error(f'[{function_name}] [XeroAPI - contactID {cid}] 💥 - Unexpected: {str(e)}')
                return None
        self.logger.error(f'[{function_name}] [XeroAPI - contactID {cid}] ❌ - Failed after multiple retries.')
        return None

    def update_contacts_with_retry(self, contacts_data: list[dict], max_retries=3):
        function_name = 'update_contacts_with_retry'
        self.logger.info(f'[{function_name}] [XeroAPI] 🔄 - Batch update of {len(contacts_data)} contact(s)...')
        self._refresh_token_if_needed()
        for attempt in range(1, max_retries + 1):
            try:
                updated = self.xero.contacts.save(contacts_data)
                return updated
            except XeroRateLimitExceeded:
                self.logger.warning(
                    f'[{function_name}] [XeroAPI] 🔃 - Rate limit on attempt {attempt}. Sleeping 65s...')
                time.sleep(65)
            except XeroException as xe:
                self.logger.error(f'[{function_name}] [XeroAPI] ❌ - XeroException in batch contact update: {xe}')
                return None
            except Exception as e:
                self.logger.error(f'[{function_name}] [XeroAPI] 💥 - Unexpected: {str(e)}')
                return None
        self.logger.error(f'[{function_name}] [XeroAPI] ❌ - Failed after multiple retries.')
        return None

    # endregion

    # region 5️⃣ Invoices & Bills (Grouped)

    # region 5.1 🔹 Invoice Methods
    def create_invoice(self, payload: dict):
        self._refresh_token_if_needed()
        self.logger.info('[create_invoice] - Creating ACCPAY invoice in Xero.')
        try:
            result = self._retry_on_unauthorized(
                self.xero.invoices.put,
                [payload]
            )
            return result
        except XeroException as e:
            self.logger.error(f'[create_invoice] ❌ XeroException: {e}')
            return None
        except Exception as e:
            self.logger.error(f'[create_invoice] 💥 Unexpected error: {e}')
            return None

    def update_invoice(self, xero_id: str, changes: dict):
        self._refresh_token_if_needed()
        self.logger.info(f'[update_invoice] - Updating invoice {xero_id} with {changes}')
        try:
            existing = self._retry_on_unauthorized(
                self.xero.invoices.filter,
                InvoiceID=xero_id
            )
            if not existing:
                self.logger.warning(f'[update_invoice] - No invoice found for {xero_id}')
                return None
            invoice_obj = existing[0]
            invoice_obj.update(changes)
            updated = self._retry_on_unauthorized(
                self.xero.invoices.save, invoice_obj
            )
            return updated
        except XeroException as e:
            self.logger.error(f'[update_invoice] ❌ XeroException: {e}')
            return None
        except Exception as e:
            self.logger.error(f'[update_invoice] 💥 Unexpected: {e}')
            return None

    def delete_invoice(self, invoice_id: str):
        self._refresh_token_if_needed()
        self.logger.info(f'[delete_invoice] - Attempting to delete (InvoiceID={invoice_id}).')
        try:
            invoice_list = self._retry_on_unauthorized(
                self.xero.invoices.filter,
                InvoiceID=invoice_id
            )
            if not invoice_list:
                self.logger.warning(f'[delete_invoice] - No invoice with ID={invoice_id}.')
                return None

            invoice_obj = invoice_list[0]
            current_status = invoice_obj.get('Status', '').upper()
            if current_status not in ['DRAFT', 'SUBMITTED']:
                self.logger.warning(f'[delete_invoice] - Cannot set status=DELETED from {current_status}.')
                return None
            invoice_obj['Status'] = 'DELETED'
            deleted_resp = self._retry_on_unauthorized(
                self.xero.invoices.save, invoice_obj
            )
            self.logger.info(f'[delete_invoice] - Invoice {invoice_id} set to DELETED successfully.')
            return deleted_resp
        except XeroException as e:
            self.logger.error(f'[delete_invoice] ❌ XeroException: {e}')
            return None
        except Exception as e:
            self.logger.error(f'[delete_invoice] 💥 Unexpected: {e}')
            return None

    def get_invoice_details(self, invoice_id: str):
        self._refresh_token_if_needed()
        self.logger.debug(f'[get_invoice_details] - Fetching invoice {invoice_id}')
        try:
            invoice_list = self._retry_on_unauthorized(
                self.xero.invoices.get,
                invoice_id
            )
            if not invoice_list:
                self.logger.warning(f'[get_invoice_details] - No invoice found with ID={invoice_id}')
                return None
            full_inv = invoice_list[0]
            if full_inv.get('Status') == 'DELETED':
                return None
            return full_inv
        except XeroException as e:
            self.logger.error(f'[get_invoice_details] ❌ XeroException: {e}')
            return None
        except Exception as e:
            self.logger.error(f'[get_invoice_details] 💥 Unexpected: {e}')
            return None

    def add_line_item_to_invoice(self, invoice_id: str, line_item_data: dict) -> dict:
        self._refresh_token_if_needed()
        try:
            invoice_list = self._retry_on_unauthorized(self.xero.invoices.filter, InvoiceID=invoice_id)
            if not invoice_list:
                self.logger.warning(f'[add_line_item_to_invoice] - No invoice found for InvoiceID={invoice_id}.')
                return {}
            invoice_obj = invoice_list[0]
            if invoice_obj.get('Status') == 'DELETED':
                self.logger.warning('[add_line_item_to_invoice] - Invoice is DELETED, cannot add line item.')
                return {}
            existing_items = invoice_obj.get('LineItems', [])
            existing_items.append(line_item_data)
            invoice_obj['LineItems'] = existing_items
            updated_invoice = self._retry_on_unauthorized(self.xero.invoices.save, invoice_obj)
            if updated_invoice:
                self.logger.info(f'[add_line_item_to_invoice] - Successfully added line item to invoice {invoice_id}.')
                return updated_invoice[0] if isinstance(updated_invoice, list) else updated_invoice
            else:
                self.logger.warning(f'[add_line_item_to_invoice] - No response from Xero after saving invoice.')
                return {}
        except XeroException as e:
            self.logger.error(f'[add_line_item_to_invoice] - XeroException: {e}')
            return {}
        except Exception as e:
            self.logger.error(f'[add_line_item_to_invoice] - Unexpected error: {e}')
            return {}

    def update_line_item_in_invoice(self, invoice_id: str, line_item_id: str, new_line_item_data: dict) -> dict:
        self._refresh_token_if_needed()
        try:
            invoice_list = self._retry_on_unauthorized(self.xero.invoices.filter, InvoiceID=invoice_id)
            if not invoice_list:
                self.logger.warning(f'[update_line_item_in_invoice] - No invoice found for InvoiceID={invoice_id}.')
                return {}
            invoice_obj = invoice_list[0]
            if invoice_obj.get('Status') == 'DELETED':
                self.logger.warning('[update_line_item_in_invoice] - Invoice is DELETED, cannot update line item.')
                return {}
            existing_items = invoice_obj.get('LineItems', [])
            matched = False
            for li in existing_items:
                if str(li.get('LineItemID')) == str(line_item_id):
                    li.update(new_line_item_data)
                    matched = True
                    break
            if not matched:
                self.logger.warning(f'[update_line_item_in_invoice] - No matching line item with ID={line_item_id}.')
                return {}
            invoice_obj['LineItems'] = existing_items
            updated_invoice = self._retry_on_unauthorized(self.xero.invoices.save, invoice_obj)
            if updated_invoice:
                self.logger.info(
                    f'[update_line_item_in_invoice] - Updated line item {line_item_id} in invoice {invoice_id}.')
                return updated_invoice[0] if isinstance(updated_invoice, list) else updated_invoice
            else:
                self.logger.warning(f'[update_line_item_in_invoice] - No response from Xero after saving invoice.')
                return {}
        except XeroException as e:
            self.logger.error(f'[update_line_item_in_invoice] - XeroException: {e}')
            return {}
        except Exception as e:
            self.logger.error(f'[update_line_item_in_invoice] - Unexpected error: {e}')
            return {}

    # endregion

    # region 5.2 🔹 Bill Methods
    def create_bill(self, session, project_id: int, po_number: int, detail_number: int):
        self._refresh_token_if_needed()
        function_name = 'create_bill'
        try:
            detail_items = session.query(DetailItem).filter(DetailItem.line_number == detail_number).all()
        except Exception as e:
            self.logger.error(
                f'[{function_name}] [XeroAPI - detail_number {detail_number}] ❌ - DB error: {str(e)}'
            )
            detail_items = []
        if not detail_items:
            self.logger.warning(
                f'[{function_name}] [XeroAPI - detail_number {detail_number}] ⚠️ - No detail items found; creating empty Bill.'
            )
            detail_items = []
        xero_line_items = []
        for di in detail_items:
            try:
                xero_line_items.append(self._convert_detail_item_to_line_item(session, di))
            except Exception as ex:
                self.logger.error(
                    f'[{function_name}] [XeroAPI - detail_item {di.id}] 💥 - Conversion error: {str(ex)}'
                )
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
        contact_xero_id_from_db = None
        if detail_items and hasattr(detail_items[0], "contact_id"):
            contact_id = detail_items[0].contact_id
            if contact_id:
                self.logger.debug(f'[{function_name}] [XeroAPI] 🔎 - Searching contact_id={contact_id}')
                found_contact = self._retry_on_unauthorized(self.xero.contacts.filter, ContactID=contact_id)
        reference = f'{project_id}_{po_number}_{detail_number}'
        new_invoice = {
            'Type': 'ACCPAY',
            'Contact': {'ContactID': contact_xero_id_from_db or '11111111-2222-3333-4444-555555555555'},
            'LineItems': xero_line_items,
            'InvoiceNumber': reference,
            'Status': xero_status
        }
        self.logger.info(f'[{function_name}] [XeroAPI - reference {reference}] 💼 - Creating Xero bill...')
        try:
            created_invoice = self._retry_on_unauthorized(self.xero.invoices.put, [new_invoice])
            self.logger.debug(
                f'[{function_name}] [XeroAPI - reference {reference}] 🔍 - Xero response: {created_invoice}')
            return created_invoice
        except XeroException as e:
            self.logger.error(f'[{function_name}] [XeroAPI - reference {reference}] ❌ - XeroException: {str(e)}')
            return None
        except Exception as e:
            self.logger.error(f'[{function_name}] [XeroAPI - reference {reference}] 💥 - Unexpected: {str(e)}')
            return None

    def update_bill_status(self, invoice_id: str, new_status: str):
        self._refresh_token_if_needed()
        function_name = 'update_bill_status'
        self.logger.info(f'[{function_name}] [XeroAPI - invoice_id {invoice_id}] 🔄 - Updating to {new_status}...')
        try:
            existing_list = self._retry_on_unauthorized(self.xero.invoices.filter, InvoiceID=invoice_id)
            if not existing_list:
                self.logger.warning(f'[{function_name}] [XeroAPI - invoice_id {invoice_id}] ⚠️ - No invoice found.')
                return None
            invoice_obj = existing_list[0]
            invoice_obj['Status'] = new_status
            updated_invoices = self._retry_on_unauthorized(self.xero.invoices.save, invoice_obj)
            self.logger.debug(
                f'[{function_name}] [XeroAPI - invoice_id {invoice_id}] 🔍 - Updated invoice: {updated_invoices}')
            return updated_invoices
        except XeroException as e:
            self.logger.error(f'[{function_name}] [XeroAPI - invoice_id {invoice_id}] ❌ - XeroException: {str(e)}')
            return None
        except Exception as e:
            self.logger.error(f'[{function_name}] [XeroAPI - invoice_id {invoice_id}] 💥 - Unexpected: {str(e)}')
            return None

    # endregion

    # region 5.3 🔹 Bill Retrieval
    def get_bills_by_reference(self, reference_str: str):
        self._refresh_token_if_needed()
        function_name = 'get_bills_by_reference'
        self.logger.info(f'[{function_name}] - Searching for ACCPAY invoices using Reference="{reference_str}"')
        try:
            raw_filter = (
                'Type=="ACCPAY" AND Reference!=null '
                f'AND Reference=="{reference_str}"'
            )
            self.logger.debug(f'[{function_name}] Using raw filter => {raw_filter}')
            invoices = self._retry_on_unauthorized(self.xero.invoices.filter, raw=raw_filter)
            if not invoices:
                self.logger.info(f'[{function_name}] - No results for reference="{reference_str}". Returning [].')
                return []
            results = [inv for inv in invoices if inv.get('Status') != 'DELETED']
            self.logger.debug(f'[{function_name}] - Filtered out DELETED. Found {len(results)} invoice(s).')
            return results
        except XeroException as e:
            self.logger.error(f'[{function_name}] ❌ XeroException: {e}')
            return []
        except Exception as e:
            self.logger.error(f'[{function_name}] 💥 Unexpected: {e}')
            return []

    def get_all_bills(self):
        self._refresh_token_if_needed()
        function_name = 'get_all_bills'
        self.logger.info(f'[{function_name}] [XeroAPI] 📄 - Retrieving all ACCPAY invoices...')
        all_invoices_summary = []
        page_number = 1
        page_size = 100
        while True:
            self.logger.debug(f'[{function_name}] [XeroAPI] 🔎 - Fetching ACCPAY page {page_number}...')
            filter_str = 'Type=="ACCPAY"'
            invoices_page = self._retry_on_unauthorized(self.xero.invoices.filter, raw=filter_str, page=page_number)
            if not invoices_page:
                self.logger.debug(f'[{function_name}] [XeroAPI] ⏹️ - No invoices on page {page_number}.')
                break
            all_invoices_summary.extend(invoices_page)
            if len(invoices_page) < page_size:
                break
            page_number += 1
        if not all_invoices_summary:
            self.logger.info(f'[{function_name}] [XeroAPI] ℹ️ - No ACCPAY invoices found.')
            return []
        self.logger.info(
            f'[{function_name}] [XeroAPI] 🔎 - Fetched {len(all_invoices_summary)} summaries, now retrieving full details...')
        detailed_invoices = []
        for summary_inv in all_invoices_summary:
            if summary_inv.get('Status') == 'DELETED':
                continue
            invoice_id = summary_inv.get('InvoiceID')
            if not invoice_id:
                continue
            full_inv_list = self._retry_on_unauthorized(self.xero.invoices.get, invoice_id)
            if not full_inv_list:
                continue
            detailed_inv = full_inv_list[0]
            if detailed_inv.get('Status') == 'DELETED':
                continue
            detailed_invoices.append(detailed_inv)
        self.logger.info(
            f'[{function_name}] [XeroAPI] ✅ - Retrieved {len(detailed_invoices)} detailed ACCPAY invoices.')
        return detailed_invoices

    def get_acpay_invoices_summary_by_ref(self, reference_substring: str) -> list:
        self._refresh_token_if_needed()
        function_name = 'get_acpay_invoices_summary_by_ref'
        raw_filter = (
            'Type=="ACCPAY" AND InvoiceNumber!=null '
            f'&& InvoiceNumber.Contains("{reference_substring}")'
        )
        self.logger.info(f'[{function_name}] [XeroAPI] 🔎 - Searching ACCPAY with substring: {reference_substring}')
        page_number = 1
        page_size = 100
        all_summaries = []
        while True:
            self.logger.debug(f'[{function_name}] [XeroAPI] 🔍 - Page {page_number}, filter: {raw_filter}')
            current_page = self._retry_on_unauthorized(self.xero.invoices.filter, raw=raw_filter, page=page_number)
            if not current_page:
                break
            filtered_page = [inv for inv in current_page if inv.get('Status') != 'DELETED']
            all_summaries.extend(filtered_page)
            if len(current_page) < page_size:
                break
            page_number += 1
        self.logger.info(f'[{function_name}] [XeroAPI] ✅ - Found {len(all_summaries)} invoice summaries.')
        return all_summaries

    # endregion

    # endregion  # End Invoices & Bills

    # region 6️⃣ Spend Money
    def create_spend_money(self, session, detail_item_id: int):
        self._refresh_token_if_needed()
        function_name = 'create_spend_money'
        detail_item = None
        try:
            detail_item = session.query(DetailItem).get(detail_item_id)
        except Exception as e:
            self.logger.error(
                f'[{function_name}] [XeroAPI - detail_item {detail_item_id}] ❌ - DB error retrieving DetailItem: {str(e)}'
            )
        if not detail_item:
            self.logger.info(
                f'[{function_name}] [XeroAPI - detail_item {detail_item_id}] 🗒️ - No DetailItem found => creating VOIDED spend money.'
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
                'Contact': {'Name': detail_item.vendor or 'Unknown Vendor'},
                'LineItems': [line_dict],
                'Status': xero_status
            }
            self.logger.info(
                f'[{function_name}] [XeroAPI - detail_item {detail_item_id}] 💸 - Creating spend money in Xero...')
            created = self._retry_on_unauthorized(self.xero.banktransactions.put, [new_tx])
            self.logger.debug(
                f'[{function_name}] [XeroAPI - detail_item {detail_item_id}] 🔍 - Xero response: {created}')
            return created
        except XeroException as e:
            self.logger.error(f'[{function_name}] [XeroAPI - detail_item {detail_item_id}] ❌ - XeroException: {str(e)}')
            return None
        except Exception as e:
            self.logger.error(
                f'[{function_name}] [XeroAPI - detail_item {detail_item_id}] 💥 - Unexpected error: {str(e)}')
            return None

    def create_spend_money_in_xero(self, spend_money_record: dict):
        self._refresh_token_if_needed()
        self.logger.info('[create_spend_money_in_xero] Creating spend money from a SpendMoney dict...')
        xero_status = (spend_money_record.get('state') or 'DRAFT').upper()
        new_tx = {
            'Type': 'SPEND',
            'Contact': {'Name': spend_money_record.get('vendor', 'Unknown Vendor')},
            'LineItems': [{
                'Description': spend_money_record.get('description', 'No description'),
                'Quantity': 1,
                'UnitAmount': float(spend_money_record.get('amount', 0.0)),
                'TaxType': 'NONE'
            }],
            'Status': xero_status
        }
        try:
            created = self._retry_on_unauthorized(self.xero.banktransactions.put, [new_tx])
            if created:
                self.logger.debug(f'[create_spend_money_in_xero] => {created}')
            return created
        except XeroException as e:
            self.logger.error(f'[create_spend_money_in_xero] ❌ - XeroException: {str(e)}')
            return None
        except Exception as e:
            self.logger.error(f'[create_spend_money_in_xero] 💥 - Unexpected: {str(e)}')
            return None

    def create_spend_money_via_detail_id(self, detail_item_id: int):
        self.logger.info(f'[create_spend_money_via_detail_id] => detail_item_id={detail_item_id}')
        # This method can simply call create_spend_money or implement its own logic.

    def update_spend_money(self, xero_spend_money_id: str, new_state: str):
        self._refresh_token_if_needed()
        function_name = 'update_spend_money'
        self.logger.info(
            f'[{function_name}] [XeroAPI - spend_money {xero_spend_money_id}] 🔄 - Updating to {new_state}...')
        try:
            existing_list = self._retry_on_unauthorized(self.xero.banktransactions.filter,
                                                        BankTransactionID=xero_spend_money_id)
            if not existing_list:
                self.logger.warning(
                    f'[{function_name}] [XeroAPI - spend_money {xero_spend_money_id}] ⚠️ - No bank transaction found.')
                return None
            bank_tx = existing_list[0]
            bank_tx['Status'] = new_state
            updated = self._retry_on_unauthorized(self.xero.banktransactions.save, bank_tx)
            self.logger.debug(
                f'[{function_name}] [XeroAPI - spend_money {xero_spend_money_id}] 🔍 - Updated: {updated}')
            return updated
        except XeroException as e:
            self.logger.error(
                f'[{function_name}] [XeroAPI - spend_money {xero_spend_money_id}] ❌ - XeroException: {str(e)}')
            return None
        except Exception as e:
            self.logger.error(
                f'[{function_name}] [XeroAPI - spend_money {xero_spend_money_id}] 💥 - Unexpected: {str(e)}')
            return None

    def update_spend_transaction_status(self, xero_spend_money_id: str, new_state: str) -> dict:
        return self.update_spend_money(xero_spend_money_id, new_state)

    def _create_voided_spend_money(self):
        function_name = '_create_voided_spend_money'
        voided_tx = {
            'Type': 'SPEND',
            'Contact': {'Name': 'Unknown Vendor'},
            'LineItems': [],
            'Status': 'VOIDED'
        }
        try:
            response = self._retry_on_unauthorized(self.xero.banktransactions.put, [voided_tx])
            return response
        except XeroException as e:
            self.logger.error(f'[{function_name}] [XeroAPI] ❌ - XeroException creating VOIDED spend money: {str(e)}')
            return None
        except Exception as e:
            self.logger.error(f'[{function_name}] [XeroAPI] 💥 - Unexpected: {str(e)}')
            return None

    # endregion

    # region 7️⃣ Concurrency Example
    def create_spend_money_in_batch(self, session, detail_item_ids: list[int]):
        function_name = 'create_spend_money_in_batch'
        self.logger.info(f'[{function_name}] [XeroAPI] 🏎 - Creating spend money for detail_item_ids={detail_item_ids}')
        futures = []
        results = []
        with ThreadPoolExecutor(max_workers=5) as executor:
            for detail_item_id in detail_item_ids:
                futures.append(executor.submit(self.create_spend_money, session, detail_item_id))
            for future in as_completed(futures):
                try:
                    result = future.result()
                    results.append(result)
                except Exception as e:
                    self.logger.error(f'[{function_name}] [XeroAPI] 💥 - Thread exception: {str(e)}')
        self.logger.info(f'[{function_name}] [XeroAPI] ✅ - Batch completed with {len(results)} result(s).')
        return results

    # endregion

    # region 8️⃣ (OLD) Legacy / Duplicate Token & Retry
    # (Legacy code commented out)
    # endregion

    def upsert_contacts_batch(self, contacts: list[dict]):
        """
        Attempts to upsert a batch of contacts in Xero (update if xero_id present,
        otherwise create). Before creating new contacts, checks Xero for any
        existing contacts by name and avoids double creation for those found.
        """
        results = []

        update_contacts = []
        create_contacts = []

        # Separate contacts into update vs create
        for contact in contacts:
            try:
                xid = contact["xero_id"]
                if xid:
                    update_contacts.append(contact)
                else:
                    create_contacts.append(contact)
            except KeyError as e:
                self.logger.error(
                    f"⛔ Skipping contact because 'xero_id' key is missing: {contact}. Error: {e}"
                )
                raise
            except Exception as e:
                self.logger.error(
                    f"⛔ Unexpected error while parsing contact for upsert: {contact}. Error: {e}"
                )
                raise

        # -----------------------------
        #  Update batch
        # -----------------------------
        if update_contacts:
            self.logger.info(f"🌀 Updating {len(update_contacts)} existing contacts in Xero.")
            try:
                updated_list = self._retry_on_unauthorized(
                    self.xero.contacts.put,
                    update_contacts
                )
                if updated_list:
                    self.logger.info(f"🌀 Successfully updated {len(updated_list)} contacts.")
                    results.extend(updated_list)
            except KeyError as e:
                self.logger.error(f"⛔ KeyError during update batch: {update_contacts}. Error: {e}")
            except Exception as e:
                self.logger.error(
                    f"⛔ Unexpected exception while updating contacts in Xero. Error: {e}"
                )

        # -----------------------------
        #  Create batch
        # -----------------------------
        if create_contacts:
            self.logger.info(f"🌀 Preparing to create {len(create_contacts)} new contacts in Xero.")

            # 1) Check Xero for each contact name before creating
            existing_contacts = []
            to_create_contacts = []

            for contact in create_contacts:
                try:
                    contact_name = contact["Name"]
                except KeyError as e:
                    self.logger.error(
                        f"⛔ Contact is missing 'Name' key and cannot be created: {contact}. Error: {e}"
                    )
                    raise
                except Exception as e:
                    self.logger.error(
                        f"⛔ Unexpected error reading contact name for creation: {contact}. Error: {e}"
                    )
                    raise

                # Use filter instead of all(..., where=...) to query by name.
                try:
                    found = self.xero.contacts.filter(Name=contact_name)
                except Exception as e:
                    self.logger.error(
                        f"⛔ Error querying Xero for existing contact by name='{contact_name}'. Error: {e}"
                    )
                    raise

                if found:
                    self.logger.info(
                        f"🌀 Found existing Xero contact for '{contact_name}'. Skipping creation."
                    )
                    existing_contacts.extend(found)
                else:
                    to_create_contacts.append(contact)

            if existing_contacts:
                self.logger.info(
                    f"🌀 {len(existing_contacts)} contacts already exist in Xero. They will be added to the results without creation."
                )
                results.extend(existing_contacts)

            if to_create_contacts:
                self.logger.info(
                    f"🌀 Creating {len(to_create_contacts)} new contacts in Xero."
                )
                try:
                    created_list = self._retry_on_unauthorized(
                        self.xero.contacts.put,
                        to_create_contacts
                    )
                    if created_list:
                        self.logger.info(
                            f"🌀 Successfully created {len(created_list)} new contacts."
                        )
                        results.extend(created_list)
                except KeyError as e:
                    self.logger.error(
                        f"⛔ KeyError during create batch: {to_create_contacts}. Error: {e}"
                    )
                except Exception as e:
                    self.logger.error(
                        f"⛔ Unexpected exception while creating contacts in Xero. Error: {e}"
                    )
            else:
                self.logger.info("🌀 No new contacts to create in Xero.")
        else:
            self.logger.info("🌀 No contacts to create in Xero.")

        return results

# endregion  # End of XeroAPI class definition

# region 🎉 Singleton Instance
xero_api = XeroAPI()
# endregion