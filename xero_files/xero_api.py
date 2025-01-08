"""
xero_api.py

Rewritten to use PyXero instead of xero-python. Manages:
- Spend Money items (mapped to bankTransactions of type 'SPEND')
- Bills (mapped to Invoices of type 'ACCPAY')
"""

import os
import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dotenv import load_dotenv

# region PyXero Imports
from xero import Xero
from xero.auth import OAuth2Credentials
from xero.constants import XeroScopes
# In some PyXero versions, XeroUnauthorized handles 401 (including expired tokens).
# If your version doesn't have it, fallback to the more generic XeroException.
from xero.exceptions import XeroException, XeroUnauthorized, XeroRateLimitExceeded
# endregion

# region Local Imports
from database.models import (
    SpendMoney,
    XeroBill,
    BillLineItem,
    DetailItem,
    AicpCode,
    TaxAccount
)
from singleton import SingletonMeta
# endregion


class XeroAPI(metaclass=SingletonMeta):
    """
    Encapsulates interactions with Xero using PyXero.
    """

    def __init__(self):
        """
        Initialize XeroAPI with environment variables and set up the Xero client.
        """
        # Load environment variables
        self.client_id = os.getenv("XERO_CLIENT_ID")
        self.client_secret = os.getenv("XERO_CLIENT_SECRET")
        self.access_token = os.getenv("XERO_ACCESS_TOKEN")
        self.refresh_token = os.getenv("XERO_REFRESH_TOKEN")
        self.tenant_id = os.getenv("XERO_TENANT_ID")
        load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))
        # Default scope if not specified in env
        # Make sure "offline_access" is included if you need token refreshes
        self.scope = (
            os.getenv("XERO_SCOPE")
            or "accounting.contacts accounting.settings accounting.transactions offline_access"
        )

        # Setup logging
        self.logger = logging.getLogger("app_logger")

        # Build our initial token dict from environment
        # PyXero expects a dict with keys like access_token, refresh_token, expires_in, expires_at, etc.
        current_time = time.time()
        default_expires_in = 1800  # 30 minutes, adjust as needed

        token_dict = {
            "access_token": self.access_token,
            "refresh_token": self.refresh_token,
            "expires_in": default_expires_in,
            "expires_at": current_time + default_expires_in,  # Avoid KeyError
            "token_type": "Bearer",
            "scope": self.scope.split(),  # Convert space-delimited string into list
        }

        # 1) Create OAuth2Credentials
        self.credentials = OAuth2Credentials(
            client_id=self.client_id,
            client_secret=self.client_secret,
            scope=self.scope.split(),
            token=token_dict,
        )

        # 2) If you already know which tenant you want, set it here.
        if self.tenant_id:
            self.credentials.tenant_id = self.tenant_id

        # 3) Create the Xero client
        self.xero = Xero(self.credentials)

        # Force an initial refresh check so we can set the tenant if not yet set
        self._refresh_token_if_needed()

        self.logger.info("XeroAPI (PyXero) initialized.")
        self._initialized = True

    def _refresh_token_if_needed(self, force=False):
        """
        Refresh the Xero token if it’s expired or about to expire.
        If 'force=True', we attempt a refresh no matter what.
        """
        # If not forcing, only refresh if credentials show expired
        if not force and not self.credentials.expired():
            self.logger.debug("Token is still valid, no refresh necessary.")
            return

        try:
            self.logger.debug("Token expired or force-refresh requested; refreshing now...")
            self.credentials.refresh()
            self.logger.info("Successfully refreshed Xero tokens with PyXero!")

            # If no tenant was set, pick the first one if available
            if not self.credentials.tenant_id:
                tenants = self.credentials.get_tenants()
                if tenants:
                    self.credentials.tenant_id = tenants[0]["tenantId"]
                    self.logger.info(f"Tenant set to {self.credentials.tenant_id}")
                else:
                    self.logger.warning("No tenants found for this user/token.")

            # Save updated tokens back to environment (ENV is now our single source of truth)
            new_token = self.credentials.token
            os.environ["XERO_ACCESS_TOKEN"] = new_token["access_token"]
            os.environ["XERO_REFRESH_TOKEN"] = new_token["refresh_token"]

            # Re-create the Xero client in case credentials updated
            self.xero = Xero(self.credentials)

        except XeroException as e:
            # If the refresh token is also invalid or too old, it might raise XeroUnauthorized or similar.
            self.logger.error(
                "Encountered an error (possibly expired refresh token). Re-auth may be required."
            )
            raise e

    def _retry_on_unauthorized(self, func, *args, **kwargs):
        """
        Helper method to call a PyXero function with retries if:
         - XeroUnauthorized occurs (token expired/invalid), or
         - XeroRateLimitExceeded occurs (429 rate limit).
        We will retry up to three times, sleeping between retries on rate limits.
        """
        max_retries = 3
        for attempt in range(1, max_retries + 1):
            try:
                return func(*args, **kwargs)
            except XeroUnauthorized:
                self.logger.warning("XeroUnauthorized caught mid-operation, attempting a force-refresh...")
                self._refresh_token_if_needed(force=True)
            except XeroRateLimitExceeded:
                self.logger.warning(
                    f"Rate limit hit. Attempt {attempt} of {max_retries}. "
                    f"Sleeping for 65 seconds before retry..."
                )
                time.sleep(65)
            except XeroException as e:
                # If it's some other XeroException that isn't Unauthorized or RateLimit, re-raise
                self.logger.error(f"XeroException occurred: {str(e)}")
                raise e

        # If we exhaust all retries (e.g., repeated 429 or 401 issues)
        self.logger.error("Failed to call Xero API after multiple retry attempts.")
        return None

    # region Utility Methods

    def _get_tax_code_for_detail_item(self, session, detail_item: DetailItem) -> str:
        """
        Retrieve the tax code from the DB (AicpCode -> TaxAccount).
        """
        tax_code = "TAX001"  # fallback
        try:
            aicp_code_record = session.query(AicpCode).filter_by(id=detail_item.aicp_code_id).first()
            if aicp_code_record and aicp_code_record.tax_account:
                tax_code = aicp_code_record.tax_account.tax_code
        except Exception as e:
            self.logger.warning(
                f"Error retrieving tax code for DetailItem ID {detail_item.id}: {str(e)}"
            )
        return tax_code

    def _convert_detail_item_to_line_item(self, session, detail_item: DetailItem) -> dict:
        """
        Convert a DetailItem record to a line item dict suitable for PyXero's bankTransactions or invoices.
        """
        tax_code = self._get_tax_code_for_detail_item(session, detail_item)
        return {
            "Description": detail_item.description or "No description",
            "Quantity": float(detail_item.quantity),
            "UnitAmount": float(detail_item.rate),
            "TaxType": tax_code,
        }

    # endregion

    # region SPEND MONEY CRUD

    def create_spend_money(self, session, detail_item_id: int):
        """
        Create a SPEND bank transaction in Xero based on a local DetailItem.
        """
        # Attempt a refresh if needed
        self._refresh_token_if_needed()

        detail_item = session.query(DetailItem).get(detail_item_id)
        if not detail_item:
            self.logger.info(
                f"Detail item not found for ID {detail_item_id}, creating VOIDED spend money record."
            )
            return self._create_voided_spend_money()

        # Decide on the Xero status
        if detail_item.state == "SUBMITTED":
            xero_status = "DRAFT"
        elif detail_item.state == "REVIEWED":
            xero_status = "AUTHORISED"
        else:
            xero_status = "VOIDED"

        # Build the bankTransaction dict for PyXero
        line_item_dict = self._convert_detail_item_to_line_item(session, detail_item)

        new_transaction = {
            "Type": "SPEND",
            "Contact": {
                "Name": detail_item.vendor or "Unknown Vendor"
            },
            "LineItems": [line_item_dict],
            "Status": xero_status,
        }

        self.logger.info("Creating spend money transaction in Xero (PyXero)...")

        try:
            # Use our retry helper to handle token issues & rate limits
            created = self._retry_on_unauthorized(self.xero.banktransactions.put, [new_transaction])
            self.logger.debug(f"Xero create response: {created}")
            return created
        except XeroException as e:
            self.logger.error(f"Failed to create spend money transaction in Xero: {str(e)}")
            return None

    def update_spend_money(self, session, xero_spend_money_id: str, new_state: str):
        """
        Update an existing spend money transaction's status in Xero.
        """
        self._refresh_token_if_needed()
        self.logger.info(f"Updating spend money transaction {xero_spend_money_id} to {new_state}...")

        try:
            existing_list = self._retry_on_unauthorized(
                self.xero.banktransactions.filter,
                BankTransactionID=xero_spend_money_id
            )
            if not existing_list:
                self.logger.warning(f"No bank transaction found with ID {xero_spend_money_id}")
                return None

            bank_tx = existing_list[0]
            bank_tx["Status"] = new_state

            updated = self._retry_on_unauthorized(self.xero.banktransactions.save, bank_tx)
            self.logger.debug(f"Updated spend money transaction: {updated}")
            return updated
        except XeroException as e:
            self.logger.error(f"Failed to update spend money transaction in Xero: {str(e)}")
            return None

    def _create_voided_spend_money(self):
        """
        Helper to create a 'VOIDED' spend money transaction in Xero
        when the detail item is missing or otherwise invalid.
        """
        voided_transaction = {
            "Type": "SPEND",
            "Contact": {"Name": "Unknown Vendor"},
            "LineItems": [],
            "Status": "VOIDED"
        }
        try:
            response = self._retry_on_unauthorized(self.xero.banktransactions.put, [voided_transaction])
            return response
        except XeroException as e:
            self.logger.error(f"Failed to create voided spend money transaction: {str(e)}")
            return None

    # endregion

    # region BILLS CRUD

    def create_bill(self, session, project_id: int, po_number: int, detail_number: int):
        """
        Create a Bill (Invoice type='ACCPAY') in Xero for a specific project/PO/detail combo.
        """
        self._refresh_token_if_needed()

        detail_items = (
            session.query(DetailItem)
            .filter(DetailItem.line_id == detail_number)
            .all()
        )
        if not detail_items:
            self.logger.warning("No detail items found; creating empty Bill.")
            detail_items = []

        xero_line_items = []
        for di in detail_items:
            xero_line_items.append(self._convert_detail_item_to_line_item(session, di))

        states = {di.state for di in detail_items}
        if len(states) == 1:
            only_state = list(states)[0]
            if only_state == "SUBMITTED":
                xero_status = "DRAFT"
            elif only_state == "RTP":
                xero_status = "SUBMITTED"
            elif only_state == "PAID":
                xero_status = "PAID"
            else:
                xero_status = "DRAFT"
        else:
            xero_status = "DRAFT"

        reference = f"{project_id}_{po_number}_{detail_number}"

        new_invoice = {
            "Type": "ACCPAY",
            "Contact": {"Name": "Vendor Name Placeholder"},
            "LineItems": xero_line_items,
            "Reference": reference,
            "Status": xero_status
        }

        self.logger.info(f"Creating Xero bill for reference {reference}...")

        try:
            created_invoice = self._retry_on_unauthorized(self.xero.invoices.put, [new_invoice])
            self.logger.debug(f"Xero create invoice response: {created_invoice}")
            return created_invoice
        except XeroException as e:
            self.logger.error(f"Failed to create bill (ACCPAY) in Xero: {str(e)}")
            return None

    def update_bill_status(self, invoice_id: str, new_status: str):
        """
        Update the status of an existing Bill (ACCPAY Invoice) in Xero.
        """
        self._refresh_token_if_needed()
        self.logger.info(f"Updating bill (invoice_id={invoice_id}) to status {new_status}...")

        try:
            existing_list = self._retry_on_unauthorized(
                self.xero.invoices.filter,
                InvoiceID=invoice_id
            )
            if not existing_list:
                self.logger.warning(f"No invoice found with ID {invoice_id}")
                return None

            invoice_obj = existing_list[0]
            invoice_obj["Status"] = new_status

            updated_invoices = self._retry_on_unauthorized(self.xero.invoices.save, invoice_obj)
            self.logger.debug(f"Updated invoice: {updated_invoices}")
            return updated_invoices
        except XeroException as e:
            self.logger.error(f"Failed to update bill in Xero: {str(e)}")
            return None

    # endregion

    # region Retrieval Methods

    def get_bills_by_reference(self, project_id: int = None, po_number: int = None, detail_number: int = None):
        """
        Fetch ACCPAY invoices from Xero by matching the 'Reference' in the
        format {projectId}_{poNumber}_{detailNumber}, with partial or exact matching.
        """
        self._refresh_token_if_needed()

        filter_clauses = []
        if project_id or po_number or detail_number:
            ref_parts = []
            if project_id is not None:
                ref_parts.append(str(project_id))
            if po_number is not None:
                ref_parts.append(str(po_number))
            if detail_number is not None:
                ref_parts.append(str(detail_number))

            if len(ref_parts) == 3:
                reference_str = "_".join(ref_parts)
                filter_clauses.append('Reference!=null')
                filter_clauses.append(f'Reference=="{reference_str}"')
            else:
                partial_str = "_".join(ref_parts)
                filter_clauses.append('Reference!=null')
                filter_clauses.append(f'Reference.StartsWith("{partial_str}")')

        filter_clauses.append('Type=="ACCPAY"')
        raw_filter = "&&".join(filter_clauses)

        self.logger.info(f"Retrieving bills (ACCPAY) from Xero with filter: {raw_filter}")

        try:
            results = self._retry_on_unauthorized(self.xero.invoices.filter, raw=raw_filter)
            if results:
                self.logger.debug(f"Found {len(results)} invoice(s) with filter '{raw_filter}'.")
                return results
            else:
                self.logger.info(f"No invoices found with filter '{raw_filter}'.")
                return []
        except XeroException as e:
            self.logger.error(f"Failed to retrieve bills from Xero: {str(e)}")
            return []

    def get_spend_money_by_reference(self, project_id: int = None, po_number: int = None, detail_number: int = None):
        """
        Fetch Spend Money transactions (bankTransactions of type 'SPEND') from Xero
        by matching the 'Reference' in {projectId}_{poNumber}_{detailNumber}.

        Note the added `Reference!=null` guard to avoid QueryParseException.
        """
        self._refresh_token_if_needed()

        filter_clauses = ['Type=="SPEND"']
        ref_parts = []
        if project_id:
            ref_parts.append(str(project_id))
        if po_number:
            ref_parts.append(str(po_number))
        if detail_number:
            ref_parts.append(str(detail_number))

        if len(ref_parts) == 3:
            exact_ref = "_".join(ref_parts)
            filter_clauses.append('Reference!=null')
            filter_clauses.append(f'Reference=="{exact_ref}"')
        elif len(ref_parts) > 0:
            partial_ref = "_".join(ref_parts)
            filter_clauses.append('Reference!=null')
            filter_clauses.append(f'Reference.StartsWith("{partial_ref}")')

        raw_filter = "&&".join(filter_clauses)
        self.logger.info(f"Retrieving SPEND money transactions with filter: {raw_filter}")

        try:
            results = self._retry_on_unauthorized(self.xero.banktransactions.filter, raw=raw_filter)
            if results:
                self.logger.debug(
                    f"Found {len(results)} spend money transaction(s) with filter '{raw_filter}'."
                )
                return results
            else:
                self.logger.info(f"No spend money transactions found with filter '{raw_filter}'.")
                return []
        except XeroException as e:
            self.logger.error(f"Failed to retrieve spend money from Xero: {str(e)}")
            return []

    # endregion

    # region  CONTACT FUNCTIONS
    def get_contact_by_name(self, name: str):
        """
        Retrieve a Xero contact by name. Returns the first match if multiple are found.
        If none exist, returns None.
        """
        self._refresh_token_if_needed()
        self.logger.info(f"Attempting to retrieve Xero contact by name: {name}")

        try:
            # Use our retry helper to handle token expiration & rate limits
            results = self._retry_on_unauthorized(self.xero.contacts.filter, Name=name)
            if results:
                self.logger.debug(f"Found Xero contact(s) for '{name}': {results}")
                return results[0]
            self.logger.info(f"No matching contact found for '{name}' in Xero.")
            return None
        except XeroException as e:
            self.logger.error(f"Error retrieving contact by name '{name}': {str(e)}")
            return None

    def get_all_contacts(self):
        """
        Fetch all contacts from Xero. This may be large for bigger organizations,
        so consider pagination or filtering if performance is a concern.
        """
        self._refresh_token_if_needed()
        self.logger.info("Fetching all contacts from Xero...")

        try:
            contacts = self._retry_on_unauthorized(self.xero.contacts.all)
            self.logger.debug(f"Retrieved {len(contacts)} contact(s) from Xero.")
            return contacts
        except XeroException as e:
            self.logger.error(f"Error retrieving all contacts: {str(e)}")
            return []

    def create_contact(self, contact_data: dict):
        """
        Create a new contact in Xero using the provided contact_data.

        contact_data should match PyXero's expected structure, e.g.:
        {
            "Name": "Vendor Name",
            "EmailAddress": "vendor@example.com",
            "TaxNumber": "123456789",
            ...
        }
        """
        self._refresh_token_if_needed()
        self.logger.info(f"Creating a new Xero contact with data: {contact_data}")

        try:
            created = self._retry_on_unauthorized(self.xero.contacts.put, [contact_data])
            self.logger.debug(f"Successfully created contact in Xero: {created}")
            return created
        except XeroException as e:
            self.logger.error(f"Failed to create contact in Xero: {str(e)}")
            return None

    def update_contact(self, contact_data: dict):
        """
        Update an existing contact in Xero. The contact_data must include ContactID.

        :param contact_data: Dictionary containing contact details to update.
        :return: Updated contact data if successful, raises exception otherwise.
        """
        self.logger.info(f"Attempting to update contact with ContactID={contact_data.get('ContactID')}")
        try:
            updated_contacts = self.xero.contacts.save(contact_data)
            self.logger.debug(f"Updated contact data: {updated_contacts}")
            if not updated_contacts:
                self.logger.error("Xero returned an empty response for contact update.")
                raise XeroException("Empty response from Xero during contact update.")
            return updated_contacts
        except XeroException as e:
            self.logger.error(f"XeroException occurred: {str(e)}")
            raise
        except Exception as e:
            self.logger.error(f"Unexpected error during contact update: {str(e)}")
            raise

    def update_contact_with_retry(self, contact_data, max_retries=3):
        """
        Attempts to update a Xero contact, retrying if we encounter rate-limit errors.
        This is an explicit example; the _retry_on_unauthorized wrapper approach
        can also cover this if we adjust it to handle XeroRateLimitExceeded.
        """
        for attempt in range(1, max_retries + 1):
            try:
                updated = self.xero.contacts.save(contact_data)
                return updated  # Xero returns a list of updated contacts
            except XeroRateLimitExceeded as e:
                self.logger.warning(
                    f"Rate limit hit. Attempt {attempt} of {max_retries}. "
                    f"Sleeping for 65 seconds before retry..."
                )
                time.sleep(65)  # Sleep a bit over a minute to let the limit reset
            except XeroException as xe:
                self.logger.error(f"XeroException while updating contact: {xe}")
                return None
        # If we exhaust all retries
        self.logger.error("Failed to update contact after multiple rate-limit retries.")
        return None
    # endregion

    def update_contacts_with_retry(self, contacts_data: list[dict], max_retries=3):
        """
        Attempts to update multiple Xero contacts, retrying if we encounter rate-limit errors.

        :param contacts_data: A list of contact dictionaries, each containing at least a 'ContactID'.
        :param max_retries: How many times to retry in the event of a rate-limit error.
        :return: List of updated contacts if successful, or None on failure.
        """
        for attempt in range(1, max_retries + 1):
            try:
                # PyXero's .save() can handle a list of contact objects
                updated = self.xero.contacts.save(contacts_data)
                return updated  # Xero returns a list of updated contacts
            except XeroRateLimitExceeded as e:
                self.logger.warning(
                    f"Rate limit hit. Attempt {attempt} of {max_retries}. "
                    f"Sleeping for 65 seconds before retry..."
                )
                time.sleep(65)  # Sleep a bit over a minute to let the limit reset
            except XeroException as xe:
                self.logger.error(f"XeroException while updating contacts in batch: {xe}")
                return None

        # If we exhaust all retries
        self.logger.error("Failed to update contacts after multiple rate-limit retries.")
        return None


    # region Concurrency Example

    def create_spend_money_in_batch(self, session, detail_item_ids: list[int]):
        """
        Example of concurrency to create multiple spend money items in parallel.
        """
        futures = []
        results = []
        with ThreadPoolExecutor(max_workers=5) as executor:
            for detail_item_id in detail_item_ids:
                futures.append(
                    executor.submit(self.create_spend_money, session, detail_item_id)
                )

            for future in as_completed(futures):
                result = future.result()
                results.append(result)

        return results

    # endregion


# Instantiate a global `XeroAPI` object you can import throughout your app
xero_api = XeroAPI()