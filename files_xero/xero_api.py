# xero_api.py

import os
import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dotenv import load_dotenv, set_key

# region PyXero Imports
from xero import Xero
from xero.auth import OAuth2Credentials
from xero.constants import XeroScopes
from xero.exceptions import XeroException, XeroUnauthorized, XeroRateLimitExceeded
# endregion

# region Local Imports
from database.models import (
    SpendMoney,
    XeroBill,
    BillLineItem,
    DetailItem,
    AccountCode,
    TaxAccount
)
from singleton import SingletonMeta
# endregion


# region XeroAPI Class
class XeroAPI(metaclass=SingletonMeta):
    """
    Encapsulates interactions with Xero using PyXero.
    """

    # region 🏗 Initialization
    def __init__(self):
        """
        Initialize XeroAPI with environment variables and set up the Xero client.
        """
        # Load environment variables
        try:
            load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))
        except Exception as e:
            # Log an error but continue (maybe no .env file)
            logging.getLogger("xero_logger").error(f"🚨 Could not load .env: {e}")

        self.client_id = os.getenv("XERO_CLIENT_ID")
        self.client_secret = os.getenv("XERO_CLIENT_SECRET")
        self.access_token = os.getenv("XERO_ACCESS_TOKEN")
        self.refresh_token = os.getenv("XERO_REFRESH_TOKEN")
        self.tenant_id = os.getenv("XERO_TENANT_ID")

        # Default scope if not specified in env
        self.scope = (
            os.getenv("XERO_SCOPE")
            or "accounting.contacts accounting.settings accounting.transactions offline_access"
        )

        # Setup logging
        self.logger = logging.getLogger("xero_logger")
        self.logger.setLevel(logging.DEBUG)  # Make sure we're logging at debug level

        # Build our initial token dict from environment
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

        # region 🔒 Create OAuth2Credentials
        self.credentials = OAuth2Credentials(
            client_id=self.client_id,
            client_secret=self.client_secret,
            scope=self.scope.split(),
            token=token_dict,
        )

        if self.tenant_id:
            self.credentials.tenant_id = self.tenant_id

        # Create the Xero client
        self.xero = Xero(self.credentials)

        # Force an initial refresh check so we can set the tenant if not yet set
        self._refresh_token_if_needed()

        self.logger.info("🚀 XeroAPI (PyXero) initialized successfully!")
        self._initialized = True
        # endregion
    # endregion

    # region 🔑 Token Refresh Methods
    def _refresh_token_if_needed(self, force=False):
        """
        Refresh the Xero token if it’s expired or about to expire.
        If 'force=True', we attempt a refresh no matter what.
        """
        # If not forcing, only refresh if credentials show expired
        if not force and not self.credentials.expired():
            self.logger.debug("🔑 Token is still valid, no refresh necessary.")
            return

        try:
            self.logger.debug("🔑 Token expired or force-refresh requested; refreshing now...")
            self.credentials.refresh()
            self.logger.info("🔄 Successfully refreshed Xero tokens with PyXero! ✅")

            # If no tenant was set, pick the first one if available
            if not self.credentials.tenant_id:
                tenants = self.credentials.get_tenants()
                if tenants:
                    self.credentials.tenant_id = tenants[0]["tenantId"]
                    self.logger.info(f"🏢 Tenant set to {self.credentials.tenant_id}")
                else:
                    self.logger.warning("⚠️ No tenants found for this user/token.")

            # Save updated tokens back to environment variables
            new_token = self.credentials.token
            os.environ["XERO_ACCESS_TOKEN"] = new_token["access_token"]
            os.environ["XERO_REFRESH_TOKEN"] = new_token["refresh_token"]

            # region ♻️ Write back to .env
            env_path = os.path.join(os.path.dirname(__file__), "..", ".env")
            set_key(env_path, "XERO_ACCESS_TOKEN", new_token["access_token"])
            set_key(env_path, "XERO_REFRESH_TOKEN", new_token["refresh_token"])
            # endregion

            # Re-create the Xero client in case credentials updated
            self.xero = Xero(self.credentials)

        except XeroException as e:
            # If the refresh token is also invalid or too old, it might raise XeroUnauthorized or similar.
            self.logger.error(
                "❌ Encountered XeroException during token refresh. Possibly need re-auth. Error: " + str(e)
            )
            raise e

    def _retry_on_unauthorized(self, func, *args, **kwargs):
        """
        Helper method to call a PyXero function with retries if:
         - XeroUnauthorized occurs (token expired/invalid),
         - XeroRateLimitExceeded occurs (429 rate limit), or
         - other XeroExceptions that might be recoverable.
        We will retry up to three times, sleeping between retries on rate limits.
        """
        max_retries = 3
        for attempt in range(1, max_retries + 1):
            try:
                self.logger.debug(f"🔄 Attempting Xero API call (attempt {attempt}) => {func.__name__}")
                return func(*args, **kwargs)
            except XeroUnauthorized:
                self.logger.warning("⚠️ XeroUnauthorized caught mid-operation, attempting a force-refresh...")
                self._refresh_token_if_needed(force=True)
            except XeroRateLimitExceeded:
                self.logger.warning(
                    f"🔃 Rate limit hit. Attempt {attempt} of {max_retries}. "
                    f"Sleeping for 65 seconds before retry..."
                )
                time.sleep(65)
            except XeroException as e:
                self.logger.error(f"❌ XeroException occurred: {str(e)}")
                # Could raise or keep trying, depends on the logic you want.
                # We'll bail out here if it's not Unauthorized or RateLimit.
                raise e

        # If we exhaust all retries (e.g., repeated 429 or 401 issues)
        self.logger.error("❌ Failed to call Xero API after multiple retry attempts.")
        return None
    # endregion

    # region 🛠 Utility Methods
    def _get_tax_code_for_detail_item(self, session, detail_item: DetailItem) -> str:
        """
        Retrieve the tax code from the DB (AccountCode -> TaxAccount).
        Falls back to "TAX001" if anything goes wrong or if not found.
        """
        tax_code = "TAX001"  # fallback
        try:
            account_code_record = (
                session.query(AccountCode).filter_by(id=detail_item.account_code_id).first()
            )
            if account_code_record and account_code_record.tax_account:
                tax_code = account_code_record.tax_account.tax_code
        except Exception as e:
            self.logger.warning(
                f"⚠️ Error retrieving tax code for DetailItem ID {detail_item.id}: {str(e)}"
            )
        return tax_code

    def _convert_detail_item_to_line_item(self, session, detail_item: DetailItem) -> dict:
        """
        Convert a DetailItem record to a line item dict suitable for PyXero's bankTransactions or invoices.
        """
        try:
            tax_code = self._get_tax_code_for_detail_item(session, detail_item)
            return {
                "Description": detail_item.description or "No description",
                "Quantity": float(detail_item.quantity),
                "UnitAmount": float(detail_item.rate),
                "TaxType": tax_code,
            }
        except Exception as e:
            self.logger.error(
                f"💥 Failed to convert DetailItem(ID={detail_item.id}) to line item: {str(e)}"
            )
            return {
                "Description": "Conversion error",
                "Quantity": 1.0,
                "UnitAmount": 0.0,
                "TaxType": "TAX001",
            }
    # endregion

    # region 💸 SPEND MONEY CRUD
    def create_spend_money(self, session, detail_item_id: int):
        """
        Create a SPEND bank transaction in Xero based on a local DetailItem.
        """
        self._refresh_token_if_needed()

        try:
            detail_item = session.query(DetailItem).get(detail_item_id)
        except Exception as e:
            self.logger.error(f"❌ DB error retrieving DetailItem(id={detail_item_id}): {str(e)}")
            detail_item = None

        if not detail_item:
            self.logger.info(
                f"🗒️ Detail item not found for ID {detail_item_id}, creating VOIDED spend money record."
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
        try:
            line_item_dict = self._convert_detail_item_to_line_item(session, detail_item)
            new_transaction = {
                "Type": "SPEND",
                "Contact": {"Name": detail_item.vendor or "Unknown Vendor"},
                "LineItems": [line_item_dict],
                "Status": xero_status,
            }

            self.logger.info("💸 Creating spend money transaction in Xero (PyXero)...")

            created = self._retry_on_unauthorized(self.xero.banktransactions.put, [new_transaction])
            self.logger.debug(f"🔍 Xero create response: {created}")
            return created
        except XeroException as e:
            self.logger.error(f"❌ Failed to create spend money transaction in Xero: {str(e)}")
            return None
        except Exception as e:
            self.logger.error(f"💥 Unexpected error creating spend money: {str(e)}")
            return None

    def update_spend_money(self, session, xero_spend_money_id: str, new_state: str):
        """
        Update an existing spend money transaction's status in Xero.
        """
        self._refresh_token_if_needed()
        self.logger.info(f"🔄 Updating spend money transaction {xero_spend_money_id} to {new_state}...")

        try:
            existing_list = self._retry_on_unauthorized(
                self.xero.banktransactions.filter,
                BankTransactionID=xero_spend_money_id
            )
            if not existing_list:
                self.logger.warning(f"⚠️ No bank transaction found with ID {xero_spend_money_id}")
                return None

            bank_tx = existing_list[0]
            bank_tx["Status"] = new_state

            updated = self._retry_on_unauthorized(self.xero.banktransactions.save, bank_tx)
            self.logger.debug(f"🔍 Updated spend money transaction: {updated}")
            return updated
        except XeroException as e:
            self.logger.error(f"❌ Failed to update spend money transaction in Xero: {str(e)}")
            return None
        except Exception as e:
            self.logger.error(f"💥 Unexpected error updating spend money: {str(e)}")
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
            self.logger.error(f"❌ Failed to create voided spend money transaction: {str(e)}")
            return None
        except Exception as e:
            self.logger.error(f"💥 Unexpected error creating voided spend money: {str(e)}")
            return None
    # endregion

    # region 💼 BILLS CRUD
    def create_bill(self, session, project_id: int, po_number: int, detail_number: int):
        """
        Create a Bill (Invoice type='ACCPAY') in Xero for a specific project/PO/detail combo.
        """
        self._refresh_token_if_needed()

        try:
            detail_items = (
                session.query(DetailItem)
                .filter(DetailItem.line_number == detail_number)
                .all()
            )
        except Exception as e:
            self.logger.error(f"❌ DB error loading DetailItems (detail_number={detail_number}): {str(e)}")
            detail_items = []

        if not detail_items:
            self.logger.warning("⚠️ No detail items found; creating empty Bill.")
            detail_items = []

        xero_line_items = []
        for di in detail_items:
            try:
                xero_line_items.append(self._convert_detail_item_to_line_item(session, di))
            except Exception as ex:
                self.logger.error(f"💥 Error converting a detail_item to line_item: {str(ex)}")

        # Determine Bill's status from detail items
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

        self.logger.info(f"💼 Creating Xero bill for reference {reference}...")

        try:
            created_invoice = self._retry_on_unauthorized(self.xero.invoices.put, [new_invoice])
            self.logger.debug(f"🔍 Xero create invoice response: {created_invoice}")
            return created_invoice
        except XeroException as e:
            self.logger.error(f"❌ Failed to create bill (ACCPAY) in Xero: {str(e)}")
            return None
        except Exception as e:
            self.logger.error(f"💥 Unexpected error creating Xero bill: {str(e)}")
            return None

    def update_bill_status(self, invoice_id: str, new_status: str):
        """
        Update the status of an existing Bill (ACCPAY Invoice) in Xero.
        """
        self._refresh_token_if_needed()
        self.logger.info(f"🔄 Updating bill (invoice_id={invoice_id}) to status {new_status}...")

        try:
            existing_list = self._retry_on_unauthorized(
                self.xero.invoices.filter,
                InvoiceID=invoice_id
            )
            if not existing_list:
                self.logger.warning(f"⚠️ No invoice found with ID {invoice_id}")
                return None

            invoice_obj = existing_list[0]
            invoice_obj["Status"] = new_status

            updated_invoices = self._retry_on_unauthorized(self.xero.invoices.save, invoice_obj)
            self.logger.debug(f"🔍 Updated invoice: {updated_invoices}")
            return updated_invoices
        except XeroException as e:
            self.logger.error(f"❌ Failed to update bill in Xero: {str(e)}")
            return None
        except Exception as e:
            self.logger.error(f"💥 Unexpected error updating Xero bill: {str(e)}")
            return None

    def get_all_bills(self):
        """
        Retrieve all Invoices from Xero of Type='ACCPAY' -- including line items.
        Excluding any with Status == "DELETED".
        """
        self._refresh_token_if_needed()

        self.logger.info("📄 Retrieving all ACCPAY invoices from Xero (paging + individual fetch).")

        all_invoices_summary = []
        page_number = 1
        page_size = 100  # Xero returns up to 100 invoices per page by default

        # Step 1: Collect a summary list of all ACCPAY invoices
        while True:
            self.logger.debug(f"🔎 Fetching page {page_number} of ACCPAY invoice summaries...")
            filter_str = 'Type=="ACCPAY"'
            invoices_page = self._retry_on_unauthorized(
                self.xero.invoices.filter,
                raw=filter_str,
                page=page_number
            )
            if not invoices_page:
                self.logger.debug(f"⏹️ No invoices found on page {page_number}. Stopping.")
                break

            all_invoices_summary.extend(invoices_page)
            self.logger.debug(f"✅ Retrieved {len(invoices_page)} invoice(s) on page {page_number}.")

            if len(invoices_page) < page_size:
                break
            page_number += 1

        if not all_invoices_summary:
            self.logger.info("ℹ️ No ACCPAY invoices found in Xero at all.")
            return []

        self.logger.info(
            f"🔎 Fetched {len(all_invoices_summary)} ACCPAY invoice summaries. Now retrieving full details..."
        )

        # Step 2: For each invoice in the summary, retrieve the full invoice by ID
        detailed_invoices = []
        for summary_inv in all_invoices_summary:
            if summary_inv.get("Status") == "DELETED":
                self.logger.debug(f"⏩ Skipping DELETED invoice {summary_inv.get('InvoiceID')}")
                continue

            invoice_id = summary_inv.get("InvoiceID")
            if not invoice_id:
                self.logger.warning("⚠️ Invoice summary is missing InvoiceID; skipping.")
                continue

            full_inv_list = self._retry_on_unauthorized(self.xero.invoices.get, invoice_id)
            if not full_inv_list:
                self.logger.warning(f"⚠️ No detailed invoice found for InvoiceID={invoice_id}; skipping.")
                continue

            detailed_inv = full_inv_list[0]
            if detailed_inv.get("Status") == "DELETED":
                self.logger.debug(f"⏩ Skipping DELETED invoice detail {invoice_id}.")
                continue

            detailed_invoices.append(detailed_inv)

        self.logger.info(
            f"✅ Finished retrieving {len(detailed_invoices)} detailed ACCPAY invoices (excluding DELETED)."
        )
        return detailed_invoices

    def get_acpay_invoices_summary_by_ref(self, reference_substring: str) -> list:
        """
        Retrieves a *summary* of ACCPAY (bills) from Xero whose InvoiceNumber
        or Reference contains the given substring. Does NOT include line items.
        Excludes any with Status == "DELETED".
        """
        self._refresh_token_if_needed()
        raw_filter = f'Type=="ACCPAY" AND InvoiceNumber!=null && InvoiceNumber.Contains("{reference_substring}")'
        self.logger.info(
            f"🔎 Fetching summary for ACCPAY invoices that match '{reference_substring}' in InvoiceNumber."
        )

        page_number = 1
        page_size = 100
        all_summaries = []

        while True:
            self.logger.debug(f"🔍 Requesting page {page_number} with filter: {raw_filter}")
            current_page = self._retry_on_unauthorized(
                self.xero.invoices.filter,
                raw=raw_filter,
                page=page_number
            )
            if not current_page:
                break

            filtered_page = [inv for inv in current_page if inv.get("Status") != "DELETED"]
            all_summaries.extend(filtered_page)

            if len(current_page) < page_size:
                break
            page_number += 1

        self.logger.info(f"✅ Found {len(all_summaries)} ACCPAY invoice summaries (excluding DELETED).")
        return all_summaries

    def get_invoice_details(self, invoice_id: str) -> dict:
        """
        Retrieves the *full* invoice with line items by InvoiceID.
        Returns a single invoice dict if found and not DELETED; otherwise None.
        """
        self._refresh_token_if_needed()
        self.logger.debug(f"🔎 Fetching detailed invoice for InvoiceID={invoice_id}")

        invoice_list = self._retry_on_unauthorized(self.xero.invoices.get, invoice_id)
        if not invoice_list:
            self.logger.warning(f"⚠️ No detailed invoice found for InvoiceID={invoice_id}.")
            return None

        full_invoice = invoice_list[0]
        if full_invoice.get("Status") == "DELETED":
            self.logger.debug(f"⏩ Invoice {invoice_id} is DELETED; returning None.")
            return None

        return full_invoice
    # endregion

    # region 🙍🏻‍♂️ Contact Methods
    def get_contact_by_name(self, name: str):
        """
        Retrieve a Xero contact by name. Returns the first match if multiple are found.
        If none exist, returns None.
        """
        self._refresh_token_if_needed()
        self.logger.info(f"🔎 Attempting to retrieve Xero contact by name: {name}")

        try:
            results = self._retry_on_unauthorized(self.xero.contacts.filter, Name=name)
            if results:
                self.logger.debug(f"✅ Found Xero contact(s) for '{name}': {results}")
                return results[0]
            self.logger.info(f"ℹ️ No matching contact found for '{name}' in Xero.")
            return None
        except XeroException as e:
            self.logger.error(f"❌ Error retrieving contact by name '{name}': {str(e)}")
            return None
        except Exception as e:
            self.logger.error(f"💥 Unexpected error retrieving contact: {str(e)}")
            return None

    def get_all_contacts(self):
        """
        Fetch all contacts from Xero. This may be large for bigger organizations,
        so consider pagination or filtering if performance is a concern.
        """
        self._refresh_token_if_needed()
        self.logger.info("📇 Fetching all contacts from Xero...")

        try:
            contacts = self._retry_on_unauthorized(self.xero.contacts.all)
            if not contacts:
                self.logger.info("ℹ️ No contacts found in Xero.")
                return []
            self.logger.debug(f"✅ Retrieved {len(contacts)} contact(s) from Xero.")
            return contacts
        except XeroException as e:
            self.logger.error(f"❌ Error retrieving all contacts: {str(e)}")
            return []
        except Exception as e:
            self.logger.error(f"💥 Unexpected error retrieving contacts: {str(e)}")
            return []

    def create_contact(self, contact_data: dict):
        """
        Create a new contact in Xero using the provided contact_data.
        """
        self._refresh_token_if_needed()
        self.logger.info(f"👤 Creating a new Xero contact with data: {contact_data}")

        try:
            created = self._retry_on_unauthorized(self.xero.contacts.put, [contact_data])
            self.logger.debug(f"✅ Successfully created contact in Xero: {created}")
            return created
        except XeroException as e:
            self.logger.error(f"❌ Failed to create contact in Xero: {str(e)}")
            return None
        except Exception as e:
            self.logger.error(f"💥 Unexpected error creating contact: {str(e)}")
            return None

    def update_contact(self, contact_data: dict):
        """
        Update an existing contact in Xero. The contact_data must include ContactID.
        """
        cid = contact_data.get('ContactID')
        self.logger.info(f"🔄 Attempting to update contact with ContactID={cid}")
        self._refresh_token_if_needed()

        try:
            updated_contacts = self._retry_on_unauthorized(self.xero.contacts.save, contact_data)
            if not updated_contacts:
                self.logger.error("❌ Xero returned an empty response for contact update.")
                return None

            self.logger.debug(f"🔍 Updated contact data: {updated_contacts}")
            return updated_contacts
        except XeroException as e:
            self.logger.error(f"❌ XeroException occurred updating contact: {str(e)}")
            return None
        except Exception as e:
            self.logger.error(f"💥 Unexpected error during contact update: {str(e)}")
            return None

    def update_contact_with_retry(self, contact_data, max_retries=3):
        """
        Attempts to update a Xero contact, retrying if we encounter rate-limit errors.
        """
        cid = contact_data.get("ContactID")
        self.logger.info(f"🔄 Attempting to update contact (ID={cid}) with retry up to {max_retries} times.")
        self._refresh_token_if_needed()

        for attempt in range(1, max_retries + 1):
            try:
                updated = self.xero.contacts.save(contact_data)
                return updated  # Xero returns a list of updated contacts
            except XeroRateLimitExceeded:
                self.logger.warning(
                    f"🔃 Rate limit hit. Attempt {attempt} of {max_retries}. "
                    f"Sleeping for 65 seconds before retry..."
                )
                time.sleep(65)
            except XeroException as xe:
                self.logger.error(f"❌ XeroException while updating contact: {xe}")
                return None
            except Exception as e:
                self.logger.error(f"💥 Unexpected error updating contact: {str(e)}")
                return None

        self.logger.error("❌ Failed to update contact after multiple rate-limit retries.")
        return None

    def update_contacts_with_retry(self, contacts_data: list[dict], max_retries=3):
        """
        Attempts to update multiple Xero contacts, retrying if we encounter rate-limit errors.
        """
        self.logger.info(
            f"🔄 Attempting batch update of {len(contacts_data)} contact(s) with up to {max_retries} retries..."
        )
        self._refresh_token_if_needed()

        for attempt in range(1, max_retries + 1):
            try:
                updated = self.xero.contacts.save(contacts_data)
                return updated  # Xero returns a list of updated contacts
            except XeroRateLimitExceeded:
                self.logger.warning(
                    f"🔃 Rate limit hit (update_contacts_with_retry). Attempt {attempt} of {max_retries}. "
                    f"Sleeping for 65 seconds before retry..."
                )
                time.sleep(65)
            except XeroException as xe:
                self.logger.error(f"❌ XeroException while updating contacts in batch: {xe}")
                return None
            except Exception as e:
                self.logger.error(f"💥 Unexpected error in batch contact update: {str(e)}")
                return None

        self.logger.error("❌ Failed to update contacts after multiple rate-limit retries.")
        return None
    # endregion

    # region 🤝 Concurrency Example
    def create_spend_money_in_batch(self, session, detail_item_ids: list[int]):
        """
        Example of concurrency to create multiple spend money items in parallel.
        """
        self.logger.info(f"🏎 Creating SPEND money in batch for detail_item_ids={detail_item_ids}")
        futures = []
        results = []

        with ThreadPoolExecutor(max_workers=5) as executor:
            for detail_item_id in detail_item_ids:
                futures.append(
                    executor.submit(self.create_spend_money, session, detail_item_id)
                )

            for future in as_completed(futures):
                try:
                    result = future.result()
                    results.append(result)
                except Exception as e:
                    self.logger.error(f"💥 Exception in thread for detail_item_id: {str(e)}")

        self.logger.info(f"✅ Batch creation complete. Results length={len(results)}")
        return results
    # endregion

# endregion

# Instantiate a global `XeroAPI` object you can import throughout your app
xero_api = XeroAPI()