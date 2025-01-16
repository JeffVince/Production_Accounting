# xero_files.xero_services.py

import logging
import time

from xero.exceptions import XeroNotFound, XeroRateLimitExceeded, XeroException

from database_util import DatabaseOperations
from singleton import SingletonMeta
from xero_files.xero_api import xero_api


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
        self.logger = logging.getLogger("app_logger")
        self.logger.debug("Initialized XeroServices.")
        self._initialized = True

    def load_spend_money_transactions(
            self,
            project_id: int = None,
            po_number: int = None,
            detail_number: int = None
        ):
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
        self.logger.info("Retrieving SPEND transactions from Xero...")
        xero_spend_transactions = self.xero_api.get_spend_money_by_reference(
            project_id=project_id,
            po_number=po_number,
            detail_number=detail_number
        )

        if not xero_spend_transactions:
            self.logger.info("No SPEND transactions returned from Xero for the provided filters.")
            return

        for tx in xero_spend_transactions:
            # Extract fields you care about from each Xero transaction.

            # If Xero indicates it's reconciled, override status
            # Otherwise, default to Xero's status or 'DRAFT'
            if tx.get("IsReconciled", False) is True:
                current_state = "RECONCILED"
            else:
                current_state = tx.get("Status", "DRAFT")

            reference_number = tx.get("Reference")
            bank_transaction_id = tx.get("BankTransactionID")

            # Example for storing a link to the transaction in Xero's UI
            xero_link = f"https://go.xero.com/Bank/ViewTransaction.aspx?bankTransactionID={bank_transaction_id}"

            # Check if there's an existing SpendMoney with this reference
            existing_spend = self.database_util.search_spend_money(
                column_names=["xero_spend_money_reference_number"],
                values=[reference_number]
            )

            if not existing_spend:
                # Create a new SpendMoney record
                created = self.database_util.create_spend_money(
                    xero_spend_money_reference_number=reference_number,
                    xero_link=xero_link,
                    state=current_state,
                )
                if created:
                    self.logger.info(
                        f"Created new SpendMoney record for reference={reference_number}, ID={created['id']}."
                    )
                else:
                    self.logger.error(
                        f"Failed to create SpendMoney for reference={reference_number}."
                    )
            else:
                # If existing_spend is a list, handle multiple matches or just use the first
                if isinstance(existing_spend, list):
                    existing_spend = existing_spend[0]

                spend_money_id = existing_spend["id"]
                updated = self.database_util.update_spend_money(
                    spend_money_id,
                    state=current_state,
                    xero_link=xero_link
                )
                if updated:
                    self.logger.info(
                        f"Updated SpendMoney (id={spend_money_id}) for reference={reference_number}."
                    )
                else:
                    self.logger.error(
                        f"Failed to update SpendMoney (id={spend_money_id}) for reference={reference_number}."
                    )

    def populate_xero_contacts(self):
        """
        Retrieve all contacts from the local DB, retrieve all contacts from Xero,
        compare them, and then perform a single batch update (only for those that need changes).
        """
        self.logger.info("🚀 Starting to populate Xero contacts from the local DB in a single batch...")

        # 1) Retrieve all contacts from the local DB
        db_contacts = self.database_util.search_contacts()
        self.logger.info(f"Found {len(db_contacts)} contacts in the local DB to process.")

        # 2) Retrieve all contacts from Xero
        self.logger.info("Retrieving all contacts from Xero...")
        try:
            all_xero_contacts = self.xero_api.get_all_contacts()
        except XeroException as xe:
            self.logger.error(f"Failed to retrieve contacts from Xero: {xe}")
            return

        # Build a dictionary with contact name as key for quick lookup
        xero_contacts_dict = {
            contact["Name"].strip().lower(): contact for contact in all_xero_contacts
            if isinstance(contact.get("Name"), str) and contact.get("Name").strip()
        }

        # We'll accumulate all contacts that actually need updating in Xero
        contacts_to_update = []

        # 3) Compare each DB contact to its counterpart in Xero
        for db_contact in db_contacts:
            errors = self.validate_xero_data(db_contact)
            if errors:
                self.logger.error(
                    f"Skipping contact '{db_contact.get('name', 'Unnamed')}' due to validation errors: {errors}"
                )
                continue

            contact_name = db_contact["name"]
            self.logger.info(f"🔎 Checking if there's a matching Xero contact for '{contact_name}'")

            # Use lowercase to match our dictionary keys
            xero_match = xero_contacts_dict.get(contact_name.strip().lower())

            if not xero_match:
                msg = f"No matching Xero contact found for: '{contact_name}' ❌"
                self.logger.warning(msg)
                continue

            # Extract the existing Xero data
            contact_id = xero_match["ContactID"]
            xero_tax_number = xero_match.get("TaxNumber", "") or ""
            xero_addresses = xero_match.get("Addresses", [])
            xero_email = xero_match.get("EmailAddress") or ""
            # Prepare the fields to update
            tax_number = str(db_contact.get("tax_number")) if db_contact.get("tax_number") else ""
            email = db_contact["email"]
            # 3a) Format or handle the SSN if needed
            # For example, if Xero requires XXX-XX-XXXX but you only have digits:
            if tax_number and len(tax_number) == 9 and tax_number.isdigit():
                # Transform 123456789 -> 123-45-6789
                formatted_ssn = f"{tax_number[0:3]}-{tax_number[3:5]}-{tax_number[5:]}"
                self.logger.debug(f"Formatting SSN from '{tax_number}' to '{formatted_ssn}' for '{contact_name}'.")
                tax_number = formatted_ssn

            address_data = [
                {
                    "AddressType": "STREET",
                    "AddressLine1": db_contact.get("address_line_1", "") or "",
                    "AddressLine2": db_contact.get("address_line_2", "") or "",
                    "City": db_contact.get("city", "") or "",
                    "PostalCode": db_contact.get("zip", "") or "",
                    "Region": db_contact.get("region", "") or "",
                    "Country": db_contact.get("country", "") or ""

                },
                {
                    "AddressType": "POBOX",
                    "AddressLine1": db_contact.get("address_line_1", "") or "",
                    "AddressLine2": db_contact.get("address_line_2", "") or "",
                    "City": db_contact.get("city", "") or "",
                    "PostalCode": db_contact.get("zip", "") or "",
                    "Region": db_contact.get("region", "") or "",
                    "Country": db_contact.get("country", "") or ""
                }
            ]

            # 3b) Compare existing Xero data vs. new data
            need_update = False

            # Compare tax number
            if xero_tax_number != tax_number:
                need_update = True
                self.logger.debug(
                    f"Tax number changed for '{contact_name}' from '{xero_tax_number}' to '{tax_number}'."
                )

            if email != xero_email:
                need_update = True
                self.logger.debug(
                    f"Email changed for '{contact_name}' from '{xero_email}' to '{email}'."
                )

            # Compare addresses
            # Make sure we have at least 2 in Xero to compare; otherwise, update is needed
            if len(xero_addresses) < 2:
                self.logger.debug(
                    f"Xero contact '{contact_name}' has fewer than 2 addresses stored. Triggering update."
                )
                need_update = True
            else:
                # Compare the first two addresses
                for idx in range(2):
                    old = xero_addresses[idx]
                    new = address_data[idx]
                    for field in ["AddressLine1", "AddressLine2", "City", "PostalCode", "Country", "Region"]:
                        if old.get(field, "") != new.get(field, ""):
                            self.logger.debug(
                                f"Address {idx} field '{field}' changed for '{contact_name}' "
                                f"from '{old.get(field, '')}' to '{new.get(field, '')}'."
                            )
                            need_update = True
                            break

            # 3c) Only add to batch if there's an actual change
            if need_update:
                updated_contact_data = {
                    "ContactID": contact_id,
                    "Name": db_contact["name"],
                    "Email": email,
                    "TaxNumber": tax_number,
                    "Addresses": address_data,
                }
                contacts_to_update.append(updated_contact_data)
            else:
                self.logger.info(f"🎉  No change needed for '{contact_name}'.")

        # 4) Perform a single batch update if there are any contacts to update
        if contacts_to_update:
            self.logger.info(
                f"💾 Sending a batch update for {len(contacts_to_update)} Xero contacts..."
            )
            try:
                # Assumes you have a method in xero_api to handle batch updates
                self.xero_api.update_contacts_with_retry(contacts_to_update)
                self.logger.info(
                    f"🎉 Successfully updated {len(contacts_to_update)} Xero contacts in a single batch."
                )
            except XeroException as xe:
                self.logger.error(f"XeroException while updating contacts in batch: {xe}")
            except Exception as e:
                self.logger.debug(f"Debugging the exception object: type={type(e)}, repr={repr(e)}")
                error_message = f"⚠️ Error in batch update: {e}"
                self.logger.error(error_message)
        else:
            self.logger.info("No contacts required updating in Xero.")

        self.logger.info("🏁 Finished populating Xero contacts from the local DB in a single batch.")

    def validate_xero_data(self, db_contact):
        """
        Validate the DB contact for required fields, address formats, etc.
        Returns a list of error messages, empty if no errors.
        """
        errors = []

        # Check mandatory fields
        if not db_contact.get("name"):
            errors.append("❗ Missing or empty name.")

        # Validate address fields
        address = {
            "AddressLine1": db_contact.get("address_line_1", ""),
            "AddressLine2": db_contact.get("address_line_2", ""),
            "City": db_contact.get("city", ""),
            "PostalCode": db_contact.get("zip", ""),
        }
        if len(address["AddressLine1"]) > 255:
            errors.append("❗ AddressLine1 exceeds character limit.")
        if len(address["City"]) > 255:
            errors.append("❗ City exceeds character limit.")
        if len(address["PostalCode"]) > 50:  # Example length limit
            errors.append("❗ PostalCode exceeds character limit.")

        # Tax Number validation
        tax_number = str(db_contact.get("tax_number", ""))
        if tax_number and not tax_number.isalnum():
            errors.append("❗ TaxNumber contains invalid characters.")

        return errors

    # xero_services.py (within the XeroServices class)

    def load_bills(self, project_number: str):
        """
        1) Retrieve summary of ACCPAY invoices matching project_number in InvoiceNumber.
        2) For each invoice, retrieve full details (line items).
        3) For each line item, match or create a local BillLineItem with parent_xero_id
           (and project_number, po_number, detail_number, etc.), then link to XeroBill.
        """
        self.logger.info(
            f"Retrieving ACCPAY invoices from Xero where InvoiceNumber contains '{project_number}'..."
        )
        # Step 1: fetch summary of matching invoices
        summaries = self.xero_api.get_acpay_invoices_summary_by_ref(project_number)
        if not summaries:
            self.logger.info(
                f"No ACCPAY invoices found with InvoiceNumber containing '{project_number}'."
            )
            return

        for summary_inv in summaries:
            invoice_id = summary_inv.get("InvoiceID")
            invoice_number = summary_inv.get("InvoiceNumber", "")
            status = summary_inv.get("Status", "DRAFT")

            self.logger.info(
                f"Fetching full details for InvoiceNumber={invoice_number} (ID={invoice_id})..."
            )

            # Step 2: retrieve full invoice details (line items)
            full_inv = self.xero_api.get_invoice_details(invoice_id)
            if not full_inv:
                self.logger.warning(f"Skipping InvoiceID={invoice_id}, no line items returned.")
                continue

            line_items = full_inv.get("LineItems", [])
            self.logger.debug(
                f"Invoice {invoice_number} has {len(line_items)} line item(s)."
            )

            # Derive project_number, po_number, detail_number from invoice_number if in format "2416_32_1"
            parts = invoice_number.split("_")
            if len(parts) >= 2:
                try:
                    project_num = int(parts[0])  # "2416"
                    po_num = int(parts[1])       # "32"
                    if len(parts) >= 3:
                        detail_num = int(parts[2])
                    else:
                        detail_num = 1
                except ValueError:
                    self.logger.warning(
                        f"InvoiceNumber='{invoice_number}' not in numeric format. Skipping line item match."
                    )
                    continue
            else:
                self.logger.warning(
                    f"InvoiceNumber='{invoice_number}' doesn't have at least two parts. Skipping line item match."
                )
                continue

            # Create or update XeroBill
            existing_bill = self.database_util.search_xero_bills(
                column_names=["xero_reference_number"],
                values=[invoice_number]
            )

            if not existing_bill:
                created_bill = self.database_util.create_xero_bill(
                    xero_reference_number=invoice_number,
                    xero_id=invoice_id,  # Store the Xero InvoiceID
                    state=status,
                    project_number=project_num,
                    po_number=po_num,
                    detail_number=detail_num,
                    xero_link=f"https://go.xero.com/AccountsPayable/View.aspx?invoiceId={invoice_id}"
                )
                if not created_bill:
                    self.logger.error(f"Failed to create xero_bill for {invoice_number}. Skipping line items.")
                    continue
                xero_bill_id = created_bill["id"]
            else:
                if isinstance(existing_bill, list):
                    existing_bill = existing_bill[0]
                xero_bill_id = existing_bill["id"]
                self.logger.info(f"Updating existing xero_bill (ID={xero_bill_id}) to status={status}.")
                self.database_util.update_xero_bill(
                    xero_bill_id,
                    xero_id=invoice_id,
                    project_number=project_num,
                    po_number=po_num,
                    detail_number=detail_num,
                    state=status,
                    xero_link=f"https://go.xero.com/AccountsPayable/View.aspx?invoiceId={invoice_id}"
                )

            # Step 3: For each line item, match or create BillLineItem by parent_xero_id
            for idx, li in enumerate(line_items):
                # Extract line item fields from Xero
                xero_line_number = li.get("LineItemID")  # e.g. '6fe99992-2544-4a3a-b53b-83a2ae70383f'
                description = li.get("Description")
                quantity = li.get("Quantity")
                unit_amount = li.get("UnitAmount")
                line_amount = li.get("LineAmount")
                account_code_str = li.get("AccountCode")  # Typically a string
                account_code = None

                # Convert AccountCode to int if possible
                if account_code_str:
                    try:
                        account_code = int(account_code_str)
                    except ValueError:
                        self.logger.warning(f"AccountCode '{account_code_str}' is not an integer. Using None.")

                # Try to find an existing BillLineItem by parent_xero_id
                existing_line = self.database_util.search_bill_line_items(
                    column_names=["parent_xero_id"],
                    values=[xero_line_number]
                )

                if existing_line:
                    # If multiple found, use the first
                    if isinstance(existing_line, list):
                        existing_line = existing_line[0]

                    bill_line_item_id = existing_line["id"]

                    # Update existing BillLineItem
                    self.logger.info(
                        f"Updating existing BillLineItem (ID={bill_line_item_id}) with Xero line data."
                    )
                    updated_line = self.database_util.update_bill_line_item(
                        bill_line_item_id,
                        description=description,
                        quantity=quantity,
                        unit_amount=unit_amount,
                        line_amount=line_amount,
                        account_code=account_code
                    )
                    if updated_line:
                        self.logger.debug(f"BillLineItem (ID={bill_line_item_id}) successfully updated.")
                    else:
                        self.logger.error(
                            f"Failed to update BillLineItem (ID={bill_line_item_id})."
                        )

                else:
                    # If no existing record, create new
                    self.logger.info(f"No BillLineItem found for parent_xero_id={xero_line_number}. Creating a new one.")

                    new_line = self.database_util.create_bill_line_item(

                        # In case you also store another parent reference
                        parent_id=xero_bill_id,

                        # Store the Xero line item ID
                        parent_xero_id=xero_line_number,

                        # Project/PO/Detail info
                        project_number=project_num,
                        po_number=po_num,
                        detail_number=detail_num,

                        # Possibly store an integer line_number if you want
                        line_number=idx + 1,

                        # Basic fields from Xero
                        description=description,
                        quantity=quantity,
                        unit_amount=unit_amount,
                        line_amount=line_amount,
                        account_code=account_code
                    )
                    if new_line:
                        self.logger.debug(
                            f"Created BillLineItem with ID={new_line['id']} and parent_xero_id={xero_line_number}."
                        )
                    else:
                        self.logger.error(
                            f"Failed to create BillLineItem for parent_xero_id={xero_line_number}."
                        )

        self.logger.info(
            f"Finished loading ACCPAY invoices matching '{project_number}', with line items mapped to local BillLineItems."
        )


# Instantiate a single instance
xero_services = XeroServices()