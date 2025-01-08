# xero_services.py

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
                    "City": db_contact.get("city", "") or "",
                    "PostalCode": db_contact.get("zip", "") or "",
                    "Region": db_contact.get("region", "") or "",
                    "Country": db_contact.get("country", "") or ""

                },
                {
                    "AddressType": "POBOX",
                    "AddressLine1": db_contact.get("address_line_1", "") or "",
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
                    for field in ["AddressLine1", "City", "PostalCode", "Country", "Region"]:
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


# Instantiate a single instance
xero_services = XeroServices()