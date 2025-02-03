# test_xero_api_integration.py
import os
import pytest
from dotenv import load_dotenv
from files_xero.xero_api import xero_api
from xero.exceptions import XeroUnauthorized

# Mark this file as integration tests.
pytestmark = pytest.mark.integration

load_dotenv("../../.env")

# Skip these tests if the required Xero credentials are not configured.
required_vars = [
    "XERO_CLIENT_ID",
    "XERO_CLIENT_SECRET",
    "XERO_ACCESS_TOKEN",
    "XERO_REFRESH_TOKEN",
    "XERO_TENANT_ID"
]
if not all(os.getenv(var) for var in required_vars):
    pytest.skip("Xero integration credentials are not configured", allow_module_level=True)

class TestXeroAPIIntegration:
    @pytest.fixture(autouse=True)
    def setup_api(self):
        # Create a real instance of XeroAPI using live (or sandbox) credentials from your environment.
        self.xero_api = xero_api
        self.xero_api.logger.setLevel("INFO")
        # Force a token refresh so that we have a valid token.
        try:
            self.xero_api._refresh_token_if_needed(force=True)
        except Exception as e:
            pytest.skip(f"Token refresh failed: {e}")
        yield

    # --- Helper function to gracefully skip if token is expired ---
    def skip_if_token_expired(self, func):
        try:
            return func()
        except XeroUnauthorized as e:
            pytest.skip(f"Skipped because token is expired or unauthorized: {e}")

    # --- GET Endpoints ---
    def test_get_invoices(self):
        """Test that GET /Invoices returns a list of invoices with expected structure."""
        invoices = self.skip_if_token_expired(lambda: self.xero_api.xero.invoices.all())
        assert isinstance(invoices, list), "Expected a list of invoices"
        if invoices:
            sample_invoice = invoices[0]
            assert "InvoiceID" in sample_invoice, "Invoice should have an InvoiceID"
            assert "Type" in sample_invoice, "Invoice should have a Type field"
            assert "Contact" in sample_invoice, "Invoice should include a Contact field"

    def test_get_contacts(self):
        """Test that GET /Contacts returns contacts with expected fields."""
        contacts = self.skip_if_token_expired(lambda: self.xero_api.xero.contacts.all())
        assert isinstance(contacts, list), "Expected a list of contacts"
        if contacts:
            sample_contact = contacts[0]
            assert "ContactID" in sample_contact, "Contact should have a ContactID"
            assert "Name" in sample_contact, "Contact should have a Name field"

    def test_get_accounts(self):
        """Test that GET /Accounts returns the chart of accounts."""
        accounts = self.skip_if_token_expired(lambda: self.xero_api.xero.accounts.all())
        assert isinstance(accounts, list), "Expected a list of accounts"
        if accounts:
            account = accounts[0]
            assert "AccountID" in account, "Account should have an AccountID"
            # Some organizations may return a 'Code' or a 'BankAccountNumber'
            assert "Code" in account or "BankAccountNumber" in account, \
                "Account should have a 'Code' or 'BankAccountNumber' field"

    def test_get_bank_transactions(self):
        """Test that GET /BankTransactions returns a list of bank transactions."""
        transactions = self.skip_if_token_expired(lambda: self.xero_api.xero.banktransactions.all())
        assert isinstance(transactions, list), "Expected a list of bank transactions"
        if transactions:
            sample_tx = transactions[0]
            assert "BankTransactionID" in sample_tx, "Transaction should have a BankTransactionID"

    def test_get_payments(self):
        """Test that GET /Payments returns a list of payments with expected structure."""
        payments = self.skip_if_token_expired(lambda: self.xero_api.xero.payments.all())
        assert isinstance(payments, list), "Expected a list of payments"
        if payments:
            sample_payment = payments[0]
            assert "PaymentID" in sample_payment, "Payment should have a PaymentID"
            assert "Invoice" in sample_payment, "Payment should include an Invoice field"

    # --- Token Refresh Test ---
    def test_refresh_token(self):
        """Force a token refresh and verify that environment tokens are updated."""
        self.xero_api._refresh_token_if_needed(force=True)
        access_token = os.getenv("XERO_ACCESS_TOKEN")
        refresh_token = os.getenv("XERO_REFRESH_TOKEN")
        assert access_token is not None and access_token != "", "XERO_ACCESS_TOKEN should be set after refresh."
        assert refresh_token is not None and refresh_token != "", "XERO_REFRESH_TOKEN should be set after refresh."

    # --- Destructive/State-Changing Endpoints ---
    def test_create_and_delete_invoice(self):
        """
        Create an invoice and then delete it.
        WARNING: This test creates and then deletes live data in Xero.
        Ensure you are using a sandbox/test organization and have a valid TEST_CONTACT_ID.
        """
        test_contact_id = "6b1be0ec-9bfa-4346-a4ae-74fe1c462267"
        if not test_contact_id:
            pytest.skip("TEST_CONTACT_ID not set for invoice creation integration test.")
        payload = {
            "Type": "ACCREC",
            "Contact": {"ContactID": test_contact_id},
            "Date": "2024-01-01",
            "DueDate": "2024-01-15",
            "LineAmountTypes": "Exclusive",
            "LineItems": [
                {
                    "Description": "Integration Test Service",
                    "Quantity": 1,
                    "UnitAmount": 100.00,
                    "TaxType": "OUTPUT",
                    "AccountCode": "200"
                }
            ]
        }
        create_response = self.xero_api.create_invoice(payload)
        assert create_response is not None, "Invoice creation failed; response is None."
        invoice_id = create_response[0].get("InvoiceID")
        assert invoice_id, "Created invoice does not have an InvoiceID."
        delete_response = self.xero_api.delete_invoice(invoice_id)
        if delete_response:
            assert delete_response[0].get("Status") == "DELETED", "Invoice was not marked as DELETED."

    def test_create_and_update_spend_money(self):
        """
        Create a spend money transaction and then update its status.
        WARNING: This test creates live spend money data in Xero.
        """
        # Instead of hardcoding a vendor name, try to use an existing contact's name.
        contacts = self.xero_api.xero.contacts.all()
        if not contacts:
            pytest.skip("No contacts available to use for spend money creation.")
        test_vendor = contacts[0].get("Name")
        spend_record = {
            "state": "DRAFT",
            "vendor": test_vendor,
            "description": "Integration Test Spend",
            "amount": 150.00,
            "Date": "2024-01-01",
            "CurrencyCode": "USD"
        }
        create_response = self.xero_api.create_spend_money_in_xero(spend_record)
        assert create_response is not None, "Spend money creation failed; response is None."
        assert isinstance(create_response, list), "Expected spend money creation response to be a list."
        if create_response:
            spend_tx_id = create_response[0].get("BankTransactionID")
            assert spend_tx_id, "Created spend money transaction does not have an ID."
            update_response = self.xero_api.update_spend_money(spend_tx_id, "PAID")
            if update_response:
                assert update_response[0].get("Status") == "PAID", "Spend money transaction status was not updated to PAID."

    # --- Additional GET Endpoint Integration Tests ---
    def test_invoices_endpoint_sample(self):
        """(Optional) Verify GET /Invoices returns data with key fields."""
        invoices = self.skip_if_token_expired(lambda: self.xero_api.xero.invoices.all())
        assert isinstance(invoices, list)
        if invoices:
            invoice = invoices[0]
            assert "InvoiceID" in invoice
            assert "Type" in invoice
            assert "Contact" in invoice

    def test_contacts_endpoint_sample(self):
        contacts = self.skip_if_token_expired(lambda: self.xero_api.xero.contacts.all())
        assert isinstance(contacts, list)
        if contacts:
            contact = contacts[0]
            assert "ContactID" in contact
            assert "Name" in contact

    def test_accounts_endpoint_sample(self):
        accounts = self.skip_if_token_expired(lambda: self.xero_api.xero.accounts.all())
        assert isinstance(accounts, list)
        if accounts:
            account = accounts[0]
            assert "AccountID" in account
            assert "Code" in account or "BankAccountNumber" in account

    def test_bank_transactions_endpoint_sample(self):
        transactions = self.skip_if_token_expired(lambda: self.xero_api.xero.banktransactions.all())
        assert isinstance(transactions, list)
        if transactions:
            tx = transactions[0]
            assert "BankTransactionID" in tx

    def test_payments_endpoint_sample(self):
        payments = self.skip_if_token_expired(lambda: self.xero_api.xero.payments.all())
        assert isinstance(payments, list)
        if payments:
            payment = payments[0]
            assert "PaymentID" in payment
            assert "Invoice" in payment