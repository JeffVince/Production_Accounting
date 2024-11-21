# integrations/xero_api.py

from xero import Xero
from xero.auth import OAuth2Credentials
from utilities.config import Config
import datetime

class XeroAPI:
    def __init__(self):
        self.credentials = OAuth2Credentials(
            client_id=Config.XERO_CLIENT_ID,
            client_secret=Config.XERO_CLIENT_SECRET,
            tenant_id=Config.XERO_TENANT_ID
        )
        self.xero = Xero(self.credentials)

    def create_draft_bill(self, bill_data: dict):
        """Creates a draft bill in Xero."""
        bill_data['Status'] = 'DRAFT'
        result = self.xero.invoices.put(bill_data)
        return result

    def submit_bill_for_approval(self, bill_id: str):
        """Submits a bill for approval."""
        bill = {'InvoiceID': bill_id, 'Status': 'SUBMITTED'}
        result = self.xero.invoices.save(bill)
        return result

    def approve_bill(self, bill_id: str):
        """Approves a bill."""
        bill = {'InvoiceID': bill_id, 'Status': 'AUTHORISED'}
        result = self.xero.invoices.save(bill)
        return result

    def create_spend_money_transaction(self, transaction_data: dict):
        """Creates a Spend Money transaction."""
        transaction_data['Type'] = 'SPEND'
        result = self.xero.bank_transactions.put(transaction_data)
        return result

    def get_transaction_status(self, transaction_id: str):
        """Retrieves the status of a transaction."""
        transaction = self.xero.bank_transactions.get(transaction_id)
        return transaction

    def update_bill_status(self, bill_id: str, status: str):
        """Updates the status of a bill."""
        bill = {'InvoiceID': bill_id, 'Status': status}
        result = self.xero.invoices.save(bill)
        return result

    def reconcile_transaction(self, transaction_id: str):
        """Reconciles a transaction."""
        # Xero API does not provide direct reconciliation via API.
        # This is a placeholder for the reconciliation process.
        raise NotImplementedError("Reconciliation via API is not supported by Xero.")