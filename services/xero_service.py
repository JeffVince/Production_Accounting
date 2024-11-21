# services/xero_service.py

from xero import Xero
from xero.auth import OAuth2Credentials
from database.models import PO, Bill, BillState, SpendMoneyTransaction, SpendMoneyState
from database.db_util import get_db_session
from database.xero_repository import (
    add_or_update_bill,
    update_bill_status,
    add_or_update_spend_money_transaction,
)
from utilities.config import Config
import logging

logger = logging.getLogger(__name__)

class XeroService:
    def __init__(self):
        self.credentials = OAuth2Credentials(
            client_id=Config.XERO_CLIENT_ID,
            client_secret=Config.XERO_CLIENT_SECRET,
            tenant_id=Config.XERO_TENANT_ID,
        )
        self.xero = Xero(self.credentials)

    def generate_draft_bill(self, po_number: str):
        """Generate a draft bill in Xero for a PO."""
        with get_db_session() as session:
            po = session.query(PO).filter_by(po_number=po_number).first()
            if not po:
                logger.warning(f"PO {po_number} not found")
                return
            contact = {'Name': po.vendor.vendor_name} if po.vendor else {'Name': 'Unknown Vendor'}
            line_items = [{
                'Description': po.description,
                'Quantity': 1,
                'UnitAmount': po.amount,
            }]
            bill_data = {
                'Type': 'ACCPAY',
                'Contact': contact,
                'LineItems': line_items,
                'Status': 'DRAFT',
            }
            # Send to Xero
            try:
                bills = self.xero.invoices.put(bill_data)
                xero_bill = bills[0]
                bill_record = {
                    'po_id': po.id,
                    'bill_id': xero_bill['InvoiceID'],
                    'state': BillState.DRAFT,
                    'amount': po.amount,
                    'due_date': xero_bill.get('DueDate'),
                }
                add_or_update_bill(bill_record)
                logger.debug(f"Draft bill generated in Xero for PO {po_number}")
            except Exception as e:
                logger.error(f"Error generating draft bill in Xero: {e}")

    def prepare_bill_for_submission(self, po_number: str):
        """Prepare the bill for submission in Xero."""
        # Implementation logic
        update_bill_status_by_po(po_number, BillState.SUBMITTED)

    def submit_bill(self, po_number: str):
        """Submit the bill in Xero for approval."""
        # Implementation logic
        update_bill_status_by_po(po_number, BillState.SUBMITTED)

    def monitor_bill_approval(self, po_number: str):
        """Monitor the approval status of a bill."""
        # Implementation logic, possibly polling or webhook

    def create_spend_money_transaction(self, po_number: str, transaction_data: dict):
        """Create a Spend Money transaction in Xero."""
        with get_db_session() as session:
            po = session.query(PO).filter_by(po_number=po_number).first()
            if not po:
                logger.warning(f"PO {po_number} not found")
                return
            transaction = {
                'Type': 'SPEND',
                'Contact': {'Name': po.vendor.vendor_name} if po.vendor else {'Name': 'Unknown Vendor'},
                'LineItems': [{
                    'Description': transaction_data.get('description', po.description),
                    'Quantity': 1,
                    'UnitAmount': transaction_data.get('amount', po.amount),
                }],
                'Status': 'DRAFT',
            }
            # Send to Xero
            try:
                transactions = self.xero.bank_transactions.put(transaction)
                xero_transaction = transactions[0]
                spend_money_record = {
                    'transaction_id': xero_transaction['BankTransactionID'],
                    'po_id': po.id,
                    'state': SpendMoneyState.DRAFT,
                    'amount': xero_transaction['Total'],
                    'description': xero_transaction['LineItems'][0]['Description'],
                }
                add_or_update_spend_money_transaction(spend_money_record)
                logger.debug(f"Spend Money transaction created in Xero for PO {po_number}")
            except Exception as e:
                logger.error(f"Error creating Spend Money transaction in Xero: {e}")

    def reconcile_transaction(self, po_number: str):
        """Reconcile a transaction in Xero."""
        # Implementation logic, possibly updating the status
        update_bill_status_by_po(po_number, BillState.RECONCILED)

# Helper function
def update_bill_status_by_po(po_number: str, status: BillState):
    with get_db_session() as session:
        po = session.query(PO).filter_by(po_number=po_number).first()
        if po:
            bill = session.query(Bill).filter_by(po_id=po.id).first()
            if bill:
                update_bill_status(bill.bill_id, status)
                logger.debug(f"Bill status updated to {status.value} for PO {po_number}")
            else:
                logger.warning(f"No bill found for PO {po_number}")
        else:
            logger.warning(f"PO {po_number} not found")