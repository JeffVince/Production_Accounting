# services/spend_money_service.py

from database.models import PO, SpendMoneyTransaction, SpendMoneyState
from database.db_util import get_db_session
from xero_service import XeroService
import logging

logger = logging.getLogger(__name__)

class SpendMoneyService:
    def __init__(self):
        self.xero_service = XeroService()

    def process_credit_card_transactions(self, po_number: str):
        """Process credit card transactions related to a PO."""
        # Implementation logic
        transaction_data = {'amount': 500.0, 'description': 'Credit Card Expense'}
        self.xero_service.create_spend_money_transaction(po_number, transaction_data)
        self.update_spend_money_status(po_number, SpendMoneyState.APPROVED)

    def process_petty_cash_transactions(self, po_number: str):
        """Process petty cash transactions related to a PO."""
        # Implementation logic
        transaction_data = {'amount': 100.0, 'description': 'Petty Cash Expense'}
        self.xero_service.create_spend_money_transaction(po_number, transaction_data)
        self.update_spend_money_status(po_number, SpendMoneyState.APPROVED)

    def validate_spend_money_totals(self, po_number: str) -> bool:
        """Validate totals for Spend Money transactions."""
        # Implementation logic
        return True

    def submit_spend_money_to_xero(self, po_number: str, transaction_data: dict):
        """Submit Spend Money transactions to Xero."""
        self.xero_service.create_spend_money_transaction(po_number, transaction_data)

    def update_spend_money_status(self, po_number: str, status: SpendMoneyState):
        """Update the status of Spend Money transactions."""
        with get_db_session() as session:
            po = session.query(PO).filter_by(po_number=po_number).first()
            if po:
                for transaction in po.spend_money_transactions:
                    transaction.state = status
                session.commit()
                logger.debug(f"Spend Money transactions for PO {po_number} updated to {status.value}")
            else:
                logger.warning(f"PO {po_number} not found")