from services.spend_money_service import SpendMoneyService
from tests.base_test import BaseTestCase
from unittest.mock import MagicMock
from database.models import PO, SpendMoneyTransaction, SpendMoneyState


class TestSpendMoneyService(BaseTestCase):
    def setUp(self):
        super().setUp()
        self.service = SpendMoneyService()
        self.service.xero_service = MagicMock()

        # Add test data
        with self.session_scope() as session:
            po = PO(po_number='PO123')
            transaction = SpendMoneyTransaction(
                transaction_id='SM123',
                po=po,
                amount=500.0,
                description='Test Transaction',
                state=SpendMoneyState.DRAFT
            )
            session.add(transaction)

    def test_update_spend_money_status(self):
        # Update the transaction's status
        self.service.update_spend_money_status('PO123', SpendMoneyState.APPROVED)

        # Verify the status update
        with self.session_scope() as session:
            transaction = session.query(SpendMoneyTransaction).filter_by(transaction_id='SM123').first()
            self.assertEqual(transaction.state, SpendMoneyState.APPROVED)

    def test_process_credit_card_transactions(self):
        # Process credit card transactions
        self.service.process_credit_card_transactions('PO123')

        # Verify the Xero service call
        self.service.xero_service.create_spend_money_transaction.assert_called()

    def test_process_petty_cash_transactions(self):
        # Process petty cash transactions
        self.service.process_petty_cash_transactions('PO123')

        # Verify the Xero service call
        self.service.xero_service.create_spend_money_transaction.assert_called()