# services/mercury_service.py

import requests
from database.models import PO, Transaction, TransactionState, POState
from database.db_util import get_db_session
from database.mercury_repository import (
    add_or_update_transaction,
    update_transaction_status,
)
from utilities.config import Config
import logging

logger = logging.getLogger(__name__)

class MercuryService:
    def __init__(self):
        self.api_token = Config.MERCURY_API_TOKEN
        self.api_url = 'https://backend.mercury.com/api/v1'

    def initiate_payment(self, po_number: str, payment_data: dict):
        """Initiate a payment through Mercury Bank."""
        headers = {'Authorization': f'Bearer {self.api_token}'}
        payload = {
            # Construct the payment payload according to Mercury API
            'amount': payment_data['amount'],
            'recipient': payment_data['recipient'],
            'memo': payment_data.get('memo', ''),
        }
        try:
            response = requests.post(f'{self.api_url}/payments', json=payload, headers=headers)
            response.raise_for_status()
            payment_info = response.json()
            transaction_data = {
                'po_id': self.get_po_id(po_number),
                'transaction_id': payment_info['id'],
                'state': TransactionState.PENDING,
                'amount': payment_info['amount'],
            }
            add_or_update_transaction(transaction_data)
            logger.debug(f"Payment initiated for PO {po_number}")
        except Exception as e:
            logger.error(f"Error initiating payment for PO {po_number}: {e}")

    def monitor_payment_status(self, po_number: str):
        """Monitor the status of a payment."""
        with get_db_session() as session:
            transaction = self.get_transaction_by_po(po_number)
            if not transaction:
                logger.warning(f"No transaction found for PO {po_number}")
                return
            headers = {'Authorization': f'Bearer {self.api_token}'}
            try:
                response = requests.get(f'{self.api_url}/payments/{transaction.transaction_id}', headers=headers)
                response.raise_for_status()
                payment_info = response.json()
                new_state = payment_info['status'].upper()
                update_transaction_status(transaction.transaction_id, new_state)
                logger.debug(f"Payment status for PO {po_number} updated to {new_state}")
            except Exception as e:
                logger.error(f"Error monitoring payment status for PO {po_number}: {e}")

    def confirm_payment_execution(self, po_number: str):
        """Confirm that a payment has been executed."""
        self.monitor_payment_status(po_number)
        with get_db_session() as session:
            transaction = self.get_transaction_by_po(po_number)
            if transaction and transaction.state == TransactionState.PAID:
                # Update PO state to PAID
                po = session.query(PO).filter_by(id=transaction.po_id).first()
                po.state = POState.PAID
                session.commit()
                logger.debug(f"Payment confirmed for PO {po_number}, PO state updated to PAID")

    def get_po_id(self, po_number: str) -> int:
        with get_db_session() as session:
            po = session.query(PO).filter_by(po_number=po_number).first()
            if po:
                return po.id
            else:
                raise ValueError(f"PO {po_number} not found")

    def get_transaction_by_po(self, po_number: str) -> Transaction:
        with get_db_session() as session:
            po = session.query(PO).filter_by(po_number=po_number).first()
            if po:
                transaction = session.query(Transaction).filter_by(po_id=po.id).first()
                return transaction
            else:
                return None