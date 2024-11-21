# integrations/mercury_bank_api.py

import requests
from utilities.config import Config

class MercuryBankAPI:
    def __init__(self):
        self.api_token = Config.MERCURY_API_TOKEN
        self.api_url = 'https://backend.mercury.com/api/v1'

    def _make_request(self, method: str, endpoint: str, data: dict = None, params: dict = None):
        headers = {'Authorization': f'Bearer {self.api_token}'}
        url = f'{self.api_url}{endpoint}'
        response = requests.request(method, url, json=data, params=params, headers=headers)
        response.raise_for_status()
        return response.json()

    def create_payment_transaction(self, transaction_data: dict):
        """Creates a payment transaction."""
        endpoint = '/payments'
        result = self._make_request('POST', endpoint, data=transaction_data)
        return result

    def get_transaction_status(self, transaction_id: str):
        """Gets the status of a transaction."""
        endpoint = f'/payments/{transaction_id}'
        result = self._make_request('GET', endpoint)
        return result

    def execute_payment(self, transaction_id: str):
        """Executes a payment transaction."""
        endpoint = f'/payments/{transaction_id}/execute'
        result = self._make_request('POST', endpoint)
        return result

    def fetch_transactions(self, filters: dict = None):
        """Fetches transactions based on filters."""
        endpoint = '/transactions'
        result = self._make_request('GET', endpoint, params=filters)
        return result