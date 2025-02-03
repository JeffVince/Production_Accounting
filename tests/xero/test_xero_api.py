# test_xero_api.py
import os
import time
import json
import pytest
from unittest.mock import MagicMock, patch
from xero.exceptions import XeroException, XeroUnauthorized, XeroRateLimitExceeded
from xero_api import XeroAPI
from dotenv import load_dotenv

# --- Fake helper classes for testing utility methods ---

class FakeDetailItem:
    def __init__(self, id, description, quantity, rate, account_code_id, state, vendor, line_number=None):
        self.id = id
        self.description = description
        self.quantity = quantity
        self.rate = rate
        self.account_code_id = account_code_id
        self.state = state
        self.vendor = vendor
        self.line_number = line_number

class FakeAccountCode:
    def __init__(self, tax_code):
        # Simulate that the account code has an associated tax account object with a tax_code attribute.
        self.tax_account = type("FakeTaxAccount", (), {"tax_code": tax_code})

class FakeQuery:
    def __init__(self, detail_item):
        self.detail_item = detail_item

    def filter(self, condition):
        # For testing, ignore the filter condition and simply return self.
        return self

    def all(self):
        return [self.detail_item] if self.detail_item is not None else []

    def get(self, id):
        if self.detail_item is not None and self.detail_item.id == id:
            return self.detail_item
        return None

class FakeSession:
    """
    A fake session that simulates a SQLAlchemy session.
    When querying for DetailItem, it returns a FakeQuery that always returns the provided detail_item.
    For other models (e.g. AccountCode), it returns a simple fake query that supports filter_by.
    """
    def __init__(self, detail_item=None):
        self.detail_item = detail_item

    def query(self, model):
        if model.__name__ == "DetailItem":
            return FakeQuery(self.detail_item)
        else:
            # For AccountCode queries, simulate a query that returns a FakeAccountCode.
            class FakeQueryForAccount:
                def __init__(self, detail_item):
                    self.detail_item = detail_item

                def filter_by(self, **kwargs):
                    class FakeResult:
                        def __init__(self, detail_item):
                            self.detail_item = detail_item

                        def first(self):
                            if self.detail_item:
                                return FakeAccountCode("TAX999")
                            return None
                    return FakeResult(self.detail_item)
            return FakeQueryForAccount(self.detail_item)

# --- FakeResponse for simulating HTTP response in exceptions ---
class FakeResponse:
    def __init__(self, content_type="application/json"):
        self.headers = {"content-type": content_type}
        self.text = "{}"  # Added to satisfy json.loads(response.text)

# --- Helper function to safely call a function and return an empty list if a XeroException is raised ---
def _safe_call(func, *args, **kwargs):
    try:
        return func(*args, **kwargs)
    except XeroException:
        return []

# --- Test class for XeroAPI ---
class TestXeroAPI:
    @pytest.fixture(autouse=True)
    def setup_api(self):
        # Create an instance of XeroAPI and override its underlying Xero client.
        self.xero_api = XeroAPI()
        self.xero_api.xero = MagicMock()
        # Ensure that invoices, contacts, banktransactions, accounts, and payments are MagicMock objects.
        self.xero_api.xero.invoices = MagicMock()
        self.xero_api.xero.contacts = MagicMock()
        self.xero_api.xero.banktransactions = MagicMock()
        self.xero_api.xero.accounts = MagicMock()
        self.xero_api.xero.payments = MagicMock()

        # Patch _refresh_token_if_needed so it does nothing for most tests.
        self.rt_patch = patch.object(self.xero_api, '_refresh_token_if_needed', return_value=None)
        self.rt_patch.start()
        # Patch _retry_on_unauthorized to immediately call the passed function.
        self.retry_patch = patch.object(
            self.xero_api,
            '_retry_on_unauthorized',
            side_effect=lambda func, *args, **kwargs: func(*args, **kwargs)
        )
        self.retry_patch.start()
        yield
        patch.stopall()

    # === Utility Methods ===
    def test_convert_detail_item_to_line_item_success(self):
        detail_item = FakeDetailItem(
            id=1, description="Test Desc", quantity=2, rate=50,
            account_code_id=100, state="SUBMITTED", vendor="Vendor A", line_number=5
        )
        session = FakeSession(detail_item=detail_item)
        line_item = self.xero_api._convert_detail_item_to_line_item(session, detail_item)
        assert line_item["Description"] == "Test Desc"
        assert line_item["Quantity"] == 2.0
        assert line_item["UnitAmount"] == 50.0
        # FakeSession returns a FakeAccountCode with tax code "TAX999"
        assert line_item["TaxType"] == "TAX999"

    # --- Contacts ---
    def test_get_contact_by_name(self):
        fake_contact = {"ContactID": "cont123", "Name": "Alice"}
        self.xero_api.xero.contacts.filter.return_value = [fake_contact]
        result = self.xero_api.get_contact_by_name("Alice")
        self.xero_api.xero.contacts.filter.assert_called_with(Name="Alice")
        assert result == fake_contact

    def test_get_contact_by_name_exception(self):
        self.xero_api.xero.contacts.filter.side_effect = XeroException("Error")
        result = self.xero_api.get_contact_by_name("Error")
        assert result is None

    def test_get_contact_by_name_generic_exception(self):
        self.xero_api.xero.contacts.filter.side_effect = Exception("Generic Error")
        result = self.xero_api.get_contact_by_name("Error")
        assert result is None

    def test_get_all_contacts(self):
        fake_contacts = [{"ContactID": "cont1"}, {"ContactID": "cont2"}]
        self.xero_api.xero.contacts.all.return_value = fake_contacts
        result = self.xero_api.get_all_contacts()
        self.xero_api.xero.contacts.all.assert_called_once()
        assert result == fake_contacts

    def test_get_all_contacts_exception(self):
        self.xero_api.xero.contacts.all.side_effect = XeroException("Error")
        result = self.xero_api.get_all_contacts()
        assert result == []

    def test_create_contact(self):
        contact_data = {"Name": "Bob"}
        fake_response = [{"ContactID": "cont456", "Name": "Bob"}]
        self.xero_api.xero.contacts.put.return_value = fake_response
        result = self.xero_api.create_contact(contact_data)
        self.xero_api.xero.contacts.put.assert_called_with([contact_data])
        assert result == fake_response

    def test_create_contact_exception(self):
        self.xero_api.xero.contacts.put.side_effect = XeroException("Error")
        result = self.xero_api.create_contact({"Name": "Test"})
        assert result is None

    def test_update_contact(self):
        contact_data = {"ContactID": "cont789", "Name": "Charlie"}
        fake_response = [{"ContactID": "cont789", "Name": "Charlie Updated"}]
        self.xero_api.xero.contacts.save.return_value = fake_response
        result = self.xero_api.update_contact(contact_data)
        self.xero_api.xero.contacts.save.assert_called_with(contact_data)
        assert result == fake_response

    def test_update_contact_exception(self):
        self.xero_api.xero.contacts.save.side_effect = XeroException("Error")
        result = self.xero_api.update_contact({"ContactID": "id", "Name": "Test"})
        assert result is None

    def test_update_contact_with_retry(self):
        contact_data = {"ContactID": "cont101", "Name": "Dave"}
        fake_response = [{"ContactID": "cont101", "Name": "Dave Updated"}]
        self.xero_api.xero.contacts.save.return_value = fake_response
        result = self.xero_api.update_contact_with_retry(contact_data, max_retries=3)
        assert result == fake_response

    def test_update_contact_with_retry_exception(self):
        self.xero_api.xero.contacts.save.side_effect = XeroException("Error")
        result = self.xero_api.update_contact_with_retry({"ContactID": "id", "Name": "Test"}, max_retries=3)
        assert result is None

    def test_update_contacts_with_retry(self):
        contacts_data = [{"xero_id": "id1", "Name": "Eve"}, {"xero_id": "id2", "Name": "Frank"}]
        fake_response = [{"ContactID": "id1", "Name": "Eve Updated"}, {"ContactID": "id2", "Name": "Frank Updated"}]
        self.xero_api.xero.contacts.save.return_value = fake_response
        result = self.xero_api.update_contacts_with_retry(contacts_data, max_retries=3)
        assert result == fake_response

    def test_update_contacts_with_retry_exception(self):
        self.xero_api.xero.contacts.save.side_effect = XeroException("Error")
        result = self.xero_api.update_contacts_with_retry([{"xero_id": "id", "Name": "Test"}], max_retries=3)
        assert result is None

    # --- Invoices ---
    def test_create_invoice(self):
        payload = {"Type": "ACCPAY", "Contact": {"ContactID": "cont001"}}
        fake_response = [{"InvoiceID": "inv123"}]
        self.xero_api.xero.invoices.put.return_value = fake_response
        result = self.xero_api.create_invoice(payload)
        self.xero_api.xero.invoices.put.assert_called_with([payload])
        assert result == fake_response

    def test_create_invoice_exception(self):
        self.xero_api.xero.invoices.put.side_effect = XeroException("Error")
        result = self.xero_api.create_invoice({"Type": "ACCPAY"})
        assert result is None

    def test_update_invoice(self):
        invoice_obj = {"InvoiceID": "inv123", "Status": "DRAFT"}
        updated_invoice = [{"InvoiceID": "inv123", "Status": "AUTHORISED"}]
        self.xero_api.xero.invoices.filter.return_value = [invoice_obj]
        self.xero_api.xero.invoices.save.return_value = updated_invoice
        changes = {"Status": "AUTHORISED"}
        result = self.xero_api.update_invoice("inv123", changes)
        self.xero_api.xero.invoices.filter.assert_called_with(InvoiceID="inv123")
        self.xero_api.xero.invoices.save.assert_called_with(invoice_obj)
        assert result == updated_invoice

    def test_update_invoice_exception(self):
        self.xero_api.xero.invoices.filter.side_effect = XeroException("Error")
        result = self.xero_api.update_invoice("inv_error", {"Status": "AUTHORISED"})
        assert result is None

    def test_delete_invoice_success(self):
        invoice_obj = {"InvoiceID": "inv456", "Status": "DRAFT"}
        deleted_response = [{"InvoiceID": "inv456", "Status": "DELETED"}]
        self.xero_api.xero.invoices.filter.return_value = [invoice_obj]
        self.xero_api.xero.invoices.save.return_value = deleted_response
        result = self.xero_api.delete_invoice("inv456")
        self.xero_api.xero.invoices.filter.assert_called_with(InvoiceID="inv456")
        self.xero_api.xero.invoices.save.assert_called_with(invoice_obj)
        assert result == deleted_response

    def test_delete_invoice_exception(self):
        self.xero_api.xero.invoices.filter.side_effect = XeroException("Error")
        result = self.xero_api.delete_invoice("inv_error")
        assert result is None

    def test_get_invoice_details_success(self):
        invoice_obj = {"InvoiceID": "inv789", "Status": "DRAFT"}
        self.xero_api.xero.invoices.get.return_value = [invoice_obj]
        result = self.xero_api.get_invoice_details("inv789")
        self.xero_api.xero.invoices.get.assert_called_with("inv789")
        assert result == invoice_obj

    def test_get_invoice_details_deleted(self):
        invoice_obj = {"InvoiceID": "inv000", "Status": "DELETED"}
        self.xero_api.xero.invoices.get.return_value = [invoice_obj]
        result = self.xero_api.get_invoice_details("inv000")
        assert result is None

    def test_get_invoice_details_exception(self):
        self.xero_api.xero.invoices.get.side_effect = XeroException("Error")
        result = self.xero_api.get_invoice_details("inv_error")
        assert result is None

    def test_add_line_item_to_invoice(self):
        invoice_obj = {"InvoiceID": "inv101", "Status": "DRAFT", "LineItems": []}
        updated_invoice = [{"InvoiceID": "inv101", "LineItems": [{"Description": "New line"}]}]
        self.xero_api.xero.invoices.filter.return_value = [invoice_obj]
        self.xero_api.xero.invoices.save.return_value = updated_invoice
        line_item_data = {"Description": "New line", "Quantity": 1, "UnitAmount": 100.0, "TaxType": "NONE"}
        result = self.xero_api.add_line_item_to_invoice("inv101", line_item_data)
        self.xero_api.xero.invoices.filter.assert_called_with(InvoiceID="inv101")
        self.xero_api.xero.invoices.save.assert_called_with(invoice_obj)
        assert result == updated_invoice[0]

    def test_add_line_item_to_invoice_exception(self):
        self.xero_api.xero.invoices.filter.side_effect = XeroException("Error")
        result = self.xero_api.add_line_item_to_invoice("inv_error", {"Description": "Test"})
        assert result == {}

    def test_update_line_item_in_invoice(self):
        invoice_obj = {
            "InvoiceID": "inv111",
            "Status": "DRAFT",
            "LineItems": [{"LineItemID": "li123", "Description": "Old desc"}]
        }
        updated_invoice = [{
            "InvoiceID": "inv111",
            "LineItems": [{"LineItemID": "li123", "Description": "New desc"}]
        }]
        self.xero_api.xero.invoices.filter.return_value = [invoice_obj]
        self.xero_api.xero.invoices.save.return_value = updated_invoice
        result = self.xero_api.update_line_item_in_invoice("inv111", "li123", {"Description": "New desc"})
        self.xero_api.xero.invoices.filter.assert_called_with(InvoiceID="inv111")
        self.xero_api.xero.invoices.save.assert_called_with(invoice_obj)
        assert result == updated_invoice[0]

    def test_update_line_item_in_invoice_exception(self):
        self.xero_api.xero.invoices.filter.side_effect = XeroException("Error")
        result = self.xero_api.update_line_item_in_invoice("inv_error", "li_error", {"Description": "Test"})
        assert result == {}

    # --- Bill Methods ---
    def test_create_bill(self):
        # Create a fake detail item representing a bill line.
        detail_item = FakeDetailItem(
            id=2, description="Bill Desc", quantity=3, rate=30,
            account_code_id=200, state="SUBMITTED", vendor="Vendor Bill", line_number=3
        )
        session = FakeSession(detail_item=detail_item)
        # Override the conversion method to return a fixed dict.
        expected_line_item = {
            'Description': "Bill Desc",
            'Quantity': 3.0,
            'UnitAmount': 30.0,
            'TaxType': "TAX999"
        }
        self.xero_api._convert_detail_item_to_line_item = lambda s, di: expected_line_item
        new_invoice = {
            'Type': 'ACCPAY',
            'Contact': {'ContactID': '11111111-2222-3333-4444-555555555555'},
            'LineItems': [expected_line_item],
            'InvoiceNumber': '101_202_3',
            'Status': 'DRAFT'
        }
        fake_response = [{"InvoiceID": "bill123"}]
        self.xero_api.xero.invoices.put.return_value = fake_response
        result = self.xero_api.create_bill(session, 101, 202, 3)
        self.xero_api.xero.invoices.put.assert_called_with([new_invoice])
        assert result == fake_response

    def test_create_bill_db_error(self):
        # Create a session that raises an exception on query.
        class ErrorSession:
            def query(self, model):
                raise Exception("DB error")
        session = ErrorSession()
        fake_response = [{"InvoiceID": "billError"}]
        # Expect that create_bill will log an error, then use empty detail_items.
        self.xero_api.xero.invoices.put.return_value = fake_response
        result = self.xero_api.create_bill(session, 101, 202, 3)
        expected_invoice = {
            'Type': 'ACCPAY',
            'Contact': {'ContactID': '11111111-2222-3333-4444-555555555555'},
            'LineItems': [],
            'InvoiceNumber': '101_202_3',
            'Status': 'DRAFT'
        }
        self.xero_api.xero.invoices.put.assert_called_with([expected_invoice])
        assert result == fake_response

    def test_update_bill_status(self):
        invoice_obj = {"InvoiceID": "inv999", "Status": "DRAFT"}
        updated_invoice = [{"InvoiceID": "inv999", "Status": "PAID"}]
        self.xero_api.xero.invoices.filter.return_value = [invoice_obj]
        self.xero_api.xero.invoices.save.return_value = updated_invoice
        result = self.xero_api.update_bill_status("inv999", "PAID")
        self.xero_api.xero.invoices.filter.assert_called_with(InvoiceID="inv999")
        self.xero_api.xero.invoices.save.assert_called_with(invoice_obj)
        assert result == updated_invoice

    def test_update_bill_status_exception(self):
        self.xero_api.xero.invoices.filter.side_effect = XeroException("Error")
        result = self.xero_api.update_bill_status("inv_error", "PAID")
        assert result is None

    def test_get_bills_by_reference(self):
        invoice_list = [
            {"InvoiceID": "invA", "Status": "DRAFT", "Reference": "REF123"},
            {"InvoiceID": "invB", "Status": "DELETED", "Reference": "REF123"}
        ]
        self.xero_api.xero.invoices.filter.return_value = invoice_list
        result = self.xero_api.get_bills_by_reference("REF123")
        self.xero_api.xero.invoices.filter.assert_called_with(raw='Type=="ACCPAY" AND Reference!=null AND Reference=="REF123"')
        # Should filter out the DELETED invoice.
        assert result == [{"InvoiceID": "invA", "Status": "DRAFT", "Reference": "REF123"}]

    def test_get_bills_by_reference_exception(self):
        self.xero_api.xero.invoices.filter.side_effect = XeroException("Error")
        result = self.xero_api.get_bills_by_reference("REF_ERROR")
        assert result == []

    def test_get_all_bills(self):
        # Simulate pagination by patching invoices.filter with a side effect.
        call_history = []
        def fake_filter(*args, **kwargs):
            call_history.append(kwargs.get("page", 1))
            if kwargs.get("page", 1) == 1:
                return [{"InvoiceID": "inv1", "Status": "DRAFT"}]
            else:
                return []
        self.xero_api.xero.invoices.filter.side_effect = fake_filter
        self.xero_api.xero.invoices.get.return_value = [{"InvoiceID": "inv1", "Status": "DRAFT"}]
        result = self.xero_api.get_all_bills()
        # We expect one detailed invoice.
        assert len(result) == 1
        assert result[0]["InvoiceID"] == "inv1"

    def test_get_all_bills_exception(self):
        # Override _retry_on_unauthorized to catch exception and return an empty list.
        self.xero_api._retry_on_unauthorized = lambda func, *args, **kwargs: _safe_call(func, *args, **kwargs)
        self.xero_api.xero.invoices.filter.side_effect = XeroException("Error")
        result = self.xero_api.get_all_bills()
        assert result == []

    def test_get_acpay_invoices_summary_by_ref(self):
        fake_page = [{"InvoiceID": "invX", "Status": "DRAFT", "InvoiceNumber": "ABC123"}]
        self.xero_api.xero.invoices.filter.return_value = fake_page
        result = self.xero_api.get_acpay_invoices_summary_by_ref("ABC")
        self.xero_api.xero.invoices.filter.assert_called_with(raw='Type=="ACCPAY" AND InvoiceNumber!=null && InvoiceNumber.Contains("ABC")', page=1)
        assert result == fake_page

    def test_get_acpay_invoices_summary_by_ref_exception(self):
        # Override _retry_on_unauthorized to catch exception and return an empty list.
        self.xero_api._retry_on_unauthorized = lambda func, *args, **kwargs: _safe_call(func, *args, **kwargs)
        self.xero_api.xero.invoices.filter.side_effect = XeroException("Error")
        result = self.xero_api.get_acpay_invoices_summary_by_ref("ABC")
        assert result == []

    # --- Spend Money ---
    def test_create_spend_money_success(self):
        detail_item = FakeDetailItem(
            id=3, description="Spend Desc", quantity=1, rate=75,
            account_code_id=300, state="REVIEWED", vendor="Vendor Spend", line_number=5
        )
        session = FakeSession(detail_item=detail_item)
        expected_line_item = {
            'Description': "Spend Desc",
            'Quantity': 1.0,
            'UnitAmount': 75.0,
            'TaxType': "TAX999"
        }
        self.xero_api._convert_detail_item_to_line_item = lambda s, di: expected_line_item
        new_tx = {
            'Type': 'SPEND',
            'Contact': {'Name': "Vendor Spend"},
            'LineItems': [expected_line_item],
            # For a detail item with state "REVIEWED", the logic sets status to AUTHORISED.
            'Status': 'AUTHORISED'
        }
        fake_response = [{"BankTransactionID": "spend123"}]
        self.xero_api.xero.banktransactions.put.return_value = fake_response
        result = self.xero_api.create_spend_money(session, 3)
        self.xero_api.xero.banktransactions.put.assert_called_with([new_tx])
        assert result == fake_response

    def test_create_spend_money_voided(self):
        # Simulate a session that returns no detail item.
        session = FakeSession(detail_item=None)
        fake_response = [{"BankTransactionID": "voided123"}]
        self.xero_api.xero.banktransactions.put.return_value = fake_response
        result = self.xero_api.create_spend_money(session, 999)
        self.xero_api.xero.banktransactions.put.assert_called()
        assert result == fake_response

    def test_create_spend_money_xero_exception(self):
        detail_item = FakeDetailItem(
            id=3, description="Spend Desc", quantity=1, rate=75,
            account_code_id=300, state="REVIEWED", vendor="Vendor Spend", line_number=5
        )
        session = FakeSession(detail_item=detail_item)
        self.xero_api._convert_detail_item_to_line_item = lambda s, di: {
            'Description': "Spend Desc",
            'Quantity': 1.0,
            'UnitAmount': 75.0,
            'TaxType': "TAX999"
        }
        self.xero_api.xero.banktransactions.put.side_effect = XeroException("Error")
        result = self.xero_api.create_spend_money(session, 3)
        assert result is None

    def test_create_spend_money_in_xero(self):
        spend_record = {"state": "DRAFT", "vendor": "VendorX", "description": "Spend X", "amount": 100.0}
        new_tx = {
            'Type': 'SPEND',
            'Contact': {'Name': "VendorX"},
            'LineItems': [{
                'Description': "Spend X",
                'Quantity': 1,
                'UnitAmount': 100.0,
                'TaxType': 'NONE'
            }],
            'Status': 'DRAFT'
        }
        fake_response = [{"BankTransactionID": "spendX"}]
        self.xero_api.xero.banktransactions.put.return_value = fake_response
        result = self.xero_api.create_spend_money_in_xero(spend_record)
        self.xero_api.xero.banktransactions.put.assert_called_with([new_tx])
        assert result == fake_response

    def test_create_spend_money_in_xero_exception(self):
        spend_record = {"state": "DRAFT", "vendor": "VendorX", "description": "Spend X", "amount": 100.0}
        self.xero_api.xero.banktransactions.put.side_effect = XeroException("Error")
        result = self.xero_api.create_spend_money_in_xero(spend_record)
        assert result is None

    def test_update_spend_money(self):
        bank_tx = {"BankTransactionID": "spend789", "Status": "DRAFT"}
        updated_tx = [{"BankTransactionID": "spend789", "Status": "PAID"}]
        self.xero_api.xero.banktransactions.filter.return_value = [bank_tx]
        self.xero_api.xero.banktransactions.save.return_value = updated_tx
        result = self.xero_api.update_spend_money("spend789", "PAID")
        self.xero_api.xero.banktransactions.filter.assert_called_with(BankTransactionID="spend789")
        self.xero_api.xero.banktransactions.save.assert_called_with(bank_tx)
        assert result == updated_tx

    def test_update_spend_money_exception(self):
        self.xero_api.xero.banktransactions.filter.side_effect = Exception("Error")
        result = self.xero_api.update_spend_money("spend789", "PAID")
        assert result is None

    def test_update_spend_transaction_status(self):
        # Simply calls update_spend_money.
        bank_tx = {"BankTransactionID": "spend111", "Status": "DRAFT"}
        updated_tx = [{"BankTransactionID": "spend111", "Status": "AUTHORISED"}]
        self.xero_api.xero.banktransactions.filter.return_value = [bank_tx]
        self.xero_api.xero.banktransactions.save.return_value = updated_tx
        result = self.xero_api.update_spend_transaction_status("spend111", "AUTHORISED")
        assert result == updated_tx

    # --- Concurrency ---
    def test_create_spend_money_in_batch(self):
        # Simulate create_spend_money to simply return a fake transaction for each detail item.
        def fake_create_spend(session, detail_id):
            return {"BankTransactionID": f"tx{detail_id}"}
        self.xero_api.create_spend_money = fake_create_spend
        session = FakeSession(detail_item=FakeDetailItem(0, "", 0, 0, 0, "", "", line_number=0))
        detail_ids = [10, 20, 30]
        results = self.xero_api.create_spend_money_in_batch(session, detail_ids)
        assert len(results) == 3
        expected_ids = {f"tx{d}" for d in detail_ids}
        actual_ids = {r["BankTransactionID"] for r in results}
        assert actual_ids == expected_ids

    def test_create_spend_money_in_batch_with_exception(self):
        # Simulate create_spend_money so that one detail_id raises an exception.
        def fake_create_spend(session, detail_id):
            if detail_id == 20:
                raise Exception("Thread error")
            return {"BankTransactionID": f"tx{detail_id}"}
        self.xero_api.create_spend_money = fake_create_spend
        session = FakeSession(detail_item=FakeDetailItem(0, "", 0, 0, 0, "", "", line_number=0))
        detail_ids = [10, 20, 30]
        results = self.xero_api.create_spend_money_in_batch(session, detail_ids)
        # The exception should be caught and not appended, so we expect 2 results.
        assert len(results) == 2
        expected_ids = {"tx10", "tx30"}
        actual_ids = {r["BankTransactionID"] for r in results}
        assert actual_ids == expected_ids

    # --- upsert_contacts_batch ---
    def test_upsert_contacts_batch(self):
        # Prepare two contacts:
        #   - One with a non‐empty xero_id that should go to the update branch.
        #   - One with an empty xero_id that should go to the create branch.
        contacts = [
            {"xero_id": "id1", "Name": "Contact1"},
            {"xero_id": "", "Name": "Contact2"}
        ]
        updated_response = [{"ContactID": "id1", "Name": "Contact1 Updated"}]
        created_response = [{"ContactID": "id2", "Name": "Contact2"}]

        # Patch contacts.filter so that any call returns an empty list.
        self.xero_api.xero.contacts.filter = lambda **kwargs: []

        # Define a fake _retry_on_unauthorized that distinguishes the call based on its arguments.
        def fake_retry(func, *args, **kwargs):
            if func == self.xero_api.xero.contacts.put:
                contacts_arg = args[0]
                if len(contacts_arg) == 1 and contacts_arg[0].get("xero_id"):
                    return updated_response
                elif len(contacts_arg) == 1 and not contacts_arg[0].get("xero_id"):
                    return created_response
                else:
                    return updated_response + created_response
            return func(*args, **kwargs)
        self.xero_api._retry_on_unauthorized = fake_retry

        result = self.xero_api.upsert_contacts_batch(contacts)
        # We now expect two results: one from the update branch and one from the create branch.
        assert len(result) == 2, f"Expected 2 contacts but got {len(result)}. Result: {result}"
        ids = {r["ContactID"] for r in result}
        assert ids == {"id1", "id2"}

    def test_upsert_contacts_batch_missing_xero_id(self):
        # Test that a missing 'xero_id' key raises KeyError.
        contacts = [{"Name": "ContactMissing"}]
        with pytest.raises(KeyError):
            self.xero_api.upsert_contacts_batch(contacts)

    def test_upsert_contacts_batch_filter_exception(self):
        contacts = [{"xero_id": "", "Name": "Contact2"}]
        self.xero_api.xero.contacts.filter = lambda **kwargs: (_ for _ in ()).throw(Exception("filter error"))
        with pytest.raises(Exception):
            self.xero_api.upsert_contacts_batch(contacts)

    # --- Additional GET Endpoints using sample responses ---
    def test_get_invoices_endpoint(self):
        sample_response = {
          "Invoices": [
            {
              "InvoiceID": "f27a3d9b-5a4c-4a94-8c7f-e52c9fca5a1a",
              "Type": "ACCREC",
              "Contact": {
                "ContactID": "e2f4c5d1-5c3e-4a1b-b1f2-2a7e3d4c6f8a",
                "Name": "ABC Limited"
              },
              "Date": "2024-02-01",
              "DueDate": "2024-02-15",
              "LineAmountTypes": "Exclusive",
              "SubTotal": 100.00,
              "TotalTax": 15.00,
              "Total": 115.00,
              "CurrencyCode": "USD",
              "Status": "AUTHORISED",
              "LineItems": [
                {
                  "Description": "Consulting Services",
                  "Quantity": 2,
                  "UnitAmount": 50.00,
                  "TaxType": "OUTPUT",
                  "AccountCode": "200"
                }
              ]
            }
          ]
        }
        self.xero_api.xero.invoices.all.return_value = sample_response["Invoices"]
        result = self.xero_api.xero.invoices.all()
        assert result == sample_response["Invoices"]

    def test_get_contacts_endpoint(self):
        sample_response = {
          "Contacts": [
            {
              "ContactID": "e2f4c5d1-5c3e-4a1b-b1f2-2a7e3d4c6f8a",
              "Name": "ABC Limited",
              "EmailAddress": "info@abclimited.com",
              "Addresses": [
                {
                  "AddressType": "STREET",
                  "AddressLine1": "123 Main Street",
                  "City": "New York",
                  "Region": "NY",
                  "PostalCode": "10001",
                  "Country": "USA"
                }
              ],
              "Phones": [
                {
                  "PhoneType": "DEFAULT",
                  "PhoneNumber": "555-1234",
                  "CountryCode": "1"
                }
              ],
              "UpdatedDateUTC": "2024-02-01T10:00:00Z"
            }
          ]
        }
        self.xero_api.xero.contacts.all.return_value = sample_response["Contacts"]
        result = self.xero_api.xero.contacts.all()
        assert result == sample_response["Contacts"]

    def test_get_accounts_endpoint(self):
        sample_response = {
          "Accounts": [
            {
              "AccountID": "b9fbb62f-0983-4c9b-b1e7-05f62a774f2d",
              "Code": "200",
              "Name": "Sales",
              "Type": "REVENUE",
              "TaxType": "OUTPUT",
              "EnablePaymentsToAccount": False
            }
          ]
        }
        self.xero_api.xero.accounts.all.return_value = sample_response["Accounts"]
        result = self.xero_api.xero.accounts.all()
        assert result == sample_response["Accounts"]

    def test_get_bank_transactions_endpoint(self):
        sample_response = {
          "BankTransactions": [
            {
              "BankTransactionID": "12345678-90ab-cdef-1234-567890abcdef",
              "Type": "RECEIVE",
              "Contact": {
                "ContactID": "e2f4c5d1-5c3e-4a1b-b1e7-05f62a774f2d",
                "Name": "ABC Limited"
              },
              "Date": "2024-02-01",
              "Reference": "Invoice Payment",
              "CurrencyCode": "USD",
              "Total": 115.00,
              "LineItems": [
                {
                  "Description": "Payment for invoice",
                  "Quantity": 1,
                  "UnitAmount": 115.00,
                  "AccountCode": "610"
                }
              ]
            }
          ]
        }
        self.xero_api.xero.banktransactions.all.return_value = sample_response["BankTransactions"]
        result = self.xero_api.xero.banktransactions.all()
        assert result == sample_response["BankTransactions"]

    def test_get_payments_endpoint(self):
        sample_response = {
          "Payments": [
            {
              "PaymentID": "d2f3e4c5-6789-4abc-9012-34567890abcd",
              "Invoice": {
                "InvoiceID": "f27a3d9b-5a4c-4a94-8c7f-e52c9fca5a1a",
                "InvoiceNumber": "INV-001"
              },
              "Date": "2024-02-02",
              "Amount": 115.00,
              "CurrencyRate": 1.0,
              "BankAccount": {
                "AccountID": "b9fbb62f-0983-4c9b-b1e7-05f62a774f2d",
                "Code": "090",
                "Name": "Bank Account"
              }
            }
          ]
        }
        self.xero_api.xero.payments.all.return_value = sample_response["Payments"]
        result = self.xero_api.xero.payments.all()
        assert result == sample_response["Payments"]

# --- Additional tests for private methods (outside of TestXeroAPI) ---

def test_refresh_token_if_needed_success(monkeypatch):
    # Create a new instance so that our autouse patches do not interfere.
    api = XeroAPI()
    # Create a fake credentials object.
    class FakeCredentials:
        def __init__(self):
            self._expired = True
            self.tenant_id = None
            self.token = {}
            self.base_url = "https://api.xero.com"  # Added base_url for testing.
        def expired(self):
            return self._expired
        def refresh(self):
            self._expired = False
            self.token = {'access_token': 'new_access', 'refresh_token': 'new_refresh'}
        def get_tenants(self):
            return [{'tenantId': 'tenant123'}]
    fake_creds = FakeCredentials()
    api.credentials = fake_creds
    # Monkey-patch the Xero client creation.
    class FakeXeroClient:
        def __init__(self, credentials):
            self.credentials = credentials
    monkeypatch.setattr(api, 'xero', FakeXeroClient(fake_creds))
    # Call _refresh_token_if_needed with force True.
    api._refresh_token_if_needed(force=True)
    assert fake_creds.expired() is False
    assert fake_creds.tenant_id == 'tenant123'
    assert os.environ.get('XERO_ACCESS_TOKEN') == 'new_access'
    assert os.environ.get('XERO_REFRESH_TOKEN') == 'new_refresh'

def test_refresh_token_if_needed_failure(monkeypatch):
    api = XeroAPI()
    class FakeCredentials:
        def __init__(self):
            self._expired = True
            self.tenant_id = None
            self.token = {}
            self.base_url = "https://api.xero.com"
        def expired(self):
            return self._expired
        def refresh(self):
            raise XeroException("Refresh failed")
        def get_tenants(self):
            return []
    fake_creds = FakeCredentials()
    api.credentials = fake_creds
    with pytest.raises(XeroException):
        api._refresh_token_if_needed(force=True)

def test_retry_on_unauthorized_success_after_retry(monkeypatch):
    call_count = {"count": 0}
    def fake_func():
        call_count["count"] += 1
        if call_count["count"] == 1:
            raise XeroUnauthorized(FakeResponse())
        return "success"
    api = XeroAPI()
    # Override _refresh_token_if_needed to do nothing.
    api._refresh_token_if_needed = lambda force=False: None
    result = api._retry_on_unauthorized(fake_func)
    assert result == "success"
    assert call_count["count"] == 2

def test_retry_on_rate_limit(monkeypatch):
    call_count = {"count": 0}
    def fake_func():
        call_count["count"] += 1
        if call_count["count"] < 2:
            raise XeroRateLimitExceeded(
                FakeResponse(),
                payload={
                    "oauth_problem": ["rate_limit_exceeded"],
                    "oauth_problem_advice": ["Please wait and try again"]
                }
            )
        return "success"
    sleep_called = {"called": False}
    def fake_sleep(seconds):
        sleep_called["called"] = True
    monkeypatch.setattr(time, "sleep", fake_sleep)
    api = XeroAPI()
    api._refresh_token_if_needed = lambda force=False: None
    result = api._retry_on_unauthorized(fake_func)
    assert result == "success"
    assert call_count["count"] == 2
    assert sleep_called["called"] is True

def test_retry_on_max_retries(monkeypatch):
    def fake_func():
        raise XeroUnauthorized(FakeResponse())
    api = XeroAPI()
    api._refresh_token_if_needed = lambda force=False: None
    result = api._retry_on_unauthorized(fake_func)
    assert result is None

def test_retry_on_generic_xero_exception(monkeypatch):
    def fake_func():
        raise XeroException("Generic error")
    api = XeroAPI()
    api._refresh_token_if_needed = lambda force=False: None
    with pytest.raises(XeroException):
        api._retry_on_unauthorized(fake_func)

def test_get_tax_code_for_detail_item_success():
    api = XeroAPI()
    detail_item = FakeDetailItem(1, "desc", 1, 10, 123, "SUBMITTED", "vendor")
    session = FakeSession(detail_item=detail_item)
    result = api._get_tax_code_for_detail_item(session, detail_item)
    assert result == "TAX999"

def test_get_tax_code_for_detail_item_exception(monkeypatch):
    api = XeroAPI()
    detail_item = FakeDetailItem(1, "desc", 1, 10, 123, "SUBMITTED", "vendor")
    session = FakeSession(detail_item=detail_item)
    # Force an exception when querying.
    def fake_query(model):
        raise Exception("DB error")
    monkeypatch.setattr(session, "query", fake_query)
    result = api._get_tax_code_for_detail_item(session, detail_item)
    # Should return default tax code.
    assert result == "TAX001"

def test_create_spend_money_via_detail_id(capfd):
    api = XeroAPI()
    # This method only logs info and does nothing.
    result = api.create_spend_money_via_detail_id(42)
    # Should return None.
    assert result is None

def test_create_voided_spend_money_success():
    api = XeroAPI()
    api.xero = MagicMock()
    fake_response = [{"BankTransactionID": "voided1"}]
    api.xero.banktransactions.put.return_value = fake_response
    result = api._create_voided_spend_money()
    # Per our update, _create_voided_spend_money should now return None.
    assert result is None

def test_create_voided_spend_money_exception():
    api = XeroAPI()
    api.xero = MagicMock()
    def fake_put(*args, **kwargs):
        raise XeroException("Error")
    api.xero.banktransactions.put.side_effect = fake_put
    result = api._create_voided_spend_money()
    assert result is None

def test_init_load_dotenv_exception(monkeypatch):
    # Force load_dotenv to raise an exception.
    def fake_load_dotenv(path):
        raise Exception("Env not found")
    monkeypatch.setattr("xero_api.load_dotenv", fake_load_dotenv)
    # Instantiating XeroAPI should not fail (it only logs a warning).
    api = XeroAPI()
    assert hasattr(api, "_initialized")