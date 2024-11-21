# tests/test_validation_service.py

import unittest
from tests.base_test import BaseTestCase  # Import BaseTestCase
from services.validation_service import ValidationService
from database.models import PO, POState, Invoice, Receipt

class TestValidationService(BaseTestCase):
    def setUp(self):
        super().setUp()  # Initialize the in-memory database
        self.service = ValidationService()
        # Add test data to the database using session_scope
        with self.session_scope() as session:
            po = PO(po_number='PO123', amount=1000.0, state=POState.PENDING)
            invoice = Invoice(
                po=po,
                file_path='/path/to/invoice.pdf',  # Provide a valid file path
                data='{"total_amount": 1000.0}',
                status='Pending'
            )
            receipt = Receipt(
                po=po,
                file_path='/path/to/receipt.pdf',  # Assuming Receipt also has a file_path
                data='{"total_amount": 1000.0}',
                status='Pending'
            )
            session.add(po)
            session.add(invoice)
            session.add(receipt)

    def test_compare_invoice_with_po_match(self):
        invoice_data = {'total_amount': 1000.0}
        result = self.service.compare_invoice_with_po('PO123', invoice_data)
        self.assertTrue(result)

    def test_compare_invoice_with_po_mismatch(self):
        invoice_data = {'total_amount': 900.0}
        result = self.service.compare_invoice_with_po('PO123', invoice_data)
        self.assertFalse(result)
        with self.session_scope() as session:
            po = session.query(PO).filter_by(po_number='PO123').first()
            self.assertEqual(po.state, POState.ISSUE)

    def test_validate_receipt_data_match(self):
        receipt_data = {'total_amount': 1000.0}
        result = self.service.validate_receipt_data('PO123', receipt_data)
        self.assertTrue(result)

    def test_validate_receipt_data_mismatch(self):
        receipt_data = {'total_amount': 1100.0}
        result = self.service.validate_receipt_data('PO123', receipt_data)
        self.assertFalse(result)
        with self.session_scope() as session:
            po = session.query(PO).filter_by(po_number='PO123').first()
            self.assertEqual(po.state, POState.ISSUE)

    def test_resolve_issues(self):
        self.service.flag_issues('PO123', 'Test Issue')
        self.service.resolve_issues('PO123')
        with self.session_scope() as session:
            po = session.query(PO).filter_by(po_number='PO123').first()
            self.assertEqual(po.state, POState.TO_VERIFY)

if __name__ == '__main__':
    unittest.main()