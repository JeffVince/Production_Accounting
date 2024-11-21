# tests/test_dropbpx_repository.py

import unittest
from tests.base_test import BaseTestCase
from database.po_repository import add_or_update_po
from database.dropbox_repository import (
    add_file_record, get_files_by_po, update_file_status
)
from database.models import POState, Vendor, Contact

class TestDropboxRepository(BaseTestCase):

    def setUp(self):
        super().setUp()
        # Add a Vendor with Contact
        contact_data = {
            'contact_id': 'C002',
            'name': 'Jane Smith',
            'email': 'jane@example.com',
            'phone': '0987654321'
        }
        vendor = Vendor(
            vendor_name='Vendor A',
            contact=Contact(**contact_data)
        )
        with self.session_scope() as session:
            session.add(vendor)
            session.commit()
            self.vendor = vendor

        # Add a PO to associate files with
        po_data = {
            'po_number': 'PO125',
            'amount': 3000.0,
            'description': 'Dropbox Test PO',
            'vendor_id': self.vendor.id
        }
        self.po = add_or_update_po(po_data)

    def test_add_invoice_file(self):
        """
        Test adding an invoice file record.
        """
        file_data = {
            'po_number': 'PO125',
            'file_type': 'invoice',
            'file_path': '/path/to/invoice.pdf',
            'data': '{"total": 3000.0}',
            'status': 'Pending'
        }
        file_record = add_file_record(file_data)
        self.assertIsNotNone(file_record.id)
        self.assertEqual(file_record.file_path, '/path/to/invoice.pdf')
        self.assertEqual(file_record.status, 'Pending')

    def test_get_files_by_po(self):
        """
        Test retrieving files associated with a PO.
        """
        # Add files
        invoice_data = {
            'po_number': 'PO125',
            'file_type': 'invoice',
            'file_path': '/path/to/invoice.pdf',
            'data': '{"total": 3000.0}',
            'status': 'Pending'
        }
        receipt_data = {
            'po_number': 'PO125',
            'file_type': 'receipt',
            'file_path': '/path/to/receipt.pdf',
            'data': '{"items": []}',
            'status': 'Pending'
        }
        add_file_record(invoice_data)
        add_file_record(receipt_data)

        files = get_files_by_po('PO125')
        self.assertEqual(len(files['invoices']), 1)
        self.assertEqual(len(files['receipts']), 1)

    def test_update_file_status(self):
        """
        Test updating the status of a file.
        """
        file_data = {
            'po_number': 'PO125',
            'file_type': 'invoice',
            'file_path': '/path/to/invoice.pdf',
            'data': '{"total": 3000.0}',
            'status': 'Pending'
        }
        file_record = add_file_record(file_data)
        update_success = update_file_status(file_record.id, 'invoice', 'Validated')
        self.assertTrue(update_success)

        # Retrieve the file to check status
        with self.session_scope() as session:
            updated_file = session.query(file_record.__class__).get(file_record.id)
            self.assertEqual(updated_file.status, 'Validated')

if __name__ == '__main__':
    unittest.main()