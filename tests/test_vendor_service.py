# tests/test_vendor_service.py

import unittest
from tests.base_test import BaseTestCase  # Import BaseTestCase
from services.vendor_service import VendorService
from database.models import Vendor, Contact
from sqlalchemy.exc import IntegrityError  # Import IntegrityError directly

class TestVendorService(BaseTestCase):
    def setUp(self):
        super().setUp()  # Initialize the in-memory database
        self.service = VendorService()
        # Add test data to the database using session_scope
        with self.session_scope() as session:
            vendor = Vendor(vendor_name='Vendor A')
            contact = Contact(
                contact_id='C123',
                name='Vendor A Contact',
                email='vendorA@example.com',
                phone='123-456-7890',
            )
            vendor.contact = contact
            session.add(vendor)
            print(f"Added Vendor: {vendor}, Contact: {contact}")

    def test_match_vendor_with_contacts_existing(self):
        # Implement your test logic here
        pass

    def test_match_vendor_with_contacts_new(self):
        # Attempt to add the same contact_id again
        with self.assertRaises(IntegrityError):
            with self.session_scope() as session:
                vendor = Vendor(vendor_name='Vendor B')
                contact = Contact(
                    contact_id='C123',  # Duplicate contact_id
                    name='Vendor B Contact',
                    email='vendorB@example.com',
                    phone='987-654-3210',
                )
                vendor.contact = contact
                session.add(vendor)
                session.commit()

if __name__ == '__main__':
    unittest.main()