# tests/test_monday_repository.py

import unittest
from tests.base_test import BaseTestCase
from database.monday_repository import (
    add_or_update_monday_po,
    link_contact_to_po,
    get_monday_po_state,
    update_monday_po_status
)
from database.models import PO, POState, Contact, Vendor
from sqlalchemy.exc import SQLAlchemyError

class TestMondayRepository(BaseTestCase):

    def setUp(self):
        super().setUp()
        # Add a Contact
        contact_data = {
            'contact_id': 'C001',
            'name': 'John Doe',
            'email': 'john@example.com',
            'phone': '1234567890'
        }
        with self.session_scope() as session:
            contact = session.query(Contact).filter_by(contact_id='C001').first()
            if not contact:
                contact = Contact(**contact_data)
                session.add(contact)
                session.commit()
            self.contact = contact

        # Add a Vendor
        vendor_data = {
            'vendor_name': 'Vendor A',
            'contact_id': self.contact.id
        }
        with self.session_scope() as session:
            vendor = session.query(Vendor).filter_by(vendor_name='Vendor A').first()
            if not vendor:
                vendor = Vendor(**vendor_data)
                session.add(vendor)
                session.commit()
            self.vendor = vendor

        # Add a PO
        po_data = {
            'po_number': 'PO128',
            'amount': 6000.0,
            'description': 'PO with Contact',
            'vendor_id': self.vendor.id,
            'state': POState.PENDING
        }
        self.po = add_or_update_monday_po(po_data)

    def test_add_monday_po(self):
        """
        Test adding a Monday PO.
        """
        po_data = {
            'po_number': 'PO129',
            'amount': 7000.0,
            'description': 'Another PO'
        }
        po = add_or_update_monday_po(po_data)
        self.assertIsNotNone(po)
        self.assertEqual(po.po_number, 'PO129')
        self.assertEqual(po.amount, 7000.0)
        self.assertEqual(po.state, POState.PENDING)

    def test_get_monday_po_state(self):
        """
        Test retrieving the state of a Monday PO.
        """
        state = get_monday_po_state('PO128')
        self.assertEqual(state, POState.PENDING)

    def test_link_contact_to_po(self):
        """
        Test linking a contact to a PO.
        """
        po_data = {
            'po_number': 'PO128',
            'amount': 6000.0,
            'description': 'PO with Contact'
        }
        add_or_update_monday_po(po_data)
        contact_data = {
            'contact_id': 'C001',
            'name': 'John Doe',
            'email': 'john@example.com',
            'phone': '1234567890'
        }
        link_contact_to_po('PO128', contact_data)

        with self.session_scope() as session:
            po = session.query(PO).filter_by(po_number='PO128').first()
            self.assertIsNotNone(po)
            self.assertEqual(po.vendor.contact.contact_id, 'C001')
            self.assertEqual(po.vendor.contact.name, 'John Doe')

    def test_update_monday_po_status(self):
        """
        Test updating the status of a Monday PO.
        """
        update_monday_po_status('PO128', POState.APPROVED)
        with self.session_scope() as session:
            po = session.query(PO).filter_by(po_number='PO128').first()
            self.assertIsNotNone(po)
            self.assertEqual(po.state, POState.APPROVED)

if __name__ == '__main__':
    unittest.main()