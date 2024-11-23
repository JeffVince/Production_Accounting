# tests/test_monday_database_util.py

import unittest
from sqlalchemy.exc import IntegrityError
from tests.base_test import BaseTestCase
from database.monday_database_util import (
    insert_main_item, insert_subitem,
    fetch_all_main_items, fetch_subitems_for_main_item,
    fetch_main_items_by_status, fetch_subitems_by_main_item_and_status
)
from sqlalchemy import inspect

class TestMondayDatabaseUtil(BaseTestCase):

    def test_initialize_database(self):
        """
        Test that the database initializes correctly with required tables.
        """
        inspector = inspect(self.engine)
        tables = inspector.get_table_names()
        self.assertIn('main_items', tables)
        self.assertIn('sub_items', tables)

    def test_insert_main_item(self):
        """
        Test inserting a main item into the database.
        """
        main_item = {
            'item_id': 'MI001',
            'name': 'Main Item 1',
            'project_id': 'P001',
            'numbers': '12345',
            'description': 'Description of Main Item 1',
            'tax_form': 'Form A',
            'folder': 'Folder1',
            'amount': '1000',
            'po_status': 'Pending',
            'producer_pm': 'Producer1',
            'updated_date': '2023-10-20'
        }

        # Insert the main item
        insert_main_item(main_item)

        # Fetch all main items and verify
        main_items = fetch_all_main_items()
        self.assertEqual(len(main_items), 1)
        fetched_item = main_items[0]
        self.assertEqual(fetched_item.item_id, main_item['item_id'])
        self.assertEqual(fetched_item.name, main_item['name'])

        # Update the main item
        updated_main_item = main_item.copy()
        updated_main_item['name'] = 'Main Item 1 Updated'
        insert_main_item(updated_main_item)

        # Verify the update
        main_items = fetch_all_main_items()
        self.assertEqual(len(main_items), 1)
        fetched_item = main_items[0]
        self.assertEqual(fetched_item.name, 'Main Item 1 Updated')

    def test_insert_subitem(self):
        """
        Test inserting a subitem linked to a main item.
        """
        # Insert a main item
        main_item = {
            'item_id': 'MI002',
            'name': 'Main Item 2',
            'project_id': 'P002',
            'numbers': '67890',
            'description': 'Description of Main Item 2',
            'tax_form': 'Form B',
            'folder': 'Folder2',
            'amount': '2000',
            'po_status': 'Approved',
            'producer_pm': 'Producer2',
            'updated_date': '2023-10-21'
        }
        insert_main_item(main_item)

        # Insert a valid subitem
        subitem = {
            'subitem_id': 'SI001',
            'main_item_id': 'MI002',
            'status': 'In Progress',
            'invoice_number': 'INV001',
            'description': 'Description of Subitem 1',
            'amount': 500.0,
            'quantity': 10,
            'account_number': 'ACC123',
            'invoice_date': '2023-10-22',
            'link': 'http://example.com/inv001',
            'due_date': '2023-11-22',
            'creation_log': 'Created by user X'
        }
        insert_subitem(subitem)

        # Verify insertion
        subitems = fetch_subitems_for_main_item('MI002')
        self.assertEqual(len(subitems), 1)
        self.assertEqual(subitems[0].subitem_id, 'SI001')

        # Attempt to insert a subitem with a non-existent main_item_id
        invalid_subitem = subitem.copy()
        invalid_subitem['subitem_id'] = 'SI002'
        invalid_subitem['main_item_id'] = 'MI999'  # Non-existent

        with self.assertRaises(IntegrityError):
            insert_subitem(invalid_subitem)

    def test_fetch_all_main_items(self):
        """
        Test fetching all main items from the database.
        """
        # Insert multiple main items
        main_items_data = [
            {
                'item_id': 'MI003',
                'name': 'Main Item 3',
                'project_id': 'P003',
                'numbers': '11111',
                'description': 'Description of Main Item 3',
                'tax_form': 'Form C',
                'folder': 'Folder3',
                'amount': '3000',
                'po_status': 'To Verify',
                'producer_pm': 'Producer3',
                'updated_date': '2023-10-22'
            },
            {
                'item_id': 'MI004',
                'name': 'Main Item 4',
                'project_id': 'P004',
                'numbers': '22222',
                'description': 'Description of Main Item 4',
                'tax_form': 'Form D',
                'folder': 'Folder4',
                'amount': '4000',
                'po_status': 'Issued',
                'producer_pm': 'Producer4',
                'updated_date': '2023-10-23'
            }
        ]
        for item in main_items_data:
            insert_main_item(item)

        # Fetch and verify
        main_items = fetch_all_main_items()
        self.assertEqual(len(main_items), 2)
        item_ids = {item.item_id for item in main_items}
        self.assertSetEqual(item_ids, {'MI003', 'MI004'})

    def test_fetch_subitems_for_main_item(self):
        """
        Test fetching all subitems for a specific main item.
        """
        # Insert a main item and subitems
        main_item = {
            'item_id': 'MI005',
            'name': 'Main Item 5',
            'project_id': 'P005',
            'numbers': '33333',
            'description': 'Description of Main Item 5',
            'tax_form': 'Form E',
            'folder': 'Folder5',
            'amount': '5000',
            'po_status': 'Approved',
            'producer_pm': 'Producer5',
            'updated_date': '2023-10-24'
        }
        insert_main_item(main_item)

        subitems_data = [
            {
                'subitem_id': 'SI003',
                'main_item_id': 'MI005',
                'status': 'Completed',
                'invoice_number': 'INV003',
                'description': 'Description of Subitem 3',
                'amount': 750.0,
                'quantity': 15,
                'account_number': 'ACC456',
                'invoice_date': '2023-10-25',
                'link': 'http://example.com/inv003',
                'due_date': '2023-11-25',
                'creation_log': 'Created by user Y'
            },
            {
                'subitem_id': 'SI004',
                'main_item_id': 'MI005',
                'status': 'Pending',
                'invoice_number': 'INV004',
                'description': 'Description of Subitem 4',
                'amount': 1250.0,
                'quantity': 20,
                'account_number': 'ACC789',
                'invoice_date': '2023-10-26',
                'link': 'http://example.com/inv004',
                'due_date': '2023-11-26',
                'creation_log': 'Created by user Z'
            }
        ]
        for subitem in subitems_data:
            insert_subitem(subitem)

        # Fetch and verify
        subitems = fetch_subitems_for_main_item('MI005')
        self.assertEqual(len(subitems), 2)
        subitem_ids = {subitem.subitem_id for subitem in subitems}
        self.assertSetEqual(subitem_ids, {'SI003', 'SI004'})

    def test_fetch_main_items_by_status(self):
        """
        Test fetching main items by status.
        """
        # Insert main items
        main_items_data = [
            {
                'item_id': 'MI006',
                'name': 'Main Item 6',
                'project_id': 'P006',
                'numbers': '44444',
                'description': 'Description of Main Item 6',
                'tax_form': 'Form F',
                'folder': 'Folder6',
                'amount': '6000',
                'po_status': 'Pending',
                'producer_pm': 'Producer6',
                'updated_date': '2023-10-25'
            },
            {
                'item_id': 'MI007',
                'name': 'Main Item 7',
                'project_id': 'P007',
                'numbers': '55555',
                'description': 'Description of Main Item 7',
                'tax_form': 'Form G',
                'folder': 'Folder7',
                'amount': '7000',
                'po_status': 'Approved',
                'producer_pm': 'Producer7',
                'updated_date': '2023-10-26'
            }
        ]
        for item in main_items_data:
            insert_main_item(item)

        # Fetch and verify
        pending_items = fetch_main_items_by_status('Pending')
        self.assertEqual(len(pending_items), 1)
        self.assertEqual(pending_items[0].item_id, 'MI006')

    def test_fetch_subitems_by_main_item_and_status(self):
        """
        Test fetching subitems by main item and status.
        """
        # Insert main item and subitems
        main_item = {
            'item_id': 'MI008',
            'name': 'Main Item 8',
            'project_id': 'P008',
            'numbers': '66666',
            'description': 'Description of Main Item 8',
            'tax_form': 'Form H',
            'folder': 'Folder8',
            'amount': '8000',
            'po_status': 'Approved',
            'producer_pm': 'Producer8',
            'updated_date': '2023-10-27'
        }
        insert_main_item(main_item)

        subitems_data = [
            {
                'subitem_id': 'SI005',
                'main_item_id': 'MI008',
                'status': 'Completed',
                'invoice_number': 'INV005',
                'description': 'Description of Subitem 5',
                'amount': 850.0,
                'quantity': 17,
                'account_number': 'ACC012',
                'invoice_date': '2023-10-28',
                'link': 'http://example.com/inv005',
                'due_date': '2023-11-28',
                'creation_log': 'Created by user A'
            },
            {
                'subitem_id': 'SI006',
                'main_item_id': 'MI008',
                'status': 'Pending',
                'invoice_number': 'INV006',
                'description': 'Description of Subitem 6',
                'amount': 950.0,
                'quantity': 19,
                'account_number': 'ACC345',
                'invoice_date': '2023-10-29',
                'link': 'http://example.com/inv006',
                'due_date': '2023-11-29',
                'creation_log': 'Created by user B'
            }
        ]
        for subitem in subitems_data:
            insert_subitem(subitem)

        # Fetch and verify
        pending_subitems = fetch_subitems_by_main_item_and_status('MI008', 'Pending')
        self.assertEqual(len(pending_subitems), 1)
        self.assertEqual(pending_subitems[0].subitem_id, 'SI006')

    def test_database_schema(self):
        """
        Test that the sub_items table has the correct foreign key constraint.
        """
        inspector = inspect(self.engine)
        foreign_keys = inspector.get_foreign_keys('sub_items')
        self.assertEqual(len(foreign_keys), 1, "Foreign key constraint not found.")
        fk = foreign_keys[0]
        self.assertEqual(fk['referred_table'], 'main_items')
        self.assertEqual(fk['constrained_columns'], ['main_item_id'])
        self.assertEqual(fk['referred_columns'], ['item_id'])


if __name__ == '__main__':
    unittest.main()