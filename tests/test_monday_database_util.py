import unittest
from unittest.mock import patch, MagicMock
from datetime import datetime
from monday_database_util import MondayDatabaseUtil
from sqlalchemy.exc import IntegrityError, SQLAlchemyError
from database.models import PurchaseOrder, DetailItem, Contact, AccountCode

class TestMondayDatabaseUtil(unittest.TestCase):

    def setUp(self):
        """
        Initialize the MondayDatabaseUtil instance and patch external dependencies.
        """
        patcher = patch('monday_database_util.get_db_session', autospec=True)
        self.mock_get_db_session = patcher.start()
        self.addCleanup(patcher.stop)
        self.mock_session = MagicMock()
        self.mock_get_db_session.return_value.__enter__.return_value = self.mock_session
        self.db_util = MondayDatabaseUtil()

    def test_prep_main_item_event_for_db_creation(self):
        """
        Test preparing main item event for database creation.
        """
        event = {'id': '123', 'column_values': [{'id': 'project_id', 'text': 'Project123'}, {'id': 'numbers08', 'text': 'PO123'}, {'id': 'status', 'text': 'CC / PC'}]}
        with patch.object(self.db_util.monday_util, 'MAIN_ITEM_COLUMN_ID_TO_DB_FIELD', {'project_id': 'project_id', 'numbers08': 'po_number', 'status': 'state'}):
            result = self.db_util.prep_main_item_event_for_db_creation(event)
            expected = {'pulse_id': 123, 'project_id': 'Project123', 'po_number': 'PO123', 'state': 'CC / PC', 'po_type': 'CC / PC'}
            self.assertEqual(result, expected)

    def test_prep_sub_item_event_for_db_change_valid(self):
        """
        Test preparing sub-item event for database change with valid data.
        """
        event = {'pulseId': '456', 'columnId': 'text0', 'columnType': 'text', 'value': 'New Value', 'changedAt': datetime.utcnow().timestamp()}
        with patch.object(self.db_util.monday_util, 'SUB_ITEM_COLUMN_ID_TO_DB_FIELD', {'text0': 'description'}):
            with patch.object(self.db_util.monday_util, 'get_column_handler', return_value=lambda x: x.get('value')):
                result = self.db_util.prep_sub_item_event_for_db_change(event)
                expected = {'pulse_id': 456, 'db_field': 'description', 'new_value': 'New Value', 'changed_at': unittest.mock.ANY}
                self.assertEqual(result['pulse_id'], expected['pulse_id'])
                self.assertEqual(result['db_field'], expected['db_field'])
                self.assertEqual(result['new_value'], expected['new_value'])
                self.assertIsInstance(result['changed_at'], datetime)

    def test_prep_sub_item_event_for_db_change_missing_keys(self):
        """
        Test preparing sub-item event for database change with missing keys.
        """
        event = {'pulseId': '456', 'columnType': 'text', 'value': 'New Value', 'changedAt': datetime.utcnow().timestamp()}
        result = self.db_util.prep_sub_item_event_for_db_change(event)
        self.assertIsNone(result)

    def test_prep_sub_item_event_for_db_change_unmapped_column(self):
        """
        Test preparing sub-item event with unmapped column ID.
        """
        event = {'pulseId': '456', 'columnId': 'unknown_column', 'columnType': 'text', 'value': 'New Value', 'changedAt': datetime.utcnow().timestamp()}
        with patch.object(self.db_util.monday_util, 'SUB_ITEM_COLUMN_ID_TO_DB_FIELD', {}):
            result = self.db_util.prep_sub_item_event_for_db_change(event)
            self.assertIsNone(result)

    def test_prep_sub_item_event_for_db_creation(self):
        """
        Test preparing sub-item event for database creation.
        """
        event = {'id': '456', 'parent_item': {'id': '123'}, 'column_values': [{'id': 'text0', 'text': 'FileID123'}, {'id': 'text98', 'text': 'Description'}, {'id': 'numbers0', 'text': '2'}]}
        with patch.object(self.db_util.monday_util, 'SUB_ITEM_COLUMN_ID_TO_DB_FIELD', {'text0': 'detail_number', 'text98': 'description', 'numbers0': 'quantity'}):
            with patch.object(self.db_util, 'get_purchase_order_surrogate_id_by_pulse_id', return_value=1):
                with patch.object(self.db_util, 'get_account_code_surrogate_id', return_value=1):
                    with patch.object(self.db_util, 'get_purchase_order_type_by_pulse_id', return_value='Vendor'):
                        result = self.db_util.prep_sub_item_event_for_db_creation(event)
                        expected = {'pulse_id': 456, 'parent_id': 1, 'detail_number': 'FileID123', 'description': 'Description', 'quantity': 2.0, 'account_number_id': 1, 'is_receipt': 0}
                        self.assertEqual(result, expected)

    def test_prep_contact_event_for_db_creation(self):
        """
        Test preparing contact event for database creation.
        """
        event = {'id': '789', 'name': 'John Doe', 'column_values': [{'id': 'email', 'text': 'john.doe@example.com'}, {'id': 'phone', 'text': '+1234567890'}]}
        with patch.object(self.db_util.monday_util, 'CONTACT_COLUMN_ID_TO_DB_FIELD', {'email': 'email', 'phone': 'phone_number'}):
            result = self.db_util.prep_contact_event_for_db_creation(event)
            expected = {'pulse_id': 789, 'name': 'John Doe', 'email': 'john.doe@example.com', 'phone_number': '+1234567890'}
            self.assertEqual(result, expected)

    @patch('monday_database_util.get_db_session')
    def test_create_or_update_main_item_in_db_create(self, mock_get_db_session):
        """
        Test creating a new main item in the database.
        """
        session = mock_get_db_session.return_value.__enter__.return_value
        item_data = {'pulse_id': 1, 'po_number': 'PO123', 'state': 'Open'}
        session.query.return_value.filter_by.return_value.one_or_none.return_value = None
        status = self.db_util.create_or_update_main_item_in_db(item_data)
        self.assertEqual(status, 'Created')
        session.add.assert_called()
        session.commit.assert_called()

    @patch('monday_database_util.get_db_session')
    def test_create_or_update_main_item_in_db_update(self, mock_get_db_session):
        """
        Test updating an existing main item in the database.
        """
        session = mock_get_db_session.return_value.__enter__.return_value
        existing_item = PurchaseOrder(pulse_id=1, po_number='PO123', state='Open')
        session.query.return_value.filter_by.return_value.one_or_none.return_value = existing_item
        item_data = {'pulse_id': 1, 'po_number': 'PO123', 'state': 'Closed'}
        status = self.db_util.create_or_update_main_item_in_db(item_data)
        self.assertEqual(status, 'Updated')
        self.assertEqual(existing_item.state, 'Closed')
        session.commit.assert_called()

    @patch('monday_database_util.get_db_session')
    def test_create_or_update_main_item_in_db_exception(self, mock_get_db_session):
        """
        Test handling exceptions when creating or updating a main item.
        """
        session = mock_get_db_session.return_value.__enter__.return_value
        session.commit.side_effect = Exception('DB Error')
        item_data = {'pulse_id': 1, 'po_number': 'PO123', 'state': 'Open'}
        status = self.db_util.create_or_update_main_item_in_db(item_data)
        self.assertEqual(status, 'Fail')
        session.rollback.assert_called()

    @patch('monday_database_util.get_db_session')
    def test_create_or_update_sub_item_in_db_create(self, mock_get_db_session):
        """
        Test creating a new sub-item in the database.
        """
        session = mock_get_db_session.return_value.__enter__.return_value
        item_data = {'pulse_id': 456, 'description': 'New SubItem'}
        session.query.return_value.filter_by.return_value.one_or_none.return_value = None
        result = self.db_util.create_or_update_sub_item_in_db(item_data)
        self.assertEqual(result['status'], 'Created')
        session.add.assert_called()
        session.commit.assert_called()

    @patch('monday_database_util.get_db_session')
    def test_create_or_update_sub_item_in_db_update(self, mock_get_db_session):
        """
        Test updating an existing sub-item in the database.
        """
        session = mock_get_db_session.return_value.__enter__.return_value
        detail_item = DetailItem(pulse_id=456, description='Old Description')
        session.query.return_value.filter_by.return_value.one_or_none.return_value = detail_item
        item_data = {'pulse_id': 456, 'description': 'Updated Description'}
        result = self.db_util.create_or_update_sub_item_in_db(item_data)
        self.assertEqual(result['status'], 'Updated')
        self.assertEqual(detail_item.description, 'Updated Description')
        session.commit.assert_called()

    @patch('monday_database_util.get_db_session')
    def test_create_or_update_sub_item_in_db_exception(self, mock_get_db_session):
        """
        Test handling exception during sub-item creation/update.
        """
        session = mock_get_db_session.return_value.__enter__.return_value
        item_data = {'pulse_id': 456, 'description': 'New SubItem'}
        session.commit.side_effect = Exception('DB Error')
        result = self.db_util.create_or_update_sub_item_in_db(item_data)
        self.assertEqual(result['status'], 'Fail')
        session.rollback.assert_called()

    @patch('monday_database_util.get_db_session')
    def test_get_purchase_order_surrogate_id_by_pulse_id_found(self, mock_get_db_session):
        """
        Test retrieving PurchaseOrder surrogate ID when found.
        """
        session = mock_get_db_session.return_value.__enter__.return_value
        po = PurchaseOrder(pulse_id=123, po_surrogate_id=1)
        session.query.return_value.filter_by.return_value.one_or_none.return_value = po
        result = self.db_util.get_purchase_order_surrogate_id_by_pulse_id(123)
        self.assertEqual(result, 1)

    @patch('monday_database_util.get_db_session')
    def test_get_purchase_order_surrogate_id_by_pulse_id_not_found(self, mock_get_db_session):
        """
        Test retrieving PurchaseOrder surrogate ID when not found.
        """
        session = mock_get_db_session.return_value.__enter__.return_value
        session.query.return_value.filter_by.return_value.one_or_none.return_value = None
        result = self.db_util.get_purchase_order_surrogate_id_by_pulse_id(123)
        self.assertIsNone(result)

    @patch('monday_database_util.get_db_session')
    def test_get_account_code_surrogate_id_found(self, mock_get_db_session):
        """
        Test retrieving Account code surrogate ID when found.
        """
        session = mock_get_db_session.return_value.__enter__.return_value
        account_code_entry = AccountCode(code='5000', account_code_surrogate_id=1)
        session.query.return_value.filter_by.return_value.one_or_none.return_value = account_code_entry
        result = self.db_util.get_account_code('5000')
        self.assertEqual(result, 1)

    @patch('monday_database_util.get_db_session')
    def test_get_account_code_surrogate_id_not_found(self, mock_get_db_session):
        """
        Test retrieving Account code surrogate ID when not found.
        """
        session = mock_get_db_session.return_value.__enter__.return_value
        session.query.return_value.filter_by.return_value.one_or_none.return_value = None
        result = self.db_util.get_account_code('9999')
        self.assertIsNone(result)

    @patch('monday_database_util.get_db_session')
    def test_update_db_with_sub_item_change_success(self, mock_get_db_session):
        """
        Test successfully updating a sub-item in the database.
        """
        session = mock_get_db_session.return_value.__enter__.return_value
        detail_item = DetailItem(pulse_id=456, description='Old Description')
        session.query.return_value.filter_by.return_value.one_or_none.return_value = detail_item
        change_item = {'pulse_id': 456, 'db_field': 'description', 'new_value': 'New Description'}
        result = self.db_util.update_db_with_sub_item_change(change_item)
        self.assertEqual(result, 'Success')
        self.assertEqual(detail_item.description, 'New Description')
        session.commit.assert_called()

    @patch('monday_database_util.get_db_session')
    def test_update_db_with_sub_item_change_not_found(self, mock_get_db_session):
        """
        Test updating a sub-item that does not exist in the database.
        """
        session = mock_get_db_session.return_value.__enter__.return_value
        session.query.return_value.filter_by.return_value.one_or_none.return_value = None
        change_item = {'pulse_id': 456, 'db_field': 'description', 'new_value': 'New Description'}
        result = self.db_util.update_db_with_sub_item_change(change_item)
        self.assertEqual(result, 'Not Found')

    @patch('monday_database_util.get_db_session')
    def test_update_db_with_sub_item_change_exception(self, mock_get_db_session):
        """
        Test handling exceptions when updating a sub-item.
        """
        session = mock_get_db_session.return_value.__enter__.return_value
        session.commit.side_effect = Exception('DB Error')
        detail_item = DetailItem(pulse_id=456, description='Old Description')
        session.query.return_value.filter_by.return_value.one_or_none.return_value = detail_item
        change_item = {'pulse_id': 456, 'db_field': 'description', 'new_value': 'New Description'}
        result = self.db_util.update_db_with_sub_item_change(change_item)
        self.assertEqual(result, 'Fail')
        session.rollback.assert_called()

    @patch('monday_database_util.get_db_session')
    def test_delete_purchase_order_in_db_success(self, mock_get_db_session):
        """
        Test successful deletion of a PurchaseOrder.
        """
        session = mock_get_db_session.return_value.__enter__.return_value
        po = PurchaseOrder(pulse_id=123, po_surrogate_id=1)
        session.query.return_value.filter_by.return_value.first.return_value = po
        result = self.db_util.delete_purchase_order_in_db(1)
        self.assertTrue(result)
        session.delete.assert_called_with(po)
        session.commit.assert_called()

    @patch('monday_database_util.get_db_session')
    def test_delete_purchase_order_in_db_not_found(self, mock_get_db_session):
        """
        Test deletion of a PurchaseOrder that does not exist.
        """
        session = mock_get_db_session.return_value.__enter__.return_value
        session.query.return_value.filter_by.return_value.first.return_value = None
        result = self.db_util.delete_purchase_order_in_db(1)
        self.assertFalse(result)
        session.commit.assert_not_called()

    @patch('monday_database_util.get_db_session')
    def test_delete_detail_item_in_db_success(self, mock_get_db_session):
        """
        Test successful deletion of a DetailItem.
        """
        session = mock_get_db_session.return_value.__enter__.return_value
        detail_item = DetailItem(pulse_id=456, detail_item_surrogate_id=2)
        session.query.return_value.filter_by.return_value.first.return_value = detail_item
        result = self.db_util.delete_detail_item_in_db(2)
        self.assertTrue(result)
        session.delete.assert_called_with(detail_item)
        session.commit.assert_called()

    @patch('monday_database_util.get_db_session')
    def test_delete_detail_item_in_db_not_found(self, mock_get_db_session):
        """
        Test deletion of a DetailItem that does not exist.
        """
        session = mock_get_db_session.return_value.__enter__.return_value
        session.query.return_value.filter_by.return_value.first.return_value = None
        result = self.db_util.delete_detail_item_in_db(2)
        self.assertFalse(result)
        session.commit.assert_not_called()

    def test_verify_url_valid(self):
        """
        Test verifying a valid URL string.
        """
        url = self.db_util.verify_url('http://example.com')
        self.assertEqual(url, 'http://example.com')

    def test_verify_url_empty(self):
        """
        Test verifying an empty string.
        """
        url = self.db_util.verify_url('')
        self.assertEqual(url, '')

    def test_verify_url_none(self):
        """
        Test verifying None as input.
        """
        url = self.db_util.verify_url(None)
        self.assertEqual(url, '')
if __name__ == '__main__':
    unittest.main()