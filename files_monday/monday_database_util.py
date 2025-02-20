"""
🌐 Monday Database Utility
=========================
Handles Monday.com -> Database synchronization, now leveraging
DatabaseOperations from database_util.py for the actual DB CRUD operations.

"""
import json
import logging
import os
from dotenv import load_dotenv
from files_monday.monday_util import monday_util
from utilities.singleton import SingletonMeta
from database.database_util import DatabaseOperations

class MondayDatabaseUtil(metaclass=SingletonMeta):

    def __init__(self):
        if not hasattr(self, '_initialized'):
            self.logger = logging.getLogger('monday_logger')
            load_dotenv()
            self.monday_api_token = os.getenv('MONDAY_API_TOKEN')
            if not self.monday_api_token:
                self.logger.error('Monday API Token not found. Please set it in environment variables.')
                raise EnvironmentError('Missing MONDAY_API_TOKEN')
            self.monday_api_url = 'https://api.monday.com/v2'
            self.monday_util = monday_util
            self.DEFAULT_ACCOUNT_NUMBER = '5000'
            self.DEFAULT_ACCOUNT_CODE = 1
            self.logger.info('Monday Database Utility initialized')
            self.db_ops = DatabaseOperations()
            self._initialized = True

    def create_or_update_main_item_in_db(self, item_data):
        """
        Example usage of DatabaseOperations for a main item (PurchaseOrder).
        """
        pulse_id = item_data.get('pulse_id')
        if not pulse_id:
            self.logger.warning('[create_or_update_main_item_in_db] - No pulse_id specified in item_data. Cannot create or update.')
            return 'Fail'
        existing_pos = self.db_ops.search_purchase_orders(['pulse_id'], [pulse_id])
        if existing_pos:
            if isinstance(existing_pos, list):
                existing_pos = existing_pos[0]
            po_id = existing_pos['id']
            updated = self.db_ops.update_purchase_order(po_id, **item_data)
            self.logger.info(f'[create_or_update_main_item_in_db] - Updated PO with pulse_id={pulse_id} in DB.')
            return 'Updated' if updated else 'Fail'
        else:
            created = self.db_ops.create_purchase_order(**item_data)
            if created:
                self.logger.info(f'[create_or_update_main_item_in_db] - Created new PO with pulse_id={pulse_id}')
                return 'Created'
            return 'Fail'

    def create_or_update_sub_item_in_db(self, item_data):
        """
        Example usage of DatabaseOperations for a subitem (DetailItem).
        """
        pulse_id = item_data.get('pulse_id')
        if not pulse_id:
            self.logger.warning('[create_or_update_sub_item_in_db] - No pulse_id specified in subitem_data. Cannot create or update.')
            return {'status': 'Fail'}
        existing_sub = self.db_ops.search_detail_items(['pulse_id'], [pulse_id])
        if existing_sub:
            if isinstance(existing_sub, list):
                existing_sub = existing_sub[0]
            detail_id = existing_sub['id']
            updated = self.db_ops.update_detail_item(detail_id, **item_data)
            if updated:
                self.logger.info(f'[create_or_update_sub_item_in_db] - Updated DetailItem with pulse_id={pulse_id} in DB.')
                return {'status': 'Updated'}
            return {'status': 'Fail'}
        else:
            created = self.db_ops.create_detail_item(**item_data)
            if created:
                self.logger.info(f'[create_or_update_sub_item_in_db] - Created new DetailItem with pulse_id={pulse_id}')
                return {'status': 'Created'}
            return {'status': 'Fail'}

    def get_purchase_order_surrogate_id_by_pulse_id(self, pulse_id):
        """
        Now uses db_ops.search_purchase_orders to find the PO by pulse_id.
        """
        found = self.db_ops.search_purchase_orders(['pulse_id'], [pulse_id])
        if not found:
            return None
        if isinstance(found, list):
            found = found[0]
        return found['id']

    def get_purchase_order_type_by_pulse_id(self, pulse_id):
        found = self.db_ops.search_purchase_orders(['pulse_id'], [pulse_id])
        if not found:
            return None
        if isinstance(found, list):
            found = found[0]
        return found.get('po_type')

    def get_detail_item_by_pulse_id(self, pulse_id):
        found = self.db_ops.search_detail_items(['pulse_id'], [pulse_id])
        if isinstance(found, list) and len(found) > 0:
            return found[0]
        return found

    def update_db_with_sub_item_change(self, change_item):
        """
        Applies a prepared change to a DetailItem.
        """
        pulse_id = change_item.get('pulse_id')
        db_field = change_item.get('db_field')
        new_value = change_item.get('new_value')
        if not pulse_id or not db_field:
            self.logger.error('[update_db_with_sub_item_change] - Incomplete change_item data provided.')
            return 'Fail'
        found_sub = self.db_ops.search_detail_items(['pulse_id'], [pulse_id])
        if not found_sub:
            self.logger.info(f'[update_db_with_sub_item_change] - No DetailItem found with pulse_id {pulse_id}.')
            return 'Not Found'
        if isinstance(found_sub, list):
            found_sub = found_sub[0]
        detail_id = found_sub['id']
        updated = self.db_ops.update_detail_item(detail_id, **{db_field: new_value})
        if updated:
            self.logger.info(f'[update_db_with_sub_item_change] - Updated DetailItem {pulse_id}: set {db_field} to {new_value}')
            return 'Success'
        return 'Fail'

    def delete_purchase_order_in_db(self, pulse_id):
        """
        Example: Search the PO by pulse_id, then if found, delete it.
        """
        found_po = self.db_ops.search_purchase_orders(['pulse_id'], [pulse_id])
        if not found_po:
            self.logger.error(f'[delete_purchase_order_in_db] - PurchaseOrder with pulse_id {pulse_id} does not exist.')
            return False
        if isinstance(found_po, list):
            found_po = found_po[0]
        from database.db_util import get_db_session
        from database_pg.models_pg import PurchaseOrder
        with get_db_session() as session:
            po_obj = session.query(PurchaseOrder).get(found_po['id'])
            if not po_obj:
                self.logger.error(f"[delete_purchase_order_in_db] - PO with ID {found_po['id']} does not exist.")
                return False
            session.delete(po_obj)
            session.commit()
            self.logger.info(f"[delete_purchase_order_in_db] - Deleted PurchaseOrder with ID {found_po['id']}.")
            return True

    def delete_detail_item_in_db(self, pulse_id):
        """
        Example: Search the DetailItem by pulse_id, then delete it if found.
        """
        found_sub = self.db_ops.search_detail_items(['pulse_id'], [pulse_id])
        if not found_sub:
            self.logger.error(f'[delete_detail_item_in_db] - DetailItem with pulse_id {pulse_id} does not exist.')
            return False
        if isinstance(found_sub, list):
            found_sub = found_sub[0]
        from database.db_util import get_db_session
        from database_pg.models_pg import DetailItem
        with get_db_session() as session:
            d_obj = session.query(DetailItem).get(found_sub['id'])
            if not d_obj:
                self.logger.error(f"[delete_detail_item_in_db] - DetailItem with ID {found_sub['id']} does not exist.")
                return False
            session.delete(d_obj)
            session.commit()
            self.logger.info(f"[delete_detail_item_in_db] - Deleted DetailItem with ID {found_sub['id']}.")
            return True
monday_database_util = MondayDatabaseUtil()