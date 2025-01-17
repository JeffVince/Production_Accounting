import logging
from flask import Blueprint, request, jsonify
from sqlalchemy.exc import SQLAlchemyError
from files_monday.monday_database_util import monday_database_util
from files_monday.monday_util import monday_util
from files_monday.monday_api import monday_api
from utilities.singleton import SingletonMeta
monday_blueprint = Blueprint('files_monday', __name__)

class MondayWebhookHandler(metaclass=SingletonMeta):

    def __init__(self):
        if not hasattr(self, '_initialized'):
            self.logger = logging.getLogger('monday_logger')
            self.mondayAPI = monday_api
            self.db_util = monday_database_util
            self.monday_util = monday_util
            self.logger.info('[__init__] - Monday Webhook Handler Initialized')
            self._initialized = True

    @staticmethod
    def verify_challenge(event):
        """
        Verify if the event contains a challenge and respond accordingly.

        Args:
            event (dict): The event data received from Monday.com.

        Returns:
            Response object if challenge is present, otherwise None.
        """
        challenge = event.get('challenge')
        if challenge:
            return (jsonify({'challenge': challenge}), 200)
        return None

    def process_po_status_change(self, event_data):
        """
        Process purchase order (PO) status change events from Monday.com.

        Args:
            event_data (dict): The event data containing PO status change information.

        Returns:
            JSON response indicating success or error.
        """
        try:
            self.loggerdebug(f'Incoming event data: {event_data}')
            event = event_data.get('event', {})
            if not event:
                self.loggererror("Missing 'event' key in the data.")
                self.logger.debug(f'[process_po_status_change] - Full data: {event_data}')
                return (jsonify({'error': 'Invalid event data'}), 400)
            item_id = event.get('pulseId')
            self.logger.debug(f'[process_po_status_change] - Extracted item ID: {item_id}')
            if not item_id:
                self.logger.error("[process_po_status_change] - Missing 'pulseId' in the event.")
                self.logger.debug(f'[process_po_status_change] - Event data: {event}')
                return (jsonify({'error': 'Invalid item ID'}), 400)
            value = event.get('value', {})
            new_status = value.get('label', {}).get('text', '')
            self.logger.debug(f'[process_po_status_change] - Extracted new status: {new_status}')
            if not new_status:
                self.logger.error('[process_po_status_change] - Missing or empty status in the event.')
                self.logger.debug(f'[process_po_status_change] - Event value: {value}')
                return (jsonify({'error': 'Invalid status'}), 400)
            (po_number, item_data) = self.monday_util.get_po_number_and_data(item_id)
            self.logger.debug(f'[process_po_status_change] - Retrieved PO number: {po_number}')
            if not po_number or not item_data:
                self.logger.error(f'[process_po_status_change] - Unable to find PO number or item data for item ID: {item_id}')
                return (jsonify({'error': 'PO number or item data not found'}), 400)
            self.logger.info(f'[process_po_status_change] - PO {po_number} status changed to {new_status} in Monday.com.')
            return (jsonify({'message': 'PO status change processed'}), 200)
        except Exception as e:
            self.logger.exception('[process_po_status_change] - Error processing PO status change.')
            return (jsonify({'error': str(e)}), 500)

    def process_sub_item_change(self, event_data):
        """
        Process SubItem change events from Monday.com and update the local DetailItem table.

        Args:
            event_data (dict): The event data containing SubItem change information.

        Returns:
            JSON response indicating success or error.
        """
        try:
            self.logger.debug(f'[process_sub_item_change] - Incoming SubItem change event data: {event_data}')
            event = event_data.get('event')
            if not event:
                self.logger.error("[process_sub_item_change] - Missing 'event' key in the event data.")
                return (jsonify({'error': "Invalid event data: Missing 'event' key"}), 400)
            change_item = self.db_util.prep_sub_item_event_for_db_change(event)
            result = self.db_util.update_db_with_sub_item_change(change_item)
            if result == 'Success':
                self.logger.info(f"[process_sub_item_change] - Successfully processed SubItem change for ID: {change_item['pulse_id']}")
            elif result == 'Not Found':
                self.logger.info('[process_sub_item_change] - SubItem not found in DB, creating one.')
                response = self.mondayAPI.fetch_item_by_ID(change_item['pulse_id'])
                subitem_data = response.get('data', {}).get('items', [])[0]
                subitem_data['parent_item'] = {'id': event['parentItemId']}
                if not self.db_util.get_purchase_order_surrogate_id_by_pulse_id(event['parentItemId']):
                    item_data = self.mondayAPI.fetch_item_by_ID(event['parentItemId'])
                    create_main_item = self.db_util.prep_main_item_event_for_db_creation(item_data)
                    status = self.db_util.create_or_update_main_item_in_db(create_main_item)
                    if status not in ['Created', 'Updated']:
                        self.logger.error('[process_sub_item_change] - Failed to create or update the main PurchaseOrder item in the database.')
                        return (jsonify({'error': 'Failed to create new main item'}), 400)
                create_item = self.db_util.prep_sub_item_event_for_db_creation(subitem_data)
                create_result = self.db_util.create_or_update_sub_item_in_db(create_item)
                if create_result.get('status') == 'Fail':
                    self.logger.error('[process_sub_item_change] - Failed to create new SubItem in the database.')
                    return (jsonify({'error': 'Failed to create new SubItem'}), 400)
            return (jsonify({'message': 'SubItem change processed successfully'}), 200)
        except SQLAlchemyError as e:
            self.logger.exception(f'[process_sub_item_change] - Database error while processing SubItem change: {e}')
            return (jsonify({'error': 'Database error'}), 500)
        except Exception as e:
            self.logger.exception('[process_sub_item_change] - Unexpected error while processing SubItem change.')
            return (jsonify({'error': str(e)}), 500)

    def process_sub_item_delete(self, event_data):
        """
        Process SubItem deletion events from Monday.com and remove the item from the local database.

        Args:
            event_data (dict): The event data containing SubItem deletion information.

        Returns:
            JSON response indicating success or error.
        """
        return (jsonify({'message': 'SubItem deleted successfully'}), 200)
        try:
            self.logger.debug('[process_sub_item_delete] - Processing SubItem delete event')
            event = event_data.get('event')
            if not event:
                self.logger.error("[process_sub_item_delete] - Missing 'event' key in the event data.")
                return (jsonify({'error': "Invalid event data: Missing 'event' key"}), 400)
            pulse_id = event.get('pulseId')
            if not pulse_id:
                self.logger.error("[process_sub_item_delete] - Missing 'pulseId' in the event.")
                return (jsonify({'error': "Invalid event data: Missing 'pulseId'"}), 400)
            success = self.db_util.delete_detail_item_in_db(pulse_id)
            if not success:
                self.logger.error(f'[process_sub_item_delete] - Error deleting SubItem with ID {pulse_id}')
                return (jsonify({'error': f'Failed to delete SubItem with ID {pulse_id}'}), 500)
            self.logger.info(f'[process_sub_item_delete] - Successfully deleted SubItem with ID: {pulse_id}')
            return (jsonify({'message': 'SubItem deleted successfully'}), 200)
        except Exception as e:
            self.logger.exception('[process_sub_item_delete] - Error processing SubItem delete.')
            return (jsonify({'error': str(e)}), 500)
monday_webhook_handler = MondayWebhookHandler()
logger = logging.getLogger('monday_logger')

@monday_blueprint.route('/po_status_change', methods=['POST'])
def po_status_change():
    """
    Endpoint to handle PO status change events from Monday.com.
    """
    logger.debug('PO status change event received')
    event = request.get_json()
    if not event:
        return (jsonify({'error': 'Invalid event data'}), 400)
    challenge_response = monday_webhook_handler.verify_challenge(event)
    if challenge_response:
        return challenge_response
    else:
        return monday_webhook_handler.process_po_status_change(event)

@monday_blueprint.route('/subitem_change', methods=['POST'])
def subitem_change():
    """
    Endpoint to handle SubItem change events from Monday.com.
    """
    logger.debug('SubItem change event received')
    event = request.get_json()
    if not event:
        return (jsonify({'error': 'Invalid event data'}), 400)
    challenge_response = monday_webhook_handler.verify_challenge(event)
    if challenge_response:
        return challenge_response
    else:
        return monday_webhook_handler.process_sub_item_change(event)

@monday_blueprint.route('/subitem_delete', methods=['POST'])
def subitem_delete():
    """
    Endpoint to handle SubItem deletion events from Monday.com.
    """
    logger.debug('SubItem delete event received')
    event = request.get_json()
    if not event:
        return (jsonify({'error': 'Invalid event data'}), 400)
    challenge_response = monday_webhook_handler.verify_challenge(event)
    if challenge_response:
        return challenge_response
    else:
        return monday_webhook_handler.process_sub_item_delete(event)