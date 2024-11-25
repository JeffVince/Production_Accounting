import logging
from flask import Blueprint, request, jsonify
from sqlalchemy.exc import SQLAlchemyError

from services.monday_service import MondayService
from services.po_modification_service import POModificationService
from database.monday_database_util import (
    update_main_item_from_monday, item_exists_by_monday_id, update_monday_po_status, insert_main_item,
    patch_detail_item, delete_sub_item_from_db
)

from utilities.logger import setup_logging
from monday import MondayClient
from utilities.config import Config
from utils import map_event_to_update_data

logger = logging.getLogger(__name__)
setup_logging()

monday_blueprint = Blueprint('monday', __name__)


class MondayWebhookHandler:
    def __init__(self):
        self.monday_service = MondayService()
        self.config = Config()
        self.monday_api = MondayClient(self.config.MONDAY_API_TOKEN)

    @staticmethod
    def verify_challenge(event):
        """Verify if the event contains a challenge."""
        challenge = event.get('challenge')
        if challenge:
            return jsonify({'challenge': challenge}), 200
        return None

    def process_po_status_change(self, event_data):
        """Process PO status change from Monday.com."""
        try:
            # Log the entire event data for debugging
            logger.debug(f"Incoming event data: {event_data}")

            # Extract the 'event' dictionary
            event = event_data.get('event', {})
            if not event:
                logger.error("Missing 'event' key in the data.")
                logger.debug(f"Full data: {event_data}")
                return jsonify({"error": "Invalid event data"}), 400

            # Extract the pulse ID (item ID)
            item_id = event.get('pulseId')
            logger.debug(f"Extracted item ID: {item_id}")

            if not item_id:
                logger.error("Missing 'pulseId' in the event.")
                logger.debug(f"Event data: {event}")
                return jsonify({"error": "Invalid item ID"}), 400

            # Extract the new status from the event data
            value = event.get('value', {})
            new_status = value.get('label', {}).get('text', '')
            logger.debug(f"Extracted new status: {new_status}")
            if not new_status:
                logger.error("Missing or empty status in the event.")
                logger.debug(f"Event value: {value}")
                return jsonify({"error": "Invalid status"}), 400

            # Fetch the PO number using the item ID
            po_number = self.monday_service.get_po_number_from_item(item_id)
            print(f"Retrieved PO number: {po_number}")

            if not po_number:
                logger.error(f"Unable to find PO number for item ID: {item_id}")
                return jsonify({"error": "PO number not found"}), 400

            logger.info(f"PO {po_number} status changed to {new_status} in Monday.com.")

            # Map statuses to states for Main Item modification
            status_mapping = {
                'To Verify': 'TO VERIFY',
                'Issue': 'ISSUE',
                'Approved': 'APPROVED',
                'CC / PC': 'CC/PC',
                'Pending': 'PENDING',
            }
            mapped_status = status_mapping.get(new_status)

            if mapped_status:
                # check for main item in database
                if item_exists_by_monday_id(item_id):
                    update_monday_po_status(item_id, mapped_status)
                else:
                    response = self.monday_api.items.fetch_items_by_id(item_id)
                    print(response['data']['items'][0])
                    insert_main_item(response['data']['items'][0])

                logger.info(f"Updated Main Item state to {mapped_status} for PO {po_number}.")
            else:
                logger.warning(f"Unhandled status: {new_status} for PO {po_number}.")
                return jsonify({"error": f"Unhandled status: {new_status}"}), 400

            return jsonify({"message": "PO status change processed"}), 200
        except Exception as e:
            logger.exception("Error processing PO status change.")
            return jsonify({"error": str(e)}), 500

    def process_sub_item_change(self, event_data):
        """
        Process SubItem change event from Monday.com and update the local SubItem table.
        """
        try:
            # Log the entire event data for debugging
            logger.debug(f"Incoming SubItem change event data: {event_data}")

            # Validate the event structure
            event = event_data.get('event')
            if not event:
                logger.error("Missing 'event' key in the event data.")
                return jsonify({"error": "Invalid event data: Missing 'event' key"}), 400

            # Extract the subitem ID (pulseId)
            subitem_id = event.get('pulseId')
            logger.debug(f"Extracted SubItem ID (pulseId): {subitem_id}")

            if not subitem_id:
                logger.error("Missing 'pulseId' in the event.")
                return jsonify({"error": "Invalid event data: Missing 'pulseId'"}), 400

            # Map the event data to update data
            update_data, error = map_event_to_update_data(event)
            if error:
                logger.error(error)
                return jsonify({"error": error}), 400

            # Patch the SubItem in the local database
            success, message = patch_detail_item(subitem_id, update_data)
            if not success:
                return jsonify({"error": message}), 500

            logger.info(f"Successfully processed SubItem change for ID: {subitem_id}")

            return jsonify({"message": "SubItem change processed successfully"}), 200

        except SQLAlchemyError as e:
            logger.exception(f"Database error while processing SubItem change: {e}")
            return jsonify({"error": "Database error"}), 500
        except Exception as e:
            logger.exception("Unexpected error while processing SubItem change.")
            return jsonify({"error": str(e)}), 500

    def process_sub_item_delete(self, event_data):
        try:
            print("Process Sub Item Delete")
            pulse_id = event_data.get('event').get('pulseId')
            delete_sub_item_from_db(pulse_id)
        except Exception as e:
            return jsonify({"error": str(e)}), 500
        return jsonify({"message": "SubItem deleted  successfully"}), 200


handler = MondayWebhookHandler()


@monday_blueprint.route('/po_status_change', methods=['POST'])
def po_status_change():
    print("po status change event")
    event = request.get_json()
    if not event:
        return jsonify({"error": "Invalid event data"}), 400
    # Handle the challenge
    challenge_response = handler.verify_challenge(event)
    if challenge_response:
        return challenge_response
    else:
        return handler.process_po_status_change(event)


@monday_blueprint.route('/subitem_change', methods=['POST'])
def subitem_change():
    print("subitem change event")
    event = request.get_json()
    if not event:
        return jsonify({"error": "Invalid event data"}), 400
    # Handle the challenge
    challenge_response = handler.verify_challenge(event)
    if challenge_response:
        return challenge_response
    else:
        return handler.process_sub_item_change(event)


@monday_blueprint.route('/subitem_delete', methods=['POST'])
def subitem_delete():
    print("subitem delete event")
    event = request.get_json()
    if not event:
        return jsonify({"error": "Invalid event data"}), 400
    # Handle the challenge
    challenge_response = handler.verify_challenge(event)
    if challenge_response:
        return challenge_response
    else:
        return handler.process_sub_item_delete(event)
