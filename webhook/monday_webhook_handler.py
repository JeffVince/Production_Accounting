# monday_webhook_handler.py
import json
import logging
from flask import Blueprint, request, jsonify
from sqlalchemy.exc import SQLAlchemyError

# Import database utility functions
from database.monday_database_util import (
    get_purchase_order_by_pulse_id,
    get_detail_item_by_pulse_id,
    delete_detail_item_in_db, update_db_with_sub_item_change,
    create_or_update_main_item_in_db, create_or_update_sub_item_in_db)

# Import Monday utility functions
from utilities.monday_util import (
    get_po_number_and_data,
    PO_STATUS_COLUMN_ID,
    SUBITEM_BOARD_ID,
    SUBITEM_STATUS_COLUMN_ID,
    PO_BOARD_ID, prep_sub_item_event_for_db_change, prep_sub_item_event_for_db_creation,
    prep_main_item_event_for_db_creation,
)

from monday_api import MondayAPI


# Import logger setup
from utilities.logger import setup_logging

logger = logging.getLogger(__name__)
setup_logging()

# Create a Flask Blueprint for Monday webhooks
monday_blueprint = Blueprint('monday', __name__)


class MondayWebhookHandler:
    def __init__(self):
        self.mondayAPI = MondayAPI()

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

            # Fetch the PO number and item data using the item ID
            po_number, item_data = get_po_number_and_data(item_id)
            logger.debug(f"Retrieved PO number: {po_number}")

            if not po_number or not item_data:
                logger.error(f"Unable to find PO number or item data for item ID: {item_id}")
                return jsonify({"error": "PO number or item data not found"}), 400

            logger.info(f"PO {po_number} status changed to {new_status} in Monday.com.")

            # Map statuses to states for Main Item modification
            status_mapping = {
                'To Verify': 'TO VERIFY',
                'Issue': 'ISSUE',
                'Approved': 'APPROVED',
                'CC / PC': 'CC / PC',
                'Pending': 'PENDING',
            }
            mapped_status = status_mapping.get(new_status)

            if mapped_status:
                # Update the status in item_data
                for col in item_data['column_values']:
                    if col['id'] == PO_STATUS_COLUMN_ID:
                        col['text'] = new_status
                        col['value'] = json.dumps({'label': new_status})
                        break
                else:
                    # If the status column is not found, add it
                    item_data['column_values'].append({
                        'id': PO_STATUS_COLUMN_ID,
                        'text': new_status,
                        'value': json.dumps({'label': new_status}),
                    })

                # Check for main item in database
               # if get_purchase_order_by_pulse_id(item_id):
                    # Update the Purchase Order in the database
                    #update_purchase_order_in_db(item_data)
              #  else:
                    # Create a new Purchase Order in the database
                    # create_purchase_order_in_db(item_data)

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
        Process SubItem change event from Monday.com and update the local DetailItem table.
        """
        try:
            # Log the entire event data for debugging
            logger.debug(f"Incoming SubItem change event data: {event_data}")

            # Validate the event structure
            event = event_data.get('event')
            if not event:
                logger.error("Missing 'event' key in the event data.")
                return jsonify({"error": "Invalid event data: Missing 'event' key"}), 400

            change_item = prep_sub_item_event_for_db_change(event)

            result = update_db_with_sub_item_change(change_item)

            if result == "Success":
                logger.info(f"Successfully processed SubItem change for ID: {change_item['pulse_id']}")
            elif result == "Not Found":
                # CREATE SUBITEM IN DB
                print("Sub Item not found in DB, creating one.")
                parent_id = event.get('parentItemId')
                # check if parent exists first
                if not get_purchase_order_by_pulse_id(parent_id):                    # add parent item to DB
                    # get item from Monday
                    item_data = self.mondayAPI.fetch_item_by_ID(parent_id)
                    # format for creation
                    create_main_item = prep_main_item_event_for_db_creation(item_data)
                    # create in DB
                    if not create_or_update_main_item_in_db(create_main_item):
                        return jsonify({"error": "Failed to create new detail item"}), 400
                item_data = self.mondayAPI.fetch_item_by_ID(change_item['pulse_id'])
                create_item = prep_sub_item_event_for_db_creation(item_data)
                create_item["parent_id"] = parent_id
                create_result = create_or_update_sub_item_in_db(create_item)
                if create_result == "Fail":
                    return jsonify({"error": "Failed to create new detail item"}), 400
                    # add to DB
            return jsonify({"message": "SubItem change processed successfully"}), 200

        except SQLAlchemyError as e:
            logger.exception(f"Database error while processing SubItem change: {e}")
            return jsonify({"error": "Database error"}), 500
        except Exception as e:
            logger.exception("Unexpected error while processing SubItem change.")
            return jsonify({"error": str(e)}), 500

    def process_sub_item_delete(self, event_data):
        try:
            logger.debug("Processing SubItem delete event")
            event = event_data.get('event')
            if not event:
                logger.error("Missing 'event' key in the event data.")
                return jsonify({"error": "Invalid event data: Missing 'event' key"}), 400

            pulse_id = event.get('pulseId')
            if not pulse_id:
                logger.error("Missing 'pulseId' in the event.")
                return jsonify({"error": "Invalid event data: Missing 'pulseId'"}), 400

            success = delete_detail_item_in_db(pulse_id)
            if not success:
                logger.error(f"Error deleting SubItem with ID {pulse_id}")
                return jsonify({"error": f"Failed to delete SubItem with ID {pulse_id}"}), 500

            logger.info(f"Successfully deleted SubItem with ID: {pulse_id}")

            return jsonify({"message": "SubItem deleted successfully"}), 200
        except Exception as e:
            logger.exception("Error processing SubItem delete.")
            return jsonify({"error": str(e)}), 500


# Instantiate the handler
handler = MondayWebhookHandler()

# Define Flask routes for the webhooks
@monday_blueprint.route('/po_status_change', methods=['POST'])
def po_status_change():
    logger.debug("PO status change event received")
    event = request.get_json()
    print(event)
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
    logger.debug("SubItem change event received")
    event = request.get_json()
    print(event)
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
    logger.debug("SubItem delete event received")
    event = request.get_json()
    print(event)
    if not event:
        return jsonify({"error": "Invalid event data"}), 400
    # Handle the challenge
    challenge_response = handler.verify_challenge(event)
    if challenge_response:
        return challenge_response
    else:
        return handler.process_sub_item_delete(event)