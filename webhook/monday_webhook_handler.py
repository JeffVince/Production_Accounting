# monday_webhook_handler.py

import json
import logging
from flask import Blueprint, request, jsonify
from sqlalchemy.exc import SQLAlchemyError

# Import database utility functions
from database.monday_database_util import (
    get_purchase_order_by_pulse_id,
    get_detail_item_by_pulse_id,
    delete_detail_item_in_db,
    update_db_with_sub_item_change,
    create_or_update_main_item_in_db,
    create_or_update_sub_item_in_db
)

# Import Monday utility functions
from utilities.monday_util import (
    get_po_number_and_data,
    PO_STATUS_COLUMN_ID,
    SUBITEM_BOARD_ID,
    SUBITEM_STATUS_COLUMN_ID,
    PO_BOARD_ID,
    prep_sub_item_event_for_db_change,
    prep_sub_item_event_for_db_creation,
    prep_main_item_event_for_db_creation,
)

from monday_api import MondayAPI

# Import logger setup
from utilities.logger import setup_logging

# Initialize the logger
logger = logging.getLogger(__name__)
setup_logging()

# Create a Flask Blueprint for Monday webhooks
monday_blueprint = Blueprint('monday', __name__)

class MondayWebhookHandler:
    def __init__(self):
        # Initialize the Monday API client
        self.mondayAPI = MondayAPI()

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
            return jsonify({'challenge': challenge}), 200
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
            # Log the incoming event data for debugging
            logger.debug(f"Incoming event data: {event_data}")

            # Extract the 'event' dictionary from the data
            event = event_data.get('event', {})
            if not event:
                logger.error("Missing 'event' key in the data.")
                logger.debug(f"Full data: {event_data}")
                return jsonify({"error": "Invalid event data"}), 400

            # Extract the pulse ID (item ID) from the event
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

            return jsonify({"message": "PO status change processed"}), 200
        except Exception as e:
            logger.exception("Error processing PO status change.")
            return jsonify({"error": str(e)}), 500

    def process_sub_item_change(self, event_data):
        """
        Process SubItem change events from Monday.com and update the local DetailItem table.

        Args:
            event_data (dict): The event data containing SubItem change information.

        Returns:
            JSON response indicating success or error.
        """
        try:
            # Log the incoming event data for debugging
            logger.debug(f"Incoming SubItem change event data: {event_data}")

            # Validate the event structure
            event = event_data.get('event')
            if not event:
                logger.error("Missing 'event' key in the event data.")
                return jsonify({"error": "Invalid event data: Missing 'event' key"}), 400

            # Prepare the SubItem event for database change
            change_item = prep_sub_item_event_for_db_change(event)

            # Update the database with the SubItem change
            result = update_db_with_sub_item_change(change_item)

            if result == "Success":
                logger.info(f"Successfully processed SubItem change for ID: {change_item['pulse_id']}")
            elif result == "Not Found":
                # SubItem not found in DB, proceed to create it
                print("SubItem not found in DB, creating one.")
                parent_id = event.get('parentItemId')

                # Check if the parent item exists in the database
                if not get_purchase_order_by_pulse_id(parent_id):
                    # Fetch the parent item from Monday.com
                    item_data = self.mondayAPI.fetch_item_by_ID(parent_id)

                    # Prepare the main item event for DB creation
                    create_main_item = prep_main_item_event_for_db_creation(item_data)

                    # Create or update the main item in the database
                    if not create_or_update_main_item_in_db(create_main_item):
                        return jsonify({"error": "Failed to create new main item"}), 400

                # Fetch the SubItem data from Monday.com
                item_data = self.mondayAPI.fetch_item_by_ID(change_item['pulse_id'])

                # Prepare the SubItem event for DB creation
                create_item = prep_sub_item_event_for_db_creation(item_data)
                create_item["parent_id"] = parent_id

                # Create or update the SubItem in the database
                create_result = create_or_update_sub_item_in_db(create_item)
                if create_result == "Fail":
                    return jsonify({"error": "Failed to create new SubItem"}), 400

            return jsonify({"message": "SubItem change processed successfully"}), 200

        except SQLAlchemyError as e:
            logger.exception(f"Database error while processing SubItem change: {e}")
            return jsonify({"error": "Database error"}), 500
        except Exception as e:
            logger.exception("Unexpected error while processing SubItem change.")
            return jsonify({"error": str(e)}), 500

    def process_sub_item_delete(self, event_data):
        """
        Process SubItem deletion events from Monday.com and remove the item from the local database.

        Args:
            event_data (dict): The event data containing SubItem deletion information.

        Returns:
            JSON response indicating success or error.
        """
        try:
            logger.debug("Processing SubItem delete event")

            # Extract the 'event' dictionary from the data
            event = event_data.get('event')
            if not event:
                logger.error("Missing 'event' key in the event data.")
                return jsonify({"error": "Invalid event data: Missing 'event' key"}), 400

            # Extract the pulse ID (SubItem ID) from the event
            pulse_id = event.get('pulseId')
            if not pulse_id:
                logger.error("Missing 'pulseId' in the event.")
                return jsonify({"error": "Invalid event data: Missing 'pulseId'"}), 400

            # Delete the SubItem from the database
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
    """
    Endpoint to handle PO status change events from Monday.com.
    """
    logger.debug("PO status change event received")
    event = request.get_json()
    print(event)
    if not event:
        return jsonify({"error": "Invalid event data"}), 400

    # Handle the challenge verification
    challenge_response = handler.verify_challenge(event)
    if challenge_response:
        return challenge_response
    else:
        return handler.process_po_status_change(event)

@monday_blueprint.route('/subitem_change', methods=['POST'])
def subitem_change():
    """
    Endpoint to handle SubItem change events from Monday.com.
    """
    logger.debug("SubItem change event received")
    event = request.get_json()
    print(event)
    if not event:
        return jsonify({"error": "Invalid event data"}), 400

    # Handle the challenge verification
    challenge_response = handler.verify_challenge(event)
    if challenge_response:
        return challenge_response
    else:
        return handler.process_sub_item_change(event)

@monday_blueprint.route('/subitem_delete', methods=['POST'])
def subitem_delete():
    """
    Endpoint to handle SubItem deletion events from Monday.com.
    """
    logger.debug("SubItem delete event received")
    event = request.get_json()
    print(event)
    if not event:
        return jsonify({"error": "Invalid event data"}), 400

    # Handle the challenge verification
    challenge_response = handler.verify_challenge(event)
    if challenge_response:
        return challenge_response
    else:
        return handler.process_sub_item_delete(event)