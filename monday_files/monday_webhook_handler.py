# monday_webhook_handler.py

import logging
from flask import Blueprint, request, jsonify
from sqlalchemy.exc import SQLAlchemyError

# Import the MondayDatabaseUtil class
from monday_files.monday_database_util import monday_database_util

# Import Monday utility functions
from monday_files.monday_util import monday_util

from monday_files.monday_api import monday_api

# Create a Flask Blueprint for Monday webhooks
from utilities.singleton import SingletonMeta

monday_blueprint = Blueprint('monday_files', __name__)


class MondayWebhookHandler(metaclass=SingletonMeta):
    def __init__(self):
        if not hasattr(self, '_initialized'):
            # Set up logging
            self.logger = logging.getLogger("app_logger")
            # Initialize the Monday API client
            self.mondayAPI = monday_api
            # Initialize the MondayDatabaseUtil instance
            self.db_util = monday_database_util
            # Initialize the MondayUtil instance
            self.monday_util = monday_util
            self.logger.info("Monday Webhook Handler Initialized")
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
            self.loggerdebug(f"Incoming event data: {event_data}")

            # Extract the 'event' dictionary from the data
            event = event_data.get('event', {})
            if not event:
                self.loggererror("Missing 'event' key in the data.")
                self.logger.debug(f"Full data: {event_data}")
                return jsonify({"error": "Invalid event data"}), 400

            # Extract the pulse ID (item ID) from the event
            item_id = event.get('pulseId')
            self.logger.debug(f"Extracted item ID: {item_id}")

            if not item_id:
                self.logger.error("Missing 'pulseId' in the event.")
                self.logger.debug(f"Event data: {event}")
                return jsonify({"error": "Invalid item ID"}), 400

            # Extract the new status from the event data
            value = event.get('value', {})
            new_status = value.get('label', {}).get('text', '')
            self.logger.debug(f"Extracted new status: {new_status}")

            if not new_status:
                self.logger.error("Missing or empty status in the event.")
                self.logger.debug(f"Event value: {value}")
                return jsonify({"error": "Invalid status"}), 400

            # Fetch the PO number and item data using the item ID
            po_number, item_data = self.monday_util.get_po_number_and_data(item_id)
            self.logger.debug(f"Retrieved PO number: {po_number}")

            if not po_number or not item_data:
                self.logger.error(f"Unable to find PO number or item data for item ID: {item_id}")
                return jsonify({"error": "PO number or item data not found"}), 400

            self.logger.info(f"PO {po_number} status changed to {new_status} in Monday.com.")

            return jsonify({"message": "PO status change processed"}), 200
        except Exception as e:
            self.logger.exception("Error processing PO status change.")
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
            self.logger.debug(f"Incoming SubItem change event data: {event_data}")

            # Validate the event structure
            event = event_data.get('event')
            if not event:
                self.logger.error("Missing 'event' key in the event data.")
                return jsonify({"error": "Invalid event data: Missing 'event' key"}), 400

            # Prepare the SubItem event for database change
            change_item = self.db_util.prep_sub_item_event_for_db_change(event)

            # Update the database with the SubItem change using the MondayDatabaseUtil instance
            result = self.db_util.update_db_with_sub_item_change(change_item)

            if result == "Success":
                self.logger.info(f"Successfully processed SubItem change for ID: {change_item['pulse_id']}")
            elif result == "Not Found":
                # SubItem not found in DB, proceed to create it
                self.logger.info("SubItem not found in DB, creating one.")
                response = self.mondayAPI.fetch_item_by_ID(change_item['pulse_id'])
                subitem_data = response.get("data", {}).get("items", [])[0]
                subitem_data["parent_item"] = {"id": event["parentItemId"]}

                # # # # Check if the parent item exists in the database
                if not self.db_util.get_purchase_order_surrogate_id_by_pulse_id(event["parentItemId"]):
                    # Fetch the parent item from Monday.com
                    item_data = self.mondayAPI.fetch_item_by_ID(event["parentItemId"])
                    # Prepare the main item event for DB creation
                    create_main_item = self.db_util.prep_main_item_event_for_db_creation(item_data)
                    # # # # Create or update the main item in the database
                    status = self.db_util.create_or_update_main_item_in_db(create_main_item)
                    if status not in ["Created", "Updated"]:
                        self.logger.error("Failed to create or update the main PurchaseOrder item in the database.")
                        return jsonify({"error": "Failed to create new main item"}), 400

                # Prepare the SubItem event for DB creation
                create_item = self.db_util.prep_sub_item_event_for_db_creation(subitem_data)

                # Create or update the SubItem in the database
                create_result = self.db_util.create_or_update_sub_item_in_db(create_item)
                if create_result.get("status") == "Fail":
                    self.logger.error("Failed to create new SubItem in the database.")
                    return jsonify({"error": "Failed to create new SubItem"}), 400

            return jsonify({"message": "SubItem change processed successfully"}), 200

        except SQLAlchemyError as e:
            self.logger.exception(f"Database error while processing SubItem change: {e}")
            return jsonify({"error": "Database error"}), 500
        except Exception as e:
            self.logger.exception("Unexpected error while processing SubItem change.")
            return jsonify({"error": str(e)}), 500

    def process_sub_item_delete(self, event_data):
        """
        Process SubItem deletion events from Monday.com and remove the item from the local database.

        Args:
            event_data (dict): The event data containing SubItem deletion information.

        Returns:
            JSON response indicating success or error.
        """
        return jsonify({"message": "SubItem deleted successfully"}), 200
        try:
            self.logger.debug("Processing SubItem delete event")

            # Extract the 'event' dictionary from the data
            event = event_data.get('event')
            if not event:
                self.logger.error("Missing 'event' key in the event data.")
                return jsonify({"error": "Invalid event data: Missing 'event' key"}), 400

            # Extract the pulse ID (SubItem ID) from the event
            pulse_id = event.get('pulseId')
            if not pulse_id:
                self.logger.error("Missing 'pulseId' in the event.")
                return jsonify({"error": "Invalid event data: Missing 'pulseId'"}), 400

            # Delete the SubItem from the database using the MondayDatabaseUtil instance
            success = self.db_util.delete_detail_item_in_db(pulse_id)
            if not success:
                self.logger.error(f"Error deleting SubItem with ID {pulse_id}")
                return jsonify({"error": f"Failed to delete SubItem with ID {pulse_id}"}), 500

            self.logger.info(f"Successfully deleted SubItem with ID: {pulse_id}")

            return jsonify({"message": "SubItem deleted successfully"}), 200
        except Exception as e:
            self.logger.exception("Error processing SubItem delete.")
            return jsonify({"error": str(e)}), 500


# Instantiate the handler
monday_webhook_handler = MondayWebhookHandler()
logger = logging.getLogger("app_logger")

# Define Flask routes for the webhooks
@monday_blueprint.route('/po_status_change', methods=['POST'])
def po_status_change():
    """
    Endpoint to handle PO status change events from Monday.com.
    """
    logger.debug("PO status change event received")
    event = request.get_json()
    if not event:
        return jsonify({"error": "Invalid event data"}), 400

    # Handle the challenge verification
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
    logger.debug("SubItem change event received")
    event = request.get_json()
    if not event:
        return jsonify({"error": "Invalid event data"}), 400

    # Handle the challenge verification
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
    logger.debug("SubItem delete event received")
    event = request.get_json()

    if not event:
        return jsonify({"error": "Invalid event data"}), 400

    # Handle the challenge verification
    challenge_response = monday_webhook_handler.verify_challenge(event)
    if challenge_response:
        return challenge_response
    else:
        return monday_webhook_handler.process_sub_item_delete(event)
