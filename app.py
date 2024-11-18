# main.py
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import logging
import hmac
import hashlib
from flask import Flask, request
from webhook.dropbox_client import get_dropbox_client, process_event_data
from database.dropbox_database_util import initialize_database
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Initialize logging
logging.basicConfig(
    level=logging.INFO,  # Set to DEBUG for detailed logs during troubleshooting
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler("../Dropbox Listener/logs/dropbox_server.log"),
        logging.StreamHandler()
    ]
)

app = Flask(__name__)

# Initialize the database
initialize_database()

# Initialize the Dropbox client at startup
try:
    dbx_client = get_dropbox_client(
        refresh_token=os.getenv('DROPBOX_REFRESH_TOKEN'),
        app_key=os.getenv('DROPBOX_APP_KEY'),
        app_secret=os.getenv('DROPBOX_APP_SECRET'),
        my_email=os.getenv('MY_EMAIL', 'jeff@ophelia.company'),
        namespace_name=os.getenv('NAMESPACE_NAME', '2024')
    )
    logging.info("Dropbox client initialized successfully.")
except Exception as e:
    logging.error(f"Failed to initialize Dropbox client: {e}", exc_info=True)
    # Exit the application as we cannot proceed without the Dropbox client
    import sys
    sys.exit(1)


def validate_request(request):
    """
    Validate incoming webhook requests using the Dropbox signature.
    """
    signature = request.headers.get('X-Dropbox-Signature')
    if not signature:
        logging.warning("Missing 'X-Dropbox-Signature' header.")
        return False
    hash_obj = hmac.new(os.getenv('DROPBOX_APP_SECRET').encode(), request.data, hashlib.sha256)
    expected_signature = hash_obj.hexdigest()
    is_valid = hmac.compare_digest(expected_signature, signature)
    if not is_valid:
        logging.warning("Dropbox signature validation failed.")
    return is_valid


@app.route('/monday-subitem-change', methods=['POST'])
def monday_webhook():
    """
    Endpoint to handle Monday.com webhook events, including URL verification and event payloads.
    """
    logging.info("POST request received from Monday.com.")

    # Parse the incoming JSON body
    event_data = request.get_json()

    if not event_data:
        logging.warning("No JSON body received in POST request.")
        return 'No JSON body', 400

    # Check if it's a challenge verification request
    challenge = event_data.get("challenge")
    if challenge:
        logging.info("Responding to Monday.com webhook challenge.")
        return {"challenge": challenge}, 200

    # Log and process event data
    if "event" in event_data:
        logging.info(f"Received Monday.com event: {event_data['event']}")

        # Process the event here
        # For example:
        # - Log the event type and relevant details
        event_type = event_data["event"].get("type")
        logging.info(f"Event type: {event_type}")

        # Add custom logic to handle different event types if needed
        # Example: Handle 'create_item' or 'update_column_value' events

        return '', 200  # Acknowledge receipt of the event

    logging.warning("No event field found in POST request.")
    return 'No event field in payload', 400


@app.route('/dropbox-webhook', methods=['GET', 'POST'])
def dropbox_webhook():
    """
    Endpoint to handle Dropbox webhook events.
    """
    if request.method == 'GET':
        challenge = request.args.get('challenge')
        if challenge:
            logging.info("Responding to Dropbox webhook challenge.")
            return challenge, 200
        logging.warning("No challenge parameter provided in GET request.")
        return 'No challenge parameter provided', 400

    if request.method == 'POST':
        if not validate_request(request):
            logging.warning("Invalid request received. Ignoring.")
            return 'Invalid request', 403

        event_data = request.get_json()
        if event_data:
            logging.info(f"Received Dropbox webhook event: {event_data}")

            # Delegate event processing to event_router.py
            process_event_data(event_data)

            return '', 200  # Always return a valid response after processing
        logging.warning("No event data received in POST request.")
        return 'No event data', 400  # Handle missing event data


# Optional: Add a health check route
@app.route('/health', methods=['GET'])
def health():
    return 'OK', 200


if __name__ == '__main__':
    # Run Flask app on all interfaces to be accessible externally
    app.run(host='0.0.0.0', port=5022)
