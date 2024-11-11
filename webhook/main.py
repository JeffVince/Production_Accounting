# main.py
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import logging
import hmac
import hashlib
from flask import Flask, request
from dropbox_client import get_dropbox_client
from database_util import initialize_database
from dotenv import load_dotenv


from processors.event_router import process_event_data  # Import the event processing function

# Load environment variables from .env file
load_dotenv()

# Initialize logging
logging.basicConfig(
    level=logging.INFO,  # Set to DEBUG for detailed logs during troubleshooting
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler("../dropbox_server.log"),
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
        namespace_name=os.getenv('NAMESPACE_NAME', '2024'),
    )
    logging.info("Dropbox client initialized successfully.")
except Exception as e:
    logging.error(f"Failed to initialize Dropbox client: {e}", exc_info=True)
    # Depending on your application's needs, you might choose to exit
    # For example:
    # import sys
    # sys.exit(1)


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
    app.run(host='0.0.0.0', port=5001)
