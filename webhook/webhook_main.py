# webhook_main.py

import logging
from flask import Flask, jsonify
from monday_files.monday_webhook_handler import monday_blueprint
from dropbox_files.dropbox_webhook_handler import dropbox_blueprint
from utilities.logger import setup_logging

# Initialize logging
logger = logging.getLogger(__name__)
setup_logging()

# Initialize Flask app
app = Flask(__name__)

# Register Blueprints with URL prefixes
app.register_blueprint(monday_blueprint, url_prefix='/webhook/monday')
app.register_blueprint(dropbox_blueprint, url_prefix='/webhook/dropbox')


# Optional: Root route for health check or info
@app.route('/health', methods=['GET'])
def index():
    return jsonify({"message": "Webhook listener is running."}), 200

