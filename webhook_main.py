# webhook_main.py

import logging
from flask import Flask, jsonify
from monday_files.monday_webhook_handler import monday_blueprint
from monday_files.monday_api import monday_api
from dropbox_files.dropbox_webhook_handler import dropbox_blueprint
from po_log_database_util import po_log_database_util
from utilities.logger import setup_logging
from flask import render_template

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

@app.route('/po/<string:po_id>', methods=['GET'])
def get_po_data(po_id):
    # Split the PO ID into project ID and PO number
    try:
        project_id, po_number = po_id.split('_')
        logger.info(project_id, po_number)
    except ValueError:
        return {"error": "Invalid PO ID format. Expected format: '2416_04'"}, 400

    # Call your function with the extracted values
    result = monday_api.fetch_item_by_po_and_project(project_id, po_number)
    try:
        json_result = jsonify(result)
    except Exception as e:
        logger.error(e)
        raise
    return  json_result


@app.route('/po_html/<string:project_ID>', methods=['GET'])
def po_html(project_ID):
    logger.info(project_ID)
    result = po_log_database_util.fetch_po_by_id(project_ID)  # Now returns PO data with detail items
    json_result = result.get_json()  # Extract the JSON data (as a dict/list)

    return render_template('po_template.html', data=json_result)