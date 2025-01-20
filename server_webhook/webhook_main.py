import logging

import requests
from flask import Flask, jsonify, request, redirect, url_for, Response

from server_webhook.routes.control_panel_routes import control_panel_bp
from utilities.config import Config
from files_monday.monday_webhook_handler import monday_blueprint
from files_monday.monday_api import monday_api
from files_dropbox.dropbox_webhook_handler import dropbox_blueprint
from files_budget.po_log_database_util import po_log_database_util
from server_webhook.logging_setup import setup_logging
from flask import render_template
from server_webhook.models.account_tax_model import AccountTaxModel
from routes.account_tax_routes import account_tax_bp

app = Flask(__name__)
app.register_blueprint(monday_blueprint, url_prefix='/webhook/monday')
app.register_blueprint(dropbox_blueprint, url_prefix='/webhook/dropbox')
app.register_blueprint(control_panel_bp)
app.register_blueprint(account_tax_bp)
setup_logging(flask_app=app)
config = Config()
db_view_util = AccountTaxModel()

logger = logging.getLogger("web_logger")

@app.route('/account_tax_view', methods=['GET'])
def account_tax_view():
    """
    Shows an Excel-like table to edit accountCodes + TaxAccounts together.
    Accepts optional ?sort=account_code or ?sort=tax_code
    """
    sort = request.args.get('sort')
    records = db_view_util.get_all_account_with_tax(sort_by=sort)
    return render_template('map_codes_view.html', records=records, sort=sort)

@app.route('/bulk_update_account_tax', methods=['POST'])
def bulk_update_account_tax():
    """
    Accepts JSON data from the front-end with a list of updated rows.
    Calls the new method in AccountTaxModel to commit changes to DB.
    """
    data = request.get_json()
    if not data or not isinstance(data, list):
        return (jsonify({'status': 'error', 'message': 'Invalid input data'}), 400)
    try:
        db_view_util.bulk_update_account_tax(data)
        return (jsonify({'status': 'success'}), 200)
    except Exception as e:
        logger.error(f'Error during bulk update: {e}', exc_info=True)
        return (jsonify({'status': 'error', 'message': str(e)}), 500)

@app.route('/health', methods=['GET'])
def index():
    return (jsonify({'message': 'Webhook listener is running.'}), 200)

@app.route('/po/<string:po_id>', methods=['GET'])
def get_po_data(po_id):
    try:
        (project_id, po_number) = po_id.split('_')
        logger.info(f"Project ID: {project_id}, PO Number: {po_number}")
    except ValueError:
        return ({'error': "Invalid PO ID format. Expected format: '2416_04'"}, 400)
    result = monday_api.fetch_item_by_po_and_project(project_id, po_number)
    try:
        json_result = jsonify(result)
    except Exception as e:
        logger.error(e)
        raise
    return json_result

@app.route('/po_html/<string:project_ID>', methods=['GET'])
def po_html(project_ID):
    logger.info(project_ID)
    result = po_log_database_util.fetch_po_by_id(project_ID)
    json_result = result.get_json()
    return render_template('po_template.html', data=json_result)

@app.route('/control_panel', methods=['GET'])
def control_panel():
    """
    Renders the Control Panel HTML page.
    """
    return render_template('control_panel.html')






@app.route('/map_codes_view', methods=['GET'])
def map_codes_view():
    """
    New approach: Renders a template with tabs for each map_code,
    each containing the two-panel (Account left / Tax right) layout,
    local storage logic, pagination, etc.
    """
    return render_template('map_codes_view.html')


#########################################
#     Dev Proxy Route: /dev/<path>      #
#########################################
@app.route('/dev/<path:subpath>', methods=['GET', 'POST', 'PUT', 'PATCH', 'DELETE'])
def dev_proxy(subpath):
    """
    Forwards any request under /dev/... to the dev server on port 5003.
    If the dev server is offline, returns a 200 with a JSON indicating offline status.
    """
    dev_url = f"http://localhost:{config.WEBHOOK_MAIN_PORT_DEBUG}/{subpath}"

    try:
        # Forward the request method, headers, and data to dev server
        resp = requests.request(
            method=request.method,
            url=dev_url,
            headers={key: value for key, value in request.headers if key != 'Host'},
            data=request.get_data(),
            cookies=request.cookies,
            allow_redirects=False
        )

        # Build a Flask Response object from the requests response
        excluded_headers = ['content-encoding', 'transfer-encoding', 'connection']
        headers = [(name, value) for (name, value) in resp.raw.headers.items()
                   if name.lower() not in excluded_headers]

        response = Response(resp.content, resp.status_code, headers)
        return response

    except requests.exceptions.ConnectionError:
        logger.warning("Dev server is offline or not reachable.")
        # Return a 200 with JSON body
        return jsonify({
            'message': 'Dev server offline',
            'status': 'offline',
            'forwarded_path': subpath
        }), 200