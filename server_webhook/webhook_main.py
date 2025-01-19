import logging

import requests
from flask import Flask, jsonify, request, redirect, url_for, Response

from utilities.config import Config
from files_dropbox.dropbox_service import dropbox_service
from orchestrator import Orchestrator
from files_monday.monday_webhook_handler import monday_blueprint
from files_monday.monday_api import monday_api
from files_dropbox.dropbox_webhook_handler import dropbox_blueprint
from po_log_database_util import po_log_database_util
from utilities.logger import setup_logging
from flask import render_template
from server_webhook.models.account_tax_model import AccountTaxModel
from routes.account_tax_routes import account_tax_bp

app = Flask(__name__)
orchestrator = Orchestrator()
app.register_blueprint(monday_blueprint, url_prefix='/webhook/monday')
app.register_blueprint(dropbox_blueprint, url_prefix='/webhook/dropbox')
app.register_blueprint(account_tax_bp)
logger = logging.getLogger(__name__)
setup_logging()
db_view_util = AccountTaxModel()
config = Config()

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
        logger.info(project_id, po_number)
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

@app.route('/toggle_temp_file', methods=['POST'])
def toggle_temp_file():
    current_value = dropbox_service.USE_TEMP_FILE
    new_value = not current_value
    dropbox_service.USE_TEMP_FILE = new_value
    logger.info(f'USE_TEMP_FILE toggled from {current_value} to {new_value}')
    return redirect(url_for('control_panel'))

@app.route('/trigger_function', methods=['POST'])
def trigger_function():
    """
    Triggers the requested function in the Orchestrator based odn form input.
    """
    logger = logging.getLogger('admin_logger')
    function_name = request.form.get('function_name', '')
    project_number = request.form.get('project_number', '')
    try:
        if function_name == 'schedule_monday_main_items_sync':
            orchestrator.sync_monday_main_items()
            logger.info('Scheduled monday_main_items_sync')
        elif function_name == 'schedule_monday_sub_items_sync':
            orchestrator.sync_monday_sub_items()
            logger.info('Scheduled monday_sub_items_sync')
        elif function_name == 'scan_project_receipts':
            if not project_number:
                logger.warning('No project number provided for scanning receipts.')
            else:
                orchestrator.scan_project_receipts(project_number)
        elif function_name == 'scan_project_invoice':
            if not project_number:
                logger.warning('No project number provided for scanning receipts.')
            else:
                orchestrator.scan_project_invoices(project_number)
        elif function_name == 'schedule_monday_contact_sync':
            orchestrator.sync_monday_contacts()
            logger.info('Scheduled monday_contact_sync')
        elif function_name == 'sync_spend_money_items':
            orchestrator.sync_spend_money_items()
            logger.info('Called sync_spend_money_items')
        elif function_name == 'sync_contacts':
            orchestrator.sync_contacts()
            logger.info('Called sync_contacts')
        elif function_name == 'sync_xero_bills':
            orchestrator.sync_xero_bills()
            logger.info('Called sync_xero_bills')
        else:
            logger.error(f'Unknown function requested: {function_name}')
    except Exception as e:
        logger.error(f'Error triggering function {function_name}: {e}')
    return redirect(url_for('control_panel'))

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