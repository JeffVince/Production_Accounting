# server_webhook/webhook_main.py
import logging
logger = logging.getLogger("admin_logger")

import requests

try:
    from flask import Blueprint, jsonify, request, Response, render_template

    from utilities.config import Config

    #from files_monday.monday_webhook_handler import monday_blueprint

    #from files_monday.monday_api import monday_api

    from files_dropbox.dropbox_webhook_handler import dropbox_blueprint
    logger.debug("Importing into Webhook Main")


    from files_budget.po_log_database_util import po_log_database_util
    from server_webhook.logging_setup import setup_logging, setup_web_logger
    from server_webhook.models.account_tax_model import AccountTaxModel
    from routes.account_tax_routes import account_tax_bp
    from server_webhook.routes.control_panel_routes import control_panel_bp
    from server_webhook.routes.agent_routes import agent_bp
except Exception as e:
    logger.error(f"Error importing into webhook main: {e}")

logger.debug("Succesfully imported into Webhook Main")


# Create a Blueprint instead of a full Flask app
webhook_main_bp = Blueprint('webhook_main_bp', __name__)

logger.debug("🐣 Blueprint created, preparing to register sub-blueprints...")

# Register other sub-blueprints:
#webhook_main_bp.register_blueprint(monday_blueprint, url_prefix='/webhook/monday')
#logger.debug("🧩 monday_blueprint registered at '/webhook/monday'")

webhook_main_bp.register_blueprint(dropbox_blueprint, url_prefix='/webhook/dropbox')
logger.debug("🧩 dropbox_blueprint registered at '/webhook/dropbox'")

webhook_main_bp.register_blueprint(control_panel_bp)
logger.debug("🧩 control_panel_bp registered with default prefix '/'")

webhook_main_bp.register_blueprint(account_tax_bp)
logger.debug("🧩 account_tax_bp registered with default prefix '/'")


# Load config
logger.debug("🔧 Loading configuration from Config()...")
config = Config()
logger.debug("🔧 Configuration loaded successfully. Details below:")
logger.info(f"📝 USE_TEMP={config.USE_TEMP}, SKIP_MAIN={config.SKIP_MAIN}, "
            f"USE_LOCAL={config.USE_LOCAL}, APP_DEBUG={config.APP_DEBUG}, "
            f"WEBHOOK_MAIN_PORT={config.WEBHOOK_MAIN_PORT}, "
            f"WEBHOOK_MAIN_PORT_DEBUG={config.WEBHOOK_MAIN_PORT_DEBUG}, "
            f"DATABASE_URL={config.DATABASE_URL}")

# Initialize the AccountTaxModel
logger.debug("🔧 Initializing AccountTaxModel...")
db_view_util = AccountTaxModel()
logger.debug("🔧 AccountTaxModel initialized successfully.")
logger.info("✅ Server webhook main blueprint is ready and loaded!")

# --------------------------------------------------------------------------
#                           Route Handlers
# --------------------------------------------------------------------------

@webhook_main_bp.route('/account_tax_view', methods=['GET'])
def account_tax_view():
    """
    Shows an Excel-like table to edit accountCodes + TaxAccounts together.
    Accepts optional ?sort=account_code or ?sort=tax_code
    """
    logger.debug("🏷 [ /account_tax_view ] - Handling GET request.")
    sort = request.args.get('sort')
    logger.debug(f"📥 [ /account_tax_view ] - Received sort parameter: {sort}")
    records = db_view_util.get_all_account_with_tax(sort_by=sort)
    logger.debug("📃 [ /account_tax_view ] - Retrieved records from db_view_util.")
#    logger.info(f"✅ [ /account_tax_view ] - Rendering map_codes_view.html with {len(records)} record(s).")
    return render_template('map_codes_view.html', records=records, sort=sort)

@webhook_main_bp.route('/bulk_update_account_tax', methods=['POST'])
def bulk_update_account_tax():
    """
    Accepts JSON data from the front-end with a list of updated rows.
    Calls the new method in AccountTaxModel to commit changes to DB.
    """
    logger.debug("📨 [ /bulk_update_account_tax ] - Handling POST request for bulk update.")
    data = request.get_json()
    logger.debug(f"📥 [ /bulk_update_account_tax ] - Data received: {data}")
    if not data or not isinstance(data, list):
        logger.warning("⚠️ [ /bulk_update_account_tax ] - Invalid input data.")
        return jsonify({'status': 'error', 'message': 'Invalid input data'}), 400
    try:
        logger.debug(f"🔨 [ /bulk_update_account_tax ] - Attempting to bulk update {len(data)} rows...")
        db_view_util.bulk_update_account_tax(data)
        logger.info("✅ [ /bulk_update_account_tax ] - Bulk update successful.")
        return jsonify({'status': 'success'}), 200
    except Exception as e:
        logger.error(f"💥 [ /bulk_update_account_tax ] - Error during bulk update: {e}", exc_info=True)
        return jsonify({'status': 'error', 'message': str(e)}), 500




@webhook_main_bp.route('/po_html/<string:project_ID>', methods=['GET'])
def po_html(project_ID):
    logger.debug(f"🔎 [ /po_html/{project_ID} ] - Handling GET request.")
    logger.info(f"📝 [ /po_html ] - Project ID requested: {project_ID}")
    result = po_log_database_util.fetch_po_by_id(project_ID)
    json_result = result.get_json()
    logger.debug("✅ [ /po_html ] - Result converted to JSON successfully. Rendering template.")
    return render_template('po_template.html', data=json_result)



@webhook_main_bp.route('/control_panel', methods=['GET'])
def control_panel():
    """
    Renders the Control Panel HTML page.
    """
    logger.debug("🏗 [ /control_panel ] - Handling GET to render Control Panel page.")
    logger.info("✅ [ /control_panel ] - Rendering control_panel.html.")
    return render_template('control_panel.html')

@webhook_main_bp.route('/map_codes_view', methods=['GET'])
def map_codes_view():
    """
    Renders a template with tabs for each map_code,
    each containing the two-panel (Account left / Tax right) layout,
    local storage logic, pagination, etc.
    """
    logger.debug("🏗 [ /map_codes_view ] - Handling GET request.")
    logger.info("✅ [ /map_codes_view ] - Rendering map_codes_view.html.")
    return render_template('map_codes_view.html')

@webhook_main_bp.route('/dev/<path:subpath>', methods=['GET', 'POST', 'PUT', 'PATCH', 'DELETE'])
def dev_proxy(subpath):
    """
    Forwards any request under /dev/... to the dev server on port 5003.
    If the dev server is offline, returns a 200 with a JSON indicating offline status.
    """
    logger.debug(f"🔎 [ /dev/{subpath} ] - Handling {request.method} request.")
    logger.info(f"➡️ [ /dev ] - Attempting to proxy request to /dev/{subpath}")
    dev_url = f"http://localhost:{config.WEBHOOK_MAIN_PORT_DEBUG}/{subpath}"
    logger.debug(f"🔧 [ /dev ] - Constructed dev server URL: {dev_url}")

    try:
        logger.debug("🌐 [ /dev ] - Forwarding request to dev server now...")
        resp = requests.request(
            method=request.method,
            url=dev_url,
            headers={key: value for key, value in request.headers if key.lower() != 'host'},
            data=request.get_data(),
            cookies=request.cookies,
            allow_redirects=False
        )
        logger.debug(f"✅ [ /dev ] - Received response with status code {resp.status_code} from dev server.")
        excluded_headers = ['content-encoding', 'transfer-encoding', 'connection']
        headers = [
            (name, value) for (name, value) in resp.raw.headers.items()
            if name.lower() not in excluded_headers
        ]
        response = Response(resp.content, resp.status_code, headers)
        logger.debug("✅ [ /dev ] - Response object built. Returning to client now.")
        return response
    except requests.exceptions.ConnectionError:
        logger.warning("⚠️ [ /dev ] - Dev server is offline or not reachable.")
        return jsonify({
            'message': 'Dev server offline',
            'status': 'offline',
            'forwarded_path': subpath
        }), 200