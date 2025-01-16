# webhook_main.py

import logging
from flask import Flask, jsonify, request, redirect, url_for

from dropbox_service import dropbox_service
from orchestration.orchestrator import Orchestrator
from monday_files.monday_webhook_handler import monday_blueprint
from monday_files.monday_api import monday_api
from dropbox_files.dropbox_webhook_handler import dropbox_blueprint
from po_log_database_util import po_log_database_util
from utilities.logger import setup_logging
from flask import render_template
# 1) Import your new utility
from database_view_util import DatabaseViewUtil

from routes.account_tax_routes import account_tax_bp


# Initialize Flask app
app = Flask(__name__)

orchestrator = Orchestrator()
# Register Blueprints with URL prefixes
app.register_blueprint(monday_blueprint, url_prefix='/webhook/monday')
app.register_blueprint(dropbox_blueprint, url_prefix='/webhook/dropbox')
app.register_blueprint(account_tax_bp)



logger = logging.getLogger(__name__)
setup_logging()


# 2) Instantiate your new class
db_view_util = DatabaseViewUtil()


@app.route("/account_tax_view", methods=["GET"])
def account_tax_view():
    """
    Shows an Excel-like table to edit accountCodes + TaxAccounts together.
    Accepts optional ?sort=account_code or ?sort=tax_code
    """
    sort = request.args.get("sort")
    # 3) Use our new method to fetch joined data
    records = db_view_util.get_all_account_with_tax(sort_by=sort)
    return render_template("map_codes_view.html", records=records, sort=sort)


@app.route("/bulk_update_account_tax", methods=["POST"])
def bulk_update_account_tax():
    """
    Accepts JSON data from the front-end with a list of updated rows.
    Calls the new method in DatabaseViewUtil to commit changes to DB.
    """
    data = request.get_json()
    if not data or not isinstance(data, list):
        return jsonify({"status": "error", "message": "Invalid input data"}), 400

    try:
        db_view_util.bulk_update_account_tax(data)
        return jsonify({"status": "success"}), 200
    except Exception as e:
        logger.error(f"Error during bulk update: {e}", exc_info=True)
        return jsonify({"status": "error", "message": str(e)}), 500

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

@app.route("/control_panel", methods=["GET"])
def control_panel():
    """
    Renders the Control Panel HTML page.
    """
    return render_template("control_panel.html")

@app.route("/toggle_temp_file", methods=["POST"])
def toggle_temp_file():
    current_value = dropbox_service.USE_TEMP_FILE
    new_value = not current_value
    dropbox_service.USE_TEMP_FILE = new_value
    logger.info(f"USE_TEMP_FILE toggled from {current_value} to {new_value}")
    return redirect(url_for("control_panel"))

@app.route("/trigger_function", methods=["POST"])
def trigger_function():
    """
    Triggers the requested function in the Orchestrator based odn form input.
    """
    logger = logging.getLogger("app_logger")
    function_name = request.form.get("function_name", "")
    project_number = request.form.get("project_number", "")  # This is optional

    try:
        if function_name == "schedule_monday_main_items_sync":
            orchestrator.sync_monday_main_items()
            logger.info("Scheduled monday_main_items_sync")
        elif function_name == "schedule_monday_sub_items_sync":
            orchestrator.sync_monday_sub_items()
            logger.info("Scheduled monday_sub_items_sync")
        elif function_name == "scan_project_receipts":
            # Make sure the user actually gave us a project_number
            if not project_number:
                logger.warning("No project number provided for scanning receipts.")
            else:
                orchestrator.scan_project_receipts(project_number)
        elif function_name == "scan_project_invoice":
            # Make sure the user actually gave us a project_number
            if not project_number:
                logger.warning("No project number provided for scanning receipts.")
            else:
                orchestrator.scan_project_invoices(project_number)
        elif function_name == "schedule_monday_contact_sync":
            orchestrator.sync_monday_contacts()
            logger.info("Scheduled monday_contact_sync")
        elif function_name == "sync_spend_money_items":
            orchestrator.sync_spend_money_items()
            logger.info("Called sync_spend_money_items")
        elif function_name == "sync_contacts":
            orchestrator.sync_contacts()
            logger.info("Called sync_contacts")
        elif function_name == "sync_xero_bills":
            orchestrator.sync_xero_bills()
            logger.info("Called sync_xero_bills")
        else:
            logger.error(f"Unknown function requested: {function_name}")
    except Exception as e:
        logger.error(f"Error triggering function {function_name}: {e}")

    # Redirect back to the control panel (or wherever you'd like)
    return redirect(url_for("control_panel"))


# region NEW MAP CODE VIEW & ROUTES
# -------------------------------------------------------------------------
@app.route("/map_codes_view", methods=["GET"])
def map_codes_view():
    """
    New approach: Renders a template with tabs for each map_code,
    each containing the two-panel (Account left / Tax right) layout,
    local storage logic, pagination, etc.
    """
    return render_template("map_codes_view.html")


