# control_panel_routes.py

from flask import Blueprint, request, jsonify
import logging

from server_webhook.models.control_panel_model import (
    trigger_po_log_new,  # Import the new trigger function
    trigger_invoice_create, trigger_invoice_delete,
    trigger_detail_item_update, trigger_detail_item_create, trigger_detail_item_delete,
    trigger_purchase_order_create, trigger_purchase_order_update, trigger_purchase_order_delete,
    trigger_contact_create, trigger_contact_update, trigger_contact_delete,
    trigger_xero_bill_line_item_create, trigger_xero_bill_line_item_update, trigger_xero_bill_line_item_delete,
    trigger_bank_transaction_create, trigger_bank_transaction_update, trigger_bank_transaction_delete,
    trigger_account_code_create, trigger_account_code_update, trigger_account_code_delete,
    trigger_receipt_create, trigger_receipt_update, trigger_receipt_delete,
    trigger_spend_money_create, trigger_spend_money_update, trigger_spend_money_delete,
    trigger_tax_account_create, trigger_tax_account_update, trigger_tax_account_delete,
    trigger_xero_bill_create, trigger_xero_bill_update, trigger_xero_bill_delete,
    trigger_create_xero_xero_bill_line_items, trigger_update_xero_xero_bill_line_item, trigger_delete_xero_xero_bill_line_item
)

# Initialize logger
logger = logging.getLogger("admin_logger")

control_panel_bp = Blueprint('control_panel_bp', __name__)

# Dictionary mapping routes to their corresponding trigger functions
ROUTE_MAPPING = {
    'trigger_po_log_new': trigger_po_log_new,  # Add the new route mapping
    'trigger_invoice_create': trigger_invoice_create,
    'trigger_invoice_delete': trigger_invoice_delete,
    'trigger_detail_item_create': trigger_detail_item_create,
    'trigger_detail_item_update': trigger_detail_item_update,
    'trigger_detail_item_delete': trigger_detail_item_delete,
    'trigger_purchase_order_create': trigger_purchase_order_create,
    'trigger_purchase_order_update': trigger_purchase_order_update,
    'trigger_purchase_order_delete': trigger_purchase_order_delete,
    'trigger_contact_create': trigger_contact_create,
    'trigger_contact_update': trigger_contact_update,
    'trigger_contact_delete': trigger_contact_delete,
    'trigger_xero_bill_line_item_create': trigger_xero_bill_line_item_create,
    'trigger_xero_bill_line_item_update': trigger_xero_bill_line_item_update,
    'trigger_xero_bill_line_item_delete': trigger_xero_bill_line_item_delete,
    'trigger_bank_transaction_create': trigger_bank_transaction_create,
    'trigger_bank_transaction_update': trigger_bank_transaction_update,
    'trigger_bank_transaction_delete': trigger_bank_transaction_delete,
    'trigger_account_code_create': trigger_account_code_create,
    'trigger_account_code_update': trigger_account_code_update,
    'trigger_account_code_delete': trigger_account_code_delete,
    'trigger_receipt_create': trigger_receipt_create,
    'trigger_receipt_update': trigger_receipt_update,
    'trigger_receipt_delete': trigger_receipt_delete,
    'trigger_spend_money_create': trigger_spend_money_create,
    'trigger_spend_money_update': trigger_spend_money_update,
    'trigger_spend_money_delete': trigger_spend_money_delete,
    'trigger_tax_account_create': trigger_tax_account_create,
    'trigger_tax_account_update': trigger_tax_account_update,
    'trigger_tax_account_delete': trigger_tax_account_delete,
    'trigger_xero_bill_create': trigger_xero_bill_create,
    'trigger_xero_bill_update': trigger_xero_bill_update,
    'trigger_xero_bill_delete': trigger_xero_bill_delete,
    'trigger_create_xero_xero_bill_line_items': trigger_create_xero_xero_bill_line_items,
    'trigger_update_xero_xero_bill_line_item': trigger_update_xero_xero_bill_line_item,
    'trigger_delete_xero_xero_bill_line_item': trigger_delete_xero_xero_bill_line_item
}


@control_panel_bp.route('/control_panel/<route>', methods=['POST'])
def handle_route(route):
    if route not in ROUTE_MAPPING:
        logger.warning(f"Invalid route attempted: {route}")
        return jsonify({"message": "Invalid task route."}), 400
    else:
        data = request.get_json()
        value = data.get('value')
        logger.info(f"🌟[Event] [Route = {route}] [Value = {value}]")





    if value is None:
        logger.warning(f"💥 [Event] [Route = {route}] [Value = None]")
        return jsonify({"message": "No ID provided."}), 400

    try:
        # Call the corresponding trigger function
        ROUTE_MAPPING[route](value)
        # Format the task name for readability
        task_name = route.replace('trigger_', '').replace('_', ' ').title()
        return jsonify({"message": f"Task '{task_name}' triggered for ID: {value}"}), 200
    except Exception as e:
        logger.exception(f"Error triggering task '{route}' with ID '{value}': {e}")
        return jsonify({"message": f"Failed to trigger task '{route}'."}), 500