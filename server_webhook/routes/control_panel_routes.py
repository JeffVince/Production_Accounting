# control_panel_routes.py

from flask import Blueprint, request, jsonify
import logging

from server_webhook.models.control_panel_model import (
    trigger_po_log_new,
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

# Import the orchestrator instance
from server_webhook.orchestrator import orchestrator

# Initialize logger
logger = logging.getLogger("admin_logger")

control_panel_bp = Blueprint('control_panel_bp', __name__)

# Existing route mapping for trigger tasks...
ROUTE_MAPPING = {
    'trigger_po_log_new': trigger_po_log_new,
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
    data = request.get_json()
    value = data.get('value')
    logger.info(f"🌟[Event] [Route = {route}] [Value = {value}]")

    if value is None:
        logger.warning(f"💥 [Event] [Route = {route}] [Value = None]")
        return jsonify({"message": "No ID provided."}), 400

    try:
        ROUTE_MAPPING[route](value)
        task_name = route.replace('trigger_', '').replace('_', ' ').title()
        return jsonify({"message": f"Task '{task_name}' triggered for ID: {value}"}), 200
    except Exception as e:
        logger.exception(f"Error triggering task '{route}' with ID '{value}': {e}")
        return jsonify({"message": f"Failed to trigger task '{route}'."}), 500

# New route for handling sync functions
@control_panel_bp.route('/sync/<sync_route>', methods=['POST'])
def handle_sync_route(sync_route):
    if sync_route == 'sync_spend_money_items':
        try:
            result = orchestrator.sync_spend_money_items()
            return jsonify({"message": f"Sync Spend Money Items triggered. Result: {result}"}), 200
        except Exception as e:
            logger.exception(f"Error triggering sync_spend_money_items: {e}")
            return jsonify({"message": "Failed to sync spend money items."}), 500
    elif sync_route == 'sync_xero_bills':
        try:
            result = orchestrator.sync_xero_bills()
            return jsonify({"message": f"Sync Xero Bills triggered. Result: {result}"}), 200
        except Exception as e:
            logger.exception(f"Error triggering sync_xero_bills: {e}")
            return jsonify({"message": "Failed to sync xero bills."}), 500
    else:
        logger.warning(f"Invalid sync route attempted: {sync_route}")
        return jsonify({"message": "Invalid sync route."}), 400