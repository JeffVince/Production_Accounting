# control_panel_model.py

from server_celery.celery_tasks import (
    process_invoice_trigger, process_invoice_delete,
    process_detail_item_update, process_detail_item_create, process_detail_item_delete,
    process_purchase_order_create, process_purchase_order_update, process_purchase_order_delete,
    process_contact_create, process_contact_update, process_contact_delete,
    process_bill_line_item_create, process_bill_line_item_update, process_bill_line_item_delete,
    process_bank_transaction_create, process_bank_transaction_update, process_bank_transaction_delete,
    process_account_code_create, process_account_code_update, process_account_code_delete,
    process_receipt_create, process_receipt_update, process_receipt_delete,
    process_spend_money_create, process_spend_money_update, process_spend_money_delete,
    process_tax_account_create, process_tax_account_update, process_tax_account_delete,
    process_xero_bill_update, process_xero_bill_create, process_xero_bill_delete,
    create_xero_bill_line_items, update_xero_bill_line_item, delete_xero_bill_line_item, process_po_log_create
)

def trigger_po_log_new(_):
    process_po_log_create.delay()

def trigger_invoice_create(invoice_id):
    # Additional logic could go here
    process_invoice_trigger.delay(int(invoice_id))

def trigger_invoice_delete(invoice_id):
    process_invoice_delete.delay(int(invoice_id))

def trigger_detail_item_update(detail_item_id):
    process_detail_item_update.delay(int(detail_item_id))

def trigger_detail_item_create(detail_item_id):
    process_detail_item_create.delay(int(detail_item_id))

def trigger_detail_item_delete(detail_item_id):
    process_detail_item_delete.delay(int(detail_item_id))

def trigger_purchase_order_create(po_id):
    process_purchase_order_create.delay(int(po_id))

def trigger_purchase_order_update(po_id):
    process_purchase_order_update.delay(int(po_id))

def trigger_purchase_order_delete(po_id):
    process_purchase_order_delete.delay(int(po_id))

def trigger_contact_create(contact_id):
    process_contact_create.delay(int(contact_id))

def trigger_contact_update(contact_id):
    process_contact_update.delay(int(contact_id))

def trigger_contact_delete(contact_id):
    process_contact_delete.delay(int(contact_id))

def trigger_bill_line_item_create(bill_line_item_id):
    process_bill_line_item_create.delay(int(bill_line_item_id))

def trigger_bill_line_item_update(bill_line_item_id):
    process_bill_line_item_update.delay(int(bill_line_item_id))

def trigger_bill_line_item_delete(bill_line_item_id):
    process_bill_line_item_delete.delay(int(bill_line_item_id))

def trigger_bank_transaction_create(bank_tx_id):
    process_bank_transaction_create.delay(int(bank_tx_id))

def trigger_bank_transaction_update(bank_tx_id):
    process_bank_transaction_update.delay(int(bank_tx_id))

def trigger_bank_transaction_delete(bank_tx_id):
    process_bank_transaction_delete.delay(int(bank_tx_id))

def trigger_account_code_create(account_code_id):
    process_account_code_create.delay(int(account_code_id))

def trigger_account_code_update(account_code_id):
    process_account_code_update.delay(int(account_code_id))

def trigger_account_code_delete(account_code_id):
    process_account_code_delete.delay(int(account_code_id))

def trigger_receipt_create(receipt_id):
    process_receipt_create.delay(int(receipt_id))

def trigger_receipt_update(receipt_id):
    process_receipt_update.delay(int(receipt_id))

def trigger_receipt_delete(receipt_id):
    process_receipt_delete.delay(int(receipt_id))

def trigger_spend_money_create(spend_money_id):
    process_spend_money_create.delay(int(spend_money_id))

def trigger_spend_money_update(spend_money_id):
    process_spend_money_update.delay(int(spend_money_id))

def trigger_spend_money_delete(spend_money_id):
    process_spend_money_delete.delay(int(spend_money_id))

def trigger_tax_account_create(tax_account_id):
    process_tax_account_create.delay(int(tax_account_id))

def trigger_tax_account_update(tax_account_id):
    process_tax_account_update.delay(int(tax_account_id))

def trigger_tax_account_delete(tax_account_id):
    process_tax_account_delete.delay(int(tax_account_id))

def trigger_xero_bill_update(bill_id):
    process_xero_bill_update.delay(int(bill_id))

def trigger_xero_bill_create(bill_id):
    """
    Note that `bill_id` might be string-based, depending on how you store it.
    Convert carefully if your Celery tasks always expect an int.
    """
    try:
        int_id = int(bill_id)
        process_xero_bill_create.delay(int_id)
    except ValueError:
        # If it's truly a string-based ID (like a GUID), then pass as-is
        process_xero_bill_create.delay(bill_id)

def trigger_xero_bill_delete(bill_id):
    process_xero_bill_delete.delay(int(bill_id))

def trigger_create_xero_bill_line_items(bill_id):
    create_xero_bill_line_items.delay(int(bill_id))

def trigger_update_xero_bill_line_item(line_item_id):
    update_xero_bill_line_item.delay(int(line_item_id))

def trigger_delete_xero_bill_line_item(line_item_id):
    delete_xero_bill_line_item.delay(int(line_item_id))