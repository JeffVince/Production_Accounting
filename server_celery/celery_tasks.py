"""
celery_tasks.py

Houses all Celery tasks.
Does not import 'database_trigger.py' to avoid circular references.
"""
from celery import shared_task
from server_celery.celery_task_services import celery_task_service

import logging
logger = logging.getLogger('admin_logger')

@shared_task
def process_invoice_trigger(invoice_id: int):
    """
    The Celery task for handling invoice creates/updates.
    """
    logger.info(f'🚀 Starting process_invoice_trigger shared task. invoice_id={invoice_id}.')
    try:
        trigger_service = celery_task_service
        trigger_service.invoice_trigger_on_create_or_update(invoice_id)
        logger.info(f'🎉 Done with invoice #{invoice_id}.')
        return f'Invoice {invoice_id} processed successfully!'
    except Exception as e:
        logger.error(f'💥 Problem in process_invoice_trigger({invoice_id}): {e}', exc_info=True)
        raise

@shared_task
def process_invoice_delete(invoice_id: int):
    """
    The Celery task for handling invoice deletes.
    """
    logger.info(f'🗑️ Handling invoice deletion for invoice_id={invoice_id}.')
    try:
        trigger_service = celery_task_service
        trigger_service.invoice_trigger_on_delete(invoice_id)
        logger.info(f'✅ Invoice #{invoice_id} deletion handled successfully.')
        return f'Invoice {invoice_id} deletion processed!'
    except Exception as e:
        logger.error(f'💥 Problem in process_invoice_delete({invoice_id}): {e}', exc_info=True)
        raise

@shared_task
def process_detail_item_update(detail_item_id: int):
    """
    The Celery task for detail items that just turned RTP (or updated).
    """
    logger.info(f'🌀 Handling updated detail item for detail_item_id={detail_item_id}')
    try:
        trigger_service = celery_task_service
        trigger_service.detail_item_trigger_on_update(detail_item_id)
        logger.info(f'✅ detail_item_set_to_rtp completed for id={detail_item_id}')
        return 'Success'
    except Exception as e:
        logger.error(f'💥 Problem with detail_item_set_to_rtp({detail_item_id}): {e}', exc_info=True)
        raise

@shared_task
def process_detail_item_create(detail_item_id: int):
    """
    The Celery task for newly created detail items.
    """
    logger.info(f'🌀 Handling created detail item for detail_item_id={detail_item_id}')
    try:
        trigger_service = celery_task_service
        trigger_service.detail_item_trigger_on_create(detail_item_id)
        logger.info(f'✅ detail_item_set_to_rtp completed for id={detail_item_id}')
        return 'Success'
    except Exception as e:
        logger.error(f'💥 Problem with detail_item_set_to_rtp({detail_item_id}): {e}', exc_info=True)
        raise

@shared_task
def process_detail_item_delete(detail_item_id: int):
    """
    The Celery task for deleted detail items.
    """
    logger.info(f'🗑️ Handling deleted detail item for detail_item_id={detail_item_id}')
    try:
        trigger_service = celery_task_service
        trigger_service.detail_item_on_delete(detail_item_id)
        logger.info(f'✅ Detail item deletion completed for id={detail_item_id}')
        return 'Success'
    except Exception as e:
        logger.error(f'💥 Problem with detail_item_on_delete({detail_item_id}): {e}', exc_info=True)
        raise

@shared_task
def process_purchase_order_create(po_id: int):
    logger.info(f'🚀 Starting process_purchase_order_create shared task. po_id={po_id}.')
    try:
        trigger_service = celery_task_service
        trigger_service.purchase_order_trigger_on_create(po_id)
        logger.info(f'🎉 Done processing newly created PO #{po_id}.')
        return f'PurchaseOrder {po_id} created successfully!'
    except Exception as e:
        logger.error(f'💥 Problem in process_purchase_order_create({po_id}): {e}', exc_info=True)
        raise

@shared_task
def process_purchase_order_update(po_id: int):
    logger.info(f'🔄 Handling updated PurchaseOrder id={po_id}.')
    try:
        trigger_service = celery_task_service
        trigger_service.purchase_order_trigger_on_update(po_id)
        logger.info(f'🎉 Done updating PO #{po_id}.')
        return f'PurchaseOrder {po_id} updated successfully!'
    except Exception as e:
        logger.error(f'💥 Problem in process_purchase_order_update({po_id}): {e}', exc_info=True)
        raise

@shared_task
def process_purchase_order_delete(po_id: int):
    logger.info(f'🗑️ Handling deleted PurchaseOrder id={po_id}.')
    try:
        trigger_service = celery_task_service
        trigger_service.purchase_order_trigger_on_delete(po_id)
        logger.info(f'✅ PurchaseOrder #{po_id} deletion handled.')
        return f'PurchaseOrder {po_id} deletion processed!'
    except Exception as e:
        logger.error(f'💥 Problem in process_purchase_order_delete({po_id}): {e}', exc_info=True)
        raise

@shared_task
def process_contact_create(contact_id: int):
    logger.info(f'🚀 Starting process_contact_create shared task. contact_id={contact_id}.')
    try:
        trigger_service = celery_task_service
        trigger_service.contact_trigger_on_create(contact_id)
        logger.info(f'🎉 Done processing newly created Contact #{contact_id}.')
        return f'Contact {contact_id} created successfully!'
    except Exception as e:
        logger.error(f'💥 Problem in process_contact_create({contact_id}): {e}', exc_info=True)
        raise

@shared_task
def process_contact_update(contact_id: int):
    logger.info(f'🔄 Handling updated Contact id={contact_id}.')
    try:
        trigger_service = celery_task_service
        trigger_service.contact_trigger_on_update(contact_id)
        logger.info(f'🎉 Done updating Contact #{contact_id}.')
        return f'Contact {contact_id} updated successfully!'
    except Exception as e:
        logger.error(f'💥 Problem in process_contact_update({contact_id}): {e}', exc_info=True)
        raise

@shared_task
def process_contact_delete(contact_id: int):
    logger.info(f'🗑️ Handling deleted Contact id={contact_id}.')
    try:
        trigger_service = celery_task_service
        trigger_service.contact_trigger_on_delete(contact_id)
        logger.info(f'✅ Contact #{contact_id} deletion handled.')
        return f'Contact {contact_id} deletion processed!'
    except Exception as e:
        logger.error(f'💥 Problem in process_contact_delete({contact_id}): {e}', exc_info=True)
        raise

@shared_task
def process_bill_line_item_create(bill_line_item_id: int):
    logger.info(f'🚀 Starting process_bill_line_item_create shared task. bill_line_item_id={bill_line_item_id}.')
    try:
        trigger_service = celery_task_service
        trigger_service.bill_line_item_trigger_on_create(bill_line_item_id)
        logger.info(f'🎉 Done processing newly created BillLineItem #{bill_line_item_id}.')
        return f'BillLineItem {bill_line_item_id} created successfully!'
    except Exception as e:
        logger.error(f'💥 Problem in process_bill_line_item_create({bill_line_item_id}): {e}', exc_info=True)
        raise

@shared_task
def process_bill_line_item_update(bill_line_item_id: int):
    logger.info(f'🔄 Handling updated BillLineItem id={bill_line_item_id}.')
    try:
        trigger_service = celery_task_service
        trigger_service.bill_line_item_trigger_on_update(bill_line_item_id)
        logger.info(f'🎉 Done updating BillLineItem #{bill_line_item_id}.')
        return f'BillLineItem {bill_line_item_id} updated successfully!'
    except Exception as e:
        logger.error(f'💥 Problem in process_bill_line_item_update({bill_line_item_id}): {e}', exc_info=True)
        raise

@shared_task
def process_bill_line_item_delete(bill_line_item_id: int):
    logger.info(f'🗑️ Handling deleted BillLineItem id={bill_line_item_id}.')
    try:
        trigger_service = celery_task_service
        trigger_service.bill_line_item_trigger_on_delete(bill_line_item_id)
        logger.info(f'✅ BillLineItem #{bill_line_item_id} deletion handled.')
        return f'BillLineItem {bill_line_item_id} deletion processed!'
    except Exception as e:
        logger.error(f'💥 Problem in process_bill_line_item_delete({bill_line_item_id}): {e}', exc_info=True)
        raise

@shared_task
def process_bank_transaction_create(bank_tx_id: int):
    logger.info(f'🚀 Starting process_bank_transaction_create shared task. bank_tx_id={bank_tx_id}.')
    try:
        trigger_service = celery_task_service
        trigger_service.bank_transaction_trigger_on_create(bank_tx_id)
        logger.info(f'🎉 Done processing newly created BankTransaction #{bank_tx_id}.')
        return f'BankTransaction {bank_tx_id} created successfully!'
    except Exception as e:
        logger.error(f'💥 Problem in process_bank_transaction_create({bank_tx_id}): {e}', exc_info=True)
        raise

@shared_task
def process_bank_transaction_update(bank_tx_id: int):
    logger.info(f'🔄 Handling updated BankTransaction id={bank_tx_id}.')
    try:
        trigger_service = celery_task_service
        trigger_service.bank_transaction_trigger_on_update(bank_tx_id)
        logger.info(f'🎉 Done updating BankTransaction #{bank_tx_id}.')
        return f'BankTransaction {bank_tx_id} updated successfully!'
    except Exception as e:
        logger.error(f'💥 Problem in process_bank_transaction_update({bank_tx_id}): {e}', exc_info=True)
        raise

@shared_task
def process_bank_transaction_delete(bank_tx_id: int):
    logger.info(f'🗑️ Handling deleted BankTransaction id={bank_tx_id}.')
    try:
        trigger_service = celery_task_service
        trigger_service.bank_transaction_trigger_on_delete(bank_tx_id)
        logger.info(f'✅ BankTransaction #{bank_tx_id} deletion handled.')
        return f'BankTransaction {bank_tx_id} deletion processed!'
    except Exception as e:
        logger.error(f'💥 Problem in process_bank_transaction_delete({bank_tx_id}): {e}', exc_info=True)
        raise

@shared_task
def process_account_code_create(account_code_id: int):
    logger.info(f'🚀 Starting process_account_code_create shared task. account_code_id={account_code_id}.')
    try:
        trigger_service = celery_task_service
        trigger_service.account_code_trigger_on_create(account_code_id)
        logger.info(f'🎉 Done processing newly created AccountCode #{account_code_id}.')
        return f'AccountCode {account_code_id} created successfully!'
    except Exception as e:
        logger.error(f'💥 Problem in process_account_code_create({account_code_id}): {e}', exc_info=True)
        raise

@shared_task
def process_account_code_update(account_code_id: int):
    logger.info(f'🔄 Handling updated AccountCode id={account_code_id}.')
    try:
        trigger_service = celery_task_service
        trigger_service.account_code_trigger_on_update(account_code_id)
        logger.info(f'🎉 Done updating AccountCode #{account_code_id}.')
        return f'AccountCode {account_code_id} updated successfully!'
    except Exception as e:
        logger.error(f'💥 Problem in process_account_code_update({account_code_id}): {e}', exc_info=True)
        raise

@shared_task
def process_account_code_delete(account_code_id: int):
    logger.info(f'🗑️ Handling deleted AccountCode id={account_code_id}.')
    try:
        trigger_service = celery_task_service
        trigger_service.account_code_trigger_on_delete(account_code_id)
        logger.info(f'✅ AccountCode #{account_code_id} deletion handled.')
        return f'AccountCode {account_code_id} deletion processed!'
    except Exception as e:
        logger.error(f'💥 Problem in process_account_code_delete({account_code_id}): {e}', exc_info=True)
        raise

@shared_task
def process_receipt_create(receipt_id: int):
    logger.info(f'🚀 Starting process_receipt_create shared task. receipt_id={receipt_id}.')
    try:
        trigger_service = celery_task_service
        trigger_service.receipt_trigger_on_create(receipt_id)
        logger.info(f'🎉 Done processing newly created Receipt #{receipt_id}.')
        return f'Receipt {receipt_id} created successfully!'
    except Exception as e:
        logger.error(f'💥 Problem in process_receipt_create({receipt_id}): {e}', exc_info=True)
        raise

@shared_task
def process_receipt_update(receipt_id: int):
    logger.info(f'🔄 Handling updated Receipt id={receipt_id}.')
    try:
        trigger_service = celery_task_service
        trigger_service.receipt_trigger_on_update(receipt_id)
        logger.info(f'🎉 Done updating Receipt #{receipt_id}.')
        return f'Receipt {receipt_id} updated successfully!'
    except Exception as e:
        logger.error(f'💥 Problem in process_receipt_update({receipt_id}): {e}', exc_info=True)
        raise

@shared_task
def process_receipt_delete(receipt_id: int):
    logger.info(f'🗑️ Handling deleted Receipt id={receipt_id}.')
    try:
        trigger_service = celery_task_service
        trigger_service.receipt_trigger_on_delete(receipt_id)
        logger.info(f'✅ Receipt #{receipt_id} deletion handled.')
        return f'Receipt {receipt_id} deletion processed!'
    except Exception as e:
        logger.error(f'💥 Problem in process_receipt_delete({receipt_id}): {e}', exc_info=True)
        raise

@shared_task
def process_spend_money_create(spend_money_id: int):
    logger.info(f'🚀 Starting process_spend_money_create shared task. spend_money_id={spend_money_id}.')
    try:
        trigger_service = celery_task_service
        trigger_service.spend_money_trigger_on_create(spend_money_id)
        logger.info(f'🎉 Done processing newly created SpendMoney #{spend_money_id}.')
        return f'SpendMoney {spend_money_id} created successfully!'
    except Exception as e:
        logger.error(f'💥 Problem in process_spend_money_create({spend_money_id}): {e}', exc_info=True)
        raise

@shared_task
def process_spend_money_update(spend_money_id: int):
    logger.info(f'🔄 Handling updated SpendMoney id={spend_money_id}.')
    try:
        trigger_service = celery_task_service
        trigger_service.spend_money_trigger_on_update(spend_money_id)
        logger.info(f'🎉 Done updating SpendMoney #{spend_money_id}.')
        return f'SpendMoney {spend_money_id} updated successfully!'
    except Exception as e:
        logger.error(f'💥 Problem in process_spend_money_update({spend_money_id}): {e}', exc_info=True)
        raise

@shared_task
def process_spend_money_delete(spend_money_id: int):
    logger.info(f'🗑️ Handling deleted SpendMoney id={spend_money_id}.')
    try:
        trigger_service = celery_task_service
        trigger_service.spend_money_trigger_on_delete(spend_money_id)
        logger.info(f'✅ SpendMoney #{spend_money_id} deletion handled.')
        return f'SpendMoney {spend_money_id} deletion processed!'
    except Exception as e:
        logger.error(f'💥 Problem in process_spend_money_delete({spend_money_id}): {e}', exc_info=True)
        raise

@shared_task
def process_tax_account_create(tax_account_id: int):
    logger.info(f'🚀 Starting process_tax_account_create shared task. tax_account_id={tax_account_id}.')
    try:
        trigger_service = celery_task_service
        trigger_service.tax_account_trigger_on_create(tax_account_id)
        logger.info(f'🎉 Done processing newly created TaxAccount #{tax_account_id}.')
        return f'TaxAccount {tax_account_id} created successfully!'
    except Exception as e:
        logger.error(f'💥 Problem in process_tax_account_create({tax_account_id}): {e}', exc_info=True)
        raise

@shared_task
def process_tax_account_update(tax_account_id: int):
    logger.info(f'🔄 Handling updated TaxAccount id={tax_account_id}.')
    try:
        trigger_service = celery_task_service
        trigger_service.tax_account_trigger_on_update(tax_account_id)
        logger.info(f'🎉 Done updating TaxAccount #{tax_account_id}.')
        return f'TaxAccount {tax_account_id} updated successfully!'
    except Exception as e:
        logger.error(f'💥 Problem in process_tax_account_update({tax_account_id}): {e}', exc_info=True)
        raise

@shared_task
def process_tax_account_delete(tax_account_id: int):
    logger.info(f'🗑️ Handling deleted TaxAccount id={tax_account_id}.')
    try:
        trigger_service = celery_task_service
        trigger_service.tax_account_trigger_on_delete(tax_account_id)
        logger.info(f'✅ TaxAccount #{tax_account_id} deletion handled.')
        return f'TaxAccount {tax_account_id} deletion processed!'
    except Exception as e:
        logger.error(f'💥 Problem in process_tax_account_delete({tax_account_id}): {e}', exc_info=True)
        raise

@shared_task
def process_xero_bill_update(bill_id: int):
    """
    The Celery task for handling updated XeroBills.
    """
    logger.info(f'🚀 Starting update_xero_bill shared task. bill_id={bill_id}.')
    try:
        trigger_service = celery_task_service
        trigger_service.update_xero_bill_trigger(bill_id)
        logger.info(f'🎉 Done with XeroBill #{bill_id}.')
        return f'XeroBill {bill_id} processed successfully!'
    except Exception as e:
        logger.error(f'💥 Problem in update_xero_bill({bill_id}): {e}', exc_info=True)
        raise

@shared_task
def process_xero_bill_create(bill_id: str):
    """
    The Celery task for handling newly created XeroBills.
    """
    logger.info(f'🌀 NEW TASK - CREATE - XERO BILL - STARTED 🌀 {bill_id}.')
    try:
        trigger_service = celery_task_service
        trigger_service.create_xero_bill_trigger(bill_id)
        logger.info(f'🎉 Done with XeroBill creation for bill_id={bill_id}.')
        return f'XeroBill creation for bill_id {bill_id} processed successfully!'
    except Exception as e:
        logger.error(f'💥 Problem in create_xero_bill({bill_id}): {e}', exc_info=True)
        raise

@shared_task
def create_xero_bill_line_items(bill_id: int):
    """
    The Celery task for handling newly inserted line items for a XeroBill.
    """
    logger.info(f'🌀 Handling created line items for bill_id={bill_id}')
    try:
        trigger_service = celery_task_service
        trigger_service.create_xero_bill_line_items_trigger(bill_id)
        logger.info(f'✅ XeroBill line items created for bill_id={bill_id}')
        return 'Success'
    except Exception as e:
        logger.error(f'💥 Problem with create_xero_bill_line_items({bill_id}): {e}', exc_info=True)
        raise

@shared_task
def update_xero_bill_line_item(line_item_id: int):
    """
    The Celery task for handling updated line items for a XeroBill.
    """
    logger.info(f'🌀 Handling updated line item for line_item_id={line_item_id}')
    try:
        trigger_service = celery_task_service
        trigger_service.update_xero_bill_line_item_trigger(line_item_id)
        logger.info(f'✅ XeroBill line item updated for id={line_item_id}')
        return 'Success'
    except Exception as e:
        logger.error(f'💥 Problem with update_xero_bill_line_item({line_item_id}): {e}', exc_info=True)
        raise

@shared_task
def process_xero_bill_delete(bill_id: int):
    """
    The Celery task for handling deleted XeroBills.
    """
    logger.info(f'🗑️ Handling deleted XeroBill bill_id={bill_id}.')
    try:
        trigger_service = celery_task_service
        trigger_service.delete_xero_bill_trigger(bill_id)
        logger.info(f'✅ XeroBill #{bill_id} deletion handled.')
        return f'XeroBill {bill_id} deletion processed!'
    except Exception as e:
        logger.error(f'💥 Problem in delete_xero_bill({bill_id}): {e}', exc_info=True)
        raise

@shared_task
def delete_xero_bill_line_item(line_item_id: int):
    """
    The Celery task for handling deleted line items for a XeroBill.
    """
    logger.info(f'🗑️ Handling deleted line item for line_item_id={line_item_id}')
    try:
        trigger_service = celery_task_service
        trigger_service.delete_xero_bill_line_item_trigger(line_item_id)
        logger.info(f'✅ XeroBill line item #{line_item_id} deletion handled.')
        return 'Success'
    except Exception as e:
        logger.error(f'💥 Problem with delete_xero_bill_line_item({line_item_id}): {e}', exc_info=True)
        raise


@shared_task
def process_po_log_create():
    """
    The Celery task for handling PO Log [NEW] action.
    Executes the logic to process PO log files.
    """
    logger.info('🚀 Starting process_po_log_new shared task.')
    try:
        trigger_service = celery_task_service
        trigger_service.po_log_trigger_on_create()
        logger.info('🎉 Done with PO Log [NEW] task.')
        return 'PO Log [NEW] task completed successfully!'
    except Exception as e:
        logger.error(f'💥 Problem in process_po_log_new(): {e}', exc_info=True)
        raise