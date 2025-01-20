import logging
from utilities.singleton import SingletonMeta
from server_celery.triggers.xero_triggers import handle_spend_money_create, handle_spend_money_update, handle_spend_money_delete, handle_xero_bill_create, handle_xero_bill_update, handle_xero_bill_delete, handle_xero_bill_line_item_create, handle_xero_bill_line_item_update, handle_xero_bill_line_item_delete
from server_celery.triggers.po_triggers import handle_project_create, handle_project_update, handle_project_delete, \
    handle_purchase_order_create, handle_purchase_order_update, handle_purchase_order_delete, handle_detail_item_create, \
    handle_detail_item_update, handle_detail_item_delete, handle_po_log_create
from server_celery.triggers.invoice_receipt_triggers import handle_invoice_create_or_update, handle_invoice_delete, handle_receipt_create, handle_receipt_update, handle_receipt_delete
from server_celery.triggers.contact_triggers import handle_contact_create, handle_contact_update, handle_contact_delete, handle_tax_account_create, handle_tax_account_update, handle_tax_account_delete, handle_account_code_create, handle_account_code_update, handle_account_code_delete

class CeleryTaskService(metaclass=SingletonMeta):
    """
    CeleryTaskService
    ======================
    Delegates all triggered tasks to the appropriate "trigger" modules.
    """

    def __init__(self):
        self.logger = logging.getLogger('admin_logger')

    def po_log_trigger_on_create(self):
        return handle_po_log_create()

    def bill_line_item_trigger_on_create(self, bill_line_item_id: int):
        return handle_xero_bill_line_item_create(bill_line_item_id)

    def bill_line_item_trigger_on_update(self, bill_line_item_id: int):
        return handle_xero_bill_line_item_update(bill_line_item_id)

    def bill_line_item_trigger_on_delete(self, bill_line_item_id: int):
        return handle_xero_bill_line_item_delete(bill_line_item_id)

    def spend_money_trigger_on_create(self, spend_money_id: int):
        return handle_spend_money_create(spend_money_id)

    def spend_money_trigger_on_update(self, spend_money_id: int):
        return handle_spend_money_update(spend_money_id)

    def spend_money_trigger_on_delete(self, spend_money_id: int):
        return handle_spend_money_delete(spend_money_id)

    def create_xero_bill_trigger(self, bill_id: int):
        return handle_xero_bill_create(bill_id)

    def update_xero_bill_trigger(self, bill_id: int):
        return handle_xero_bill_update(bill_id)

    def delete_xero_bill_trigger(self, bill_id: int):
        return handle_xero_bill_delete(bill_id)

    def create_xero_bill_line_items_trigger(self, bill_id: int):
        return handle_xero_bill_line_item_create(bill_id)

    def update_xero_bill_line_item_trigger(self, line_item_id: int):
        return handle_xero_bill_line_item_update(line_item_id)

    def delete_xero_bill_line_item_trigger(self, line_item_id: int):
        return handle_xero_bill_line_item_delete(line_item_id)

    def project_trigger_on_create(self, project_id: int):
        return handle_project_create(project_id)

    def project_trigger_on_update(self, project_id: int):
        return handle_project_update(project_id)

    def project_trigger_on_delete(self, project_id: int):
        return handle_project_delete(project_id)

    def purchase_order_trigger_on_create(self, po_id: int):
        return handle_purchase_order_create(po_id)

    def purchase_order_trigger_on_update(self, po_id: int):
        return handle_purchase_order_update(po_id)

    def purchase_order_trigger_on_delete(self, po_id: int):
        return handle_purchase_order_delete(po_id)

    def detail_item_trigger_on_create(self, detail_item_id: int):
        return handle_detail_item_create(detail_item_id)

    def detail_item_trigger_on_update(self, detail_item_id: int):
        return handle_detail_item_update(detail_item_id)

    def detail_item_on_delete(self, detail_item_id: int):
        return handle_detail_item_delete(detail_item_id)

    def invoice_trigger_on_create_or_update(self, invoice_id: int):
        return handle_invoice_create_or_update(invoice_id)

    def invoice_trigger_on_delete(self, invoice_id: int):
        return handle_invoice_delete(invoice_id)

    def receipt_trigger_on_create(self, receipt_id: int):
        return handle_receipt_create(receipt_id)

    def receipt_trigger_on_update(self, receipt_id: int):
        return handle_receipt_update(receipt_id)

    def receipt_trigger_on_delete(self, receipt_id: int):
        return handle_receipt_delete(receipt_id)

    def contact_trigger_on_create(self, contact_id: int):
        return handle_contact_create(contact_id)

    def contact_trigger_on_update(self, contact_id: int):
        return handle_contact_update(contact_id)

    def contact_trigger_on_delete(self, contact_id: int):
        return handle_contact_delete(contact_id)

    def tax_account_trigger_on_create(self, tax_account_id: int):
        return handle_tax_account_create(tax_account_id)

    def tax_account_trigger_on_update(self, tax_account_id: int):
        return handle_tax_account_update(tax_account_id)

    def tax_account_trigger_on_delete(self, tax_account_id: int):
        return handle_tax_account_delete(tax_account_id)

    def account_code_trigger_on_create(self, account_code_id: int):
        return handle_account_code_create(account_code_id)

    def account_code_trigger_on_update(self, account_code_id: int):
        return handle_account_code_update(account_code_id)

    def account_code_trigger_on_delete(self, account_code_id: int):
        return handle_account_code_delete(account_code_id)

celery_task_service = CeleryTaskService()