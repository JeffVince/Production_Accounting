import logging
from database.database_util import DatabaseOperations
from files_xero.xero_services import xero_services  # from your new xero_services file

logger = logging.getLogger('admin_logger')
logger.setLevel(logging.DEBUG)



def handle_spend_money_create(spend_money_id: int) -> None:
    """
    Minimal. If you want additional logic, you can call a service method here.
    """
    logger.info(f'[handle_spend_money_create] [SpendMoney - {spend_money_id}] 💰 - Triggered.')
    # xero_services.something_spend_money(spend_money_id)
    # or do nothing

def handle_spend_money_update(spend_money_id: int) -> None:
    logger.info(f'[handle_spend_money_update] [SpendMoney - {spend_money_id}] 🔄 - Triggered.')
    # xero_services.update_spend_money(spend_money_id)

def handle_spend_money_delete(spend_money_id: int) -> None:
    logger.info(f'[handle_spend_money_delete] [SpendMoney - {spend_money_id}] ❌ - Triggered.')
    # xero_services.delete_spend_money(spend_money_id)



def handle_xero_bill_create(bill_id: int) -> None:
    """
    Called when a new XeroBill record is created locally, so let's reflect it in Xero.
    """
    logger.info(f'[handle_xero_bill_create] [XeroBill - {bill_id}] 🚀 - Triggered.')
    xero_services.create_xero_bill_in_xero(bill_id)
    return

def handle_xero_bill_update(bill_id: int) -> None:
    """
    Called when an existing XeroBill record is updated locally.
    """
    logger.info(f'[handle_xero_bill_update] [XeroBill - {bill_id}] 🔄 - Triggered.')
    xero_services.update_xero_bill(bill_id)
    return

def handle_xero_bill_delete(bill_id: int) -> None:
    """
    Called when a XeroBill record is deleted locally.
    """
    logger.info(f'[handle_xero_bill_delete] [XeroBill - {bill_id}] ❌ - Triggered.')
    xero_services.delete_xero_bill(bill_id)
    return



def handle_xero_bill_line_item_create(bill_line_item_id: int) -> None:
    logger.info(f'[handle_xero_bill_line_item_create] [LineItem - {bill_line_item_id}] 🚀 - Triggered.')
    # Possibly xero_services.create_line_item_in_xero(bill_line_item_id)

def handle_xero_bill_line_item_update(bill_line_item_id: int) -> None:
    logger.info(f'[handle_xero_bill_line_item_update] [LineItem - {bill_line_item_id}] 🔄 - Triggered.')
    # Possibly xero_services.update_line_item_in_xero(bill_line_item_id)

def handle_xero_bill_line_item_delete(bill_line_item_id: int) -> None:
    logger.info(f'[handle_xero_bill_line_item_delete] [LineItem - {bill_line_item_id}] ❌ - Triggered.')
    # Possibly xero_services.delete_line_item_in_xero(bill_line_item_id)
