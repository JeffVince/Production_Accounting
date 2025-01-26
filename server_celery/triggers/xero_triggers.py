"""
xero_triggers.py

Handles XeroBill, XeroBillLineItem events. Integrates aggregator checks from budget_service.
"""
import logging
from database.database_util import DatabaseOperations
from files_budget.budget_service import budget_service
from files_xero.xero_services import xero_services

logger = logging.getLogger('xero_triggers')
db_ops = DatabaseOperations()

# region 🧾 Xero Bill

def handle_xero_bill_create(bill_id: int) -> None:
    """
    Called when a new XeroBill record is created locally, so let's reflect it in Xero.
    We also do aggregator checks, then call xero_services to do the rest.
    """
    logger.info(f'[handle_xero_bill_create] => BillID={bill_id}')

    xero_bill = db_ops.search_xero_bills(['id'], [bill_id])
    if not xero_bill:
        logger.warning(f"No XeroBill found for ID={bill_id}.")
        return
    if isinstance(xero_bill, list):
        xero_bill = xero_bill[0]

    # region ## AGGREGATOR CHECK ##
    if budget_service.is_aggregator_in_progress(xero_bill):
        logger.info("[XERO BILL CREATE] aggregator=STARTED => partial skip.")
        return
    # endregion

    # region 🏁 Normal creation logic
    xero_services.create_xero_bill_in_xero(xero_bill)
    # Possibly set line items: xero_services.set_xero_id_on_line_items(xero_bill)
    # endregion

    logger.info('[handle_xero_bill_create] => Done.')

def handle_xero_bill_update(bill_id: int) -> None:
    """
    Called when an existing XeroBill record is updated locally.
    Checks aggregator, then calls xero_services.update_xero_bill().
    """
    logger.info(f'[handle_xero_bill_update] => BillID={bill_id}')

    xero_bill = db_ops.search_xero_bills(['id'], [bill_id])
    if not xero_bill:
        logger.warning(f"No XeroBill found for ID={bill_id}.")
        return
    if isinstance(xero_bill, list):
        xero_bill = xero_bill[0]

    # region ## AGGREGATOR CHECK ##
    if budget_service.is_aggregator_in_progress(xero_bill):
        logger.info("[XERO BILL UPDATE] aggregator=STARTED => partial skip.")
        return
    # endregion

    # region 🏁 Normal update logic
    xero_services.update_xero_bill(bill_id)
    # endregion

    logger.info('[handle_xero_bill_update] => Done.')

def handle_xero_bill_delete(bill_id: int) -> None:
    """
    Called when a XeroBill record is deleted locally => set the Xero invoice status=DELETED if possible.
    """
    logger.info(f'[handle_xero_bill_delete] => BillID={bill_id}')
    # aggregator might not matter for a delete, but you can add a check if needed
    xero_services.delete_xero_bill(bill_id)
    logger.info('[handle_xero_bill_delete] => Done.')
# endregion

# region 💼 Bill Line Item

def handle_xero_xero_bill_line_item_create(xero_bill_line_item_id: int) -> None:
    """
    Triggered when a XeroBillLineItem is created. 
    We'll do aggregator check, then call xero_services for partial or final logic.
    """
    logger.info(f'[handle_xero_xero_bill_line_item_create] => ID={xero_bill_line_item_id}')

    line_item = db_ops.search_xero_bill_line_items(['id'], [xero_bill_line_item_id])
    if not line_item:
        logger.warning(f"No XeroBillLineItem found for ID={xero_bill_line_item_id}.")
        return
    if isinstance(line_item, list):
        line_item = line_item[0]

    # region ## AGGREGATOR CHECK ##
    if budget_service.is_aggregator_in_progress(line_item):
        logger.info("[BILL LINE ITEM CREATE] aggregator=STARTED => partial skip.")
        return
    # endregion

    # region 🏁 Normal logic
    xero_services.create_xero_bill_line_item_in_xero(xero_bill_line_item_id)
    # Possibly update parent's date range, etc.
    # endregion

    logger.info('[handle_xero_xero_bill_line_item_create] => Done.')


def handle_xero_xero_bill_line_item_update(xero_bill_line_item_id: int) -> None:
    """
    Triggered when XeroBillLineItem is updated.
    Checks aggregator, then calls xero_services for partial or final logic.
    """
    logger.info(f'[handle_xero_xero_bill_line_item_update] => ID={xero_bill_line_item_id}')
    line_item = db_ops.search_xero_bill_line_items(['id'], [xero_bill_line_item_id])
    if not line_item:
        logger.warning(f"No XeroBillLineItem found for ID={xero_bill_line_item_id}.")
        return
    if isinstance(line_item, list):
        line_item = line_item[0]

    # region ## AGGREGATOR CHECK ##
    if budget_service.is_aggregator_in_progress(line_item):
        logger.info("[BILL LINE ITEM UPDATE] aggregator=STARTED => partial skip.")
        return
    # endregion

    # region 🏁 Normal update logic
    xero_services.update_xero_bill_line_item_in_xero(line_item)
    logger.info('[handle_xero_xero_bill_line_item_update] => Done.')
# endregion


def handle_spend_money_create(spend_money_id: int) -> None:
    return None


def handle_spend_money_update(spend_money_id: int) -> None:
    return None


def handle_spend_money_delete(spend_money_id: int) -> None:
    return None


def handle_xero_xero_bill_line_item_delete(spend_money_id: int) -> None:
    return None