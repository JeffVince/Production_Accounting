# xero_triggers.py

import logging
from typing import Dict, Any

from database.database_util import DatabaseOperations
from xero_api import xero_api

db_ops = DatabaseOperations()
logger = logging.getLogger("database_logger")
logger.setLevel(logging.DEBUG)

FINAL_STATES = {"PAID", "RECONCILED", "AUTHORIZED", "APPROVED"}


def _update_xero_bill_dates(xero_bill: Dict[str, Any]) -> None:
    """
    Helper that checks BillLineItems or DetailItems to find earliest transaction_date
    and latest due_date, then updates XeroBill in the DB accordingly.
    """
    bill_id = xero_bill["id"]
    project_number = xero_bill.get("project_number")
    po_number = xero_bill.get("po_number")
    detail_number = xero_bill.get("detail_number")

    logger.debug(
        f"[_update_xero_bill_dates] Checking earliest/lates dates for XeroBill(id={bill_id}), "
        f"project_number={project_number}, po_number={po_number}, detail_number={detail_number}"
    )

    # Fetch all related DetailItems to figure out earliest transaction date and latest due date
    detail_items = db_ops.search_detail_items(
        ["project_number", "po_number", "detail_number"],
        [project_number, po_number, detail_number]
    )
    if not detail_items:
        logger.debug("[_update_xero_bill_dates] No DetailItems found, skipping date update.")
        return
    if isinstance(detail_items, dict):
        detail_items = [detail_items]

    transaction_dates = []
    due_dates = []
    for di in detail_items:
        if di.get("transaction_date"):
            transaction_dates.append(di["transaction_date"])
        if di.get("due_date"):
            due_dates.append(di["due_date"])

    logger.debug(
        f"[_update_xero_bill_dates] Found {len(detail_items)} DetailItems. "
        f"TransactionDates={transaction_dates}, DueDates={due_dates}"
    )

    if not transaction_dates and not due_dates:
        return

    earliest_trans = min(transaction_dates) if transaction_dates else None
    latest_due = max(due_dates) if due_dates else None

    updated_fields = {}
    if earliest_trans and xero_bill.get("transaction_date") != earliest_trans:
        updated_fields["transaction_date"] = earliest_trans
    if latest_due and xero_bill.get("due_date") != latest_due:
        updated_fields["due_date"] = latest_due

    if updated_fields:
        updated_record = db_ops.update_xero_bill(bill_id, **updated_fields)
        if updated_record:
            logger.info(
                f"[_update_xero_bill_dates] Updated XeroBill(id={bill_id}) with {updated_fields}"
            )
        else:
            logger.warning(
                f"[_update_xero_bill_dates] Failed to update XeroBill(id={bill_id})."
            )


# region 🏦 SpendMoney Triggers
def handle_spend_money_create(spend_money_id: int) -> None:
    """
    Trigger: Called when a new SpendMoney record is created in the DB.
    Add your logic to push to Xero if needed.
    """
    logger.info(f"[SPEND MONEY CREATE] ID={spend_money_id}")
    # Example: xero_api.create_spend_money(...)
    pass


def handle_spend_money_update(spend_money_id: int) -> None:
    """
    Trigger: Called when a SpendMoney record is updated in the DB.
    """
    logger.info(f"[SPEND MONEY UPDATE] ID={spend_money_id}")
    # Example: xero_api.update_spend_money(...)
    pass


def handle_spend_money_delete(spend_money_id: int) -> None:
    """
    Trigger: Called when a SpendMoney record is deleted in the DB.
    Possibly mark as voided or remove from Xero if not final.
    """
    logger.info(f"[SPEND MONEY DELETE] ID={spend_money_id}")
    pass
# endregion


# region 🏷 XeroBill Triggers
def handle_xero_bill_create(bill_id: int) -> None:
    """
    Trigger: Called when a XeroBill record is created in the DB.

    Steps:
      1) Fetch the local XeroBill record
      2) Check if xero_id is present
         - If not, search Xero by the reference number
           -> If none found, create new in Xero (ACCPAY) and update local Bill with xero_id, xero_link
           -> If found:
               if final => pull data from Xero into DB if different
               else => upsert
      3) If xero_id is present but no xero_link, fetch from Xero and update local
      4) Check BillLineItems / DetailItems => update earliest trans date & latest due date
    """
    logger.info(f"[BILL CREATE] XeroBill(id={bill_id}) triggered.")
    xero_bill = db_ops.search_xero_bills(["id"], [bill_id])
    if not xero_bill:
        logger.warning(f"handle_xero_bill_create: XeroBill(id={bill_id}) not found in DB.")
        return
    if isinstance(xero_bill, list):
        xero_bill = xero_bill[0]

    xero_id = xero_bill.get("xero_id")
    xero_link = xero_bill.get("xero_link")
    reference = xero_bill.get("xero_reference_number")

    # 1) If no xero_id, see if there's an existing Bill in Xero by reference
    if not xero_id:
        logger.info(
            f"[BILL CREATE] No xero_id. Searching Xero by reference='{reference}'."
        )
        existing_invs = xero_api.get_bills_by_reference(reference) or []
        matched = [inv for inv in existing_invs if inv.get("Reference") == reference]
        if not matched:
            # Create a new ACCPAY invoice in Xero
            logger.info(f"[BILL CREATE] No existing invoice found. Creating a new one in Xero.")
            created_invoices = xero_api._retry_on_unauthorized(
                xero_api.xero.invoices.put,
                [{"Type": "ACCPAY", "Reference": reference}]
            )
            if not created_invoices:
                logger.error("[BILL CREATE] Failed to create new invoice in Xero.")
                return
            try:
                new_inv = created_invoices[0]["Invoices"][0]
                new_xero_id = new_inv.get("InvoiceID")
                link = f"https://go.xero.com/AccountsPayable/View.aspx?invoiceId={new_xero_id}"
                db_ops.update_xero_bill(bill_id, xero_id=new_xero_id, xero_link=link)
                logger.info(
                    f"[BILL CREATE] Created new invoice in Xero. Bill(id={bill_id}) => xero_id={new_xero_id}."
                )
                # Optionally upsert line items here
            except Exception as e:
                logger.error(f"[BILL CREATE] Error parsing invoice create response: {e}")
        else:
            # Found an existing invoice
            existing_inv = matched[0]
            status = existing_inv.get("Status", "").upper()
            if status in FINAL_STATES:
                logger.info("[BILL CREATE] Found final-state invoice in Xero => pull data if needed.")
                # Compare fields, pull data down
            else:
                logger.info("[BILL CREATE] Found not-final invoice in Xero => upsert changes if needed.")
                # Upsert logic

    else:
        # We have a xero_id
        logger.info(f"[BILL CREATE] Bill already has xero_id={xero_id}, checking link.")
        if not xero_link:
            logger.info("[BILL CREATE] No xero_link => building link from Xero data if possible.")
            invoice_obj = xero_api.get_invoice_details(xero_id)
            if invoice_obj:
                link = f"https://go.xero.com/AccountsPayable/View.aspx?invoiceId={xero_id}"
                db_ops.update_xero_bill(bill_id, xero_link=link)
                logger.info(
                    f"[BILL CREATE] Stored xero_link='{link}' for Bill(id={bill_id})."
                )
            else:
                logger.warning(
                    f"[BILL CREATE] Could not fetch Xero invoice with ID={xero_id}, no link stored."
                )

    # 2) Update earliest/lates dates from DetailItems
    _update_xero_bill_dates(xero_bill)


def handle_xero_bill_update(bill_id: int) -> None:
    """
    Trigger: Called when a XeroBill record is updated in the DB.

    Steps:
      1) Fetch local XeroBill
      2) If no xero_id, try to find an existing invoice in Xero by reference or create new
      3) If final => pull data, else upsert
      4) If xero_id but missing link => build link
      5) Update earliest/lates date from BillLineItems / DetailItems
    """
    logger.info(f"[BILL UPDATE] XeroBill(id={bill_id}) triggered.")
    xero_bill = db_ops.search_xero_bills(["id"], [bill_id])
    if not xero_bill:
        logger.warning(f"[BILL UPDATE] XeroBill(id={bill_id}) not found in DB.")
        return
    if isinstance(xero_bill, list):
        xero_bill = xero_bill[0]

    xero_id = xero_bill.get("xero_id")
    xero_link = xero_bill.get("xero_link")
    reference = xero_bill.get("xero_reference_number")

    # 1) If no xero_id, see if there's an existing Bill in Xero
    if not xero_id:
        logger.info(f"[BILL UPDATE] No xero_id. Checking Xero by reference='{reference}'...")
        existing_invs = xero_api.get_bills_by_reference(reference) or []
        matched = [inv for inv in existing_invs if inv.get("Reference") == reference]
        if not matched:
            # Create new
            logger.info("[BILL UPDATE] No existing invoice => creating new in Xero.")
            created_invoices = xero_api._retry_on_unauthorized(
                xero_api.xero.invoices.put,
                [{"Type": "ACCPAY", "Reference": reference}]
            )
            if not created_invoices:
                logger.error("[BILL UPDATE] Failed to create invoice in Xero.")
                return
            try:
                new_inv = created_invoices[0]["Invoices"][0]
                new_xero_id = new_inv.get("InvoiceID")
                link = f"https://go.xero.com/AccountsPayable/View.aspx?invoiceId={new_xero_id}"
                db_ops.update_xero_bill(bill_id, xero_id=new_xero_id, xero_link=link)
                logger.info(f"[BILL UPDATE] Created new Xero invoice => xero_id={new_xero_id}.")
            except Exception as e:
                logger.error(f"[BILL UPDATE] Error parsing newly created invoice: {e}")
        else:
            existing_inv = matched[0]
            status = existing_inv.get("Status", "").upper()
            if status in FINAL_STATES:
                logger.info("[BILL UPDATE] Found final-state invoice => pull data if needed.")
                # Pull logic
            else:
                logger.info("[BILL UPDATE] Found not-final invoice => upsert logic.")
                # Upsert logic

    else:
        # We do have a xero_id, ensure xero_link
        logger.info(f"[BILL UPDATE] Bill already has xero_id={xero_id}, checking xero_link.")
        if not xero_link:
            invoice_data = xero_api.get_invoice_details(xero_id)
            if invoice_data:
                link = f"https://go.xero.com/AccountsPayable/View.aspx?invoiceId={xero_id}"
                db_ops.update_xero_bill(bill_id, xero_link=link)
                logger.info(f"[BILL UPDATE] Stored xero_link='{link}' in Bill(id={bill_id}).")
            else:
                logger.warning(
                    f"[BILL UPDATE] Could not fetch Xero invoice with xero_id={xero_id} to build link."
                )

    # 2) Update earliest/ latest dates
    _update_xero_bill_dates(xero_bill)

    logger.info(f"[BILL UPDATE] Completed for bill_id={bill_id}")


def handle_xero_bill_delete(bill_id: int) -> None:
    """
    Trigger: Called when a XeroBill is deleted in the DB.
    Possibly you might want to set it as voided in Xero if not final.
    """
    logger.info(f"[BILL DELETE] XeroBill(id={bill_id}) triggered.")
    # If you want to do something in Xero, like void the invoice, do it here.
    pass
# endregion


# region 📄 BillLineItem Triggers
def handle_xero_bill_line_item_create(bill_line_item_id: int) -> None:
    """
    Trigger: Called when a BillLineItem is created in the DB.
    Insert your "push to Xero" logic if you want to create or update the line item in Xero.
    """
    logger.info(f"[BILL LINE ITEM CREATE] ID={bill_line_item_id}")
    pass


def handle_xero_bill_line_item_update(bill_line_item_id: int) -> None:
    """
    Trigger: Called when a BillLineItem is updated in the DB.
    Insert logic to upsert or fix in Xero if not final.
    """
    logger.info(f"[BILL LINE ITEM UPDATE] ID={bill_line_item_id}")
    pass


def handle_xero_bill_line_item_delete(bill_line_item_id: int) -> None:
    """
    Trigger: Called when a BillLineItem is deleted in the DB.
    Possibly remove or void in Xero if not final.
    """
    logger.info(f"[BILL LINE ITEM DELETE] ID={bill_line_item_id}")
    pass
# endregion