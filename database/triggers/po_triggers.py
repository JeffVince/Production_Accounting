# po_triggers.py

import logging
from typing import Optional, Dict, Any

from database.database_util import DatabaseOperations

db_ops = DatabaseOperations()

logger = logging.getLogger("app_logger")


class DatabaseOperationError(Exception):
    """Raised when a database operation fails or returns an unexpected result."""
    pass


# ------------------------------------------------------------------
# Public Handler Functions
# ------------------------------------------------------------------

def handle_project_create(project_id: int) -> None:
    logger.info(f"[PROJECT CREATE] ID={project_id}")
    # ...
    pass


def handle_project_update(project_id: int) -> None:
    logger.info(f"[PROJECT UPDATE] ID={project_id}")
    # ...
    pass


def handle_project_delete(project_id: int) -> None:
    logger.info(f"[PROJECT DELETE] ID={project_id}")
    # ...
    pass


def handle_purchase_order_create(po_id: int) -> None:
    logger.info(f"[PO CREATE] po_id={po_id}")
    # ...
    pass


def handle_purchase_order_update(po_id: int) -> None:
    logger.info(f"[PO UPDATE] po_id={po_id}")
    # ...
    pass


def handle_purchase_order_delete(po_id: int) -> None:
    logger.info(f"[PO DELETE] po_id={po_id}")
    # ...
    pass


def handle_detail_item_create(detail_item_id: int) -> None:
    """
    Triggered when a new DetailItem record is inserted into the DB.
    If the item is in RTP and payment_type=INV, attempts to create/retrieve
    a XeroBill and attach this item as a BillLineItem.
    """
    logger.info(f"[DETAIL ITEM CREATE] detail_item_id={detail_item_id}")
    _detail_item_set_to_rtp(detail_item_id)


def handle_detail_item_update(detail_item_id: int) -> None:
    """
    Triggered when a DetailItem record is updated in the DB.
    If the item transitions to RTP and payment_type=INV, attempts to create/retrieve
    a XeroBill and attach this item as a BillLineItem.
    """
    logger.info(f"[DETAIL ITEM UPDATE] detail_item_id={detail_item_id}")
    _detail_item_set_to_rtp(detail_item_id)


def handle_detail_item_delete(detail_item_id: int) -> None:
    logger.info(f"[DETAIL ITEM DELETE] detail_item_id={detail_item_id}")
    # ...
    pass


# ------------------------------------------------------------------
# Private / Internal Logic
# ------------------------------------------------------------------

def _detail_item_set_to_rtp(detail_item_id: int) -> Optional[Dict[str, Any]]:
    """
    Called when a DetailItem transitions to "RTP" (Ready to Pay).
    If the item & PO meet certain conditions (payment_type=INV),
    create or get a XeroBill in the DB, then create/update a BillLineItem.
    """
    logger.info(
        f"✨ Checking if DetailItem(id={detail_item_id}) is in RTP => Potential Bill creation/update."
    )

    # 1) Retrieve the DetailItem
    try:
        detail_item = db_ops.search_detail_items(["id"], [detail_item_id])
    except Exception as e:
        message = f"DB error searching for DetailItem with id={detail_item_id}: {e}"
        logger.error(message)
        raise DatabaseOperationError(message)

    if not detail_item or isinstance(detail_item, list):
        message = f"Could not find a unique DetailItem with id={detail_item_id}."
        logger.warning(message)
        raise DatabaseOperationError(message)

    item_state = detail_item.get("state", "")
    payment_type = detail_item.get("payment_type", "").upper()
    po_id = detail_item.get("po_id")
    detail_number = detail_item.get("detail_number")

    # Only proceed if detail_item is "RTP" and payment_type="INV"
    if item_state != "RTP":
        logger.debug(
            f"DetailItem(id={detail_item_id}) is state={item_state}, not 'RTP'. Skipping Bill creation."
        )
        return None

    if payment_type not in ("INV"):
        logger.debug(
            f"DetailItem(id={detail_item_id}) has payment_type={payment_type}, not 'INV'. Ignoring for Bill creation."
        )
        return None

    # 2) Retrieve the parent PO
    try:
        po_record = db_ops.search_purchase_orders(["id"], [po_id])
    except Exception as e:
        message = f"DB error searching for PurchaseOrder with id={po_id}: {e}"
        logger.error(message)
        raise DatabaseOperationError(message)

    if not po_record or isinstance(po_record, list):
        message = (
            f"Could not find a unique PurchaseOrder with id={po_id} "
            f"for DetailItem(id={detail_item_id})."
        )
        logger.warning(message)
        raise DatabaseOperationError(message)

    # 3) Build a reference key -> 'projectNumber_PO_Number_detailNumber'
    project_number = _fetch_project_number_for_po(po_record)
    po_number = po_record.get("po_number") or "UNKNOWN"
    if not detail_number:
        logger.warning(f"DetailItem(id={detail_item_id}) has no detail_number. Using 'XX'.")
        detail_number = "XX"

    reference_key = f"{project_number}_{po_number}_{detail_number}"
    logger.info(f"Computed reference_key='{reference_key}' for XeroBill.")

    # 4) Create or get the XeroBill for this reference_key
    xero_bill = _create_or_get_xero_bill(reference_key, po_id)
    if not xero_bill:
        message = (
            f"Failed to create or retrieve a XeroBill for reference='{reference_key}'. "
            f"Cannot attach DetailItem(id={detail_item_id})."
        )
        logger.error(message)
        raise DatabaseOperationError(message)

    xero_bill_id = xero_bill.get("id")
    if not xero_bill_id:
        message = f"XeroBill for reference='{reference_key}' has no 'id'. Unexpected."
        logger.error(message)
        raise DatabaseOperationError(message)

    # 5) Attach the DetailItem as a BillLineItem
    try:
        result = _ensure_bill_line_item(detail_item, xero_bill_id)
        logger.info(
            f"✅ Attached DetailItem(id={detail_item_id}) to XeroBill(id={xero_bill_id})."
        )
        return result
    except Exception as e:
        message = f"Error creating/updating BillLineItem for Bill ID={xero_bill_id}: {e}"
        logger.error(message)
        raise DatabaseOperationError(message)


def _create_or_get_xero_bill(reference_key: str, po_id: int) -> Optional[Dict[str, Any]]:
    """
    Looks up an existing XeroBill by xero_reference_number; if not found,
    creates a new one. No concurrency fallback—if creation fails, we just raise an error.
    """
    # 1) Try to find an existing Bill
    try:
        existing_bills = db_ops.search_xero_bills(["xero_reference_number"], [reference_key])
    except Exception as e:
        message = f"DB error searching for xero_bill with reference='{reference_key}': {e}"
        logger.error(message)
        raise DatabaseOperationError(message)

    if existing_bills:
        logger.debug(
            f"Found existing Bill(s) for reference_key='{reference_key}'. Returning the first."
        )
        return existing_bills[0] if isinstance(existing_bills, list) else existing_bills

    # 2) If no Bill found, create one
    try:
        new_xero_bill = db_ops.create_xero_bill(
            state="Draft",
            xero_reference_number=reference_key,
            po_id=po_id
        )
        if not new_xero_bill:
            raise DatabaseOperationError(
                f"Could not create XeroBill for reference='{reference_key}'."
            )
        logger.info(
            f"Created new XeroBill for reference={reference_key} with id={new_xero_bill.get('id')}"
        )
        return new_xero_bill
    except Exception as e:
        # If creation fails (duplicate or other DB error), just raise
        message = f"Failed to create XeroBill for reference='{reference_key}': {e}"
        logger.error(message)
        raise DatabaseOperationError(message)


def _ensure_bill_line_item(detail_item: Dict[str, Any], xero_bill_id: int) -> Optional[Dict[str, Any]]:
    """
    If there's no BillLineItem linking (xero_bill_id, detail_item_id), create one.
    Otherwise, compare & update if differences are found. No concurrency fallback.
    """
    detail_item_id = detail_item["id"]
    try:
        found = db_ops.search_bill_line_items(
            ["xero_bill_id", "detail_item_id"],
            [xero_bill_id, detail_item_id]
        )
    except Exception as e:
        message = (
            f"DB error searching for BillLineItems with xero_bill_id={xero_bill_id}, "
            f"detail_item_id={detail_item_id}: {e}"
        )
        logger.error(message)
        raise DatabaseOperationError(message)

    description = detail_item.get("description", "No description provided")
    sub_total = detail_item.get("sub_total", 0)
    aicp_code_id = detail_item.get("aicp_code_id")

    account_code = _fetch_account_code(aicp_code_id)

    if found:
        if isinstance(found, list):
            found = found[0]
        logger.info(
            f"BillLineItem already exists for xero_bill_id={xero_bill_id}, detail_item_id={detail_item_id}."
        )

        changes_needed = (
            found.get("description") != description
            or found.get("line_amount") != sub_total
            or found.get("account_code") != account_code
        )
        if changes_needed:
            try:
                updated_line = db_ops.update_bill_line_item(
                    bill_line_item_id=found["id"],
                    description=description,
                    quantity=1,
                    unit_amount=sub_total,
                    line_amount=sub_total,
                    account_code=account_code
                )
                logger.info(
                    f"Updated BillLineItem(id={found['id']}) for xero_bill_id={xero_bill_id}."
                )
                return updated_line
            except Exception as e:
                message = f"Error updating BillLineItem(id={found['id']}): {str(e)}"
                logger.warning(message)
                raise DatabaseOperationError(message)
        else:
            logger.info("No changes needed for existing BillLineItem.")
            return found

    # No existing BillLineItem => create a new one
    logger.info(
        f"Creating BillLineItem in DB for xero_bill_id={xero_bill_id}, detail_item_id={detail_item_id}..."
    )
    try:
        new_line = db_ops.create_bill_line_item(
            xero_bill_id=xero_bill_id,
            detail_item_id=detail_item_id,
            description=description,
            quantity=1,
            unit_amount=sub_total,
            line_amount=sub_total,
            account_code=account_code,
            xero_id=""
        )

        if not new_line:
            message = f"Could not create BillLineItem for xero_bill_id={xero_bill_id}."
            logger.warning(message)
            raise DatabaseOperationError(message)

        logger.info(
            f"BillLineItem created (id={new_line['id']}) for xero_bill_id={xero_bill_id}, detail_item_id={detail_item_id}."
        )
        return new_line

    except Exception as e:
        message = f"Failed to create BillLineItem for xero_bill_id={xero_bill_id}, detail_item_id={detail_item_id}: {e}"
        logger.error(message)
        raise DatabaseOperationError(message)


def _fetch_account_code(aicp_code_id: Optional[int]) -> Optional[str]:
    """
    Utility method to fetch the account_code from an AICP code’s tax account.
    """
    if not aicp_code_id:
        return None

    try:
        aicp_code_record = db_ops.search_aicp_codes(["id"], [aicp_code_id])
    except Exception as e:
        message = f"DB error searching for AICP code with id={aicp_code_id}: {e}"
        logger.error(message)
        raise DatabaseOperationError(message)

    if not aicp_code_record or isinstance(aicp_code_record, list):
        logger.warning(f"AICP code not found or not unique for id={aicp_code_id}.")
        return None

    tax_account_id = aicp_code_record.get("tax_id")
    if not tax_account_id:
        return None

    try:
        tax_account_record = db_ops.search_tax_accounts(["id"], [tax_account_id])
    except Exception as e:
        message = f"DB error searching for tax account with id={tax_account_id}: {e}"
        logger.error(message)
        raise DatabaseOperationError(message)

    if not tax_account_record or isinstance(tax_account_record, list):
        logger.warning(f"Tax account not found or not unique for id={tax_account_id}.")
        return None

    return tax_account_record.get("tax_code")


def _fetch_project_number_for_po(po_record: dict) -> int:
    """
    Retrieve project_number from a PO record, or fallback.
    """
    if "project_number" in po_record and po_record["project_number"]:
        return po_record["project_number"]

    project_id = po_record.get("project_id")
    if not project_id:
        logger.warning("PO record has no project_id. Using 9999 fallback.")
        return 9999

    try:
        found_proj = db_ops.search_projects(["id"], [project_id])
    except Exception as e:
        message = f"DB error searching for Project with id={project_id}: {e}"
        logger.error(message)
        raise DatabaseOperationError(message)

    if found_proj and not isinstance(found_proj, list):
        return found_proj["project_number"]

    logger.warning("Could not determine project_number; using 9999 fallback.")
    return 9999