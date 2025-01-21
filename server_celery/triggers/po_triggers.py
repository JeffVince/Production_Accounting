"""
server_celery/triggers/po_triggers.py

This module holds trigger functions for purchase orders, detail items,
and other entities. These triggers run upon create/read/update/delete events,
dispatching additional logic such as referencing xero bills or performing
downstream actions in the system.
"""
import logging
from typing import Optional, Dict, Any

# region 🔧 Imports
from database.database_util import DatabaseOperations
from files_budget.po_log_service import po_log_service

# endregion

# region 🏗️ Setup
db_ops = DatabaseOperations()
logger = logging.getLogger('database_logger')


class DatabaseOperationError(Exception):
    """
    Raised when a database operation fails or returns an unexpected result.
    """
    pass


# endregion


# region 📝 PO LOG
def handle_po_log_create() -> None:
    """
    Triggered for PO log creation.
    """
    logger.info("[PO LOG CREATE] - Invoking po_log_new_trigger.")
    po_log_service.po_log_new_trigger()


# endregion


# region 🏗️ PROJECT EVENTS
def handle_project_create(project_id: int) -> None:
    """
    Triggered when a new Project record is created.
    """
    logger.info(f"[PROJECT CREATE] - project_id={project_id}")
    # Additional logic can go here.


def handle_project_update(project_id: int) -> None:
    """
    Triggered when an existing Project record is updated.
    """
    logger.info(f"[PROJECT UPDATE] - project_id={project_id}")
    # Additional logic can go here.


def handle_project_delete(project_id: int) -> None:
    """
    Triggered when a Project record is deleted.
    """
    logger.info(f"[PROJECT DELETE] - project_id={project_id}")
    # Additional logic can go here.


# endregion


# region 📝 PURCHASE ORDER EVENTS
def handle_purchase_order_create(po_number: int) -> None:
    """
    Triggered when a new PurchaseOrder record is inserted into the DB.
    The schema uses 'po_number' (plus project_number) as a unique key.
    """
    logger.info(f"[PO CREATE] - po_number={po_number}")
    # Additional logic can go here.


def handle_purchase_order_update(po_number: int) -> None:
    """
    Triggered when a PurchaseOrder is updated.
    """
    logger.info(f"[PO UPDATE] - po_number={po_number}")
    # Additional logic can go here.


def handle_purchase_order_delete(po_number: int) -> None:
    """
    Triggered when a PurchaseOrder is deleted.
    """
    logger.info(f"[PO DELETE] - po_number={po_number}")
    # Additional logic can go here.


# endregion


# region 🧱 DETAIL ITEM EVENTS 🧱
def handle_detail_item_create(detail_item_id: int) -> None:
    """
    Triggered when a new DetailItem record is created.
    Delegates further logic to handle_detail_item_create_logic.
    """
    logger.info(f"[DETAIL ITEM CREATE] - detail_item_id={detail_item_id}")
    handle_detail_item_create_logic(detail_item_id)


def handle_detail_item_update(detail_item_id: int) -> None:
    """
    Triggered when a DetailItem record is updated.
    If the item transitions to RTP or SUBMITTED FOR APPROVAL
    and payment_type in [INV, PROJ], we can fetch the matching XeroBill
    and call update_xero_bill_dates_from_detail_item to adjust date ranges.
    """
    logger.info(f"[DETAIL ITEM UPDATE] - detail_item_id={detail_item_id}")

    # region 🔎 Fetch Detail Item
    try:
        detail_item = db_ops.search_detail_items(['id'], [detail_item_id])
    except Exception as e:
        logger.error(f"DB error searching for DetailItem(id={detail_item_id}): {e}")
        return

    if not detail_item or isinstance(detail_item, list):
        logger.warning(f"Could not find a unique DetailItem with id={detail_item_id}.")
        return
    # endregion

    # region 🤝 Check Payment Type & State
    current_state = (detail_item.get('state') or '').upper()
    payment_type = (detail_item.get('payment_type') or '').upper()

    if current_state in ['RTP', 'SUBMITTED FOR APPROVAL'] and payment_type in ['INV', 'PROJ']:
        # region 🔎 Lookup XeroBill
        project_number = detail_item.get('project_number')
        po_number = detail_item.get('po_number')
        detail_number = detail_item.get('detail_number')

        try:
            xero_bills = db_ops.search_xero_bills(
                ['project_number', 'po_number', 'detail_number'],
                [project_number, po_number, detail_number]
            )
        except Exception as e:
            logger.error(
                f"DB error searching xero_bills for proj={project_number}, po={po_number}, detail={detail_number}: {e}"
            )
            return
        # endregion

        # region 🏷 Update XeroBill Date
        if xero_bills:
            xero_bill = xero_bills[0] if isinstance(xero_bills, list) else xero_bills
            xero_bill_id = xero_bill.get('id')
            if xero_bill_id:
                logger.info(f"Invoking update_xero_bill_dates_from_detail_item for XeroBill(id={xero_bill_id}).")
                po_log_service.update_xero_bill_dates_from_detail_item(xero_bill)
            else:
                logger.warning("Found a XeroBill record but it has no 'id' field.")
        else:
            logger.info(f"No XeroBill found for proj={project_number}, po={po_number}, detail={detail_number}.")
        # endregion
    # endregion


def handle_detail_item_delete(detail_item_id: int) -> None:
    """
    Triggered when a DetailItem record is deleted.
    """
    logger.info(f"[DETAIL ITEM DELETE] - detail_item_id={detail_item_id}")
    # Additional logic can go here.


#region 🧱  Detail Item Helper Functions
def handle_detail_item_create_logic(detail_item_id: int) -> None:
    """
    Triggered when a new DetailItem record is inserted.

    Main logic flow:
      1) If payment_type in [CC, PC]:
         - Compare sub_total to matching Receipt total.
           If they match => state=REVIEWED; else => state=PO MISMATCH.
         - Also update or create SpendMoney (if reviewed => AUTHORIZED, else => DRAFT).
      2) If payment_type in [INV, PROJ]:
         - Sum sub_total for all detail items with (project_number, po_number, detail_number).
         - Compare to the matching Invoice total. If match => set them all to RTP, else => PO MISMATCH.
      3) If detail_item=RTP and payment_type in [INV, PROJ], create or retrieve a XeroBill
         and link the BillLineItem.
      4) Check if item is overdue.
      5) If final state (PAID, RECONCILED, AUTHORIZED, APPROVED), compare amounts to BillLineItem or SpendMoney:
         => If mismatch => state=ISSUE.
      6) Update DB states accordingly.
    """
    logger.info(f"[DETAIL ITEM CREATE] - detail_item_id={detail_item_id}")

    # region 🔎 Fetch Detail Item
    try:
        detail_item = db_ops.search_detail_items(['id'], [detail_item_id])
    except Exception as e:
        logger.error(f"DB error searching for DetailItem(id={detail_item_id}): {e}")
        return

    if not detail_item or isinstance(detail_item, list):
        logger.warning(f"Could not find a unique DetailItem with id={detail_item_id}.")
        return
    # endregion

    # region 📋 Extract Info
    current_state = (detail_item.get('state') or '').upper()
    payment_type = (detail_item.get('payment_type') or '').upper()
    project_number = detail_item.get('project_number')
    po_number = detail_item.get('po_number')
    detail_number = detail_item.get('detail_number')
    line_number = detail_item.get('line_number')
    due_date = detail_item.get('due_date')
    sub_total = float(detail_item.get('sub_total') or 0.0)
    new_state = current_state
    # endregion

    # region 💵 If Payment Type = CC/PC
    if payment_type in ['CC', 'PC']:
        # region 🚚 Check Receipt Total
        receipt_total = None
        try:
            receipts = db_ops.search_receipts(
                ['project_number', 'po_number', 'detail_number', 'line_number'],
                [project_number, po_number, detail_number, line_number]
            )
            if receipts:
                found_receipt = receipts[0] if isinstance(receipts, list) else receipts
                receipt_total = float(found_receipt.get('total', 0.0))
        except Exception as e:
            logger.warning(f"Receipt lookup failed for detail_item_id={detail_item_id}: {e}")

        if receipt_total is not None:
            if abs(sub_total - receipt_total) < 0.0001:
                if new_state not in {"PAID", "RECONCILED", "AUTHORIZED", "APPROVED"}:
                    new_state = "REVIEWED"
            elif new_state not in {"PAID", "RECONCILED", "AUTHORIZED", "APPROVED"}:
                new_state = "PO MISMATCH"
        # endregion

        # region 💰 Check/Update SpendMoney
        try:
            spend_money_recs = db_ops.search_spend_money_by_keys(
                project_number=project_number,
                po_number=po_number,
                detail_number=detail_number,
                line_number=line_number
            )
        except Exception as e:
            logger.warning(f"SpendMoney lookup failed for detail_item_id={detail_item_id}: {e}")
            spend_money_recs = None

        desired_spend_state = "DRAFT"
        if new_state == "REVIEWED":
            desired_spend_state = "AUTHORIZED"

        if spend_money_recs:
            if isinstance(spend_money_recs, list):
                spend_money_recs = spend_money_recs[0]
            spend_money_id = spend_money_recs["id"]
            try:
                db_ops.update_spend_money(spend_money_id, state=desired_spend_state)
                logger.info(f"SpendMoney(id={spend_money_id}) => {desired_spend_state}.")
            except Exception as e:
                logger.warning(f"Failed to update SpendMoney(id={spend_money_id}) => {desired_spend_state}: {e}")
        else:
            try:
                new_spend = db_ops.create_spend_money_by_keys(
                    project_number=project_number,
                    po_number=po_number,
                    detail_number=detail_number,
                    line_number=line_number,
                    state=desired_spend_state
                )
                logger.info(f"Created SpendMoney(id={new_spend.get('id')}) => {desired_spend_state}.")
            except Exception as e:
                logger.warning(f"Failed to create SpendMoney for DetailItem(id={detail_item_id}): {e}")
        # endregion

    # endregion

    # region 💸 If Payment Type = INV/PROJ
    elif payment_type in ['INV', 'PROJ']:
        # region 🌐 Gather same-key detail items
        try:
            detail_items_same_key = db_ops.search_detail_items(
                ['project_number', 'po_number', 'detail_number'],
                [project_number, po_number, detail_number]
            )
        except Exception as e:
            logger.error(
                f"DB error searching detail items for proj={project_number}, "
                f"po={po_number}, detail={detail_number}: {e}"
            )
            detail_items_same_key = None

        total_of_details = 0.0
        if detail_items_same_key and isinstance(detail_items_same_key, list):
            total_of_details = sum((float(di.get('sub_total', 0.0)) for di in detail_items_same_key))
        elif detail_items_same_key and isinstance(detail_items_same_key, dict):
            total_of_details = float(detail_items_same_key.get('sub_total', 0.0))
            detail_items_same_key = [detail_items_same_key]
        # endregion

        # region 📄 Compare Invoice total
        invoice_total = None
        try:
            invoices = db_ops.search_invoices(
                ['project_number', 'po_number', 'invoice_number'],
                [str(project_number), str(po_number), str(detail_number)]
            )
            if invoices:
                found_invoice = invoices[0] if isinstance(invoices, list) else invoices
                invoice_total = float(found_invoice.get('total', 0.0))
        except Exception as e:
            logger.warning(f"Invoice lookup failed for detail_item_id={detail_item_id}: {e}")

        if invoice_total is not None and detail_items_same_key:
            if abs(total_of_details - invoice_total) < 0.0001:
                # region ✅ Mark detail items = RTP
                for di in detail_items_same_key:
                    di_id = di['id']
                    di_state = (di.get('state') or '').upper()
                    if di_state not in {"PAID", "RECONCILED", "AUTHORIZED", "APPROVED"}:
                        try:
                            db_ops.update_detail_item(di_id, state="RTP")
                            logger.info(f"DetailItem(id={di_id}) => RTP (invoice sums matched).")
                            if di_id == detail_item_id:
                                new_state = "RTP"
                        except Exception as e:
                            logger.error(f"Could not update DetailItem(id={di_id}) => RTP: {e}")
                # endregion
            else:
                # region ❌ Mark detail items = PO MISMATCH
                for di in detail_items_same_key:
                    di_id = di['id']
                    di_state = (di.get('state') or '').upper()
                    if di_state not in {"PAID", "RECONCILED", "AUTHORIZED", "APPROVED"}:
                        try:
                            db_ops.update_detail_item(di_id, state="PO MISMATCH")
                            logger.info(f"DetailItem(id={di_id}) => PO MISMATCH (invoice sums mismatch).")
                            if di_id == detail_item_id:
                                new_state = "PO MISMATCH"
                        except Exception as e:
                            logger.error(f"Could not update DetailItem(id={di_id}) => PO MISMATCH: {e}")
                # endregion
        # endregion
    # endregion

    # region ⏳ If RTP => Create/Link Bill
    if new_state == "RTP" and payment_type in ['INV', 'PROJ']:
        logger.info(f"DetailItem(id={detail_item_id}) => RTP => inlining Bill creation logic.")
        reference_key = f"{project_number}_{po_number}_{detail_number or 'XX'}"
        xero_bill = _create_or_get_xero_bill(
            reference_key=reference_key,
            project_number=project_number,
            po_number=po_number,
            detail_number=detail_number
        )
        if xero_bill and xero_bill.get('id'):
            parent_id = xero_bill['id']
            _ensure_bill_line_item(detail_item, parent_id)
        else:
            logger.error(f"Failed to create/retrieve XeroBill for ref_key='{reference_key}'.")
    # endregion

    # region 📆 Check if Overdue
    from datetime import datetime, date
    if new_state in {"REVIEWED", "PO MISMATCH"} and due_date:
        try:
            if isinstance(due_date, str):
                due_dt = datetime.strptime(due_date, '%Y-%m-%d').date()
            elif isinstance(due_date, datetime):
                due_dt = due_date.date()
            elif isinstance(due_date, date):
                due_dt = due_date
            else:
                due_dt = None
            if due_dt and due_dt < date.today():
                new_state = "OVERDUE"
        except Exception as e:
            logger.warning(f"Failed to parse due_date for detail_item_id={detail_item_id}: {e}")
    # endregion

    # region 🏁 Final/Approved -> Check for mismatch
    if new_state in {"PAID", "RECONCILED", "AUTHORIZED", "APPROVED"}:
        if payment_type in ['INV', 'PROJ']:
            # region 🔍 BillLineItem
            try:
                line_items = db_ops.search_bill_line_items(['detail_item_id'], [detail_item_id])
                if line_items:
                    if isinstance(line_items, list):
                        line_items = line_items[0]
                    xero_line_amount = float(line_items.get('line_amount', 0.0))
                    if abs(xero_line_amount - sub_total) >= 0.0001:
                        new_state = "ISSUE"
            except Exception as e:
                logger.warning(f"Could not check BillLineItems for detail_item_id={detail_item_id}: {e}")
            # endregion
        elif payment_type in ['CC', 'PC']:
            # region 💲 SpendMoney
            try:
                sm_recs = db_ops.search_spend_money_by_keys(
                    project_number=project_number,
                    po_number=po_number,
                    detail_number=detail_number,
                    line_number=line_number
                )
                if sm_recs:
                    if isinstance(sm_recs, list):
                        sm_recs = sm_recs[0]
                    spend_amount = float(sm_recs.get('amount', 0.0))
                    if abs(spend_amount - sub_total) >= 0.0001:
                        new_state = "ISSUE"
            except Exception as e:
                logger.warning(f"Could not check SpendMoney for detail_item_id={detail_item_id}: {e}")
            # endregion
    # endregion

    # region 🎨 Update State if Changed
    if new_state != current_state:
        try:
            db_ops.update_detail_item(detail_item_id=detail_item_id, state=new_state)
            logger.info(f"DetailItem(id={detail_item_id}) state changed {current_state} => {new_state}")
        except Exception as e:
            logger.error(f"Failed to update DetailItem(id={detail_item_id}) => {new_state}: {e}")
    else:
        logger.debug(f"No state change for DetailItem(id={detail_item_id}); remains '{new_state}'.")
    # endregion
#endregion

#endregion

#region HELPER FILES

def _create_or_get_xero_bill(reference_key: str, project_number: int, po_number: int, detail_number: int):
    """
    Looks up an existing XeroBill by (project_number, po_number, detail_number).
    If not found, attempts to create a new one with xero_reference_number=reference_key.
    Returns the found or created XeroBill record, or None on error.
    """
    # region 🔎 Search XeroBill
    try:
        existing = db_ops.search_xero_bills(
            ['project_number', 'po_number', 'detail_number'],
            [project_number, po_number, detail_number]
        )
        if existing:
            return existing[0] if isinstance(existing, list) else existing
    except Exception as e:
        logger.error(
            f"DB error searching xero_bills for proj={project_number}, po={po_number}, detail={detail_number}: {e}"
        )
        return None
    # endregion

    # region 🆕 Create XeroBill
    try:
        new_xero_bill = db_ops.create_xero_bill(
            state='Draft',
            xero_reference_number=reference_key,
            project_number=project_number,
            po_number=po_number,
            detail_number=detail_number
        )
        if new_xero_bill:
            logger.info(f"Created XeroBill(id={new_xero_bill.get('id')}) for reference='{reference_key}'")
            return new_xero_bill
        else:
            logger.warning(f"Could not create XeroBill for reference='{reference_key}'.")
            return None
    except Exception as e:
        logger.error(f"Failed to create XeroBill for ref='{reference_key}': {e}")
        return None
    # endregion


def _ensure_bill_line_item(detail_item: dict, parent_id: int):
    """
    If there's a BillLineItem linking (parent_id, line_number), update if changed;
    otherwise create a new BillLineItem.

    This ensures the BillLineItem has the correct project_number, po_number, detail_number,
    line_number, account_code, etc.

    NOTE: We also attempt to pull a numeric tax code from BudgetMap → AccountCode → TaxAccount
          if available, to store in BillLineItem.account_code.
    """
    # region 📋 Basic Info
    line_number = detail_item['line_number']
    description = detail_item.get('description', 'No description provided')
    sub_total = float(detail_item.get('sub_total') or 0.0)
    numeric_tax_code = None
    project_number = detail_item['project_number']
    po_number = detail_item['po_number']
    detail_number = detail_item['detail_number']
    # endregion

    # region 🔎 Possibly fetch numeric tax code via BudgetMap
    try:
        project_records = db_ops.search_projects(['project_number'], [project_number])
        if project_records:
            project = project_records[0] if isinstance(project_records, list) else project_records
            budget_map_id = project.get('budget_map_id')
            logger.info(f"Found project_number={project_number} => budget_map_id={budget_map_id}.")
        else:
            logger.warning(f"Could not find project with project_number={project_number}.")
            budget_map_id = None
    except Exception as e:
        logger.warning(f"Failed to fetch project project_number={project_number}: {e}")
        budget_map_id = None

    if budget_map_id:
        try:
            budget_maps = db_ops.search_budget_maps(['id'], [budget_map_id])
            if budget_maps:
                budget_map_record = budget_maps[0] if isinstance(budget_maps, list) else budget_maps
                budget_map_name = budget_map_record.get('map_name')
                logger.info(
                    f"Found BudgetMap(id={budget_map_id}) => map_name='{budget_map_name}'.")
            else:
                logger.warning(f"No BudgetMap found for ID={budget_map_id}.")
        except Exception as e:
            logger.warning(f"Failed to fetch BudgetMap with ID={budget_map_id}: {e}")

    account_code_id = None
    final_account_code = detail_item['account_code']
    if budget_map_id:
        try:
            logger.info(
                f"Searching AccountCodes for code='{detail_item['account_code']}', budget_map_id={budget_map_id}."
            )
            account_codes = db_ops.search_account_codes(
                ['code', 'budget_map_id'],
                [detail_item['account_code'], budget_map_id]
            )
            if account_codes:
                account_code_rec = account_codes[0] if isinstance(account_codes, list) else account_codes
                account_code_id = account_code_rec.get('id')
                tax_id = account_code_rec.get('tax_id')
                logger.info(
                    f"Found AccountCode(id={account_code_id}). "
                    f"tax_id={tax_id}, code={detail_item['account_code']}"
                )
                if tax_id:
                    try:
                        tax_record = db_ops.search_tax_accounts(['id'], [tax_id])
                        if tax_record and (not isinstance(tax_record, list)):
                            numeric_tax_code = tax_record.get('tax_code')
                            logger.info(
                                f"TaxAccount found for id={tax_id}, numeric_tax_code='{numeric_tax_code}'."
                            )
                        else:
                            logger.info(
                                f"No single TaxAccount found for id={tax_id} (or returned multiple)."
                            )
                    except Exception as e:
                        logger.warning(f"Failed to fetch TaxAccount(id={tax_id}): {e}")
                else:
                    logger.info("tax_id=None; no TaxAccount set for this AccountCode.")
            else:
                logger.warning(
                    f"No AccountCode found for code='{detail_item['account_code']}', "
                    f"budget_map_id={budget_map_id}."
                )
        except Exception as e:
            logger.warning(
                f"Error searching AccountCodes with code='{detail_item['account_code']}', "
                f"budget_map_id={budget_map_id}: {e}"
            )

    if numeric_tax_code:
        final_account_code = numeric_tax_code
    # endregion

    logger.info(
        f"final_account_code={final_account_code} for detail_item.account_code={detail_item['account_code']}")

    # region 🔎 Existing BillLineItem?
    try:
        found = db_ops.search_bill_line_items(['parent_id', 'line_number'], [parent_id, line_number])
    except Exception as e:
        logger.error(f"DB error searching BillLineItem(parent_id={parent_id}, line_number={line_number}): {e}")
        return

    if found:
        if isinstance(found, list):
            found = found[0]
        bill_line_item_id = found['id']
        old_desc = found.get('description')
        old_amount = float(found.get('line_amount', 0.0))
        old_code = found.get('code')
        old_project = found.get('project_number')
        old_po = found.get('po_number')
        old_detail = found.get('detail_number')

        needs_update = (
                old_desc != description
                or abs(old_amount - sub_total) >= 0.0001
                or old_code != final_account_code
                or (old_project != project_number)
                or (old_po != po_number)
                or (old_detail != detail_number)
        )
        if needs_update:
            try:
                updated_line = db_ops.update_bill_line_item(
                    bill_line_item_id=bill_line_item_id,
                    description=description,
                    quantity=1,
                    unit_amount=sub_total,
                    line_amount=sub_total,
                    account_code=final_account_code,
                    project_number=project_number,
                    po_number=po_number,
                    detail_number=detail_number
                )
                logger.info(f"Updated BillLineItem(id={bill_line_item_id}) with new amounts/description/code.")
                return updated_line
            except Exception as e:
                logger.warning(f"Failed to update BillLineItem(id={bill_line_item_id}): {e}")
        else:
            logger.debug(f"No changes needed for existing BillLineItem(id={bill_line_item_id}).")
        return found
    # endregion

    # region 🆕 Create BillLineItem
    try:
        new_line = db_ops.create_bill_line_item(
            parent_id=parent_id,
            project_number=project_number,
            po_number=po_number,
            detail_number=detail_number,
            line_number=line_number,
            description=description,
            quantity=1,
            unit_amount=sub_total,
            line_amount=sub_total,
            account_code=final_account_code
        )
        if new_line:
            logger.info(f"Created BillLineItem(id={new_line.get('id')}) for parent_id={parent_id}.")
        else:
            logger.warning(f"Could not create BillLineItem for parent_id={parent_id}.")
        return new_line
    except Exception as e:
        logger.error(f"Failed to create BillLineItem for parent_id={parent_id}, line_number={line_number}: {e}")
        return None
    # endregion


def _fetch_account_code(account_code_id: Optional[int]):
    """
    If the new schema still supports storing an 'account_code_id' on DetailItem,
    fetch the associated 'tax_code' or similar from AccountCode and TaxAccount.
    Otherwise, this might be extraneous or simplified.
    """
    if not account_code_id:
        return None

    try:
        account_code_record = db_ops.search_account_codes(['id'], [account_code_id])
        if not account_code_record or isinstance(account_code_record, list):
            logger.warning(f"Account code not found or not unique for id={account_code_id}.")
            return None

        tax_account_id = account_code_record.get('tax_id')
        if not tax_account_id:
            return None

        tax_account_record = db_ops.search_tax_accounts(['id'], [tax_account_id])
        if not tax_account_record or isinstance(tax_account_record, list):
            logger.warning(f"Tax account not found or not unique for id={tax_account_id}.")
            return None

        return tax_account_record.get('tax_code')
    except Exception as e:
        logger.error(f"DB error searching for Account code with id={account_code_id}: {e}")
        return None

#endregion