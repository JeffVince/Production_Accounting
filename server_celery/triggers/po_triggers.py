import logging
from typing import Optional, Dict, Any
from database.database_util import DatabaseOperations
from files_budget.po_log_service import po_log_service
db_ops = DatabaseOperations()
logger = logging.getLogger('database_logger')

class DatabaseOperationError(Exception):
    """Raised when a database operation fails or returns an unexpected result."""
    pass

def handle_po_log_create() -> None:
    logger.info(f'[PO LOG CREATE]')
    po_log_service.po_log_new_trigger()

def handle_project_create(project_id: int) -> None:
    logger.info(f'[PROJECT CREATE] ID={project_id}')
    pass

def handle_project_update(project_id: int) -> None:
    logger.info(f'[PROJECT UPDATE] ID={project_id}')
    pass

def handle_project_delete(project_id: int) -> None:
    logger.info(f'[PROJECT DELETE] ID={project_id}')
    pass

def handle_purchase_order_create(po_number: int) -> None:
    """
    Triggered when a new PurchaseOrder record is inserted into the DB.
    Note: The schema no longer references an internal ID for POs;
          assume 'po_number' is our unique key (plus project_number).
    """
    logger.info(f'[PO CREATE] po_number={po_number}')
    pass

def handle_purchase_order_update(po_number: int) -> None:
    logger.info(f'[PO UPDATE] po_number={po_number}')
    pass

def handle_purchase_order_delete(po_number: int) -> None:
    logger.info(f'[PO DELETE] po_number={po_number}')
    pass

def handle_detail_item_create(detail_item_id: int) -> None:
    """
    Triggered when a new DetailItem record is inserted into the DB.
    Delegates to handle_detail_item_create_logic.
    """
    logger.info(f'[DETAIL ITEM CREATE] detail_item_id={detail_item_id}')
    handle_detail_item_create_logic(detail_item_id)

def handle_detail_item_update(detail_item_id: int) -> None:
    """
    Triggered when a DetailItem record is updated in the DB.
    If the item transitions to RTP and payment_type=INV (or PROJ),
    we may need to create/retrieve a XeroBill and attach this item.
    """
    logger.info(f'[DETAIL ITEM UPDATE] detail_item_id={detail_item_id}')

def handle_detail_item_delete(detail_item_id: int) -> None:
    logger.info(f'[DETAIL ITEM DELETE] detail_item_id={detail_item_id}')
    pass

def handle_detail_item_create_logic(detail_item_id: int) -> None:
    """
    Triggered when a new DetailItem record is inserted into the DB.

    Updated Logic (Streamlined) with the new schema:
      1) If payment_type in [CC, PC]:
         - Compare DetailItem.sub_total to the matching Receipt total (single detail).
           If match => DetailItem=REVIEWED, else => PO MISMATCH.
         - Handle SpendMoney creation/update accordingly (AUTHORIZED if detail=REVIEWED, else DRAFT).
      2) If payment_type in [INV, PROJ]:
         - Sum sub_total for all DetailItems with (project_number, po_number, detail_number).
         - Compare to the matching Invoice total for (project_number, po_number, invoice_number=detail_number).
           If match => all those items => RTP, else => PO MISMATCH.
      3) If an item is set to RTP and payment_type in [INV, PROJ], handle Bill creation logic:
         - We do not have a po_id in XeroBill or DetailItem; we reference
           (project_number, po_number, detail_number) to link them.
      4) If item ends up in {REVIEWED, PO MISMATCH}, check if overdue => OVERDUE.
      5) If item is final (PAID, RECONCILED, AUTHORIZED, APPROVED), compare final amounts:
         - For INV/PROJ => BillLineItem vs. sub_total
         - For CC/PC => SpendMoney vs. sub_total
         => If mismatch => ISSUE
      6) Update DB states at the end as needed.
    """
    logger.info(f'[DETAIL ITEM CREATE] detail_item_id={detail_item_id}')
    try:
        detail_item = db_ops.search_detail_items(['id'], [detail_item_id])
    except Exception as e:
        logger.error(f'DB error searching for DetailItem(id={detail_item_id}): {e}')
        return
    if not detail_item or isinstance(detail_item, list):
        logger.warning(f'Could not find a unique DetailItem with id={detail_item_id}.')
        return
    current_state = (detail_item.get('state') or '').upper()
    payment_type = (detail_item.get('payment_type') or '').upper()
    project_number = detail_item.get('project_number')
    po_number = detail_item.get('po_number')
    detail_number = detail_item.get('detail_number')
    line_number = detail_item.get('line_number')
    due_date = detail_item.get('due_date')
    sub_total = float(detail_item.get('sub_total') or 0.0)
    new_state = current_state
    if payment_type in ['CC', 'PC']:
        receipt_total = None
        try:
            receipts = db_ops.search_receipts(['project_number', 'po_number', 'detail_number', 'line_number'], [project_number, po_number, detail_number, line_number])
            if receipts:
                found_receipt = receipts[0] if isinstance(receipts, list) else receipts
                receipt_total = float(found_receipt.get('total', 0.0))
        except Exception as e:
            logger.warning(f'Receipt lookup failed for detail_item_id={detail_item_id}: {e}')
        if receipt_total is not None:
            if abs(sub_total - receipt_total) < 0.0001:
                if new_state not in {'PAID', 'RECONCILED', 'AUTHORIZED', 'APPROVED'}:
                    new_state = 'REVIEWED'
            elif new_state not in {'PAID', 'RECONCILED', 'AUTHORIZED', 'APPROVED'}:
                new_state = 'PO MISMATCH'
        try:
            spend_money_recs = db_ops.search_spend_money_by_keys(project_number=project_number, po_number=po_number, detail_number=detail_number, line_number=line_number)
        except Exception as e:
            logger.warning(f'SpendMoney lookup failed for detail_item_id={detail_item_id}: {e}')
            spend_money_recs = None
        desired_spend_state = 'DRAFT'
        if new_state == 'REVIEWED':
            desired_spend_state = 'AUTHORIZED'
        if spend_money_recs:
            if isinstance(spend_money_recs, list):
                spend_money_recs = spend_money_recs[0]
            spend_money_id = spend_money_recs['id']
            try:
                db_ops.update_spend_money(spend_money_id, state=desired_spend_state)
                logger.info(f'SpendMoney(id={spend_money_id}) => {desired_spend_state}.')
            except Exception as e:
                logger.warning(f'Failed to update SpendMoney(id={spend_money_id}) => {desired_spend_state}: {e}')
        else:
            try:
                new_spend = db_ops.create_spend_money_by_keys(project_number=project_number, po_number=po_number, detail_number=detail_number, line_number=line_number, state=desired_spend_state)
                logger.info(f"Created SpendMoney(id={new_spend.get('id')}) => {desired_spend_state}.")
            except Exception as e:
                logger.warning(f'Failed to create SpendMoney for DetailItem(id={detail_item_id}): {e}')
    elif payment_type in ['INV', 'PROJ']:
        try:
            detail_items_same_key = db_ops.search_detail_items(['project_number', 'po_number', 'detail_number'], [project_number, po_number, detail_number])
        except Exception as e:
            logger.error(f'DB error searching detail items for (proj={project_number}, po={po_number}, detail={detail_number}): {e}')
            detail_items_same_key = None
        total_of_details = 0.0
        if detail_items_same_key and isinstance(detail_items_same_key, list):
            total_of_details = sum((float(di.get('sub_total', 0.0)) for di in detail_items_same_key))
        elif detail_items_same_key and isinstance(detail_items_same_key, dict):
            total_of_details = float(detail_items_same_key.get('sub_total', 0.0))
            detail_items_same_key = [detail_items_same_key]
        invoice_total = None
        try:
            invoices = db_ops.search_invoices(['project_number', 'po_number', 'invoice_number'], [str(project_number), str(po_number), str(detail_number)])
            if invoices:
                found_invoice = invoices[0] if isinstance(invoices, list) else invoices
                invoice_total = float(found_invoice.get('total', 0.0))
        except Exception as e:
            logger.warning(f'Invoice lookup failed for detail_item_id={detail_item_id}: {e}')
        if invoice_total is not None and detail_items_same_key:
            if abs(total_of_details - invoice_total) < 0.0001:
                for di in detail_items_same_key:
                    di_id = di['id']
                    di_state = (di.get('state') or '').upper()
                    if di_state not in {'PAID', 'RECONCILED', 'AUTHORIZED', 'APPROVED'}:
                        try:
                            db_ops.update_detail_item(di_id, state='RTP')
                            logger.info(f'DetailItem(id={di_id}) => RTP (invoice sums matched).')
                            if di_id == detail_item_id:
                                new_state = 'RTP'
                        except Exception as e:
                            logger.error(f'Could not update DetailItem(id={di_id}) => RTP: {e}')
            else:
                for di in detail_items_same_key:
                    di_id = di['id']
                    di_state = (di.get('state') or '').upper()
                    if di_state not in {'PAID', 'RECONCILED', 'AUTHORIZED', 'APPROVED'}:
                        try:
                            db_ops.update_detail_item(di_id, state='PO MISMATCH')
                            logger.info(f'DetailItem(id={di_id}) => PO MISMATCH (invoice sums mismatch).')
                            if di_id == detail_item_id:
                                new_state = 'PO MISMATCH'
                        except Exception as e:
                            logger.error(f'Could not update DetailItem(id={di_id}) => PO MISMATCH: {e}')
    if new_state == 'RTP' and payment_type in ['INV', 'PROJ']:
        logger.info(f'DetailItem(id={detail_item_id}) => RTP => inlining Bill creation logic.')
        reference_key = f"{project_number}_{po_number}_{detail_number or 'XX'}"
        xero_bill = _create_or_get_xero_bill(reference_key=reference_key, project_number=project_number, po_number=po_number, detail_number=detail_number)
        if xero_bill and xero_bill.get('id'):
            parent_id = xero_bill['id']
            _ensure_bill_line_item(detail_item, parent_id)
        else:
            logger.error(f"Failed to create/retrieve XeroBill for ref_key='{reference_key}'.")
    from datetime import datetime, date
    if new_state in {'REVIEWED', 'PO MISMATCH'} and due_date:
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
                new_state = 'OVERDUE'
        except Exception as e:
            logger.warning(f'Failed to parse due_date for detail_item_id={detail_item_id}: {e}')
    if new_state in {'PAID', 'RECONCILED', 'AUTHORIZED', 'APPROVED'}:
        if payment_type in ['INV', 'PROJ']:
            try:
                line_items = db_ops.search_bill_line_items(['detail_item_id'], [detail_item_id])
                if line_items:
                    if isinstance(line_items, list):
                        line_items = line_items[0]
                    xero_line_amount = float(line_items.get('line_amount', 0.0))
                    if abs(xero_line_amount - sub_total) >= 0.0001:
                        new_state = 'ISSUE'
            except Exception as e:
                logger.warning(f'Could not check BillLineItems for detail_item_id={detail_item_id}: {e}')
        elif payment_type in ['CC', 'PC']:
            try:
                sm_recs = db_ops.search_spend_money_by_keys(project_number=project_number, po_number=po_number, detail_number=detail_number, line_number=line_number)
                if sm_recs:
                    if isinstance(sm_recs, list):
                        sm_recs = sm_recs[0]
                    spend_amount = float(sm_recs.get('amount', 0.0))
                    if abs(spend_amount - sub_total) >= 0.0001:
                        new_state = 'ISSUE'
            except Exception as e:
                logger.warning(f'Could not check SpendMoney for detail_item_id={detail_item_id}: {e}')
    if new_state != current_state:
        try:
            db_ops.update_detail_item(detail_item_id=detail_item_id, state=new_state)
            logger.info(f'DetailItem(id={detail_item_id}) state changed {current_state} => {new_state}')
        except Exception as e:
            logger.error(f'Failed to update DetailItem(id={detail_item_id}) => {new_state}: {e}')
    else:
        logger.debug(f"No state change for DetailItem(id={detail_item_id}); remains '{new_state}'.")

def _create_or_get_xero_bill(reference_key: str, project_number: int, po_number: int, detail_number: int):
    """
    Looks up an existing XeroBill by (project_number, po_number, detail_number).
    If not found, creates a new one with xero_reference_number=reference_key.
    """
    try:
        existing = db_ops.search_xero_bills(['project_number', 'po_number', 'detail_number'], [project_number, po_number, detail_number])
        if existing:
            return existing[0] if isinstance(existing, list) else existing
    except Exception as e:
        logger.error(f'DB error searching xero_bills for (proj={project_number}, po={po_number}, detail={detail_number}): {e}')
        return None
    try:
        new_xero_bill = db_ops.create_xero_bill(state='Draft', xero_reference_number=reference_key, project_number=project_number, po_number=po_number, detail_number=detail_number)
        if new_xero_bill:
            logger.info(f"Created XeroBill(id={new_xero_bill.get('id')}) for reference='{reference_key}'")
            return new_xero_bill
        else:
            logger.warning(f"Could not create XeroBill for reference='{reference_key}'.")
            return None
    except Exception as e:
        logger.error(f"Failed to create XeroBill for ref='{reference_key}': {e}")
        return None

def _ensure_bill_line_item(detail_item: dict, parent_id: int):
    """
    If there's a BillLineItem linking (parent_id, line_number), update if changed;
    otherwise create a new BillLineItem.
    Ensures that BillLineItem also has the correct project_number, po_number, detail_number, and line_number.

    NOTE: This version fetches the numeric tax code from BudgetMap→AccountCode→TaxAccount
          and stores it in BillLineItem.account_code. We do not send it to Xero.
    """
    line_number = detail_item['line_number']
    description = detail_item.get('description', 'No description provided')
    sub_total = float(detail_item.get('sub_total') or 0.0)
    numeric_tax_code = None
    try:
        project_records = db_ops.search_projects(['project_number'], [detail_item['project_number']])
        if project_records:
            project = project_records[0] if isinstance(project_records, list) else project_records
            budget_map_id = project.get('budget_map_id')
            logger.info(f"[some_trigger_logic] Found project for project_number={detail_item['project_number']} -> budget_map_id={budget_map_id}.")
        else:
            logger.warning(f"[some_trigger_logic] Could not find project with project_number={detail_item['project_number']}.")
            budget_map_id = None
    except Exception as e:
        logger.warning(f"[some_trigger_logic] Failed to fetch project for project_number={detail_item['project_number']}: {e}")
        budget_map_id = None
    if budget_map_id:
        try:
            budget_maps = db_ops.search_budget_maps(['id'], [budget_map_id])
            if budget_maps:
                budget_map_record = budget_maps[0] if isinstance(budget_maps, list) else budget_maps
                budget_map_name = budget_map_record.get('map_name')
                logger.info(f"[some_trigger_logic] Found BudgetMap(id={budget_map_id}) for map_name='{budget_map_name}'.")
            else:
                logger.warning(f"[some_trigger_logic] No BudgetMap found for ID= {budget_map_id}'.")
        except Exception as e:
            logger.warning(f"[some_trigger_logic] Failed to fetch BudgetMap with ID='{budget_map_id}': {e}")
    account_code_id = None
    if budget_map_id:
        try:
            logger.info(f"[some_trigger_logic] Searching AccountCodes for code='{detail_item['account_code']}' with budget_map_id={budget_map_id}.")
            account_codes = db_ops.search_account_codes(['code', 'budget_map_id'], [detail_item['account_code'], budget_map_id])
            if account_codes:
                account_code_rec = account_codes[0] if isinstance(account_codes, list) else account_codes
                account_code_id = account_code_rec.get('id')
                tax_id = account_code_rec.get('tax_id')
                logger.info(f"[some_trigger_logic] Found AccountCode(id={account_code_id}). tax_id={tax_id}. detail_item.account_code={detail_item['account_code']}")
                if tax_id:
                    try:
                        tax_record = db_ops.search_tax_accounts(['id'], [tax_id])
                        if tax_record and (not isinstance(tax_record, list)):
                            numeric_tax_code = tax_record.get('tax_code')
                            logger.info(f"[some_trigger_logic] TaxAccount found for id={tax_id}, numeric_tax_code='{numeric_tax_code}'.")
                        else:
                            logger.info(f'[some_trigger_logic] No single TaxAccount record found for id={tax_id} (or returned multiple).')
                    except Exception as e:
                        logger.warning(f'[some_trigger_logic] Failed to fetch TaxAccount(id={tax_id}): {e}')
                else:
                    logger.info(f'[some_trigger_logic] tax_id was None, so no TaxAccount is set for this AccountCode.')
            else:
                logger.warning(f"[some_trigger_logic] No AccountCode found for code='{detail_item['account_code']}' and budget_map_id={budget_map_id}.")
        except Exception as e:
            logger.warning(f"[some_trigger_logic] Failed searching AccountCodes with code='{detail_item['account_code']}', budget_map_id={budget_map_id}: {e}")
    logger.info(f"[some_trigger_logic] Final numeric_tax_code={numeric_tax_code} for detail_item.account_code={detail_item['account_code']}.")
    project_number = detail_item['project_number']
    po_number = detail_item['po_number']
    detail_number = detail_item['detail_number']
    final_account_code = numeric_tax_code if numeric_tax_code else detail_item['account_code']
    try:
        found = db_ops.search_bill_line_items(['parent_id', 'line_number'], [parent_id, line_number])
    except Exception as e:
        logger.error(f'DB error searching BillLineItem(parent_id={parent_id}, line_number={line_number}): {e}')
        return
    if found:
        if isinstance(found, list):
            found = found[0]
        bill_line_item_id = found['id']
        needs_update = found.get('description') != description or abs(float(found.get('line_amount', 0.0)) - sub_total) >= 0.0001 or found.get('code') != final_account_code or (found.get('project_number') != project_number) or (found.get('po_number') != po_number) or (found.get('detail_number') != detail_number)
        if needs_update:
            try:
                updated_line = db_ops.update_bill_line_item(bill_line_item_id=bill_line_item_id, description=description, quantity=1, unit_amount=sub_total, line_amount=sub_total, account_code=final_account_code, project_number=project_number, po_number=po_number, detail_number=detail_number)
                logger.info(f'Updated BillLineItem(id={bill_line_item_id}) with new amounts/description/code.')
                return updated_line
            except Exception as e:
                logger.warning(f'Failed to update BillLineItem(id={bill_line_item_id}): {e}')
        else:
            logger.debug(f'No changes needed for existing BillLineItem(id={bill_line_item_id}).')
        return found
    try:
        new_line = db_ops.create_bill_line_item(parent_id=parent_id, project_number=project_number, po_number=po_number, detail_number=detail_number, line_number=line_number, description=description, quantity=1, unit_amount=sub_total, line_amount=sub_total, account_code=final_account_code)
        if new_line:
            logger.info(f"Created BillLineItem(id={new_line.get('id')}) for parent_id={parent_id}.")
        else:
            logger.warning(f'Could not create BillLineItem for parent_id={parent_id}.')
        return new_line
    except Exception as e:
        logger.error(f'Failed to create BillLineItem for parent_id={parent_id}, line_number={line_number}: {e}')
    return None

def _fetch_account_code(account_code_id: Optional[int]):
    """
    If your new schema still supports storing an 'account_code_id' on DetailItem,
    fetch the associated 'tax_code' or similar from AccountCode and TaxAccount.
    Otherwise, remove or simplify this function.
    """
    if not account_code_id:
        return None
    try:
        account_code_record = db_ops.search_account_codes(['id'], [account_code_id])
        if not account_code_record or isinstance(account_code_record, list):
            logger.warning(f'Account code not found or not unique for id={account_code_id}.')
            return None
        tax_account_id = account_code_record.get('tax_id')
        if not tax_account_id:
            return None
        tax_account_record = db_ops.search_tax_accounts(['id'], [tax_account_id])
        if not tax_account_record or isinstance(tax_account_record, list):
            logger.warning(f'Tax account not found or not unique for id={tax_account_id}.')
            return None
        return tax_account_record.get('tax_code')
    except Exception as e:
        logger.error(f'DB error searching for Account code with id={account_code_id}: {e}')
        return None