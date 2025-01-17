import logging
from database.database_util import DatabaseOperations
db_ops = DatabaseOperations()
logger = logging.getLogger('database_logger')

def handle_invoice_create_or_update(invoice_id: int) -> None:
    """
    Trigger logic for Invoice create/update.
    1) Locate the invoice record (including invoice_number, project_number, po_number).
    2) Link any unlinked detail items (payment_type="INV" and detail_number = invoice_number).
    3) Mark those newly linked items 'INVOICED' if they aren't in a final state.
    4) Finally, compare sum(sub_totals) vs. invoice.total to set them 'RTP' or 'PO MISMATCH'.
    """
    logger.info(f'📄 [INVOICE CREATE/UPDATE] invoice_id={invoice_id}')
    logger.info(f'📄 🔔 Triggering invoice check for invoice_id={invoice_id}...')
    invoice_record = db_ops.search_invoices(['id'], [invoice_id])
    if not invoice_record or isinstance(invoice_record, list):
        logger.warning(f'📄 ❌ Could not find unique Invoice with id={invoice_id}. Bailing out.')
        return
    invoice_total = float(invoice_record.get('total', 0.0))
    project_number = invoice_record.get('project_number')
    po_number = invoice_record.get('po_number')
    invoice_num = invoice_record.get('invoice_number')
    po_record = db_ops.search_purchase_order_by_keys(project_number=project_number, po_number=po_number)
    detail_matches = db_ops.search_detail_items(column_names=['po_id', 'detail_number', 'payment_type'], values=[po_record['id'], invoice_num, 'INV'])
    if detail_matches:
        if not isinstance(detail_matches, list):
            detail_matches = [detail_matches]
        for d_item in detail_matches:
            if d_item.get('invoice_id') != invoice_id:
                db_ops.update_detail_item_by_keys(project_number, po_number, d_item['detail_number'], d_item['line_number'], invoice_id=invoice_id)
                logger.info(f"📄 Linked invoice_id={invoice_id} to detail_item #{d_item['detail_number']} (DB ID={d_item['id']}).")
            current_state = (d_item.get('state') or '').upper()
            if current_state not in ['RECONCILED', 'PAID', 'RTP', 'PO MISMATCH']:
                db_ops.update_detail_item_by_keys(project_number, po_number, d_item['detail_number'], d_item['line_number'], state='PENDING')
                logger.info(f"📄 Marked detail_item #{d_item['detail_number']} (ID={d_item['id']}) as PENDING.")
    else:
        logger.info(f'📄 💤 No matching detail items found to link for invoice_id={invoice_id}, invoice_number={invoice_num}.')
    linked_items = db_ops.search_detail_items(['invoice_id'], [invoice_id])
    if not linked_items:
        logger.info(f'📄 💤 No detail items linked to invoice_id={invoice_id} after linking step. Nothing to do.')
        return
    if isinstance(linked_items, dict):
        linked_items = [linked_items]
    valid_items = [d for d in linked_items if d.get('state', '').upper() not in ['RECONCILED', 'PAID']]
    if not valid_items:
        logger.info(f'📄 🎉 All detail items for invoice_id={invoice_id} are RECONCILED or PAID. No sum check needed.')
        return
    total_sub = sum((float(d.get('sub_total', 0.0)) for d in valid_items))
    if abs(total_sub - invoice_total) < 0.0001:
        logger.info(f"📄 ✅ Invoice total = sum of details for invoice_id={invoice_id}. Marking them 'RTP'.")
        for d in valid_items:
            db_ops.update_detail_item_by_keys(project_number, po_number, d['detail_number'], d['line_number'], state='RTP')
    else:
        logger.info(f"📄 ⚠️ Invoice total != sum of details for invoice_id={invoice_id}. Marking 'PO MISMATCH'.")
        for d in valid_items:
            db_ops.update_detail_item_by_keys(project_number, po_number, d['detail_number'], d['line_number'], state='PO MISMATCH')
    logger.info(f'📄 🏁 Finished trigger logic for invoice_id={invoice_id}.')

def handle_invoice_delete(invoice_id: int) -> None:
    logger.info(f'🗑️ [INVOICE DELETE] invoice_id={invoice_id}')
    logger.info(f'🗑️ Deleting invoice with id={invoice_id}...')
    pass

def handle_receipt_create(receipt_id: int) -> None:
    logger.info(f'🧾 [RECEIPT CREATE] receipt_id={receipt_id}')
    logger.info(f'🧾 Creating receipt with id={receipt_id}...')
    pass

def handle_receipt_update(receipt_id: int) -> None:
    logger.info(f'🧾 [RECEIPT UPDATE] receipt_id={receipt_id}')
    logger.info(f'🧾 Updating receipt with id={receipt_id}...')
    pass

def handle_receipt_delete(receipt_id: int) -> None:
    logger.info(f'🧾 [RECEIPT DELETE] receipt_id={receipt_id}')
    logger.info(f'🧾 Deleting receipt with id={receipt_id}...')
    pass