"""
server_celery/triggers/budget_triggers.py

Holds trigger functions for:
  - PO Log
  - PurchaseOrder (PO)
  - DetailItem

Utilizes aggregator checks to distinguish between partial vs. final logic.
In partial mode (aggregator in progress), we skip big logic like Xero or Monday calls.
Once aggregator is done (status='COMPLETED'), we perform the single-item logic,
including sibling checks (like for INV sums, CC receipts, etc.).
"""

import logging

# region 🔧 Imports
from database.db_util import get_db_session
from database.database_util import DatabaseOperations
from files_xero.xero_services import xero_services   # for Xero calls
from files_dropbox.dropbox_service import DropboxService  #for links and files
from files_monday.monday_service import monday_service    # for Monday upserts
from files_budget.budget_service import budget_service  # aggregator checks + date-range updates
# endregion

# region 🏗️ Setup
db_ops = DatabaseOperations()
logger = logging.getLogger('budget_logger')
dropbox_service = DropboxService()
use_control_panel = True
# endregion



# region 📄 PO LOG TRIGGERS
def handle_po_log_create(po_log_id: int) -> None:
    """
    Triggered when a new PO Log entry is created or updated in the DB.
    Typically, the aggregator might set status='STARTED', parse data, then set status='COMPLETED'.
    We'll just call whatever aggregator logic we have at budget_service.
    """
    """
    1) Finds po_log row(s) with status='STARTED'
    2) Parses aggregator data from some text file or source
    3) For each item:
       - If detail item is CC/PC => compare sub_total vs. receipt => if match => state=REVIEWED => create spend money in Xero
       - If detail item is INV/PROF => sum sub_total for siblings => if match invoice => set them RTP
         => if now RTP => create xero bill with line items in Xero
       - Also update Monday for main (PO) and sub (detail item) changes
    4) Finally set po_log.status='COMPLETED'
    """
    logger.info("🚀 Running aggregator flow => 'po_log_new_trigger'!")



    if use_control_panel:
        db_ops.update_po_log(po_log_id, status = "STARTED")
        logger.info("🚀 CONTROL PANEL SET PO LOG STATUS TO - STARTED")

    # 1) Find po_log rows with status='STARTED'
    po_log = db_ops.search_po_logs(['id'], [po_log_id])
    if not po_log or not po_log["status"] == "STARTED":
        logger.info("🤷 No po_logs with status=STARTED found. Nothing to do.")
        return


    # 2) Parse aggregator data from a the text file or source
    po_log_data = budget_service.parse_po_log_data(po_log)
    if not po_log_data:
        logger.info("😶 No aggregator data parsed => skipping.")
        return

    #3) Load PO Log Data into the DB 1 section at a timea
    with get_db_session() as session_1:
        budget_service.process_contact_aggregator(po_log_data["contacts"], session=session_1)

    # with get_db_session() as session_2:
    #     budget_service.process_aggregator_pos(po_log_data, session=session_2)
    #
    # with get_db_session() as session_3:
    #     budget_service.process_aggregator_detail_item(po_log_data, session=session_3)

    # 4) Once we’ve processed everything, set po_log.status='COMPLETED'
    updated = db_ops.update_po_log(
        po_log_id=po_log_id,
        status='COMPLETED'
    )
    if updated:
        logger.info(f"🏁 PO log (ID={po_log_id}) => status='COMPLETED'!")
    else:
        logger.warning(f"⚠️ Could not update PO log ID={po_log_id} => COMPLETED.")
    logger.info("🏁 Done aggregator logic for all 'STARTED' logs.")
    
# endregion

# region 📝 PURCHASE ORDER TRIGGERS
def handle_purchase_order_create(po_id: int) -> None:
    """
    Triggered when a new PurchaseOrder is inserted.
    """
    logger.info("📦 PurchaseOrder CREATE trigger fired!")
    purchase_order = db_ops.search_purchase_orders(['id'], [po_id])
    if not purchase_order:
        logger.warning("❌ No PurchaseOrder found in DB. Possibly a mismatch!")
        return
    if isinstance(purchase_order, list):
        purchase_order = purchase_order[0]

    project_number = purchase_order["project_number"]
    po_log = db_ops.search_po_logs(["project_number"], [project_number])

    logger.info(f"🖋️ Checking aggregator status for project={purchase_order.get('project_number')} and ID={po_id}...")

    # region 🎛️ Aggregator Check
    if budget_service.is_aggregator_in_progress(purchase_order):
        logger.info("⏳ Aggregator is still in progress (status=STARTED). We'll do partial logic only!")
        return
    logger.info("✅ Aggregator is done (or no aggregator). Let's finalize this PO creation!")
    # endregion

    # region 🔧 Normal Single-Record Logic
    # e.g. link vendor contact if missing
    if not purchase_order.get('contact_id'):
        logger.info("🕵️  Searching or creating contact for this PO!")
        new_contact_id = dropbox_service.find_or_create_vendor_contact(purchase_order)
        if new_contact_id:
            logger.info("👤 Found/created contact, updating DB!")
            db_ops.update_purchase_order(purchase_order['id'], contact_id=new_contact_id)

    # # Possibly find dropbox folder link TODO
    # if not purchase_order.get('folder_link'):
    #     logger.info("🔗 Checking for dropbox folder link for this PO!")
    #     folder_link = dropbox_service.find_po_folder_link(purchase_order)
    #     if folder_link:
    #         logger.info(f"🌐 Found folder link: {folder_link}, updating DB!")
    #         db_ops.update_purchase_order(purchase_order['id'], folder_link=folder_link)
    #
    # # Upsert to Monday board
    # logger.info("🔄 Sending to Monday board to reflect this new PO!")
    # monday_service.upsert_po_in_monday(purchase_order)

    logger.info("🎉 PurchaseOrder CREATE trigger finished final logic.")
    # endregion


def handle_purchase_order_update(po_number: int) -> None:
    """
    Triggered when an existing PurchaseOrder is updated.
    We do the same aggregator check, then normal single logic if aggregator=done.
    """
    logger.info("📦 PurchaseOrder UPDATE trigger fired!")
    purchase_order = db_ops.search_purchase_orders(['po_number'], [po_number])
    if not purchase_order:
        logger.warning("❌ No PurchaseOrder found in DB for update!")
        return
    if isinstance(purchase_order, list):
        purchase_order = purchase_order[0]

    logger.info("🕵️ Checking aggregator status for this PO!")
    # region 🎛️ Aggregator Check
    if budget_service.is_aggregator_in_progress(purchase_order):
        logger.info("⏳ Aggregator still in progress => partial skip!")
        return
    logger.info("✅ Aggregator done => let's do final update logic!")
    # endregion

    # region 🔧 Normal Single-Record Logic
    # e.g. re-check contact, folder link, upsert Monday
    monday_service.upsert_po_in_monday(purchase_order)
    logger.info("🎉 PurchaseOrder UPDATE trigger final logic complete.")
    # endregion


def handle_purchase_order_delete(po_number: int) -> None:
    """
    Triggered when a PurchaseOrder is deleted.
    """
    logger.info("📦 PurchaseOrder DELETE trigger fired!")
    # aggregator check might not be needed, but we can do it if we want
    logger.info("🔎 Checking aggregator for partial skip if needed!")
    # Possibly skip or do final logic
    logger.info("🗑️ PurchaseOrder DELETE done. 🏁")
# endregion

# region 🧱 DETAIL ITEM TRIGGERS
def handle_detail_item_create(detail_item_id: int) -> None:
    """
    Triggered when a new DetailItem is created.
    We'll call handle_detail_item_create_logic, which does aggregator check + single logic.
    """
    logger.info("🧱 DetailItem CREATE trigger fired!")
    handle_detail_item_create_logic(detail_item_id)

def handle_detail_item_create_logic(detail_item_id: int) -> None:
    """
    Our main create logic for new DetailItem. We do aggregator checks, then final logic:
      - If type=CC or PC => attempt to match a receipt
      - If type=INV => sum invoice total, if matched => set them all RTP
      - Possibly upsert subitems to Monday
      - Possibly create or update Xero records if aggregator is done
    """
    logger.info("🧱 Checking DB for the newly inserted DetailItem!")
    detail_item = db_ops.search_detail_items(['id'], [detail_item_id])
    if not detail_item or isinstance(detail_item, list):
        logger.warning("❌ Could not find a unique DetailItem record in DB!")
        return

    logger.info("🔎 Seeing if aggregator is in progress for this detail item...")
    # region 🎛️ Aggregator Check
    if budget_service.is_aggregator_in_progress(detail_item):
        logger.info("⏳ Aggregator=STARTED => partial skip. We'll wait until aggregator=COMPLETED to do final logic.")
        return
    logger.info("✅ Aggregator done => continuineg single-record logic for detail item!")
    # endregion

    # region 🔧 Single-record logic
    payment_type = (detail_item.get('payment_type') or '').upper()
    current_state = (detail_item.get('state') or '').upper()

    # If CC/PC => try matching receipts
    if payment_type in ["CC", "PC"]:
        logger.info("💳 PaymentType=CC/PC => let's see if there's a matching receipt in Dropbox/DB!")
        matched_receipt = dropbox_service.match_receipt_for_detail(detail_item)
        if matched_receipt:
            logger.info(f"🧾 Found a receipt => total={matched_receipt['total']} SubTotal={detail_item['sub_total']}")
            if abs(float(detail_item['sub_total'] or 0.0) - float(matched_receipt['total'] or 0.0)) < 0.0001:
                logger.info("🔑 Subtotal matches => let's set detail item to REVIEWED or RTP if you prefer.")
                db_ops.update_detail_item(detail_item_id, state="REVIEWED")
                # Possibly create a spend_money record & call xero
                logger.info("🌀 Creating SpendMoney in DB + Xero if aggregator=done.")
            else:
                logger.info("🛑 Mismatch => setting state=PO MISMATCH or partial state.")
                db_ops.update_detail_item(detail_item_id, state="PO MISMATCH")
        else:
            logger.info("😶 No matching receipt found => partial skip or mismatch.")
            # Possibly set state = 'PO MISMATCH' or remain 'PENDING'

    # If INV => sum up detail items, compare to invoice
    elif payment_type == "INV":
        logger.info("🗒️ PaymentType=INV => let's see if all detail items sum matches invoice total. If so => set RTP!")
        # Suppose we call a service:
        if budget_service.sum_detail_items_and_compare_invoice(detail_item):
            logger.info("✅ All detail items match invoice => set them to RTP!")
            # maybe call something like:
            budget_service.set_invoice_details_rtp(detail_item)
            # If all are now RTP, possibly create Xero Bill
            # or we rely on detail_item_update logic for that
        else:
            logger.info("🔻 The sums do not match => setting mismatch or partial state.")
            # your logic for mismatch

    # region Upsert subitem to Monday
    logger.info("🔄 Upserting detail item to Monday subitem board!")
    monday_service.upsert_detail_subitem_in_monday(detail_item)
    # endregion

    logger.info("🎉 Done processing detail_item_create_logic for aggregator=done scenario.")
    # endregion

def handle_detail_item_update(detail_item_id: int) -> None:
    """
    Triggered when a DetailItem is updated. We do aggregator checks, then final logic:
     - e.g. if it transitions to RTP for an INV, we might create Xero Bill
    """
    logger.info("🧱 DetailItem UPDATE trigger fired!")
    detail_item = db_ops.search_detail_items(['id'], [detail_item_id])
    if not detail_item or isinstance(detail_item, list):
        logger.warning("❌ No unique DetailItem found in DB!")
        return

    # region 🎛️ Aggregator Check
    if budget_service.is_aggregator_in_progress(detail_item):
        logger.info("⏳ Aggregator=STARTED => partial skip for detail_item UPDATE.")
        return
    logger.info("✅ Aggregator=COMPLETED => let's do the single logic for detail_item update!")
    # endregion

    def handle_detail_item_update(detail_item_id: int) -> None:
        """
        Triggered when a DetailItem is updated. We do aggregator checks, then final logic:
         - e.g. if it transitions to RTP for an INV, we might create or upsert a Xero Bill/Line Items
           if all siblings are also RTP.
         - If CC/PC and newly reviewed, we can finalize spend money in Xero (if aggregator is done).
        """
        logger.info("🧱 DetailItem UPDATE trigger fired!")
        detail_item = db_ops.search_detail_items(['id'], [detail_item_id])
        if not detail_item or isinstance(detail_item, list):
            logger.warning("❌ No unique DetailItem found in DB!")
            return

        # region 🎛️ Aggregator Check
        if budget_service.is_aggregator_in_progress(detail_item):
            logger.info("⏳ Aggregator=STARTED => partial skip for detail_item UPDATE.")
            return
        logger.info("✅ Aggregator=COMPLETED => let's do the single logic for detail_item update!")

        # endregion

        # --------------------------------------------------------
        # Helper function: upsert a XeroBillLineItem for detail_item
        # --------------------------------------------------------
        def upsert_xero_bill_line_item(xero_bill_id: int, detail_item_data: dict) -> int:
            """
            Looks for an existing XeroBillLineItem for the given XeroBill (xero_bill_id) and
            (detail_number, line_number). If found, update. Otherwise, create.
            Returns the ID of the XeroBillLineItem.
            """
            detail_num = detail_item_data['detail_number']
            line_num = detail_item_data.get('line_number')  # might be None or zero
            logger.info(
                f"🔎 Attempting upsert on XeroBillLineItem for xero_bill_id={xero_bill_id}, "
                f"detail_number={detail_num}, line_number={line_num}"
            )

            existing_line = db_ops.search_xero_bill_line_items(
                ["parent_id", "detail_number", "line_number"],
                [xero_bill_id, detail_num, line_num]
            )
            # If no match found, create
            if not existing_line:
                logger.info("🆕 No existing line item found; creating new XeroBillLineItem.")
                created_line = db_ops.create_xero_bill_line_item(
                    parent_id=xero_bill_id,
                    project_number=detail_item_data['project_number'],
                    po_number=detail_item_data['po_number'],
                    detail_number=detail_num,
                    line_number=line_num,
                    line_amount=detail_item_data.get('sub_total'),
                    unit_amount=detail_item_data.get('sub_total'),
                    description=detail_item_data.get('description', ''),
                    quantity=detail_item_data.get('qty', 1),
                    transaction_date = detail_item_data.get('transaction_date'),
                    due_date = detail_item_data.get('due_date'),
                    account_code= _get_tax_from_detail(detail_item_data)
                )
                return created_line['id'] if created_line else 0

            # If we do have one, update the first match (or pick the single record)
            if isinstance(existing_line, list):
                line_to_update = existing_line[0]
            else:
                line_to_update = existing_line

            logger.info(f"🔄 Updating existing XeroBillLineItem id={line_to_update['id']}.")
            updated_line = db_ops.update_xero_bill_line_item(
                line_to_update['id'],
                project_number=detail_item_data['project_number'],
                po_number=detail_item_data['po_number'],
                detail_number=detail_num,
                line_number=line_num,
                line_amount=detail_item_data.get('sub_total'),
                unit_amount=detail_item_data.get('sub_total'),
                description=detail_item_data.get('description', ''),
                quantity=detail_item_data.get('qty', 1),
                transaction_date=detail_item_data.get('transaction_date'),
                due_date=detail_item_data.get('due_date'),
                account_code=_get_tax_from_detail(detail_item_data)
            )
            return updated_line['id'] if updated_line else 0

        # region 🔧 Single-Record Logic
        state_now = (detail_item.get('state') or '').upper()
        pay_type = (detail_item.get('payment_type') or '').upper()

        # -------------------------
        # 1) If INV => Check siblings. If all are RTP, create or upsert XeroBill + line items
        # -------------------------
        if state_now == "RTP" and pay_type == "INV":
            logger.info("🟩 This item is now RTP with pay_type=INV => let's see if all siblings are also RTP!")
            all_rtp = budget_service.check_siblings_all_rtp(detail_item)
            if all_rtp:
                logger.info(
                    "✨ All siblings are RTP => either create a new Xero Bill if none exists, "
                    "or upsert an existing Xero Bill's line items."
                )

                # Search for an existing XeroBill for this project/PO
                xero_bill = db_ops.search_xero_bill_by_keys(
                    project_number=detail_item['project_number'],
                    po_number=detail_item['po_number'],
                    detail_number=detail_item['detail_number']
                )

                if not xero_bill:
                    logger.info("🚀 No existing XeroBill found. Creating a new one.")
                    created_bill = db_ops.create_xero_bill_by_keys(
                        project_number=detail_item['project_number'],
                        po_number=detail_item['po_number'],
                        detail_number=detail_item['detail_number'],
                        state="DRAFT"
                    )
                    if created_bill:
                        xero_bill_id = created_bill['id']
                        logger.info(f"🏷️ Created new XeroBill (ID={xero_bill_id}).")
                    else:
                        logger.warning("❌ Could not create a new XeroBill. Skipping line item logic.")
                        xero_bill_id = None
                else:
                    # If we got a list, take the first entry
                    if isinstance(xero_bill, list):
                        xero_bill_id = xero_bill[0]['id']
                    else:
                        xero_bill_id = xero_bill['id']
                    logger.info(f"✅ Found existing XeroBill (ID={xero_bill_id}).")

                if xero_bill_id:
                    # Upsert the associated line item
                    line_item_id = upsert_xero_bill_line_item(xero_bill_id, detail_item)
                    logger.info(f"🔄 Upserted detail item into XeroBillLineItem (ID={line_item_id}).")

                    # Optionally call xero_triggers.handle_xero_bill_update
                    from xero_triggers import handle_xero_bill_update
                    handle_xero_bill_update(xero_bill_id)
            else:
                logger.info("🤔 Not all siblings are RTP => skipping Xero Bill for now.")

        # -------------------------
        # 2) If CC/PC => possibly finalize spend money when state=REVIEWED
        # -------------------------
        if pay_type in ["CC", "PC"] and state_now == "REVIEWED":
            logger.info("🌀 Possibly finalize spend money in Xero if aggregator=done, or partial skip.")
            # e.g. create or update (upsert) spend money
            # In the same style, do a search -> if found, update -> else create
            spend_money = db_ops.search_spend_money_by_keys(
                project_number=detail_item['project_number'],
                po_number=detail_item['po_number'],
                detail_number=detail_item['detail_number'],
                line_number=detail_item.get('line_number', 0),
                deleted=False
            )
            if not spend_money:
                logger.info("🆕 No SpendMoney record => creating new one.")
                new_sm = db_ops.create_spend_money(
                    project_number=detail_item['project_number'],
                    po_number=detail_item['po_number'],
                    detail_number=detail_item['detail_number'],
                    line_number=detail_item.get('line_number', 0),
                    amount=detail_item.get('sub_total', 0.0),
                    state = "DRAFT",
                    tax_code = _get_tax_from_detail(detail_item)
                )
                spend_money_id = new_sm['id'] if new_sm else 0
            else:
                if isinstance(spend_money, list):
                    spend_money = spend_money[0]
                logger.info(f"🔄 Found existing SpendMoney (ID={spend_money['id']}). Updating.")
                updated_sm = db_ops.update_spend_money(
                    spend_money['id'],
                    amount=detail_item.get('sub_total', 0.0),
                    # etc.
                )
                spend_money_id = updated_sm['id'] if updated_sm else 0

            logger.info(f"🔄 Spend money record (ID={spend_money_id}) upserted.")
            # Optionally call Xero triggers to push the spend money record
            # from xero_triggers import handle_spend_money_create
            # handle_spend_money_create(spend_money_id)

        # -------------------------
        # 3) Upsert subitem to Monday
        # -------------------------
        logger.info("🔄 Upserting updated detail item to Monday subitem board!")
        monday_service.upsert_detail_subitem_in_monday(detail_item)

        logger.info("🎉 Done with single-logic for detail_item_update!")
        # endregion
    #   #endregion

def handle_detail_item_delete(detail_item_id: int) -> None:
    """
    Triggered when a DetailItem is deleted.
    """
    logger.info("🧱 DetailItem DELETE trigger fired!")
    # aggregator check? Possibly skip
    logger.info("🗑️  Completed detail item DELETE logic, if any. 🏁")
#endregion

#region 🪻PROJECT TRIGGERS
def handle_project_update():
    return None

def handle_project_create():
    return None

def handle_project_delete():
    return None
#endregion



#region HELPER FUNCTIONS
def _get_tax_from_detail(detail_item: dict) -> int:
    account_code = detail_item["account_code"]
    budget_map_id = db_ops.search_projects(["project_number"], detail_item["project_number"])["budget_map_id"]
    tax_code_id = db_ops.search_tax_accounts(["budget_map_id", "code"], [budget_map_id, account_code] )["tax_id"]
    tax_code = db_ops.search_tax_accounts(['id'], tax_code_id)
    return tax_code
#endregion
