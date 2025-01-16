# xero_triggers.py

import logging
import re
from typing import Dict, Any, List
from database.database_util import DatabaseOperations
from xero_api import xero_api

db_ops = DatabaseOperations()
logger = logging.getLogger("celery_logger")


# ------------------------------------------------------------------------
# HELPER: Compare essential invoice fields to see if Xero invoice differs from local data
# ------------------------------------------------------------------------
def _compare_xero_invoice_data(
        existing_invoice: Dict[str, Any],
        new_invoice_data: Dict[str, Any]
) -> bool:
    """
    Returns True if the existing Xero invoice and our local invoice_data differ
    in one of the key fields. Otherwise, returns False.

    We focus on 'Date', 'DueDate', and 'Reference' here, but expand as needed.
    """
    existing_date = existing_invoice.get("Date")
    existing_due_date = existing_invoice.get("DueDate")
    existing_reference = existing_invoice.get("Reference")

    new_date = new_invoice_data.get("Date")
    new_due_date = new_invoice_data.get("DueDate")
    new_reference = new_invoice_data.get("Reference")

    # Compare the values. If any differ, return True
    if existing_date != new_date:
        return True
    if existing_due_date != new_due_date:
        return True
    if existing_reference != new_reference:
        return True

    # If all match, return False
    return False


def parse_reference_key(reference_key: str) -> dict:
    """
    Parses a reference key of the format 'projectNumber_poNumber_detailNumber'.
    If the detail number is missing, it defaults to 1.

    Args:
        reference_key (str): The reference key to parse, e.g., '2416_04_03'.

    Returns:
        dict: A dictionary with keys 'project_number', 'po_number', and 'detail_number'.
    """
    if not reference_key:
        raise ValueError("reference_key cannot be empty or None.")

    pattern = r"^(\d+)_0*(\d+)(?:_0*(\d+))?$"
    match = re.match(pattern, reference_key)

    if not match:
        raise ValueError(f"Invalid reference key format: '{reference_key}'.")

    project_number, po_number, detail_number = match.groups()
    return {
        "project_number": int(project_number),
        "po_number": int(po_number),
        "detail_number": int(detail_number) if detail_number else 1
    }


def _upsert_xero_bill_line_item(bill_line_item_id: int) -> None:
    """
    Creates or updates a single line item in an ACCPAY invoice in Xero,
    based on the local BillLineItem record. If BillLineItem.parent_xero_id is empty,
    we attempt to create a new line item in the invoice; otherwise we try to
    update the existing line item in Xero.

    Steps:
      1) Fetch the BillLineItem from DB.
      2) Fetch the associated XeroBill (for invoice reference).
      3) Retrieve or build the line item data (quantity, unit, tax codes, etc.).
      4) Retrieve the ACCPAY invoice from Xero by the XeroBill.xero_reference_number.
      5) Find or create the matching line item in Xero.
      6) Save invoice changes to Xero.
      7) Update local BillLineItem.parent_xero_id if we created a new line item.
    """
    logger.info(f"[XERO BillLineItem UPSERT] BillLineItem ID={bill_line_item_id}")

    # 1) Fetch the BillLineItem from DB
    bill_line_item = db_ops.search_bill_line_items(["id"], [bill_line_item_id])
    if not bill_line_item:
        logger.warning(f"BillLineItem(id={bill_line_item_id}) not found; aborting.")
        return
    if isinstance(bill_line_item, list):
        bill_line_item = bill_line_item[0]

    parent_id = bill_line_item.get("parent_id")
    if not parent_id:
        logger.warning(f"BillLineItem(id={bill_line_item_id}) has no parent_id; cannot upsert.")
        return

    # 2) Fetch the XeroBill
    xero_bill = db_ops.search_xero_bills(["id"], [parent_id])
    if not xero_bill or isinstance(xero_bill, list):
        logger.warning(f"No unique XeroBill found with id={parent_id}; aborting line item upsert.")
        return

    reference = xero_bill.get("xero_reference_number")
    if not reference:
        logger.warning(f"XeroBill(id={parent_id}) missing xero_reference_number; aborting line item upsert.")
        return

    # 3) Attempt to find the matching DetailItem
    project_number = bill_line_item.get("project_number")
    po_number = bill_line_item.get("po_number")
    detail_number = bill_line_item.get("detail_number")
    line_number = bill_line_item.get("line_number")

    detail_item = db_ops.search_detail_items(
        ["project_number", "po_number", "detail_number", "line_number"],
        [project_number, po_number, detail_number, line_number]
    )
    if isinstance(detail_item, list) and detail_item:
        detail_item = detail_item[0]
    else:
        detail_item = None

    # Build the Xero line item data. If detail_item is None, fallback to BillLineItem fields
    xero_line_item_data = {
        "Description": bill_line_item.get("description")
            or (detail_item.get("description") if detail_item else "No description"),
        "Quantity": float(bill_line_item.get("quantity") or 1.0),
        "UnitAmount": float(bill_line_item.get("unit_amount") or 0.0),
        "TaxType": "NONE",
        "AccountCode": str(bill_line_item.get("account_code") or "400"),
        # We'll insert Xero's "LineItemID" if we have it
    }

    # 4) Retrieve the ACCPAY invoice from Xero by reference
    existing_invoices = None  # Typically: xero_api.get_bills_by_reference(reference) or []
    if not existing_invoices:
        logger.warning(f"No ACCPAY invoice in Xero found with reference='{reference}'; cannot upsert line item.")
        return

    # Filter out the invoice that matches exactly our xero_bill.xero_reference_number
    matching_invoice = [inv for inv in existing_invoices if inv.get("Reference") == reference]
    if not matching_invoice:
        logger.warning(f"No ACCPAY invoice in Xero found with reference='{reference}'; cannot upsert line item.")
        return

    invoice_in_xero = matching_invoice[0]
    xero_invoice_id = invoice_in_xero.get("InvoiceID")
    if not xero_invoice_id:
        logger.warning(f"Invoice with reference='{reference}' has no InvoiceID; aborting line item upsert.")
        return

    # 5) Find or create the matching line item in the invoice
    xero_line_items = invoice_in_xero.get("LineItems", [])
    existing_line_item_id = bill_line_item.get("parent_xero_id")  # Our local reference to Xero's line item ID

    matched_line = None
    if existing_line_item_id:
        for li in xero_line_items:
            if li.get("LineItemID") == existing_line_item_id:
                matched_line = li
                break

    if matched_line:
        # ===============  Updating existing line item  ===============
        logger.info(f"Updating existing Xero line item (LineItemID={existing_line_item_id}) "
                    f"in invoice {xero_invoice_id}.")
        matched_line.update(xero_line_item_data)
    else:
        # ===============  Creating new line item  ===============
        logger.info(f"Creating new Xero line item in invoice {xero_invoice_id} (reference='{reference}').")
        xero_line_items.append(xero_line_item_data)

    # 6) Push the invoice update to Xero
    invoice_in_xero["LineItems"] = xero_line_items
    invoice_in_xero["InvoiceID"] = xero_invoice_id

    updated_invoice_list = xero_api._retry_on_unauthorized(
        xero_api.xero.invoices.save,
        invoice_in_xero
    )
    if not updated_invoice_list:
        logger.error(f"Failed to upsert line item for invoice (reference='{reference}') in Xero.")
        return

    # 7) Update local BillLineItem.parent_xero_id if we created a new line item
    updated_invoice_obj = updated_invoice_list[0].get("Invoices", [{}])[0]
    new_line_items = updated_invoice_obj.get("LineItems", [])

    if not existing_line_item_id:
        # Means we *just* created a new line item. We'll search for the best match:
        candidate = None
        for li in new_line_items:
            if (
                li.get("Description") == xero_line_item_data["Description"]
                and abs(float(li.get("Quantity", 0)) - float(xero_line_item_data["Quantity"])) < 0.0001
                and abs(float(li.get("UnitAmount", 0)) - float(xero_line_item_data["UnitAmount"])) < 0.0001
            ):
                candidate = li
        if candidate:
            new_id = candidate.get("LineItemID")
            if new_id:
                logger.info(f"New Xero BillLineItem(id={bill_line_item_id}).")

    logger.info(f"[XERO BillLineItem UPSERT] Done for BillLineItem ID={bill_line_item_id}.")


# ------------------------------------------------------------------------
# HELPER: Upsert a XeroBill in Xero
# ------------------------------------------------------------------------
def _upsert_xero_bill(xero_bill: Dict[str, Any]) -> None:
    """
    This helper function encapsulates the "create or update" logic for a XeroBill in Xero.

    1) Check if we have enough info (at least project_number, po_number, xero_reference_number, etc.).
    2) If there's an ACCPAY invoice in Xero with the same reference, see if we need to update it
       (compare local data vs. Xero). Update if different; skip if they're the same.
    3) Otherwise, create a new ACCPAY invoice in Xero using the local Bill's data.
    4) Update the local Bill with the link to Xero.
    """
    bill_id = xero_bill.get("id")
    if not bill_id:
        logger.warning("XeroBill dictionary has no id key. Aborting upsert.")
        return

    reference_key = xero_bill.get("xero_reference_number")
    if not reference_key:
        logger.warning(f"XeroBill(id={bill_id}) missing xero_reference_number. Aborting upsert.")
        return

    # Parse the reference to get project_number, po_number, detail_number
    pr_rk = parse_reference_key(reference_key)
    project_number = pr_rk["project_number"]
    po_number = pr_rk["po_number"]
    detail_number = pr_rk["detail_number"]

    if not project_number or not po_number:
        logger.warning(
            f"XeroBill(id={bill_id}) missing project_number or po_number; cannot look up PurchaseOrder."
        )
        return

    # 1) Look up the related PurchaseOrder by (project_number, po_number)
    po_record = db_ops.search_purchase_orders(["id", "contact_id"],
                                             [project_number, po_number])
    if not po_record or isinstance(po_record, list):
        logger.warning(f"Could not find a unique PurchaseOrder matching project_number={project_number}, "
                       f"po_number={po_number}. Aborting.")
        return

    contact_id = po_record.get("contact_id")
    if not contact_id:
        logger.warning(
            f"PurchaseOrder matching project_number={project_number}, po_number={po_number} has no contact_id; "
            f"skipping Xero creation."
        )
        return

    # 2) Retrieve the Contact
    contact_record = db_ops.search_contacts(["id"], [contact_id])
    if not contact_record or isinstance(contact_record, list):
        logger.warning(f"No unique Contact found with id={contact_id}. Aborting Xero creation.")
        return

    # Example fields; adapt as needed
    contact_name = contact_record.get("name", "Unknown Vendor")
    contact_email = contact_record.get("email", "")
    contact_phone = contact_record.get("phone", "")
    tax_number = contact_record.get("tax_number", "")
    address_line_1 = contact_record.get("address_line_1", "")
    address_line_2 = contact_record.get("address_line_2", "")
    postalcode = contact_record.get("zip", "")
    city = contact_record.get("city", "")
    country = contact_record.get("country", "")

    # 3) Get the Bill detail items (NOT line items) for date fields
    #    Query by project_number, po_number, and detail_number
    detail_items = db_ops.search_detail_items(
        ["project_number", "po_number", "detail_number"],
        [project_number, po_number, detail_number]
    )
    if not detail_items:
        detail_items = []

    transaction_dates = []
    due_dates = []
    for detail in detail_items:
        if detail.get("transaction_date"):
            transaction_dates.append(detail["transaction_date"])
        if detail.get("due_date"):
            due_dates.append(detail["due_date"])

    earliest_transaction_date = min(transaction_dates) if transaction_dates else None
    latest_due_date = max(due_dates) if due_dates else None

    # Build the "base" invoice data structure (no line items yet)
    new_invoice_data = {
        "Type": "ACCPAY",
        "Contact": {
            "Name": contact_name,
            "EmailAddress": contact_email,
            "Phones": [
                {
                    "PhoneType": "DEFAULT",
                    "PhoneNumber": contact_phone
                }
            ],
            "TaxNumber": tax_number,
            "Addresses": [
                {
                    "AddressType": "POBOX",
                    "AddressLine1": address_line_1,
                    "AddressLine2": address_line_2,
                    "City": city,
                    "PostalCode": postalcode,
                    "Country": country
                }
            ]
        },
        "LineItems": [],
        "InvoiceNumber": reference_key,
        "Status": "DRAFT"
    }
    if earliest_transaction_date:
        new_invoice_data["Date"] = earliest_transaction_date
    if latest_due_date:
        new_invoice_data["DueDate"] = latest_due_date

    # 4) Check if there's an existing invoice in Xero with this reference
    existing_invoice_list = xero_api.get_bills_by_reference(
        project_number=project_number,
        po_number=po_number,
        detail_number=detail_number
    ) or []
    matching_invoice = [
        inv for inv in existing_invoice_list
        if inv.get("Reference") == reference_key
    ]

    if matching_invoice:
        # We have an existing ACCPAY invoice - see if it differs
        existing_invoice = matching_invoice[0]
        xero_invoice_id = existing_invoice.get("InvoiceID")
        if not xero_invoice_id:
            logger.warning(
                f"Existing invoice with reference={reference_key} has no InvoiceID in Xero. Cannot update."
            )
            return

        # Compare data
        needs_update = _compare_xero_invoice_data(existing_invoice, new_invoice_data)
        if not needs_update:
            logger.info(
                f"No changes found in Xero invoice (reference='{reference_key}'). Skipping update."
            )
        else:
            # Update the Xero invoice
            logger.info(
                f"Changes detected. Updating existing ACCPAY invoice in Xero (reference='{reference_key}')."
            )
            update_data = dict(new_invoice_data)
            update_data["InvoiceID"] = xero_invoice_id  # Must provide to update

            updated_invoice_list = xero_api._retry_on_unauthorized(
                xero_api.xero.invoices.put,
                [update_data]
            )
            if not updated_invoice_list:
                logger.error(
                    f"Failed to update ACCPAY invoice in Xero (reference='{reference_key}'). Check logs."
                )
                return
            else:
                logger.info(
                    f"Updated ACCPAY invoice in Xero (reference='{reference_key}')."
                )

        # Store (or re-store) the link
        xero_link_url = f"https://go.xero.com/AccountsPayable/View.aspx?invoiceId={xero_invoice_id}"
        updated = db_ops.update_xero_bill(
            bill_id,
            xero_link=xero_link_url,
            transaction_date=earliest_transaction_date,
            due_date=latest_due_date
        )
        logger.info(
            f"Updated local XeroBill(id={bill_id}) with link: {xero_link_url}."
        )

    else:
        # If there's no existing invoice, create one
        logger.info(
            f"Creating ACCPAY invoice in Xero with reference='{reference_key}' and contact='{contact_name}'."
        )
        created_invoice_list = xero_api._retry_on_unauthorized(
            xero_api.xero.invoices.put,
            [new_invoice_data]
        )
        if not created_invoice_list:
            logger.error("Failed to create ACCPAY invoice in Xero. Check logs for details.")
            return

        # Parse new invoice response
        try:
            new_invoice = created_invoice_list[0]["Invoices"][0]
        except (IndexError, KeyError, TypeError):
            logger.error("Could not parse created ACCPAY invoice response. Check logs for details.")
            return

        xero_invoice_id = new_invoice.get("InvoiceID")
        if not xero_invoice_id:
            logger.info("Newly created invoice has no InvoiceID. Cannot build link.")
            return

        xero_link_url = f"https://go.xero.com/AccountsPayable/View.aspx?invoiceId={xero_invoice_id}"
        updated = db_ops.update_xero_bill(
            bill_id,
            xero_link=xero_link_url,
            transaction_date=earliest_transaction_date,
            due_date=latest_due_date
        )
        if updated:
            logger.info(f"Stored Xero link on local XeroBill(id={bill_id}).")
        else:
            logger.warning(f"Failed to store Xero link in XeroBill(id={bill_id}).")


# ------------------------------------------------------------------------
# HELPER: Ensure a XeroBill has a xero_link
# ------------------------------------------------------------------------
def _ensure_xero_link_for_xero_bill(xero_bill: Dict[str, Any]) -> None:
    """
    If a XeroBill record has no `xero_link`, we look up an existing ACCPAY invoice in Xero
    by the `xero_reference_number` and update the local record if found.
    """
    bill_id = xero_bill["id"]
    existing_link = xero_bill.get("xero_link")
    if existing_link:
        logger.info(f"XeroBill(id={bill_id}) already has xero_link={existing_link}. Skipping.")
        return

    reference_key = xero_bill.get("xero_reference_number")
    if not reference_key:
        logger.warning(f"XeroBill(id={bill_id}) has no xero_reference_number; cannot link.")
        return

    logger.info(f"XeroBill(id={bill_id}) missing xero_link. Searching Xero by reference='{reference_key}'.")

    existing_invoice_list = xero_api.get_bills_by_reference(reference_key) or []
    matching_invoices = [
        inv for inv in existing_invoice_list
        if inv.get("Reference") == reference_key
    ]
    if not matching_invoices:
        logger.info(f"No Xero invoice found with Reference='{reference_key}'. Cannot update xero_link.")
        return

    xero_invoice_id = matching_invoices[0].get("InvoiceID")
    if not xero_invoice_id:
        logger.info("Matching invoice has no InvoiceID; cannot build link.")
        return

    xero_link_url = f"https://go.xero.com/AccountsPayable/View.aspx?invoiceId={xero_invoice_id}"
    updated = db_ops.update_xero_bill(bill_id, xero_link=xero_link_url)
    if updated:
        logger.info(f"Updated XeroBill(id={bill_id}) with xero_link='{xero_link_url}'.")
    else:
        logger.warning(f"Failed to store xero_link in XeroBill(id={bill_id}).")


# ------------------------------------------------------------------------
# TRIGGERS: Xero Bill
# ------------------------------------------------------------------------
def handle_xero_bill_create(bill_id: int) -> None:
    """
    Triggered when a new XeroBill record is created in the DB.
    We fetch the record once, then call `_upsert_xero_bill(...)`.
    """
    logger.info(f"[XERO BILL CREATE] bill_id={bill_id}")

    # 1) Fetch the new XeroBill from DB
    xero_bill = db_ops.search_xero_bills(["id"], [bill_id])
    if not xero_bill:
        logger.warning(f"Could not find new XeroBill with id={bill_id}. Bailing out.")
        return
    if isinstance(xero_bill, list):
        xero_bill = xero_bill[0]

    # 2) Upsert it in Xero
    _upsert_xero_bill(xero_bill)
    _ensure_xero_link_for_xero_bill(xero_bill)


def handle_xero_bill_update(bill_id: int) -> None:
    """
    Main XeroBill update trigger.
    - We fetch the record once
    - We call `_upsert_xero_bill(...)` to ensure data is correct in Xero
    - Then we ensure the xero_link if it's still missing
    """
    logger.info(f"[XERO BILL UPDATE] bill_id={bill_id}")

    # 1) Fetch the XeroBill record
    xero_bill = db_ops.search_xero_bills(["id"], [bill_id])
    if not xero_bill:
        logger.warning(f"Could not find XeroBill with id={bill_id}. Aborting trigger.")
        return
    if isinstance(xero_bill, list):
        xero_bill = xero_bill[0]

    # 2) Upsert to catch any changes from the DB side
    _upsert_xero_bill(xero_bill)
    _ensure_xero_link_for_xero_bill(xero_bill)

    logger.info(f"[XERO BILL UPDATE] Completed for bill_id={bill_id}")


def handle_xero_bill_delete(bill_id: int) -> None:
    logger.info(f"[XERO BILL DELETE] bill_id={bill_id}")
    # Possibly set a "DELETED" status or do some housekeeping
    pass


# ------------------------------------------------------------------------
# TRIGGERS: Bill Line Item
# ------------------------------------------------------------------------
def handle_xero_bill_line_item_create(bill_id: int) -> None:
    logger.info(f"[XERO BILL LINE ITEMS CREATE] bill_id={bill_id}")
    # Future logic for line-item creation in Xero
    pass


def handle_xero_bill_line_item_update(line_item_id: int) -> None:
    logger.info(f"[XERO BILL LINE ITEM UPDATE] line_item_id={line_item_id}")
    # Future logic for line-item update in Xero
    pass


def handle_xero_bill_line_item_delete(line_item_id: int) -> None:
    logger.info(f"[XERO BILL LINE ITEM DELETE] line_item_id={line_item_id}")
    # Future logic for line-item deletion in Xero
    pass


# ------------------------------------------------------------------------
# TRIGGERS: BillLineItem, SpendMoney, etc., left minimal for now
# ------------------------------------------------------------------------
def handle_bill_line_item_create(bill_line_item_id: int) -> None:
    logger.info(f"[BILL LINE ITEM CREATE] ID={bill_line_item_id}")
    pass


def handle_bill_line_item_update(bill_line_item_id: int) -> None:
    logger.info(f"[BILL LINE ITEM UPDATE] ID={bill_line_item_id}")
    pass


def handle_bill_line_item_delete(bill_line_item_id: int) -> None:
    logger.info(f"[BILL LINE ITEM DELETE] ID={bill_line_item_id}")
    pass


def handle_spend_money_create(spend_money_id: int) -> None:
    logger.info(f"[SPEND MONEY CREATE] ID={spend_money_id}")
    pass


def handle_spend_money_update(spend_money_id: int) -> None:
    logger.info(f"[SPEND MONEY UPDATE] ID={spend_money_id}")
    pass


def handle_spend_money_delete(spend_money_id: int) -> None:
    logger.info(f"[SPEND MONEY DELETE] ID={spend_money_id}")
    pass