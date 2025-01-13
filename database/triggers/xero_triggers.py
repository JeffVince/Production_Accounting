# xero_triggers.py

import logging
from typing import Optional, Dict, Any
from database.database_util import DatabaseOperations
from xero_api import xero_api

db_ops = DatabaseOperations()
logger = logging.getLogger("app_logger")

def handle_bill_line_item_create(bill_line_item_id: int) -> None:
    logger.info(f"[BILL LINE ITEM CREATE] ID={bill_line_item_id}")
    # Actual BillLineItem creation logic goes here...
    # e.g.: db_ops.do_something_with(bill_line_item_id)
    pass

def handle_bill_line_item_update(bill_line_item_id: int) -> None:
    logger.info(f"[BILL LINE ITEM UPDATE] ID={bill_line_item_id}")
    # ...
    pass

def handle_bill_line_item_delete(bill_line_item_id: int) -> None:
    logger.info(f"[BILL LINE ITEM DELETE] ID={bill_line_item_id}")
    # ...
    pass


def handle_spend_money_create(spend_money_id: int) -> None:
    logger.info(f"[SPEND MONEY CREATE] ID={spend_money_id}")
    # ...
    pass

def handle_spend_money_update(spend_money_id: int) -> None:
    logger.info(f"[SPEND MONEY UPDATE] ID={spend_money_id}")
    # ...
    pass

def handle_spend_money_delete(spend_money_id: int) -> None:
    logger.info(f"[SPEND MONEY DELETE] ID={spend_money_id}")
    # ...
    pass


def handle_xero_bill_create(bill_id: int) -> None:
    """
    Triggered when a new XeroBill record is created in the DB.
    We'll retrieve-or-create an ACCPAY invoice in Xero using the Bill's contact data,
    then store the returned link in xero_bill.xero_link.
    """
    logger.info(f"[XERO BILL CREATE] bill_id={bill_id}")

    # 1) Look up the newly created XeroBill in the DB
    xero_bill = db_ops.search_xero_bills(["id"], [bill_id])
    if not xero_bill:
        logger.warning(f"Could not find new XeroBill with id={bill_id}. Bailing out.")
        return
    if isinstance(xero_bill, list):
        xero_bill = xero_bill[0]

    reference_key = xero_bill.get("xero_reference_number")
    po_id = xero_bill.get("po_id")
    if not po_id:
        logger.warning("XeroBill is missing po_id, cannot look up the PurchaseOrder or Contact.")
        return

    # 2) Look up the related PurchaseOrder (for contact_id, etc.)
    po_record = db_ops.search_purchase_orders(["id"], [po_id])
    if not po_record or isinstance(po_record, list):
        logger.warning(f"Could not find a unique PurchaseOrder for po_id={po_id}. Aborting.")
        return

    contact_id = po_record.get("contact_id")
    if not contact_id:
        logger.warning(f"PurchaseOrder (id={po_id}) has no contact_id; skipping Xero creation.")
        return

    # 3) Retrieve the Contact record to get name, email, phone, etc.
    contact_record = db_ops.search_contacts(["id"], [contact_id])
    if not contact_record or isinstance(contact_record, list):
        logger.warning(f"No unique Contact found with id={contact_id}. Aborting Xero creation.")
        return

    # Example fields; adapt to match your actual DB columns
    contact_name = contact_record.get("name", "Unknown Vendor")
    contact_email = contact_record.get("email", "")
    contact_phone = contact_record.get("phone", "")
    tax_number = contact_record.get("tax_number", "")
    address_line_1 = contact_record.get("address_line_1", "")
    address_line_2 = contact_record.get("address_line_2", "")
    postalcode = contact_record.get("zip", "")
    city = contact_record.get("city", "")
    country = contact_record.get("country", "")

    # 4) FIRST, see if Xero already has this bill (ACCPAY invoice) by reference
    existing_invoices = xero_api.get_bills_by_reference(detail_number=None)  # or direct method if you have one
    # If you have a direct search by reference, e.g. xero_api.get_bills_by_reference(reference_key),
    # you can do that. The snippet below is just a conceptual approach:

    matching_invoices = [
        inv for inv in existing_invoices
        if inv.get("Reference") == reference_key
    ] if existing_invoices else []

    if matching_invoices:
        # Already exists in Xero => skip creation or optionally do an update
        logger.info(
            f"Found existing ACCPAY invoice in Xero with reference='{reference_key}'. "
            f"Will store link if available and skip creation."
        )
        invoice_obj = matching_invoices[0]
        xero_link_url = invoice_obj.get("url", "")

        if xero_link_url:
            db_ops.update_xero_bill(xero_bill["id"], xero_link=xero_link_url)
            logger.info(f"Updated local XeroBill(id={xero_bill['id']}) with existing Xero link: {xero_link_url}.")
        else:
            logger.info("No 'OnlineInvoiceUrl' on the existing invoice. Not storing a link.")
        return

    # 5) Build the invoice object for Xero (no line items initially)
    invoice_data = {
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
        "Reference": reference_key,
        "Status": "DRAFT"
    }

    logger.info(
        f"Creating ACCPAY invoice in Xero with reference='{reference_key}' and contact='{contact_name}'."
    )

    # 6) Create the invoice in Xero using your PyXero-based approach
    created_invoice_list = xero_api._retry_on_unauthorized(
        xero_api.xero.invoices.put,
        [invoice_data]
    )
    if not created_invoice_list:
        logger.error("Failed to create ACCPAY invoice in Xero. Check logs for details.")
        return

    created_invoice = created_invoice_list[0]  # PyXero typically returns a list with one invoice
    # Some Xero versions provide an 'OnlineInvoiceUrl' or 'Url' for a direct link.
    xero_link_url = created_invoice.get("OnlineInvoiceUrl") or ""

    if not xero_link_url:
        logger.warning("No 'OnlineInvoiceUrl' returned from Xero. Cannot store link.")
        return

    logger.info(f"Xero invoice created at link: {xero_link_url}")

    # 7) Update our local XeroBill with the Xero link
    xero_bill_id = xero_bill.get("id")
    updated = db_ops.update_xero_bill(xero_bill_id, xero_link=xero_link_url)
    if updated:
        logger.info(f"Stored Xero link on local XeroBill(id={xero_bill_id}).")
        # (Optional) Mark a 'just_synced_at' timestamp on this record to skip immediate webhooks
        # db_ops.update_xero_bill(xero_bill_id, just_synced_at=datetime.utcnow())
    else:
        logger.warning(f"Failed to store Xero link in XeroBill(id={xero_bill_id}).")

def handle_xero_bill_update(bill_id: int) -> None:
    logger.info(f"[XERO BILL UPDATE] bill_id={bill_id}")
    # ...
    pass

def handle_xero_bill_delete(bill_id: int) -> None:
    logger.info(f"[XERO BILL DELETE] bill_id={bill_id}")
    # ...
    pass


def handle_xero_bill_line_item_create(bill_id: int) -> None:
    logger.info(f"[XERO BILL LINE ITEMS CREATE] bill_id={bill_id}")
    # ...
    pass

def handle_xero_bill_line_item_update(line_item_id: int) -> None:
    logger.info(f"[XERO BILL LINE ITEM UPDATE] line_item_id={line_item_id}")
    # ...
    pass

def handle_xero_bill_line_item_delete(line_item_id: int) -> None:
    logger.info(f"[XERO BILL LINE ITEM DELETE] line_item_id={line_item_id}")
    # ...
    pass