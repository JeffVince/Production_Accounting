#!/usr/bin/env python

"""
Migration script: MySQL (old) => Postgres (new).
Drops all PG tables first, then re-creates them, then migrates data.
Run from PyCharm or CLI. Adjust user/pw/host/DB names as needed.
"""

import sys
import logging
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

# ---------------------------------------------------
# 1) Import your MySQL (old) models
# ---------------------------------------------------
from database_pg.models_pg import (
    Base as OldBase,
    Contact as OldContact,
    User as OldUser,
    BudgetMap as OldBudgetMap,
    AccountCode as OldAccountCode,
    TaxLedger as OldTaxLedger,
    TaxAccount as OldTaxAccount,
    AuditLog as OldAuditLog,
    BankTransaction as OldBankTransaction,
    XeroBill as OldXeroBill,
    XeroBillLineItem as OldXeroBillLineItem,
    PoLog as OldPoLog,
    DetailItem as OldDetailItem,
    Project as OldProject,
    PurchaseOrder as OldPurchaseOrder,
    Invoice as OldInvoice,
    Receipt as OldReceipt,
    SpendMoney as OldSpendMoney,
    SysTable as OldSysTable,
    TaxForm as OldTaxForm,
    Dropboxfolder as OldDropboxfolder
)

# ---------------------------------------------------
# 2) Import your Postgres (new) models
# ---------------------------------------------------
from database_pg.models_pg import (
    Base as NewBase,
    Contact as NewContact,
    User as NewUser,
    BudgetMap as NewBudgetMap,
    AccountCode as NewAccountCode,
    TaxLedger as NewTaxLedger,
    TaxAccount as NewTaxAccount,
    AuditLog as NewAuditLog,
    BankTransaction as NewBankTransaction,
    XeroBill as NewXeroBill,
    XeroBillLineItem as NewXeroBillLineItem,
    PoLog as NewPoLog,
    DetailItem as NewDetailItem,
    Project as NewProject,
    PurchaseOrder as NewPurchaseOrder,
    Invoice as NewInvoice,
    Receipt as NewReceipt,
    SpendMoney as NewSpendMoney,
    SysTable as NewSysTable,
    TaxForm as NewTaxForm,
    Dropboxfolder as NewDropboxfolder
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("migration")

def main():
    """
    Migrate data from local MySQL to DigitalOcean Postgres,
    dropping all PG tables first for a fresh start.
    """
    # 1) Define your connection URIs
    mysql_uri = "mysql+pymysql://root:z //55gohi@localhost:3306/virtual_pm"
    postgres_uri = (
        "postgresql://olivine-db-main-do-user-11898912-0.l.db.ondigitalocean.com:25060/"
        "defaultdb?sslmode=require&user=doadmin&password=AVNS_wxTAj-nNJrpMakwsi--"
    )

    # Create engines/sessions
    mysql_engine = create_engine(mysql_uri, echo=False)
    mysql_session = sessionmaker(bind=mysql_engine)()

    pg_engine = create_engine(postgres_uri, echo=False)
    pg_session = sessionmaker(bind=pg_engine)()

    logger.info("Dropping all existing tables in Postgres (DROP SCHEMA CASCADE)...")
    with pg_engine.connect() as conn:
        conn.execute(text("DROP SCHEMA IF EXISTS public CASCADE;"))
        conn.execute(text("CREATE SCHEMA public;"))
        conn.execute(text("SET search_path TO public;"))

    logger.info("Re-creating Postgres tables from model definitions...")
    NewBase.metadata.create_all(pg_engine)


    try:
        # MIGRATION ORDER
        logger.info("Starting data migration...")

        # 1) CONTACT
        logger.info("Migrating Contact...")
        old_contacts = mysql_session.query(OldContact).all()
        for o in old_contacts:
            n = NewContact(
                id=o.id,
                name=o.name,
                vendor_status=o.vendor_status,
                payment_details=o.payment_details,
                vendor_type=o.vendor_type,
                email=o.email,
                phone=o.phone,
                address_line_1=o.address_line_1,
                address_line_2=o.address_line_2,
                city=o.city,
                zip=o.zip,
                region=o.region,
                country=o.country,
                tax_type=o.tax_type,
                tax_number=o.tax_number,
                pulse_id=o.pulse_id,
                xero_id=o.xero_id,
                tax_form_id=o.tax_form_id,
                created_at=o.created_at,
                updated_at=o.updated_at
            )
            pg_session.add(n)
        pg_session.commit()

        # 2) USER
        logger.info("Migrating User...")
        old_users = mysql_session.query(OldUser).all()
        for o in old_users:
            n = NewUser(
                id=o.id,
                username=o.username,
                contact_id=o.contact_id,
                created_at=o.created_at,
                updated_at=o.updated_at
            )
            pg_session.add(n)
        pg_session.commit()

        # 3) TAX LEDGER
        logger.info("Migrating TaxLedger...")
        old_ledgers = mysql_session.query(OldTaxLedger).all()
        for o in old_ledgers:
            n = NewTaxLedger(
                id=o.id,
                name=o.name,
                user_id=o.user_id,
                created_at=o.created_at,
                updated_at=o.updated_at
            )
            pg_session.add(n)
        pg_session.commit()

        # 4) TAX ACCOUNT
        logger.info("Migrating TaxAccount...")
        old_tax_accounts = mysql_session.query(OldTaxAccount).all()
        for o in old_tax_accounts:
            n = NewTaxAccount(
                id=o.id,
                tax_code=o.tax_code,
                description=o.description,
                tax_ledger_id=o.tax_ledger_id,
                created_at=o.created_at,
                updated_at=o.updated_at
            )
            pg_session.add(n)
        pg_session.commit()

        # 5) BUDGET MAP
        logger.info("Migrating BudgetMap...")
        old_maps = mysql_session.query(OldBudgetMap).all()
        for o in old_maps:
            n = NewBudgetMap(
                id=o.id,
                map_name=o.map_name,
                user_id=o.user_id,
                created_at=o.created_at,
                updated_at=o.updated_at
            )
            pg_session.add(n)
        pg_session.commit()

        # 6) ACCOUNT CODE
        logger.info("Migrating AccountCode...")
        old_ac = mysql_session.query(OldAccountCode).all()
        for o in old_ac:
            n = NewAccountCode(
                id=o.id,
                code=o.code,
                budget_map_id=o.budget_map_id,
                tax_id=o.tax_id,
                account_description=o.account_description,
                created_at=o.created_at,
                updated_at=o.updated_at
            )
            pg_session.add(n)
        pg_session.commit()

        # 7) PROJECT
        logger.info("Migrating Project...")
        old_projects = mysql_session.query(OldProject).all()
        for o in old_projects:
            n = NewProject(
                id=o.id,
                user_id=o.user_id,
                project_number=o.project_number,
                name=o.name,
                status=o.status,
                tax_ledger=o.tax_ledger,
                budget_map_id=o.budget_map_id,
                created_at=o.created_at,
                updated_at=o.updated_at
            )
            pg_session.add(n)
        pg_session.commit()

        # 8) PURCHASE ORDER
        logger.info("Migrating PurchaseOrder...")
        old_pos = mysql_session.query(OldPurchaseOrder).all()
        for o in old_pos:
            n = NewPurchaseOrder(
                id=o.id,
                project_number=o.project_number,
                po_number=o.po_number,
                vendor_name=o.vendor_name,
                description=o.description,
                po_type=o.po_type,
                producer=o.producer,
                pulse_id=o.pulse_id,
                folder_link=o.folder_link,
                contact_id=o.contact_id,
                project_id=o.project_id,
                created_at=o.created_at,
                updated_at=o.updated_at
            )
            pg_session.add(n)
        pg_session.commit()

        # 9) DETAIL ITEM
        logger.info("Migrating DetailItem...")
        old_details = mysql_session.query(OldDetailItem).all()
        for o in old_details:
            n = NewDetailItem(
                id=o.id,
                project_number=o.project_number,
                po_number=o.po_number,
                detail_number=o.detail_number,
                line_number=o.line_number,
                account_code=o.account_code,
                vendor=o.vendor,
                payment_type=o.payment_type,
                state=o.state,
                description=o.description,
                transaction_date=o.transaction_date,
                due_date=o.due_date,
                rate=o.rate,
                quantity=o.quantity,
                ot=o.ot,
                fringes=o.fringes,
                # We assume the old detail item might have a sub_total we want to copy
                sub_total=o.sub_total,  # It's now a normal column in Postgres
                pulse_id=o.pulse_id,
                xero_id=o.xero_id,
                parent_pulse_id=o.parent_pulse_id,
                created_at=o.created_at,
                updated_at=o.updated_at
            )
            pg_session.add(n)
        pg_session.commit()

        # 10) SPEND MONEY
        logger.info("Migrating SpendMoney...")
        old_spend = mysql_session.query(OldSpendMoney).all()
        for o in old_spend:
            # If old MySQL had a generated reference, you can replicate or just copy it
            # For example:
            # computed_ref = f"{o.project_number:04d}_{o.po_number:02d}_{o.detail_number:02d}_{o.line_number:02d}"
            # Or just use what's in o.xero_spend_money_reference_number, if it existed
            n = NewSpendMoney(
                id=o.id,
                project_number=o.project_number,
                po_number=o.po_number,
                detail_number=o.detail_number,
                line_number=o.line_number,
                description=o.description,
                contact_id=o.contact_id,
                date=o.date,
                xero_spend_money_id=o.xero_spend_money_id,
                # no computed column in Postgres, just store the original
                xero_link=o.xero_link,
                amount=o.amount,
                tax_code=o.tax_code,
                state=o.state,
                created_at=o.created_at,
                updated_at=o.updated_at
            )
            pg_session.add(n)
        pg_session.commit()

        # # 11) BANK TRANSACTION
        # logger.info("Migrating BankTransaction...")
        # old_bt = mysql_session.query(OldBankTransaction).all()
        # for o in old_bt:
        #     n = NewBankTransaction(
        #         id=o.id,
        #         mercury_transaction_id=o.mercury_transaction_id,
        #         state=o.state,
        #         xero_bill_id=o.xero_bill_id,
        #         xero_spend_money_id=o.xero_spend_money_id,
        #
        #         amount=o.amount,
        #         created_at=o.created_at,
        #         updated_at=o.updated_at
        #     )
        #     pg_session.add(n)
        # pg_session.commit()

        # 12) INVOICE
        logger.info("Migrating Invoice...")
        old_invoices = mysql_session.query(OldInvoice).all()
        for o in old_invoices:
            n = NewInvoice(
                id=o.id,
                project_number=o.project_number,
                po_number=o.po_number,
                invoice_number=o.invoice_number,
                term=o.term,
                total=o.total,
                status=o.status,
                transaction_date=o.transaction_date,
                file_link=o.file_link,
                created_at=o.created_at,
                updated_at=o.updated_at
            )
            pg_session.add(n)
        pg_session.commit()

        # 13) RECEIPT
        logger.info("Migrating Receipt...")
        old_receipts = mysql_session.query(OldReceipt).all()
        for o in old_receipts:
            n = NewReceipt(
                id=o.id,
                project_number=o.project_number,
                po_number=o.po_number,
                detail_number=o.detail_number,
                line_number=o.line_number,
                receipt_description=o.receipt_description,
                total=o.total,
                status=o.status,
                purchase_date=o.purchase_date,
                dropbox_path=o.dropbox_path,
                file_link=o.file_link,
                created_at=o.created_at,
                updated_at=o.updated_at,
                spend_money_id=o.spend_money_id
            )
            pg_session.add(n)
        pg_session.commit()

        # 14) PO LOG
        logger.info("Migrating PoLog...")
        old_po_logs = mysql_session.query(OldPoLog).all()
        for o in old_po_logs:
            n = NewPoLog(
                id=o.id,
                project_number=o.project_number,
                filename=o.filename,
                db_path=o.db_path,
                status=o.status,
                created_at=o.created_at,
                updated_at=o.updated_at
            )
            pg_session.add(n)
        pg_session.commit()

        # 15) XERO BILL
        logger.info("Migrating XeroBill...")

        old_xb = mysql_session.query(OldXeroBill).all()
        for o in old_xb:
            # If you want to replicate the old lpad logic, do it here in Python:
            if o.project_number is not None and o.po_number is not None and o.detail_number is not None:
                generated_ref = f"{o.project_number:04d}_{o.po_number:02d}_{o.detail_number:02d}"
            else:
                generated_ref = o.xero_reference_number or None

            n = NewXeroBill(
                id=o.id,
                state=o.state,
                project_number=o.project_number,
                po_number=o.po_number,
                detail_number=o.detail_number,
                transaction_date=o.transaction_date,
                due_date=o.due_date,
                contact_xero_id=o.contact_xero_id,
                xero_reference_number=generated_ref,
                xero_id=o.xero_id,
                xero_link=o.xero_link,
                updated_at=o.updated_at,
                created_at=o.created_at
            )
            pg_session.add(n)
        pg_session.commit()

        # 16) XERO BILL LINE ITEM
        logger.info("Migrating XeroBillLineItem...")
        old_xbl = mysql_session.query(OldXeroBillLineItem).all()
        for o in old_xbl:
            n = NewXeroBillLineItem(
                id=o.id,
                project_number=o.project_number,
                po_number=o.po_number,
                detail_number=o.detail_number,
                line_number=o.line_number,
                description=o.description,
                transaction_date=o.transaction_date,
                due_date=o.due_date,
                quantity=o.quantity,
                unit_amount=o.unit_amount,
                line_amount=o.line_amount,
                tax_code=o.tax_code,
                parent_id=o.parent_id,
                xero_bill_line_id=o.xero_bill_line_id,
                parent_xero_id=o.parent_xero_id,
                updated_at=o.updated_at,
                created_at=o.created_at
            )
            pg_session.add(n)
        pg_session.commit()

        # 17) AUDIT LOG
        logger.info("Migrating AuditLog...")
        old_al = mysql_session.query(OldAuditLog).all()
        for o in old_al:
            n = NewAuditLog(
                id=o.id,
                table_id=o.table_id,
                operation=o.operation,
                record_id=o.record_id,
                message=o.message,
                created_at=o.created_at
            )
            pg_session.add(n)
        pg_session.commit()

        # 18) SYS TABLE
        logger.info("Migrating SysTable...")
        old_sys = mysql_session.query(OldSysTable).all()
        for o in old_sys:
            n = NewSysTable(
                id=o.id,
                name=o.name,
                type=o.type,
                integration_name=o.integration_name,
                integration_type=o.integration_type,
                integration_connection=o.integration_connection,
                created_at=o.created_at,
                updated_at=o.updated_at
            )
            pg_session.add(n)
        pg_session.commit()

        # 19) TAX FORM
        logger.info("Migrating TaxForm...")
        old_tf = mysql_session.query(OldTaxForm).all()
        for o in old_tf:
            n = NewTaxForm(
                id=o.id,
                type=o.type,
                status=o.status,
                entity_name=o.entity_name,
                filename=o.filename,
                db_path=o.db_path,
                tax_form_link=o.tax_form_link,
                created_at=o.created_at,
                updated_at=o.updated_at
            )
            pg_session.add(n)
        pg_session.commit()

        # 20) DROPBOXFOLDER
        logger.info("Migrating Dropboxfolder...")
        old_dbf = mysql_session.query(OldDropboxfolder).all()
        for o in old_dbf:
            n = NewDropboxfolder(
                id=o.id,
                project_number=o.project_number,
                po_number=o.po_number,
                vendor_name=o.vendor_name,
                dropbox_path=o.dropbox_path,
                share_link=o.share_link
            )
            pg_session.add(n)
        pg_session.commit()

        logger.info("All data migrated successfully (tables dropped and recreated)!")

    except Exception as e:
        logger.exception("Migration failed, rolling back.")
        pg_session.rollback()
        sys.exit(1)
    finally:
        mysql_session.close()
        pg_session.close()


if __name__ == "__main__":
    main()