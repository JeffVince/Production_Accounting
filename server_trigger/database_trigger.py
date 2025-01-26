"""
database_trigger.py

🔔 DB-Level Trigger Listener (Dedicated Server-Style)
=====================================================

This module polls a MySQL 'audit_log' table (populated by MySQL triggers)
to detect row changes and enqueues the appropriate Celery tasks whenever
a new row is found.

It now uses db_util.get_db_session() for database access, eliminating
the need to pass host, user, and password directly.

Usage:
------
  1. Run this as a standalone process: python database_trigger.py
  2. It will start a polling loop that listens for new rows in `audit_log`.
  3. For each new row, it calls the correct _enqueue_xyz function based
     on the (table_name, operation) combination.

IMPORTANT:
----------
 - The DB triggers themselves are defined in SQL (AFTER INSERT, UPDATE, DELETE),
   writing to an 'audit_log' table with columns: table_name, operation, record_id, ...
 - The actual “RTP” or business logic resides in the Celery tasks, not here.

"""

# region 🛠️ IMPORTS
import logging
import time
import signal
import sys
from sqlalchemy import text
from logging_setup import setup_logging
from utilities.config import Config
from server_celery.celery_tasks import (
    process_invoice_trigger, process_invoice_delete,
    process_detail_item_create, process_detail_item_update, process_detail_item_delete,
    process_purchase_order_create, process_purchase_order_update, process_purchase_order_delete,
    process_contact_create, process_contact_update, process_contact_delete,
    process_xero_bill_line_item_create, process_xero_bill_line_item_update, process_xero_bill_line_item_delete,
    process_bank_transaction_create, process_bank_transaction_update, process_bank_transaction_delete,
    process_account_code_create, process_account_code_update, process_account_code_delete,
    process_receipt_create, process_receipt_update, process_receipt_delete,
    process_spend_money_create, process_spend_money_update, process_spend_money_delete,
    process_tax_account_create, process_tax_account_update, process_tax_account_delete,
    process_xero_bill_create, process_xero_bill_delete, process_xero_bill_update, process_po_log_create
)
from db_util import get_db_session, initialize_database
# endregion

# region 🔧 SETUP LOGGING
setup_logging()
logger = logging.getLogger('admin_logger')
logger.setLevel(logging.DEBUG)
# endregion

# region 📦 TASK ROUTING
TASK_ROUTING = {
    ('invoice', 'INSERT'): lambda rid: process_invoice_trigger.delay(rid),
    ('invoice', 'UPDATE'): lambda rid: process_invoice_trigger.delay(rid),
    ('invoice', 'DELETE'): lambda rid: process_invoice_delete.delay(rid),
    ('detail_item', 'INSERT'): lambda rid: process_detail_item_create.delay(rid),
    ('detail_item', 'UPDATE'): lambda rid: process_detail_item_update.delay(rid),
    ('detail_item', 'DELETE'): lambda rid: process_detail_item_delete.delay(rid),
    ('purchase_order', 'INSERT'): lambda rid: process_purchase_order_create.delay(rid),
    ('purchase_order', 'UPDATE'): lambda rid: process_purchase_order_update.delay(rid),
    ('purchase_order', 'DELETE'): lambda rid: process_purchase_order_delete.delay(rid),
    ('contact', 'INSERT'): lambda rid: process_contact_create.delay(rid),
    ('contact', 'UPDATE'): lambda rid: process_contact_update.delay(rid),
    ('contact', 'DELETE'): lambda rid: process_contact_delete.delay(rid),
    ('xero_bill_line_item', 'INSERT'): lambda rid: process_xero_bill_line_item_create.delay(rid),
    ('xero_bill_line_item', 'UPDATE'): lambda rid: process_xero_bill_line_item_update.delay(rid),
    ('xero_bill_line_item', 'DELETE'): lambda rid: process_xero_bill_line_item_delete.delay(rid),
    ('bank_transaction', 'INSERT'): lambda rid: process_bank_transaction_create.delay(rid),
    ('bank_transaction', 'UPDATE'): lambda rid: process_bank_transaction_update.delay(rid),
    ('bank_transaction', 'DELETE'): lambda rid: process_bank_transaction_delete.delay(rid),
    ('account_code', 'INSERT'): lambda rid: process_account_code_create.delay(rid),
    ('account_code', 'UPDATE'): lambda rid: process_account_code_update.delay(rid),
    ('account_code', 'DELETE'): lambda rid: process_account_code_delete.delay(rid),
    ('receipt', 'INSERT'): lambda rid: process_receipt_create.delay(rid),
    ('receipt', 'UPDATE'): lambda rid: process_receipt_update.delay(rid),
    ('receipt', 'DELETE'): lambda rid: process_receipt_delete.delay(rid),
    ('spend_money', 'INSERT'): lambda rid: process_spend_money_create.delay(rid),
    ('spend_money', 'UPDATE'): lambda rid: process_spend_money_update.delay(rid),
    ('spend_money', 'DELETE'): lambda rid: process_spend_money_delete.delay(rid),
    ('tax_account', 'INSERT'): lambda rid: process_tax_account_create.delay(rid),
    ('tax_account', 'UPDATE'): lambda rid: process_tax_account_update.delay(rid),
    ('tax_account', 'DELETE'): lambda rid: process_tax_account_delete.delay(rid),
    ('xero_bill', 'INSERT'): lambda rid: process_xero_bill_create.delay(rid),
    ('xero_bill', 'UPDATE'): lambda rid: process_xero_bill_update.delay(rid),
    ('xero_bill', 'DELETE'): lambda rid: process_xero_bill_delete.delay(rid),
    ('po_log', 'CREATE'): lambda rid: process_po_log_create.delay(rid)
}
# endregion

# region 📡 POLLING FUNCTION
def poll_audit_log(poll_interval=5.0):
    """
    Continuously poll the `audit_log` table for new entries.
    For each new row, route it to the appropriate Celery task.
    Uses db_util.get_db_session() for all DB operations.
    """
    audit_log_debug_audit_limit = 368857

    logger.info('🚀 Starting audit_log polling loop as a dedicated server...')
    with get_db_session() as session:
        row = session.execute(text('SELECT IFNULL(MAX(id), 0) AS max_id FROM audit_log')).fetchone()
        last_processed_id = row.max_id if row.max_id > audit_log_debug_audit_limit else audit_log_debug_audit_limit
    logger.info(f'🔍 Initial last_processed_id={last_processed_id}')

    while True:
        try:
            with get_db_session() as session:
                query = text("""
                    SELECT al.id, t.name, al.operation, al.record_id
                    FROM audit_log al
                    JOIN sys_table t ON al.table_id = t.id
                    WHERE al.id > :last_id
                    ORDER BY al.id ASC
                """)
                results = session.execute(query, {'last_id': last_processed_id}).fetchall()

                for r in results:
                    audit_id = r.id
                    table_name = r.name
                    operation = r.operation
                    record_id = r.record_id

                    logger.debug(f'🆕 New row => audit_id={audit_id}, table={table_name}, operation={operation}, record_id={record_id}')

                    key = (table_name, operation)
                    if key in TASK_ROUTING:
                        try:
                            TASK_ROUTING[key](record_id)
                            logger.info(f'✅ Enqueued Celery task for {table_name} {operation}, record_id={record_id}')
                        except Exception as exc:
                            logger.exception(f'❌ Failed to enqueue Celery task for {table_name} {operation}, record_id={record_id}: {exc}')
                    else:
                        logger.warning(f'⚠️ No route for (table={table_name}, operation={operation}). Skipping...')

                    last_processed_id = max(last_processed_id, audit_id)
            time.sleep(poll_interval)
        except Exception as e:
            logger.exception(f'❗ Error polling audit_log: {e}')
            time.sleep(poll_interval)
# endregion

# region 🚨 SIGNAL HANDLER
def signal_handler(sig, frame):
    """
    Graceful shutdown on SIGINT or SIGTERM.
    """
    logger.info('🛑 Received shutdown signal. Stopping the listener...')
    sys.exit(0)
# endregion

# region 🏁 START LISTENER
def start_db_trigger_listener():
    """
    Call this function once at app startup in a dedicated thread or process.
    """
    poll_audit_log(poll_interval=2.0)
# endregion

# region 🎬 MAIN FUNCTION
def main():
    """
    Main function for running this script as a dedicated server.
    """
    logger.info('🏗️ Initializing DB Trigger Listener server...')
    config = Config()
    db_settings = config.get_database_settings(config.USE_LOCAL)
    initialize_database(db_settings['url'])
    logger.info('🗄️ Database initialized.')

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    start_db_trigger_listener()
# endregion

# region 🏃‍♂️ ENTRY POINT
if __name__ == '__main__':
    main()
# endregion