# server_celery/logging_setup.py

import logging
import os


def setup_logging():
    """
    Create loggers for each module: budget, dropbox, invoice, monday, po_log, xero, database.
    Each logger writes to a separate file in ./logs, plus a StreamHandler for the console.
    """
    # Ensure the logs folder exists
    os.makedirs("./logs", exist_ok=True)
    folder_name = "server_celery"

    basepath = "./" + folder_name + "/logs/"
    # Common log format
    log_format = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"

    formatter = logging.Formatter(log_format)

    # StreamHandler for console (all logs)
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)

    # ---------------------------------------------------------
    #  Logger for admin
    # ---------------------------------------------------------
    admin_logger = logging.getLogger("admin_logger")
    admin_logger.setLevel(logging.DEBUG)
    file_handler_admin = logging.FileHandler("./logs/admin.log")
    file_handler_admin.setFormatter(formatter)
    admin_logger.addHandler(file_handler_admin)
    admin_logger.addHandler(console_handler)
    admin_logger.propagate = False

    # ---------------------------------------------------------
    #  Logger for budget
    # ---------------------------------------------------------
    budget_logger = logging.getLogger("budget_logger")
    budget_logger.setLevel(logging.DEBUG)
    file_handler_budget = logging.FileHandler("./logs/budget.log")
    file_handler_budget.setFormatter(formatter)
    budget_logger.addHandler(file_handler_budget)
    budget_logger.addHandler(console_handler)
    budget_logger.propagate = False

    # ---------------------------------------------------------
    #  Logger for dropbox
    # ---------------------------------------------------------
    dropbox_logger = logging.getLogger("dropbox_logger")
    dropbox_logger.setLevel(logging.DEBUG)
    file_handler_dropbox = logging.FileHandler("./logs/dropbox.log")
    file_handler_dropbox.setFormatter(formatter)
    dropbox_logger.addHandler(file_handler_dropbox)
    dropbox_logger.addHandler(console_handler)
    dropbox_logger.propagate = False

    # ---------------------------------------------------------
    #  Logger for invoice
    # ---------------------------------------------------------
    invoice_logger = logging.getLogger("invoice_logger")
    invoice_logger.setLevel(logging.DEBUG)
    file_handler_invoice = logging.FileHandler("./logs/invoice.log")
    file_handler_invoice.setFormatter(formatter)
    invoice_logger.addHandler(file_handler_invoice)
    invoice_logger.addHandler(console_handler)
    invoice_logger.propagate = False

    # ---------------------------------------------------------
    #  Logger for monday
    # ---------------------------------------------------------
    monday_logger = logging.getLogger("monday_logger")
    monday_logger.setLevel(logging.DEBUG)
    file_handler_monday = logging.FileHandler("./logs/monday.log")
    file_handler_monday.setFormatter(formatter)
    monday_logger.addHandler(file_handler_monday)
    monday_logger.addHandler(console_handler)
    monday_logger.propagate = False

    # ---------------------------------------------------------
    #  Logger for po_log
    # ---------------------------------------------------------
    po_log_logger = logging.getLogger("po_log_logger")
    po_log_logger.setLevel(logging.DEBUG)
    file_handler_po = logging.FileHandler("./logs/po_log.log")
    file_handler_po.setFormatter(formatter)
    po_log_logger.addHandler(file_handler_po)
    po_log_logger.addHandler(console_handler)
    po_log_logger.propagate = False

    # ---------------------------------------------------------
    #  Logger for xero
    # ---------------------------------------------------------
    xero_logger = logging.getLogger("xero_logger")
    xero_logger.setLevel(logging.DEBUG)
    file_handler_xero = logging.FileHandler("./logs/xero.log")
    file_handler_xero.setFormatter(formatter)
    xero_logger.addHandler(file_handler_xero)
    xero_logger.addHandler(console_handler)
    xero_logger.propagate = False

    # ---------------------------------------------------------
    #  Logger for database
    # ---------------------------------------------------------
    db_logger = logging.getLogger("database_logger")
    db_logger.setLevel(logging.DEBUG)
    file_handler_db = logging.FileHandler("./logs/database.log")
    file_handler_db.setFormatter(formatter)
    db_logger.addHandler(file_handler_db)
    db_logger.addHandler(console_handler)
    db_logger.propagate = False

    # Optional: set root logger if you want
    logging.basicConfig(level=logging.INFO)