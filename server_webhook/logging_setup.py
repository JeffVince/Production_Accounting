import logging
import os
from datetime import datetime


class PaddedFormatter(logging.Formatter):
    def __init__(self, fmt=None, datefmt=None, max_length=80):
        super().__init__(fmt, datefmt)
        self.max_length = max_length
        self.COLUMN_WIDTHS = {
            "time": 14,
            "levelname": 5,
            "filename": 16,
            "funcName": 14,
        }

    def pad_center(self, text, width):
        """Pad text to the specified width, centering it."""
        text = text[:width] if len(text) > width else text  # Truncate if too long
        return f"{text:^{width}}"

    def pad_left(self, text, width):
        """Pad text to the specified width, aligning it to the left."""
        text = text[:width] if len(text) > width else text  # Truncate if too long
        return f"{text:<{width}}"

    def format(self, record):
        # Map log levels to their corresponding icons
        level_icons = {
            "ERROR": "🌕",
            "WARNING": "🌗",
            "INFO": "🌑",
            "DEBUG": "🌒",
        }



        # Prepend the icon to the level name
        icon = level_icons.get(record.levelname, " ")
        record.levelname = f"{icon}{record.levelname}"

        # Center-align and pad each field dynamically
        record.asctime = self.pad_center(self.formatTime(record), self.COLUMN_WIDTHS["time"])
        record.levelname = self.pad_center(record.levelname, self.COLUMN_WIDTHS["levelname"])
        record.filename = self.pad_left(record.filename, self.COLUMN_WIDTHS["filename"])
        record.funcName = self.pad_left(record.funcName, self.COLUMN_WIDTHS["funcName"])

        # Truncate message if necessary
        if len(record.msg) > self.max_length:
            record.msg = record.msg[:self.max_length] + "..."
        return super().format(record)

    def formatTime(self, record, datefmt=None):
        """
        Override formatTime to include milliseconds.
        """
        from datetime import datetime  # Import locally
        ct = datetime.fromtimestamp(record.created)  # Access the `created` attribute from the record
        if datefmt:
            result = self.pad_center(f"[{ ct.strftime(datefmt)}]-[{int(record.msecs):03d}]",  self.COLUMN_WIDTHS["time"])
            return result
        else:
            result =  self.pad_center(f"[{ct.strftime('%M:%S')}].[{int(record.msecs):03d}]",  self.COLUMN_WIDTHS["time"])
            return result


def setup_logging(flask_app=None):
    """
    Create loggers for each module: budget, dropbox, invoice, monday, po_log, xero, database, and web_logger.
    Each logger writes to a separate file in ./logs, plus a StreamHandler for the console.
    """
    os.makedirs('./logs', exist_ok=True)
    folder_name = 'server_webhook'
    basepath = './' + folder_name + '/logs/'

    # Use the custom truncating formatter
    # Initialize the formatter
    formatter = PaddedFormatter(
        fmt="%(asctime)s [%(levelname)s] [%(filename)s] [%(funcName)s] %(message)s",
        datefmt="%M:%S"
    )
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)

    # Web logger setup for Flask
    if flask_app:
        web_logger = logging.getLogger('web_logger')
        web_logger.setLevel(logging.DEBUG)
        file_handler_web = logging.FileHandler(basepath + 'web.log')
        file_handler_web.setFormatter(formatter)
        web_logger.addHandler(file_handler_web)
        web_logger.addHandler(console_handler)
        web_logger.propagate = False
        # Attach Flask's logger to the web_logger
        flask_app.logger.handlers = []  # Remove default handlers
        flask_app.logger.addHandler(file_handler_web)
        flask_app.logger.addHandler(console_handler)
        flask_app.logger.setLevel(logging.DEBUG)

    # Admin logger
    admin_logger = logging.getLogger('admin_logger')
    admin_logger.setLevel(logging.DEBUG)
    file_handler_admin = logging.FileHandler(basepath + 'admin.log')
    file_handler_admin.setFormatter(formatter)
    admin_logger.addHandler(file_handler_admin)
    admin_logger.addHandler(console_handler)
    admin_logger.propagate = False

    # Budget logger
    budget_logger = logging.getLogger('budget_logger')
    budget_logger.setLevel(logging.DEBUG)
    file_handler_budget = logging.FileHandler(basepath + 'budget.log')
    file_handler_budget.setFormatter(formatter)
    budget_logger.addHandler(file_handler_budget)
    budget_logger.addHandler(console_handler)
    budget_logger.propagate = False

    # Dropbox logger
    dropbox_logger = logging.getLogger('dropbox_logger')
    dropbox_logger.setLevel(logging.DEBUG)
    file_handler_dropbox = logging.FileHandler(basepath + 'dropbox.log')
    file_handler_dropbox.setFormatter(formatter)
    dropbox_logger.addHandler(file_handler_dropbox)
    dropbox_logger.addHandler(console_handler)
    dropbox_logger.propagate = False

    # Invoice logger
    invoice_logger = logging.getLogger('invoice_logger')
    invoice_logger.setLevel(logging.DEBUG)
    file_handler_invoice = logging.FileHandler(basepath + 'invoice.log')
    file_handler_invoice.setFormatter(formatter)
    invoice_logger.addHandler(file_handler_invoice)
    invoice_logger.addHandler(console_handler)
    invoice_logger.propagate = False

    # Monday logger
    monday_logger = logging.getLogger('monday_logger')
    monday_logger.setLevel(logging.DEBUG)
    file_handler_monday = logging.FileHandler(basepath + 'monday.log')
    file_handler_monday.setFormatter(formatter)
    monday_logger.addHandler(file_handler_monday)
    monday_logger.addHandler(console_handler)
    monday_logger.propagate = False

    # PO log logger
    po_log_logger = logging.getLogger('po_log_logger')
    po_log_logger.setLevel(logging.DEBUG)
    file_handler_po = logging.FileHandler(basepath + 'po_log.log')
    file_handler_po.setFormatter(formatter)
    po_log_logger.addHandler(file_handler_po)
    po_log_logger.addHandler(console_handler)
    po_log_logger.propagate = False

    # Xero logger
    xero_logger = logging.getLogger('xero_logger')
    xero_logger.setLevel(logging.DEBUG)
    file_handler_xero = logging.FileHandler(basepath + 'xero.log')
    file_handler_xero.setFormatter(formatter)
    xero_logger.addHandler(file_handler_xero)
    xero_logger.addHandler(console_handler)
    xero_logger.propagate = False

    # Database logger
    db_logger = logging.getLogger('database_logger')
    db_logger.setLevel(logging.DEBUG)
    file_handler_db = logging.FileHandler(basepath + 'database.log')
    file_handler_db.setFormatter(formatter)
    db_logger.addHandler(file_handler_db)
    db_logger.addHandler(console_handler)
    db_logger.propagate = False

    logging.basicConfig(level=logging.INFO)

# Call the setup_logging function
setup_logging()

