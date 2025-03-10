import logging
import os
from datetime import datetime

def clear_log_files():
    """
    Opens each log file in write mode to truncate (empty) it.
    Call this function whenever you need to clear your log files.
    """
    basepath = './server_webhook/logs/'
    log_files = [
        'admin.log',
        'budget.log',
        'dropbox.log',
        'invoice.log',
        'monday.log',
        'po_log.log',
        'xero.log',
        'database.log',
        'web.log'
    ]
    for log_file in log_files:
        file_path = os.path.join(basepath, log_file)
        # Only clear the file if it exists
        if os.path.exists(file_path):
            with open(file_path, 'w'):
                pass  # Opening in 'w' mode truncates the file

class PaddedFormatter(logging.Formatter):
    def __init__(self, fmt=None, datefmt=None, max_length=200):
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
        icon = level_icons.get(record.levelname, " ")
        record.levelname = f"{icon}{record.levelname}"

        # Center-align and pad each field dynamically
        record.asctime  = self.pad_center(self.formatTime(record), self.COLUMN_WIDTHS["time"])
        record.levelname = self.pad_center(record.levelname,    self.COLUMN_WIDTHS["levelname"])
        record.filename = self.pad_left(record.filename,       self.COLUMN_WIDTHS["filename"])
        record.funcName = self.pad_left(record.funcName,       self.COLUMN_WIDTHS["funcName"])

        # Truncate message if necessary
        if len(record.msg) > self.max_length:
            record.msg = record.msg[:self.max_length] + "..."
        return super().format(record)

    def formatTime(self, record, datefmt=None):
        ct = datetime.fromtimestamp(record.created)
        if datefmt:
            result = f"[{ ct.strftime(datefmt)}]-[{int(record.msecs):03d}]"
        else:
            result = f"[{ct.strftime('%M:%S')}].[{int(record.msecs):03d}]"
        # We pad in the calling method so everything lines up
        return result

def setup_logging():
    """
    Configure all module-specific loggers (admin, budget, dropbox, invoice, etc.)
    EXCEPT for the Flask `web_logger`. We'll do that in `setup_web_logger()`.
    """
    # Ensure the log directories exist
    os.makedirs('./logs', exist_ok=True)
    basepath = './server_webhook/logs/'

    # Option 1: Clear log files on startup by calling the helper function
    # Comment out the next line if you prefer to use FileHandler's 'w' mode instead.
    clear_log_files()

    # Prepare our custom formatter
    formatter = PaddedFormatter(
        fmt="%(asctime)s [%(levelname)s] [%(filename)s] [%(funcName)s] %(message)s",
        datefmt="%M:%S"
    )

    # A console handler to display logs to stdout
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)

    # --------------------------------------------------------------------------
    # Admin logger
    # --------------------------------------------------------------------------
    admin_logger = logging.getLogger('admin_logger')
    admin_logger.setLevel(logging.DEBUG)
    # Option 2: Instead of clearing the file manually, you could open it in 'w' mode:
    # file_handler_admin = logging.FileHandler(os.path.join(basepath, 'admin.log'), mode='w')
    file_handler_admin = logging.FileHandler(os.path.join(basepath, 'admin.log'), mode='a')
    file_handler_admin.setFormatter(formatter)
    admin_logger.addHandler(file_handler_admin)
    admin_logger.addHandler(console_handler)
    admin_logger.propagate = False

    # --------------------------------------------------------------------------
    # Budget logger
    # --------------------------------------------------------------------------
    budget_logger = logging.getLogger('budget_logger')
    budget_logger.setLevel(logging.DEBUG)
    file_handler_budget = logging.FileHandler(os.path.join(basepath, 'budget.log'), mode='a')
    file_handler_budget.setFormatter(formatter)
    budget_logger.addHandler(file_handler_budget)
    budget_logger.addHandler(console_handler)
    budget_logger.propagate = False

    # --------------------------------------------------------------------------
    # Dropbox logger (SDK logs)
    # --------------------------------------------------------------------------
    dropbox_logger = logging.getLogger('dropbox')
    dropbox_logger.setLevel(logging.DEBUG)
    file_handler_dropbox = logging.FileHandler(os.path.join(basepath, 'dropbox.log'), mode='a')
    file_handler_dropbox.setFormatter(formatter)
    dropbox_logger.addHandler(file_handler_dropbox)
    dropbox_logger.addHandler(console_handler)
    dropbox_logger.propagate = False

    # --------------------------------------------------------------------------
    # Invoice logger
    # --------------------------------------------------------------------------
    invoice_logger = logging.getLogger('invoice_logger')
    invoice_logger.setLevel(logging.DEBUG)
    file_handler_invoice = logging.FileHandler(os.path.join(basepath, 'invoice.log'), mode='a')
    file_handler_invoice.setFormatter(formatter)
    invoice_logger.addHandler(file_handler_invoice)
    invoice_logger.addHandler(console_handler)
    invoice_logger.propagate = False

    # --------------------------------------------------------------------------
    # Monday logger
    # --------------------------------------------------------------------------
    monday_logger = logging.getLogger('monday_logger')
    monday_logger.setLevel(logging.DEBUG)
    file_handler_monday = logging.FileHandler(os.path.join(basepath, 'monday.log'), mode='a')
    file_handler_monday.setFormatter(formatter)
    monday_logger.addHandler(file_handler_monday)
    monday_logger.addHandler(console_handler)
    monday_logger.propagate = False

    # --------------------------------------------------------------------------
    # PO logger
    # --------------------------------------------------------------------------
    po_log_logger = logging.getLogger('po_log_logger')
    po_log_logger.setLevel(logging.DEBUG)
    file_handler_po = logging.FileHandler(os.path.join(basepath, 'po_log.log'), mode='a')
    file_handler_po.setFormatter(formatter)
    po_log_logger.addHandler(file_handler_po)
    po_log_logger.addHandler(console_handler)
    po_log_logger.propagate = False

    # --------------------------------------------------------------------------
    # Xero logger
    # --------------------------------------------------------------------------
    xero_logger = logging.getLogger('xero_logger')
    xero_logger.setLevel(logging.DEBUG)
    file_handler_xero = logging.FileHandler(os.path.join(basepath, 'xero.log'), mode='a')
    file_handler_xero.setFormatter(formatter)
    xero_logger.addHandler(file_handler_xero)
    xero_logger.addHandler(console_handler)
    xero_logger.propagate = False

    # --------------------------------------------------------------------------
    # Database logger
    # --------------------------------------------------------------------------
    db_logger = logging.getLogger('database_logger')
    db_logger.setLevel(logging.DEBUG)
    file_handler_db = logging.FileHandler(os.path.join(basepath, 'database.log'), mode='a')
    file_handler_db.setFormatter(formatter)
    db_logger.addHandler(file_handler_db)
    db_logger.addHandler(console_handler)
    db_logger.propagate = False

    # --------------------------------------------------------------------------
    # Basic config to ensure at least INFO messages are displayed for the root
    # This can help in capturing library logs that propagate to the root logger
    # (though we've turned off propagate for our main named loggers above).
    # --------------------------------------------------------------------------
    logging.basicConfig(level=logging.INFO)

    # Return console_handler (or any other handler) for potential reuse
    return console_handler, formatter

def setup_web_logger(flask_app, console_handler, formatter):
    """
    Set up a 'web_logger' dedicated to Flask logs.
    Then attach these handlers to flask_app.logger, removing any defaults.
    """
    basepath = './server_webhook/logs/'
    web_logger = logging.getLogger('web_logger')
    web_logger.setLevel(logging.DEBUG)

    # Create a file handler specifically for web logs
    file_handler_web = logging.FileHandler(os.path.join(basepath, 'web.log'), mode='a')
    file_handler_web.setFormatter(formatter)

    web_logger.addHandler(file_handler_web)
    web_logger.addHandler(console_handler)
    web_logger.propagate = False

    # Attach Flask's logger to our 'web_logger' handlers
    flask_app.logger.handlers = []
    flask_app.logger.addHandler(file_handler_web)
    flask_app.logger.addHandler(console_handler)
    flask_app.logger.setLevel(logging.DEBUG)

