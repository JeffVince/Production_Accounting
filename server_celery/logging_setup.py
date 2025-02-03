import logging
import logging.config
import os
from datetime import datetime

class PaddedFormatter(logging.Formatter):
    def __init__(self, fmt=None, datefmt=None, max_length=2000000):
        super().__init__(fmt, datefmt)
        self.max_length = max_length
        self.COLUMN_WIDTHS = {
            "time": 18,
            "levelname": 7,
            "filename": 16,
            "funcName": 24,
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
        record.levelname= self.pad_center(record.levelname,    self.COLUMN_WIDTHS["levelname"])
        record.filename = self.pad_left(record.filename,       self.COLUMN_WIDTHS["filename"])
        record.funcName = self.pad_center(record.funcName,       self.COLUMN_WIDTHS["funcName"])

        # Truncate message if necessary
        # if len(record.msg) > self.max_length:
        #     record.msg = record.msg[:self.max_length] + "..."
        return super().format(record)

    def formatTime(self, record, datefmt=None):
        ct = datetime.fromtimestamp(record.created)
        if datefmt:
            result = f"[{ ct.strftime(datefmt)}]-[{int(record.msecs):03d}]"
        else:
            result = f"[{ct.strftime('%H%M:%S')}].[{int(record.msecs):03d}]"
        # We pad in the calling method so everything lines up
        return result

def setup_logging():
    """
    Configure all module-specific loggers (admin, budget, dropbox, invoice, etc.)
    EXCEPT for the Flask `web_logger`. We'll do that in `setup_web_logger()`.
    """
    os.makedirs('./logs', exist_ok=True)
    basepath = './logs/'

    LOGGING_CONFIG = {
        'version': 1,
        'disable_existing_loggers': False,  # Keep existing loggers
        'formatters': {
            'padded': {
                '()': PaddedFormatter,
                'fmt': "%(asctime)s [%(levelname)s] [%(filename)s] [%(funcName)s] %(message)s",
                'datefmt': "%H:%M:%S",
                'max_length': 10000000,
            },
        },
        'handlers': {
            'console': {
                'class': 'logging.StreamHandler',
                'formatter': 'padded',
            },
            'admintax_form': {
                'class': 'logging.FileHandler',
                'filename': os.path.join(basepath, 'admin.log'),
                'formatter': 'padded',
            },
            'budgettax_form': {
                'class': 'logging.FileHandler',
                'filename': os.path.join(basepath, 'budget.log'),
                'formatter': 'padded',
            },
            'dropboxtax_form': {
                'class': 'logging.FileHandler',
                'filename': os.path.join(basepath, 'dropbox.log'),
                'formatter': 'padded',
            },
            'invoicetax_form': {
                'class': 'logging.FileHandler',
                'filename': os.path.join(basepath, 'invoice.log'),
                'formatter': 'padded',
            },
            'mondaytax_form': {
                'class': 'logging.FileHandler',
                'filename': os.path.join(basepath, 'monday.log'),
                'formatter': 'padded',
            },
            'po_logtax_form': {
                'class': 'logging.FileHandler',
                'filename': os.path.join(basepath, 'po_log.log'),
                'formatter': 'padded',
            },
            'xerotax_form': {
                'class': 'logging.FileHandler',
                'filename': os.path.join(basepath, 'xero.log'),
                'formatter': 'padded',
            },
            'databasetax_form': {
                'class': 'logging.FileHandler',
                'filename': os.path.join(basepath, 'database.log'),
                'formatter': 'padded',
            },
        },
        'loggers': {
            'admin_logger': {
                'handlers': ['admintax_form', 'console'],
                'level': 'DEBUG',
                'propagate': False,
            },
            'budget_logger': {
                'handlers': ['budgettax_form', 'console'],
                'level': 'DEBUG',
                'propagate': False,
            },
            'dropbox': {
                'handlers': ['dropboxtax_form', 'console'],
                'level': 'DEBUG',
                'propagate': False,
            },
            'invoice_logger': {
                'handlers': ['invoicetax_form', 'console'],
                'level': 'DEBUG',
                'propagate': False,
            },
            'monday_logger': {
                'handlers': ['mondaytax_form', 'console'],
                'level': 'DEBUG',
                'propagate': False,
            },
            'po_log_logger': {
                'handlers': ['po_logtax_form', 'console'],
                'level': 'DEBUG',
                'propagate': False,
            },
            'xero_logger': {
                'handlers': ['xerotax_form', 'console'],
                'level': 'DEBUG',
                'propagate': False,
            },
            'database_logger': {
                'handlers': ['databasetax_form', 'console'],
                'level': 'DEBUG',
                'propagate': False,
            },
            # Add other loggers as needed
        },
        'root': {
            'handlers': ['console'],
            'level': 'INFO',
        },
    }

    logging.config.dictConfig(LOGGING_CONFIG)

def setup_web_logger(flask_app):
    """
    Set up a 'web_logger' dedicated to Flask logs.
    Then attach these handlers to flask_app.logger, removing any defaults.
    """
    basepath = './server_webhook/logs/'
    os.makedirs(basepath, exist_ok=True)

    web_logging_config = {
        'version': 1,
        'disable_existing_loggers': False,
        'formatters': {
            'padded': {
                '()': PaddedFormatter,
                'fmt': "%(asctime)s [%(levelname)s] [%(filename)s] [%(funcName)s] %(message)s",
                'datefmt': "%H:%M:%S",
            },
        },
        'handlers': {
            'console': {
                'class': 'logging.StreamHandler',
                'formatter': 'padded',
            },
            'webtax_form': {
                'class': 'logging.FileHandler',
                'filename': os.path.join(basepath, 'web.log'),
                'formatter': 'padded',
            },
        },
        'loggers': {
            'web_logger': {
                'handlers': ['webtax_form', 'console'],
                'level': 'DEBUG',
                'propagate': False,
            },
        },
        'root': {
            'handlers': ['console'],
            'level': 'INFO',
        },
    }

    logging.config.dictConfig(web_logging_config)

    web_logger = logging.getLogger('web_logger')

    # Remove default Flask handlers
    flask_app.logger.handlers = []
    flask_app.logger.addHandler(web_logger.handlers[0])  # webtax_form
    flask_app.logger.addHandler(web_logger.handlers[1])  # console
    flask_app.logger.setLevel(logging.DEBUG)