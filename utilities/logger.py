import os
import logging
from logging.handlers import TimedRotatingFileHandler
from threading import Lock
from dotenv import load_dotenv
import traceback
load_dotenv()
LOGS_DIR = os.path.join(os.path.dirname(__file__), 'logs')
os.makedirs(LOGS_DIR, exist_ok=True)
_logger_initialized = False
_lock = Lock()

class CustomLogger(logging.Logger):

    def log_error_trace(self, exception: Exception, message: str=None):
        """
        Logs an optional message and then the traceback frames of the given exception.
        Each frame logs the file, line number, and function name.

        :param exception: The exception to log the traceback for.
        :param message: An optional string message to precede the traceback log.
        """
        if message:
            self.error(message)
        (exc_type, exc_value, exc_traceback) = (exception.__class__, exception, exception.__traceback__)
        frames = traceback.extract_tb(exc_traceback)
        for frame in frames:
            self.error(f'File: {os.path.basename(frame.filename)}, Line: {frame.lineno}, Function: {frame.name}')

def setup_logging(name='app_logger'):
    """
    Sets up logging for the application. The log level is determined by the
    LOG_LEVEL environment variable. Defaults to DEBUG if not set.

    Returns:
        logging.Logger: Configured logger.
    """
    global _logger_initialized
    with _lock:
        if _logger_initialized:
            logger = logging.getLogger(name)
            return logger
        logging.setLoggerClass(CustomLogger)
        log_level_str = os.getenv('LOG_LEVEL', 'DEBUG').upper()
        log_level = getattr(logging, log_level_str, logging.DEBUG)
        logger = logging.getLogger(name)
        logger.setLevel(log_level)
        logging.getLogger('sqlalchemy.engine').setLevel(logging.ERROR)
        logging.getLogger('sqlalchemy.pool').setLevel(logging.ERROR)
        log_format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        file_handler = TimedRotatingFileHandler(filename=os.path.join(LOGS_DIR, 'application.log'), when='midnight', interval=1, backupCount=7, encoding='utf-8')
        file_handler.setFormatter(log_format)
        file_handler.setLevel(log_level)
        file_handler.name = 'file_handler'
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(log_format)
        console_handler.setLevel(log_level)
        console_handler.name = 'console_handler'
        if not any((h.name == 'file_handler' for h in logger.handlers)):
            logger.addHandler(file_handler)
            logger.debug('Added file handler to app_logger.')
        if not any((h.name == 'console_handler' for h in logger.handlers)):
            logger.addHandler(console_handler)
            logger.debug('Added console handler to app_logger.')
        logger.propagate = False
        _logger_initialized = True
        logger.debug(f'[DEBUG] {name} has {len(logger.handlers)} handlers attached.')
        return logger

def log_event(event_type: str, details: dict):
    """
    Logs a specific event with its type and details.

    :param event_type: A short string categorizing the event (e.g., 'ERROR', 'INFO').
    :param details: A dictionary containing event-specific details.
    """
    logger = logging.getLogger('app_logger')
    try:
        message = f'Event Type: {event_type}, Details: {details}'
        if event_type.upper() in ['ERROR', 'CRITICAL']:
            logger.error(message)
        elif event_type.upper() == 'WARNING':
            logger.warning(message)
        else:
            logger.info(message)
    except Exception as e:
        logger.error(f'Failed to log event: {e}')

def log_handler_details():
    """
    Diagnostic function to log handler details.
    """
    logger = logging.getLogger('app_logger')
    for (idx, handler) in enumerate(logger.handlers, start=1):
        logger.debug(f'Handler {idx}: {handler}')