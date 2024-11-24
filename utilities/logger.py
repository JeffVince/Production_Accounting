import os
import logging
from logging.handlers import TimedRotatingFileHandler

# Ensure the logs directory exists
LOGS_DIR = os.path.join(os.path.dirname(__file__), "logs")
os.makedirs(LOGS_DIR, exist_ok=True)


# Configure logger
def setup_logging():
    logger = logging.getLogger("app_logger")
    logger.setLevel(logging.ERROR)
    # Suppress SQLAlchemy engine logs
    logging.getLogger("sqlalchemy.engine").setLevel(logging.ERROR)
    # Formatter for the logs
    log_format = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )

    # File handler (rotates daily)
    file_handler = TimedRotatingFileHandler(
        filename=os.path.join(LOGS_DIR, "application.log"),
        when="midnight",
        interval=1,
        backupCount=7,  # Keeps last 7 days of logs
    )
    file_handler.setFormatter(log_format)
    file_handler.setLevel(logging.ERROR)

    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(log_format)
    console_handler.setLevel(logging.ERROR)

    # Add handlers to the logger
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger


# Log an event
def log_event(event_type: str, details: dict):
    """
    Logs a specific event with its type and details.

    :param event_type: A short string categorizing the event (e.g., 'ERROR', 'INFO').
    :param details: A dictionary containing event-specific details.
    """
    try:
        message = f"Event Type: {event_type}, Details: {details}"
        if event_type.upper() in ["ERROR", "CRITICAL"]:
            logger.error(message)
        elif event_type.upper() == "WARNING":
            logger.warning(message)
        else:
            logger.info(message)
    except Exception as e:
        logger.error(f"Failed to log event: {e}")


# Initialize logging
logger = setup_logging()