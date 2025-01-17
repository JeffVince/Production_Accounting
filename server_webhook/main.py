# main.py

import logging

from config import Config
config = Config()
from logging_setup import setup_logging

setup_logging()
logger = logging.getLogger("admin_logger")
logger.info("Starting the application...")

import time
import threading
from orchestration.orchestrator import Orchestrator
from database.db_util import initialize_database
from utilities.config import Config
from webhook_main import app  # Import the Flask app instance
from werkzeug.serving import make_server


def run_flask_app():
    """Function to run the Flask app."""
    setup_logging()  # This sets up the 7 loggers + file + console
    logger = logging.getLogger("webhook_main")
    logger.info("Starting Flask server for webhooks...")

    # Enable template auto-reloading
    app.config['TEMPLATES_AUTO_RELOAD'] = config.APP_DEBUG

    # Optionally set debugging mode for easier development
    app.debug = config.APP_DEBUG

    # Initialize and start the server
    server = make_server('0.0.0.0', Config.WEBHOOK_MAIN_PORT, app)
    try:
        logger.info(f"Flask server running on port {Config.WEBHOOK_MAIN_PORT}.")
        server.serve_forever()
    except KeyboardInterrupt:
        logger.info("Shutting down Flask server...")
        server.shutdown()


def main():
    # Setup logging and get the configured logger
    logger = logging.getLogger("admin_logger")

    # Initialize the database
    config = Config()

    db_settings = config.get_database_settings(config.USE_LOCAL)
    initialize_database(db_settings['url'])
    logger.info("Database initialized.")

    # Initialize the orchestrator
    orchestrator = Orchestrator()
    logger.info("Orchestrator initialized.")

    # Start background tasks such as PO Log checking and state coordination
    orchestrator.start_background_tasks()

    # Start webhook server in a thread
    flask_thread = threading.Thread(target=run_flask_app)
    flask_thread.start()

    # Keep the main thread alive
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Shutting down the application...")


if __name__ == '__main__':
    main()
