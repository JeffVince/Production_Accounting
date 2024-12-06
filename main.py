# main.py

import logging
from utilities.logger import setup_logging

setup_logging()
logger = logging.getLogger("app_logger")
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
    logger = logging.getLogger("app_logger")  # Use the named logger
    logger.info("Starting Flask server for webhooks...")
    server = make_server('0.0.0.0', Config.WEBHOOK_MAIN_PORT, app)
    server.serve_forever()


def main():
    # Setup logging and get the configured logger
    logger = logging.getLogger("app_logger")

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
