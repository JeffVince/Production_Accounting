# main.py

import logging
import time
import threading
from orchestration.orchestrator import Orchestrator
from database.db_util import initialize_database
from utilities.logger import setup_logging
from utilities.config import Config
from webhook.webhook_main import app  # Import the Flask app instance
from werkzeug.serving import make_server


def run_flask_app():
    """Function to run the Flask app."""
    logger = logging.getLogger("FlaskApp")
    logger.info("Starting Flask server for webhooks...")
    server = make_server('0.0.0.0', Config.WEBHOOK_MAIN_PORT, app)
    server.serve_forever()


def main():
    # Setup logging
    setup_logging()
    logger = logging.getLogger(__name__)
    logger.info("Starting the application...")

    # Initialize the database
    config = Config()
    db_settings = config.get_database_settings()
    initialize_database(db_settings['url'])
    logger.info("Database initialized.")

    # Initialize the orchestrator
    orchestrator = Orchestrator()
    logger.info("Orchestrator initialized.")

    # Start background tasks such as PO Log checking and state coordination
    #orchestrator.start_background_tasks()
    # logger.info("Orchestrator background tasks started.")

    # Start Flask server in a thread
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
