# main.py
import time

from orchestration.orchestrator import Orchestrator
from database.db_util import initialize_database
from utilities.logger import setup_logging

def main():
    setup_logging()
    logger = logging.getLogger(__name__)
    logger.info("Starting the application...")

    # Initialize the database
    Session = initialize_database()

    # Pass the Session to the orchestrator or set it globally if needed
    orchestrator = Orchestrator()

    # Start webhook listeners and background tasks
    orchestrator.initialize_webhook_listeners()
    orchestrator.start_background_tasks()

    # Keep the main thread alive
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Shutting down the application...")

if __name__ == '__main__':
    main()