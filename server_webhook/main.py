import logging
from config import Config
config = Config()
from logging_setup import setup_logging
setup_logging()
logger = logging.getLogger('admin_logger')
logger.info('Starting the application...')
import time
import threading
from orchestration.orchestrator import Orchestrator
from database.db_util import initialize_database
from utilities.config import Config
from webhook_main import app
from werkzeug.serving import make_server

def run_flask_app():
    """Function to run the Flask app."""
    setup_logging()
    logger = logging.getLogger('webhook_main')
    logger.info('Starting Flask server for webhooks...')
    app.config['TEMPLATES_AUTO_RELOAD'] = config.APP_DEBUG
    app.debug = config.APP_DEBUG
    server = make_server('0.0.0.0', Config.WEBHOOK_MAIN_PORT, app)
    try:
        logger.info(f'Flask server running on port {Config.WEBHOOK_MAIN_PORT}.')
        server.serve_forever()
    except KeyboardInterrupt:
        logger.info('Shutting down Flask server...')
        server.shutdown()

def main():
    logger = logging.getLogger('admin_logger')
    config = Config()
    db_settings = config.get_database_settings(config.USE_LOCAL)
    initialize_database(db_settings['url'])
    logger.info('Database initialized.')
    orchestrator = Orchestrator()
    logger.info('Orchestrator initialized.')
    orchestrator.start_background_tasks()
    flask_thread = threading.Thread(target=run_flask_app)
    flask_thread.start()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info('Shutting down the application...')
if __name__ == '__main__':
    main()