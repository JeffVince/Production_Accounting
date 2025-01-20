import logging
import sys

from utilities.config import Config
from logging_setup import setup_logging
logger = logging.getLogger('admin_logger')

import time
import threading

from database.db_util import initialize_database


try:
    from server_webhook.webhook_main import app
except Exception as e:
    print(f"Error importing webhook_main: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)
logger.info('Starting the application...')

from werkzeug.serving import make_server
def run_flask_app():
    """Function to run the Flask app."""

    logger.info('[ run_flask_app ] [ MAIN.PY ]- 👻 Starting Flask server for webhooks...')

    # Decide the port: if APP_DEBUG is True, use the debug port; otherwise use the normal port.
    chosen_port = config.get_running_port()


    app.config['TEMPLATES_AUTO_RELOAD'] = True
    app.debug = config.APP_DEBUG
    server = make_server('0.0.0.0', chosen_port, app)
    setup_logging(server)

    try:
        logger.info(f'[ run_flask_app ] [ MAIN.PY ] Flask server running on port {chosen_port}.')
        server.serve_forever()
    except KeyboardInterrupt:
        logger.info('[ run_flask_app ] [ MAIN.PY ] Shutting down Flask server...')
        server.shutdown()

def main():
    config = Config()
    db_settings = config.get_database_settings(config.USE_LOCAL)
    initialize_database(db_settings['url'])
    logger.info('Database initialized.')
    flask_thread = threading.Thread(target=run_flask_app)
    flask_thread.start()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info('Shutting down the application...')
if __name__ == '__main__':
    main()