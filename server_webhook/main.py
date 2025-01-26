# main.py
import logging
import time
import threading

from werkzeug.serving import make_server

# Import your app factory from __init__.py
from server_webhook import create_app  # <-- Adjust "myapp" to match your actual package name

from utilities.config import Config
from logging_setup import setup_logging, setup_web_logger
from database.db_util import initialize_database

logger = logging.getLogger('admin_logger')

def run_flask_app(app):
    """
    Function to run the Flask app in a blocking manner (serve_forever).
    """
    config = Config()
    chosen_port = config.get_running_port()
    logger.info(f"🚀 Starting Flask server on port {chosen_port}...")

    server = make_server('0.0.0.0', chosen_port, app)
    logger.info(f"🌐 Server listening at 0.0.0.0:{chosen_port}. Serving forever...")

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        logger.info("🛑 KeyboardInterrupt received; shutting down server.")
        server.shutdown()

def main():
    logger.info("🔑 Starting the application...")

    # 1. Initialize the database
    config = Config()
    db_settings = config.get_database_settings(config.USE_LOCAL)
    logger.info(f"💾 Initializing the database with URL: {db_settings['url']}")
    initialize_database(db_settings['url'])
    logger.info("✅ Database initialized successfully.")

    # 2. Create the Flask app via the factory
    app = create_app()

    # 3. Start the Flask server in a new thread
    flask_thread = threading.Thread(target=run_flask_app, args=(app,))
    flask_thread.start()
    logger.info("🏃 Flask server thread started. Main thread will remain active.")

    # 4. Keep the main thread alive
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("🛑 Shutting down the entire application...")

if __name__ == '__main__':
    main()