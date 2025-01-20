# __init__.py
import logging
from flask import Flask

# Your custom Config and logging setup
from utilities.config import Config
from logging_setup import setup_logging, setup_web_logger

# Initialize the top-level console_handler, formatter, and admin_logger
console_handler, formatter = setup_logging()
logger = logging.getLogger('admin_logger')


def create_app():
    """Application factory that creates and configures the Flask app."""

    logger.info("🔥 [ create_app ] - Creating the Flask application instance...")
    app = Flask(__name__)

    # 1. Load/Use the custom config
    config = Config()
    app.debug = config.APP_DEBUG
    app.config['TEMPLATES_AUTO_RELOAD'] = True
    logger.info(f"🐢 [ create_app ] - Set Flask debug={config.APP_DEBUG}, TEMPLATES_AUTO_RELOAD=True")

    # 2. Setup web logger
    setup_web_logger(app, console_handler, formatter)
    logger.info("🔧 [ create_app ] - Web logger set up with console_handler & formatter")

    # 3. Register Blueprints
    logger.info("📦 [ create_app ] - Registering Blueprints from server_webhook.webhook_main...")
    from server_webhook.webhook_main import webhook_main_bp
    app.register_blueprint(webhook_main_bp)
    logger.info("🎉 [ create_app ] - webhook_main_bp registered successfully!")

    # If you have additional blueprint imports, register them similarly:
    # from files_monday.monday_webhook_handler import monday_blueprint
    # app.register_blueprint(monday_blueprint, url_prefix='/webhook/monday')
    # logger.info("📦 [ create_app ] - Registered monday_blueprint at '/webhook/monday'")

    logger.info("✅ [ create_app ] - All set. Returning the Flask 'app' instance now.")
    return app