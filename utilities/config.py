# utilities/config.py

import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Configuration settings
class Config:

    # WEBHOOK PORTS
    DROPBOX_WEBHOOK_PORT = 5001
    MONDAY_WEBHOOK_PORT = 5002
    XERO_WEBHOOK_PORT = 5003
    MERCURY_WEBHOOK_PORT = 5004

    # Celery
    CELERY_BROKER_URL = os.getenv("CELERY_BROKER_URL")
    CELERY_RESULT_BACKEND = os.getenv("CELERY_RESULT_BACKEND")

    # Dropbox
    DROPBOX_REFRESH_TOKEN = os.getenv("DROPBOX_REFRESH_TOKEN")
    DROPBOX_APP_KEY = os.getenv("DROPBOX_APP_KEY")
    DROPBOX_APP_SECRET = os.getenv("DROPBOX_APP_SECRET")
    LOCAL_FOLDER_PATH = os.getenv("LOCAL_FOLDER_PATH")
    TARGET_PURCHASE_ORDERS_FOLDER = os.getenv("TARGET_PURCHASE_ORDERS_FOLDER")
    NAMESPACE_NAME = os.getenv("NAMESPACE_NAME")

    # Monday API
    MONDAY_API_TOKEN = os.getenv("MONDAY_API_TOKEN")

    # Slack
    SLACK_TOKEN = os.getenv("SLACK_TOKEN")

    # Mercury
    MERCURY_API_TOKEN = os.getenv("MERCURY_API_TOKEN")

    # Xero
    XERO_CLIENT_ID = os.getenv("XERO_CLIENT_ID")
    XERO_CLIENT_SECRET = os.getenv("XERO_CLIENT_SECRET")
    XERO_PARAM_AUTH = os.getenv("XERO_PARAM_AUTH")
    XERO_ACCESS_TOKEN = os.getenv("XERO_ACCESS_TOKEN")
    XERO_REFRESH_TOKEN = os.getenv("XERO_REFRESH_TOKEN")
    XERO_TENANT_ID = os.getenv("XERO_TENANT_ID")

    # OpenAI
    OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

    # Other
    MY_EMAIL = os.getenv("MY_EMAIL")

    @staticmethod
    def load_configuration():
        """
        Loads all configuration settings into a dictionary for easy access.
        """
        return {
            "CELERY_BROKER_URL": Config.CELERY_BROKER_URL,
            "CELERY_RESULT_BACKEND": Config.CELERY_RESULT_BACKEND,
            "DROPBOX_APP_KEY": Config.DROPBOX_APP_KEY,
            "DROPBOX_APP_SECRET": Config.DROPBOX_APP_SECRET,
            "DROPBOX_REFRESH_TOKEN": Config.DROPBOX_REFRESH_TOKEN,
            "MONDAY_API_TOKEN": Config.MONDAY_API_TOKEN,
            "SLACK_TOKEN": Config.SLACK_TOKEN,
            "MERCURY_API_TOKEN": Config.MERCURY_API_TOKEN,
            "XERO_CLIENT_ID": Config.XERO_CLIENT_ID,
            "XERO_CLIENT_SECRET": Config.XERO_CLIENT_SECRET,
            "XERO_TENANT_ID": Config.XERO_TENANT_ID,
            "OPENAI_API_KEY": Config.OPENAI_API_KEY,
            "MY_EMAIL": Config.MY_EMAIL,
        }

    @staticmethod
    def get_api_keys():
        """
        Returns a dictionary of API keys for external services.
        """
        return {
            "Dropbox": Config.DROPBOX_APP_KEY,
            "Monday": Config.MONDAY_API_TOKEN,
            "Slack": Config.SLACK_TOKEN,
            "Mercury": Config.MERCURY_API_TOKEN,
            "Xero": Config.XERO_CLIENT_ID,
            "OpenAI": Config.OPENAI_API_KEY,
        }

    @staticmethod
    def get_database_settings():
        """
        Returns database connection settings.
        """
        return {
            "url": os.getenv("DATABASE_URL", "sqlite:///default.db"),  # Changed key to 'url'
        }