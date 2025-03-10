import os
import sys

from dotenv import load_dotenv
load_dotenv()

class Config:
    USE_TEMP = True
    SKIP_MAIN = False
    USE_LOCAL = False

    APP_DEBUG = True
    if sys.gettrace():
        # Debug mode detected
        APP_DEBUG = True
    else:
        # Normal mode
        APP_DEBUG = False

    WEBHOOK_MAIN_PORT = 5002
    WEBHOOK_MAIN_PORT_DEBUG = 5002


    CELERY_BROKER_URL = os.getenv('CELERY_BROKER_URL')
    CELERY_RESULT_BACKEND = os.getenv('CELERY_RESULT_BACKEND')
    DROPBOX_REFRESH_TOKEN = os.getenv('DROPBOX_REFRESH_TOKEN')
    DROPBOX_APP_KEY = os.getenv('DROPBOX_APP_KEY')
    DROPBOX_APP_SECRET = os.getenv('DROPBOX_APP_SECRET')
    LOCAL_FOLDER_PATH = os.getenv('LOCAL_FOLDER_PATH')
    TARGET_PURCHASE_ORDERS_FOLDER = os.getenv('TARGET_PURCHASE_ORDERS_FOLDER')
    NAMESPACE_NAME = os.getenv('NAMESPACE_NAME')
    MONDAY_API_TOKEN = os.getenv('MONDAY_API_TOKEN')
    SLACK_TOKEN = os.getenv('SLACK_TOKEN')
    MERCURY_API_TOKEN = os.getenv('MERCURY_API_TOKEN')
    XERO_CLIENT_ID = os.getenv('XERO_CLIENT_ID')
    XERO_CLIENT_SECRET = os.getenv('XERO_CLIENT_SECRET')
    XERO_PARAM_AUTH = os.getenv('XERO_PARAM_AUTH')
    XERO_ACCESS_TOKEN = os.getenv('XERO_ACCESS_TOKEN')
    XERO_REFRESH_TOKEN = os.getenv('XERO_REFRESH_TOKEN')
    XERO_TENANT_ID = os.getenv('XERO_TENANT_ID')
    OPENAI_API_KEY = os.getenv('OPENAI_API_KEY')
    MY_EMAIL = os.getenv('MY_EMAIL')
    DATABASE_URL = os.getenv('DATABASE_URL', 'postgresql://doadmin:AVNS_wxTAj-nNJrpMakwsi--@olivine-db-main-do-user-11898912-0.l.db.ondigitalocean.com:25060/defaultdb?sslmode=require')

    @staticmethod
    def load_configuration():
        """
        Loads all configuration settings into a dictionary for easy access.
        """
        return {'CELERY_BROKER_URL': Config.CELERY_BROKER_URL, 'CELERY_RESULT_BACKEND': Config.CELERY_RESULT_BACKEND, 'DROPBOX_APP_KEY': Config.DROPBOX_APP_KEY, 'DROPBOX_APP_SECRET': Config.DROPBOX_APP_SECRET, 'DROPBOX_REFRESH_TOKEN': Config.DROPBOX_REFRESH_TOKEN, 'MONDAY_API_TOKEN': Config.MONDAY_API_TOKEN, 'SLACK_TOKEN': Config.SLACK_TOKEN, 'MERCURY_API_TOKEN': Config.MERCURY_API_TOKEN, 'XERO_CLIENT_ID': Config.XERO_CLIENT_ID, 'XERO_CLIENT_SECRET': Config.XERO_CLIENT_SECRET, 'XERO_TENANT_ID': Config.XERO_TENANT_ID, 'OPENAI_API_KEY': Config.OPENAI_API_KEY, 'MY_EMAIL': Config.MY_EMAIL, 'DATABASE': Config.DATABASE_URL}

    @staticmethod
    def get_api_keys():
        """
        Returns a dictionary of API keys for external services.
        """
        return {'Dropbox': Config.DROPBOX_APP_KEY, 'Monday': Config.MONDAY_API_TOKEN, 'Slack': Config.SLACK_TOKEN, 'Mercury': Config.MERCURY_API_TOKEN, 'Xero': Config.XERO_CLIENT_ID, 'OpenAI': Config.OPENAI_API_KEY}

    @staticmethod
    def get_database_settings(local=False):
        """
        Returns database connection settings.
        """
        if local:
            return {'url': os.getenv('DATABASE_URL', 'mysql+pymysql://root:z //55gohi@localhost:3306/virtual_pm')}
        else:
            return {'url': os.getenv('DATABASE_URL', 'postgresql://doadmin:AVNS_wxTAj-nNJrpMakwsi--@olivine-db-main-do-user-11898912-0.l.db.ondigitalocean.com:25060/defaultdb?sslmode=require')}

    import os
    import sys
    from dotenv import load_dotenv
    load_dotenv()

    class Config:
        USE_TEMP = True
        SKIP_MAIN = False
        USE_LOCAL = True
        APP_DEBUG = True
        WEBHOOK_MAIN_PORT = 5002
        WEBHOOK_MAIN_PORT_DEBUG = 5003
        CELERY_BROKER_URL = os.getenv('CELERY_BROKER_URL')
        CELERY_RESULT_BACKEND = os.getenv('CELERY_RESULT_BACKEND')
        DROPBOX_REFRESH_TOKEN = os.getenv('DROPBOX_REFRESH_TOKEN')
        DROPBOX_APP_KEY = os.getenv('DROPBOX_APP_KEY')
        DROPBOX_APP_SECRET = os.getenv('DROPBOX_APP_SECRET')
        LOCAL_FOLDER_PATH = os.getenv('LOCAL_FOLDER_PATH')
        TARGET_PURCHASE_ORDERS_FOLDER = os.getenv('TARGET_PURCHASE_ORDERS_FOLDER')
        NAMESPACE_NAME = os.getenv('NAMESPACE_NAME')
        MONDAY_API_TOKEN = os.getenv('MONDAY_API_TOKEN')
        SLACK_TOKEN = os.getenv('SLACK_TOKEN')
        MERCURY_API_TOKEN = os.getenv('MERCURY_API_TOKEN')
        XERO_CLIENT_ID = os.getenv('XERO_CLIENT_ID')
        XERO_CLIENT_SECRET = os.getenv('XERO_CLIENT_SECRET')
        XERO_PARAM_AUTH = os.getenv('XERO_PARAM_AUTH')
        XERO_ACCESS_TOKEN = os.getenv('XERO_ACCESS_TOKEN')
        XERO_REFRESH_TOKEN = os.getenv('XERO_REFRESH_TOKEN')
        XERO_TENANT_ID = os.getenv('XERO_TENANT_ID')
        OPENAI_API_KEY = os.getenv('OPENAI_API_KEY')
        MY_EMAIL = os.getenv('MY_EMAIL')
        DATABASE_URL = os.getenv('DATABASE_URL', 'mysql+pymysql://root:z //55gohi@localhost:3306/virtual_pm')
        DATABASE_URL = os.getenv('DATABASE_URL', 'postgresql://doadmin:AVNS_wxTAj-nNJrpMakwsi--@olivine-db-main-do-user-11898912-0.l.db.ondigitalocean.com:25060/defaultdb?sslmode=require')

        @staticmethod
        def load_configuration():
            """
            Loads all configuration settings into a dictionary for easy access.
            """
            return {
                'CELERY_BROKER_URL': Config.CELERY_BROKER_URL,
                'CELERY_RESULT_BACKEND': Config.CELERY_RESULT_BACKEND,
                'DROPBOX_APP_KEY': Config.DROPBOX_APP_KEY,
                'DROPBOX_APP_SECRET': Config.DROPBOX_APP_SECRET,
                'DROPBOX_REFRESH_TOKEN': Config.DROPBOX_REFRESH_TOKEN,
                'MONDAY_API_TOKEN': Config.MONDAY_API_TOKEN,
                'SLACK_TOKEN': Config.SLACK_TOKEN,
                'MERCURY_API_TOKEN': Config.MERCURY_API_TOKEN,
                'XERO_CLIENT_ID': Config.XERO_CLIENT_ID,
                'XERO_CLIENT_SECRET': Config.XERO_CLIENT_SECRET,
                'XERO_TENANT_ID': Config.XERO_TENANT_ID,
                'OPENAI_API_KEY': Config.OPENAI_API_KEY,
                'MY_EMAIL': Config.MY_EMAIL,
                'DATABASE': Config.DATABASE_URL
            }

        @staticmethod
        def get_api_keys():
            """
            Returns a dictionary of API keys for external services.
            """
            return {
                'Dropbox': Config.DROPBOX_APP_KEY,
                'Monday': Config.MONDAY_API_TOKEN,
                'Slack': Config.SLACK_TOKEN,
                'Mercury': Config.MERCURY_API_TOKEN,
                'Xero': Config.XERO_CLIENT_ID,
                'OpenAI': Config.OPENAI_API_KEY
            }

        @staticmethod
        def get_database_settings(local=False):
            """
            Returns database connection settings.
            """
            if local:
                return {'url': os.getenv('DATABASE_URL', 'mysql+pymysql://root:z //55gohi@localhost:3306/virtual_pm')}
            else:
                return {'url': os.getenv('DATABASE_URL',
                                         'postgresql://doadmin:AVNS_wxTAj-nNJrpMakwsi--@olivine-db-main-do-user-11898912-0.l.db.ondigitalocean.com:25060/defaultdb?sslmode=require')}

        @staticmethod
        def we_in_debug_mode():
            """            """


            return Config.APP_DEBUG

    def get_running_port(self):
        """
        Determines the port to use based on whether the script is running in debug mode.
        """
        if Config.APP_DEBUG:
            # Debug mode detected
            return Config.WEBHOOK_MAIN_PORT_DEBUG
        else:
            # Normal mode
            return Config.WEBHOOK_MAIN_PORT

    def set_breakpoint(self):
        # Connect the worker to Pycharm's Debugger
        if 'pydevd' in sys.modules:
            import pydevd
            pydevd.settrace('localhost ', port=12345, stdoutToServer=True, stderrToServer=True)