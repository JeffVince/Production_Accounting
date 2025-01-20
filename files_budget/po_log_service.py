import glob
import logging
import os

from utilities.config import Config
from database.database_util import DatabaseOperations
from files_budget.po_log_database_util import po_log_database_util
from files_budget.po_log_processor import POLogProcessor
from files_dropbox.dropbox_service import DropboxService
from utilities.singleton import SingletonMeta


class PurchaseOrderLogService(metaclass=SingletonMeta):

    def __init__(self):
        """
        Initializes the DropboxService singleton, setting up logging, external
        APIs, and the new DatabaseOperations object for DB interactions.
        """
        self.logger = logging.getLogger('po_log_logger')
        self.logger.info('[__init__] - 📦 PO LOG Service initializing - Importing dependencies')

        try:
            if not hasattr(self, '_initialized'):
                self.po_log_processor = POLogProcessor()
                self.config = Config()
                self.dropbox_service = DropboxService()
                self.po_log_database_util = po_log_database_util
                self.database_util = DatabaseOperations()
                self.logger.info('[__init__] - 📦 PO LOG Service initialized. Ready to manage PO logs!')
                self._initialized = True
            else:
                self.logger.info('[__init__] - 📦 PO LOG Service already initialized. Ready to manage PO logs, again!')
        except Exception as e:
            self.logger.exception(f"Error inside PurchaseOrderLogService.__init__: {e}")

    def po_log_new_trigger(self):
        """
        Handles the PO Log [NEW] action by processing the latest PO log file.
        """
        try:
            current_dir = os.getcwd()
            self.logger.info(f'[start_background_tasks] - Current working directory: {current_dir}')

            log_dir = './../temp_files'
            absolute_log_dir = os.path.abspath(log_dir)

            # Debug: List files in the directory
            if os.path.exists(absolute_log_dir):
                self.logger.info(f'[start_background_tasks] - absolute_log_dir exists: {absolute_log_dir}')
                try:
                    files_in_dir = os.listdir(absolute_log_dir)
                    self.logger.info(f'[start_background_tasks] - Files in {absolute_log_dir}: {files_in_dir}')
                except Exception as e:
                    self.logger.error(f'[start_background_tasks] - Error listing files: {e}')
            else:
                self.logger.error(f'[start_background_tasks] - Directory does not exist: {absolute_log_dir}')

            # Now, try to find the log files by pattern
            log_files = glob.glob(os.path.join(absolute_log_dir, 'PO_LOG_2416-*.txt'))
            self.logger.info(f'[start_background_tasks] - Found log files: {log_files}')

            if log_files:
                latest_log_file = max(log_files, key=os.path.getmtime)
                self.logger.info(f'[start_background_tasks] - Latest log file: {latest_log_file}')
                self.dropbox_service.po_log_orchestrator(latest_log_file)
            else:
                self.logger.error('[start_background_tasks] - No PO LOG FILES FOUND FOR TESTING')
        except Exception as e:
            self.logger.error(f'[po_log_new_trigger] - 💥 Unexpected error: {e}', exc_info=True)
            raise


po_log_service = PurchaseOrderLogService()