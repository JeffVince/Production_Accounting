import glob
import logging
import os

from utilities.config import Config
from database.database_util import DatabaseOperations
from files_budget.po_log_database_util import po_log_database_util
from files_budget.po_log_processor import POLogProcessor
from files_dropbox.dropbox_service import DropboxService
from utilities.singleton import SingletonMeta
from files_xero.xero_services import xero_services


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
                self.xero_service = xero_services
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

    def update_xero_bill_dates_from_detail_item(self, xero_bill):

        project_number = xero_bill["project_number"]
        po_number = xero_bill["po_number"]
        detail_number = xero_bill["detail_number"]
        parent_bill_id = xero_bill["id"]
        # 3) Find *all* detail items with the same (project_number, po_number, detail_number)
        detail_items = self.database_util.search_detail_item_by_keys(
            project_number=project_number,
            po_number=po_number,
            detail_number=detail_number
        )
        if not detail_items:
            self.logger.info(
                f'No detail items found for (proj={project_number}, po={po_number}, detail={detail_number}).'
            )
            self.logger.info(
                f'[ParentBillID={parent_bill_id}] 🏁 END function (no detail items).'
            )
            return

        # If the DB operation returns a single dict for a single record, normalize to a list
        if isinstance(detail_items, dict):
            detail_items = [detail_items]

        self.logger.debug(
            f'Found {len(detail_items)} detail item(s) matching parent bill.'
        )

        # 4) Determine earliest transaction_date and latest due_date among all detail items
        existing_parent_date = xero_bill.get('transaction_date')
        existing_parent_due = xero_bill.get('due_date')

        # Collect all detail item dates (ignoring None)
        detail_dates = [
            di['transaction_date'] for di in detail_items
            if di.get('transaction_date') is not None
        ]
        detail_dues = [
            di['due_date'] for di in detail_items
            if di.get('due_date') is not None
        ]

        # Include parent's existing date and due date in the comparison (if present)
        if existing_parent_date is not None:
            detail_dates.append(existing_parent_date)
        if existing_parent_due is not None:
            detail_dues.append(existing_parent_due)

        self.logger.debug(
            f'Existing parent dates => transaction_date={existing_parent_date}, '
            f'due_date={existing_parent_due}'
        )

        from datetime import datetime, date

        # Helper function to convert datetime to date
        def to_date(d):
            if isinstance(d, datetime):
                return d.date()
            elif isinstance(d, date):
                return d
            else:
                raise TypeError(f"Unsupported type: {type(d)}")

        # Apply the conversion to all dates in detail_dates and detail_dues
        detail_dates = [to_date(d) for d in detail_dates if d is not None]
        detail_dues = [to_date(d) for d in detail_dues if d is not None]

        # Include existing parent dates if they are present
        if existing_parent_date:
            existing_parent_date = to_date(existing_parent_date)
        if existing_parent_due:
            existing_parent_due = to_date(existing_parent_due)

        # Calculate the new earliest transaction_date and latest due_date
        if detail_dates:
            earliest_date = min(detail_dates)
        else:
            earliest_date = existing_parent_date

        if detail_dues:
            latest_due = max(detail_dues)
        else:
            latest_due = existing_parent_due

        self.logger.debug(
            f'Computed new Bill dates => transaction_date={earliest_date}, due_date={latest_due}'
        )

        # 5) Update the parent bill if there's a difference
        if (earliest_date != existing_parent_date) or (latest_due != existing_parent_due):
            self.logger.info(
                f'Updating Bill={parent_bill_id} with '
                f'earliest transaction_date={earliest_date} and latest due_date={latest_due}.'
            )
            self.database_util.update_xero_bill(
                xero_bill_id=xero_bill['id'],
                xero_bill=xero_bill.get('xero_id'),
                transaction_date=earliest_date,
                due_date=latest_due
            )
        else:
            self.logger.info(
                f'No changes needed to Bill={parent_bill_id} date range.'
            )

        self.logger.info(
            f'[ParentBillID={parent_bill_id}] 🏁 END function.'
        )

po_log_service = PurchaseOrderLogService()