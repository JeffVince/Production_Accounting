# xero_services.py

import logging

from database_util import DatabaseOperations
from singleton import SingletonMeta
from xero_files.xero_api import xero_api


class XeroServices(metaclass=SingletonMeta):

    """
    XeroServices
    ============
    Provides higher-level operations for working with the XeroAPI and storing
    data in the database via DatabaseOperations.
    """

    def __init__(self):
        """
        Initialize the XeroServices with references to DatabaseOperations
        and XeroAPI instances.
        """
        self.database_util = DatabaseOperations()
        self.xero_api = xero_api
        self.logger = logging.getLogger("app_logger")
        self.logger.debug("Initialized XeroServices.")
        self._initialized = True

    def load_spend_money_transactions(
            self,
            project_id: int = None,
            po_number: int = None,
            detail_number: int = None
        ):
            """
            Loads Xero SPEND transactions (bankTransactions) into the 'spend_money' table.

            Args:
                project_id (int, optional): The project ID portion of the reference filter.
                po_number (int, optional): The PO number portion of the reference filter.
                detail_number (int, optional): The detail number portion of the reference filter.

            Usage example:
                database_util = DatabaseOperations()
                xero_api_instance = XeroAPI()
                xero_services = XeroServices(database_util, xero_api_instance)
                xero_services.load_spend_money_transactions(project_id=1234, po_number=101, detail_number=2)
            """
            self.logger.info("Retrieving SPEND transactions from Xero...")
            xero_spend_transactions = self.xero_api.get_spend_money_by_reference(
                project_id=project_id,
                po_number=po_number,
                detail_number=detail_number
            )

            if not xero_spend_transactions:
                self.logger.info("No SPEND transactions returned from Xero for the provided filters.")
                return

            for tx in xero_spend_transactions:
                # Extract fields you care about from each Xero transaction.

                # If Xero indicates it's reconciled, override status
                # Otherwise, default to Xero's status or 'DRAFT'
                if tx.get("IsReconciled", False) is True:
                    current_state = "RECONCILED"
                else:
                    current_state = tx.get("Status", "DRAFT")

                reference_number = tx.get("Reference")
                bank_transaction_id = tx.get("BankTransactionID")

                # Example for storing a link to the transaction in Xero's UI
                xero_link = f"https://go.xero.com/Bank/ViewTransaction.aspx?bankTransactionID={bank_transaction_id}"

                # Check if there's an existing SpendMoney with this reference
                existing_spend = self.database_util.search_spend_money(
                    column_names=["xero_spend_money_reference_number"],
                    values=[reference_number]
                )

                if not existing_spend:
                    # Create a new SpendMoney record
                    created = self.database_util.create_spend_money(
                        xero_spend_money_reference_number=reference_number,
                        xero_link=xero_link,
                        state=current_state,
                    )
                    if created:
                        self.logger.info(
                            f"Created new SpendMoney record for reference={reference_number}, ID={created['id']}."
                        )
                    else:
                        self.logger.error(
                            f"Failed to create SpendMoney for reference={reference_number}."
                        )
                else:
                    # If existing_spend is a list, handle multiple matches or just use the first
                    if isinstance(existing_spend, list):
                        existing_spend = existing_spend[0]

                    spend_money_id = existing_spend["id"]
                    updated = self.database_util.update_spend_money(
                        spend_money_id,
                        state=current_state,
                        xero_link=xero_link
                    )
                    if updated:
                        self.logger.info(
                            f"Updated SpendMoney (id={spend_money_id}) for reference={reference_number}."
                        )
                    else:
                        self.logger.error(
                            f"Failed to update SpendMoney (id={spend_money_id}) for reference={reference_number}."
                        )
                        
xero_services = XeroServices()