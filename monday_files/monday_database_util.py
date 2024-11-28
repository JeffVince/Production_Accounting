# monday_database_util.py

import json
import logging
import os
from datetime import datetime

from dotenv import load_dotenv
from sqlalchemy import create_engine, func
from sqlalchemy.exc import IntegrityError, SQLAlchemyError
from sqlalchemy.orm import sessionmaker

from database.db_util import get_db_session
from database.models import (
    PurchaseOrder,
    DetailItem,
    Contact,
    AicpCode,
)
from monday_api import MondayAPI


class MondayDatabaseUtil:
    def __init__(self):
        # Set up logging
        self.logger = logging.getLogger(self.__class__.__name__)
        logging.basicConfig(level=logging.DEBUG)

        # Load environment variables
        load_dotenv()
        self.monday_api_token = os.getenv("MONDAY_API_TOKEN")

        if not self.monday_api_token:
            self.logger.error("Monday API Token not found. Please set it in the environment variables.")
            raise EnvironmentError("Missing MONDAY_API_TOKEN")

        self.monday_api_url = 'https://api.monday.com/v2'
        self.monday_api = MondayAPI()

    # --------------------- CREATE METHODS ---------------------

    def create_or_update_main_item_in_db(self, item_data):
        """
        Creates or updates a main item record in the database.

        Args:
            item_data (dict): The prepared database creation item containing details.

        Returns:
            str: Status message indicating success, creation, or failure.
        """
        pulse_id = item_data.get("pulse_id")
        with get_db_session() as session:
            try:
                # Check if the item already exists in the database
                po_item = session.query(PurchaseOrder).filter_by(pulse_id=pulse_id).one_or_none()

                if po_item:
                    # Update existing record
                    for db_field, value in item_data.items():
                        if db_field != "pulse_id":  # Skip pulse_id since it's the key
                            setattr(po_item, db_field, value)
                    self.logger.info(f"Updated existing PurchaseOrder with pulse_id: {pulse_id}")
                    status = "Updated"
                else:  # Create a new record
                    new_item = PurchaseOrder(**item_data)
                    session.add(new_item)
                    self.logger.info(f"Created new PurchaseOrder with pulse_id: {pulse_id}")
                    status = "Created"
                # Commit the transaction
                session.commit()
                return status
            except Exception as e:
                session.rollback()
                self.logger.error(f"Error processing PurchaseOrder in DB: {e}")
                return "Fail"

    def create_or_update_sub_item_in_db(self, item_data):
        """
        Creates or updates a subitem record in the database.

        Args:
            item_data (dict): The prepared database creation item containing subitem details.

        Returns:
            dict: Status message indicating success, creation, or failure.
        """
        pulse_id = item_data.get("pulse_id")
        parent_id = item_data.get("parent_id")
        account_number = item_data.get("account_number_id", "5000")  # Default to "5000" if not provided

        with get_db_session() as session:
            try:
                # Fetch the parent PurchaseOrder using the pulse_id
                po_surrogate_id = self.get_purchase_order_surrogate_id_by_pulse_id(parent_id)
                aicp_code_surrogate_id = self.get_aicp_code_surrogate_id(account_number)
                po_type = self.get_purchase_order_type_by_pulse_id(parent_id)
                if not po_surrogate_id:
                    raise ValueError(f"No PurchaseOrder found with pulse_id: {parent_id}")
                if not aicp_code_surrogate_id:
                    raise ValueError(f"No AICP Line found with surrogate id: {account_number}")

                # Set the account number to account surrogate id
                item_data['account_number_id'] = aicp_code_surrogate_id
                # Set the parent_id to the po_surrogate_id
                item_data['parent_id'] = po_surrogate_id
                # Set is receipt?
                if po_type ==  "Vendor":
                    item_data["is_receipt"] = 0
                else:
                    item_data["is_receipt"] = 1

                # Create the new DetailItem
                new_detail_item = DetailItem(**item_data)
                session.add(new_detail_item)
                session.commit()

                self.logger.info(
                    f"Created new DetailItem with pulse_id: {pulse_id}, surrogate_id: {new_detail_item.detail_item_surrogate_id}")
                return {
                    "status": "Created",
                    "detail_item_id": new_detail_item.detail_item_surrogate_id
                }
            except IntegrityError as ie:
                session.rollback()
                self.logger.error(f"IntegrityError processing DetailItem in DB: {ie}")
                return {
                    "status": "Fail",
                    "error": str(ie)
                }
            except Exception as e:
                session.rollback()
                self.logger.error(f"Error processing DetailItem in DB: {e}")
                return {
                    "status": "Fail",
                    "error": str(e)
                }

    def create_or_update_contact_item_in_db(self, item_data):
        """
        Creates or updates a contact record in the database.

        Args:
            item_data (dict): The prepared database creation item containing contact details.

        Returns:
            str: Status message indicating success, creation, or failure.
        """
        pulse_id = item_data.get("pulse_id")

        with get_db_session() as session:
            try:
                # Check if the contact already exists in the database
                contact_item = session.query(Contact).filter_by(pulse_id=pulse_id).one_or_none()

                if contact_item:
                    # Update existing record
                    for db_field, value in item_data.items():
                        if db_field != "pulse_id":  # Skip pulse_id since it's the key
                            setattr(contact_item, db_field, value)
                    self.logger.info(f"Updated existing Contact with pulse_id: {pulse_id}")
                    status = "Updated"
                else:  # Create a new record
                    new_contact = Contact(**item_data)
                    session.add(new_contact)
                    self.logger.info(f"Created new Contact with pulse_id: {pulse_id}")
                    status = "Created"
                # Commit the transaction
                session.commit()
                return status
            except Exception as e:
                session.rollback()
                self.logger.error(f"Error processing Contact in DB: {e}")
                return "Fail"

    # --------------------- READ METHODS ---------------------

    def get_purchase_order_surrogate_id_by_pulse_id(self, pulse_id):
        """
        Retrieve a PurchaseOrder by its pulse_id.

        Args:
            pulse_id (str): The pulse_id of the PurchaseOrder.

        Returns:
            PurchaseOrder or None: The PurchaseOrder object if found, else None.
        """
        with get_db_session() as session:
            return session.query(PurchaseOrder).filter_by(pulse_id=pulse_id).one_or_none().po_surrogate_id

    def get_purchase_order_type_by_pulse_id(self, pulse_id):
        """
        Retrieve a PurchaseOrder by its pulse_id.

        Args:
            pulse_id (str): The pulse_id of the PurchaseOrder.

        Returns:
            PurchaseOrder or None: The PurchaseOrder object if found, else None.
        """
        with get_db_session() as session:
            return session.query(PurchaseOrder).filter_by(pulse_id=pulse_id).one_or_none().po_type

    def get_detail_item_by_pulse_id(self, pulse_id):
        """
        Retrieve a DetailItem by its pulse_id.

        Args:
            pulse_id (str): The pulse_id of the DetailItem.

        Returns:
            DetailItem or None: The DetailItem object if found, else None.
        """
        with get_db_session() as session:
            return session.query(DetailItem).filter_by(detail_item_surrogate_id=pulse_id).first()

    def get_aicp_code_surrogate_id(self, aicp_code):
        """
        Retrieve the AicpCode surrogate ID based on the code.

        Args:
            aicp_code (str): The Aicp code.

        Returns:
            int or None: The corresponding aicp_code_surrogate_id, or None if not found.
        """
        with get_db_session() as session:
            aicp_code_entry = session.query(AicpCode).filter_by(code=aicp_code).one_or_none()
            if aicp_code_entry:
                return aicp_code_entry.aicp_code_surrogate_id
            else:
                return None

    # --------------------- UPDATE METHODS ---------------------

    def update_db_with_sub_item_change(self, change_item):
        """
        Applies the prepared change to the DetailItem in the database.

        Args:
            change_item (dict): The prepared database change item containing 'pulse_id', 'db_field', and 'new_value'.

        Returns:
            str: Status message indicating success or failure.
        """
        pulse_id = change_item.get("pulse_id")
        db_field = change_item.get("db_field")
        new_value = change_item.get("new_value")

        if not all([pulse_id, db_field, new_value]):
            self.logger.error("Incomplete change_item data provided.")
            return "Fail"

        with get_db_session() as session:
            try:
                # Locate the DetailItem using the pulse_id
                detail_item = session.query(DetailItem).filter_by(pulse_id=pulse_id).one_or_none()
                if not detail_item:
                    self.logger.info(f"No DetailItem found with pulse_id {pulse_id}.")
                    return "Not Found"

                # Update the appropriate field
                setattr(detail_item, db_field, new_value)

                # Commit the change
                session.commit()
                self.logger.info(f"Updated DetailItem {pulse_id}: set {db_field} to {new_value}")
                return "Success"
            except Exception as e:
                session.rollback()
                self.logger.error(f"Error updating DetailItem in DB: {e}")
                return "Fail"

    # --------------------- DELETE METHODS ---------------------

    def delete_purchase_order_in_db(self, pulse_id):
        """
        Deletes a PurchaseOrder from the database based on its pulse_id.

        Args:
            pulse_id (str): The pulse_id of the PurchaseOrder to delete.

        Returns:
            bool: True if deletion was successful, False otherwise.
        """
        with get_db_session() as session:
            po = session.query(PurchaseOrder).filter_by(po_surrogate_id=pulse_id).first()
            if not po:
                self.logger.error(f"PurchaseOrder with ID {pulse_id} does not exist.")
                return False
            session.delete(po)
            try:
                session.commit()
                self.logger.info(f"Deleted PurchaseOrder with ID {pulse_id}.")
                return True
            except SQLAlchemyError as e:
                session.rollback()
                self.logger.error(f"Error deleting PurchaseOrder: {e}")
                return False

    def delete_detail_item_in_db(self, pulse_id):
        """
        Deletes a DetailItem from the database based on its pulse_id.

        Args:
            pulse_id (str): The pulse_id of the DetailItem to delete.

        Returns:
            bool: True if deletion was successful, False otherwise.
        """
        with get_db_session() as session:
            detail_item = session.query(DetailItem).filter_by(detail_item_surrogate_id=pulse_id).first()
            if not detail_item:
                self.logger.error(f"DetailItem with ID {pulse_id} does not exist.")
                return False
            session.delete(detail_item)
            try:
                session.commit()
                self.logger.info(f"Deleted DetailItem with ID {pulse_id}.")
                return True
            except SQLAlchemyError as e:
                session.rollback()
                self.logger.error(f"Error deleting DetailItem: {e}")
                return False