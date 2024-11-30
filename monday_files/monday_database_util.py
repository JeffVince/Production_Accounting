# monday_database_util.py
import json
import logging
import os
from datetime import datetime
from typing import Any, Optional, Dict

from dotenv import load_dotenv
from sqlalchemy.exc import IntegrityError, SQLAlchemyError
from database.db_util import get_db_session
from database.models import (
    PurchaseOrder,
    DetailItem,
    Contact,
    AicpCode,
)
from monday_files.monday_util import monday_util
from singleton import SingletonMeta

class MondayDatabaseUtil(metaclass=SingletonMeta):
    def __init__(self):
        if not hasattr(self, '_initialized'):

            # Setup logging and get the configured logger
            self.logger = logging.getLogger("app_logger")

            # Load environment variables
            load_dotenv()
            self.monday_api_token = os.getenv("MONDAY_API_TOKEN")

            if not self.monday_api_token:
                self.logger.error("Monday API Token not found. Please set it in the environment variables.")
                raise EnvironmentError("Missing MONDAY_API_TOKEN")

            self.monday_api_url = 'https://api.monday.com/v2'
            # self.monday_api = MondayAPI()
            self.monday_util = monday_util
            self.DEFAULT_ACCOUNT_NUMBER = "5000"
            self.DEFAULT_AICP_CODE_SURROGATE_ID = 1
            self.logger.info("Monday Database Utility initialized")

            self._initialized = True

    # ----------------------PRE PROCESSING ----------------------

    def prep_main_item_event_for_db_creation(self, event):
        """
        Prepares the Monday event payload into a database creation item.

        Args:
            event (dict): The Monday event payload.

        Returns:
            dict: A dictionary representing the database creation item.
        """
        item = event
        self.logger.debug(f"MAIN ITEM FOR CREATION: {item}")

        # Prepare the database creation item
        creation_item = {
            "pulse_id": int(item["id"]),  # Monday subitem ID
        }

        po_type = "Vendor"  # Default po_type value

        for column in item.get("column_values", []):
            column_id = column.get("id")
            db_field = self.monday_util.MAIN_ITEM_COLUMN_ID_TO_DB_FIELD.get(column_id)

            if db_field:
                # Map the corresponding value to the database field
                value = column.get("text") or column.get("value")

                # Handle cases where value is a JSON string
                if isinstance(value, str):
                    try:
                        value = json.loads(value)
                    except (ValueError, TypeError):
                        pass

                # Special handling for specific fields
                if db_field == "contact_id" and isinstance(value, dict):
                    # Extract the first linkedPulseId if present
                    linked_pulse_ids = value.get("linkedPulseIds", [])
                    value = linked_pulse_ids[0].get("linkedPulseId") if linked_pulse_ids else None

                # Check status to determine po_type
                if db_field == "state" and value == "CC / PC":
                    po_type = "CC / PC"

                # Assign processed value
                creation_item[db_field] = value

        # Add po_type to the creation item
        creation_item["po_type"] = po_type

        self.logger.debug(f"Prepared creation item: {creation_item}")
        return creation_item

    def prep_sub_item_event_for_db_change(self, event: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Prepares the Monday event into a database change item.

        Args:
            event (dict): The Monday event payload.

        Returns:
            dict: A prepared database change item or None if invalid.
        """
        required_keys = ['columnId', 'pulseId', 'changedAt']
        missing_keys = [key for key in required_keys if key not in event]
        if missing_keys:
            self.logger.error(f"Missing keys in event: {missing_keys}")
            return None

        column_id = event.get('columnId')
        column_type = event.get('columnType', 'default')  # Use 'default' if columnType is not provided

        # Determine the database field for the column
        db_field = self.monday_util.SUB_ITEM_COLUMN_ID_TO_DB_FIELD.get(column_id)
        if not db_field:
            self.logger.error(f"Column ID '{column_id}' is not mapped to a database field.")
            return None

        # Get the appropriate handler for the column type
        handler = self.monday_util.get_column_handler(column_type)

        # Process the value using the appropriate handler
        try:
            new_value = handler(event)
        except Exception as e:
            self.logger.error(f"Error processing column '{column_id}' with handler: {e}")
            return None

        # Special handling for 'file_link' column
        if db_field == "file_link":
            new_value = self.verify_url(new_value)

        # Start Preprocessing and Data Validation if necessary
        if db_field == 'account_number_id':
            if new_value is None:
                new_value = self.DEFAULT_ACCOUNT_NUMBER
            if isinstance(new_value, dict):
                new_value = new_value.get("value", self.DEFAULT_ACCOUNT_NUMBER)
            aicp_code_surrogate_id = self.get_aicp_code_surrogate_id(new_value)
            if not aicp_code_surrogate_id:
                self.logger.warning(f"No AICP Line found with surrogate id: {new_value}, setting to default.")
                aicp_code_surrogate_id = self.DEFAULT_AICP_CODE_SURROGATE_ID
            new_value = aicp_code_surrogate_id

        # Construct the change item
        try:
            pulse_id = int(event.get('pulseId'))
        except (ValueError, TypeError) as e:
            self.logger.error(f"Invalid pulseId: {event.get('pulseId')}, error: {e}")
            return None  # Or handle accordingly

        changed_at_timestamp = event.get('changedAt')
        try:
            changed_at = datetime.fromtimestamp(changed_at_timestamp) if changed_at_timestamp else datetime.utcnow()
        except (ValueError, TypeError) as e:
            self.logger.error(f"Invalid changedAt timestamp: {changed_at_timestamp}, error: {e}")
            changed_at = datetime.utcnow()

        change_item = {
            "pulse_id": pulse_id,  # Monday pulse ID (subitem ID)
            "db_field": db_field,  # Corresponding DB field
            "new_value": new_value,  # Extracted and processed new value
            "changed_at": changed_at,
        }

        self.logger.debug(f"Prepared change item: {change_item}")
        return change_item

    def prep_sub_item_event_for_db_creation(self, event):
        """
        Prepares the Monday event payload into a database creation item.

        Args:
            event (dict): The Monday event payload.

        Returns:
            dict: A dictionary representing the database creation item, or None if orphan.
        """
        item = event
        self.logger.debug(f"ITEM DATA: {item}")
        parent_id = item["parent_item"]['id']

        # Remove orphan detail items
        if not parent_id:
            self.logger.warning("Orphan detail item detected; skipping creation.")
            return None

        # Prepare the database creation item
        creation_item = {
            "pulse_id": int(item["id"]),
            "parent_id": parent_id
        }

        for column in item.get("column_values", []):
            column_id = column.get("id")
            db_field = self.monday_util.SUB_ITEM_COLUMN_ID_TO_DB_FIELD.get(column_id)
            if db_field:
                if db_field == "file_link":
                    if column.get("value"):
                        value = json.loads(column.get("value")).get("url")
                    else:
                        value = ""
                else:
                    value = column.get("text") or column.get("value")
                if isinstance(value, str):
                    try:
                        value = json.loads(value)
                    except (ValueError, TypeError):
                        pass
                # Convert 'quantity' to double if it's a numeric string
                if db_field == 'quantity' and isinstance(value, str):
                    try:
                        value = float(value)
                    except ValueError:
                        self.logger.warning(f"Invalid quantity value: {value}, setting to 0")
                        value = 0  # Default value or handle as needed
                creation_item[db_field] = value

        # Start Preprocessing and Data Validation
        account_number = creation_item.get("account_number_id")
        if account_number is None:
            account_number = "5000"
        if isinstance(account_number, dict):
            account_number = account_number.get("value")

        # Fetch the parent PurchaseOrder surrogate ID using the pulse_id
        po_surrogate_id = self.get_purchase_order_surrogate_id_by_pulse_id(parent_id)
        if not po_surrogate_id:
            self.logger.debug(f"No PurchaseOrder found with pulse_id: {parent_id}")
            return None  # Or handle as per your requirements

        # Get the AICP Code surrogate ID
        aicp_code_surrogate_id = self.get_aicp_code_surrogate_id(account_number)
        if not aicp_code_surrogate_id:
            self.logger.warning(f"No AICP Line found with surrogate id: {account_number}, setting to default.")
            aicp_code_surrogate_id = self.DEFAULT_AICP_CODE_SURROGATE_ID

        # Get the Purchase Order type
        po_type = self.get_purchase_order_type_by_pulse_id(parent_id)

        # Update creation_item with processed values
        creation_item['account_number_id'] = aicp_code_surrogate_id
        creation_item['parent_id'] = po_surrogate_id
        creation_item["is_receipt"] = 0 if po_type == "Vendor" else 1

        self.logger.debug(f"Prepared subitem creation item: {creation_item}")
        return creation_item

    def prep_contact_event_for_db_creation(self, event):
        """
        Prepares the Monday event payload into a database creation item.

        Args:
            event (dict): The Monday event payload.

        Returns:
            dict: A dictionary representing the database creation item.
        """
        if not event or "id" not in event:
            self.logger.error(f"Invalid event structure: {event}")
            raise ValueError("Invalid event structure. Ensure the event payload is properly formatted.")

        # Extract the first item from the event payload (assuming a single contact event)
        item = event
        self.logger.debug(f"CONTACT ITEM DATA: {item}")

        # Prepare the database creation item
        creation_item = {
            "pulse_id": int(item["id"]),
            "name": item.get('name')
        }

        for column in item.get("column_values", []):
            column_id = column.get("id")
            db_field = self.monday_util.CONTACT_COLUMN_ID_TO_DB_FIELD.get(column_id)

            if db_field:
                # Map the corresponding value to the database field
                value = column.get("text") or column.get("value")

                # Handle cases where value is a JSON string
                if isinstance(value, str):
                    try:
                        value = json.loads(value)
                    except (ValueError, TypeError):
                        pass

                creation_item[db_field] = value

        self.logger.debug(f"Prepared contact creation item: {creation_item}")
        return creation_item

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

        with get_db_session() as session:
            try:
                # Check if detail item exists
                detail_item = session.query(DetailItem).filter_by(pulse_id=pulse_id).one_or_none()
                # If exists, update it
                if detail_item:
                    for db_field, value in item_data.items():
                        if value is not None:
                            setattr(detail_item, db_field, value)
                    session.commit()
                    self.logger.debug(f"Updated existing DetailItem with pulse_id: {pulse_id}")
                    return {
                        "status": "Updated",
                    }
                else:
                    # Create a new DetailItem
                    new_detail_item = DetailItem(**item_data)
                    session.add(new_detail_item)
                    session.commit()
                    self.logger.info(
                        f"Created new DetailItem with pulse_id: {pulse_id}, surrogate_id: {new_detail_item.detail_item_surrogate_id}")
                    return {
                        "status": "Created",
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
            po_item = session.query(PurchaseOrder).filter_by(pulse_id=pulse_id).one_or_none()
            if not po_item:
                return None
            else:
                return po_item.po_surrogate_id

    def get_purchase_order_type_by_pulse_id(self, pulse_id):
        """
        Retrieve a PurchaseOrder by its pulse_id.

        Args:
            pulse_id (str): The pulse_id of the PurchaseOrder.

        Returns:
            PurchaseOrder or None: The PurchaseOrder object if found, else None.
        """
        with get_db_session() as session:
            po_item = session.query(PurchaseOrder).filter_by(pulse_id=pulse_id).one_or_none()
            if not po_item:
                return None
            return po_item.po_type

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

        if not all([pulse_id, db_field]):
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

    # --------------------- HELPER METHODS ----------------------

    def verify_url(self, string_value):
        """
        Extracts the 'url' from a  string.

        Args:
            str (str): The  string containing the URL.

        Returns:
            str: The extracted URL or an empty string if not found or invalid.
            :param string_value:
        """
        if not string_value or not isinstance(string_value, str):
            self.logger.debug("file_link is empty or not a string.")
            return ""
        try:
            url = string_value
            self.logger.debug(f"Extracted URL from file_link: {url}")
            return url
        except (ValueError, TypeError) as e:
            self.logger.warning(f"Error parsing file_link: {e}")
            return ""


monday_database_util = MondayDatabaseUtil()
