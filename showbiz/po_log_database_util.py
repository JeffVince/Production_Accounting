import json
import logging
import os
from datetime import datetime
from decimal import Decimal
from typing import Any, Optional, Dict, List

from dotenv import load_dotenv
from sqlalchemy.exc import IntegrityError, SQLAlchemyError
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine

from database.db_util import get_db_session
from database.models import (
    PurchaseOrder,
    DetailItem,
    Contact,
    AicpCode,
    TaxAccount,
    Project,
)
from file_processor import parse_po_log, preprocess_data


class PoLogDatabaseUtil:
    def __init__(self):
        # Set up logging
        self.logger = logging.getLogger(self.__class__.__name__)
        logging.basicConfig(level=logging.INFO)

        # Load environment variables
        load_dotenv()
        self.DEFAULT_ACCOUNT_NUMBER = "5000"
        self.DEFAULT_AICP_CODE_SURROGATE_ID = 1
        self.DEFAULT_TAX_CODE_ID = 1
        self.DEFAULT_PROJECT_ID = 1  # Replace with actual project_id if available

        # Database connection string (update with your actual credentials)
        self.DATABASE_URI = os.getenv('DATABASE_URI', 'mysql+pymysql://username:password@localhost:3306/your_database')
        self.engine = create_engine(self.DATABASE_URI)
        self.Session = sessionmaker(bind=self.engine)

    # ---------------------- PREPROCESSING ----------------------

    def map_po_log_to_db_fields(self, main_items: List[Dict[str, Any]], detail_items: List[Dict[str, Any]]):
        """
        Maps PO_log columns to database fields for main items and detail items.

        Args:
            main_items (List[Dict[str, Any]]): List of main items from the PO_log.
            detail_items (List[Dict[str, Any]]): List of detail items from the PO_log.

        Returns:
            Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]: Mapped main and detail items.
        """
        mapped_main_items = []
        mapped_detail_items = []

        # Map main items
        for item in main_items:
            mapped_item = {
                'po_number': int(item.get('No', 0)),
                'state': item.get('Phase', 'PENDING'),
                'po_type': item.get('St/Type', 'Standard'),
                'description': item.get('Description', ''),
                'amount_total': item.get('Actualized $', Decimal('0.00')),
                'contact_name': item.get('Vendor', '').strip(),
                'date': item.get('Date'),
            }
            mapped_main_items.append(mapped_item)

        # Map detail items
        for item in detail_items:
            mapped_item = {
                'parent_po_number': int(item.get('Main No', 0)),
                'detail_item_number': item.get('Detail Item Number', 1),
                'transaction_date': item.get('Date'),
                'state': item.get('St/Type', 'PENDING'),
                'description': item.get('Description', ''),
                'rate': item.get('Actualized $', Decimal('0.00')),
                'account_code': item.get('Account', '').strip(),
                'quantity': Decimal('1.00'),  # Default quantity
                'vendor_name': item.get('Vendor', '').strip(),
            }
            mapped_detail_items.append(mapped_item)

        return mapped_main_items, mapped_detail_items

    # --------------------- CREATE OR UPDATE METHODS ---------------------

    def create_or_update_purchase_orders(self, main_items: List[Dict[str, Any]]):
        """
        Creates or updates PurchaseOrder records in the database.

        Args:
            main_items (List[Dict[str, Any]]): List of mapped main items.

        Returns:
            None
        """
        with self.Session() as session:
            for item in main_items:
                po_number = item['po_number']
                project_id = self.DEFAULT_PROJECT_ID

                # Get or create Contact
                contact_name = item.get('contact_name')
                contact = None
                if contact_name:
                    contact = session.query(Contact).filter(Contact.name == contact_name).first()
                    if not contact:
                        contact = Contact(
                            name=contact_name,
                            payment_details='PENDING',
                        )
                        session.add(contact)
                        session.flush()

                # Check if PurchaseOrder exists
                po = session.query(PurchaseOrder).filter(
                    PurchaseOrder.project_id == project_id,
                    PurchaseOrder.po_number == po_number
                ).first()

                if po:
                    # Update existing PurchaseOrder
                    po.state = item.get('state', po.state)
                    po.po_type = item.get('po_type', po.po_type)
                    po.description = item.get('description', po.description)
                    po.amount_total = item.get('amount_total', po.amount_total)
                    po.contact_id = contact.contact_surrogate_id if contact else po.contact_id
                else:
                    # Create new PurchaseOrder
                    po = PurchaseOrder(
                        po_number=po_number,
                        project_id=project_id,
                        state=item.get('state', 'PENDING'),
                        po_type=item.get('po_type', 'Standard'),
                        description=item.get('description', ''),
                        amount_total=item.get('amount_total', Decimal('0.00')),
                        contact_id=contact.contact_surrogate_id if contact else None,
                    )
                    session.add(po)
                    session.flush()

                # Map the PurchaseOrder surrogate ID for detail items
                item['po_surrogate_id'] = po.po_surrogate_id

            session.commit()

    def create_or_update_detail_items(self, detail_items: List[Dict[str, Any]], po_dict: Dict[int, int]):
        """
        Creates or updates DetailItem records in the database.

        Args:
            detail_items (List[Dict[str, Any]]): List of mapped detail items.
            po_dict (Dict[int, int]): Mapping of po_number to po_surrogate_id.

        Returns:
            None
        """
        with self.Session() as session:
            for item in detail_items:
                parent_po_number = item['parent_po_number']
                po_surrogate_id = po_dict.get(parent_po_number)
                if not po_surrogate_id:
                    self.logger.warning(f"No PurchaseOrder found for po_number {parent_po_number}")
                    continue

                detail_item_number = item['detail_item_number']

                # Check if DetailItem exists
                detail = session.query(DetailItem).filter(
                    DetailItem.parent_id == po_surrogate_id,
                    DetailItem.detail_item_number == detail_item_number
                ).first()

                # Get or create AicpCode
                account_code_str = item.get('account_code', self.DEFAULT_ACCOUNT_NUMBER)
                aicp_code = session.query(AicpCode).filter(AicpCode.code == account_code_str).first()
                if not aicp_code:
                    aicp_code = AicpCode(
                        code=account_code_str,
                        description='',
                        tax_code_id=self.DEFAULT_TAX_CODE_ID,
                    )
                    session.add(aicp_code)
                    session.flush()

                # Get or create Contact (Vendor)
                vendor_name = item.get('vendor_name')
                vendor_contact = None
                if vendor_name:
                    vendor_contact = session.query(Contact).filter(Contact.name == vendor_name).first()
                    if not vendor_contact:
                        vendor_contact = Contact(
                            name=vendor_name,
                            payment_details='PENDING',
                        )
                        session.add(vendor_contact)
                        session.flush()

                if detail:
                    # Update existing DetailItem
                    detail.transaction_date = item.get('transaction_date', detail.transaction_date)
                    detail.description = item.get('description', detail.description)
                    detail.rate = item.get('rate', detail.rate)
                    detail.quantity = item.get('quantity', detail.quantity)
                    detail.account_number_id = aicp_code.aicp_code_surrogate_id
                    detail.state = item.get('state', detail.state)
                else:
                    # Create new DetailItem
                    detail = DetailItem(
                        transaction_date=item.get('transaction_date'),
                        description=item.get('description', ''),
                        rate=item.get('rate', Decimal('0.00')),
                        quantity=item.get('quantity', Decimal('1.00')),
                        parent_id=po_surrogate_id,
                        detail_item_number=detail_item_number,
                        account_number_id=aicp_code.aicp_code_surrogate_id,
                        state=item.get('state', 'PENDING'),
                    )
                    session.add(detail)

            session.commit()

    def process_po_log_data(self, main_items: List[Dict[str, Any]], detail_items: List[Dict[str, Any]]):
        """
        Processes the PO_log data and updates the database accordingly.

        Args:
            main_items (List[Dict[str, Any]]): List of main items.
            detail_items (List[Dict[str, Any]]): List of detail items.

        Returns:
            None
        """
        # Map the PO_log columns to DB fields
        mapped_main_items, mapped_detail_items = self.map_po_log_to_db_fields(main_items, detail_items)

        # Preprocess data (convert dates, amounts, etc.)
        mapped_main_items, mapped_detail_items = preprocess_data(mapped_main_items, mapped_detail_items)

        # Create or update PurchaseOrders
        self.create_or_update_purchase_orders(mapped_main_items)

        # Create a mapping of po_number to po_surrogate_id
        po_dict = {item['po_number']: item['po_surrogate_id'] for item in mapped_main_items}

        # Create or update DetailItems
        self.create_or_update_detail_items(mapped_detail_items, po_dict)

    # ---------------------- MAIN EXECUTION ----------------------

    def run(self, file_path: str):
        """
        Main method to process the PO_log file and update the database.

        Args:
            file_path (str): Path to the PO_log text file.

        Returns:
            None
        """
        # Parse the PO_log file
        main_items, detail_items = parse_po_log(file_path)

        # Process the data and update the database
        self.process_po_log_data(main_items, detail_items)


if __name__ == '__main__':
    # Replace 'po_log.txt' with your actual data file path
    po_log_file_path = 'po_log.txt'

    # Initialize the PoLogDatabaseUtil
    po_log_util = PoLogDatabaseUtil()

    # Run the processing
    po_log_util.run(po_log_file_path)