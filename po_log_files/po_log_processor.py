import csv
import logging
import re
from datetime import datetime

from utilities.singleton import SingletonMeta


class POLogProcessor(metaclass=SingletonMeta):
    def __init__(self):
        if not hasattr(self, '_initialized'):
            # Set up logging
            self.logger = logging.getLogger("app_logger")
            self.logger.info("PO Log Processor  initialized")
            self._initialized = True

    def parse_po_log_main_items(self, file_path):
        main_items = []

        with open(file_path, 'r', newline='', encoding='utf-8') as csvfile:
            reader = csv.reader(csvfile, delimiter='\t')

            headers = next(reader)  # Read header row
            headers = [header.strip() for header in headers]

            for row in reader:
                if not any(row):
                    continue  # Skip empty rows
                try:
                    test = row[7], row[8], row[9]
                except Exception as e:
                    self.logger.warning(f"Main Item Not filled out: {e}")
                    continue

                # Remove leading and trailing spaces from each field
                row = [field.strip() for field in row]

                # Check if this is a main item or a detail item
                if re.match(r'^\d+', row[0]):  # Main item starts with a number
                    # Map columns to main item fields
                    main_item = {
                        'PO': int(row[0]),
                        'Date': row[2],
                        'St/Type': row[3],
                        'Vendor': row[6],
                        'Purpose': row[7],
                        'Actualized $': row[9] if len(row) > 9 else ''
                    }
                    main_items.append(main_item)

        return main_items

    def parse_po_log_sub_items(self, file_path):
        main_items = []
        detail_items = []
        current_main_item = None

        with open(file_path, 'r', newline='', encoding='utf-8') as csvfile:
            reader = csv.reader(csvfile, delimiter='\t')

            headers = next(reader)  # Read header row
            headers = [header.strip() for header in headers]

            for row in reader:
                if not any(row):
                    continue  # Skip empty rows
                try:
                    test = row[7], row[8], row[9]
                except Exception as e:
                    self.logger.warning(f"Main Item Not filled out: {e}")
                    continue

                # Remove leading and trailing spaces from each field
                row = [field.strip() for field in row]

                # Check if this is a main item or a detail item
                if re.match(r'^\d+', row[0]):  # Main item starts with a number
                    # Reset detail_item_number for new PurchaseOrder
                    detail_item_number = 1

                    # Map columns to main item fields
                    main_item = {
                        'PO': int(row[0]),
                        'Date': row[2],
                        'St/Type': row[3],
                        'Vendor': row[6],
                    }
                    main_items.append(main_item)
                    current_main_item = main_item
                elif re.match(r'^\s*[\d-]*\s*$', row[0]):  # Detail items may have empty or numeric No.
                    # Detail item
                    if current_main_item is None:
                        continue  # Skip detail items without a main item

                    # Map columns to detail item fields
                    detail_item = {
                        'PO': current_main_item['PO'],
                        'Detail Item Number': row[5],
                        'Phase': row[1],
                        'Date': row[2],
                        'Pay ID': row[4],
                        'Payment Type': row[3],
                        'Vendor': row[6],
                        'Description': row[7],
                        'Account': row[8],
                        'Actualized $': row[9] if len(row) > 8 else ''
                    }

                    if not detail_item['Description'] == "TOTAL":
                        detail_items.append(detail_item)

        return detail_items

    def get_contacts_list(self, main_items, detail_items):
        contact_list = []
        for item in main_items:
            if not item.get("Vendor") == "Petty Cash" and not item.get("Vendor").__contains__("PLACEHOLDER"):
                crd = False
                for detail in detail_items:
                    if item.get("PO") == detail.get("PO"):
                        if detail.get("Payment Type") == "CRD":
                            crd = True
                if not crd:
                    contact_list.append({
                        "name": item.get("Vendor"),
                        "PO": item.get("PO")
                    })
                    crd = False
                else:
                    contact_list.append({
                        "name": "Ophelia Credit Card",
                        "PO": item.get("PO")
                    })

        return contact_list


po_log_processor = POLogProcessor()
