import csv
import re
from datetime import datetime

def parse_po_log(file_path):
    main_items = []
    detail_items = []
    current_main_item = None
    detail_item_number = 1  # To assign unique detail item numbers per PurchaseOrder

    with open(file_path, 'r', newline='', encoding='utf-8') as csvfile:
        reader = csv.reader(csvfile, delimiter='\t')

        headers = next(reader)  # Read header row
        headers = [header.strip() for header in headers]

        for row in reader:
            if not any(row):
                continue  # Skip empty rows

            # Remove leading and trailing spaces from each field
            row = [field.strip() for field in row]

            # Check if this is a main item or a detail item
            if re.match(r'^\d+', row[0]):  # Main item starts with a number
                # Reset detail_item_number for new PurchaseOrder
                detail_item_number = 1

                # Map columns to main item fields
                main_item = {
                    'No': row[0],
                    'Phase': row[1],
                    'Date': row[2],
                    'St/Type': row[3],
                    'Pay ID': row[4],
                    'ID': row[5],
                    'Vendor': row[6],
                    'Description': row[7],
                    'Account': row[8] if len(row) > 8 else '',
                    'Actualized $': row[9] if len(row) > 9 else ''
                }
                main_items.append(main_item)
                current_main_item = main_item
            elif re.match(r'^\s*[\d-]*\s*$', row[0]):  # Detail items may have empty or numeric No.
                # Detail item
                if current_main_item is None:
                    continue  # Skip detail items without a main item

                # Map columns to detail item fields
                detail_item = {
                    'Main No': current_main_item['No'],
                    'Detail Item Number': detail_item_number,
                    'Phase': row[0],
                    'Date': row[1],
                    'St/Type': row[2],
                    'Pay ID': row[3],
                    'ID': row[4],
                    'Vendor': row[5],
                    'Description': row[6],
                    'Account': row[7],
                    'Actualized $': row[8] if len(row) > 8 else ''
                }
                detail_items.append(detail_item)
                detail_item_number += 1

    return main_items, detail_items

if __name__ == '__main__':
    # Replace 'po_log.txt' with your actual data file path
    main_items, detail_items = parse_po_log('data.txt')

    # For demonstration purposes, print the parsed data
    print("Main Items:")
    for item in main_items:
        print(item)

    print("\nDetail Items:")
    for item in detail_items:
        print(item)