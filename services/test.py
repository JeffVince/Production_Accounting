import os

# List of filenames to create
file_names = [
    "po_log_service.py",
    "dropbox_service.py",
    "monday_service.py",
    "vendor_service.py",
    "tax_form_service.py",
    "ocr_service.py",
    "validation_service.py",
    "xero_service.py",
    "mercury_service.py",
    "payment_backpropagation_service.py",
    "po_modification_service.py",
    "reconciliation_service.py",
    "spend_money_service.py"
]

# Iterate over the list and create each file
for file_name in file_names:
    try:
        # Create the file
        with open(file_name, 'w') as f:
            f.write("# This is the {} file\n".format(file_name))
        print(f"Created: {file_name}")
    except Exception as e:
        print(f"Failed to create {file_name}: {e}")