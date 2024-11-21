import utilities.monday_util as monday
import database.monday_database_util as db
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("../utilities/logs/monday_po_service.log"),  # Logs to a file
        logging.StreamHandler()  # Also logs to the console
    ]
)

# Initialize the database
logging.info("Initializing database...")
db.initialize_database()
logging.info("Database initialized successfully.")

# Fetch data from Monday.com and store in the database
def sync_monday_data_to_db():
    """
    Syncs data from Monday.com to the local SQLite database using existing `Monday_util` functions.
    """
    logging.info("Starting sync process for Monday.com data...")
    try:
        # Fetch all main items from the Monday.com PO board
        main_items = monday.get_all_items_from_board(monday.PO_BOARD_ID)
        logging.info(f"Fetched {len(main_items)} main items from Monday.com.")

        for item in main_items:
            # Map item data for insertion into the main_items table
            item_data = {
                "item_id": item['id'],
                "name": item['name'],
                "project_id": next((col['text'] for col in item['column_values'] if col['id'] == monday.PO_PROJECT_ID_COLUMN), None),
                "numbers": next((col['text'] for col in item['column_values'] if col['id'] == monday.PO_NUMBER_COLUMN), None),
                "description": next((col['text'] for col in item['column_values'] if col['id'] == monday.PO_DESCRIPTION_COLUMN_ID), None),
                "tax_form": next((col['text'] for col in item['column_values'] if col['id'] == monday.PO_TAX_COLUMN_ID), None),
                "folder": next((col['text'] for col in item['column_values'] if col['id'] == monday.PO_FOLDER_LINK_COLUMN_ID), None),
                "amount": next((col['text'] for col in item['column_values'] if col['id'] == "subitems_sub_total"), None),
                "po_status": next((col['text'] for col in item['column_values'] if col['id'] == monday.PO_STATUS_COLUMN_ID), None),
                "producer_pm": next((col['text'] for col in item['column_values'] if col['id'] == "people"), None),
                "updated_date": next((col['text'] for col in item['column_values'] if col['id'] == "date01"), None),
            }
            db.insert_main_item(item_data)
            logging.info(f"Inserted main item: {item_data['item_id']} - {item_data['name']}")

            # Fetch subitems for the current main item
            subitems = monday.get_all_subitems_for_item(item['id'])
            logging.info(f"Fetched {len(subitems)} subitems for main item {item['id']}.")

            for subitem in subitems:
                # Map subitem data for insertion into the sub_items table
                subitem_data = {
                    "subitem_id": subitem['id'],
                    "main_item_id": item['id'],
                    "status": next((col['text'] for col in subitem['column_values'] if col['id'] == monday.SUBITEM_STATUS_COLUMN_ID), None),
                    "invoice_number": next((col['text'] for col in subitem['column_values'] if col['id'] == monday.SUBITEM_ID_COLUMN_ID), None),
                    "description": next((col['text'] for col in subitem['column_values'] if col['id'] == monday.SUBITEM_DESCRIPTION_COLUMN_ID), None),
                    "amount": next((col['text'] for col in subitem['column_values'] if col['id'] == monday.SUBITEM_RATE_COLUMN_ID), None),
                    "quantity": next((col['text'] for col in subitem['column_values'] if col['id'] == monday.SUBITEM_QUANTITY_COLUMN_ID), None),
                    "account_number": next((col['text'] for col in subitem['column_values'] if col['id'] == monday.SUBITEM_ACCOUNT_NUMBER_COLUMN_ID), None),
                    "invoice_date": next((col['text'] for col in subitem['column_values'] if col['id'] == monday.SUBITEM_DATE_COLUMN_ID), None),
                    "link": next((col['text'] for col in subitem['column_values'] if col['id'] == monday.SUBITEM_LINK_COLUMN_ID), None),
                    "due_date": next((col['text'] for col in subitem['column_values'] if col['id'] == monday.SUBITEM_DUE_DATE_COLUMN_ID), None),
                    "creation_log": next((col['text'] for col in subitem['column_values'] if col['id'] == "creation_log__1"), None),
                }
                db.insert_subitem(subitem_data)
                logging.info(f"Inserted subitem: {subitem_data['subitem_id']} - {subitem_data['description']}")

        logging.info("Data successfully synced from Monday.com to the database.")
    except Exception as e:
        logging.error(f"Error syncing data: {e}")

# Retrieve data from the database
def get_all_data():
    """
    Retrieves all data from the database, including main items and their subitems.

    Returns:
        dict: A dictionary with main items and their subitems.
    """
    all_data = {}
    try:
        main_items = db.fetch_all_main_items()
        logging.info(f"Retrieved {len(main_items)} main items from the database.")

        for main_item in main_items:
            main_item_id = main_item['item_id']
            subitems = db.fetch_subitems_for_main_item(main_item_id)
            logging.info(f"Retrieved {len(subitems)} subitems for main item {main_item_id}.")
            all_data[main_item_id] = {
                "main_item": main_item,
                "subitems": subitems
            }

        logging.info("Data retrieved successfully from the database.")
    except Exception as e:
        logging.error(f"Error retrieving data from the database: {e}")
    return all_data

# Example: Sync and retrieve data
if __name__ == "__main__":
    # Sync Monday.com data to the database
    sync_monday_data_to_db()

    # Retrieve and print all data from the database
    data = get_all_data()
    for main_item_id, details in data.items():
        print(f"Main Item ID: {main_item_id}")
        print("Main Item Details:")
        print(details["main_item"])
        print("Subitems:")
        for subitem in details["subitems"]:
            print(subitem)
        print("\n")