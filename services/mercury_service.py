import requests
import logging
import database.monday_database_util as db
import os

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("../utilities/logs/create_bills_for_approval.log"),  # Logs to a file
        logging.StreamHandler()  # Also logs to the console
    ]
)

# Mercury API configuration
MERCURY_API_URL = "https://api.mercury.com/api/v1"
API_TOKEN = os.getenv('MERCURY_API_TOKEN')
ACCOUNT_ID = os.getenv('ACCOUNT_ID')


# Function to create a bill for approval
def create_bill_for_approval(main_item, total_amount):
    """
    Creates a bill for approval in Mercury for the given main item and total amount.

    Args:
        main_item (dict): The main item data.
        total_amount (float): The total amount to be paid.
    """
    # Prepare the payload for the API request
    payload = {
        "amount": int(total_amount * 100),  # Amount in cents
        "currency": "USD",
        "description": f"Payment for {main_item['name']}",
        "external_id": main_item['item_id'],
        "receiving_account": {
            "account_number": main_item['account_number'],  # Ensure this field exists in your database
            "routing_number": main_item['routing_number'],  # Ensure this field exists in your database
            "account_type": "checking",
            "account_holder_name": main_item['name']
        }
    }

    # Make the API request to queue the payment
    response = requests.post(
        f"{MERCURY_API_URL}/account/{ACCOUNT_ID}/request-send-money",
        json=payload,
        auth=(API_TOKEN, '')
    )

    if response.status_code == 200:
        logging.info(f"Successfully created bill for approval for main item {main_item['item_id']}.")
    else:
        logging.error(f"Failed to create bill for main item {main_item['item_id']}. Status code: {response.status_code}, Response: {response.text}")


# Main function to process approved items and create bills
def process_approved_items():
    """
    Processes all approved main items and creates bills for approval based on their RTP subitems.
    """
    # Fetch all main items with status 'Approved'
    main_items = db.fetch_main_items_by_status('Approved')
    logging.info(f"Found {len(main_items)} approved main items.")

    for main_item in main_items:
        # Fetch subitems with status 'RTP' for the current main item
        subitems = db.fetch_subitems_by_main_item_and_status(main_item['item_id'], 'RTP')
        total_amount = sum(subitem['amount'] for subitem in subitems if subitem['amount'] is not None)
        logging.info(f"Main item {main_item['item_id']} has {len(subitems)} RTP subitems with a total amount of ${total_amount:.2f}.")

        if total_amount > 0:
            create_bill_for_approval(main_item, total_amount)
        else:
            logging.info(f"No RTP subitems with a positive amount found for main item {main_item['item_id']}.")

if __name__ == "__main__":
    process_approved_items()