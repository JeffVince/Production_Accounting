# log_printer_cron.py

import os
import subprocess
import time
import logging
import re
import requests

import dropbox
import psutil
import pyautogui
from AppKit import NSWorkspace

import cv2

# Import Monday_util and file_util modules
from utilities.monday_util import (
    MONDAY_API_URL,
    PO_BOARD_ID,
    MONDAY_API_TOKEN
)

from webhook.dropbox_webhook_handler import (
    get_dropbox_client,
    create_share_link
)


# Configure logging
logging.basicConfig(
    level=logging.INFO,  # Change to DEBUG for more detailed logs
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("../utilities/logs/showbiz_budgeting.log"),
        logging.StreamHandler()
    ]
)

# Constants
PROGRAM_NAME = "Showbiz Budgeting"
PRINT_WINDOW_TITLE = "Print"  # Title of the Print dialog window

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
OTHER_REPORTS_BUTTON_IMAGE = os.path.join(SCRIPT_DIR, 'pics', 'other_reports.png')
CHECKBOX_IMAGES = {
    'PO_LOG_CHECKBOX': {
        'checked': os.path.join(SCRIPT_DIR, 'pics', 'po_log.png'),
        'unchecked': os.path.join(SCRIPT_DIR, 'pics', 'uncheck_po_log.png')  # Optional
    },
    'SHOW_POS_CHECKBOX': {
        'checked': os.path.join(SCRIPT_DIR, 'pics', 'show_po.png'),
        'unchecked': os.path.join(SCRIPT_DIR, 'pics', 'uncheck_show_po.png')  # Optional
    },
    'SHOW_ACTUALIZED_ITEMS_CHECKBOX': {
        'checked': os.path.join(SCRIPT_DIR, 'pics', 'show_actuals.png'),
        'unchecked': os.path.join(SCRIPT_DIR, 'pics', 'uncheck_show_actual.png')  # Optional
    }
}

BUTTON_IMAGES = {
    'PREVIEW': os.path.join(SCRIPT_DIR, 'pics', 'preview.png'),
    'RAW_TEXT': os.path.join(SCRIPT_DIR, 'pics', 'raw_text.png'),
    'OKAY': os.path.join(SCRIPT_DIR, 'pics', 'okay.png'),
    'SAVE_AS_FORM': os.path.join(SCRIPT_DIR, 'pics', 'save_as_form.png'),
    'SAVE': os.path.join(SCRIPT_DIR, 'pics', 'save.png'),
    'CLOSE': os.path.join(SCRIPT_DIR, 'pics', 'close.png')
}

SLEEP_TIME_OPEN = 5  # Seconds to wait after opening the program
SLEEP_TIME_AFTER_KEYSTROKE = 1  # Seconds to wait after sending keystroke
SLEEP_TIME_AFTER_CLICK = 2  # Seconds to wait after clicking the button
RETRY_LIMIT = 3  # Maximum number of retry attempts

# Define the size of the region to capture around each checkbox (width, height)
REGION_SIZE = (20, 20)  # Adjust as needed

# OpenCV Template Matching Parameters
MATCHING_METHOD = cv2.TM_CCOEFF_NORMED
MATCHING_THRESHOLD = 0.7

pyautogui.FAILSAFE = True
pyautogui.PAUSE = 0.5

def convert_templates_to_grayscale():
    """
    Convert all checkbox template images to grayscale and save them.
    """
    for checkbox_name, images in CHECKBOX_IMAGES.items():
        for state, image_path in images.items():
            image = cv2.imread(image_path)
            if image is None:
                logging.error(f"Failed to load template image from '{image_path}'.")
                continue
            gray_image = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)
            cv2.imwrite(image_path, gray_image)
            logging.info(f"Converted '{image_path}' to grayscale.")

def is_program_running(program_name):
    """
    Check if a program is currently running.

    Args:
        program_name (str): The name of the program to check.

    Returns:
        bool: True if the program is running, False otherwise.
    """
    try:
        running = any(proc.info['name'] == program_name for proc in psutil.process_iter(['name']))
        logging.debug(f"Program '{program_name}' running: {running}")
        return running
    except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess) as e:
        logging.error(f"Error checking if program is running: {e}")
        return False

def click_button(image_path):
    """
    Locate and click a button specified by the image path.

    Args:
        image_path (str): Path to the image of the button.

    Returns:
        bool: True if the button was found and clicked, False otherwise.
    """
    location = pyautogui.locateOnScreen(image_path, confidence=0.8)
    if location:
        center_x, center_y = pyautogui.center(location)
        x, y = adjust_coordinates_for_retina(center_x, center_y)
        pyautogui.moveTo(x, y, duration=0.5)
        pyautogui.click()
        logging.info(f"Clicked button at ({x}, {y}) using image: {image_path}.")
        return True
    else:
        logging.error(f"Button image '{image_path}' not found.")
        return False

def type_in_field(text):
    """
    Clear the currently selected field's contents and type the provided text.

    Args:
        text (str): Text to type into the selected field.

    Returns:
        bool: True if the text was cleared and typed successfully, False otherwise.
    """
    try:
        # Delete any existing text
        pyautogui.press('backspace')
        time.sleep(0.2)  # Wait for the text to be cleared

        # Type the new text
        pyautogui.typewrite(text, interval=0.1)
        logging.info(f"Typed text '{text}' into the field.")
        return True
    except Exception as e:
        logging.error(f"Failed to type text '{text}' into the field: {e}")
        return False

def bring_to_front(app_name):
    """
    Bring the specified application to the foreground.

    Args:
        app_name (str): The name of the application to bring to front.
    """
    workspace = NSWorkspace.sharedWorkspace()
    apps = workspace.runningApplications()
    app = next((app for app in apps if app.localizedName() == app_name), None)

    if app:
        app.activateWithOptions_(1)  # NSApplicationActivateIgnoringOtherApps
        logging.info(f"Brought '{app_name}' to the front.")
        time.sleep(1)  # Ensure the app is in focus
    else:
        logging.warning(f"Application '{app_name}' not found.")

def is_file_open_by_window_title(file_name, retries=3, delay=1):
    """
    Check if a window with the specified file name is open in Showbiz Budgeting.

    Args:
        file_name (str): The name of the file (without path).
        retries (int): Number of retries if window title detection fails.
        delay (int): Delay in seconds between retries.

    Returns:
        bool: True if a window with the file name is open, False otherwise.
    """
    for attempt in range(retries):
        bring_to_front("Showbiz Budgeting")  # Bring to the front before each check
        script = f'''
        tell application "System Events"
            tell process "Showbiz Budgeting"
                set windowNames to name of windows
                repeat with winName in windowNames
                    if winName contains "{file_name}" then
                        return "true"
                    end if
                end repeat
                return "false"
            end tell
        end tell
        '''
        try:
            result = subprocess.run(['osascript', '-e', script], capture_output=True, text=True)
            output = result.stdout.strip().lower()
            if output == "true":
                logging.info(f"File '{file_name}' detected as open.")
                return True
            else:
                logging.info(f"Attempt {attempt + 1}/{retries}: File '{file_name}' not detected as open.")
        except subprocess.CalledProcessError as e:
            logging.error(f"Failed to check window titles on attempt {attempt + 1}: {e}")

        time.sleep(delay)  # Wait before retrying

    logging.info(f"File '{file_name}' not detected as open after {retries} attempts.")
    return False

def open_file(file_path):
    """
    Open the specified file using the default application.

    Args:
        file_path (str): The path to the file to open.
        /Users/haske107/Library/CloudStorage/Dropbox-OpheliaLLC/2024
    """
    try:
        # Remove leading slash from file_path if it exists
        file_path = file_path.lstrip("/")
        full_path = os.path.join("/Users/haske107/Library/CloudStorage/Dropbox-OpheliaLLC/2024", file_path)
        subprocess.run(['open', full_path], check=True)
        logging.info(f"Opened file '{file_path}'.")
    except subprocess.CalledProcessError as e:
        logging.error(f"Failed to open file '{full_path}': {e}")

def send_keystroke(command, modifier):
    """
    Send a keystroke using AppleScript.

    Args:
        command (str): The key to press.
        modifier (str): The modifier key (e.g., 'command', 'option').
    """
    script = f'''
    tell application "System Events"
        keystroke "{command}" using {modifier} down
    end tell
    '''
    try:
        subprocess.run(['osascript', '-e', script], check=True)
        logging.info(f"Sent keystroke '{modifier} + {command}'.")
    except subprocess.CalledProcessError as e:
        logging.error(f"Failed to send keystroke: {e}")

def adjust_coordinates_for_retina(x, y):
    """
    Adjust coordinates for Retina displays.

    Args:
        x (int): Original X-coordinate.
        y (int): Original Y-coordinate.

    Returns:
        tuple: Adjusted (x, y) coordinates.
    """
    adjusted_x = x / 2
    adjusted_y = y / 2
    return adjusted_x, adjusted_y

def is_print_window_open(window_title):
    """
    Check if the Print dialog window is currently open.

    Args:
        window_title (str): The title of the window to check.

    Returns:
        bool: True if the window is open, False otherwise.
    """
    script = f'''
    tell application "System Events"
        tell process "{PROGRAM_NAME}"
            if (exists window "{window_title}") then
                return "open"
            else
                return "closed"
            end if
        end tell
    end tell
    '''
    try:
        result = subprocess.run(['osascript', '-e', script], capture_output=True, text=True, check=True)
        status = result.stdout.strip()
        logging.debug(f"Print window '{window_title}' status: {status}")
        return status == "open"
    except subprocess.CalledProcessError as e:
        logging.error(f"Failed to check window status: {e}")
        return False

def click_other_reports_button(image_path):
    """
    Locate and click the "Other Reports" button on the screen.

    Args:
        image_path (str): Path to the "Other Reports" button image.

    Returns:
        tuple: (x, y) coordinates of the button's center if found, else None.
    """
    time.sleep(2)

    # Attempt to locate the image on the screen
    location = pyautogui.locateOnScreen(image_path, confidence=0.8)

    if location is not None:
        logging.info(f"'Other Reports' button found at: {location}")

        # Calculate the center of the located image
        center_x, center_y = pyautogui.center(location)

        # Adjust for Retina screen
        x, y = adjust_coordinates_for_retina(center_x, center_y)

        # Move the mouse to the center of the image and click
        pyautogui.moveTo(x, y, duration=0.5)
        pyautogui.click()
        logging.info(f"Clicked 'Other Reports' button at adjusted coordinates ({x}, {y}).")

        return (x, y)
    else:
        logging.error(f"'Other Reports' button image not found on the screen.")
        return None

def check_checkbox(checkbox_name, checked_image_path, retry=RETRY_LIMIT):
    """
    Verify if a checkbox is checked. If not, click it to check.

    Args:
        checkbox_name (str): The name identifier of the checkbox.
        checked_image_path (str): Path to the 'checked' image of the checkbox.
        retry (int): Number of attempts to verify/check the checkbox.

    Returns:
        bool: True if the checkbox is confirmed to be checked, False otherwise.
    """
    for attempt in range(1, retry + 1):
        try:
            # Use the original color image for template matching
            location = pyautogui.locateOnScreen(checked_image_path, confidence=0.99)
            if location:
                logging.info(f"Checkbox '{checkbox_name}' is already checked.")
                return True
        except Exception as e:
            logging.warning(f"Checkbox '{checkbox_name}' is not checked. Attempt {attempt} to check it.")
            # Option 1: If 'unchecked' image is available, locate and click it
            unchecked_image_path = CHECKBOX_IMAGES[checkbox_name].get('unchecked')
            if unchecked_image_path and os.path.isfile(unchecked_image_path):
                unchecked_location = pyautogui.locateOnScreen(unchecked_image_path, confidence=0.8)
                if unchecked_location:
                    center_x, center_y = pyautogui.center(unchecked_location)
                    x, y = adjust_coordinates_for_retina(center_x, center_y)
                    pyautogui.moveTo(x, y, duration=0.1)
                    pyautogui.click()
                    logging.info(f"Clicked unchecked checkbox '{checkbox_name}' at ({x}, {y}).")
                    time.sleep(SLEEP_TIME_AFTER_CLICK)
                    continue  # Retry verification after clicking
                else:
                    logging.error(f"Unchecked image for '{checkbox_name}' not found on screen.")
            else:
                # Option 2: If 'unchecked' image is not available, use coordinates
                checkbox_coordinates = {
                    'PO_LOG_CHECKBOX': (100, 200),  # Replace with actual coordinates
                    'SHOW_POS_CHECKBOX': (100, 250),
                    'SHOW_ACTUALIZED_ITEMS_CHECKBOX': (100, 300)
                }
                coords = checkbox_coordinates.get(checkbox_name)
                if coords:
                    x, y = adjust_coordinates_for_retina(*coords)
                    pyautogui.moveTo(x, y, duration=0.1)
                    pyautogui.click()
                    logging.info(f"Clicked checkbox '{checkbox_name}' at ({x}, {y}).")
                    time.sleep(SLEEP_TIME_AFTER_CLICK)
                    continue  # Retry verification after clicking
                else:
                    logging.error(f"No coordinates defined for checkbox '{checkbox_name}'.")
            time.sleep(1)
    logging.critical(f"Failed to ensure checkbox '{checkbox_name}' is checked after {retry} attempts.")
    return False

def press_enter():
    """
    Press the Enter key to confirm alert dialogs.
    """
    pyautogui.press('enter')
    logging.info("Pressed Enter key.")

def verify_and_check_all_checkboxes():
    """
    Verify that all required checkboxes are checked. If not, attempt to check them.

    Returns:
        bool: True if all checkboxes are confirmed checked, False otherwise.
    """
    all_checked = True
    for checkbox_name, images in CHECKBOX_IMAGES.items():
        checked_image = images.get('checked')
        if not checked_image or not os.path.isfile(checked_image):
            logging.error(f"Checked image for '{checkbox_name}' is missing.")
            all_checked = False
            continue

        is_checked = check_checkbox(checkbox_name, checked_image)
        if not is_checked:
            logging.error(f"Checkbox '{checkbox_name}' could not be checked.")
            all_checked = False
    return all_checked

def process_budget_file(file_path, project_id):
    """
    Process the budget file by interacting with the Showbiz Budgeting application.

    Args:
        file_path (str): Path to the .mbb budget file.
        project_id (str): The Project ID extracted from the group name.
    """
    logging.info(f"Starting processing for Project ID {project_id} with file {file_path}.")

    # Set the global variables dynamically
    global FILE_PATH, file_name_text
    FILE_PATH = file_path
    file_name_text = project_id  # Assuming the file name text is the Project ID

    running = is_program_running(PROGRAM_NAME)

    if not running:
        logging.info(f"Opening '{PROGRAM_NAME}' with file '{FILE_PATH}'.")
        open_file(FILE_PATH)
        time.sleep(SLEEP_TIME_OPEN)  # Wait for the application to load
    else:
        logging.info(f"'{PROGRAM_NAME}' is already running.")
        if is_file_open_by_window_title(os.path.basename(FILE_PATH)):
            logging.info(f"The file '{FILE_PATH}' is currently open. Bringing '{PROGRAM_NAME}' to front.")
            bring_to_front(PROGRAM_NAME)
            time.sleep(1)  # Allow time for the application to come to front
        else:
            logging.info("File isn't open: opening budget file.")
            open_file(FILE_PATH)
            time.sleep(SLEEP_TIME_AFTER_CLICK)

    logging.info("Sending Command + P keystroke to open Print dialog.")
    send_keystroke('p', 'command')
    time.sleep(SLEEP_TIME_AFTER_KEYSTROKE)

    logging.info("Attempting to find and click on 'Other Reports' button using OpenCV.")
    other_reports_position = click_other_reports_button(OTHER_REPORTS_BUTTON_IMAGE)

    if other_reports_position is None:
        logging.critical("Cannot proceed without locating the 'Other Reports' button.")
        return

    time.sleep(0.1)  # Allow UI to update after clicking "Other Reports"
    # Validate checkboxes before proceeding
    logging.info("Verifying that all required checkboxes are checked.")
    if verify_and_check_all_checkboxes():
        logging.info("All required checkboxes are checked. Proceeding with the script.")
    else:
        logging.critical("Failed to ensure all required checkboxes are checked. Exiting.")
        return

    # Step 1: Click on the Preview button
    if not click_button(BUTTON_IMAGES['PREVIEW']):
        logging.critical("Failed to click on the 'Preview' button.")
        return

    # Step 2: Click on the Raw Text button
    if not click_button(BUTTON_IMAGES['RAW_TEXT']):
        logging.critical("Failed to click on the 'Raw Text' button.")
        return

    # Step 3: Click on the Okay button
    press_enter()

    # Step 4: Click on the Save As Form field and type the Project ID
    type_in_field(file_name_text)

    # Step 5: Click on the Save button
    press_enter()

    # Step 6: Click on the Okay button again
    press_enter()

    # Step 7: Click on the Close button
    if not click_button(BUTTON_IMAGES['CLOSE']):
        logging.critical("Failed to click on the 'Close' button.")
        return

    logging.info("Script finished.")

def list_all_groups(board_id):
    """
    Retrieves all groups in the specified Monday.com board.

    Args:
        board_id (int): The ID of the board to query.

    Returns:
        list: A list of dictionaries containing group 'id' and 'title'.
    """
    query = f'''
    query {{
        boards(ids: {board_id}) {{
            groups {{
                id
                title
            }}
        }}
    }}
    '''
    headers = {
        'Authorization': MONDAY_API_TOKEN,
        'Content-Type': 'application/json',
        'API-Version': '2023-10'
    }
    response = requests.post(MONDAY_API_URL, headers=headers, json={'query': query})
    data = response.json()

    if response.status_code == 200:
        if 'data' in data and 'boards' in data['data']:
            groups = data['data']['boards'][0]['groups']
            logging.info(f"Retrieved {len(groups)} groups from board ID {board_id}.")
            return groups
        elif 'errors' in data:
            logging.error(f"Error fetching groups from Monday.com: {data['errors']}")
            return []
        else:
            logging.error(f"Unexpected response structure: {data}")
            return []
    else:
        logging.error(f"HTTP Error {response.status_code}: {response.text}")
        return []

def get_items_in_group(group_id):
    """
    Retrieves all items within a specified group.

    Args:
        group_id (str): The ID of the group to query.

    Returns:
        list: A list of dictionaries containing item details.
    """
    query = f'''
    query {{
          boards(ids: {PO_BOARD_ID}) {{
            groups(ids: "{group_id}") {{
                  items_page {{
                items {{
                    id
                    name
                    column_values {{
                        id
                        text
                        value
                        }}
                    }}
                }}
            }} 
        }}
    }}
    '''
    headers = {
        'Authorization': MONDAY_API_TOKEN,
        'Content-Type': 'application/json',
        'API-Version': '2023-10'
    }
    response = requests.post(MONDAY_API_URL, headers=headers, json={'query': query})
    data = response.json()

    if response.status_code == 200:
        if 'data' in data and 'boards' in data['data']:
            try:
                items = data['data']['boards'][0]['groups'][0]['items_page']['items']
                logging.info(f"Retrieved {len(items)} items from group ID {group_id}.")
                return items
            except KeyError:
                logging.error(f"Unexpected response structure: {data}")
                return []
        elif 'errors' in data:
            logging.error(f"Error fetching items from Monday.com: {data['errors']}")
            return []
    else:
        logging.error(f"HTTP Error {response.status_code}: {response.text}")
        return []

def get_column_value(item, column_id):
    """
    Retrieves the value of a specified column for an item.

    Args:
        item (dict): The item dictionary from Monday.com.
        column_id (str): The ID of the column to retrieve.

    Returns:
        str: The text value of the column, or None if not found.
    """
    for column in item['column_values']:
        if column['id'] == column_id:
            return column['text']  # or 'value' depending on data
    return None

def extract_project_id_from_group_title(title):
    """
    Extracts the four-digit Project ID from the group title.

    Args:
        title (str): The title of the group.

    Returns:
        str: The extracted Project ID, or None if not found.
    """
    match = re.search(r'\b(\d{4})\b', title)
    if match:
        return match.group(1)
    return None

def extract_project_name_from_group_title(title):
    """
    Extracts the project name from the group title.

    Args:
        title (str): The title of the group.

    Returns:
        str: The extracted project name, or "Unknown Project" if not found.
    """
    match = re.search(r'\d{4}\s*[-_]\s*(.*)', title)
    if match:
        return match.group(1).strip()
    return "Unknown Project"

def construct_dropbox_path(project_id, project_name):
    """
    Constructs the Dropbox path for the working budget file based on Project ID and name.

    Args:
        project_id (str): The Project ID.
        project_name (str): The Project Name.

    Returns:
        str: The constructed Dropbox path.
    """
    # Updated to point to '1.2 Working Budget' folder
    dropbox_path = f"/{project_id} - {project_name}/5. Budget/1.2 Working/"
    return dropbox_path

def find_mbb_files(project_folder_path):
    """
    Locate all .mbb files within the specified project budget folder.

    Args:
        dbx_client (DropboxClientSingleton): The Dropbox client instance.
        project_folder_path (str): Path to the project's budget folder in Dropbox.

    Returns:
        list: List of full paths to the located .mbb files. Empty list if none found.
    """
    try:
        # List all files in the directory
        dbx_client = get_dropbox_client()
        result = dbx_client.dbx.files_list_folder(project_folder_path)
        mbb_files = [entry.path_display for entry in result.entries if isinstance(entry, dropbox.files.FileMetadata) and entry.name.lower().endswith('.mbb')]
        logging.info(f"Found {len(mbb_files)} .mbb file(s) in '{project_folder_path}'.")
        return mbb_files
    except dropbox.exceptions.ApiError as e:
        if isinstance(e.error, dropbox.files.ListFolderError) and e.error.is_path() and e.error.get_path().is_not_found():
            logging.error(f"Directory '{project_folder_path}' not found in Dropbox.")
        else:
            logging.error(f"Error accessing Dropbox folder '{project_folder_path}': {e}")
        return []

def check_monday_and_print():
    """
    Checks Monday.com for groups in the PO Log Board, processes them, and invokes the printing workflow.
    """
    logging.info("Starting Monday.com check.")
    try:
        # Retrieve all groups from the PO Log Board
        groups = list_all_groups(PO_BOARD_ID)

        for group in groups:
            group_id = group['id']
            group_title = group['title']
            logging.info(f"Processing group '{group_title}' with ID {group_id}.")

            # Retrieve all items in the current group
            items = get_items_in_group(group_id)

            if not items:
                logging.info(f"No items found in group '{group_title}'. Skipping.")
                continue

            # Check if all items have status "PAID"
            all_paid = True
            for item in items:
                status = get_column_value(item, 'status__1')  # Replace 'status__1' with actual column ID if different
                if status != "Paid":
                    all_paid = False
                    break

            if all_paid:
                logging.info(f"All items in group '{group_title}' are PAID. Ignoring this group.")
                continue  # Skip processing for this group if all items are PAID

            # Extract Project ID from group title (four-digit number)
            project_id = extract_project_id_from_group_title(group_title)
            if not project_id:
                logging.error(f"Failed to extract Project ID from group title '{group_title}'. Skipping.")
                continue

            # Extract Project Name from group title
            project_name = extract_project_name_from_group_title(group_title)

            # Construct Dropbox path for the working budget file
            dropbox_path = construct_dropbox_path(project_id, project_name)

            # Verify that the .mbb file exists in Dropbox
            mbb_files = find_mbb_files(dropbox_path)
            if not mbb_files:
                logging.error(f".mbb file not found at '{dropbox_path}'. Skipping.")
                continue
            else:
                process_budget_file(mbb_files[0], project_id)

    except Exception as e:
        logging.error(f"An error occurred during Monday.com check: {e}", exc_info=True)

def main_loop():
    """
    Main loop that runs the Monday.com check every 30 minutes.
    """
    while True:
        check_monday_and_print()
        logging.info("Sleeping for 30 minutes.")
        time.sleep(1800)  # Sleep for 30 minutes

if __name__ == "__main__":
    logging.info("Script started. Beginning main loop.")
    main_loop()