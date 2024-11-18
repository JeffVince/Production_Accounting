# log_printer.py

import os
import subprocess
import time
import logging

import psutil
import pyautogui
from AppKit import NSWorkspace

import cv2
import numpy as np

# Configure logging
logging.basicConfig(
    level=logging.INFO,  # Change to DEBUG for more detailed logs
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("../logs/showbiz_budgeting.log"),
        logging.StreamHandler()
    ]
)


# PO LOG SAVE AS TEXT
file_name_text = "REPLACE WITH PROJECT ID"
FILE_PATH = "REPLACE WITH FILE PATH"


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
# Since the checkboxes are 13x13 pixels, we'll set a small buffer
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
    for checkbox_name, image_path in CHECKBOX_IMAGES.items():
        # Load the original image
        image = cv2.imread(image_path)
        if image is None:
            logging.error(f"Failed to load template image from '{image_path}'.")
            continue

        # Convert to grayscale
        gray_image = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)

        # Save the grayscale image, overwriting the original
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
    """
    try:
        subprocess.run(['open', file_path], check=True)
        logging.info(f"Opened file '{file_path}'.")
    except subprocess.CalledProcessError as e:
        logging.error(f"Failed to open file '{file_path}': {e}")


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


def main():
    """
    Main function to manage the Showbiz Budgeting application and perform actions.
    """
    logging.info("Script started.")

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

    # Step 4: Click on the Save As Form field and type "TEST"
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


if __name__ == "__main__":
    main()