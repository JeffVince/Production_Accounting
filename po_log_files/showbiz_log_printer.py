import os
import subprocess
import time
import logging
import psutil
import pyautogui
from AppKit import NSWorkspace
import cv2


# Example SingletonMeta definition (use your existing one if you have it)
class SingletonMeta(type):
    """
    🏗️ Singleton Meta Class 🏗️

    Ensures only one instance of the class is created.
    """
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super().__call__(*args, **kwargs)
        return cls._instances[cls]


class ShowbizPoLogPrinter(metaclass=SingletonMeta):
    """
    📜 ShowbizPoLogPrinter Singleton Class 📜

    This class encapsulates the logic for interacting with Showbiz Budgeting,
    checking phases, ensuring checkboxes are set, and printing PO logs.
    """

    # Logging configuration


    #region CONFIG VARIABLES 📝
    # 🍏 Program-specific constants
    PROGRAM_NAME = "Showbiz Budgeting"
    PRINT_WINDOW_TITLE = "Print"  # Title of the Print dialog window

    # 💤 Timing constants
    SLEEP_TIME_OPEN = 5  # Seconds to wait after opening the program
    SLEEP_TIME_AFTER_KEYSTROKE = 1  # Seconds to wait after sending keystroke
    SLEEP_TIME_AFTER_CLICK = 2  # Seconds to wait after clicking the button
    RETRY_LIMIT = 3  # Maximum number of retry attempts

    # 🖼️ Template Matching Settings for OpenCV
    MATCHING_METHOD = cv2.TM_CCOEFF_NORMED
    MATCHING_THRESHOLD = 0.7

    # 🚫 Fail-safe for PyAutoGUI (moving mouse to a corner will abort)
    pyautogui.FAILSAFE = True
    pyautogui.PAUSE = 0.5
    #endregion

    def __init__(self, project_id="REPLACE WITH PROJECT ID", file_path="REPLACE WITH FILE PATH"):
        # 📃 Project/File details
        self.file_name_text = project_id  # 📝 Replace this with the actual project ID
        self.FILE_PATH = file_path  # 📝 Replace this with the actual file path
        self.logger = logging.getLogger("app_logger")

        # 📂 Paths and directories
        self.SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

        # 🖼️ Image file paths for button detection
        self.OTHER_REPORTS_BUTTON_IMAGE = os.path.join(self.SCRIPT_DIR, 'pics', 'other_reports.png')
        self.DETAIL_REPORTS_BUTTON_IMAGE = os.path.join(self.SCRIPT_DIR, 'pics', 'detail_report_tab.png')

        # ✅ Checkbox images (both checked and unchecked versions)
        self.CHECKBOX_IMAGES = {
            'PO_LOG_CHECKBOX': {
                'checked': os.path.join(self.SCRIPT_DIR, 'pics', 'po_log.png'),
                'unchecked': os.path.join(self.SCRIPT_DIR, 'pics', 'uncheck_po_log.png')  # Optional
            },
            'SHOW_POS_CHECKBOX': {
                'checked': os.path.join(self.SCRIPT_DIR, 'pics', 'show_po.png'),
                'unchecked': os.path.join(self.SCRIPT_DIR, 'pics', 'uncheck_show_po.png')  # Optional
            },
            'SHOW_ACTUALIZED_ITEMS_CHECKBOX': {
                'checked': os.path.join(self.SCRIPT_DIR, 'pics', 'show_actuals.png'),
                'unchecked': os.path.join(self.SCRIPT_DIR, 'pics', 'uncheck_show_actual.png')  # Optional
            }
        }

        # 🎛️ Button images for various UI actions
        self.BUTTON_IMAGES = {
            'PREVIEW': os.path.join(self.SCRIPT_DIR, 'pics', 'preview.png'),
            'RAW_TEXT': os.path.join(self.SCRIPT_DIR, 'pics', 'raw_text.png'),
            'OKAY': os.path.join(self.SCRIPT_DIR, 'pics', 'okay.png'),
            'SAVE_AS_FORM': os.path.join(self.SCRIPT_DIR, 'pics', 'save_as_form.png'),
            'SAVE': os.path.join(self.SCRIPT_DIR, 'pics', 'save.png'),
            'CLOSE': os.path.join(self.SCRIPT_DIR, 'pics', 'close.png')
        }

        # PHASE IMAGES
        # Paths to images for each phase state (make sure these images exist)
        self.PHASE_IMAGES = {
            'estimated': os.path.join(self.SCRIPT_DIR, 'pics', 'phase_estimated.png'),
            'working': os.path.join(self.SCRIPT_DIR, 'pics', 'phase_working.png'),
            'actual': os.path.join(self.SCRIPT_DIR, 'pics', 'phase_actual.png')
        }

    # ===========================================================================================
    # region Utility Functions

    def adjust_coordinates_for_retina(self, x, y):
        """
        🖥️ Adjust coordinates for Retina displays.
        """
        adjusted_x = x / 2
        adjusted_y = y / 2
        return adjusted_x, adjusted_y

    def is_program_running(self, program_name):
        """
        💻 Check if a program is currently running.
        """
        try:
            running = any(proc.info['name'] == program_name for proc in psutil.process_iter(['name']))
            logging.debug(f"Program '{program_name}' running: {running}")
            return running
        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess) as e:
            logging.error(f"Error checking if program is running: {e}")
            return False

    def open_file(self, file_path):
        """
        📂 Open the specified file using the default application.
        """
        try:
            subprocess.run(['open', file_path], check=True)
            logging.info(f"Opened file '{file_path}'.")
        except subprocess.CalledProcessError as e:
            logging.error(f"Failed to open file '{file_path}': {e}")

    def bring_to_front(self, app_name):
        """
        🎛️ Bring the specified application to the foreground.
        """
        workspace = NSWorkspace.sharedWorkspace()
        apps = workspace.runningApplications()
        app = next((app for app in apps if app.localizedName() == app_name), None)

        if app:
            app.activateWithOptions_(1)  # NSApplicationActivateIgnoringOtherApps
            logging.info(f"Brought '{app_name}' to the front.")
            time.sleep(1)
        else:
            logging.warning(f"Application '{app_name}' not found.")

    def is_file_open_by_window_title(self, file_name, retries=3, delay=1):
        """
        🔎 Check if a window with the specified file name is open in Showbiz Budgeting.
        """
        for attempt in range(retries):
            self.bring_to_front(self.PROGRAM_NAME)
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
                logging.error(f"Failed to check window titles: {e}")
            time.sleep(delay)

        logging.info(f"File '{file_name}' not detected as open after {retries} attempts.")
        return False

    def send_keystroke(self, command, modifier):
        """
        ⌨️ Send a keystroke using AppleScript.
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

    def click_button(self, image_path):
        """
        🖱️ Locate and click a button specified by the given image path.
        """
        location = pyautogui.locateOnScreen(image_path, confidence=0.8)
        if location:
            center_x, center_y = pyautogui.center(location)
            x, y = self.adjust_coordinates_for_retina(center_x, center_y)
            pyautogui.moveTo(x, y, duration=0.5)
            pyautogui.click()
            logging.info(f"Clicked button at ({x}, {y}) using image: {image_path}.")
            return True
        else:
            logging.error(f"Button image '{image_path}' not found.")
            return False

    def type_in_field(self, text):
        """
        ⌨️ Type text into the currently selected field, clearing any existing text first.
        """
        try:
            pyautogui.press('backspace')
            time.sleep(0.2)
            pyautogui.typewrite(text, interval=0.1)
            logging.info(f"Typed text '{text}' into the field.")
            return True
        except Exception as e:
            logging.error(f"Failed to type text '{text}' into the field: {e}")
            return False

    def is_print_window_open(self, window_title):
        """
        🖨️ Check if the Print dialog window is currently open.
        """
        script = f'''
        tell application "System Events"
            tell process "{self.PROGRAM_NAME}"
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

    def click_other_reports_button(self, image_path):
        """
        🖱️ Locate and click the "Other Reports" button on the screen.
        """
        time.sleep(2)
        location = pyautogui.locateOnScreen(image_path, confidence=0.8)
        if location is not None:
            logging.info(f"'Other Reports' button found at: {location}")
            center_x, center_y = pyautogui.center(location)
            x, y = self.adjust_coordinates_for_retina(center_x, center_y)
            pyautogui.moveTo(x, y, duration=0.5)
            pyautogui.click()
            logging.info(f"Clicked 'Other Reports' button at adjusted coords ({x}, {y}).")
            return (x, y)
        else:
            logging.error("'Other Reports' button image not found on the screen.")
            return None

    def check_checkbox(self, checkbox_name, checked_image_path, retry=None):
        """
        ☑️ Verify if a checkbox is checked, and if not, attempt to check it.
        """
        if retry is None:
            retry = self.RETRY_LIMIT
        for attempt in range(1, retry + 1):
            try:
                location = pyautogui.locateOnScreen(checked_image_path, confidence=0.99)
                if location:
                    logging.info(f"Checkbox '{checkbox_name}' is already checked.")
                    return True
            except Exception:
                logging.warning(f"Checkbox '{checkbox_name}' not confirmed checked. Attempt {attempt} to check it.")
                unchecked_image_path = self.CHECKBOX_IMAGES[checkbox_name].get('unchecked')
                if unchecked_image_path and os.path.isfile(unchecked_image_path):
                    unchecked_location = pyautogui.locateOnScreen(unchecked_image_path, confidence=0.8)
                    if unchecked_location:
                        center_x, center_y = pyautogui.center(unchecked_location)
                        x, y = self.adjust_coordinates_for_retina(center_x, center_y)
                        pyautogui.moveTo(x, y, duration=0.1)
                        pyautogui.click()
                        logging.info(f"Clicked unchecked checkbox '{checkbox_name}' at ({x}, {y}).")
                        time.sleep(self.SLEEP_TIME_AFTER_CLICK)
                        continue
                    else:
                        logging.error(f"Unchecked image for '{checkbox_name}' not found on screen.")
                else:
                    # If no 'unchecked' image, fallback to coordinates
                    checkbox_coordinates = {
                        'PO_LOG_CHECKBOX': (100, 200),  # 🧩 TODO: Replace with real coords
                        'SHOW_POS_CHECKBOX': (100, 250),
                        'SHOW_ACTUALIZED_ITEMS_CHECKBOX': (100, 300)
                    }
                    coords = checkbox_coordinates.get(checkbox_name)
                    if coords:
                        x, y = self.adjust_coordinates_for_retina(*coords)
                        pyautogui.moveTo(x, y, duration=0.1)
                        pyautogui.click()
                        logging.info(f"Clicked checkbox '{checkbox_name}' at ({x}, {y}).")
                        time.sleep(self.SLEEP_TIME_AFTER_CLICK)
                        continue
                    else:
                        logging.error(f"No coordinates defined for checkbox '{checkbox_name}'.")
                time.sleep(1)

        logging.critical(f"Failed to ensure checkbox '{checkbox_name}' is checked after {retry} attempts.")
        return False

    def press_enter(self):
        """
        ↵ Press the Enter key to confirm dialogs.
        """
        pyautogui.press('enter')
        logging.info("Pressed Enter key.")

    def verify_and_check_all_checkboxes(self):
        """
        ☑️ Verify that all required checkboxes are checked.
        """
        all_checked = True
        for checkbox_name, images in self.CHECKBOX_IMAGES.items():
            checked_image = images.get('checked')
            if not checked_image or not os.path.isfile(checked_image):
                logging.error(f"Checked image for '{checkbox_name}' is missing.")
                all_checked = False
                continue

            is_checked = self.check_checkbox(checkbox_name, checked_image)
            if not is_checked:
                logging.error(f"Checkbox '{checkbox_name}' could not be checked.")
                all_checked = False
        return all_checked

    def set_phase_to_actual(self, other_reports_position):
        """
        Ensure the 'Phase' is set to 'Actual'.
        We know:
        - The dropdown area is 30px to the right and 80px below the detail report button we just clicked.
        - The menu options are stacked vertically: Estimated (top), Working (25px below Estimated), Actual (25px below Working).
          So from top (Estimated) to Actual is 50px down.

        Steps:
        1. Identify which phase is currently selected.
        2. If it's Actual, do nothing.
        3. If it's Working, when we open the dropdown, we move down by 25px to click Actual.
        4. If it's Estimated, we move down by 50px.
        """
        or_x, or_y = other_reports_position
        phase_x = or_x + 30
        phase_y = or_y + 80

        logging.info(f"Phase dropdown baseline coordinates: ({phase_x}, {phase_y})")

        current_phase = None
        # Identify the currently selected phase by checking images
        for phase_name, image_path in self.PHASE_IMAGES.items():
            if pyautogui.locateOnScreen(image_path, confidence=0.8, region=(phase_x - 50, phase_y - 20, 200, 60)):
                current_phase = phase_name
                logging.info(f"Current phase detected as: {phase_name.capitalize()}")
                break

        if not current_phase:
            logging.warning("Could not determine current phase. Proceeding with caution.")
            return

        if current_phase == 'actual':
            logging.info("Phase already set to 'Actual'. No action needed.")
            return
        else:
            # Click to open the dropdown
            pyautogui.moveTo(phase_x, phase_y, duration=0.5)
            pyautogui.click()
            time.sleep(0.5)  # Wait for the dropdown to open

            # The dropdown is now open with "Estimated" at the top position of phase_x, phase_y
            if current_phase == 'estimated':
                # Move down by 50px to reach Actual
                target_y = phase_y + 50
            elif current_phase == 'working':
                # Move down by 25px to reach Actual
                target_y = phase_y + 25
            else:
                # Just a safeguard
                logging.error("Unhandled phase state. No action taken.")
                return

            # Move to the Actual option and click
            pyautogui.moveTo(phase_x, target_y, duration=0.5)
            pyautogui.click()
            logging.info("Selected 'Actual' phase.")

    # endregion
    # ===========================================================================================

    # ===========================================================================================
    # region Main Functionality

    def run(self):
        """
        🏁 Main function to manage the Showbiz Budgeting application and perform actions.

        Steps:
        1. Check if the program is running. If not, open the file.
        2. Bring application to front and check if the file is open.
        3. Send Command+P to open Print dialog.
        4. Locate and click 'Detail Reports' button.
        5. Verify all required checkboxes, set phase to Actual, etc.
        6. Click various buttons (Preview, Raw Text), press Enter, etc.
        7. Type filename and Save.
        8. Close dialog and finish.
        """
        logging.info("Script started. 🎬")

        running = self.is_program_running(self.PROGRAM_NAME)

        if not running:
            logging.info(f"Opening '{self.PROGRAM_NAME}' with file '{self.FILE_PATH}'.")
            self.open_file(self.FILE_PATH)
            time.sleep(self.SLEEP_TIME_OPEN)
        else:
            logging.info(f"'{self.PROGRAM_NAME}' is already running.")
            if self.is_file_open_by_window_title(os.path.basename(self.FILE_PATH)):
                logging.info(f"The file '{self.FILE_PATH}' is currently open. Bringing '{self.PROGRAM_NAME}' to front.")
                self.bring_to_front(self.PROGRAM_NAME)
                time.sleep(1)
            else:
                logging.info("File isn't open: opening budget file.")
                self.open_file(self.FILE_PATH)
                time.sleep(self.SLEEP_TIME_AFTER_CLICK)

        # 🖨️ Open Print dialog
        logging.info("Sending Command + P keystroke to open Print dialog.")
        self.send_keystroke('p', 'command')
        time.sleep(self.SLEEP_TIME_AFTER_KEYSTROKE)

        # 🖱️ Click on 'Detail Reports'
        logging.info("Attempting to find and click on 'Detail Reports' button.")
        other_reports_position = self.click_other_reports_button(self.DETAIL_REPORTS_BUTTON_IMAGE)
        if other_reports_position is None:
            logging.critical("Cannot proceed without locating the 'Detail Reports' button.")
            return

        time.sleep(0.1)

        # Ensure phase is set to Actual
        self.set_phase_to_actual(other_reports_position)

        # If you want to ensure checkboxes are checked, uncomment:
        # if not self.verify_and_check_all_checkboxes():
        #     logging.critical("Failed to ensure all required checkboxes are checked. Exiting.")
        #     return

        # 🖱️ Click on the Preview button
        if not self.click_button(self.BUTTON_IMAGES['PREVIEW']):
            logging.critical("Failed to click on the 'Preview' button.")
            return

        # 🖱️ Click on the Raw Text button
        if not self.click_button(self.BUTTON_IMAGES['RAW_TEXT']):
            logging.critical("Failed to click on the 'Raw Text' button.")
            return

        # ↵ Press Enter (instead of OK button)
        self.press_enter()

        # ⌨️ Type the file name text
        self.type_in_field(self.file_name_text)

        # ↵ Press Enter (to save)
        self.press_enter()

        # ↵ Press Enter again (to confirm)
        self.press_enter()

        # 🖱️ Click on the Close button
        if not self.click_button(self.BUTTON_IMAGES['CLOSE']):
            logging.critical("Failed to click on the 'Close' button.")
            return

        logging.info("Script finished. 🎉")

    # endregion
    # ===========================================================================================

