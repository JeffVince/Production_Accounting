import os
import subprocess
import time
import logging
from datetime import datetime
from pathlib import Path

import psutil
import pyautogui
from AppKit import NSWorkspace
import cv2


# Example SingletonMeta definition (use your existing one if you have it)
from logger import setup_logging
from singleton import SingletonMeta

RUN_FROM_LOCAL = False


class ShowbizPoLogPrinter(metaclass=SingletonMeta):
    """
    📜 ShowbizPoLogPrinter Singleton Class 📜

    This class encapsulates the logic for interacting with Showbiz Budgeting,
    ensuring phases are set to Actual, correct checkboxes are chosen, and printing PO logs.

    Features:
    - Opens Showbiz Budgeting and brings the desired budget file to the foreground.
    - Sends keystrokes and clicks buttons to print PO logs.
    - Locates UI elements by images (provided they exist in `pics` directory).
    - Has a RUN_FROM_LOCAL mode to test the script with a locally available .mbb file without opening Showbiz.
    """

    # region ⚙️ CONFIGURATION
    # 🌐 RUN_FROM_LOCAL Flag: If True, run in offline mode from a local .mbb file in temp_files.
    LOCAL_BUDGET_FILENAME = "ACTUALS 2416 - Whop Keynote.mbb"  # Change this to your local .mbb file name when RUN_FROM_LOCAL=True.

    # 🍏 Program-specific constants
    PROGRAM_NAME = "Showbiz Budgeting"
    PRINT_WINDOW_TITLE = "Print"  # Title of the Print dialog window

    # 💤 Timing constants
    SLEEP_TIME_OPEN = 5  # Seconds to wait after opening the program
    SLEEP_TIME_AFTER_KEYSTROKE = 1
    SLEEP_TIME_AFTER_CLICK = 2
    RETRY_LIMIT = 3  # Maximum number of retry attempts

    # 🖼️ Template Matching Settings for OpenCV
    MATCHING_METHOD = cv2.TM_CCOEFF_NORMED
    MATCHING_THRESHOLD = 0.7

    # 🚫 Fail-safe for PyAutoGUI
    pyautogui.FAILSAFE = True
    pyautogui.PAUSE = 0.5

    # endregion

    def __init__(self, project_id="REPLACE WITH PROJECT ID", file_path="REPLACE WITH FILE PATH"):
        if not hasattr(self, '_initialized'):
            self.logger = logging.getLogger("app_logger")

            # region 🗂 Init Setup
            # 📃 Project/File details
            self.file_name_text = project_id  # 📝 Project ID or identifier for naming saved printouts

            if RUN_FROM_LOCAL:
                self.FILE_PATH = self.LOCAL_BUDGET_FILENAME
            else:
                self.FILE_PATH = file_path  # 📝 Path to the .mbb file (used if RUN_FROM_LOCAL=False)


            # 📂 Script directory
            self.SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

            # 🖼️ Image file paths for identifying UI elements
            self.DETAIL_REPORTS_BUTTON_IMAGE = os.path.join(self.SCRIPT_DIR, 'pics', 'detail_report_tab.png')


            # ✅ Checkbox images (both checked and unchecked)
            self.CHECKBOX_IMAGES = {
                'PO_LOG_CHECKBOX': {
                    'checked': os.path.join(self.SCRIPT_DIR, 'pics', 'po_log.png'),
                    'unchecked': os.path.join(self.SCRIPT_DIR, 'pics', 'uncheck_po_log.png')
                },
                'SHOW_POS_CHECKBOX': {
                    'checked': os.path.join(self.SCRIPT_DIR, 'pics', 'show_po.png'),
                    'unchecked': os.path.join(self.SCRIPT_DIR, 'pics', 'uncheck_show_po.png')
                },
                'SHOW_ACTUALIZED_ITEMS_CHECKBOX': {
                    'checked': os.path.join(self.SCRIPT_DIR, 'pics', 'show_actuals.png'),
                    'unchecked': os.path.join(self.SCRIPT_DIR, 'pics', 'uncheck_show_actual.png')
                }
            }

            # 🎛️ Button images for UI actions
            self.BUTTON_IMAGES = {
                'PREVIEW': os.path.join(self.SCRIPT_DIR, 'pics', 'preview.png'),
                'RAW_TEXT': os.path.join(self.SCRIPT_DIR, 'pics', 'raw_text.png'),
                'OKAY': os.path.join(self.SCRIPT_DIR, 'pics', 'okay.png'),
                'SAVE_AS_FORM': os.path.join(self.SCRIPT_DIR, 'pics', 'save_as_form.png'),
                'SAVE': os.path.join(self.SCRIPT_DIR, 'pics', 'save.png'),
                'CLOSE': os.path.join(self.SCRIPT_DIR, 'pics', 'close.png')
            }

            # PHASE IMAGES: to detect the current phase state
            self.PHASE_IMAGES = {
                'actual': os.path.join(self.SCRIPT_DIR, 'pics', 'phase_actual.png'),
                'estimated': os.path.join(self.SCRIPT_DIR, 'pics', 'phase_estimated.png'),
                'working': os.path.join(self.SCRIPT_DIR, 'pics', 'phase_working.png')
            }

            self.dont_subtotal_image = os.path.join(self.SCRIPT_DIR, 'pics', 'dont_subtotal.png')
            self.sub_total_title_image = os.path.join(self.SCRIPT_DIR, 'pics', 'sub_total_title.png')
            self.dont_subtotal_dropdown_image = os.path.join(self.SCRIPT_DIR, 'pics', 'dont_subtotal_dropdown.png')

            # Define the dictionary for checkboxes with their paths
            CHECKBOX_IMAGES = {
                'Invoice': {
                    'checked': os.path.join(self.SCRIPT_DIR, 'pics', 'invoice_check.png'),
                    'unchecked': os.path.join(self.SCRIPT_DIR, 'pics', 'invoice_uncheck.png')
                },
                'Time Card': {
                    'checked': os.path.join(self.SCRIPT_DIR, 'pics', 'time_card_check.png'),
                    'unchecked': os.path.join(self.SCRIPT_DIR, 'pics', 'time_card_uncheck.png')
                },
                'Check': {
                    'checked': os.path.join(self.SCRIPT_DIR, 'pics', 'check_check.png'),
                    'unchecked': os.path.join(self.SCRIPT_DIR, 'pics', 'check_uncheck.png')
                },
                'Petty Cash': {
                    'checked': os.path.join(self.SCRIPT_DIR, 'pics', 'petty_cash_check.png'),
                    'unchecked': os.path.join(self.SCRIPT_DIR, 'pics', 'petty_cash_uncheck.png')
                },
                'Credit Card': {
                    'checked': os.path.join(self.SCRIPT_DIR, 'pics', 'credit_card_check.png'),
                    'unchecked': os.path.join(self.SCRIPT_DIR, 'pics', 'credit_card_uncheck.png')
                },
                'Projection': {
                    'checked': os.path.join(self.SCRIPT_DIR, 'pics', 'projection_check.png'),
                    'unchecked': os.path.join(self.SCRIPT_DIR, 'pics', 'projection_uncheck.png')
                },
                'EFT': {
                    'checked': os.path.join(self.SCRIPT_DIR, 'pics', 'eft_check.png'),
                    'unchecked': os.path.join(self.SCRIPT_DIR, 'pics', 'eft_uncheck.png')
                },
                'Non-Detail': {
                    'checked': os.path.join(self.SCRIPT_DIR, 'pics', 'non_detail_check.png'),
                    'unchecked': os.path.join(self.SCRIPT_DIR, 'pics', 'non_detail_uncheck.png')
                },
                'Show Inactive Items': {
                    'checked': os.path.join(self.SCRIPT_DIR, 'pics', 'show_inactive_items_check.png'),
                    'unchecked': os.path.join(self.SCRIPT_DIR, 'pics', 'show_inactive_items_uncheck.png')
                }
            }
            self.logger.info("Inititalizing Showbiz PO Printer")

            self._initialized = True

        # endregion

    # ===========================================================================================
    # region Utility Functions

    def adjust_coordinates_for_retina(self, x, y):
        """
        🖥️ Adjust coordinates for Retina displays.
        If using a Retina screen, the captured screenshot might have double resolution.
        Adjust coordinates by dividing by 2 to account for this.
        """
        adjusted_x = x / 2
        adjusted_y = y / 2
        return adjusted_x, adjusted_y

    def ensure_checkbox_state(self, checkbox_name, checked_image_path, unchecked_image_path, desired_state, retry=3):
        """
        Ensures the given checkbox is in the desired state (True for checked, False for unchecked).

        :param checkbox_name: Name of the checkbox (for logging).
        :param checked_image_path: File path to the 'checked' image of this checkbox.
        :param unchecked_image_path: File path to the 'unchecked' image of this checkbox.
        :param desired_state: Boolean, True if we want it checked, False if we want it unchecked.
        :param retry: How many times to attempt toggling before giving up.
        :return: True if the checkbox is in the desired state after attempts, False otherwise.
        """
        for attempt in range(1, retry + 1):
            try:
                if desired_state:
                    # Want it checked
                    try:
                        location = pyautogui.locateOnScreen(checked_image_path, confidence=0.97)
                    except Exception as e:
                        self.logger.error(f"Error locating checked image for '{checkbox_name}': {e}")
                        location = None

                    if location:
                        self.logger.info(f"Checkbox '{checkbox_name}' is already checked.")
                        return True
                    else:
                        # Not checked, try clicking the unchecked image
                        try:
                            unchecked_location = pyautogui.locateOnScreen(unchecked_image_path, confidence=0.97)
                        except Exception as e:
                            self.logger.error(f"Error locating unchecked image for '{checkbox_name}': {e}")
                            unchecked_location = None

                        if unchecked_location:
                            center_x, center_y = pyautogui.center(unchecked_location)
                            x, y = self.adjust_coordinates_for_retina(center_x, center_y)
                            pyautogui.moveTo(x, y, duration=0.1)
                            pyautogui.click()
                            self.logger.info(f"Toggled '{checkbox_name}' to checked at ({x}, {y}).")
                        else:
                            self.logger.error(f"Unchecked image for '{checkbox_name}' not found on attempt {attempt}.")
                            # Continue to next attempt if any
                else:
                    # Want it unchecked
                    try:
                        location = pyautogui.locateOnScreen(unchecked_image_path, confidence=0.97)
                    except Exception as e:
                        self.logger.error(f"Error locating unchecked image for '{checkbox_name}': {e}")
                        location = None

                    if location:
                        self.logger.info(f"Checkbox '{checkbox_name}' is already unchecked.")
                        return True
                    else:
                        # It might be checked, try clicking the checked image to uncheck
                        try:
                            checked_location = pyautogui.locateOnScreen(checked_image_path, confidence=0.8)
                        except Exception as e:
                            self.logger.error(f"Error locating checked image for '{checkbox_name}': {e}")
                            checked_location = None

                        if checked_location:
                            center_x, center_y = pyautogui.center(checked_location)
                            x, y = self.adjust_coordinates_for_retina(center_x, center_y)
                            pyautogui.moveTo(x, y, duration=0.1)
                            pyautogui.click()
                            self.logger.info(f"Toggled '{checkbox_name}' to unchecked at ({x}, {y}).")
                        else:
                            self.logger.error(f"Checked image for '{checkbox_name}' not found on attempt {attempt}.")
                time.sleep(1)

            except Exception as e:
                self.logger.error(f"Unexpected error ensuring checkbox '{checkbox_name}' state: {e}")

        self.logger.critical(
            f"Failed to set checkbox '{checkbox_name}' to desired state ({desired_state}) after {retry} attempts.")
        return False


    def ensure_all_settings(self):
        """
        Ensures all desired checkboxes are in the correct state:
        - The following should be checked: Invoice, Time Card, Check, Petty Cash, Credit Card, Projection, EFT
        - Non-Detail and Show Inactive Items should be unchecked.

        Assumes images are named in a consistent pattern, e.g.:
        - 'invoice_check.png', 'invoice_uncheck.png'
        - 'time_card_check.png', 'time_card_uncheck.png'
        - ... and so forth.
        """
        # Define your checkboxes and their desired states
        # True = Should be checked, False = Should be unchecked
        desired_checkboxes = {
            'Invoice': True,
            'Time Card': True,
            'Check': True,
            'Petty Cash': True,
            'Credit Card': True,
            'Projection': True,
            'EFT': True,
            'Non-Detail': False,
            'Show Inactive Items': True
        }

        # Build a dictionary of image paths. Adjust filenames or paths as needed.
        base_path = os.path.join(self.SCRIPT_DIR, 'pics')
        check_pattern = "{}_check.png"  # e.g. invoice_check.png
        uncheck_pattern = "{}_uncheck.png"  # e.g. invoice_uncheck.png

        all_good = True
        for checkbox_name, should_be_checked in desired_checkboxes.items():
            checkbox_key = checkbox_name.lower().replace(" ", "_").replace("-", "_")
            # Example: "Time Card" -> "time_card"
            # Ensure the image names match your actual filenames
            checked_image = os.path.join(base_path, check_pattern.format(checkbox_key))
            unchecked_image = os.path.join(base_path, uncheck_pattern.format(checkbox_key))

            self.logger.debug(f"Ensuring '{checkbox_name}' is {'checked' if should_be_checked else 'unchecked'}.")

            # Attempt to ensure the correct state
            if not self.ensure_checkbox_state(checkbox_name, checked_image, unchecked_image, should_be_checked):
                all_good = False

        if all_good:
            self.logger.info("✅ All settings are in the desired state.")
        else:
            self.logger.warning("⚠️ Some settings could not be set correctly.")

    def is_program_running(self, program_name):
        """
        💻 Check if a program is currently running by name.
        Uses psutil to iterate over processes.
        """
        try:
            running = any(proc.info['name'] == program_name for proc in psutil.process_iter(['name']))
            self.logger.debug(f"Program '{program_name}' running: {running}")
            return running
        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess) as e:
            self.logger.error(f"Error checking if program is running: {e}")
            return False

    def open_file(self, file_path):
        """
        📂 Open the specified file using the default application on macOS.
        """
        try:
            subprocess.run(['open', file_path], check=True)
            self.logger.info(f"Opened file '{file_path}'.")
        except subprocess.CalledProcessError as e:
            self.logger.error(f"Failed to open file '{file_path}': {e}")

    def bring_to_front(self, app_name):
        """
        🎛️ Bring the specified application to the foreground using NSWorkspace on macOS.
        """
        workspace = NSWorkspace.sharedWorkspace()
        apps = workspace.runningApplications()
        app = next((app for app in apps if app.localizedName() == app_name), None)

        if app:
            app.activateWithOptions_(1)  # NSApplicationActivateIgnoringOtherApps
            self.logger.info(f"Brought '{app_name}' to the front.")
            time.sleep(1)
        else:
            self.logger.warning(f"Application '{app_name}' not found.")

    def is_file_open_by_window_title(self, file_name, retries=3, delay=1):
        """
        🔎 Check if a window with the specified file name is open in Showbiz Budgeting.
        Uses AppleScript to list window names.
        """
        for attempt in range(retries):
            self.bring_to_front(self.PROGRAM_NAME)
            script = f'''
            tell application "System Events"
                tell process "{self.PROGRAM_NAME}"
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
                    self.logger.info(f"File '{file_name}' detected as open.")
                    return True
                else:
                    self.logger.info(f"Attempt {attempt + 1}/{retries}: File '{file_name}' not detected as open.")
            except subprocess.CalledProcessError as e:
                self.logger.error(f"Failed to check window titles: {e}")
            time.sleep(delay)

        self.logger.info(f"File '{file_name}' not detected as open after {retries} attempts.")
        return False

    def send_keystroke(self, command, modifier):
        """
        ⌨️ Send a keystroke using AppleScript. E.g. Command + P.
        """
        script = f'''
        tell application "System Events"
            keystroke "{command}" using {modifier} down
        end tell
        '''
        try:
            subprocess.run(['osascript', '-e', script], check=True)
            self.logger.info(f"Sent keystroke '{modifier} + {command}'.")
        except subprocess.CalledProcessError as e:
            self.logger.error(f"Failed to send keystroke: {e}")

    def click_button(self, image_path):
        """
        🖱️ Locate and click a button specified by the given image path on screen.
        """
        location = pyautogui.locateOnScreen(image_path, confidence=0.8)
        if location:
            center_x, center_y = pyautogui.center(location)
            x, y = self.adjust_coordinates_for_retina(center_x, center_y)
            pyautogui.moveTo(x, y, duration=0.05)
            pyautogui.click()
            self.logger.info(f"Clicked button at ({x}, {y}) using image: {image_path}.")
            return True
        else:
            self.logger.error(f"Button image '{image_path}' not found.")
            return False

    def type_in_field(self, text):
        """
        ⌨️ Type text into the currently selected field.
        Clears existing text by pressing backspace first.
        """
        try:
            pyautogui.press('backspace')
            time.sleep(0.2)
            pyautogui.typewrite(text, interval=0.1)
            self.logger.info(f"Typed text '{text}' into the field.")
            return True
        except Exception as e:
            self.logger.error(f"Failed to type text '{text}' into the field: {e}")
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
            self.logger.debug(f"Print window '{window_title}' status: {status}")
            return status == "open"
        except subprocess.CalledProcessError as e:
            self.logger.error(f"Failed to check window status: {e}")
            return False

    def click_detail_reports_button(self, image_path):
        """
        🖱️ Locate and click the 'Detail Reports' (or 'Other Reports') button on the screen.
        Returns the coordinates where it clicked, for reference.
        """
        time.sleep(2)
        location = pyautogui.locateOnScreen(image_path, confidence=0.8)
        if location is not None:
            self.logger.info(f"'Other Reports' button found at: {location}")
            center_x, center_y = pyautogui.center(location)
            x, y = self.adjust_coordinates_for_retina(center_x, center_y)
            pyautogui.moveTo(x, y, duration=0.05)
            pyautogui.click()
            self.logger.info(f"Clicked 'Detail Reports' button at adjusted coords ({x}, {y}).")
            return (x, y)
        else:
            self.logger.error("'Detail Reports' button image not found on the screen.")
            return None

    def check_checkbox(self, checkbox_name, checked_image_path, retry=None):
        """
        ☑️ Verify if a checkbox is checked, and if not, attempt to check it.
        Uses provided images or fallback coordinates.
        """
        if retry is None:
            retry = self.RETRY_LIMIT
        for attempt in range(1, retry + 1):
            try:
                location = pyautogui.locateOnScreen(checked_image_path, confidence=0.99)
                if location:
                    self.logger.info(f"Checkbox '{checkbox_name}' is already checked.")
                    return True
            except Exception:
                self.logger.warning(f"Checkbox '{checkbox_name}' not confirmed checked. Attempt {attempt} to check it.")
                unchecked_image_path = self.CHECKBOX_IMAGES[checkbox_name].get('unchecked')
                if unchecked_image_path and os.path.isfile(unchecked_image_path):
                    unchecked_location = pyautogui.locateOnScreen(unchecked_image_path, confidence=0.8)
                    if unchecked_location:
                        center_x, center_y = pyautogui.center(unchecked_location)
                        x, y = self.adjust_coordinates_for_retina(center_x, center_y)
                        pyautogui.moveTo(x, y, duration=0.05)
                        pyautogui.click()
                        self.logger.info(f"Clicked unchecked checkbox '{checkbox_name}' at ({x}, {y}).")
                        time.sleep(self.SLEEP_TIME_AFTER_CLICK)
                        continue
                    else:
                        self.logger.error(f"Unchecked image for '{checkbox_name}' not found on screen.")
                else:
                    # If no 'unchecked' image, fallback to predefined coordinates (Replace with real coords)
                    checkbox_coordinates = {
                        'PO_LOG_CHECKBOX': (100, 200),
                        'SHOW_POS_CHECKBOX': (100, 250),
                        'SHOW_ACTUALIZED_ITEMS_CHECKBOX': (100, 300)
                    }
                    coords = checkbox_coordinates.get(checkbox_name)
                    if coords:
                        x, y = self.adjust_coordinates_for_retina(*coords)
                        pyautogui.moveTo(x, y, duration=0.01)
                        pyautogui.click()
                        self.logger.info(f"Clicked checkbox '{checkbox_name}' at ({x}, {y}).")
                        time.sleep(self.SLEEP_TIME_AFTER_CLICK)
                        continue
                    else:
                        self.logger.error(f"No coordinates defined for checkbox '{checkbox_name}'.")
                time.sleep(1)

        self.logger.critical(f"Failed to ensure checkbox '{checkbox_name}' is checked after {retry} attempts.")
        return False

    def get_po_log_file_link(self, filename):
        """
        Given a filename for the PO log located in Desktop/PO_LOGS,
        return a 'file://' link that can be used to directly access the file.
        """
        # Construct the full path to the file in the Desktop/PO_LOGS directory.
        # Using Pathlib and expanduser to handle the '~' user directory shortcut.
        base_dir = Path("~/Desktop/PO_LOGS").expanduser()
        file_path = base_dir / filename

        if not file_path.exists():
            raise FileNotFoundError(f"File '{file_path}' does not exist.")

        # Convert the file path into a file URI
        file_uri = file_path.resolve().as_uri()  # This results in something like file:///Users/<you>/Desktop/PO_LOGS/<filename>

        return file_uri

    def press_enter(self):
        """
        ↵ Press the Enter key to confirm dialogs.
        """
        pyautogui.press('enter')
        self.logger.info("Pressed Enter key.")

    def verify_and_check_all_checkboxes(self):
        """
        ☑️ Verify that all required checkboxes (PO Log, Show POs, Show Actualized Items) are checked.
        """
        all_checked = True
        for checkbox_name, images in self.CHECKBOX_IMAGES.items():
            checked_image = images.get('checked')
            if not checked_image or not os.path.isfile(checked_image):
                self.logger.error(f"Checked image for '{checkbox_name}' is missing.")
                all_checked = False
                continue

            is_checked = self.check_checkbox(checkbox_name, checked_image)
            if not is_checked:
                self.logger.error(f"Checkbox '{checkbox_name}' could not be checked.")
                all_checked = False
        return all_checked

    def ensure_no_subtotal(self):
        """
        Ensures that "Don't Subtotal" is selected. If not found:
        - Locate the 'sub_total_title' image to find where the dropdown is positioned.
        - Click 150px to the right of that title to open the dropdown.
        - Then find and click the 'dont_subtotal_dropdown' option.
        :param dont_subtotal_image: Path to the image for the already selected "Don't Subtotal" option.
        :param sub_total_title_image: Path to the image for the "Sub Total" title label.
        :param dont_subtotal_dropdown_image: Path to the image for the "Don't Subtotal" option within the dropdown.
        :return: True if "Don't Subtotal" is selected or set successfully, False otherwise.
        """
        try:
            # First, check if "Don't Subtotal" is already selected
            try:
                dont_subtotal_location = pyautogui.locateOnScreen(self.dont_subtotal_image, confidence=0.9)
            except Exception as e:
                self.logger.info(f"❌  State is not set to Don't Subtotal")
                dont_subtotal_location = None

            if dont_subtotal_location:
                self.logger.info("✅ 'Don't Subtotal' is already selected.")
                return True

            # If not found, find the 'sub_total_title' reference point
            try:
                sub_total_title_location = pyautogui.locateOnScreen(self.sub_total_title_image, confidence=0.97)
                self.logger.info("✅ 'Dropdown Menu Found")

            except Exception as e:
                self.logger.error(f"Error locating 'sub_total_title_image': {e}")
                sub_total_title_location = None

            if not sub_total_title_location:
                self.logger.error("❌ Could not find 'Sub Total' title on screen. Cannot change subtotal setting.")
                return False

            # Calculate the position of the dropdown (150px to the right of the Sub Total title)
            center_x, center_y = pyautogui.center(sub_total_title_location)
            x, y = self.adjust_coordinates_for_retina(center_x, center_y)

            dropdown_x = x + 150
            dropdown_y = y

            # Open the dropdown
            pyautogui.moveTo(dropdown_x, dropdown_y, duration=.1)
            pyautogui.click()
            self.logger.info(f"Opened the Subtotal dropdown menu at ({dropdown_x}, {dropdown_y}).")

            # Press D for Don't subtotal
            pyautogui.press('d')

            # ↵ Press Enter (instead of  button)
            self.press_enter()

            self.logger.info("✅ Selected 'Don't Subtotal' from the dropdown.")
            return True

        except Exception as e:
            self.logger.error(f"Unexpected error ensuring 'Don't Subtotal' setting: {e}")
            return False

    # endregion
    # ===========================================================================================

    # ===========================================================================================
    # region Phase Handling

    def set_phase_to_actual(self, other_reports_position):
        """
        🎯 Ensure the 'Phase' is set to 'Actual'.

        other_reports_position is expected to be a tuple (x, y) indicating the coordinates of the 'Detail Reports' button.
        Steps:
        1. Validate other_reports_position is not None.
        2. Calculate integer coords for phase dropdown.
        3. Locate current phase via images in the given region.
        4. If not 'Actual', open dropdown and select 'Actual'.

        If current_phase can't be determined, a warning is logged but it won't raise an exception.
        """

        # region 🛠 Initial Checks
        if other_reports_position is None:
            self.logger.error("❌ Cannot set phase to Actual because other_reports_position is None.")
            return
        # endregion

        # region 🗺 Extract Coordinates
        or_x, or_y = other_reports_position

        # phase_x and phase_y should be integers
        phase_x = int(or_x + 30)
        phase_y = int(or_y + 80)

        self.logger.info(f"📍 Phase dropdown baseline coordinates: ({phase_x}, {phase_y})")
        # endregion

        # region 🔎 Identify Current Phase
        current_phase = None
        for phase_name, image_path in self.PHASE_IMAGES.items():
            # Attempt to locate phase image on screen within specified region
            try:
                if pyautogui.locateOnScreen(image_path, confidence=0.95):
                    current_phase = phase_name
                    self.logger.info(f"📒 Current phase detected as: {phase_name.capitalize()}")
                    break
            except Exception as e:
                self.logger.info(f"Phase is not {phase_name.capitalize()}")
        # endregion

        # region ⚠️ Handle Unknown Phase
        if not current_phase:
            self.logger.warning("⚠️ Could not determine current phase. Proceeding with caution.")
            return
        # endregion

        # region ✅ Set Phase to Actual If Needed
        if current_phase == 'actual':
            self.logger.info("✅ Phase already set to 'Actual'. No action needed.")
            return

        # Click dropdown
        pyautogui.moveTo(phase_x, phase_y, duration=0.05)
        pyautogui.click()
        time.sleep(0.5)  # Wait for the dropdown to open

        # Determine how far to move down to select Actual
        if current_phase == 'estimated':
            target_y = phase_y + 50  # Estimated -> Actual: 50px down
        elif current_phase == 'working':
            target_y = phase_y + 25  # Working -> Actual: 25px down
        else:
            self.logger.error("💥 Unhandled phase state. No action taken.")
            return

        pyautogui.moveTo(phase_x, target_y, duration=0.05)
        pyautogui.click()
        self.logger.info("🎉 Selected 'Actual' phase.")
        # endregion

    # endregion
    # ===========================================================================================

    # ===========================================================================================
    # region Main Run Function

    def run(self):
        """
        🏁 Main function to manage the Showbiz Budgeting application and perform actions.

        Steps:
        1. If RUN_FROM_LOCAL is True:
           - Skip opening Showbiz or file from Dropbox.
           - Use the local .mbb file in ./temp_files.
           Assume the Showbiz window is already open and focused.

        2. If RUN_FROM_LOCAL is False:
           - Check if Showbiz Budgeting is running. If not, open the .mbb file.
           - If running, bring it to front, ensure file is open.
           - Send Command+P to open Print dialog.

        3. Find and click 'Detail Reports' button.
        4. Set phase to Actual.
        5. (Optional) Verify checkboxes if needed.
        6. Click 'Preview', then 'Raw Text', hit Enter.
        7. Type file_name_text, press Enter to save, Enter to confirm.
        8. Click Close button.
        9. Done!
        """
        self.logger.info("Script started. 🎬")

        # region 🌐 RUN_FROM_LOCAL CHECK
        if RUN_FROM_LOCAL:
            self.logger.info("🌐 RUN_FROM_LOCAL is True. Running in offline mode from local temp_files folder.")
            # Use local .mbb file directly
            local_file_path = os.path.join("../temp_files", self.LOCAL_BUDGET_FILENAME)
            if not os.path.exists(local_file_path):
                self.logger.error(f"❌ Local budget file not found: {local_file_path}. Cannot proceed.")
                return
            self.logger.info(f"🗂 Using local file: {local_file_path}")
        # endregion

        running = self.is_program_running(self.PROGRAM_NAME)
        if not running:
            self.logger.info(f"Opening '{self.PROGRAM_NAME}' with file '{self.FILE_PATH}'.")
            self.open_file(self.FILE_PATH)
            time.sleep(self.SLEEP_TIME_OPEN)
        else:
            self.logger.info(f"'{self.PROGRAM_NAME}' is already running.")
            if self.is_file_open_by_window_title(os.path.basename(self.FILE_PATH)):
                self.logger.info(
                    f"The file '{self.FILE_PATH}' is currently open. Bringing '{self.PROGRAM_NAME}' to front.")
                self.bring_to_front(self.PROGRAM_NAME)
                time.sleep(1)
            else:
                self.logger.info("File isn't open: opening budget file.")
                self.open_file(self.FILE_PATH)
                time.sleep(self.SLEEP_TIME_AFTER_CLICK)

        # 🖨️ Open Print dialog
        self.logger.info("Sending Command + P keystroke to open Print dialog.")
        self.send_keystroke('p', 'command')
        time.sleep(self.SLEEP_TIME_AFTER_KEYSTROKE)

        # 🖱️ Click on 'Detail Reports'
        self.logger.info("Attempting to find and click on 'Detail Reports' button.")
        other_reports_position = self.click_detail_reports_button(self.DETAIL_REPORTS_BUTTON_IMAGE)
        if other_reports_position is None:
            self.logger.critical("Cannot proceed without locating the 'Detail Reports' button.")
            return

        time.sleep(0.1)

        # Ensure phase is set to Actual
        self.set_phase_to_actual(other_reports_position)

        # Ensure settings are set properly
        self.ensure_all_settings()

        # ensure we don't subtotal
        self.ensure_no_subtotal()

        # 🖱️ Click on the Preview button
        if not self.click_button(self.BUTTON_IMAGES['PREVIEW']):
            self.logger.critical("Failed to click on the 'Preview' button.")
            return

        # 🖱️ Click on the Raw Text button
        if not self.click_button(self.BUTTON_IMAGES['RAW_TEXT']):
            self.logger.critical("Failed to click on the 'Raw Text' button.")
            return

        # ↵ Press Enter (instead of OK button)
        self.press_enter()

        # ⌨️ Type the file name text
        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        filename = "PO_LOG_" + self.file_name_text + "-" + timestamp
        self.type_in_field(filename)

        # ↵ Press Enter (to save)
        self.press_enter()

        # ↵ Press Enter again (to confirm)
        self.press_enter()

        # 🖱️ Click on the Close button
        if not self.click_button(self.BUTTON_IMAGES['CLOSE']):
            self.logger.critical("Failed to click on the 'Close' button.")
            return
        self.logger.info("Script finished. 🎉")

        return self.get_po_log_file_link(filename+".txt")

    # endregion
    # ===========================================================================================


#if RUN_FROM_LOCAL:
  #  setup_logging()
    #showbiz_logger= ShowbizPoLogPrinter("2416")
    #showbiz_logger.run()