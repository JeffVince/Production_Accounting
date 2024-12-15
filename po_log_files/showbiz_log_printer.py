import os
import subprocess
import time
import logging
import threading
import queue
from datetime import datetime
from pathlib import Path
from urllib.parse import urlparse

import psutil
import pyautogui
import cv2
from AppKit import NSWorkspace
from flask import Flask, request, jsonify

from logger import setup_logging
from singleton import SingletonMeta
from dropbox_files.dropbox_api import dropbox_api  # Adjust as needed

setup_logging()
logger = logging.getLogger("server_logger")

RUN_FROM_LOCAL = False
UPLOAD_ONLY = False
job_queue = queue.Queue()
job_statuses = {}
# job_statuses[job_id] = {"status":"queued"/"processing"/"done"/"error", "result":None, "error":None,"progress":None}

app = Flask(__name__)

class ShowbizPoLogPrinter(metaclass=SingletonMeta):
    LOCAL_BUDGET_FILENAME = "ACTUALS 2416 - Whop Keynote.mbb"
    PROGRAM_NAME = "Showbiz Budgeting"
    PRINT_WINDOW_TITLE = "Print"
    SLEEP_TIME_OPEN = 5
    SLEEP_TIME_AFTER_KEYSTROKE = 1
    SLEEP_TIME_AFTER_CLICK = 2
    RETRY_LIMIT = 3
    pyautogui.FAILSAFE = True
    pyautogui.PAUSE = 0.5
    MATCHING_METHOD = cv2.TM_CCOEFF_NORMED
    MATCHING_THRESHOLD = 0.7

    def __init__(self, project_id="REPLACE WITH PROJECT ID", file_path="REPLACE WITH FILE PATH", progress_callback=None, upload_only=False):
        if not hasattr(self, '_initialized'):
            self.logger = logging.getLogger("app_logger")
            self.progress_callback = progress_callback
            self.file_name_text = project_id
            self.FILE_PATH = file_path
            self.upload_only = upload_only
            self.SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

            # Only extract project_folder_name if not in upload_only mode
            self.project_folder_name = None
            if not self.upload_only and self.FILE_PATH.startswith("/"):
                segments = self.FILE_PATH.strip("/").split("/")
                if len(segments) >= 1:
                    self.project_folder_name = segments[0]  # e.g. "2416 - Whop Keynote"
                else:
                    self.logger.error("❌ Cannot determine project_folder_name from file_path.")

            # Images (only needed if not upload_only)
            if not self.upload_only:
                self.DETAIL_REPORTS_BUTTON_IMAGE = os.path.join(self.SCRIPT_DIR, 'pics', 'detail_report_tab.png')
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
                self.BUTTON_IMAGES = {
                    'PREVIEW': os.path.join(self.SCRIPT_DIR, 'pics', 'preview.png'),
                    'RAW_TEXT': os.path.join(self.SCRIPT_DIR, 'pics', 'raw_text.png'),
                    'OKAY': os.path.join(self.SCRIPT_DIR, 'pics', 'okay.png'),
                    'SAVE_AS_FORM': os.path.join(self.SCRIPT_DIR, 'pics', 'save_as_form.png'),
                    'SAVE': os.path.join(self.SCRIPT_DIR, 'pics', 'save.png'),
                    'CLOSE': os.path.join(self.SCRIPT_DIR, 'pics', 'close.png')
                }
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
        else:
            if progress_callback:
                self.progress_callback = progress_callback

    def report_progress(self, message):
        self.logger.info(f"Progress: {message}")
        if self.progress_callback is not None:
            self.progress_callback(message)

    def adjust_coordinates_for_retina(self, x, y):
        return x/2, y/2

    def is_program_running(self, program_name):
        try:
            running = any(proc.info['name'] == program_name for proc in psutil.process_iter(['name']))
            return running
        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess) as e:
            self.logger.error(f"Error checking if program is running: {e}")
            return False

    def open_file(self, file_path):
        try:
            subprocess.run(['open', file_path], check=True)
            self.logger.info(f"Opened file '{file_path}'.")
        except subprocess.CalledProcessError as e:
            self.logger.error(f"Failed to open file '{file_path}': {e}")

    def bring_to_front(self, app_name):
        workspace = NSWorkspace.sharedWorkspace()
        apps = workspace.runningApplications()
        app = next((app for app in apps if app.localizedName() == app_name), None)

        if app:
            app.activateWithOptions_(1)
            self.logger.info(f"Brought '{app_name}' to the front.")
            time.sleep(1)
        else:
            self.logger.warning(f"Application '{app_name}' not found.")

    def is_file_open_by_window_title(self, file_name, retries=3, delay=1):
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
                    return True
            except subprocess.CalledProcessError as e:
                self.logger.error(f"Failed to check window titles: {e}")
            time.sleep(delay)
        return False

    def send_keystroke(self, command, modifier):
        script = f'''
        tell application "System Events"
            keystroke "{command}" using {modifier} down
        end tell
        '''
        try:
            subprocess.run(['osascript', '-e', script], check=True)
        except subprocess.CalledProcessError as e:
            self.logger.error(f"Failed to send keystroke: {e}")

    def click_button(self, image_path):
        location = pyautogui.locateOnScreen(image_path, confidence=0.8)
        if location:
            center_x, center_y = pyautogui.center(location)
            x, y = self.adjust_coordinates_for_retina(center_x, center_y)
            pyautogui.moveTo(x, y, duration=0.05)
            pyautogui.click()
            return True
        return False

    def type_in_field(self, text):
        try:
            pyautogui.press('backspace')
            time.sleep(0.2)
            pyautogui.typewrite(text, interval=0.1)
            return True
        except Exception as e:
            self.logger.error(f"Failed to type text '{text}': {e}")
            return False

    def press_enter(self):
        pyautogui.press('enter')

    def click_detail_reports_button(self, image_path):
        time.sleep(2)
        location = pyautogui.locateOnScreen(image_path, confidence=0.8)
        if location:
            center_x, center_y = pyautogui.center(location)
            x, y = self.adjust_coordinates_for_retina(center_x, center_y)
            pyautogui.moveTo(x, y, duration=0.05)
            pyautogui.click()
            return (x, y)
        return None

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

    def ensure_no_subtotal(self):
        try:
            try:
                dont_subtotal_location = pyautogui.locateOnScreen(self.dont_subtotal_image, confidence=0.9)
            except Exception as e:
                self.logger.error(f"'Don't Subtotal' not currently selected")

            sub_total_title_location = pyautogui.locateOnScreen(self.sub_total_title_image, confidence=0.97)
            if not sub_total_title_location:
                return False

            center_x, center_y = pyautogui.center(sub_total_title_location)
            x, y = self.adjust_coordinates_for_retina(center_x, center_y)
            dropdown_x = x + 150
            dropdown_y = y
            pyautogui.moveTo(dropdown_x, dropdown_y, duration=.1)
            pyautogui.click()

            pyautogui.press('d')
            self.press_enter()
            return True
        except Exception as e:
            self.logger.error(f"Error ensuring 'Don't Subtotal': {e}")
            return False

    def set_phase_to_actual(self, other_reports_position):
        if other_reports_position is None:
            return

        or_x, or_y = other_reports_position
        phase_x = int(or_x + 30)
        phase_y = int(or_y + 80)

        current_phase = None
        for phase_name, image_path in getattr(self, 'PHASE_IMAGES', {}).items():
            try:
                if pyautogui.locateOnScreen(image_path, confidence=0.95):
                    current_phase = phase_name
                    break
            except Exception:
                pass

        if not current_phase:
            return

        if current_phase == 'actual':
            return

        pyautogui.moveTo(phase_x, phase_y, duration=0.05)
        pyautogui.click()
        time.sleep(0.5)

        if current_phase == 'estimated':
            target_y = phase_y + 50
        elif current_phase == 'working':
            target_y = phase_y + 25
        else:
            return

        pyautogui.moveTo(phase_x, target_y, duration=0.05)
        pyautogui.click()

    def get_po_log_file_link(self, filename):
        base_dir = Path("~/Desktop/PO_LOGS").expanduser()
        file_path = base_dir / filename
        if not file_path.exists():
            raise FileNotFoundError(f"File '{file_path}' does not exist.")
        return file_path.resolve().as_uri()

    def find_latest_po_log_file(self):
        base_dir = Path("~/Desktop/PO_LOGS").expanduser()
        if not base_dir.exists():
            self.report_progress("❌ PO_LOGS folder does not exist.")
            return None

        po_logs = list(base_dir.glob("PO_LOG_*.txt"))
        if not po_logs:
            self.report_progress("❌ No PO Log files found in PO_LOGS folder.")
            return None

        po_logs.sort(key=lambda f: f.stat().st_mtime, reverse=True)
        latest_file = po_logs[0]
        self.report_progress(f"Latest PO Log found: {latest_file}")
        return latest_file

    def extract_project_id_from_filename(self, filename):
        import re
        base = os.path.basename(filename)
        match = re.search(r'PO_LOG_(\d{4})', base)
        if match:
            return match.group(1)
        else:
            return self.file_name_text

    def extract_top_level_folder(self, file_path):
        # Split the path into components
        components = file_path.split(os.sep)

        # Check if the first component is empty (to handle paths starting with a slash)
        if not components[0]:
            components = components[1:]

        # Combine the root directory with the first component (folder name)
        top_level_folder = os.sep + components[0]

        return top_level_folder

    def upload_po_log(self, local_path, project_folder_name):
        dropbox_destination = f"/{project_folder_name}/5. Budget/1.5 PO Logs/{os.path.basename(local_path)}"
        self.report_progress(f"⏫ Uploading PO log to Dropbox: {dropbox_destination}")
        try:
            dropbox_api.upload_file(local_path, dropbox_destination)
            self.report_progress("✅ PO Log uploaded to Dropbox successfully.")
        except Exception as e:
            self.report_progress(f"❌ Failed to upload PO Log: {str(e)}")

    def click_button_with_retry_and_reopen(self, image_path, retries=3, delay=2):
        for attempt in range(retries):
            if self.click_button(image_path):
                return True
            time.sleep(delay)
        self.report_progress("Failed to click button. Closing and reopening budget.")
        self.close_budget()
        time.sleep(3)
        self.open_file(self.FILE_PATH)
        time.sleep(self.SLEEP_TIME_OPEN)
        return self.click_button(image_path)

    def close_budget(self):
        # Terminate the Showbiz Budgeting process
        for proc in psutil.process_iter(['name']):
            if proc.info['name'] == self.PROGRAM_NAME:
                proc.terminate()
                proc.wait()
                self.logger.info(f"Closed '{self.PROGRAM_NAME}' process.")
                break

    def run(self):
        if self.upload_only:
            self.report_progress("UPLOAD_ONLY mode enabled. Skipping Showbiz interaction.")

            latest_file = self.find_latest_po_log_file()
            if not latest_file:
                return

            # Get current timestamp
            timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
            new_filename = f"PO_LOG_{self.file_name_text}-{timestamp}.txt"
            new_file_path = Path(latest_file.parent) / new_filename

            # Rename the file to the new timestamped filename
            try:
                # Using os.rename or shutil.move if source and destination are on the same filesystem.
                # If you want to keep the original file as well, consider copying with shutil.copy instead.
                os.rename(latest_file, new_file_path)
                self.report_progress(f"Renamed {latest_file.name} to {new_filename}")
            except Exception as e:
                self.report_progress(f"❌ Failed to rename file: {e}")
                return

            # Now upload the newly renamed file
            project_folder_name = self.extract_top_level_folder(self.FILE_PATH).lstrip("/")
            if not project_folder_name:
                self.report_progress(
                    f"❌ Could not find project folder for project_id {self.file_name_text} in Dropbox.")
                return

            self.upload_po_log(str(new_file_path), project_folder_name)
            return str(new_file_path)

        # Normal (non-upload_only) mode:
        self.report_progress("Script started. Initializing.")

        if RUN_FROM_LOCAL:
            self.report_progress("RUN_FROM_LOCAL=True, offline mode.")
            local_file_path = os.path.join("../temp_files", self.LOCAL_BUDGET_FILENAME)
            if not os.path.exists(local_file_path):
                self.report_progress(f"Local budget file not found: {local_file_path}.")
                return
            self.report_progress(f"Using local file: {local_file_path}")
            actual_file_path = local_file_path
        else:
            actual_file_path = self.FILE_PATH

        self.report_progress("Checking if Showbiz Budgeting is running.")
        running = self.is_program_running(self.PROGRAM_NAME)
        if not running:
            self.report_progress(f"Opening '{self.PROGRAM_NAME}' with file '{actual_file_path}'.")
            self.open_file(actual_file_path)
            time.sleep(self.SLEEP_TIME_OPEN)
        else:
            self.report_progress(f"'{self.PROGRAM_NAME}' is already running.")
            if self.is_file_open_by_window_title(os.path.basename(actual_file_path)):
                self.report_progress(f"Budget file '{actual_file_path}' is open. Bringing to the front.")
                self.bring_to_front(self.PROGRAM_NAME)
                time.sleep(1)
            else:
                self.report_progress("File isn't open yet. Opening now.")
                self.open_file(actual_file_path)
                time.sleep(self.SLEEP_TIME_AFTER_CLICK)

        self.report_progress("Opening Print dialog with Command+P.")
        self.send_keystroke('p', 'command')
        time.sleep(self.SLEEP_TIME_AFTER_KEYSTROKE)

        self.report_progress("Locating and clicking 'Detail Reports' button.")
        other_reports_position = self.click_detail_reports_button(self.DETAIL_REPORTS_BUTTON_IMAGE)
        if other_reports_position is None:
            self.report_progress("Cannot find 'Detail Reports' button. Aborting.")
            return

        time.sleep(0.1)
        self.report_progress("Ensuring phase is set to Actual.")
        self.set_phase_to_actual(other_reports_position)

        self.report_progress("Ensuring all checkboxes and settings.")
        self.ensure_all_settings()

        self.report_progress("Ensuring 'Don't Subtotal' is selected.")
        self.ensure_no_subtotal()

        self.report_progress("Clicking 'Preview' button.")
        if not self.click_button(self.BUTTON_IMAGES['PREVIEW']):
            self.report_progress("Failed to click 'Preview'. Aborting.")
            return

        self.report_progress("Clicking 'Raw Text' button.")
        if not self.click_button_with_retry_and_reopen(self.BUTTON_IMAGES['RAW_TEXT'], retries=3, delay=2):
            self.report_progress("Failed to click 'Raw Text' after multiple retries and re-opening. Aborting.")
            return

        self.report_progress("Confirming dialog (Press Enter).")
        self.press_enter()

        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        filename = "PO_LOG_" + self.file_name_text + "-" + timestamp
        self.report_progress(f"Typing filename: {filename}.")
        self.type_in_field(filename)

        self.report_progress("Saving the file (Press Enter).")
        self.press_enter()

        self.report_progress("Confirming save (Press Enter again).")
        self.press_enter()

        self.report_progress("Clicking 'Close' button.")
        if not self.click_button(self.BUTTON_IMAGES['CLOSE']):
            self.report_progress("Failed to click 'Close' button.")
            return

        self.report_progress("Script finished successfully. Retrieving file link.")
        file_uri = self.get_po_log_file_link(filename + ".txt")

        parsed = urlparse(file_uri)
        po_log_local_path = parsed.path
        if not os.path.exists(po_log_local_path):
            self.report_progress(f"❌ PO log not found at '{po_log_local_path}'")
            return file_uri

        if not self.project_folder_name:
            self.report_progress("❌ Could not determine project_folder_name from file_path.")
            return file_uri

        self.upload_po_log(po_log_local_path, self.project_folder_name)
        return file_uri

def worker():
    while True:
        job = job_queue.get()
        if job is None:
            break
        # job can be (job_id, project_id, file_path, upload_only)
        # If upload_only not provided, default to False
        if len(job) == 4:
            job_id, project_id, file_path, upload_only = job
        else:
            job_id, project_id, file_path = job
            upload_only = False

        def progress_callback(msg):
            if job_id in job_statuses:
                job_statuses[job_id]["progress"] = msg
                logger.info(f"Job {job_id} progress: {msg}")

        job_statuses[job_id]["status"] = "processing"
        try:
            printer = ShowbizPoLogPrinter(project_id=project_id, file_path=file_path, progress_callback=progress_callback, upload_only=upload_only)
            result_link = printer.run()
            job_statuses[job_id]["status"] = "done"
            job_statuses[job_id]["result"] = result_link
        except Exception as e:
            job_statuses[job_id]["status"] = "error"
            job_statuses[job_id]["error"] = str(e)
        finally:
            job_queue.task_done()

thread = threading.Thread(target=worker, daemon=True)
thread.start()

@app.route('/enqueue', methods=['POST'])
def enqueue_job():
    data = request.get_json()
    if not data or "project_id" not in data:
        return jsonify({"error": "Missing project_id"}), 400

    project_id = data["project_id"]
    file_path = data.get("file_path", "")
    upload_only = data.get("upload_only", UPLOAD_ONLY)

    job_id = datetime.now().strftime("%Y%m%d%H%M%S%f")
    job_statuses[job_id] = {
        "status": "queued",
        "result": None,
        "error": None,
        "progress": None
    }

    if upload_only:
        logger.info(f"Enqueued job {job_id} for project_id={project_id} in UPLOAD_ONLY mode.")
        job_queue.put((job_id, project_id, file_path, True))
    else:
        if not file_path:
            return jsonify({"error": "Missing file_path for normal mode"}), 400
        logger.info(f"Enqueued job {job_id} for project_id={project_id}, file_path={file_path}")
        job_queue.put((job_id, project_id, file_path))

    return jsonify({"job_id": job_id}), 200

@app.route('/status/<job_id>', methods=['GET'])
def get_status(job_id):
    if job_id not in job_statuses:
        return jsonify({"error": "Invalid job_id"}), 404
    return jsonify(job_statuses[job_id]), 200

if __name__ == '__main__':
    # Run the server
    app.run(host='0.0.0.0', port=5004)