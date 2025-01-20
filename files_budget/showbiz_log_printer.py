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
from files_dropbox.dropbox_api import dropbox_api
setup_logging()
logger = logging.getLogger('admin_logger')
RUN_FROM_LOCAL = False
UPLOAD_ONLY = False
job_queue = queue.Queue()
job_statuses = {}
app = Flask(__name__)

def retry_on_failure(action_func, action_description='', retries=3, delay=3, fallback_func=None):
    """
    🌀 Generic retry wrapper: Tries the given action_func multiple times.
    - If the action fails, waits 'delay' seconds and tries again.
    - If all attempts fail, runs fallback_func (if provided).
    - Returns the result of action_func if it succeeds, else None.

    :param action_func: A callable that performs the action. Should raise Exception on failure.
    :param action_description: A string describing the action for logging.
    :param retries: Number of times to retry.
    :param delay: Delay in seconds between retries.
    :param fallback_func: A function to call if all retries fail.
    :return: The result of action_func on success, else None.
    """
    for attempt in range(1, retries + 1):
        try:
            logger.info(f'🔄 Attempt {attempt}/{retries} for action: {action_description}')
            result = action_func()
            logger.info(f'✅ Action succeeded: {action_description}')
            return result
        except Exception as e:
            logger.error(f'❌ Attempt {attempt} failed for action: {action_description}. Error: {e}')
            if attempt < retries:
                logger.info(f'🕒 Waiting {delay} seconds before retrying...')
                time.sleep(delay)
    logger.error(f'💥 All {retries} attempts failed for action: {action_description}')
    if fallback_func:
        logger.info('🔧 Running fallback function due to repeated failures.')
        fallback_func()
    return None

def bring_app_to_front(app_name):
    """
    🪟 Brings the specified application to the front using NSWorkspace.
    Raises an exception if the application is not found.
    """
    workspace = NSWorkspace.sharedWorkspace()
    apps = workspace.runningApplications()
    app = next((a for a in apps if a.localizedName() == app_name), None)
    if app:
        app.activateWithOptions_(1)
        time.sleep(1)
    else:
        raise Exception(f"Application '{app_name}' not found.")

class ShowbizPoLogPrinter:
    """
    🎬 ShowbizPoLogPrinter
    This class handles the interaction with Showbiz Budgeting to print out PO logs.
    It can run in normal mode (interact and print logs) or upload-only mode (just handle uploads).

    Major Changes:
    - Removed Singleton pattern, each instance is independent.
    - Added retry logic for critical actions.
    - Enhanced logging and comments.
    """
    LOCAL_BUDGET_FILENAME = 'ACTUALS 2416 - Whop Keynote.mbb'
    PROGRAM_NAME = 'Showbiz Budgeting'
    PRINT_WINDOW_TITLE = 'Print'
    SLEEP_TIME_OPEN = 8
    SLEEP_TIME_AFTER_KEYSTROKE = 1
    SLEEP_TIME_AFTER_CLICK = 2
    RETRY_LIMIT = 3
    pyautogui.FAILSAFE = True
    pyautogui.PAUSE = 0.5
    MATCHING_METHOD = cv2.TM_CCOEFF_NORMED
    MATCHING_THRESHOLD = 0.7

    def __init__(self, project_number='REPLACE_WITH_project_number', file_path='REPLACE_WITH_FILE_PATH', progress_callback=None, upload_only=False):
        """
        💻 Constructor:
        Initializes the printer with the given project_number and file_path.

        :param project_number: A string representing the project ID.
        :param file_path: The full path to the budget file.
        :param progress_callback: A callback function for reporting progress.
        :param upload_only: Boolean indicating if we only want to upload a previously generated PO log.
        """
        self.logger = logging.getLogger('budget_logger')
        self.progress_callback = progress_callback
        self.file_name_text = project_number
        self.FILE_PATH = file_path
        self.upload_only = upload_only
        self.SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
        self.temp_x = 0
        self.temp_y = 0
        self.project_folder_name = None
        if not self.upload_only and self.FILE_PATH.startswith('/'):
            segments = self.FILE_PATH.strip('/').split('/')
            if len(segments) >= 1:
                self.project_folder_name = segments[0]
            else:
                self.logger.error('[__init__] - ❌ Cannot determine project_folder_name from file_path.')
        if not self.upload_only:
            self.setup_images()
        self.logger.info('[__init__] - 🎉 Initialized ShowbizPoLogPrinter instance.')

    def setup_images(self):
        """
        🖼️ Setup and define all image paths for buttons, checkboxes, and other UI elements.
        """
        self.DETAIL_REPORTS_BUTTON_IMAGE = os.path.join(self.SCRIPT_DIR, 'pics', 'detail_report_tab.png')
        self.CHECKBOX_IMAGES = {'PO_LOG_CHECKBOX': {'checked': os.path.join(self.SCRIPT_DIR, 'pics', 'po_log.png'), 'unchecked': os.path.join(self.SCRIPT_DIR, 'pics', 'uncheck_po_log.png')}, 'SHOW_POS_CHECKBOX': {'checked': os.path.join(self.SCRIPT_DIR, 'pics', 'show_po.png'), 'unchecked': os.path.join(self.SCRIPT_DIR, 'pics', 'uncheck_show_po.png')}, 'SHOW_ACTUALIZED_ITEMS_CHECKBOX': {'checked': os.path.join(self.SCRIPT_DIR, 'pics', 'show_actuals.png'), 'unchecked': os.path.join(self.SCRIPT_DIR, 'pics', 'uncheck_show_actual.png')}}
        self.BUTTON_IMAGES = {'PREVIEW': os.path.join(self.SCRIPT_DIR, 'pics', 'preview.png'), 'RAW_TEXT': os.path.join(self.SCRIPT_DIR, 'pics', 'raw_text.png'), 'OKAY': os.path.join(self.SCRIPT_DIR, 'pics', 'okay.png'), 'SAVE_AS_FORM': os.path.join(self.SCRIPT_DIR, 'pics', 'save_as_form.png'), 'SAVE': os.path.join(self.SCRIPT_DIR, 'pics', 'save.png'), 'CLOSE': os.path.join(self.SCRIPT_DIR, 'pics', 'close.png')}
        self.PHASE_IMAGES = {'actual': os.path.join(self.SCRIPT_DIR, 'pics', 'phase_actual.png'), 'estimated': os.path.join(self.SCRIPT_DIR, 'pics', 'phase_estimated.png'), 'working': os.path.join(self.SCRIPT_DIR, 'pics', 'phase_working.png')}
        self.dont_subtotal_image = os.path.join(self.SCRIPT_DIR, 'pics', 'dont_subtotal.png')
        self.sub_total_title_image = os.path.join(self.SCRIPT_DIR, 'pics', 'sub_total_title.png')
        self.dont_subtotal_dropdown_image = os.path.join(self.SCRIPT_DIR, 'pics', 'dont_subtotal_dropdown.png')
        self.logger.info('[setup_images] - 🖼️ Image paths initialized successfully.')

    def report_progress(self, message):
        """
        💬 Report progress: Logs and optionally calls the progress callback.
        """
        self.logger.info(f'[report_progress] - 📣 Progress: {message}')
        if self.progress_callback:
            self.progress_callback(message)

    def adjust_coordinates_for_retina(self, x, y):
        return (x / 2, y / 2)

    def is_program_running(self, program_name):
        """
        🏃 Checks if the given program is running.
        """
        try:
            running = any((proc.info['name'] == program_name for proc in psutil.process_iter(['name'])))
            return running
        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess) as e:
            self.logger.error(f'[is_program_running] - ❌ Error checking if program is running: {e}')
            return False

    def open_file(self, file_path):
        """
        📂 Opens the specified file using the 'open' command.
        """
        try:
            subprocess.run(['open', '/Users/haske107/Library/CloudStorage/Dropbox-OpheliaLLC/2024' + file_path], check=True)
            self.logger.info(f"[open_file] - 📂 Opened file '{file_path}'.")
        except subprocess.CalledProcessError as e:
            self.logger.error(f"[open_file] - ❌ Failed to open file '{file_path}': {e}")
            raise

    def bring_to_front(self, app_name):
        """
        💻 Brings the specified application to the front.
        Uses retry logic in case of delays.
        """

        def action():
            bring_app_to_front(app_name)
        return retry_on_failure(action, action_description=f'Bringing {app_name} to front')

    def is_file_open_by_window_title(self, file_name, retries=3, delay=1):
        """
        🔎 Checks if a window containing file_name is open in Showbiz.
        Uses AppleScript and retries.
        """

        def check_window():
            script = f'\n                tell application "System Events"\n                    tell process "{self.PROGRAM_NAME}"\n                        set windowNames to name of windows\n                        repeat with winName in windowNames\n                            if winName contains "{file_name}" then\n                                return "true"\n                            end if\n                        end repeat\n                        return "false"\n                    end tell\n                end tell\n            '
            result = subprocess.run(['osascript', '-e', script], capture_output=True, text=True)
            output = result.stdout.strip().lower()
            if output == 'true':
                return True
            else:
                raise Exception('Window not found')
        for attempt in range(retries):
            try:
                self.bring_to_front(self.PROGRAM_NAME)
                if check_window():
                    return True
            except Exception as e:
                self.logger.error(f'[is_file_open_by_window_title] - ❌ Attempt {attempt + 1}/{retries} to find window: {e}')
            time.sleep(delay)
        return False

    def send_keystroke(self, command, modifier):
        """
        ⌨️ Sends a keystroke command to the system.
        """
        script = f'\n        tell application "System Events"\n            keystroke "{command}" using {modifier} down\n        end tell\n        '
        try:
            subprocess.run(['osascript', '-e', script], check=True)
        except subprocess.CalledProcessError as e:
            self.logger.error(f'[send_keystroke] - ❌ Failed to send keystroke: {e}')
            raise

    def click_button(self, image_path, confidence=0.8):
        """
        🖱️ Clicks a button identified by image_path on screen using pyautogui.
        """
        location = pyautogui.locateOnScreen(image_path, confidence=confidence)
        if location:
            (center_x, center_y) = pyautogui.center(location)
            (x, y) = self.adjust_coordinates_for_retina(center_x, center_y)
            if image_path == os.path.join(self.SCRIPT_DIR, 'pics', 'raw_text.png'):
                self.temp_x = x
                self.temp_y = y
            pyautogui.moveTo(x, y, duration=0.1)
            pyautogui.click()
            return True
        else:
            raise Exception(f'Button image not found: {image_path}')

    def type_in_field(self, text):
        """
        📝 Types the given text into the currently active field.
        """
        try:
            pyautogui.press('backspace')
            time.sleep(0.2)
            pyautogui.typewrite(text, interval=0.1)
            return True
        except Exception as e:
            self.logger.error(f"[type_in_field] - ❌ Failed to type text '{text}': {e}")
            return False

    def press_enter(self):
        pyautogui.press('enter')

    def click_detail_reports_button(self, image_path):
        """
        📝 Clicks the 'Detail Reports' button.
        """

        def action():
            time.sleep(2)
            location = pyautogui.locateOnScreen(image_path, confidence=0.8)
            if not location:
                raise Exception('Detail Reports button not found.')
            (center_x, center_y) = pyautogui.center(location)
            (x, y) = self.adjust_coordinates_for_retina(center_x, center_y)
            pyautogui.moveTo(x, y, duration=0.05)
            pyautogui.click()
            return (x, y)
        return retry_on_failure(action, action_description='Clicking Detail Reports button', fallback_func=self.close_budget)

    def ensure_checkbox_state(self, checkbox_name, checked_image_path, unchecked_image_path, desired_state, retry=3):
        """
        ✅ Ensures a checkbox is in the desired state (checked or unchecked).
        Retries a few times before giving up.
        """
        for attempt in range(1, retry + 1):
            try:
                if desired_state:
                    location_checked = pyautogui.locateOnScreen(checked_image_path, confidence=0.97)
                    if location_checked:
                        self.logger.info(f"[ensure_checkbox_state] - ✅ Checkbox '{checkbox_name}' is already checked.")
                        return True
                    else:
                        location_unchecked = pyautogui.locateOnScreen(unchecked_image_path, confidence=0.97)
                        if location_unchecked:
                            (center_x, center_y) = pyautogui.center(location_unchecked)
                            (x, y) = self.adjust_coordinates_for_retina(center_x, center_y)
                            pyautogui.moveTo(x, y, duration=0.1)
                            pyautogui.click()
                            self.logger.info(f"[ensure_checkbox_state] - 🔘 Toggled '{checkbox_name}' to checked.")
                            time.sleep(1)
                        else:
                            self.logger.warning(f"[ensure_checkbox_state] - ⚠️ Unchecked image for '{checkbox_name}' not found on attempt {attempt}.")
                else:
                    location_unchecked = pyautogui.locateOnScreen(unchecked_image_path, confidence=0.97)
                    if location_unchecked:
                        self.logger.info(f"[ensure_checkbox_state] - ✅ Checkbox '{checkbox_name}' is already unchecked.")
                        return True
                    else:
                        location_checked = pyautogui.locateOnScreen(checked_image_path, confidence=0.8)
                        if location_checked:
                            (center_x, center_y) = pyautogui.center(location_checked)
                            (x, y) = self.adjust_coordinates_for_retina(center_x, center_y)
                            pyautogui.moveTo(x, y, duration=0.1)
                            pyautogui.click()
                            self.logger.info(f"[ensure_checkbox_state] - 🔘 Toggled '{checkbox_name}' to unchecked.")
                            time.sleep(1)
                        else:
                            self.logger.warning(f"[ensure_checkbox_state] - ⚠️ Checked image for '{checkbox_name}' not found on attempt {attempt}.")
            except Exception as e:
                self.logger.error(f"[ensure_checkbox_state] - ❌ Error ensuring checkbox '{checkbox_name}' state: {e}")
        self.logger.critical(f"[ensure_checkbox_state] - 💥 Failed to set checkbox '{checkbox_name}' to desired state ({desired_state}) after {retry} attempts.")
        return False

    def ensure_all_settings(self):
        """
        ✅ Ensures all desired checkboxes are in the correct state.
        Uses the logic:
          - Invoice, Time Card, Check, Petty Cash, Credit Card, Projection, EFT, Show Inactive Items = checked
          - Non-Detail = unchecked
        """
        desired_checkboxes = {'Invoice': True, 'Time Card': True, 'Check': True, 'Petty Cash': True, 'Credit Card': True, 'Projection': True, 'EFT': True, 'Non-Detail': False, 'Show Inactive Items': True}
        all_good = True
        for (checkbox_name, should_be_checked) in desired_checkboxes.items():
            self.logger.debug(f"[ensure_all_settings] - ⚙️ Ensuring '{checkbox_name}' is {('checked' if should_be_checked else 'unchecked')}.")
            checked_image = os.path.join(self.SCRIPT_DIR, 'pics', f"{checkbox_name.lower().replace(' ', '_').replace('-', '_')}_check.png")
            unchecked_image = os.path.join(self.SCRIPT_DIR, 'pics', f"{checkbox_name.lower().replace(' ', '_').replace('-', '_')}_uncheck.png")
            if not self.ensure_checkbox_state(checkbox_name, checked_image, unchecked_image, should_be_checked):
                all_good = False
        if all_good:
            self.logger.info('[ensure_all_settings] - ✅ All desired settings are in place.')
        else:
            self.logger.warning('[ensure_all_settings] - ⚠️ Some settings could not be corrected.')

    def close_preview(self, temp_x, temp_y):
        phase_x = int(temp_x - 100)
        phase_y = temp_y
        pyautogui.moveTo(phase_x, phase_y, duration=0.05)
        pyautogui.click()

    def ensure_no_subtotal(self):
        """
        📊 Ensures 'Don't Subtotal' is selected.
        If not selectable, logs an error but continues.
        """
        try:
            sub_total_title_location = pyautogui.locateOnScreen(self.sub_total_title_image, confidence=0.97)
            if not sub_total_title_location:
                self.logger.info("[ensure_no_subtotal] - ℹ️ 'Subtotal Title' not found, might already be don't subtotal.")
                return True
            (center_x, center_y) = pyautogui.center(sub_total_title_location)
            (x, y) = self.adjust_coordinates_for_retina(center_x, center_y)
            dropdown_x = x + 150
            dropdown_y = y
            pyautogui.moveTo(dropdown_x, dropdown_y, duration=0.1)
            pyautogui.click()
            pyautogui.press('d')
            self.press_enter()
            self.logger.info("[ensure_no_subtotal] - 📝 'Don't Subtotal' selected.")
            return True
        except Exception as e:
            self.logger.error(f"[ensure_no_subtotal] - ❌ Error ensuring 'Don't Subtotal': {e}")
            return False

    def set_phase_to_actual(self, other_reports_position):
        """
        🎬 Sets the phase to Actual by simulating UI interactions.
        """
        if other_reports_position is None:
            self.logger.warning('[set_phase_to_actual] - ⚠️ Cannot set phase to Actual. Other Reports position not found.')
            return
        (or_x, or_y) = other_reports_position
        phase_x = int(or_x + 30)
        phase_y = int(or_y + 80)
        pyautogui.moveTo(phase_x, phase_y, duration=0.05)
        pyautogui.click()
        self.type_in_field('a')
        self.press_enter()
        self.logger.info("[set_phase_to_actual] - 📝 Phase set to 'Actual'.")

    def retry_set_phase_to_actual(self, other_reports_position, retries=3):
        """
        🔄 Attempts to set the phase to Actual multiple times.
        If it fails, it will close and reopen the budget file and try again.
        """
        for attempt in range(1, retries + 1):
            self.logger.info(f'[retry_set_phase_to_actual] - 🔄 Attempt {attempt}/{retries} to set phase to Actual.')
            try:
                self.set_phase_to_actual(other_reports_position)
                actual_phase_img = self.PHASE_IMAGES.get('actual')
                if actual_phase_img and pyautogui.locateOnScreen(actual_phase_img, confidence=0.95):
                    self.logger.info("[retry_set_phase_to_actual] - ✅ Phase successfully set to 'Actual'.")
                    return True
                else:
                    raise Exception("Phase not confirmed as 'Actual' after setting.")
            except Exception as e:
                self.logger.error(f'[retry_set_phase_to_actual] - ❌ Failed to set phase to Actual on attempt {attempt}: {e}')
                if attempt < retries:
                    self.logger.info('[retry_set_phase_to_actual] - 🔧 Closing and reopening the budget to retry phase setting.')
                    self.close_budget()
                    time.sleep(3)
                    self.logger.info(f"[retry_set_phase_to_actual] - 📂 Reopening file '{self.FILE_PATH}' to retry phase setting.")
                    self.open_file(self.FILE_PATH)
                    time.sleep(self.SLEEP_TIME_OPEN)
                    self.report_progress('🖨️ Opening Print dialog with Command+P for retry.')
                    self.send_keystroke('p', 'command')
                    time.sleep(self.SLEEP_TIME_AFTER_KEYSTROKE)
                    self.report_progress("🔎 Locating and clicking 'Detail Reports' button for retry.")
                    other_reports_position = self.click_detail_reports_button(self.DETAIL_REPORTS_BUTTON_IMAGE)
                    if not other_reports_position:
                        self.logger.error("[retry_set_phase_to_actual] - ❌ Cannot find 'Detail Reports' button during retry. Aborting.")
                        return False
                else:
                    self.logger.critical('[retry_set_phase_to_actual] - 💥 All attempts to set phase to Actual have failed.')
                    return False
        return False

    def get_po_log_file_link(self, filename):
        """
        🔗 Returns a file URI for the given PO log filename on the user's Desktop.
        """
        base_dir = Path('~/Desktop/PO_LOGS').expanduser()
        file_path = base_dir / filename
        if not file_path.exists():
            raise FileNotFoundError(f"❌ File '{file_path}' does not exist.")
        return file_path.resolve().as_uri()

    def find_latest_po_log_file(self):
        """
        🔍 Finds the latest PO_LOG_*.txt file from ~/Desktop/PO_LOGS.
        """
        base_dir = Path('~/Desktop/PO_LOGS').expanduser()
        if not base_dir.exists():
            self.report_progress('❌ PO_LOGS folder does not exist.')
            return None
        po_logs = list(base_dir.glob('PO_LOG_*.txt'))
        if not po_logs:
            self.report_progress('❌ No PO Log files found in PO_LOGS folder.')
            return None
        po_logs.sort(key=lambda f: f.stat().st_mtime, reverse=True)
        latest_file = po_logs[0]
        self.report_progress(f'📄 Latest PO Log found: {latest_file}')
        return latest_file

    def extract_top_level_folder(self, file_path):
        """
        🎛️ Extracts the top-level folder name from the given file_path.
        """
        components = file_path.split(os.sep)
        if not components[0]:
            components = components[1:]
        top_level_folder = os.sep + components[0]
        return top_level_folder

    def upload_po_log(self, local_path, project_folder_name):
        """
        ⏫ Uploads the PO log file to Dropbox under the correct project folder.
        """
        dropbox_destination = f'/{project_folder_name}/5. Budget/1.5 PO Logs/{os.path.basename(local_path)}'
        self.report_progress(f'⏫ Uploading PO log to Dropbox: {dropbox_destination}')
        try:
            dropbox_api.upload_file(local_path, dropbox_destination)
            self.report_progress('✅ PO Log uploaded to Dropbox successfully.')
        except Exception as e:
            self.report_progress(f'❌ Failed to upload PO Log: {str(e)}')

    def click_button_with_retry_and_reopen(self, image_path, retries=3, delay=2):
        """
        ⏳ Attempts to click a button multiple times. If it fails, closes the budget and reopens.
        """
        for attempt in range(1, retries + 1):
            try:
                (x, y) = self.click_button(image_path)
                if not x or y:
                    return False
            except Exception:
                self.logger.warning(f"[click_button_with_retry_and_reopen] - ⚠️ Attempt {attempt} to click '{image_path}' failed.")
            time.sleep(delay)
        self.report_progress('❌ Failed to click button after retries. Closing and reopening budget.')
        self.close_budget()
        time.sleep(3)
        self.open_file(self.FILE_PATH)
        time.sleep(self.SLEEP_TIME_OPEN)
        try:
            return self.click_button(image_path)
        except Exception:
            return False

    def close_budget(self):
        """
        🔒 Closes the Showbiz Budgeting application by terminating its process.
        Ensures a clean state for the next run.
        """
        self.logger.info('[close_budget] - 🔻 Closing Showbiz Budgeting.')
        for proc in psutil.process_iter(['name']):
            if proc.info['name'] == self.PROGRAM_NAME:
                proc.terminate()
                proc.wait()
                self.logger.info(f"[close_budget] - ✅ Closed '{self.PROGRAM_NAME}' process.")
                break

    def run(self):
        try:
            if not self.upload_only:
                self.logger.info('[run] - 🧹 Ensuring a clean state before starting job.')
                self.close_budget()
                time.sleep(3)
            if self.upload_only:
                return
            self.report_progress('🎬 Script started. Initializing Showbiz process.')
            actual_file_path = self.FILE_PATH
            self.report_progress('🕵️ Checking if Showbiz Budgeting is running.')
            running = self.is_program_running(self.PROGRAM_NAME)
            if running:
                self.report_progress(f"'{self.PROGRAM_NAME}' is already running. Closing it to ensure a fresh state.")
                self.close_budget()
                time.sleep(3)
            self.report_progress(f"🚀 Opening '{self.PROGRAM_NAME}' with file '{actual_file_path}'.")
            self.open_file(actual_file_path)
            time.sleep(self.SLEEP_TIME_OPEN)
            self.report_progress('🖨️ Opening Print dialog with Command+P.')
            self.send_keystroke('p', 'command')
            time.sleep(self.SLEEP_TIME_AFTER_KEYSTROKE)
            self.report_progress("🔎 Locating and clicking 'Detail Reports' button.")
            other_reports_position = self.click_detail_reports_button(self.DETAIL_REPORTS_BUTTON_IMAGE)
            if not other_reports_position:
                self.report_progress("❌ Cannot find 'Detail Reports' button. Aborting.")
                return
            self.report_progress('⚙️ Ensuring phase is set to Actual.')
            if not self.retry_set_phase_to_actual(other_reports_position, retries=3):
                self.report_progress('❌ Failed to set phase to Actual after multiple attempts. Aborting.')
                return
            self.report_progress('⚙️ Ensuring all checkboxes and settings.')
            self.ensure_all_settings()
            self.report_progress("🔧 Ensuring 'Don't Subtotal' is selected.")
            self.ensure_no_subtotal()
            self.report_progress("👁️ Clicking 'Preview' button.")
            if not retry_on_failure(lambda : self.click_button(self.BUTTON_IMAGES['PREVIEW']), action_description='Clicking PREVIEW button', fallback_func=self.close_budget):
                self.report_progress("❌ Failed to click 'Preview'. Aborting.")
                return
            self.report_progress("👁️ Clicking 'Raw Text' button.")
            if not retry_on_failure(lambda : self.click_button(self.BUTTON_IMAGES['RAW_TEXT']), action_description='Clicking Raw Text button', fallback_func=self.close_budget):
                self.report_progress("❌ Failed to click 'Raw Text' after multiple retries. Aborting.")
                return
            self.report_progress('✔️ Confirming dialog (Press Enter).')
            self.press_enter()
            time.sleep(1)
            timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
            filename = 'PO_LOG_' + self.file_name_text + '-' + timestamp
            self.report_progress(f'📝 Typing filename: {filename}.')
            self.type_in_field(filename)
            self.report_progress('💾 Saving the file (Press Enter).')
            self.press_enter()
            time.sleep(1)
            self.report_progress('💾 Confirming save (Press Enter again).')
            self.press_enter()
            time.sleep(2)
            self.report_progress("🚪 Clicking 'Close' button.")
            self.close_preview(self.temp_x, self.temp_y)
            self.report_progress('🏁 Script finished successfully. Retrieving file link.')
            file_uri = self.get_po_log_file_link(filename + '.txt')
            parsed = urlparse(file_uri)
            po_log_local_path = parsed.path
            if not os.path.exists(po_log_local_path):
                self.report_progress(f"❌ PO log not found at '{po_log_local_path}'")
                return file_uri
            if not self.project_folder_name:
                self.report_progress('❌ Could not determine project_folder_name from file_path.')
                return file_uri
            self.upload_po_log(po_log_local_path, self.project_folder_name)
            return file_uri
        finally:
            if not self.upload_only:
                self.logger.info('[run] - 🧼 Cleaning up after job run. Closing Showbiz to ensure a fresh start next time.')
                self.close_budget()

def worker():
    """
    🎡 Background worker thread that processes jobs from the job_queue.
    """
    while True:
        job = job_queue.get()
        if job is None:
            break
        if len(job) == 4:
            (job_id, project_number, file_path, upload_only) = job
        else:
            (job_id, project_number, file_path) = job
            upload_only = False

        def progress_callback(msg):
            if job_id in job_statuses:
                job_statuses[job_id]['progress'] = msg
                logger.info(f'🏗️ Job {job_id} progress: {msg}')
        job_statuses[job_id]['status'] = 'processing'
        try:
            printer = ShowbizPoLogPrinter(project_number=project_number, file_path=file_path, progress_callback=progress_callback, upload_only=upload_only)
            result_link = printer.run()
            job_statuses[job_id]['status'] = 'done'
            job_statuses[job_id]['result'] = result_link
        except Exception as e:
            job_statuses[job_id]['status'] = 'error'
            job_statuses[job_id]['error'] = str(e)
        finally:
            job_queue.task_done()

@app.route('/enqueue', methods=['POST'])
def enqueue_job():
    data = request.get_json()
    if not data or 'project_number' not in data:
        return (jsonify({'error': 'Missing project_number'}), 400)
    project_number = data['project_number']
    file_path = data.get('file_path', '')
    upload_only = data.get('upload_only', UPLOAD_ONLY)
    job_id = datetime.now().strftime('%Y%m%d%H%M%S%f')
    job_statuses[job_id] = {'status': 'queued', 'result': None, 'error': None, 'progress': None}
    if upload_only:
        logger.info(f'📝 Enqueued job {job_id} for project_number={project_number} in UPLOAD_ONLY mode.')
        job_queue.put((job_id, project_number, file_path, True))
    else:
        if not file_path:
            return (jsonify({'error': 'Missing file_path for normal mode'}), 400)
        logger.info(f'📝 Enqueued job {job_id} for project_number={project_number}, file_path={file_path}')
        job_queue.put((job_id, project_number, file_path))
    return (jsonify({'job_id': job_id}), 200)

@app.route('/status/<job_id>', methods=['GET'])
def get_status(job_id):
    if job_id not in job_statuses:
        return (jsonify({'error': 'Invalid job_id'}), 404)
    return (jsonify(job_statuses[job_id]), 200)
if __name__ == '__main__':
    thread = threading.Thread(target=worker, daemon=True)
    thread.start()
    app.run(host='0.0.0.0', port=5004, use_reloader=False)