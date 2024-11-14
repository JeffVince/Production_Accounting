import psutil
import pyautogui
import time
import os
import shlex
import pywinctl
from AppKit import NSWorkspace


# Open Showbiz Budgeting
file_path = "/Users/haske107/Library/CloudStorage/Dropbox-OpheliaLLC/2024/2416 - Whop Keynote/5. Budget/1.3 Actuals/ACTUALS 2416 - Whop Keynote.mbb"


# Define the program name you’re looking for
program_name = "Showbiz Budgeting"  # Adjust if the process name differs


# Function to check if the program is running
def is_program_running(program_name):
    for proc in psutil.process_iter(['name']):
        if proc.info['name'] == program_name:
            return True
    return False


# Check if Showbiz Budgeting is open
running = is_program_running(program_name)

# Open Showbiz Budgeting if it isn’t running
if not running:
    os.system(f'open {shlex.quote(file_path)}')
    time.sleep(5)  # Adjust for loading time
else:
    print("Showbiz Budgeting is already running.")
    # Find all windows with "Showbiz Budgeting" in the title
    # Retrieve all windows with "Showbiz Budgeting" in the title
    print("Pulling Showbiz Budgeting into focus.")

    app_name = "Showbiz Budgeting"  # Adjust if needed
    workspace = NSWorkspace.sharedWorkspace()

    apps = workspace.runningApplications()
    showbiz_app = None
    for app in apps:
        if app.localizedName() == app_name:
            showbiz_app = app
            break

    if showbiz_app:
        showbiz_app.activateWithOptions_(1)
        time.sleep(1)  # Wait to ensure it’s in focus
    else:
        print(f"Application '{app_name}' not found.")


   # time.sleep(1)  # Wait a moment to ensure the window is in focus

   # pyautogui.hotkey('command', 'o')


# Wait a moment to ensure the application is in focus
#time.sleep(2)

