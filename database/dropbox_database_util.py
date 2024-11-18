# dropbox_database_util.py

import sqlite3
import logging
import threading
import os  # Import os module to handle file paths

db_lock = threading.Lock()

TARGET_PURCHASE_ORDERS_FOLDER = os.getenv('TARGET_PURCHASE_ORDERS_FOLDER', '1. Purchase Orders')


def dict_factory(cursor, row):
    """
    Converts SQLite row to a dictionary.
    """
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d


def get_db_path():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.abspath(os.path.join(script_dir, '..'))
    db_dir = os.path.join(project_root, 'database')
    db_path = os.path.join(db_dir, 'database/processed_files.db')

    # Log all intermediate paths for debugging
    logging.info(f"Script directory: {script_dir}")
    logging.info(f"Project root: {project_root}")
    logging.info(f"Database directory: {db_dir}")
    logging.info(f"Final database path: {db_path}")

    if not os.path.exists(db_dir):
        os.makedirs(db_dir, exist_ok=True)
    
    return db_path


def initialize_database():
    """
    Initializes the SQLite database with the necessary schema.
    """
    db_path = get_db_path()
    logging.info(f"Initializing database at {db_path}")

    conn = sqlite3.connect(db_path)
    conn.row_factory = dict_factory  # Set row factory to dictionary
    cursor = conn.cursor()

    try:
        # Create events table with additional columns and a unique constraint to prevent duplicates
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                file_id TEXT,
                file_name TEXT NOT NULL,
                path TEXT NOT NULL,
                old_path TEXT,
                event_type TEXT NOT NULL,
                timestamp DATETIME DEFAULT (strftime('%Y-%m-%d %H:%M', 'now')),
                status TEXT DEFAULT 'pending',
                project_id TEXT,
                po_number TEXT,
                vendor_name TEXT,
                vendor_type TEXT,
                file_type TEXT,
                file_number TEXT,
                dropbox_share_link TEXT,
                file_stream_link TEXT,
                ocr_data TEXT,
                openai_data TEXT,
                UNIQUE(project_id, po_number, file_number, timestamp, event_type, path)
            )
        ''')

        # Create po_logs table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS po_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                file_name TEXT NOT NULL,
                project_id TEXT NOT NULL,
                dropbox_file_path TEXT NOT NULL,
                file_format TEXT NOT NULL CHECK(file_format IN ('txt', 'csv', 'tsv')),
                status TEXT DEFAULT 'unprocessed',
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        # Create indexes for faster lookups
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_file_id ON events (file_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_status ON events (status)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_po_logs_status ON po_logs (status)')

        conn.commit()
        logging.info("Database initialized successfully with updated schema.")
    except Exception as e:
        logging.error(f"Error initializing database: {e}", exc_info=True)
        raise  # Re-raise the exception to handle it in app.py
    finally:
        conn.close()



def add_event_to_db(
        file_id,
        file_name,
        path,
        old_path=None,
        event_type=None,
        project_id=None,
        po_number=None,
        vendor_name=None,
        vendor_type=None,
        file_type=None,
        file_number=None,
        dropbox_share_link=None,
        file_stream_link=None
):
    """
    Adds an event to the SQLite database while preventing duplicates and ensuring it's within the target Purchase Orders folder.
    Returns a tuple (event_id, is_duplicate).
    """
    db_path = get_db_path()
    with db_lock:
        conn = sqlite3.connect(db_path, check_same_thread=False)
        conn.row_factory = dict_factory
        cursor = conn.cursor()

        try:
            # Check if the path is within the TARGET_PURCHASE_ORDERS_FOLDER
            if TARGET_PURCHASE_ORDERS_FOLDER not in path:
                logging.info(f"Skipping event for '{file_name}' as it's not within '{TARGET_PURCHASE_ORDERS_FOLDER}' folder.")
                return None, False  # Do not add to the database

            # If not a duplicate, insert the new event
            cursor.execute('''
                INSERT INTO events (
                    file_id,
                    file_name,
                    path,
                    old_path,
                    event_type,
                    project_id,
                    po_number,
                    vendor_name,
                    vendor_type,
                    file_type,
                    file_number,
                    dropbox_share_link,
                    file_stream_link
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                file_id,
                file_name,
                path,
                old_path,
                event_type,
                project_id,
                po_number,
                vendor_name,
                vendor_type,
                file_type,
                file_number,
                dropbox_share_link,
                file_stream_link
            ))
            conn.commit()
            event_id = cursor.lastrowid
            logging.info(
                f"Event '{event_type}' for '{file_name}' at '{path}' added to the database with ID {event_id}.")
            return event_id, False  # Not a duplicate
        except sqlite3.IntegrityError as e:
            logging.error(f"IntegrityError while adding event to database: {e}")
            return None, False
        except sqlite3.Error as e:
            logging.error(f"Error adding event to database: {e}")
            return None, False
        finally:
            conn.close()



def fetch_pending_events():
    """
    Fetches all events from the database with a status of 'pending'.

    Returns:
        list of dicts: Each dict represents an event row.
    """
    db_path = get_db_path()
    with db_lock:
        conn = sqlite3.connect(db_path, check_same_thread=False)
        conn.row_factory = dict_factory
        cursor = conn.cursor()
        try:
            cursor.execute("SELECT * FROM events WHERE status = 'pending'")
            events = cursor.fetchall()
            logging.info(f"Fetched {len(events)} pending events.")
            return events
        except sqlite3.Error as e:
            logging.error(f"Database error while fetching pending events: {e}")
            return []
        finally:
            conn.close()


def update_event_status(event_id, new_status):
    """
    Updates the status of an event in the database.

    Args:
        event_id (int): The ID of the event to update.
        new_status (str): The new status ('processed', 'failed', etc.).
    """
    db_path = get_db_path()
    with db_lock:
        conn = sqlite3.connect(db_path, check_same_thread=False)
        conn.row_factory = dict_factory
        cursor = conn.cursor()
        try:
            cursor.execute("UPDATE events SET status = ? WHERE id = ?", (new_status, event_id))
            conn.commit()
            logging.info(f"Updated event ID {event_id} status to '{new_status}'.")
        except sqlite3.Error as e:
            logging.error(f"Database error while updating event ID {event_id}: {e}")
            conn.rollback()
        finally:
            conn.close()


def add_po_log(file_name, project_id, dropbox_file_path, file_format):
    """
    Adds a PO log entry to the po_logs table.

    Args:
        file_name (str): Name of the log file.
        project_id (str): Associated project ID.
        dropbox_file_path (str): Path to the log file in Dropbox.
        file_format (str): Format of the log file ('txt', 'csv', 'tsv').

    Returns:
        int: The ID of the inserted log entry, or None if insertion failed.
    """
    db_path = get_db_path()
    with db_lock:
        conn = sqlite3.connect(db_path, check_same_thread=False)
        cursor = conn.cursor()

        try:
            cursor.execute('''
                INSERT INTO po_logs (
                    file_name,
                    project_id,
                    dropbox_file_path,
                    file_format
                ) VALUES (?, ?, ?, ?)
            ''', (
                file_name,
                project_id,
                dropbox_file_path,
                file_format.lower()  # Ensure lowercase for consistency
            ))
            conn.commit()
            log_id = cursor.lastrowid
            logging.info(f"PO log '{file_name}' added with ID {log_id}.")
            return log_id
        except sqlite3.IntegrityError as e:
            logging.error(f"IntegrityError while adding PO log '{file_name}': {e}")
            return None
        except sqlite3.Error as e:
            logging.error(f"Error adding PO log '{file_name}': {e}")
            return None
        finally:
            conn.close()


def fetch_unprocessed_po_logs():
    """
    Fetches all PO logs from the database with a status of 'unprocessed'.

    Returns:
        list of dicts: Each dict represents a PO log row.
    """
    db_path = get_db_path()
    with db_lock:
        conn = sqlite3.connect(db_path, check_same_thread=False)
        conn.row_factory = dict_factory
        cursor = conn.cursor()
        try:
            cursor.execute("SELECT * FROM po_logs WHERE status = 'unprocessed'")
            logs = cursor.fetchall()
            logging.info(f"Fetched {len(logs)} unprocessed PO logs.")
            return logs
        except sqlite3.Error as e:
            logging.error(f"Database error while fetching unprocessed PO logs: {e}")
            return []
        finally:
            conn.close()


def update_po_log_status(log_id, new_status):
    """
    Updates the status of a PO log in the database.

    Args:
        log_id (int): The ID of the PO log to update.
        new_status (str): The new status ('processed', 'failed', etc.).
    """
    db_path = get_db_path()
    with db_lock:
        conn = sqlite3.connect(db_path, check_same_thread=False)
        cursor = conn.cursor()
        try:
            cursor.execute("UPDATE po_logs SET status = ? WHERE id = ?", (new_status, log_id))
            conn.commit()
            logging.info(f"Updated PO log ID {log_id} status to '{new_status}'.")
        except sqlite3.Error as e:
            logging.error(f"Database error while updating PO log ID {log_id}: {e}")
            conn.rollback()
        finally:
            conn.close()
