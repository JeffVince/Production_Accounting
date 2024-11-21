# dropbox_database_util.py

import sqlite3
import logging
import threading
import os

# Thread safety for database operations
db_lock = threading.Lock()

# Environment variables and defaults
TARGET_PURCHASE_ORDERS_FOLDER = os.getenv('TARGET_PURCHASE_ORDERS_FOLDER', '1. Purchase Orders')


def dict_factory(cursor, row):
    """
    Converts SQLite row to a dictionary.
    """
    return {col[0]: row[idx] for idx, col in enumerate(cursor.description)}


def get_db_path():
    """
    Generates the database path dynamically, ensuring the directory exists.
    """
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.abspath(os.path.join(script_dir, '..'))
    db_dir = os.path.join(project_root, 'database/database')
    db_path = os.path.join(db_dir, 'processed_files.db')

    # Ensure database directory exists
    os.makedirs(db_dir, exist_ok=True)
    return db_path


def initialize_database():
    """
    Initializes the SQLite database schema and ensures required tables are created.
    """
    db_path = get_db_path()
    logging.info(f"Initializing database at {db_path}")

    with sqlite3.connect(db_path) as conn:
        conn.row_factory = dict_factory
        cursor = conn.cursor()

        try:
            # Create events table
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

            # Create indexes for optimization
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_file_id ON events (file_id)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_status ON events (status)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_po_logs_status ON po_logs (status)')

            logging.info("Database schema initialized successfully.")
        except sqlite3.Error as e:
            logging.error(f"Error initializing database: {e}", exc_info=True)
            raise


def add_event_to_db(**event_data):
    """
    Adds an event to the events table, filtering by folder constraints and avoiding duplicates.

    Returns:
        tuple: (event_id, is_duplicate)
    """
    db_path = get_db_path()

    with db_lock, sqlite3.connect(db_path, check_same_thread=False) as conn:
        conn.row_factory = dict_factory
        cursor = conn.cursor()

        try:
            if TARGET_PURCHASE_ORDERS_FOLDER not in event_data.get('path', ''):
                logging.info(f"Skipping event outside target folder: {event_data['path']}")
                return None, False

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
                ) VALUES (
                    :file_id,
                    :file_name,
                    :path,
                    :old_path,
                    :event_type,
                    :project_id,
                    :po_number,
                    :vendor_name,
                    :vendor_type,
                    :file_type,
                    :file_number,
                    :dropbox_share_link,
                    :file_stream_link
                )
            ''', event_data)
            conn.commit()
            return cursor.lastrowid, False  # New event added successfully
        except sqlite3.IntegrityError:
            logging.warning(f"Duplicate event detected: {event_data}")
            return None, True
        except sqlite3.Error as e:
            logging.error(f"Error adding event to database: {e}", exc_info=True)
            return None, False


def fetch_pending_events():
    """
    Fetches all events with status 'pending'.
    """
    db_path = get_db_path()

    with db_lock, sqlite3.connect(db_path, check_same_thread=False) as conn:
        conn.row_factory = dict_factory
        cursor = conn.cursor()

        try:
            cursor.execute("SELECT * FROM events WHERE status = 'pending'")
            events = cursor.fetchall()
            logging.info(f"Fetched {len(events)} pending events.")
            return events
        except sqlite3.Error as e:
            logging.error(f"Error fetching pending events: {e}", exc_info=True)
            return []


def update_event_status(event_id, new_status):
    """
    Updates the status of a specific event.
    """
    db_path = get_db_path()

    with db_lock, sqlite3.connect(db_path, check_same_thread=False) as conn:
        cursor = conn.cursor()

        try:
            cursor.execute("UPDATE events SET status = ? WHERE id = ?", (new_status, event_id))
            conn.commit()
            logging.info(f"Event {event_id} status updated to '{new_status}'.")
        except sqlite3.Error as e:
            logging.error(f"Error updating event {event_id}: {e}", exc_info=True)


def add_po_log(**po_log_data):
    """
    Adds a PO log entry to the po_logs table.
    """
    db_path = get_db_path()

    with db_lock, sqlite3.connect(db_path, check_same_thread=False) as conn:
        cursor = conn.cursor()

        try:
            cursor.execute('''
                INSERT INTO po_logs (
                    file_name,
                    project_id,
                    dropbox_file_path,
                    file_format
                ) VALUES (
                    :file_name,
                    :project_id,
                    :dropbox_file_path,
                    :file_format
                )
            ''', po_log_data)
            conn.commit()
            return cursor.lastrowid
        except sqlite3.IntegrityError:
            logging.warning(f"Duplicate PO log detected: {po_log_data}")
            return None
        except sqlite3.Error as e:
            logging.error(f"Error adding PO log: {e}", exc_info=True)
            return None


def fetch_unprocessed_po_logs():
    """
    Fetches all unprocessed PO logs.
    """
    db_path = get_db_path()

    with db_lock, sqlite3.connect(db_path, check_same_thread=False) as conn:
        conn.row_factory = dict_factory
        cursor = conn.cursor()

        try:
            cursor.execute("SELECT * FROM po_logs WHERE status = 'unprocessed'")
            logs = cursor.fetchall()
            logging.info(f"Fetched {len(logs)} unprocessed PO logs.")
            return logs
        except sqlite3.Error as e:
            logging.error(f"Error fetching unprocessed PO logs: {e}", exc_info=True)
            return []


def update_po_log_status(log_id, new_status):
    """
    Updates the status of a specific PO log.
    """
    db_path = get_db_path()

    with db_lock, sqlite3.connect(db_path, check_same_thread=False) as conn:
        cursor = conn.cursor()

        try:
            cursor.execute("UPDATE po_logs SET status = ? WHERE id = ?", (new_status, log_id))
            conn.commit()
            logging.info(f"PO log {log_id} status updated to '{new_status}'.")
        except sqlite3.Error as e:
            logging.error(f"Error updating PO log {log_id}: {e}", exc_info=True)