# database_util.py

import sqlite3
import logging
import threading

db_lock = threading.Lock()


def dict_factory(cursor, row):
    """
    Converts SQLite row to a dictionary.
    """
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d


def initialize_database():
    """
    Initializes the SQLite database with the necessary schema.
    """
    conn = sqlite3.connect('processed_files.db')
    conn.row_factory = dict_factory  # Set row factory to dictionary
    cursor = conn.cursor()

    # Create events table with additional columns and a unique constraint to prevent duplicates
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            file_id TEXT,
            file_name TEXT NOT NULL,
            path TEXT NOT NULL,
            old_path TEXT,
            event_type TEXT NOT NULL,
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
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
            UNIQUE(file_id, event_type, path, old_path)
        )
    ''')

    # Create indexes for faster lookups
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_file_id ON events (file_id)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_status ON events (status)')

    conn.commit()
    conn.close()
    logging.info("Database initialized successfully with updated schema.")


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
    Adds an event to the SQLite database while preventing duplicates.
    Returns the ID of the inserted or existing event.
    """
    with db_lock:
        conn = sqlite3.connect('processed_files.db', check_same_thread=False)
        conn.row_factory = dict_factory
        cursor = conn.cursor()

        try:
            # Corrected INSERT statement with 13 placeholders
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
            return event_id
        except sqlite3.IntegrityError:
            # This event is a duplicate and already exists in the database
            logging.info(f"Duplicate event '{event_type}' for '{file_name}' at '{path}' detected. Skipping insertion.")
            cursor.execute('SELECT id FROM events WHERE file_id=? AND event_type=? AND path=? AND old_path=?',
                           (file_id, event_type, path, old_path))
            result = cursor.fetchone()
            event_id = result['id'] if result else None
            return event_id
        except sqlite3.Error as e:
            logging.error(f"Error adding event to database: {e}")
            return None
        finally:
            conn.close()


def fetch_pending_events():
    """
    Fetches all events from the database with a status of 'pending'.

    Returns:
        list of dicts: Each dict represents an event row.
    """
    with db_lock:
        conn = sqlite3.connect('processed_files.db', check_same_thread=False)
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
    with db_lock:
        conn = sqlite3.connect('processed_files.db', check_same_thread=False)
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