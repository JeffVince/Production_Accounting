# database_util.py

import sqlite3
import logging
import threading
import os  # Import os module to handle file paths

db_lock = threading.Lock()


def dict_factory(cursor, row):
    """
    Converts SQLite row to a dictionary.
    """
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d


def get_db_path():
    """
    Computes the absolute path to the database file relative to this script.
    """
    # Get the absolute path to the directory containing this script
    script_dir = os.path.dirname(os.path.abspath(__file__))
    # Construct the path to 'processed_files.db' one level up from script_dir
    db_path = os.path.join(script_dir, '..', 'processed_files.db')
    # Ensure the path is absolute
    db_path = os.path.abspath(db_path)
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
                UNIQUE(project_id, po_number, file_number, file_type)
            )
        ''')

        # Create indexes for faster lookups
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_file_id ON events (file_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_status ON events (status)')

        conn.commit()
        logging.info("Database initialized successfully with updated schema.")
    except Exception as e:
        logging.error(f"Error initializing database: {e}", exc_info=True)
        raise  # Re-raise the exception to handle it in main.py
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
    Adds an event to the SQLite database while preventing duplicates.
    Returns a tuple (event_id, is_duplicate).
    """
    db_path = get_db_path()
    with db_lock:
        conn = sqlite3.connect(db_path, check_same_thread=False)
        conn.row_factory = dict_factory
        cursor = conn.cursor()

        try:
            # Check for duplicate based on project_id, po_number, file_number, file_type
            if project_id and po_number and file_number and file_type:
                cursor.execute('''
                    SELECT id, status FROM events
                    WHERE project_id = ?
                      AND po_number = ?
                      AND file_number = ?
                      AND file_type = ?
                ''', (project_id, po_number, file_number, file_type))
                result = cursor.fetchone()

                if result:
                    event_id = result['id']
                    current_status = result['status']
                    if current_status != 'duplicate':
                        cursor.execute('''
                            UPDATE events
                            SET status = 'duplicate'
                            WHERE id = ?
                        ''', (event_id,))
                        conn.commit()
                        logging.info(f"Event ID {event_id} marked as 'duplicate'.")
                    else:
                        logging.info(f"Event ID {event_id} is already marked as 'duplicate'.")
                    return event_id, True  # Is a duplicate

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