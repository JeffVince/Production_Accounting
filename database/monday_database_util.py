# database/monday_database_util.py

import sqlite3
import logging
import os
from database.db_util import get_db_session
from database.models import MainItem, SubItem

# Define the database directory
BASE_DIRECTORY = os.path.dirname(os.path.abspath(__file__))
DB_DIRECTORY = os.path.join(BASE_DIRECTORY, '..', 'database/database/')  # Adjust path as needed
DB_PATH = os.path.join(DB_DIRECTORY, "purchase_orders.db")

# Ensure the database folder exists
os.makedirs(DB_DIRECTORY, exist_ok=True)

# Configure logging
logging.basicConfig(level=logging.DEBUG)


def get_connection():
    """
    Returns a SQLite connection with foreign key constraints enabled.
    """
    logging.debug(f"Connecting to database at {DB_PATH}")
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA foreign_keys = ON;")  # Enable foreign key constraints
    return conn


def initialize_database():
    """
    Initializes the database by creating the required tables.
    """
    conn = get_connection()
    cursor = conn.cursor()
    try:
        logging.debug("Creating main_items table...")
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS main_items (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                item_id TEXT UNIQUE,
                name TEXT,
                project_id TEXT,
                numbers TEXT,
                description TEXT,
                tax_form TEXT,
                folder TEXT,
                amount TEXT,
                po_status TEXT,
                producer_pm TEXT,
                updated_date TEXT
            )
        ''')
        logging.debug("Creating sub_items table...")
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS sub_items (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                subitem_id TEXT UNIQUE,
                main_item_id TEXT,
                status TEXT,
                invoice_number TEXT,
                description TEXT,
                amount REAL,
                quantity REAL,
                account_number TEXT,
                invoice_date TEXT,
                link TEXT,
                due_date TEXT,
                creation_log TEXT,
                FOREIGN KEY (main_item_id) REFERENCES main_items (item_id) ON DELETE CASCADE
            )
        ''')
        conn.commit()
        logging.info(f"Database initialized successfully at {DB_PATH}.")
    except Exception as e:
        logging.error(f"Error initializing database: {e}")
        raise e
    finally:
        conn.close()


def insert_main_item(item_data):
    with get_db_session() as session:
        existing_item = session.query(MainItem).filter_by(item_id=item_data['item_id']).first()
        if existing_item:
            for key, value in item_data.items():
                setattr(existing_item, key, value)
        else:
            main_item = MainItem(**item_data)
            session.add(main_item)
        session.commit()


def insert_subitem(subitem_data):
    with get_db_session() as session:
        existing_subitem = session.query(SubItem).filter_by(subitem_id=subitem_data['subitem_id']).first()
        if existing_subitem:
            for key, value in subitem_data.items():
                setattr(existing_subitem, key, value)
        else:
            subitem = SubItem(**subitem_data)
            session.add(subitem)
        session.commit()


def fetch_all_main_items():
    with get_db_session() as session:
        return session.query(MainItem).all()


def fetch_subitems_for_main_item(main_item_id):
    with get_db_session() as session:
        return session.query(SubItem).filter_by(main_item_id=main_item_id).all()


def fetch_main_items_by_status(status):
    with get_db_session() as session:
        return session.query(MainItem).filter_by(po_status=status).all()


def fetch_subitems_by_main_item_and_status(main_item_id, status):
    with get_db_session() as session:
        return session.query(SubItem).filter_by(main_item_id=main_item_id, status=status).all()