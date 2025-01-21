import pandas as pd
from sqlalchemy import create_engine, inspect
import getpass
import os
import sys

def get_db_credentials():
    """
    Prompt the user for database credentials.
    """
    print("Please enter your MySQL database credentials.")
    host = input("Host (e.g., localhost): ").strip()
    port = input("Port (default 3306): ").strip() or "3306"
    user = input("Username: ").strip()
    password = getpass.getpass("Password: ")
    database = input("Database name (e.g., virtual_pm): ").strip()
    return host, port, user, password, database

def create_output_directory(directory_name="mysql_exports"):
    """
    Create an output directory for CSV files.
    """
    if not os.path.exists(directory_name):
        os.makedirs(directory_name)
        print(f"Created directory '{directory_name}' for CSV exports.")
    else:
        print(f"Using existing directory '{directory_name}' for CSV exports.")
    return directory_name

def get_all_tables(engine, database):
    """
    Retrieve all table names from the specified database.
    """
    inspector = inspect(engine)
    tables = inspector.get_table_names(schema=database)
    return tables

def export_table_to_csv(engine, table_name, output_dir):
    """
    Export a single table to a CSV file.
    """
    try:
        print(f"Exporting table '{table_name}'...")
        query = f"SELECT * FROM `{table_name}`;"
        df = pd.read_sql(query, engine)
        # Replace any characters in table_name that are invalid for filenames
        safe_table_name = "".join([c if c.isalnum() or c in (' ', '_', '-') else "_" for c in table_name])
        csv_file_path = os.path.join(output_dir, f"{safe_table_name}.csv")
        df.to_csv(csv_file_path, index=False, encoding='utf-8-sig')
        print(f"Successfully exported '{table_name}' to '{csv_file_path}'.")
    except Exception as e:
        print(f"Error exporting table '{table_name}': {e}")

def main():
    # Step 1: Get database credentials
    host, port, user, password, database = get_db_credentials()

    # Step 2: Create database connection
    try:
        connection_string = f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}"
        engine = create_engine(connection_string)
        # Test the connection
        with engine.connect() as connection:
            print(f"Successfully connected to the database '{database}' on {host}:{port} as user '{user}'.")
    except Exception as e:
        print(f"Failed to connect to the database: {e}")
        sys.exit(1)

    # Step 3: Create output directory
    output_dir = create_output_directory()

    # Step 4: Get all tables
    try:
        tables = get_all_tables(engine, database)
        if not tables:
            print(f"No tables found in the database '{database}'. Exiting.")
            sys.exit(0)
        else:
            print(f"Found {len(tables)} tables in the database '{database}'.")
    except Exception as e:
        print(f"Error retrieving table names: {e}")
        sys.exit(1)

    # Step 5: Export each table to CSV
    for table in tables:
        export_table_to_csv(engine, table, output_dir)

    print("\nAll tables have been exported successfully.")

if __name__ == "__main__":
    main()