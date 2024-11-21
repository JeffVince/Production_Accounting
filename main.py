# main.py

from database.db_util import initialize_database

def main():
    initialize_database()
    print("Database initialized successfully.")

if __name__ == "__main__":
    main()