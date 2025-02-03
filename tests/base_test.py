# base_test.py
import unittest
import os
from sqlalchemy import inspect
from contextlib import contextmanager
import pymysql  # ensure the MySQL driver is installed
from database.base import Base
from database.db_util import initialize_database, get_db_session, engine


class BaseTestCase(unittest.TestCase):
    """
    Base test case for setting up a MySQL test database.
    Each test runs against a fresh database.
    """

    def setUp(self):
        """
        Set up a fresh MySQL test database.
        The connection string is taken from the environment variable
        TEST_DB_CONNECTION_STRING (or defaults to a local test database).
        """
        # Use a MySQL test connection string; adjust as needed.
        connection_string = os.getenv(
            "TEST_DB_CONNECTION_STRING",
            "mysql+pymysql://root:password@127.0.0.1/test_db"
        )
        # Initialize the database engine, sessionmaker, and create tables.
        initialize_database(connection_string)
        # Create all tables (this may already have been done in initialize_database).
        Base.metadata.create_all(engine)

        # Optionally, print table names (helpful for debugging in PyCharm).
        inspector = inspect(engine)
        tables = inspector.get_table_names()
        print("Tables in the test database:", tables)

        # Start a DB session using the unified context manager.
        self.db_context = get_db_session()
        self.session = self.db_context.__enter__()

    def tearDown(self):
        """
        Drop all tables and dispose of the engine after each test.
        """
        # Exit the session context.
        self.db_context.__exit__(None, None, None)
        # Drop all tables.
        Base.metadata.drop_all(engine)
        engine.dispose()

    @contextmanager
    def session_scope(self):
        """
        Provide a transactional scope around a series of operations.
        """
        with get_db_session() as session:
            yield session


if __name__ == '__main__':
    unittest.main()