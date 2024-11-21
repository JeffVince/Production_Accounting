# tests/base_test.py

import unittest
from sqlalchemy import create_engine, inspect
from sqlalchemy.orm import sessionmaker, scoped_session
from database.models import Base
from database.db_util import get_db_session, initialize_session_factory
from contextlib import contextmanager


class BaseTestCase(unittest.TestCase):
    """
    Base test case for setting up an in-memory SQLite database.
    Each test runs against a fresh database.
    """

    def setUp(self):
        """
        Set up a fresh in-memory SQLite database and bind it to the Base metadata before each test.
        """
        # Create a new in-memory SQLite engine
        self.engine = create_engine('sqlite:///:memory:', echo=False)

        # Create a new scoped session with expire_on_commit=False to prevent DetachedInstanceError
        self.Session = scoped_session(sessionmaker(bind=self.engine, expire_on_commit=False))

        # Initialize the session factory used by db_util.py
        initialize_session_factory(self.Session)

        # Create all tables in the in-memory database
        Base.metadata.create_all(self.engine)

        # Verify table creation (optional, for debugging)
        inspector = inspect(self.engine)
        tables = inspector.get_table_names()
        print("Tables in the database:", tables)

        # Create a new session for the test
        self.session = self.Session()

    def tearDown(self):
        """
        Drop all tables and dispose of the engine after each test.
        """
        # Close the session
        self.session.close()

        # Drop all tables
        Base.metadata.drop_all(self.engine)

        # Dispose of the engine
        self.engine.dispose()

    @contextmanager
    def session_scope(self):
        """
        Provide a transactional scope around a series of operations.
        """
        session = self.Session()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()