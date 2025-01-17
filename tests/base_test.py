import unittest
from sqlalchemy import create_engine, inspect, event
from sqlalchemy.orm import sessionmaker, scoped_session
from database.base import Base
from database.db_util import initialize_session_factory
from contextlib import contextmanager
import sqlite3

class BaseTestCase(unittest.TestCase):
    """
    Base test case for setting up an in-memory SQLite database.
    Each test runs against a fresh database.
    """

    def setUp(self):
        """
        Set up a fresh in-memory SQLite database and bind it to the Base metadata before each test.
        """
        self.engine = create_engine('sqlite:///:memory:', echo=False)

        @event.listens_for(self.engine, 'connect')
        def set_sqlite_pragma(dbapi_connection, connection_record):
            if isinstance(dbapi_connection, sqlite3.Connection):
                cursor = dbapi_connection.cursor()
                cursor.execute('PRAGMA foreign_keys=ON;')
                cursor.close()
        self.Session = scoped_session(sessionmaker(bind=self.engine, expire_on_commit=False))
        initialize_session_factory(self.Session)
        Base.metadata.create_all(self.engine)
        inspector = inspect(self.engine)
        tables = inspector.get_table_names()
        print('Tables in the database:', tables)
        self.session = self.Session()

    def tearDown(self):
        """
        Drop all tables and dispose of the engine after each test.
        """
        self.session.close()
        Base.metadata.drop_all(self.engine)
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