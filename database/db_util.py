# database/db_util.py

from contextlib import contextmanager
from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker
from database.base import Base
# Global session factory variable
from logger import logger  # Remove if not used
import logging

logging.getLogger("sqlalchemy.engine.Engine").setLevel(logging.DEBUG)
# logging.getLogger("sqlalchemy.pool").setLevel(logging.DEBUG)
session_factory = None


def initialize_database(connection_string):
    """Initializes the database connection, session factory, and creates tables."""
    global session_factory
    engine = create_engine(connection_string, echo=False)
    session_factory = scoped_session(sessionmaker(bind=engine))
    Base.metadata.create_all(engine)  # Add this line to create tables
    logger.debug(f"Database connection string: {connection_string}")


@contextmanager
def get_db_session():
    global session_factory
    if session_factory is None:
        raise RuntimeError("Session factory not initialized.")
    session = session_factory  # Correctly create a session by calling the factory

    try:
        yield session
        session.commit()
    except Exception as e:
        session.rollback()
        raise e
    finally:
        session.close()


def initialize_session_factory(session_factory_instance: scoped_session):
    global session_factory
    session_factory = session_factory_instance
