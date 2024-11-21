# database/db_util.py

from contextlib import contextmanager

from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker

# Global session factory variable
from logger import logger

session_factory = None


def initialize_database(connection_string):
    """Initializes the database connection and session factory."""
    global session_factory
    engine = create_engine(connection_string, echo=True)  # Use echo=True for debugging
    session_factory = scoped_session(sessionmaker(bind=engine))


@contextmanager
def get_db_session():
    global session_factory
    if session_factory is None:
        raise RuntimeError("Session factory not initialized.")
    session = session_factory()  # Correctly create a session
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


