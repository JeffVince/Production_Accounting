from contextlib import contextmanager
from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker
from database.base import Base
import logging
logger = logging.getLogger('database_logger')
session_factory = None
local_session = None

def initialize_database(connection_string):
    """Initializes the database connection, session factory, and creates tables."""
    global session_factory
    engine = create_engine(connection_string, echo=False)
    session_factory = scoped_session(sessionmaker(bind=engine))
    Base.metadata.create_all(engine)
    logger.debug(f'Database connection string: {connection_string}')

@contextmanager
def get_db_session():
    global session_factory
    if session_factory is None:
        raise RuntimeError('Session factory not initialized.')
    session = session_factory
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.remove()

@contextmanager
def make_local_session():
    """
    Creates a new Session instance from the global 'session_factory' or engine.
    Ensures each call yields a *real* Session object with .query(), .rollback(), etc.
    """
    global session_factory
    if session_factory is None:
        raise RuntimeError('Session factory not initialized.')

    # If session_factory is a scoped_session, get the engine from .bind
    engine = session_factory.bind
    if engine is None:
        raise RuntimeError('Engine not found. The session_factory is not bound to an engine.')

    # Create a brand-new Session class NOT scoped
    SessionLocal = sessionmaker(bind=engine, autocommit=False, autoflush=False)

    # Instantiate the actual Session object (parentheses)
    session = SessionLocal()

    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        # For a normal Session, we call .close() to release the connection
        session.close()

def initialize_session_factory(session_factory_instance: scoped_session):
    global session_factory
    session_factory = session_factory_instance