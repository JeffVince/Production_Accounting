# database/db_util.py

from contextlib import contextmanager
from sqlalchemy.orm import scoped_session

# Global session factory variable
session_factory = None

@contextmanager
def get_db_session():
    if session_factory is None:
        raise RuntimeError("Session factory not initialized.")
    session = session_factory()
    try:
        yield session
        session.commit()
    except:
        session.rollback()
        raise
    finally:
        session.close()

def initialize_session_factory(session_factory_instance: scoped_session):
    global session_factory
    session_factory = session_factory_instance