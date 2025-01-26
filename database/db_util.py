# ===============================
# 1) database/db_util.py
# ===============================
from contextlib import contextmanager
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from database.base import Base
import logging

logger = logging.getLogger('database_logger')

engine = None
SessionLocal = None

def initialize_database(connection_string):
    """
    Initializes the database engine, sessionmaker, and creates tables.
    Implements:
      - pool_pre_ping=True to avoid stale connections
      - pool_recycle=3600 to recycle connections older than 1h
    """
    global engine, SessionLocal
    engine = create_engine(
        connection_string,
        echo=False,
        pool_pre_ping=True,
        pool_recycle=3600
    )
    SessionLocal = sessionmaker(bind=engine, autocommit=False, autoflush=False)
    Base.metadata.create_all(engine)
    logger.debug(f"Database initialized with connection string: {connection_string}")

@contextmanager
def get_db_session():
    """
    A single, unified context manager for obtaining and releasing a DB session.
    """
    global SessionLocal
    if not SessionLocal:
        raise RuntimeError("SessionLocal not initialized. Call initialize_database first.")

    session = SessionLocal()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()