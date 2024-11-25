# database/mercury_repository.py

from sqlalchemy.exc import SQLAlchemyError
from database.db_util import get_db_session
import logging

logger = logging.getLogger(__name__)
