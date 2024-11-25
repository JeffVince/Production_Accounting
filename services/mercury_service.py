# services/mercury_service.py

import requests
from database.db_util import get_db_session

from utilities.config import Config
import logging

logger = logging.getLogger(__name__)

