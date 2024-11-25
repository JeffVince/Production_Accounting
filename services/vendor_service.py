# services/vendor_service.py

from database.models import Contact
from database.db_util import get_db_session
import logging

logger = logging.getLogger(__name__)

