# services/validation_service.py

from database.models import PurchaseOrder, POState
from database.db_util import get_db_session
import logging

logger = logging.getLogger(__name__)

