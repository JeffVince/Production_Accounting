# services/xero_service.py

from xero import Xero
from xero.auth import OAuth2Credentials
from database.db_util import get_db_session
from database.xero_repository import (
    add_or_update_bill,
    update_bill_status,
    add_or_update_spend_money_transaction,
)
from utilities.config import Config
import logging

logger = logging.getLogger(__name__)

