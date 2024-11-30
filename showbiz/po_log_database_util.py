import json
import logging
import os
from datetime import datetime
from decimal import Decimal
from typing import Any, Optional, Dict, List

from dotenv import load_dotenv
from sqlalchemy.exc import IntegrityError, SQLAlchemyError
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine

from database.db_util import get_db_session
from database.models import (
    PurchaseOrder,
    DetailItem,
    Contact,
    AicpCode,
    TaxAccount,
    Project,
)

import po_log_processor


class PoLogDatabaseUtil:
    def __init__(self):
        # Set up logging
        self.logger = logging.getLogger(self.__class__.__name__)
        # Load environment variables
        load_dotenv()

    # ---------------------- PREPROCESSING ----------------------
    def get_contact_surrogate_ids(self, contacts_list):
        '''
        Looks for matching contacts and creates new ones if necessary
        :param contacts_list:
        :return: contacts_list with contact surrogate ID attached
        '''
        new_contact_list = []
        for contact in contacts_list:
            try:
                with get_db_session() as session:
                    # check if contact exists
                    db_contact = session.query(Contact).filter_by(name=contact.name).one_or_none()
                    if contact:
                        new_contact_list.append({
                            "name": contact.name,
                            "PO": contact.PO,
                            "contact_surrogate_id": db_contact.contact_surrogate_id
                        })
            except Exception as e:
                self.logger.error(f"Error looking for contact: {e}")

        print(new_contact_list)
        return new_contact_list

    # --------------------- CREATE OR UPDATE METHODS ---------------------

    # ---------------------- MAIN EXECUTION ----------------------
