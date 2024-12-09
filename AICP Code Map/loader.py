import csv

import json
import logging
import os
from datetime import datetime
from typing import Any, Optional, Dict

from dotenv import load_dotenv
from sqlalchemy.exc import IntegrityError, SQLAlchemyError

from utilities.config import Config
from database.db_util import get_db_session, initialize_database
from database.models import (
    PurchaseOrder,
    DetailItem,
    Contact,
    AicpCode, TaxAccount,
)




def main(csv_file_path):
    try:
        with get_db_session() as session:

            with open(csv_file_path, mode='r', encoding='utf-8-sig') as csvfile:
                reader = csv.DictReader(csvfile)

                # Iterate over each row in the CSV
                for row in reader:
                    aicp_number = row['AICP Number'].strip()
                    tax_code = row['Tax Code'].strip()
                    tax_description = row['Tax Description'].strip()
                    aicp_description = row['AICP Description'].strip()

                    # Skip rows without AICP Number or Tax Code
                    if not aicp_number or not tax_code:
                        print(f"Skipping incomplete row: {row}")
                        continue

                    # Fetch the TaxAccount based on Tax Code
                    tax_account = session.query(TaxAccount).filter_by(code=tax_code).one_or_none()

                    if not tax_account:
                        # If TaxAccount does not exist, you can choose to create it or skip
                        # Here, we'll create it
                        tax_account = TaxAccount(
                            code=tax_code,
                            description=tax_description
                        )
                        session.add(tax_account)
                        session.add(tax_account)
                        try:
                            session.commit()
                            print(f"Created new TaxAccount: {tax_code} - {tax_description}")
                        except IntegrityError:
                            session.rollback()
                            print(f"Failed to create Tax Account for Tax Code: {tax_code}")
                            continue

                    # Check if AicpCode already exists to avoid duplicates
                    existing_aicp = session.query(AicpCode).filter_by(code=aicp_number).one_or_none()
                    if existing_aicp:
                        print(f"AicpCode with code {aicp_number} already exists. Skipping.")
                        continue

                    # Create a new AicpCode instance
                    new_aicp = AicpCode(
                        code=aicp_number,
                        tax_code=tax_code,
                        tax_description=tax_description,
                        aicp_description=aicp_description
                    )

                    # Add to session
                    session.add(new_aicp)

                # Commit all changes
                session.commit()
                print("CSV data has been successfully imported into the database.")

    except FileNotFoundError:
        print(f"Error: The file '{csv_file_path}' was not found.")
    except Exception as e:
        session.rollback()
        print(f"An error occurred: {e}")
    finally:
        # Close the session
        session.close()


if __name__ == "__main__":

    # Initialize the database
    config = Config()
    db_settings = config.get_database_settings(local=True)
    initialize_database(db_settings['url'])

    csv_file_path = "codes.csv"
    main(csv_file_path)