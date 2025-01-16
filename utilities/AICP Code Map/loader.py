import csv
import logging
from sqlalchemy.exc import IntegrityError, SQLAlchemyError

from utilities.config import Config
from database.db_util import get_db_session, initialize_database
from database.models import AccountCode, TaxAccount


def main(csv_file_path: str):
    """
    Reads rows from `codes.csv` and inserts them into the `tax_account` and `account_code` tables.
    """
    try:
        with get_db_session() as session:
            with open(csv_file_path, mode='r', encoding='utf-8-sig') as csvfile:
                reader = csv.DictReader(csvfile)

                for row in reader:
                    account_number      = row['coder'].strip()
                    tax_code_value   = row['Tax Code'].strip()       # e.g. '5300'
                    tax_description  = row['Tax Description'].strip() # e.g. 'Subcontractor'
                    account_description = row['Account Description'].strip()

                    # Skip if Account Number or Tax Code is missing
                    if not account_number or not tax_code_value:
                        logging.warning(f"Skipping incomplete row: {row}")
                        continue

                    # 1) Look up (or create) the TaxAccount first
                    tax_account = session.query(TaxAccount).filter_by(tax_code=tax_code_value).one_or_none()
                    if not tax_account:
                        tax_account = TaxAccount(
                            tax_code=tax_code_value,
                            description=tax_description
                        )
                        session.add(tax_account)
                        try:
                            session.commit()
                            logging.info(f"Created TaxAccount: {tax_code_value} - {tax_description}")
                        except IntegrityError:
                            session.rollback()
                            logging.error(f"Failed to create TaxAccount for Tax Code: {tax_code_value}")
                            # If we can't create the tax account, skip creating the Account code
                            continue
                    else:
                        # Update description if desired (optional)
                        if tax_description and (tax_account.description != tax_description):
                            tax_account.description = tax_description
                            session.commit()

                    # 2) Check if Account code already exists
                    existing_account = session.query(AccountCode).filter_by(account_code=account_number).one_or_none()
                    if existing_account:
                        logging.info(f"AccountCode {account_number} already exists. Skipping.")
                        continue

                    # 3) Create a new AccountCode with the correct columns
                    new_account = AccountCode(
                        account_code=account_number,
                        tax_id=tax_account.id,        # link to TaxAccount by ID
                        account_description=account_description
                    )

                    # Add new AccountCode to session
                    session.add(new_account)

                # Commit once all rows have been processed
                session.commit()
                logging.info("CSV data has been successfully imported into the database.")

    except FileNotFoundError:
        logging.error(f"Error: The file '{csv_file_path}' was not found.")
    except SQLAlchemyError as e:
        # Roll back the transaction on any SQLAlchemy error
        session.rollback()
        logging.error(f"SQLAlchemy error occurred: {e}")
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")
    finally:
        session.close()


if __name__ == "__main__":
    # Initialize the database
    config = Config()
    db_settings = config.get_database_settings(local=True)
    initialize_database(db_settings['url'])

    csv_file_path = "codes.csv"  # Make sure codes.csv is in the same directory
    main(csv_file_path)