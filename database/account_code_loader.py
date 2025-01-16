from db_util import get_db_session
from models import AccountCode

def update_account_codes(file_path):
    """
    Updates the `account_codes` table using data from the specified file.

    Args:
        file_path (str): Path to the file containing ACCOUNT codes and descriptions.
    """
    try:
        # Read the file and process each line
        with open(file_path, "r", encoding="utf-8") as file:
            data_lines = file.readlines()

        # Extract valid rows for Account codes and descriptions
        account_data = []
        for line in data_lines:
            parts = line.strip().split("\t")
            if len(parts) >= 2 and parts[0].isdigit():
                account_data.append({
                    "account_code_surrogate_id": int(parts[0]),
                    "description": parts[1]
                })

        # Update the database
        with get_db_session() as session:
            for record in account_data:
                try:
                    # Check if the Account code already exists
                    existing_code = session.query(AccountCode).filter_by(
                        account_code_surrogate_id=record["account_code_surrogate_id"]
                    ).one_or_none()

                    if existing_code:
                        # Update the description if it exists
                        existing_code.description = record["description"]
                        session.commit()
                        print(f"Updated Account code: {record['account_code_surrogate_id']}")
                    else:
                        # Create a new record if it doesn't exist
                        new_code = AccountCode(**record)
                        session.add(new_code)
                        session.commit()
                        print(f"Added new Account code: {record['account_code_surrogate_id']}")
                except Exception as e:
                    session.rollback()
                    print(f"Error processing code {record['account_code_surrogate_id']}: {e}")

    except Exception as e:
        print(f"Error processing file: {e}")