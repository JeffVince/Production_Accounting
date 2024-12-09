from db_util import get_db_session
from models import AicpCode

def update_aicp_codes(file_path):
    """
    Updates the `aicp_codes` table using data from the specified file.

    Args:
        file_path (str): Path to the file containing AICP codes and descriptions.
    """
    try:
        # Read the file and process each line
        with open(file_path, "r", encoding="utf-8") as file:
            data_lines = file.readlines()

        # Extract valid rows for AICP codes and descriptions
        aicp_data = []
        for line in data_lines:
            parts = line.strip().split("\t")
            if len(parts) >= 2 and parts[0].isdigit():
                aicp_data.append({
                    "aicp_code_surrogate_id": int(parts[0]),
                    "description": parts[1]
                })

        # Update the database
        with get_db_session() as session:
            for record in aicp_data:
                try:
                    # Check if the AICP code already exists
                    existing_code = session.query(AicpCode).filter_by(
                        aicp_code_surrogate_id=record["aicp_code_surrogate_id"]
                    ).one_or_none()

                    if existing_code:
                        # Update the description if it exists
                        existing_code.description = record["description"]
                        session.commit()
                        print(f"Updated AICP code: {record['aicp_code_surrogate_id']}")
                    else:
                        # Create a new record if it doesn't exist
                        new_code = AicpCode(**record)
                        session.add(new_code)
                        session.commit()
                        print(f"Added new AICP code: {record['aicp_code_surrogate_id']}")
                except Exception as e:
                    session.rollback()
                    print(f"Error processing code {record['aicp_code_surrogate_id']}: {e}")

    except Exception as e:
        print(f"Error processing file: {e}")