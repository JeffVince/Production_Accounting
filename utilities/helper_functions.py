import datetime


def parse_naming_convention(name_string: str) -> dict:
    """
    Parses a naming convention string and extracts structured data.
    Example format: "PO123-VendorName-InvoiceDate"
    """
    try:
        parts = name_string.split("-")
        if len(parts) < 3:
            raise ValueError("Naming convention is invalid. Expected at least three parts.")

        return {
            "po_number": parts[0],
            "vendor_name": parts[1],
            "invoice_date": datetime.datetime.strptime(parts[2], "%Y%m%d").date(),
        }
    except Exception as e:
        raise ValueError(f"Failed to parse naming convention: {e}")


def validate_state_transition(current_state: str, new_state: str, allowed_transitions: dict) -> bool:
    """
    Validates if the transition from current_state to new_state is allowed.
    :param current_state: The current state of the object.
    :param new_state: The desired next state.
    :param allowed_transitions: A dictionary defining allowed transitions.
    :return: True if the transition is valid, otherwise False.
    """
    if current_state not in allowed_transitions:
        raise ValueError(f"Invalid current state: {current_state}")

    valid_transitions = allowed_transitions[current_state]
    return new_state in valid_transitions


def format_date(date_string: str, input_format: str = "%Y-%m-%d", output_format: str = "%d-%b-%Y") -> str:
    """
    Converts a date string from one format to another.
    Example: "2024-11-18" -> "18-Nov-2024"
    """
    try:
        date_obj = datetime.datetime.strptime(date_string, input_format)
        return date_obj.strftime(output_format)
    except Exception as e:
        raise ValueError(f"Error formatting date: {e}")


def calculate_total_amounts(data: list[dict], key: str) -> float:
    """
    Calculates the total amount from a list of dictionaries.
    :param data: List of dictionaries containing numerical values.
    :param key: The key whose values need to be summed up.
    :return: Total sum of the values.
    """
    try:
        return sum(item[key] for item in data if key in item and isinstance(item[key], (int, float)))
    except Exception as e:
        raise ValueError(f"Error calculating totals: {e}")


def extract_filename_extension(file_name: str) -> str:
    """
    Extracts and returns the extension of a file name.
    Example: "document.pdf" -> "pdf"
    """
    if "." not in file_name:
        raise ValueError("File name does not have an extension.")
    return file_name.split(".")[-1].lower()


def sanitize_input(input_str: str) -> str:
    """
    Removes unwanted characters from a string to make it safe for further processing.
    Example: "   Invoice 123!@#   " -> "Invoice 123"
    """
    return "".join(c for c in input_str if c.isalnum() or c.isspace()).strip()


def list_to_dict(data_list):
    """
    Converts a list of dictionaries to a dictionary using the 'id' field as the key.
    Each dictionary in the list must have an 'id', 'value', and 'text' field.

    :param data_list: List of dictionaries with 'id', 'value', and 'text' fields
    :return: Dictionary with 'id' as the key and {'value', 'text'} as the value
    """
    return {item['id']: {'value': item['value'], 'text': item['text']} for item in data_list}
