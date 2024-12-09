import csv
import logging
import re
from collections import defaultdict
from datetime import datetime, timedelta

from utilities.singleton import SingletonMeta


class POLogProcessor(metaclass=SingletonMeta):
    # region CONFIG FLAGS
    TEST_MODE = False  # Set to True for extra debugging info

    # endregion

    def __init__(self):
        if not hasattr(self, '_initialized'):
            # Set up logging
            self.logger = logging.getLogger("app_logger")
            self.logger.info("PO Log Processor initialized (Custom Version)")
            self._initialized = True

    # region HELPER METHODS
    def _extract_project_id(self, file_path: str) -> str:
        """
        Extracts the project ID from the filename.
        Example: 'temp_2416.txt' => '2416'
        """
        match = re.search(r'(\d+)', file_path)
        if match:
            return match.group(1)
        self.logger.warning("No project ID found in filename, defaulting to '0000'")
        return '0000'

    def _map_payment_type(self, raw_type: str) -> str:
        """
        Maps raw 'Type' values to Payment Types: INV, CC, or PC
        CRD => CC
        PC  => PC
        Otherwise => INV
        """
        if raw_type == "CRD":
            return "CC"
        elif raw_type == "PC":
            return "PC"
        else:
            return "INV"

    def _determine_status(self, pay_id: str) -> str:
        """
        Determine the status of the PO based on the Pay ID.
        If 'Pay ID' contains 'PAID', status = 'Paid', else 'To Process'.
        """
        if "PAID" in (pay_id or "").upper():
            return "Paid"
        return "PENDING"

    def _parse_date(self, date_str: str) -> datetime:
        """
        Parse date strings like '11/6/24' into datetime objects.
        """
        try:
            return datetime.strptime(date_str.strip(), '%m/%d/%y')
        except ValueError as e:
            self.logger.warning(f"Date parsing error for '{date_str}': {e}, using today's date.")
            return datetime.today()

    def _clean_numeric(self, num_str: str) -> float:
        """
        Cleans a numeric string by removing commas and other non-numeric characters,
        then converts it to a float.
        """
        try:
            # Remove commas and any whitespace
            clean_str = num_str.replace(',', '').strip()
            return float(clean_str) if clean_str else 0.0
        except ValueError as e:
            self.logger.warning(f"Numeric parsing error for '{num_str}': {e}, defaulting to 0.0")
            return 0.0

    def _parse_factors(self, factors: str, subtotal: float):
        """
        Parse the 'Factors' column to extract quantity, rate, and OT.

        Expected patterns:
        - "1 x 2400"
        - "4 days x 850"
        - "2 x 300 + $32.14 OT"
        - "1 days x 300 + $0.01 Misc"

        We'll extract the first 'quantity x rate' pattern. If extra costs are present after a '+',
        we consider them OT costs (or any additional cost) and store them in OT.

        If no pattern found, fallback to quantity=1, rate=subtotal.
        """
        clean_factors = factors.replace(',', '')

        # Regex to match "4 days x 850" or "2 x 300"
        main_pattern = r'(\d+(?:\.\d+)?)\s*(?:days?)?\s*x\s*(\d+(?:\.\d+)?)'
        match = re.search(main_pattern, clean_factors, flags=re.IGNORECASE)

        quantity = 1.0
        rate = float(subtotal)
        ot = 0.0

        if match:
            try:
                quantity = float(match.group(1))
                rate = float(match.group(2))
            except Exception as e:
                self.logger.warning(f"Error parsing factors '{factors}': {e}")
                # fallback: quantity=1, rate=subtotal

        # Check for OT or extra costs
        # Pattern like: "+ $32.14 OT" or "+ 0.01 Misc"
        # We'll capture any amount after a plus sign
        plus_pattern = r'\+\s*\$?(\d+(?:\.\d+)?)(?:\s*\w+)?'
        plus_match = re.search(plus_pattern, clean_factors)
        if plus_match:
            try:
                ot = float(plus_match.group(1))
            except Exception as e:
                self.logger.warning(f"Error parsing OT from factors '{factors}': {e}")
                ot = 0.0

        return quantity, rate, ot

    # endregion

    # region MAIN PARSER METHOD
    def parse_showbiz_po_log(self, file_path: str):
        """
        Parse the custom PO log from the given file.

        Returns:
            main_items (list of dict)
            detail_items (list of dict)
            contacts (list of dict)
        """

        # region INITIALIZATION
        project_id = self._extract_project_id(file_path)
        main_items = []
        detail_items = []
        contacts = []

        # Track which POs we've encountered
        po_map = {}  # key: (project_id, PO) => index in main_items
        # Track if main item description is empty, so we can fill it from detail item if needed
        main_item_has_description = {}

        # Track auto-increment detail item IDs for items lacking an ID per PO
        auto_increment_counter = defaultdict(int)  # Starts at 0 for each new PO

        # Track assigned item_ids per PO to prevent auto-assigned IDs from clashing with manual IDs
        assigned_item_ids = defaultdict(set)

        # endregion

        # region FILE READ
        with open(file_path, 'r', newline='', encoding='utf-8') as txtfile:
            reader = csv.reader(txtfile, delimiter='\t')

            # Attempt to read a header row
            # The next line after that might be data or another header line. We'll detect headers.
            headers = next(reader, None)
            # If headers doesn't match expected, we just proceed because data might be multiple partial headers

            # The data structure is known:
            # Date(0) | Type(1) | Pay ID(2) | Account(3) | ID(4) | Vendor(5) | Description(6) | PO(7) | Factors(8) | Sub-Total $(9) | Fringes $(10)

            #region PROCESS LINES
            for row in reader:
                if not any(row):
                    continue  # skip empty lines

                # Skip if this line looks like a header (starts with "Date")
                if row[0].strip().upper() == "DATE":
                    continue

                # Safely extract columns
                try:
                    transaction_date_str = row[0].strip()
                    raw_type = row[1].strip()
                    pay_id = row[2].strip()
                    account = row[3].strip()
                    item_id = row[4].strip()
                    vendor = row[5].strip()
                    description = row[6].strip()
                    po_number = row[7].strip()
                    factors = row[8].strip()
                    subtotal_str = row[9].strip()
                    fringes_str = row[10].strip() if len(row) > 10 else ''
                except IndexError as e:
                    self.logger.warning(f"Malformed line skipped: {row}, error: {e}")
                    continue

                # If there's no PO, this is unexpected. Log and skip.
                if not po_number and raw_type != 'PC':
                    self.logger.warning(f"No PO number found in line: {row}")
                    continue

                # Clean and convert numerical fields
                subtotal = self._clean_numeric(subtotal_str)
                fringes = self._clean_numeric(fringes_str)

                # Determine payment type
                payment_type = self._map_payment_type(raw_type)

                # If PC, force PO number to "1"
                if payment_type == "PC":
                    po_number = "1"
                else:
                    po_number = po_number.lstrip("0")

                # Determine status
                status = self._determine_status(pay_id)

                # Contact parsing
                # For PC: contact = "PETTY CASH"
                if payment_type == "PC":
                    contact_name = "PETTY CASH"
                elif payment_type == "CC":
                    contact_name = f"Credit Card {pay_id}"
                else:
                    contact_name = vendor if vendor else "UNKNOWN CONTACT"

                #region MAIN ITEM LOGIC
                po_key = (project_id, po_number)
                if po_key not in po_map:
                    # If main item description is empty, it might be filled by detail items later.
                    main_item_desc = description if description else ''
                    main_item = {
                        'project_id': project_id,
                        'name': contact_name,
                        'PO': po_number,
                        'status': status,
                        'po_type': payment_type,  # INV, CC, or PC
                        'description': main_item_desc,
                        'amount': 0.0
                    }
                    po_map[po_key] = len(main_items)
                    main_items.append(main_item)
                    main_item_has_description[po_key] = bool(main_item_desc)

                    # Create a contact entry for this PO
                    contacts.append({
                        "name": contact_name,
                        "project_id": project_id,
                        "PO": po_number
                    })

                #endregion

                #region DETAIL ITEM PARSING


                transaction_date = self._parse_date(transaction_date_str)

                #region Due date logic 🧑‍🍳
                if payment_type == "INV":
                    due_date = transaction_date + timedelta(days=30)
                else:
                    due_date = transaction_date
                #endregion

                #region Parse factors for quantity, rate, and OT 🧳
                quantity, rate, ot = self._parse_factors(factors, subtotal)
                #endregion

                #region Detail item number processing  🥰
                if not item_id or not item_id.strip():
                    # Item has no item_id; assign a unique auto-incremented number
                    auto_increment_counter[po_number] += 1  # Increment the counter for this PO

                    # Ensure the number doesn't clash with manually assigned IDs
                    while auto_increment_counter[po_number] in assigned_item_ids[po_number]:
                        auto_increment_counter[po_number] += 1

                    detail_item_number = str(auto_increment_counter[po_number])
                else:
                    # Item has an item_id; strip leading zeros
                    stripped_id = item_id.lstrip('0') or '1'  # Ensure at least '1' if all zeros
                    detail_item_number = stripped_id
                    # Add to the set of assigned item_ids for this PO
                    try:
                        numeric_id = int(stripped_id)
                    except ValueError:
                        self.logger.warning(f"Non-numeric item_id '{item_id}' encountered. Defaulting to '1'.")
                        numeric_id = 1
                    assigned_item_ids[po_number].add(numeric_id)
                #endregion

                #region Vendor name parsing 🥹
                if payment_type == "PC":
                    # pay_id format: PC_2416_04 -> envelope is last part
                    parts = pay_id.split('_')
                    if len(parts) >= 3:
                        envelope_str = parts[-1].strip()
                        envelope_number = int(envelope_str.lstrip('0') or '0')
                    else:
                        envelope_number = 0
                    key_for_ids = (po_number, envelope_number)
                else:
                    envelope_number = 0
                    key_for_ids = (po_number, envelope_number)

                if payment_type == "PC":
                    # PC logic
                    if not item_id or not item_id.strip():
                        # No item_id given: auto-increment receipt number
                        auto_increment_counter[key_for_ids] += 1
                        # Ensure no conflict with assigned_item_ids if needed
                        # (If duplicates allowed for manual entries only, we must ensure unique auto-assigns)
                        detail_item_number = f"{envelope_number}.{auto_increment_counter[key_for_ids]}"
                        while detail_item_number in assigned_item_ids[key_for_ids]:
                            auto_increment_counter[key_for_ids] += 1
                            detail_item_number = f"{envelope_number}.{auto_increment_counter[key_for_ids]}"

                        # Add to assigned
                        assigned_item_ids[key_for_ids].add(detail_item_number)
                    else:
                        # item_id given (manual)
                        # Strip and convert to numeric
                        stripped_id = item_id.lstrip('0') or '1'
                        try:
                            numeric_id = int(stripped_id)
                        except ValueError:
                            self.logger.warning(f"Non-numeric item_id '{item_id}' encountered. Defaulting to '1'.")
                            numeric_id = 1
                        detail_item_number = f"{envelope_number}.{numeric_id}"
                        # Manual entries can have duplicates, so no uniqueness check
                        assigned_item_ids[key_for_ids].add(detail_item_number)
                else:
                    # Non-PC logic (unchanged except we use (po_number,0) as keys)
                    if not item_id or not item_id.strip():
                        # Auto-increment per PO
                        auto_increment_counter[key_for_ids] += 1
                        detail_item_number = str(auto_increment_counter[key_for_ids])
                        # Ensure uniqueness against assigned_item_ids for auto-assigned
                        while detail_item_number in assigned_item_ids[key_for_ids]:
                            auto_increment_counter[key_for_ids] += 1
                            detail_item_number = str(auto_increment_counter[key_for_ids])
                        assigned_item_ids[key_for_ids].add(detail_item_number)
                    else:
                        # item_id given (manual), duplicates allowed
                        stripped_id = item_id.lstrip('0') or '1'
                        try:
                            numeric_id = int(stripped_id)
                            detail_item_number = str(numeric_id)
                        except ValueError:
                            self.logger.warning(f"Non-numeric item_id '{item_id}' encountered. Defaulting to '1'.")
                            detail_item_number = '1'
                        assigned_item_ids[key_for_ids].add(detail_item_number)
                #endregion

                detail_item = {
                    'project_id': project_id,
                    'PO': po_number,
                    'vendor': vendor,
                    'date': transaction_date.strftime('%Y-%m-%d'),
                    'due date': due_date.strftime('%Y-%m-%d'),
                    'quantity': quantity,
                    'rate': rate,
                    'description': description,
                    'state': status,
                    'account': account,
                    'item_id': detail_item_number,
                    'payment_type': payment_type,
                    'total': subtotal,
                    'OT': ot,
                    'fringes': fringes
                }
                detail_items.append(detail_item)

                # If main item description was empty and this detail has a description,
                # fill in the main item description from the first encountered detail description.
                if not main_item_has_description[po_key] and description:
                    mi_index = po_map[po_key]
                    main_items[mi_index]['description'] = description
                    main_item_has_description[po_key] = True
                # endregion

            # endregion
        # endregion

        # region SUM AMOUNTS FOR MAIN ITEMS
        # For each main item, sum up detail items
        for m in main_items:
            rel_details = [d for d in detail_items if d['PO'] == m['PO'] and d['project_id'] == m['project_id']]
            total_amount = sum(d['total'] for d in rel_details)
            m['amount'] = total_amount
        # endregion

        # region LOGGING RESULTS
        self.logger.info(
            f"Parsed {len(main_items)} main items, {len(detail_items)} detail items, {len(contacts)} contacts for project {project_id}.")
        if self.TEST_MODE:
            self.logger.debug(f"Main Items: {main_items}")
            self.logger.debug(f"Detail Items: {detail_items}")
            self.logger.debug(f"Contacts: {contacts}")
        # endregion

        return main_items, detail_items, contacts
    # endregion

# Example usage:
po_log_processor = POLogProcessor()
#main_items, detail_items, contacts = po_log_processor.parse_custom_po_log('../temp_2416.txt')
#pass
