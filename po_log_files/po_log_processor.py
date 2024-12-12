import csv
import logging
import os
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
        Example: 'PO_LOG_2416-2024-12-12_01-30-30.txt' => '2416'

        :param file_path: Full path to the file.
        :return: Extracted Project ID as a string. Returns '0000' if not found.
        """
        # Extract the filename from the full file path
        filename = os.path.basename(file_path)
        self.logger.debug(f"Extracting Project ID from filename: {filename}")

        # Define a regex pattern to match the filename format:
        # PO_LOG_<ProjectID>-<Timestamp>.txt or PO_LOG_<ProjectID>_<Timestamp>.txt
        pattern = r"^PO_LOG_(\d{4})[-_]\d{4}-\d{2}-\d{2}_\d{2}-\d{2}-\d{2}\.txt$"

        # Attempt to match the pattern
        match = re.match(pattern, filename)

        if match:
            project_id = match.group(1)
            self.logger.info(f"✅ Project ID '{project_id}' extracted from filename '{filename}'.")
            return project_id
        else:
            # If not matched, log a warning and return a default value
            self.logger.warning(f"⚠️ No Project ID found in filename '{filename}'. Defaulting to '0000'.")
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
        # Normalize multiple spaces to single space
        clean_factors = re.sub(r'\s+', ' ', factors.replace(',', ''))

        # Enhanced regex to match variations like "1 x 473", "1x473", "1 days x 473", etc.
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

        # Enhanced regex to capture OT or extra costs, accounting for various formats
        plus_pattern = r'\+\s*\$?(\d+(?:\.\d+)?)\s*(?:OT|Misc)?'
        plus_match = re.search(plus_pattern, clean_factors, flags=re.IGNORECASE)
        if plus_match:
            try:
                ot = float(plus_match.group(1))
            except Exception as e:
                self.logger.warning(f"Error parsing OT from factors '{factors}': {e}")
                ot = 0.0

        return quantity, rate, ot

    def _read_and_store_entries(self, file_path: str, project_id: str):
        main_items = []
        contacts = []
        raw_entries = []
        manual_ids_by_po = defaultdict(set)
        po_map = {}
        main_item_has_description = {}

        with open(file_path, 'r', newline='', encoding='utf-8') as txtfile:
            reader = csv.reader(txtfile, delimiter='\t')
            headers = next(reader, None)

            for row_number, row in enumerate(reader, start=2):  # Start at 2 considering header
                if not any(row):
                    continue  # skip empty lines
                if row[0].strip().upper() == "DATE":
                    continue

                # Pad the row with empty strings if it's shorter than expected
                expected_columns = 11  # Adjust based on your data structure
                if len(row) < expected_columns:
                    row += [''] * (expected_columns - len(row))

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
                    fringes_str = row[10].strip()
                except IndexError as e:
                    self.logger.warning(f"Malformed line skipped at row {row_number}: {row}, error: {e}")
                    continue

                if not transaction_date_str:
                    self.logger.warning(f"Missing transaction date at row {row_number}: {row}")
                    continue

                if not raw_type:
                    self.logger.warning(f"Missing raw type at row {row_number}: {row}")
                    continue

                if not po_number and raw_type != 'PC':
                    self.logger.warning(f"No PO number found in line at row {row_number}: {row}")
                    continue

                subtotal = self._clean_numeric(subtotal_str)
                fringes = self._clean_numeric(fringes_str) if fringes_str else 0.0
                payment_type = self._map_payment_type(raw_type)

                # If PC, force PO number to "1"
                if payment_type == "PC":
                    po_number = "1"
                else:
                    po_number = po_number.lstrip("0")

                status = self._determine_status(pay_id)

                # Contact
                if payment_type == "PC":
                    contact_name = "PETTY CASH"
                elif payment_type == "CC":
                    contact_name = f"Credit Card {pay_id}"
                else:
                    contact_name = vendor if vendor else "UNKNOWN CONTACT"

                po_key = (project_id, po_number)
                if po_key not in po_map:
                    main_item_desc = description if description else ''
                    main_item = {
                        'project_id': project_id,
                        'name': contact_name,
                        'PO': po_number,
                        'status': status,
                        'po_type': payment_type,
                        'description': main_item_desc,
                        'amount': 0.0
                    }
                    po_map[po_key] = len(main_items)
                    main_items.append(main_item)
                    main_item_has_description[po_key] = bool(main_item_desc)

                    contacts.append({
                        "name": contact_name,
                        "project_id": project_id,
                        "PO": po_number
                    })

                try:
                    transaction_date = self._parse_date(transaction_date_str)
                except Exception as e:
                    self.logger.warning(f"Invalid date format at row {row_number}: {transaction_date_str}, error: {e}")
                    transaction_date = datetime.today()

                if payment_type.lower() not in ["cc", "pc", "crd"]:
                    due_date = transaction_date + timedelta(days=30)
                else:
                    due_date = transaction_date

                # Determine envelope_number for PC
                if payment_type == "PC":
                    parts = pay_id.split('_')
                    if len(parts) >= 3:
                        envelope_str = parts[-1].strip()
                        try:
                            envelope_number = int(envelope_str.lstrip('0') or '0')
                        except ValueError:
                            self.logger.warning(f"Invalid envelope number '{envelope_str}' at row {row_number}")
                            envelope_number = 0
                    else:
                        envelope_number = 0
                else:
                    envelope_number = 0

                # Store raw entry
                raw_entries.append({
                    'project_id': project_id,
                    'PO': po_number,
                    'vendor': vendor,
                    'date': transaction_date,
                    'due_date': due_date,
                    'factors': factors,
                    'subtotal': subtotal,
                    'description': description,
                    'status': status,
                    'account': account,
                    'payment_type': payment_type,
                    'fringes': fringes,
                    'item_id_raw': item_id,
                    'envelope_number': envelope_number,
                    'pay_id': pay_id
                })

                # Track manual IDs if present
                if item_id and item_id.strip():
                    stripped_id = item_id.lstrip('0') or '1'
                    if payment_type == "PC":
                        try:
                            numeric_id = int(stripped_id)
                        except ValueError:
                            numeric_id = 1
                        detail_item_id_key = f"{envelope_number}.{numeric_id}"
                    else:
                        try:
                            numeric_id = int(stripped_id)
                        except ValueError:
                            numeric_id = 1
                        detail_item_id_key = str(numeric_id)
                    manual_ids_by_po[(po_number, envelope_number)].add(detail_item_id_key)

                # If main item description was empty and this detail has a description
                if not main_item_has_description[po_key] and description:
                    mi_index = po_map[po_key]
                    main_items[mi_index]['description'] = description
                    main_item_has_description[po_key] = True

        return main_items, contacts, raw_entries, manual_ids_by_po

    def _assign_item_ids(self, raw_entries, manual_ids_by_po):
        assigned_item_ids = defaultdict(set)
        # First, record all manual IDs
        for key_for_ids, manual_ids in manual_ids_by_po.items():
            for mid in manual_ids:
                assigned_item_ids[key_for_ids].add(mid)

        auto_increment_counters = defaultdict(int)

        for entry in raw_entries:
            key_for_ids = (entry['PO'], entry['envelope_number'])
            item_id_raw = entry['item_id_raw']

            if not item_id_raw or not item_id_raw.strip():
                # Need to assign an auto ID
                if entry['payment_type'] == "PC":
                    # PC logic
                    auto_increment_counters[key_for_ids] += 1
                    detail_item_number = f"{entry['envelope_number']}.{auto_increment_counters[key_for_ids]}"
                    while detail_item_number in assigned_item_ids[key_for_ids]:
                        auto_increment_counters[key_for_ids] += 1
                        detail_item_number = f"{entry['envelope_number']}.{auto_increment_counters[key_for_ids]}"
                else:
                    # Non-PC
                    auto_increment_counters[key_for_ids] += 1
                    detail_item_number = str(auto_increment_counters[key_for_ids])
                    while detail_item_number in assigned_item_ids[key_for_ids]:
                        auto_increment_counters[key_for_ids] += 1
                        detail_item_number = str(auto_increment_counters[key_for_ids])

                assigned_item_ids[key_for_ids].add(detail_item_number)
                entry['detail_item_number'] = detail_item_number
            else:
                # Manual ID - reconstruct detail_item_number
                stripped_id = item_id_raw.lstrip('0') or '1'
                if entry['payment_type'] == 'PC':
                    try:
                        numeric_id = int(stripped_id)
                    except ValueError:
                        self.logger.warning(f"Invalid item_id_raw '{item_id_raw}' at PO {entry['PO']}")
                        numeric_id = 1
                    detail_item_number = f"{entry['envelope_number']}.{numeric_id}"
                else:
                    try:
                        numeric_id = int(stripped_id)
                    except ValueError:
                        self.logger.warning(f"Invalid item_id_raw '{item_id_raw}' at PO {entry['PO']}")
                        numeric_id = 1
                    detail_item_number = str(numeric_id)
                entry['detail_item_number'] = detail_item_number

    # endregion

    # region MAIN PARSER METHOD
    def parse_showbiz_po_log(self, file_path: str):
        project_id = self._extract_project_id(file_path)

        # First, read and store all entries
        main_items, contacts, raw_entries, manual_ids_by_po = self._read_and_store_entries(file_path, project_id)

        # Assign item IDs in second pass
        self._assign_item_ids(raw_entries, manual_ids_by_po)

        # Now build detail_items
        detail_items = []
        for entry in raw_entries:
            quantity, rate, ot = self._parse_factors(entry['factors'], entry['subtotal'])
            detail_item = {
                'project_id': entry['project_id'],
                'PO': entry['PO'],
                'vendor': entry['vendor'],
                'date': entry['date'].strftime('%Y-%m-%d'),
                'due date': entry['due_date'].strftime('%Y-%m-%d'),
                'quantity': quantity,
                'rate': rate,
                'description': entry['description'],
                'state': entry['status'],
                'account': entry['account'],
                'item_id': entry['detail_item_number'],
                'payment_type': entry['payment_type'],
                'total': entry['subtotal'],
                'OT': ot,
                'fringes': entry['fringes']
            }
            detail_items.append(detail_item)

        # Sum amounts for main items
        for m in main_items:
            rel_details = [d for d in detail_items if d['PO'] == m['PO'] and d['project_id'] == m['project_id']]
            total_amount = sum(d['total'] for d in rel_details)
            m['amount'] = total_amount

        self.logger.info(
            f"Parsed {len(main_items)} main items, {len(detail_items)} detail items, {len(contacts)} contacts for project {project_id}.")
        if self.TEST_MODE:
            self.logger.debug(f"Main Items: {main_items}")
            self.logger.debug(f"Detail Items: {detail_items}")
            self.logger.debug(f"Contacts: {contacts}")

        return main_items, detail_items, contacts
    # endregion


po_log_processor = POLogProcessor()

# Example usage:
# main_items, detail_items, contacts = po_log_processor.parse_showbiz_po_log('../temp_2416.txt')
# pass