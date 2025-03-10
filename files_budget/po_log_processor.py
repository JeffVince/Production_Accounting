import csv
import logging
import os
import re
from collections import defaultdict
from datetime import datetime, timedelta
from utilities.singleton import SingletonMeta

class POLogProcessor(metaclass=SingletonMeta):
    TEST_MODE = False

    def __init__(self):
        if not hasattr(self, '_initialized'):
            self.logger = logging.getLogger('po_log_logger')
            self.logger.info('🎬🍿 PO Log Processor initialized 🌟')
            self._initialized = True

    def _extract_project_number(self, file_path: str) -> str:
        filename = os.path.basename(file_path)
        self.logger.debug(f"[_extract_project_number] - 🔍 Searching for project ID in filename: '{filename}'")
        pattern = '^PO_LOG_(\\d{4})[-_]\\d{4}-\\d{2}-\\d{2}_\\d{2}-\\d{2}-\\d{2}\\.txt$'
        match = re.match(pattern, filename)
        if match:
            project_number = match.group(1)
            self.logger.info(f"[_extract_project_number] - ✅ Project ID '{project_number}' extracted from filename '{filename}'. 🎉")
            return project_number
        else:
            self.logger.warning(f"[_extract_project_number] - ⚠️ No Project ID found in filename '{filename}'. Defaulting to '0000' 🤔")
            return '0000'

    def _map_payment_type(self, raw_type: str) -> str:
        self.logger.debug(f"[_map_payment_type] - 🔧 Mapping payment type for raw_type='{raw_type}'")
        if raw_type == 'CRD':
            return 'CC'
        elif raw_type == 'PC':
            return 'PC'
        else:
            return 'INV'

    def _determine_status_and_due_date(self, pay_id: str, payment_type: str, transaction_date: datetime) -> (str, datetime):
        """
        Determines the status and due_date based on the rules:
          - If status = "PAID" and payment type = CC or PC => status=PAID,  due_date=transaction_date
          - If status = "RTP"  and payment type = INV     => status=RTP,   due_date=current_date
          - If status = "NET0" and payment type = INV     => status=RTP,   due_date=transaction_date
          - If status = "NET(some number)" and payment type = INV
                 => status=RTP, due_date=transaction_date + that number of days
          - If status = "PAID" and payment type = INV     => status=PAID,  due_date=current_date
          - If none of the above and payment type = INV   => status=PENDING, due_date=transaction_date + 30 days
          - Otherwise, default to status=PENDING, due_date=transaction_date
        """
        pay_id_upper = (pay_id or '').strip().upper()
        current_date = datetime.today()
        if payment_type in ['CC', 'PC']:
            return ('SUBMITTED', transaction_date)
        if pay_id_upper == 'RTP' and payment_type == 'INV':
            return ('RTP', current_date)
        if pay_id_upper == 'NET0' and payment_type == 'INV':
            return ('RTP', transaction_date)
        net_match = re.match('^NET(\\d+)$', pay_id_upper)
        if net_match and payment_type == 'INV':
            net_days = int(net_match.group(1))
            return ('RTP', transaction_date + timedelta(days=net_days))
        if pay_id_upper == 'PAID' and payment_type == 'INV':
            return ('PAID', current_date)
        if payment_type == 'INV':
            return ('PENDING', transaction_date + timedelta(days=30))
        return ('PENDING', transaction_date)

    def _parse_date(self, date_str: str) -> datetime:
        self.logger.debug(f"[_parse_date] - ⏰ Parsing date from '{date_str}'")
        try:
            return datetime.strptime(date_str.strip(), '%m/%d/%y')
        except ValueError as e:
            self.logger.warning(f"[_parse_date] - ❗️ Date parsing error for '{date_str}': {e}, using today's date.")
            return datetime.today()

    def _clean_numeric(self, num_str: str) -> float:
        self.logger.debug(f"[_clean_numeric] - 💲 Cleaning numeric value '{num_str}'")
        try:
            clean_str = num_str.replace(',', '').strip()
            return float(clean_str) if clean_str else 0.0
        except ValueError as e:
            self.logger.warning(f"[_clean_numeric] - ❗️ Numeric parsing error for '{num_str}': {e}, defaulting to 0.0")
            return 0.0

    def _parse_factors(self, factors: str, subtotal: float):
        self.logger.debug(f"[_parse_factors] - 🔧 Parsing factors: '{factors}' with subtotal='{subtotal}' ")
        clean_factors = re.sub('\\s+', ' ', factors.replace(',', ''))
        main_pattern = '(-?\\d+(?:\\.\\d+)?)\\s*\\w*\\s*x\\s*(-?\\d+(?:\\.\\d+)?)'
        match = re.search(main_pattern, clean_factors, flags=re.IGNORECASE)
        quantity = 1.0
        rate = float(subtotal)
        ot = 0.0
        if match:
            try:
                quantity = float(match.group(1))
                rate = float(match.group(2))
                self.logger.debug(f"[_parse_factors] - ✔️ Found quantity='{quantity}' and rate='{rate}' from factors.")
            except ValueError as e:
                error_msg = f"❗️ Error parsing factors '{factors}': {e}"
                self.logger.error('[_parse_factors] - ' + error_msg)
                raise e
        else:
            error_msg = f"❗️ Factors '{factors}' do not match the expected pattern."
            self.logger.error('[_parse_factors] - ' + error_msg)
        plus_pattern = '\\+\\s*\\$?(-?\\d+(?:\\.\\d+)?)\\s*(?:OT|Misc)?'
        plus_match = re.search(plus_pattern, clean_factors, flags=re.IGNORECASE)
        if plus_match:
            try:
                ot = float(plus_match.group(1))
                self.logger.debug(f"[_parse_factors] - ✔️ Found OT='{ot}' from factors.")
            except ValueError as e:
                self.logger.warning(f"[_parse_factors] - ❗️ Error parsing OT from factors '{factors}': {e}")
        return (quantity, rate, ot)

    def _read_and_store_entries(self, file_path: str, project_number: str):
        self.logger.info(f"[_read_and_store_entries] - 📂 Reading file: '{file_path}' for project_number='{project_number}'")
        main_items = []
        contacts = []
        raw_entries = []
        manual_ids_by_po = defaultdict(set)
        po_map = {}
        main_item_has_description = {}
        expected_columns = 11
        with open(file_path, 'r', newline='', encoding='utf-8') as txtfile:
            reader = csv.reader(txtfile, delimiter='\t')
            headers = next(reader, None)
            self.logger.debug(f'[_read_and_store_entries] - 🗂 Headers found: {headers}')
            for (row_number, row) in enumerate(reader, start=2):
                self.logger.debug(f'[_read_and_store_entries] - 📜 Processing row {row_number}: {row}')
                if not any(row):
                    self.logger.debug('[_read_and_store_entries] - 🚫 Empty row skipped.')
                    continue
                if row[0].strip().upper() == 'DATE':
                    self.logger.debug('[_read_and_store_entries] - 🚫 Header-like row encountered, skipping.')
                    continue
                if len(row) < expected_columns:
                    self.logger.debug(f'[_read_and_store_entries] - 🔧 Row {row_number} has fewer than {expected_columns} columns. Padding with empty strings.')
                    row += [''] * (expected_columns - len(row))
                elif len(row) > expected_columns:
                    self.logger.debug(f'[_read_and_store_entries] - ⚠️ Row {row_number} has more than {expected_columns} columns. Truncating extras.')
                    row = row[:expected_columns]
                try:
                    transaction_date_str = row[0].strip()
                    raw_type = row[1].strip()
                    pay_id = row[2].strip()
                    account = row[3].strip().lstrip('0')
                    item_id = row[4].strip()
                    vendor = row[5].strip()
                    description = row[6].strip()
                    po_number = row[7].strip()
                    factors = row[8].strip()
                    subtotal_str = row[9].strip()
                    fringes_str = row[10].strip()
                except IndexError as e:
                    self.logger.warning(f'[_read_and_store_entries] - ❗️ Malformed line at row {row_number}: {row}, error: {e}')
                    continue
                if not transaction_date_str:
                    self.logger.warning(f'[_read_and_store_entries] - ❗️ Missing transaction date at row {row_number}: {row}')
                    continue
                if not raw_type:
                    self.logger.warning(f'[_read_and_store_entries] - ❗️ Missing raw type at row {row_number}: {row}')
                    continue
                if not po_number and raw_type != 'PC':
                    self.logger.warning(f'[_read_and_store_entries] - ❗️ No PO number found at row {row_number}: {row}')
                    continue
                subtotal = self._clean_numeric(subtotal_str)
                fringes = self._clean_numeric(fringes_str) if fringes_str else 0.0
                payment_type = self._map_payment_type(raw_type)
                if payment_type == 'PC':
                    po_number = '1'
                else:
                    po_number = po_number.lstrip('0')
                try:
                    transaction_date = self._parse_date(transaction_date_str)
                except Exception as e:
                    self.logger.warning(f'[_read_and_store_entries] - ❗️ Invalid date at row {row_number}: {transaction_date_str}, error: {e}')
                    transaction_date = datetime.today()
                (status, due_date) = self._determine_status_and_due_date(pay_id, payment_type, transaction_date)
                if payment_type == 'PC':
                    contact_name = 'PETTY CASH'
                    vendor_type = 'PC'
                elif payment_type == 'CC':
                    contact_name = f'Credit Card {pay_id}'
                    vendor_type = 'CC'
                else:
                    contact_name = vendor if vendor else 'UNKNOWN CONTACT'
                    vendor_type = 'Vendor'
                po_key = (project_number, po_number)
                if po_key not in po_map:
                    main_item_desc = description if description else ''
                    main_item = {'project_number': project_number, 'contact_name': contact_name, 'po_number': po_number, 'status': status, 'po_type': payment_type, 'description': main_item_desc, 'amount': 0.0}
                    po_map[po_key] = len(main_items)
                    main_items.append(main_item)
                    main_item_has_description[po_key] = bool(main_item_desc)
                    contacts.append({'name': contact_name, 'project_number': project_number, 'po_number': po_number, 'vendor_type': vendor_type})
                    self.logger.debug(f"[_read_and_store_entries] - 📦 Created main item for PO='{po_number}' with contact='{contact_name}'.")
                try:
                    transaction_date = self._parse_date(transaction_date_str)
                except Exception as e:
                    self.logger.warning(f'[_read_and_store_entries] - ❗️ Invalid date at row {row_number}: {transaction_date_str}, error: {e}')
                    transaction_date = datetime.today()
                if payment_type.lower() not in ['cc', 'pc', 'crd']:
                    due_date = transaction_date + timedelta(days=30)
                else:
                    due_date = transaction_date
                if payment_type == 'PC':
                    parts = pay_id.split('_')
                    if len(parts) >= 3:
                        envelope_str = parts[-1].strip()
                        try:
                            envelope_number = int(envelope_str.lstrip('0') or '0')
                        except ValueError:
                            self.logger.warning(f"[_read_and_store_entries] - ❗️ Invalid envelope number '{envelope_str}' at row {row_number}")
                            envelope_number = 0
                    else:
                        envelope_number = 0
                else:
                    envelope_number = 0
                entry = {'project_number': project_number, 'po_number': po_number, 'vendor': vendor, 'date': transaction_date, 'due_date': due_date, 'factors': factors, 'subtotal': subtotal, 'description': description, 'status': status, 'account': account, 'payment_type': payment_type, 'fringes': fringes, 'item_id_raw': item_id, 'envelope_number': envelope_number, 'pay_id': pay_id}
                raw_entries.append(entry)
                self.logger.debug(f'[_read_and_store_entries] - 📝 Added raw entry: {entry}')
                if item_id and item_id.strip():
                    stripped_id = item_id.lstrip('0') or '1'
                    if payment_type == 'PC':
                        try:
                            numeric_id = int(stripped_id)
                        except ValueError:
                            numeric_id = 1
                        detail_item_id_key = envelope_number
                    else:
                        try:
                            numeric_id = int(stripped_id)
                        except ValueError:
                            numeric_id = 1
                        detail_item_id_key = str(numeric_id)
                    manual_ids_by_po[po_number, envelope_number].add(detail_item_id_key)
                    self.logger.debug(f"[_read_and_store_entries] - 🔗 Tracked manual ID='{detail_item_id_key}' for PO='{po_number}'.")
                if not main_item_has_description[po_key] and description:
                    mi_index = po_map[po_key]
                    main_items[mi_index]['description'] = description
                    main_item_has_description[po_key] = True
                    self.logger.debug(f"[_read_and_store_entries] - ✏️ Updated main item description for PO='{po_number}'.")
        self.logger.info(f'[_read_and_store_entries] - 📑 Finished reading entries. Found {len(main_items)} main items, {len(raw_entries)} raw entries.')
        return (main_items, contacts, raw_entries, manual_ids_by_po)

    def _assign_item_ids(self, raw_entries, manual_ids_by_po):
        """
        Logic Changes:
        1) Retain PC logic (petty cash). If item_id_raw is missing for PC, we set line_number=1.
        2) For non-PC:
           - If item_id_raw is empty -> detail_item_id="1"
           - If item_id_raw is present -> detail_item_id=<parsed numeric>
           - We **auto-increment** line_number for each repeated (po_number, detail_item_id).
        """
        self.logger.debug('🔖 Assigning detail_item_id and line_number to entries...')
        assigned_item_ids = defaultdict(set)
        for (key_for_ids, manual_ids) in manual_ids_by_po.items():
            for mid in manual_ids:
                assigned_item_ids[key_for_ids].add(mid)
                self.logger.debug(f"🔗 Manual ID='{mid}' recorded for key='{key_for_ids}'")
        line_number_counters = defaultdict(int)
        for entry in raw_entries:
            key_for_ids = (entry['po_number'], entry['envelope_number'])
            item_id_raw = entry['item_id_raw']
            payment_type = entry['payment_type']
            envelope_number = entry['envelope_number']
            if payment_type == 'PC':
                detail_item_id = envelope_number
                if not item_id_raw or not item_id_raw.strip():
                    line_number = 1
                else:
                    stripped_id = item_id_raw.lstrip('0') or '1'
                    try:
                        numeric_id = int(stripped_id)
                    except ValueError:
                        numeric_id = 1
                    line_number = numeric_id
            else:
                if not item_id_raw or not item_id_raw.strip():
                    detail_item_id = '1'
                else:
                    stripped_id = item_id_raw.lstrip('0') or '1'
                    try:
                        numeric_id = int(stripped_id)
                    except ValueError:
                        numeric_id = 1
                    detail_item_id = str(numeric_id)
                line_number_key = (entry['po_number'], detail_item_id)
                line_number_counters[line_number_key] += 1
                line_number = line_number_counters[line_number_key]
            entry['detail_item_id'] = detail_item_id
            entry['line_number'] = line_number
            self.logger.debug(f"🆔 Assigned detail_item_id='{detail_item_id}', line_number='{line_number}' for PO='{entry['po_number']}', payment_type='{payment_type}'.")

    def parse_showbiz_po_log(self, file_path: str):
        self.logger.info(f'[parse_showbiz_po_log] - 🚀 Starting parse_showbiz_po_log for file: {file_path}')
        project_number = self._extract_project_number(file_path)
        (main_items, contacts, raw_entries, manual_ids_by_po) = self._read_and_store_entries(file_path, project_number)
        self._assign_item_ids(raw_entries, manual_ids_by_po)
        detail_items = []
        self.logger.debug('[parse_showbiz_po_log] - 🔄 Creating detail_items from raw_entries...')
        for entry in raw_entries:
            (quantity, rate, ot) = self._parse_factors(entry['factors'], entry['subtotal'])
            detail_item = {'project_number': entry['project_number'], 'po_number': entry['po_number'], 'detail_item_id': entry['detail_item_id'], 'line_number': entry['line_number'], 'vendor': entry['vendor'], 'date': entry['date'].strftime('%Y-%m-%d'), 'due date': entry['due_date'].strftime('%Y-%m-%d'), 'quantity': quantity, 'rate': rate, 'description': entry['description'], 'state': entry['status'], 'account': entry['account'], 'payment_type': entry['payment_type'], 'total': entry['subtotal'], 'ot': ot, 'fringes': entry['fringes']}
            detail_items.append(detail_item)
            self.logger.debug(f'[parse_showbiz_po_log] - 💾 Created detail_item: {detail_item}')
        self.logger.debug('[parse_showbiz_po_log] - 🔢 Summing up amounts for main items...')
        for m in main_items:
            rel_details = [d for d in detail_items if d['po_number'] == m['po_number'] and d['project_number'] == m['project_number']]
            total_amount = sum((d['total'] for d in rel_details))
            m['amount'] = total_amount
            self.logger.debug(f"[parse_showbiz_po_log] - 📈 PO='{m['po_number']}' total amount='{total_amount}'")
        self.logger.info(f'[parse_showbiz_po_log] - 🎉 Parsed {len(main_items)} main items, {len(detail_items)} detail items, and {len(contacts)} contacts for project {project_number}.')
        if self.TEST_MODE:
            self.logger.debug(f'[parse_showbiz_po_log] - 🗒 Main Items: {main_items}')
            self.logger.debug(f'[parse_showbiz_po_log] - 🗒 Detail Items: {detail_items}')
            self.logger.debug(f'[parse_showbiz_po_log] - 🗒 Contacts: {contacts}')
        self.logger.info('[parse_showbiz_po_log] - ✅ Parsing completed successfully! 🏁')




        return (main_items, detail_items, contacts)



po_log_processor = POLogProcessor()


