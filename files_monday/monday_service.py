import json
import logging
import requests
from typing import Any

import database_util
from database.database_util import DatabaseOperations
from utilities.singleton import SingletonMeta
from utilities.config import Config
from files_monday.monday_util import monday_util
from files_monday.monday_api import monday_api
from files_monday.monday_database_util import monday_database_util


class MondayService(metaclass=SingletonMeta):
    """
    📝 MondayService
    ===============
    Provides high-level methods to interact with Monday.com for POs, Detail Items, and Contacts:
      - create/update PO in Monday,
      - create/update sub-items,
      - match or create contacts,
      - sync data from Monday boards to DB (and vice versa).
    """

    # region 🎛️ Initialization
    def __init__(self):
        """
        🌟 Sets up logging, references to Monday utilities & DB ops,
        and relevant board/column config from Config & monday_util.
        """
        if not hasattr(self, '_initialized'):
            self.logger = logging.getLogger('monday_logger')
            self.monday_util = monday_util
            self.db_util = monday_database_util    # custom DB layer for Monday data (if needed)
            self.monday_api = monday_api           # MondayAPI singleton for raw GraphQL calls
            self.api_token = Config.MONDAY_API_TOKEN
            self.board_id = self.monday_util.PO_BOARD_ID
            self.subitem_board_id = self.monday_util.SUBITEM_BOARD_ID
            self.contact_board_id = self.monday_util.CONTACT_BOARD_ID
            self.api_url = self.monday_util.MONDAY_API_URL
            # ✨ Our queue for buffered contact upserts:
            self.contact_upsert_queue = []
            self._detail_upsert_queue = []
            self._po_upsert_queue = []
            self.db_ops = DatabaseOperations()
            self.logger.info('🌐 [MondayService __init__] - Monday Service initialized 🎉')
            self._initialized = True
    # endregion

    # region 🔒 Internal: GraphQL Request Wrapper
    def _make_request(self, query: str, variables: dict = None):
        """
        Internal method that delegates to the official 'monday_api' for raw GraphQL queries.
        """
        self.logger.debug("🔒 [_make_request] - Delegating GraphQL request to monday_api.")
        return self.monday_api._make_request(query, variables=variables)
    # endregion

    # region 🏷️ Basic PO Methods
    def update_po_status(self, pulse_id: int, status: str):
        """
        🎨 Update the status of a Purchase Order (PO) in Monday.com
        using a known column_id for 'status' from monday_util.
        """
        self.logger.info(f"🖍️ [update_po_status] - Updating PO item_id={pulse_id} to status='{status}'...")
        try:
            query = '''
            mutation ($board_id: Int!, $item_id: Int!, $column_id: String!, $value: JSON!) {
              change_column_value(
                board_id: $board_id,
                item_id: $item_id,
                column_id: $column_id,
                value: $value
              ) {
                id
              }
            }'''
            variables = {
                'board_id': int(self.board_id),
                'item_id': pulse_id,
                'column_id': self.monday_util.PO_STATUS_COLUMN_ID,
                'value': json.dumps({'label': status})
            }
            self._make_request(query, variables)
            self.logger.info(f"✅ [update_po_status] - Updated PO status for item {pulse_id} => '{status}'")
        except requests.HTTPError as e:
            self.logger.error(f"❌ [update_po_status] - Failed to update PO status for item={pulse_id}: {e}")
            raise
    # endregion

    # region 🏷️ Contact Matching
    def match_or_create_contact(self, vendor_name: str, po_number: str) -> int:
        """
        🔎 Either finds an existing contact by vendor_name or creates a new one, then optionally links it to the PO in local DB.

        :param vendor_name: The vendor's name from your PO record
        :param po_number: The PO number for linking logic
        :return: contact_id in Monday.com (pulse_id)
        """
        self.logger.info(f"🔗 [match_or_create_contact] - Attempting to match or create contact for vendor='{vendor_name}', PO='{po_number}'...")
        try:
            # Use your monday_util / monday_api to find contact item by name:
            existing_items = self.monday_util.fetch_item_by_name(vendor_name, board='Contacts')
            if existing_items:
                contact = existing_items[0]
                contact_id = contact['id']
                self.logger.info(f"👤 Found existing contact '{vendor_name}' with ID={contact_id}")
            else:
                self.logger.info(f"🆕 No existing contact found for '{vendor_name}' => creating new contact!")
                create_resp = self.monday_api.create_contact(vendor_name)
                contact_id = create_resp['data']['create_item']['id']
                self.logger.info(f"🎉 Created new contact '{vendor_name}' => ID={contact_id}")

            # Optionally, link in your DB that contact_id => that PO number
            self.db_util.link_contact_to_po(po_number, contact_id)  # if your custom logic needs that

            return int(contact_id)
        except Exception as e:
            self.logger.error(f"❌ [match_or_create_contact] - Error in matching/creating contact: {e}")
            raise
    # endregion

    # region 🔎 Retrieval Helpers
    def get_po_number_from_item(self, item_id: int) -> Any | None:
        """
        Retrieves the PO number from a specific item in Monday.com using your existing monday_util method.
        """
        self.logger.info(f"🔎 [get_po_number_from_item] - Attempting to retrieve PO number for item={item_id}...")
        try:
            (po_number, _) = self.monday_util.get_po_number_and_data(item_id)
            if po_number:
                self.logger.info(f"✅ [get_po_number_from_item] - Found PO number '{po_number}' for item={item_id}")
                return po_number
            else:
                self.logger.warning(f"⚠️ [get_po_number_from_item] - No PO number found for item={item_id}")
                return None
        except Exception as e:
            self.logger.error(f"❌ [get_po_number_from_item] - Error retrieving PO number for item={item_id}: {e}")
            return None
    # endregion

    # region 🌐 Upsert PO
    def upsert_po_in_monday(self, po_record: dict):
        self.logger.info(f"🌐 [upsert_po_in_monday] - Handling upsert for PO record:\n    {po_record}")
        project_number = po_record.get('project_number')
        po_number = po_record.get('po_number')
        pulse_id = po_record.get('pulse_id')
        description = po_record.get('description')
        contact_name = po_record.get("vendor_name")

        # Prepare column values using the formatter function from monday_util.
        column_values_dict = self._build_po_column_values(po_record)
        column_values_str = json.dumps(column_values_dict)

        if not pulse_id:
            self.logger.info("🔎 Checking if item already exists in Monday by project_number & po_number...")
            search_resp = self.monday_api.fetch_item_by_po_and_project(project_number, po_number)
            items_found = search_resp['data']['items_page_by_column_values']['items']

            if len(items_found) == 1:
                existing_item = items_found[0]
                existing_id = existing_item['id']
                self.logger.info(f"🔗 Found existing PO item => ID={existing_id}. Updating columns.")
                po_record['pulse_id'] = existing_id
                self.monday_api.update_item(existing_id, column_values_str, type='main')
            else:
                self.logger.info("🆕 No matching item found => creating new one.")
                group_id = 'topics'  # or however you choose the group
                create_resp = self.monday_api.create_item(
                    board_id=self.board_id,
                    group_id=group_id,
                    name=contact_name or f"PO#{po_number}",
                    column_values=column_values_str
                )
                try:
                    new_id = create_resp['data']['create_item']['id']
                    po_record['pulse_id'] = new_id
                    self.logger.info(f"🎉 Created new PO item => ID={new_id} for PO#{po_number}")
                except Exception as ce:
                    self.logger.error(f"❌ Could not create PO item for po_number={po_number}: {ce}")
        else:
            self.logger.info(f"ℹ️ Existing pulse_id={pulse_id} found => updating columns.")
            self.monday_api.update_item(pulse_id, column_values_str, type='main')

        self.logger.info("✅ [upsert_po_in_monday] - Finished upsert logic for PO record.")
        # endregion

    # region ⚙️ Upsert Detail Subitem
    def upsert_detail_subitem_in_monday(self, detail_item: dict):
        self.logger.info(f"🧱 [upsert_detail_subitem_in_monday] - Handling subitem upsert:\n    {detail_item}")
        parent_id = detail_item.get('parent_pulse_id')
        if not parent_id:
            self.logger.warning("⚠️ No parent_pulse_id; cannot create subitem. Exiting.")
            return

        column_values_dict = self._build_detail_subitem_values(detail_item)
        column_values_json_str = json.dumps(column_values_dict)
        self.logger.debug(f"🖌️ Column values for subitem: {column_values_json_str}")

        subitem_id = detail_item.get('pulse_id')
        if subitem_id:
            self.logger.info(f"🔄 Updating existing subitem ID={subitem_id}...")
            self.monday_api.update_item(subitem_id, column_values_json_str, type='subitem')
        else:
            self.logger.info("🔎 No existing subitem found; creating new one.")
            create_resp = self.monday_api.create_subitem(
                parent_item_id=parent_id,
                subitem_name=detail_item.get('description') or f"Line {detail_item.get('line_number')}",
                column_values=column_values_dict  # Pass the dict directly as variable
            )
            if create_resp and create_resp['data']['create_subitem'].get('id'):
                new_id = create_resp['data']['create_subitem']['id']
                detail_item['pulse_id'] = new_id
                self.logger.info(f"🎉 Created new subitem with ID={new_id}")
            else:
                self.logger.warning("❌ Could not create new subitem; no 'id' in response.")

        self.logger.info("🏁 [upsert_detail_subitem_in_monday] - Completed subitem upsert.")
        # endregion

    # region 🎉 Sync from Monday -> DB
    def sync_main_items_from_monday_board(self):
        """
        ♻️ Pull main items from Monday's PO board, then attempt to sync them into the DB.
        You can store them using self.db_util if needed.
        """
        self.logger.info(f'📥 [sync_main_items_from_monday_board] - Fetching items from board {self.board_id}...')
        try:
            all_items = self.monday_api.fetch_all_items(self.board_id)
            self.logger.info(f'🗂 [sync_main_items_from_monday_board] - Fetched {len(all_items)} items from board {self.board_id}')
            for item in all_items:
                creation_item = self.db_util.prep_main_item_event_for_db_creation(item)
                if creation_item:
                    status = self.db_util.create_or_update_main_item_in_db(creation_item)
                    self.logger.info(f"🔄 [sync_main_items_from_monday_board] - Synced PO pulse_id={creation_item.get('pulse_id')}, status={status}")
        except Exception as e:
            self.logger.exception(f'❌ [sync_main_items_from_monday_board] - Unexpected error: {e}')
    # endregion

    # region 🔂 Sync Subitems
    def sync_sub_items_from_monday_board(self):
        """
        ♻️ Pull subitems from Monday's subitem board, then sync them to DB.
        """
        self.logger.info(f'📥 [sync_sub_items_from_monday_board] - Fetching sub-items from board {self.subitem_board_id}...')
        try:
            all_subitems = self.monday_api.fetch_all_sub_items()
            self.logger.info(f'🧩 [sync_sub_items_from_monday_board] - Fetched {len(all_subitems)} sub-items from board {self.subitem_board_id}')
        except Exception as e:
            self.logger.error(f'⚠️ [sync_sub_items_from_monday_board] - Error fetching sub-items: {e}')
            return

        # region 🗂 Process items
        try:
            orphan_count = 0
            for subitem in all_subitems:
                creation_item = self.db_util.prep_sub_item_event_for_db_creation(subitem)
                if not creation_item:
                    orphan_count += 1
                    self.logger.debug(f"🔎 [sync_sub_items_from_monday_board] - Skipping sub-item ID={subitem.get('id')} (missing parent).")
                    continue
                result = self.db_util.create_or_update_sub_item_in_db(creation_item)
                if not result:
                    self.logger.error(f"❌ [sync_sub_items_from_monday_board] - Failed sync for sub-item pulse_id={creation_item.get('pulse_id')}")
                    continue
                status = result.get('status')
                if status == 'Orphan':
                    orphan_count += 1
                    self.logger.debug(f"🙅 [sync_sub_items_from_monday_board] - Orphan sub-item pulse_id={creation_item.get('pulse_id')}")
                elif status in ['Created', 'Updated']:
                    self.logger.info(f"🎉 [sync_sub_items_from_monday_board] - {status} sub-item pulse_id={creation_item.get('pulse_id')}")
                else:
                    self.logger.error(f"❌ [sync_sub_items_from_monday_board] - Unexpected error for sub-item {creation_item.get('pulse_id')}: {result.get('error')}")
            self.logger.info(f"[sync_sub_items_from_monday_board] - Sub-items sync completed. Orphans={orphan_count}, total={len(all_subitems)}")
        except Exception as e:
            self.logger.exception(f'❌ [sync_sub_items_from_monday_board] - Unexpected error in sub-items sync: {e}')
        # endregion
    # endregion

    # region 🗂 Contacts
    def sync_contacts_from_monday_board(self):
        """
        ♻️ Pull contacts from Monday's contact board, then sync them into local DB.
        """
        self.logger.info(f'📥 [sync_contacts_from_monday_board] - Fetching contacts from board {self.contact_board_id}...')
        try:
            all_contacts = self.monday_api.fetch_all_contacts()
            self.logger.info(f'🗂 [sync_contacts_from_monday_board] - Fetched {len(all_contacts)} contact(s).')
        except Exception as e:
            self.logger.error(f'⚠️ [sync_contacts_from_monday_board] - Error fetching contacts: {e}')
            return

        # region 🗃 Store or Update in DB
        try:
            for contact in all_contacts:
                monday_fields = self.monday_api.extract_monday_contact_fields(contact)
                tax_number_int = None
                if monday_fields['tax_number_str']:
                    tax_number_int = self.database_util.parse_tax_number(monday_fields['tax_number_str'])
                vendor_status = monday_fields['vendor_status']
                if vendor_status not in ['PENDING', 'TO VERIFY', 'APPROVED', 'ISSUE']:
                    vendor_status = 'PENDING'
                try:
                    existing_contact = self.database_util.find_contact_by_name(contact_name=contact['name'])
                    if not existing_contact:
                        db_contact = self.database_util.create_contact(
                            name=contact['name'],
                            pulse_id=monday_fields['pulse_id'],
                            phone=monday_fields['phone'],
                            email=monday_fields['email'],
                            address_line_1=monday_fields['address_line_1'],
                            address_line_2=monday_fields['address_line_2'],
                            city=monday_fields['city'],
                            zip=monday_fields['zip_code'],
                            region=monday_fields['region'],
                            country=monday_fields['country'],
                            tax_type=monday_fields['tax_type'],
                            tax_number=tax_number_int,
                            payment_details=monday_fields['payment_details'],
                            vendor_status=vendor_status,
                            tax_form_link=monday_fields['tax_form_link']
                        )
                    else:
                        db_contact = self.database_util.update_contact(
                            contact_id=existing_contact['id'],
                            name=contact['name'],
                            pulse_id=monday_fields['pulse_id'],
                            phone=monday_fields['phone'],
                            email=monday_fields['email'],
                            address_line_1=monday_fields['address_line_1'],
                            address_line_2=monday_fields['address_line_2'],
                            city=monday_fields['city'],
                            zip=monday_fields['zip_code'],
                            region=monday_fields['region'],
                            country=monday_fields['country'],
                            tax_type=monday_fields['tax_type'],
                            tax_number=tax_number_int,
                            payment_details=monday_fields['payment_details'],
                            vendor_status=vendor_status,
                            tax_form_link=monday_fields['tax_form_link']
                        )
                    self.logger.info(f"🔄 [sync_contacts_from_monday_board] - Synced contact => {contact['name']}, ID={db_contact.get('id') if db_contact else '??'}")
                except Exception as ce:
                    self.logger.error(f'❌ [sync_contacts_from_monday_board] - Error adding contact to DB: {ce}')
        except Exception as e:
            self.logger.error(f'❌ [sync_contacts_from_monday_board] - Error syncing contacts to DB: {e}')
        self.logger.info('✅ [sync_contacts_from_monday_board] - Contacts synchronization done.')


    # endregion

    # -------------------------------------------------------------------------
    #                         PURCHASE ORDER AGGREGATOR
    # -------------------------------------------------------------------------
    def buffered_upsert_po(self, po_record: dict):
        """
        🌀 [START] Stage a PurchaseOrder for eventual upsert in Monday IF:
          - There's no 'pulse_id' (new record),
          - OR if db_ops.purchase_order_has_changes(...) is True.

        :param po_record: Local DB dict (has 'id', 'project_number', 'po_number', possibly 'pulse_id', etc.)
        """
        self.logger.info("🌀 [START] Attempting to stage a PurchaseOrder for Monday upsert...")
        if not po_record:
            self.logger.warning("🌀 No PO record => skipping.")
            self.logger.info("🌀 [COMPLETED] [STATUS=Fail]")
            return

        local_id = po_record.get('id')
        pulse_id = po_record.get('pulse_id')

        # Check if newly created (no pulse_id) or if aggregator says we have DB changes:
        if not pulse_id:
            self.logger.info("🆕 PO has no pulse_id => definitely enqueueing for creation in Monday.")
            self._po_upsert_queue.append(po_record)
        else:
            # Use db_ops to see if we have changes
            has_changes = self.db_ops.purchase_order_has_changes(
                record_id=local_id,
                # if you want to pass newly updated fields to check, for example:
                project_number=po_record.get('project_number'),
                po_number=po_record.get('po_number'),
                description=po_record.get('description'),
                folder_link=po_record.get('folder_link')
                # ... any other fields that might differ ...
            )
            if has_changes:
                self.logger.info(f"🌀 PO {local_id} => aggregator indicates changes => enqueueing update.")
                self._po_upsert_queue.append(po_record)
            else:
                self.logger.info("🌀 No changes => skipping Monday upsert for this PO.")

        self.logger.info("🌀 [COMPLETED] [STATUS=Success] Staged PO for Monday upsert.")

    def execute_batch_upsert_pos(self):
        """
        🌀 [START] Actually perform the create/update in Monday for all queued POs,
        using the existing monday_api's batch_create_or_update_items method.

        Clears the queue at the end.
        """
        self.logger.info("🌀 [START] Performing batched PO upserts in Monday...")

        if not self._po_upsert_queue:
            self.logger.info("🌀 No POs in queue => nothing to upsert.")
            self.logger.info("🌀 [COMPLETED] [STATUS=Success] No work done.")
            return

        items_to_create = []
        items_to_update = []

        # Build batch data for each queued PO
        for po_data in self._po_upsert_queue:
            pulse_id = po_data.get('pulse_id')

            contact_record = self.db_ops.search_contacts(["id"], [po_data["contact_id"]])
            if isinstance(contact_record, list):
                contact_record = contact_record[0]
            contact_pulse_id = contact_record['pulse_id']

            if not pulse_id:
                items_to_create.append({
                    'db_item': po_data,
                    'monday_item_id': None,
                    'monday_contact_id': contact_pulse_id
                })
            else:
                items_to_update.append({
                    'db_item': po_data,
                    'monday_item_id': pulse_id,
                    "monday_contact_id": contact_pulse_id
                })

        self.logger.info(f"🌀 Upserting POs => create={len(items_to_create)}, update={len(items_to_update)}")

        # If you want to group by project_number, do so. We'll pick from the first create item:
        project_id = None
        if items_to_create:
            project_id = items_to_create[0]['db_item'].get('project_number')

        # Actually do the create calls
        created_results = []
        if items_to_create:
            created_results = self.monday_api.batch_create_or_update_items(
                batch=items_to_create,
                project_id=project_id or "Unknown",
                create=True
            )

        # Do the update calls
        updated_results = []
        if items_to_update:
            updated_results = self.monday_api.batch_create_or_update_items(
                batch=items_to_update,
                project_id=project_id or "Unknown",
                create=False
            )

        self._po_upsert_queue.clear()

        total_processed = len(created_results) + len(updated_results)
        self.logger.info(f"🌀 [COMPLETED] [STATUS=Success] PO batch upsert => total={total_processed} processed.")

        #Return the created results
        return created_results
        self.logger.info(f"🛟 Saved {len(created_results)}  Pulse IDs to DB")

    # -------------------------------------------------------------------------
    #                         CONTACT AGGREGATOR
    # -------------------------------------------------------------------------
    def buffered_upsert_contact(self, contact_record: dict):
        """
        🌀 [START] Stage a Contact record for Monday upsert
        if newly created or aggregator says it changed (contact_has_changes).
        """
        self.logger.info("🌀 [START] Attempting to stage a Contact for Monday upsert...")
        if not contact_record:
            self.logger.warning("🌀 No contact_record => skipping.")
            self.logger.info("🌀 [COMPLETED] [STATUS=Fail]")
            return

        contact_id = contact_record.get('id')
        pulse_id = contact_record.get('pulse_id')

        # If no pulse_id => definitely new in Monday
        if not pulse_id:
            self.logger.info("🆕 Contact has no pulse_id => enqueuing for Monday create.")
            self.contact_upsert_queue.append(contact_record)
        else:
            # Check if aggregator logic indicates changes
            has_changes = self.db_ops.contact_has_changes(
                record_id=contact_id,
                name=contact_record.get('name'),
                email=contact_record.get('email'),
                phone=contact_record.get('phone'),
                payment_details=contact_record.get('payment_details'),
                vendor_status=contact_record.get('vendor_status'),
                address_line_1=contact_record.get('address_line_1'),
                address_line_2=contact_record.get('address_line_2'),
                city=contact_record.get('city'),
                zip=contact_record.get('zip'),
                region=contact_record.get('region'),
                country=contact_record.get('country'),
                tax_type=contact_record.get('tax_type'),
                tax_number=contact_record.get('tax_number'),
                tax_form_link=contact_record.get('tax_form_link'),
                xero_id=contact_record.get('xero_id'),
                session=None,  # or pass a session if needed
            )
            if has_changes:
                self.logger.info(f"🌀 Contact {contact_id} => aggregator indicates changes => enqueueing update.")
                self.contact_upsert_queue.append(contact_record)
            else:
                self.logger.info("🌀 No contact changes => skipping Monday upsert.")

        self.logger.info("🌀 [COMPLETED] [STATUS=Success] Staged Contact for Monday upsert.")

    def execute_batch_upsert_contacts(self):
        """
        🌀 Perform the batch create/update in Monday for all queued contacts,
        clearing the queue afterwards.
        """
        self.logger.info("🌀 [START] Performing batched Contact upserts in Monday...")

        if not self.contact_upsert_queue:
            self.logger.info("🌀 No contacts in queue => nothing to upsert.")
            self.logger.info("🌀 [COMPLETED] [STATUS=Success] None processed.")
            return

        items_to_create = []
        items_to_update = []

        for ct_data in self.contact_upsert_queue:
            pulse_id = ct_data.get('pulse_id')
            col_vals = self._build_contact_column_values(ct_data)

            if not pulse_id:
                items_to_create.append({
                    'db_item': ct_data,
                    'column_values': col_vals,
                    'monday_item_id': None
                })
            else:
                items_to_update.append({
                    'db_item': ct_data,
                    'column_values': col_vals,
                    'monday_item_id': pulse_id
                })

        self.logger.info(f"🌀 Upserting Contacts => create={len(items_to_create)}, update={len(items_to_update)}")

        # For contacts, we might not have a 'project_id', so just call them "Contacts"
        project_id = "Contacts"
        created_results = []
        updated_results = []

        if items_to_create:
            created_results = self.monday_api.batch_create_or_update_items(
                batch=items_to_create,
                project_id=project_id,
                create=True
            )
        if items_to_update:
            updated_results = self.monday_api.batch_create_or_update_items(
                batch=items_to_update,
                project_id=project_id,
                create=False
            )

        self.contact_upsert_queue.clear()
        total = len(created_results) + len(updated_results)
        self.logger.info(f"🌀 [COMPLETED] [STATUS=Success] Contact batch => total={total} processed.")

    def _build_contact_column_values(self, contact_data: dict) -> dict:
        """
        Build column values dict for a contact.
        Only do so if aggregator says changes or new record.
        """
        col_vals = {}
        if contact_data.get('name'):
            col_vals[self.monday_util.CONTACT_NAME] = str(contact_data['name'])
        if contact_data.get('vendor_status'):
            col_vals[self.monday_util.CONTACT_STATUS] = {'label': contact_data["vendor_status"]}
        if contact_data.get('payment_details'):
            col_vals[self.monday_util.CONTACT_PAYMENT_DETAILS] = {'label': contact_data["payment_details"]}
        if contact_data.get("vendor_type"):
            pass #TODO need to add field to mnd
        if contact_data.get('email'):
            col_vals[self.monday_util.CONTACT_EMAIL] = str(contact_data['email'])
        if contact_data.get('phone'):
            col_vals[self.monday_util.CONTACT_PHONE] = str(contact_data['phone'])
        if contact_data.get('address_line_1'):
            col_vals[self.monday_util.CONTACT_ADDRESS_LINE_1] = str(contact_data['address_line_1'])
        if contact_data.get('address_line_2'):
            col_vals[self.monday_util.CONTACT_ADDRESS_LINE_2] = str(contact_data['address_line_2'])
        if contact_data.get('city'):
            col_vals[self.monday_util.CONTACT_ADDRESS_CITY] = str(contact_data['city'])
        if contact_data.get('zip'):
            col_vals[self.monday_util.CONTACT_ADDRESS_ZIP] = str(contact_data['zip'])
        if contact_data.get('region'):
            pass #TODO need to add field to mnd
        if contact_data.get('country'):
            col_vals[self.monday_util.CONTACT_ADDRESS_COUNTRY] = str(contact_data['country'])
        if contact_data.get('tax_type'):
            col_vals[self.monday_util.CONTACT_TAX_TYPE] = str(contact_data['tax_type'])
        if contact_data.get('tax_number'):
            col_vals[self.monday_util.CONTACT_TAX_NUMBER] = str(contact_data['tax_number'])
           #TODO modify this for new tax form structure
        if contact_data.get('tax_form_link'):
            link_lower = contact_data['tax_form_link'].lower()
            if 'w9' in link_lower:
                link_text = '🇺🇸 W-9'
            elif 'w8-ben-e' in link_lower:
                link_text = '🌎 W-8BEN-E 🏢'
            elif 'w8-ben' in link_lower:
                link_text = '🌎 W-8BEN 🙋'
            else:
                link_text = 'Tax Form 🤷'
            col_vals[self.monday_util.CONTACT_TAX_FORM_LINK] = {
                'url': contact_data['tax_form_link'],
                'text': link_text
            }

        if contact_data.get("xero_id"):
            pass #TODO need to add field to mnd



        self.logger.debug(f"💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥{col_vals}")

        return col_vals

    # -------------------------------------------------------------------------
    #                     DETAIL ITEM (SUBITEM) AGGREGATOR
    # -------------------------------------------------------------------------
    def buffered_upsert_detail_item(self, detail_record: dict):
        """
        🌀 [START] Stage a detail item for Monday subitem upsert.
        If aggregator found changes (detail_item_has_changes) or no subitem ID,
        we enqueue.

        detail_record: local dict, possibly has 'parent_id', 'pulse_id', etc.
        For subitems, we might store 'parent_pulse_id' for the main item
        and 'pulse_id' for the subitem itself.
        """
        if not detail_record:
            self.logger.warning("🌀 No detail_item record => skipping.")
            self.logger.info("🌀 [COMPLETED] [STATUS=Fail]")
            return

        record_id = detail_record.get('id')
        subitem_id = detail_record.get('pulse_id')  # or some field that means 'subitem pulse_id'
        parent_pulse_id = detail_record.get('parent_pulse_id')

        # If aggregator says no changes + subitem_id exists => skip
        has_changes = self.db_ops.detail_item_has_changes(
            record_id=record_id,
            project_number=detail_record.get('project_number'),
            po_number=detail_record.get('po_number'),
            detail_number=detail_record.get('detail_number'),
            line_number=detail_record.get('line_number'),
            state=detail_record.get('state'),
            description=detail_record.get('description'),
            transaction=detail_record.get('transaction_date'),
            due_date=detail_record.get('due_date'),
            rate=detail_record.get('rate'),
            quantity=detail_record.get('quantity'),
            ot=detail_record.get('ot'),
            fringes=detail_record.get('fringes'),
            receipt_id=detail_record.get('receipt_id'),
            invoid_id=detail_record.get('invoid_id'),
            pulse_id=detail_record.get('pulse_id'),
            parent_pulse_id=detail_record.get('parent_pulse_id'),
            payment_type=detail_record.get('payment_type')
        )

        if not subitem_id and not has_changes:
            # If there's absolutely no subitem ID, but aggregator also says no changes,
            # it's still "new"? Actually, aggregator presumably created it in DB =>
            # that alone might be changes. But if there's truly no difference from the aggregator perspective,
            # we skip.
            self.logger.info("🌀 No subitem_id, aggregator says no changes => skipping subitem upsert.")
            self.logger.info("🌀 [COMPLETED] [STATUS=NoChange]")
            return

        if subitem_id:
            # We have a subitem ID => only queue if has_changes
            if has_changes:
                self.logger.info("🌀 Subitem has changes => enqueueing update.")
                self._detail_upsert_queue.append(detail_record)
            else:
                self.logger.info("🌀 Subitem no changes => skipping Monday.")
        else:
            # No subitem_id => new subitem
            self.logger.info("🆕 No subitem_id => definitely enqueueing for Monday create.")
            self._detail_upsert_queue.append(detail_record)

        self.logger.info("🌀 [COMPLETED] [STATUS=Success] Staged detail item for subitem upsert.")

    def execute_batch_upsert_detail_items(self):
        """
        🌀 Perform batch create/update of detail items as Monday subitems.
        Clears the queue after.
        """
        self.logger.info("🌀 [START] Performing batched subitem upserts in Monday...")

        if not self._detail_upsert_queue:
            self.logger.info("🌀 No detail items queued => done.")
            self.logger.info("🌀 [COMPLETED] [STATUS=Success]")
            return

        items_to_create = []
        items_to_update = []

        for di_data in self._detail_upsert_queue:
            subitem_id = di_data.get('pulse_id')
            parent_item_id = di_data.get('parent_pulse_id')
            col_vals = self._build_detail_subitem_values(di_data)

            if not subitem_id:
                # create
                items_to_create.append({
                    'db_sub_item': di_data,
                    'column_values': col_vals,
                    'parent_id': parent_item_id
                })
            else:
                # update
                items_to_update.append({
                    'db_sub_item': di_data,
                    'column_values': col_vals,
                    'parent_id': parent_item_id,
                    'monday_item_id': subitem_id
                })

        self.logger.info(f"🌀 Subitem upsert => create={len(items_to_create)}, update={len(items_to_update)}")

        created_results = []
        updated_results = []

        if items_to_create:
            self.logger.info("🌀 Creating subitems in Monday...")
            created_results = self.monday_api.batch_create_or_update_subitems(
                subitems_batch=items_to_create,
                create=True
            )
        if items_to_update:
            self.logger.info("🌀 Updating subitems in Monday...")
            updated_results = self.monday_api.batch_create_or_update_subitems(
                subitems_batch=items_to_update,
                create=False
            )

        self._detail_upsert_queue.clear()
        total_proc = len(created_results) + len(updated_results)
        self.logger.info(f"🌀 [COMPLETED] [STATUS=Success] subitem upsert => total={total_proc} processed.")

    def _build_detail_subitem_values(self, detail_item: dict) -> dict:
        """
        Constructs the column values for a detail item subitem using the formatter
        function from monday_util.
        """
        formatted_json_str = self.monday_util.subitem_column_values_formatter(
            project_id=detail_item.get('project_number'),
            po_number=detail_item.get('po_number'),
            detail_number=detail_item.get('detail_number'),
            line_number=detail_item.get('line_number'),
            description=detail_item.get('description'),
            quantity=detail_item.get('quantity'),
            rate=detail_item.get('rate'),
            date=detail_item.get('transaction_date'),
            due_date=detail_item.get('due_date'),
            account_number=detail_item.get('account_code'),
            link=detail_item.get('file_link'),
            OT=detail_item.get('ot'),
            fringes=detail_item.get('fringes')
        )
        return json.loads(formatted_json_str)

monday_service = MondayService()