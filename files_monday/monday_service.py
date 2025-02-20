# region 1: Imports
import json
import logging

from database.database_util import DatabaseOperations
from utilities.singleton import SingletonMeta
from utilities.config import Config
from files_monday.monday_util import monday_util
from files_monday.monday_api import monday_api
# endregion

# region 2: MondayService Class Definition
class MondayService(metaclass=SingletonMeta):
    """
    📝 MondayService
    ===============
    Provides high-level methods to interact with Monday.com for Purchase Orders (POs),
    Detail Items (Subitems), and Contacts.
    """

    # region 2.1: Initialization
    def __init__(self):
        """
        Initializes logging and sets up references to monday_util, DatabaseOperations,
        and the MondayAPI singleton.
        """
        if not hasattr(self, '_initialized'):
            self.logger = logging.getLogger('monday_logger')
            self.monday_util = monday_util
            self.monday_api = monday_api  # All raw API calls are delegated to MondayAPI.
            self.api_token = Config.MONDAY_API_TOKEN
            self.board_id = self.monday_util.PO_BOARD_ID
            self.subitem_board_id = self.monday_util.SUBITEM_BOARD_ID
            self.contact_board_id = self.monday_util.CONTACT_BOARD_ID
            # Queues for buffered upserts:
            self.contact_upsert_queue = []
            self._detail_upsert_queue = []
            self._po_upsert_queue = []
            self.db_ops = DatabaseOperations()
            self.logger.info('🌐 [MondayService __init__] - Monday Service initialized 🎉')
            self._initialized = True
    # endregion

    # region 2.2: High-Level PO Methods
    def upsert_po_in_monday(self, po_record: dict):
        """
        Upserts a Purchase Order (PO) in Monday.com. This method checks for an existing PO
        (using project_number and po_number) and then either updates the existing Monday item
        or creates a new one.
        """
        self.logger.info(f"🌐 Processing PO record:\n{po_record}")
        project_number = po_record.get('project_number')
        po_number = po_record.get('po_number')
        pulse_id = po_record.get('pulse_id')
        contact_name = po_record.get("vendor_name")

        # Build column values using monday_util's formatter.
        column_values = json.dumps(
            json.loads(
                self.monday_util.po_column_values_formatter(
                    project_id=project_number,
                    po_number=po_number,
                    tax_id=po_record.get("tax_id"),
                    description=po_record.get("description"),
                    contact_pulse_id=po_record.get("contact_id"),
                    folder_link=po_record.get("folder_link"),
                    status=po_record.get("status"),
                    producer_id=po_record.get("producer_id")
                )
            )
        )

        if not pulse_id:
            self.logger.info("🔎 No pulse_id; searching for existing PO...")
            existing = self.db_ops.search_purchase_order_by_keys(project_number, po_number)
            if not existing:
                self.logger.info("🆕 No existing PO found; creating new PO.")
                new_po = self.db_ops.create_purchase_order_by_keys(
                    project_number=project_number,
                    po_number=po_number,
                    session=None,
                    description=po_record.get("description"),
                    vendor_name=contact_name,
                    po_type=po_record.get("po_type"),
                    producer=po_record.get("producer"),
                    folder_link=po_record.get("folder_link"),
                    contact_id=po_record.get("contact_id")
                )
                if new_po:
                    po_record['pulse_id'] = new_po.get("pulse_id")  # Assuming your DB record stores the Monday pulse_id
                    self.logger.info(f"🎉 Created PO with DB ID {new_po['id']}")
                else:
                    self.logger.error("❌ PO creation failed.")
                    return
                # Now upsert to Monday.
                self.monday_api.create_item(
                    board_id=self.board_id,
                    group_id='topics',
                    name=contact_name or f"PO#{po_number}",
                    column_values=column_values
                )
            else:
                self.logger.info("🔄 Existing PO found; updating PO.")
                self.db_ops.update_purchase_order_by_keys(project_number, po_number, session=None,
                                                          description=po_record.get("description"),
                                                          vendor_name=contact_name,
                                                          po_type=po_record.get("po_type"),
                                                          producer=po_record.get("producer"),
                                                          folder_link=po_record.get("folder_link"),
                                                          contact_id=po_record.get("contact_id"))
                # Upsert to Monday.
                self.monday_api.update_item(existing.get("pulse_id"), column_values, type='main')
        else:
            self.logger.info(f"ℹ️ Pulse_id {pulse_id} exists; updating Monday item.")
            self.monday_api.update_item(pulse_id, column_values, type='main')
        self.logger.info("✅ PO upsert complete.")

    def sync_main_items_from_monday_board(self):
        """
        Syncs main PO items from Monday.com into the local DB using existing DatabaseOperations methods.
        """
        self.logger.info(f"📥 [sync_main_items_from_monday_board] - Fetching items from board {self.board_id}...")
        try:
            all_items = self.monday_api.fetch_all_items(self.board_id)
            self.logger.info(f"🗂 [sync_main_items_from_monday_board] - Fetched {len(all_items)} items.")
            for item in all_items:
                # Assume monday_util.get_po_number_and_data extracts a tuple (po_number, additional_data)
                po_number, extra_data = self.monday_util.get_po_number_and_data(item)
                project_number = extra_data.get("project_number")
                if not project_number or not po_number:
                    self.logger.warning("Missing project_number or po_number; skipping item.")
                    continue
                # Use existing methods to search, create, or update the PO record.
                existing = self.db_ops.search_purchase_order_by_keys(project_number, po_number)
                if not existing:
                    self.logger.info(f"Creating new PO for project {project_number}, PO #{po_number}.")
                    new_po = self.db_ops.create_purchase_order_by_keys(
                        project_number=project_number,
                        po_number=po_number,
                        session=None,
                        description=extra_data.get("description"),
                        vendor_name=extra_data.get("vendor_name")
                    )
                    if new_po:
                        self.logger.info(f"🎉 Created new PO with ID {new_po['id']}.")
                    else:
                        self.logger.warning("❌ PO creation failed.")
                else:
                    self.logger.info(f"Updating existing PO for project {project_number}, PO #{po_number}.")
                    self.db_ops.update_purchase_order_by_keys(
                        project_number, po_number, session=None,
                        description=extra_data.get("description"),
                        vendor_name=extra_data.get("vendor_name")
                    )
            self.logger.info("[sync_main_items_from_monday_board] - PO sync complete.")
        except Exception as e:
            self.logger.exception(f"❌ [sync_main_items_from_monday_board] - Error: {e}")
    # endregion

    # region 2.3: Upsert Detail Subitem Methods
    # region 2.3: Upsert Detail Subitem Methods
    def upsert_detail_subitem_in_monday(self, detail_item: dict):
        """
        Upserts a detail item as a Monday subitem.
        """
        self.logger.info(f"🧱 [upsert_detail_subitem_in_monday] - Processing detail item:\n{detail_item}")
        parent_id = detail_item.get('parent_pulse_id')
        if not parent_id:
            self.logger.warning("⚠️ [upsert_detail_subitem_in_monday] - Missing parent_pulse_id; cannot proceed.")
            return

        column_values = self.build_subitem_column_values(detail_item)
        column_values_json = json.dumps(column_values)
        subitem_id = detail_item.get('pulse_id')

        if subitem_id:
            self.logger.info(f"🔄 [upsert_detail_subitem_in_monday] - Updating subitem {subitem_id}")
            self.monday_api.update_item(subitem_id, column_values_json, type='subitem')
        else:
            self.logger.info("🔎 [upsert_detail_subitem_in_monday] - Creating new subitem.")
            create_resp = self.monday_api.create_subitem(
                parent_item_id=parent_id,
                subitem_name=detail_item.get('description') or f"Line {detail_item.get('line_number')}",
                column_values=column_values
            )
            if create_resp and create_resp.get('data', {}).get('create_subitem', {}).get('id'):
                new_id = create_resp['data']['create_subitem']['id']
                detail_item['pulse_id'] = new_id
                self.logger.info(f"🎉 [upsert_detail_subitem_in_monday] - Created subitem with ID {new_id}")
            else:
                self.logger.warning("❌ [upsert_detail_subitem_in_monday] - Failed to create subitem; no ID returned.")
        self.logger.info("🏁 [upsert_detail_subitem_in_monday] - Detail subitem upsert complete.")

    def execute_batch_upsert_detail_items(self):
        """
        Batch upserts detail subitems in Monday.
        This method separates detail items in the _detail_upsert_queue into two groups:
          - Items with a pulse_id are updated.
          - Items without a pulse_id are created.
        It then processes both batches and returns the consolidated results.
        """
        self.logger.info("🌀 Processing batch subitem upserts...")
        if not self._detail_upsert_queue:
            self.logger.info("🌀 No detail items queued for upsert.")
            return []

        items_to_create = []
        items_to_update = []

        # Separate items based on pulse_id.
        for di in self._detail_upsert_queue:
            parent_id = di.get('parent_pulse_id')
            col_vals = self.build_subitem_column_values(di)
            if di.get('pulse_id'):
                items_to_update.append({
                    'db_sub_item': di,
                    'column_values': col_vals,
                    'parent_id': parent_id,
                    'monday_item_id': di.get('pulse_id')
                })
            else:
                items_to_create.append({
                    'db_sub_item': di,
                    'column_values': col_vals,
                    'parent_id': parent_id
                })

        self.logger.info(f"🌀 Items to create: {len(items_to_create)}; items to update: {len(items_to_update)}")

        monday_results = []
        # Process creations.
        if items_to_create:
            created_results = self.monday_api.batch_create_or_update_subitems(
                subitems_batch=items_to_create,
                create=True
            )
            monday_results.extend(created_results)

        # Process updates.
        if items_to_update:
            updated_results = self.monday_api.batch_create_or_update_subitems(
                subitems_batch=items_to_update,
                create=False
            )
            monday_results.extend(updated_results)

        results = []
        # Build a mapping for matching responses to the original detail items.
        detail_mapping = {}
        for item in items_to_create + items_to_update:
            db_item = item['db_sub_item']
            key = (
                str(db_item.get('project_number', '')).strip(),
                str(db_item.get('po_number', '')).strip(),
                str(db_item.get('detail_number', '')).strip(),
                str(db_item.get('line_number', '')).strip(),
            )
            detail_mapping[key] = db_item

        # Process each Monday API response.
        def flatten(item_):
            if isinstance(item_, dict):
                yield item_
            elif isinstance(item_, list):
                for sub in item_:
                    yield from flatten(sub)

        for response in monday_results:
            for sub_response in flatten(response):
                try:
                    col_vals = sub_response.get("column_values", {})
                    if isinstance(col_vals, list):
                        col_vals = {col.get("id"): col for col in col_vals if "id" in col}
                    project_id = str(
                        col_vals.get(self.monday_util.SUBITEM_PROJECT_ID_COLUMN_ID, {}).get("text", "")).strip()
                    po_number = str(col_vals.get(self.monday_util.SUBITEM_PO_COLUMN_ID, {}).get("text", "")).strip()
                    detail_number = str(
                        col_vals.get(self.monday_util.SUBITEM_DETAIL_NUMBER_COLUMN_ID, {}).get("text", "")).strip()
                    line_number = str(
                        col_vals.get(self.monday_util.SUBITEM_LINE_NUMBER_COLUMN_ID, {}).get("text", "")).strip()
                except Exception as e:
                    self.logger.error(f"Error extracting identifiers from Monday response: {e}")
                    continue

                key = (project_id, po_number, detail_number, line_number)
                if key in detail_mapping:
                    results.append({
                        'db_sub_item': detail_mapping[key],
                        'monday_item': sub_response
                    })
                else:
                    self.logger.warning(f"No matching detail item found for Monday response with key {key}")

        self.logger.info(f"🌀 Processed {len(results)} subitems.")
        self._detail_upsert_queue.clear()
        return results

    def buffered_upsert_detail_item(self, detail_item: dict):
        """
        Stages a detail item for later batch upsert to Monday.com.
        """
        self.logger.debug(f"Buffering detail item for Monday upsert: {detail_item}")
        self._detail_upsert_queue.append(detail_item)

    # endregion


    # region 2.4: Contact Aggregator Methods
    def buffered_upsert_contact(self, contact_record: dict):
        """
        Stages a Contact record for upsert to Monday.
        """
        self.logger.info("🌀 Processing contact record for upsert...")
        if not contact_record:
            self.logger.warning("🌀 No contact record provided.")
            return

        contact_id = contact_record.get('id')
        pulse_id = contact_record.get('pulse_id')

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
            session=None
        )

        if not pulse_id or has_changes:
            self.logger.info("🆕 Enqueuing contact for upsert.")
            self.contact_upsert_queue.append(contact_record)
        else:
            self.logger.info("🌀 No changes detected; skipping upsert.")

    def execute_batch_upsert_contacts(self):
        """
        Processes all queued Contact upserts in a batch via MondayAPI.
        """
        self.logger.info("🌀 Starting batch contact upsert...")
        if not self.contact_upsert_queue:
            self.logger.info("🌀 No contacts queued for upsert.")
            return

        items_to_create = []
        items_to_update = []

        for ct in self.contact_upsert_queue:
            pulse_id = ct.get('pulse_id')
            col_vals = json.loads(self.monday_util.contact_column_values_formatter(
                email=ct.get("email"),
                phone=ct.get("phone"),
                address_line_1=ct.get("address_line_1"),
                address_line_2=ct.get("address_line_2"),
                city=ct.get("city"),
                zip=ct.get("zip"),
                region=ct.get("region"),
                country=ct.get("country"),
                tax_type=ct.get("tax_type"),
                tax_number=ct.get("tax_number"),
                payment_details=ct.get("payment_details"),
                vendor_status=ct.get("vendor_status"),
                tax_form_link=ct.get("tax_form_link")
            ))
            if not pulse_id:
                items_to_create.append({
                    'db_item': ct,
                    'column_values': col_vals,
                    'monday_item_id': None
                })
            else:
                items_to_update.append({
                    'db_item': ct,
                    'column_values': col_vals,
                    'monday_item_id': pulse_id
                })

        self.logger.info(f"🌀 Creating: {len(items_to_create)}; Updating: {len(items_to_update)}")
        created_results = []
        updated_results = []
        if items_to_create:
            created_results = self.monday_api.batch_create_or_update_contacts(
                contacts_batch=items_to_create,
                create=True
            )
        if items_to_update:
            updated_results = self.monday_api.batch_create_or_update_contacts(
                contacts_batch=items_to_update,
                create=False
            )
        self.contact_upsert_queue.clear()
        total = len(created_results) + len(updated_results)
        self.logger.info(f"🌀 Upserted {total} contacts.")
    # endregion

    # region 2.11: Purchase Order Aggregator Methods
    def buffered_upsert_po(self, po_record: dict, db_record: dict = None):
        """
        Stages a Purchase Order for eventual upsert to Monday.
        Enqueues the PO record if no pulse_id exists or if changes are detected.
        If a pre-fetched db_record is provided, it uses that for change detection.
        """
        self.logger.info("🌀Staging PO for upsert...")
        if not po_record:
            self.logger.warning("🌀No PO record provided; skipping.")
            return

        pulse_id = po_record.get('pulse_id')

        # If we already have the DB record, perform in-memory comparison.
        if db_record:
            # Compare only the relevant fields.
            has_changes = self.has_diff(db_record, po_record)
        else:
            # Fall back to the existing method that performs a DB call.
            has_changes = self.db_ops.purchase_order_has_changes(
                record_id=po_record.get('id'),
                project_number=po_record.get('project_number'),
                po_number=po_record.get('po_number'),
                description=po_record.get('description'),
                folder_link=po_record.get('folder_link')
            )

        if not pulse_id or has_changes:
            self.logger.info("🆕 Enqueuing PO for upsert.")
            self._po_upsert_queue.append(po_record)
        else:
            self.logger.info("🌀 No changes detected; skipping upsert.")

    # endregion

    # region 2.11.1: Execute Batch Upsert for POs
    def execute_batch_upsert_pos(self, provided_contacts=None):
        """
        Processes all buffered Purchase Order upserts in Monday.
        Accepts a list of "merged contacts", each having:
          - project_number, po_number
          - pulse_id (if known)
        So we can line up each PO's contact_id with the correct contact's pulse_id.
        """
        self.logger.info("🌀 Starting batch upsert of PO records.")
        if not self._po_upsert_queue:
            self.logger.info("🌀 No PO records to upsert.")
            return []

        # Build dict => key = (project_number, po_number), val = contact dict
        contact_map = {}
        if provided_contacts:
            self.logger.info(f"🔎 Received {len(provided_contacts)} provided contacts for matching.")
            for c in provided_contacts:
                pno = c.get("project_number")
                pono = c.get("po_number")
                if pno is not None and pono is not None:
                    key = (int(pno), int(pono))
                    contact_map[key] = c
        else:
            self.logger.info("🌀 No provided contacts; skipping contact->PO matching logic.")

        items_to_create = []
        items_to_update = []

        # Loop over each PO in queue
        for po in self._po_upsert_queue:
            pulse_id = po.get('pulse_id')
            project_no = po.get('project_number')
            po_no = po.get('po_number')

            # Try to find a matching contact from (project_no, po_no)
            contact_pulse_id = None
            if (project_no is not None) and (po_no is not None):
                match_key = (int(project_no), int(po_no))
                if match_key in contact_map:
                    # The merged contact dict has the DB contact's pulse_id
                    contact_obj = contact_map[match_key]
                    contact_pulse_id = contact_obj.get("pulse_id")

            if not pulse_id:
                items_to_create.append({
                    'db_item': po,
                    'monday_item_id': None,
                    'monday_contact_id': contact_pulse_id
                })
            else:
                items_to_update.append({
                    'db_item': po,
                    'monday_item_id': pulse_id,
                    'monday_contact_id': contact_pulse_id
                })

        self.logger.info(
            f"🌀 Preparing to create {len(items_to_create)} items and update {len(items_to_update)} items."
        )

        created_results = []
        updated_results = []

        # For grouping in Monday, pick a project from the create batch, if any
        project_id = items_to_create[0]['db_item'].get('project_number') if items_to_create else None

        if items_to_create:
            created_results = self.monday_api.batch_create_or_update_items(
                batch=items_to_create,
                project_id=project_id or "Unknown",
                create=True
            )

        if items_to_update:
            updated_results = self.monday_api.batch_create_or_update_items(
                batch=items_to_update,
                project_id=project_id or "Unknown",
                create=False
            )

        total_processed = len(created_results) + len(updated_results)
        self.logger.info(f"🌀 Processed {total_processed} PO records.")
        self._po_upsert_queue.clear()

        # Return the newly created results if you need them
        return created_results

    # endregion

    # region 3.7: Build Subitem Column Values
    def build_subitem_column_values(self, detail_item: dict) -> dict:
        """
        Constructs column values for a detail item subitem using monday_util's formatter.
        Retrieves the appropriate link from the DB based on payment type:
          - For 'CC' or 'PC', fetch the spend money link.
          - For 'INV' or 'PROJ', fetch the xero bill link.
        """
        formatted = self.monday_util.subitem_column_values_formatter(
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
            ot=detail_item.get('ot'),
            fringes=detail_item.get('fringes'),
            xero_link=detail_item.get('xero_link'),
            status=detail_item.get('state')
        )
        return json.loads(formatted)
    # endregion

    def has_diff(self, dict1: dict, dict2: dict) -> bool:
        """
        Compares two dictionaries and returns True if any of the fields
        that are present in both dictionaries have different values.

        Only the common keys between the two dictionaries are compared.

        Args:
            dict1 (dict): The first dictionary.
            dict2 (dict): The second dictionary.

        Returns:
            bool: True if a difference is found among the common keys, False otherwise.
        """
        # Find the intersection of keys from both dictionaries.
        common_keys = set(dict1.keys()) & set(dict2.keys())

        # Check for any differences in the values of common keys.
        for key in common_keys:
            if dict1[key] != dict2[key]:
                return True
        return False

# endregion

# region 3: Instantiate MondayService
monday_service = MondayService()
# endregion