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
        self.logger.info(f"🌐 [upsert_po_in_monday] - Processing PO record:\n{po_record}")
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
            self.logger.info("🔎 [upsert_po_in_monday] - No pulse_id; searching for existing PO...")
            existing = self.db_ops.search_purchase_order_by_keys(project_number, po_number)
            if not existing:
                self.logger.info("🆕 [upsert_po_in_monday] - No existing PO found; creating new PO.")
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
                    self.logger.info(f"🎉 [upsert_po_in_monday] - Created PO with DB ID {new_po['id']}")
                else:
                    self.logger.error("❌ [upsert_po_in_monday] - PO creation failed.")
                    return
                # Now upsert to Monday.
                self.monday_api.create_item(
                    board_id=self.board_id,
                    group_id='topics',
                    name=contact_name or f"PO#{po_number}",
                    column_values=column_values
                )
            else:
                self.logger.info("🔄 [upsert_po_in_monday] - Existing PO found; updating PO.")
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
            self.logger.info(f"ℹ️ [upsert_po_in_monday] - Pulse_id {pulse_id} exists; updating Monday item.")
            self.monday_api.update_item(pulse_id, column_values, type='main')
        self.logger.info("✅ [upsert_po_in_monday] - PO upsert complete.")

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
    def upsert_detail_subitem_in_monday(self, detail_item: dict):
        """
        Upserts a detail item as a Monday subitem.
        """
        self.logger.info(f"🧱 [upsert_detail_subitem_in_monday] - Processing detail item:\n{detail_item}")
        parent_id = detail_item.get('parent_pulse_id')
        if not parent_id:
            self.logger.warning("⚠️ [upsert_detail_subitem_in_monday] - Missing parent_pulse_id; cannot proceed.")
            return

        column_values = self.monday_api.build_subitem_column_values(detail_item)
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
        Returns a tuple: (created_results, updated_results)
        """
        self.logger.info("🌀 [execute_batch_upsert_detail_items] - Processing batch subitem upserts...")
        if not self._detail_upsert_queue:
            self.logger.info("🌀 No detail items queued for upsert.")
            return

        items_to_create = []
        items_to_update = []

        for di in self._detail_upsert_queue:
            subitem_id = di.get('pulse_id')
            parent_id = di.get('parent_pulse_id')
            col_vals = self.monday_api.build_subitem_column_values(di)
            if not subitem_id:
                items_to_create.append({
                    'db_sub_item': di,
                    'column_values': col_vals,
                    'parent_id': parent_id
                })
            else:
                items_to_update.append({
                    'db_sub_item': di,
                    'column_values': col_vals,
                    'parent_id': parent_id,
                    'monday_item_id': subitem_id
                })

        self.logger.info(f"🌀 [execute_batch_upsert_detail_items] - Creating: {len(items_to_create)}; Updating: {len(items_to_update)}")
        created_results = []
        updated_results = []
        if items_to_create:
            created_results = self.monday_api.batch_create_or_update_subitems(
                subitems_batch=items_to_create,
                create=True
            )
        if items_to_update:
            updated_results = self.monday_api.batch_create_or_update_subitems(
                subitems_batch=items_to_update,
                create=False
            )
        self._detail_upsert_queue.clear()
        total = len(created_results) + len(updated_results)
        self.logger.info(f"🌀 [execute_batch_upsert_detail_items] - Processed {total} subitems.")
        return created_results, updated_results

    def buffered_upsert_detail_item(self, detail_item: dict):
        """
        Stages a detail item for later batch upsert to Monday.com.

        This method appends the provided detail item to the internal queue (_detail_upsert_queue)
        which will be processed in batch via execute_batch_upsert_detail_items().
        """
        self.logger.debug(f"Buffering detail item for Monday upsert: {detail_item}")
        self._detail_upsert_queue.append(detail_item)

    # endregion

    # region 2.4: Contact Aggregator Methods
    def buffered_upsert_contact(self, contact_record: dict):
        """
        Stages a Contact record for upsert to Monday.
        """
        self.logger.info("🌀 [buffered_upsert_contact] - Processing contact record for upsert...")
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
            self.logger.info("🆕 [buffered_upsert_contact] - Enqueuing contact for upsert.")
            self.contact_upsert_queue.append(contact_record)
        else:
            self.logger.info("🌀 [buffered_upsert_contact] - No changes detected; skipping upsert.")

    def execute_batch_upsert_contacts(self):
        """
        Processes all queued Contact upserts in a batch via MondayAPI.
        """
        self.logger.info("🌀 [execute_batch_upsert_contacts] - Starting batch contact upsert...")
        if not self.contact_upsert_queue:
            self.logger.info("🌀 No contacts queued for upsert.")
            return

        items_to_create = []
        items_to_update = []

        for ct in self.contact_upsert_queue:
            pulse_id = ct.get('pulse_id')
            col_vals = json.loads(self.monday_util.contact_column_values_formatter(
                name=ct.get("name"),
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

        self.logger.info(f"🌀 [execute_batch_upsert_contacts] - Creating: {len(items_to_create)}; Updating: {len(items_to_update)}")
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
        self.logger.info(f"🌀 [execute_batch_upsert_contacts] - Upserted {total} contacts.")
    # endregion

    # region 2.11: Purchase Order Aggregator Methods
    def buffered_upsert_po(self, po_record: dict):
        """
        Stages a Purchase Order for eventual upsert to Monday.
        Enqueues the PO record if no pulse_id exists or if changes are detected.
        """
        self.logger.info("🌀 [buffered_upsert_po] - Staging PO for upsert...")
        if not po_record:
            self.logger.warning("🌀 [buffered_upsert_po] - No PO record provided; skipping.")
            return

        pulse_id = po_record.get('pulse_id')
        # Check for changes using the DatabaseOperations method.
        has_changes = self.db_ops.purchase_order_has_changes(
            record_id=po_record.get('id'),
            project_number=po_record.get('project_number'),
            po_number=po_record.get('po_number'),
            description=po_record.get('description'),
            folder_link=po_record.get('folder_link')
        )
        if not pulse_id or has_changes:
            self.logger.info("🆕 [buffered_upsert_po] - Enqueuing PO for upsert.")
            self._po_upsert_queue.append(po_record)
        else:
            self.logger.info("🌀 [buffered_upsert_po] - No changes detected; skipping upsert.")

    # endregion

    # region 2.11.1: Execute Batch Upsert for POs
    def execute_batch_upsert_pos(self):
        """
        Processes all buffered Purchase Order upserts in Monday.
        It takes the PO records in the _po_upsert_queue and uses MondayAPI's
        batch mutation method to create or update them. After processing, it clears the queue.

        Returns:
            list: A list of created (or updated) PO results from Monday.
        """
        self.logger.info("🌀 [execute_batch_upsert_pos] - Starting batch upsert of PO records.")
        if not self._po_upsert_queue:
            self.logger.info("🌀 [execute_batch_upsert_pos] - No PO records to upsert.")
            return []

        items_to_create = []
        items_to_update = []

        # For each PO record, determine if it needs creation or update.
        for po in self._po_upsert_queue:
            pulse_id = po.get('pulse_id')
            contact_record = self.db_ops.search_contacts(["id"], [po.get("contact_id")])
            if isinstance(contact_record, list) and contact_record:
                contact_record = contact_record[0]
            contact_pulse_id = contact_record.get("pulse_id") if contact_record else None

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
            f"🌀 [execute_batch_upsert_pos] - Preparing to create {len(items_to_create)} items and update {len(items_to_update)} items.")
        created_results = []
        updated_results = []
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
        self.logger.info(f"🌀 [execute_batch_upsert_pos] - Processed {total_processed} PO records.")
        self._po_upsert_queue.clear()
        return created_results
    # endregion

# endregion

# region 3: Instantiate MondayService
monday_service = MondayService()
# endregion