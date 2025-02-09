import logging
from typing import Dict, Any
from utilities.config import Config
from database.database_util import DatabaseOperations
from db_util import initialize_database
from codegen_service import (
    apply_sql_script,
    update_sqlalchemy_models,
    update_database_util_files,
    update_database_trigger_file,
    update_celery_tasks_file
)


class ChatService:
    """
    Encapsulates all DB and code actions, plus fallback for best-practices queries and data insights.
    """

    def __init__(self):
        self.logger = logging.getLogger('agent_logger')
        self.db_ops = DatabaseOperations()
        # Initialize the database here so that SessionLocal is set before any DB operations.
        config = Config()
        db_settings = config.get_database_settings(config.USE_LOCAL)
        initialize_database(db_settings['url'])
        self.logger.debug('Database initialized in ChatService constructor.')

    def create_table(self, table_name: str, columns_or_defs: Any):
        """
        Creates a new table in the DB with the provided columns, then updates code references.
        """
        self.logger.info(f'[create_table] - create_table -> {table_name}, columns={columns_or_defs}')
        if isinstance(columns_or_defs, list):
            create_sql = self._build_create_table_sql(table_name, columns_or_defs)
        else:
            col_list = columns_or_defs.get('col_defs', [])
            create_sql = self._build_create_table_sql(table_name, col_list)
        apply_sql_script(create_sql)
        update_sqlalchemy_models(table_name, columns_or_defs, create=True)
        update_database_util_files(table_name, create=True)
        update_database_trigger_file(table_name, create=True)
        update_celery_tasks_file(table_name, create=True)
        self.logger.info(f"[create_table] - SysTable '{table_name}' created successfully in DB + code.")

    def update_table(self, table_name: str, updates: Dict[str, Any]):
        """
        Updates columns (add/drop) or other aspects, then updates references in code.
        """
        self.logger.info(f'[update_table] - update_table -> {table_name}, updates={updates}')
        alter_sql = self._build_alter_sql(table_name, updates)
        apply_sql_script(alter_sql)
        update_sqlalchemy_models(table_name, updates, create=False)
        update_database_util_files(table_name, create=False)
        update_database_trigger_file(table_name, create=False)
        update_celery_tasks_file(table_name, create=False)
        self.logger.info(f"[update_table] - SysTable '{table_name}' updated successfully (DB + code).")

    def delete_table(self, table_name: str):
        self.logger.info(f'[delete_table] - delete_table -> {table_name}')
        sql = f'DROP TABLE IF EXISTS `{table_name}`;'
        apply_sql_script(sql)
        update_sqlalchemy_models(table_name, None, delete=True)
        update_database_util_files(table_name, delete=True)
        update_database_trigger_file(table_name, delete=True)
        update_celery_tasks_file(table_name, delete=True)
        self.logger.info(f"[delete_table] - SysTable '{table_name}' dropped (DB + code).")

    def insert_data(self, table_name: str, data: Dict[str, Any]):
        self.logger.info(f'[insert_data] - insert_data -> {table_name}, data={data}')
        model_cls = self._resolve_model(table_name)
        if not model_cls:
            self.logger.warning(f'[insert_data] - No model found for {table_name}, insert aborted.')
            return
        self.db_ops._create_record(model_cls, unique_lookup=None, **data)
        self.logger.info(f'[insert_data] - Data inserted into {table_name}.')

    def update_data(self, table_name: str, updates: Dict[str, Any], where: Dict[str, Any]):
        self.logger.info(f'[update_data] - update_data -> {table_name}, updates={updates}, where={where}')
        model_cls = self._resolve_model(table_name)
        if not model_cls:
            self.logger.warning(f'[update_data] - No model found for {table_name}, update aborted.')
            return
        record_id = where.get('id')
        if record_id is not None:
            self.db_ops._update_record(model_cls, record_id, **updates)
        else:
            found = self.db_ops._search_records(model_cls, list(where.keys()), list(where.values()))
            if not found:
                self.logger.warning('[update_data] - No matching records. Skipping update.')
                return
            if isinstance(found, list):
                first = found[0]
                rid = first['id']
                self.db_ops._update_record(model_cls, rid, **updates)
            else:
                rid = found['id']
                self.db_ops._update_record(model_cls, rid, **updates)
        self.logger.info('[update_data] - Update successful.')

    def query_data(self, table_name: str, filters: Dict[str, Any]):
        self.logger.info(f'[query_data] - query_data -> {table_name}, filters={filters}')
        model_cls = self._resolve_model(table_name)
        if not model_cls:
            self.logger.warning(f'[query_data] - No model found for {table_name}, query aborted.')
            return []
        return self.db_ops._search_records(model_cls, list(filters.keys()), list(filters.values()))

    def answer_general_question(self, user_input: str):
        self.logger.info(f'[answer_general_question] - answer_general_question -> {user_input}')
        print("AI Agent: You asked for best practices or general advice. I'm here to help!")

    def describe_data(self, table_name: str) -> str:
        """
        Provides metadata and basic statistics about the specified table.
        Returns a formatted string with details.
        """
        self.logger.info(f'[describe_data] - describe_data -> {table_name}')
        try:
            # Query metadata using MySQL SHOW COLUMNS
            metadata_query = f"SHOW COLUMNS FROM `{table_name}`;"
            metadata_results = self.db_ops._execute_query(metadata_query)
            # Query basic statistics: total row count
            stats_query = f"SELECT COUNT(*) as total_rows FROM `{table_name}`;"
            stats_results = self.db_ops._execute_query(stats_query)
        except Exception as e:
            self.logger.error(f'[describe_data] - Error retrieving description for table {table_name}: {e}',
                              exc_info=True)
            return f"Error retrieving description for table {table_name}: {e}"

        result_text = f"Table: {table_name}\nMetadata:\n"
        for row in metadata_results:
            field = row.get('Field', 'N/A')
            type_ = row.get('Type', 'N/A')
            null = row.get('Null', 'N/A')
            key = row.get('Key', 'N/A')
            default = row.get('Default', 'N/A')
            extra = row.get('Extra', 'N/A')
            result_text += f" - {field}: {type_} (Null: {null}, Key: {key}, Default: {default}, Extra: {extra})\n"

        total_rows = stats_results[0].get('total_rows', 'N/A') if stats_results else 'N/A'
        result_text += f"\nStatistics:\n - Total Rows: {total_rows}\n"
        self.logger.info(f'[describe_data] - Completed describing data for {table_name}')
        return result_text

    def _resolve_model(self, table_name: str):
        """
        Maps table_name to the actual SQLAlchemy model.
        """
        from database.models import (
            Project, Contact, PurchaseOrder, DetailItem, Invoice, AuditLog, XeroBill,
            BankTransaction, AccountCode, TaxAccount, Receipt, SpendMoney, User, TaxLedger, BudgetMap
        )
        table_map = {
            'project': Project,
            'contact': Contact,
            'purchase_order': PurchaseOrder,
            'detail_item': DetailItem,
            'invoice': Invoice,
            'audit_log': AuditLog,
            'xero_bill': XeroBill,
            'bank_transaction': BankTransaction,
            'account_code': AccountCode,
            'tax_account': TaxAccount,
            'receipt': Receipt,
            'spend_money': SpendMoney,
            'users': User,
            'tax_ledger': TaxLedger,
            'budget_map': BudgetMap
        }
        return table_map.get(table_name.lower())

    def _build_create_table_sql(self, table_name: str, columns: list) -> str:
        col_defs = ',\n  '.join(columns)
        return f'\n        CREATE TABLE IF NOT EXISTS `{table_name}` (\n          {col_defs}\n        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;'.strip()

    def _build_alter_sql(self, table_name: str, updates: Dict[str, Any]) -> str:
        """
        Builds an ALTER TABLE SQL command for the given updates.
        """
        stmts = []
        add_cols = updates.get('add_columns', [])
        drop_cols = updates.get('drop_columns', [])
        for ac in add_cols:
            if 'FOREIGN KEY' in ac.upper():
                stmts.append(f'ADD {ac}')
            else:
                stmts.append(f'ADD COLUMN {ac}')
        for dc in drop_cols:
            stmts.append(f'DROP COLUMN {dc}')
        if stmts:
            return f"ALTER TABLE `{table_name}`\n  {',  '.join(stmts)};"
        return f'-- No changes for table {table_name}'
