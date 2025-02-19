import logging
import os
import re
from typing import Optional, Dict, Any, List
from sqlalchemy import text
from sqlalchemy.dialects.mysql import INTEGER as MYSQL_INTEGER, DECIMAL as MYSQL_DECIMAL
from db_util import get_db_session
logger = logging.getLogger('agent_logger')

class AiLogCaptureHandler(logging.Handler):
    """
    A custom logging handler that stores all log records in-memory
    so your AI server_agent can retrieve and summarize them later
    (instead of printing them to console).
    """

    def __init__(self):
        super().__init__()
        self.records = []

    def emit(self, record: logging.LogRecord):
        self.records.append(self.format(record))

    def get_logs(self) -> List[str]:
        return self.records

    def clear_logs(self):
        self.records.clear()
_ai_log_handler = AiLogCaptureHandler()
_ai_log_handler.setLevel(logging.DEBUG)
formatter = logging.Formatter('[%(levelname)s] %(name)s - %(message)s')
_ai_log_handler.setFormatter(formatter)
logger.addHandler(_ai_log_handler)
logger.setLevel(logging.DEBUG)

def apply_sql_script(sql_script: str):
    """
    Execute multi-statement SQL script against the DB using the provided session factory.
    For foreign key constraints, ensure the referencing and referenced tables use same engine (InnoDB).
    """
    logger.info('[apply_sql_script] Running SQL:\n%s', sql_script)
    with get_db_session() as session:
        statements = sql_script.strip().split(';')
        for stmt in statements:
            s = stmt.strip()
            if s:
                try:
                    session.execute(text(s))
                except Exception as e:
                    logger.error(f"SQL error when executing statement '{s}': {e}", exc_info=True)
                    raise
        session.commit()

def update_sqlalchemy_models(table_name: str, columns_or_updates: Any, create=False, delete=False, models_file_path: str='../database/models_pg.py'):
    """
    - If create=True, insert a new class into models_pg.py (at bottom).
    - If delete=True, remove existing definition for that table.
    - Otherwise, do partial updates.
    """
    if not os.path.exists(models_file_path):
        logger.error('Models file not found: %s', models_file_path)
        return
    with open(models_file_path, 'r', encoding='utf-8') as f:
        content = f.read()
    class_name = _table_name_to_class_name(table_name)
    if create:
        class_pattern = re.compile(f'class\\s+{class_name}\\(Base\\)')
        if re.search(class_pattern, content):
            logger.warning("Class definition for table '%s' already exists. Skipping creation.", table_name)
            updated_content = content
        else:
            new_class = _build_sqlalchemy_class(table_name, columns_or_updates)
            updated_content = content.strip() + f'\n\n{new_class}\n'
            logger.info("Inserted new model class for table '%s' into %s", table_name, models_file_path)
    elif delete:
        updated_content = _remove_model_class(table_name, content)
    else:
        updated_content = _update_model_class(table_name, content, columns_or_updates)
    if updated_content != content:
        with open(models_file_path, 'w', encoding='utf-8') as wf:
            wf.write(updated_content)
        logger.info('Successfully updated models file: %s', models_file_path)
    else:
        logger.info("No changes made to models file for table '%s'.", table_name)

def _build_sqlalchemy_class(table_name: str, columns: Any) -> str:
    class_name = _table_name_to_class_name(table_name)
    lines = []
    if isinstance(columns, list):
        for col in columns:
            lines.append(_parse_column_def(col))
    elif isinstance(columns, dict):
        col_defs = columns.get('col_defs', [])
        for raw_col in col_defs:
            lines.append(_parse_column_def(raw_col))
    else:
        lines.append('# Add your column definitions here')
    col_defs = '\n    '.join(lines)
    return f"# region 🚧 {class_name.upper()} TABLE\nclass {class_name}(Base):\n    __tablename__ = '{table_name}'\n\n    id = Column(MYSQL_INTEGER(unsigned=True), primary_key=True, autoincrement=True)\n    {col_defs}\n\n    def to_dict(self):\n        return {{c.name: getattr(self, c.name) for c in self.__table__.columns}}\n# endregion\n".strip()

def _parse_column_def(column_string: str) -> str:
    tokens = column_string.strip().split(None, 1)
    col_name = tokens[0]
    remainder = tokens[1].upper() if len(tokens) > 1 else ''
    dec_match = re.search('DECIMAL\\s*\\(\\s*(\\d+)\\s*,\\s*(\\d+)\\s*\\)', remainder)
    if dec_match:
        precision = dec_match.group(1)
        scale = dec_match.group(2)
        return f'{col_name} = Column(MYSQL_DECIMAL({precision}, {scale}), nullable=True)'
    int_match = re.search('INT', remainder)
    if int_match:
        return f'{col_name} = Column(MYSQL_INTEGER(unsigned=True), nullable=True)'
    return f'{col_name} = Column(MYSQL_INTEGER(unsigned=True), nullable=True)'

def _remove_model_class(table_name: str, file_content: str) -> str:
    class_name = _table_name_to_class_name(table_name)
    region_pattern = f'# region.*?{class_name.upper()} TABLE.*?# endregion'
    updated = re.sub(region_pattern, '', file_content, flags=re.DOTALL)
    if updated == file_content:
        pattern = f'class\\s+{class_name}\\(Base\\).*?\\n# endregion'
        updated = re.sub(pattern, '', updated, flags=re.DOTALL)
    if updated == file_content:
        logger.warning('No matching class definition found for %s. Nothing removed.', table_name)
    else:
        logger.info('Removed model class (and region) for %s.', table_name)
    return updated

def _update_model_class(table_name: str, file_content: str, updates: Dict[str, Any]) -> str:
    class_name = _table_name_to_class_name(table_name)
    pattern = f'(class\\s+{class_name}\\(Base\\).*?)(\\nclass|\\Z)'
    matches = list(re.finditer(pattern, file_content, flags=re.DOTALL))
    if not matches:
        logger.warning("No existing model class found for %s. Can't update.", table_name)
        return file_content
    first_match = matches[0]
    class_block = first_match.group(1)
    note = '\n    # --- Updates below ---\n'
    add_cols = updates.get('add_columns', [])
    for col in add_cols:
        note += f'    # ADD COLUMN: {col}\n'
    drop_cols = updates.get('drop_columns', [])
    for col in drop_cols:
        note += f'    # DROP COLUMN: {col}\n'
    updated_class_block = class_block + note
    (start, end) = first_match.span(1)
    new_content = file_content[:start] + updated_class_block + file_content[end:]
    logger.info("Appended update notes to class '%s'.", class_name)
    return new_content

def _table_name_to_class_name(table_name: str) -> str:
    return ''.join((word.capitalize() for word in table_name.split('_')))

def update_database_util_files(table_name: str, create=False, delete=False, db_util_path: str='../database/database_util.py'):
    """
    If create=True, we append new CRUD stubs at bottom. If delete=True, remove them.
    """
    if not os.path.exists(db_util_path):
        logger.warning('database_util.py not found at %s', db_util_path)
        return
    with open(db_util_path, 'r', encoding='utf-8') as f:
        content = f.read()
    if create:
        region_re = f'# region.*?{table_name}.*?# endregion'
        if re.search(region_re, content, flags=re.DOTALL):
            logger.warning("CRUD region for '%s' already exists. Skipping creation.", table_name)
            updated_content = content
        else:
            crud_pattern = f'def\\s+search_{table_name}\\(.*?\\)|def\\s+create_{table_name}\\(.*?\\)|def\\s+update_{table_name}\\(.*?\\)'
            if re.search(crud_pattern, content, flags=re.DOTALL):
                logger.warning("CRUD methods for '%s' already exist. Skipping creation.", table_name)
                updated_content = content
            else:
                new_methods = _build_crud_methods(table_name)
                updated_content = content.strip() + f'\n\n{new_methods}\n'
                logger.info("Added new CRUD methods for table '%s' to database_util.py", table_name)
    elif delete:
        updated_content = _remove_crud_methods(table_name, content)
    else:
        updated_content = content
    if updated_content != content:
        with open(db_util_path, 'w', encoding='utf-8') as wf:
            wf.write(updated_content)
        logger.info("Successfully updated database_util.py for table '%s'.", table_name)
    else:
        logger.info("No changes made to database_util.py for table '%s'.", table_name)

def _build_crud_methods(table_name: str) -> str:
    class_name = _table_name_to_class_name(table_name)
    return f'\n    # region 🧪 {table_name}\n    def search_{table_name}(self, column_names=None, values=None):\n        """\n        Search {table_name} records.\n        """\n        from database.models import {class_name}\n        return self._search_records({class_name}, column_names, values)\n\n    def create_{table_name}(self, **kwargs):\n        """\n        Create a new {table_name} record.\n        """\n        from database.models import {class_name}\n        unique_lookup = {{}}\n        return self._create_record({class_name}, unique_lookup=unique_lookup, **kwargs)\n\n    def update_{table_name}(self, record_id, **kwargs):\n        """\n        Update an existing {table_name} record by ID.\n        """\n        from database.models import {class_name}\n        return self._update_record({class_name}, record_id, **kwargs)\n# endregion\n'.strip()

def _remove_crud_methods(table_name: str, content: str) -> str:
    region_pattern = f'# region.*?{table_name}.*?# endregion'
    updated = re.sub(region_pattern, '', content, flags=re.DOTALL)
    if updated != content:
        logger.info("Removed region for '%s' from database_util.py", table_name)
        return updated
    pattern = f'def\\s+search_{table_name}\\(.*?\\n\\n|def\\s+create_{table_name}\\(.*?\\n\\n|def\\s+update_{table_name}\\(.*?\\n\\n'
    updated = re.sub(pattern, '', content, flags=re.DOTALL)
    if updated == content:
        logger.warning("No existing CRUD stubs found for '%s'. Nothing removed.", table_name)
    else:
        logger.info("Removed existing CRUD methods for '%s'.", table_name)
    return updated

def update_database_trigger_file(table_name: str, create=False, delete=False, triggers_file_path: str='../database_trigger.py'):
    """
    If create=True, add references to TASK_ROUTING or create it.
    Could also add an import for process_{table_name}_ tasks if needed.
    """
    if not os.path.exists(triggers_file_path):
        logger.warning('database_trigger.py not found at %s', triggers_file_path)
        return
    with open(triggers_file_path, 'r', encoding='utf-8') as f:
        content = f.read()
    updated_content = content
    updated_content = _add_import_for_celery_tasks(table_name, updated_content)
    if create:
        updated_content = _add_trigger_routing(table_name, updated_content)
    elif delete:
        updated_content = _remove_trigger_routing(table_name, updated_content)
    if updated_content != content:
        with open(triggers_file_path, 'w', encoding='utf-8') as wf:
            wf.write(updated_content)
        logger.info("database_trigger.py updated for table '%s'.", table_name)
    else:
        logger.info("No changes made to database_trigger.py for table '%s'.", table_name)

def _add_import_for_celery_tasks(table_name: str, content: str) -> str:
    import_pattern = '(from\\s+celery_tasks\\s+import\\s*\\([\\s\\S]*?\\))'
    match = re.search(import_pattern, content)
    new_imports = f'process_{table_name}_create, process_{table_name}_update, process_{table_name}_delete,'
    if match:
        original_block = match.group(1)
        if f'process_{table_name}_create' in original_block:
            logger.warning("Celery tasks import for '%s' already present. Skipping import addition.", table_name)
            return content
        insertion_point = original_block.rfind(')')
        updated_block = original_block[:insertion_point] + f'\n    {new_imports}\n' + original_block[insertion_point:]
        return content.replace(original_block, updated_block)
    else:
        lines = content.split('\n')
        new_import_block = f'from celery_tasks import (\n    {new_imports}\n)\n'
        insert_index = 0
        for (i, line) in enumerate(lines):
            if 'import logging' in line:
                insert_index = i + 1
                break
        lines.insert(insert_index, new_import_block)
        return '\n'.join(lines)

def _add_trigger_routing(table_name: str, content: str) -> str:
    routing_pattern = 'TASK_ROUTING\\s*=\\s*\\{\\s*\\(.*?\\}\\s*\\}'
    match = re.search(routing_pattern, content, flags=re.DOTALL)
    new_entries = f'    ("{table_name}", "INSERT"): lambda rid: process_{table_name}_create.delay(rid),\n    ("{table_name}", "UPDATE"): lambda rid: process_{table_name}_update.delay(rid),\n    ("{table_name}", "DELETE"): lambda rid: process_{table_name}_delete.delay(rid),'
    if match:
        original_block = match.group(0)
        insertion_point = original_block.rfind('}')
        exist_pat = f'\\("{table_name}",\\s*"INSERT"'
        if re.search(exist_pat, original_block):
            logger.warning("Routing for table '%s' already exists. Skipping creation.", table_name)
            return content
        updated_block = original_block[:insertion_point] + '\n' + new_entries + '\n' + original_block[insertion_point:]
        return content.replace(original_block, updated_block)
    else:
        logger.warning("No TASK_ROUTING dict found; creating one at bottom for table '%s'.", table_name)
        new_block = f'\n\nTASK_ROUTING = {{\n{new_entries}\n}}\n'
        return content.strip() + new_block

def _remove_trigger_routing(table_name: str, content: str) -> str:
    pattern = f'\\(\\s*"{table_name}"\\s*,\\s*"INSERT"\\s*\\).*?\\n|\\(\\s*"{table_name}"\\s*,\\s*"UPDATE"\\s*\\).*?\\n|\\(\\s*"{table_name}"\\s*,\\s*"DELETE"\\s*\\).*?\\n'
    updated = re.sub(pattern, '', content)
    if updated == content:
        logger.warning('No references found in database_trigger.py for %s. Nothing removed.', table_name)
    else:
        logger.info("Removed references for '%s' from database_trigger.py", table_name)
    return updated

def update_database_trigger_service_file(table_name: str, create=False, delete=False, trigger_service_path: str='../server_celery/celery_task_router.py'):
    """
    If create=True, add new <table_name>_trigger_on_create/update/delete at bottom.
    If delete=True, remove them.
    """
    if not os.path.exists(trigger_service_path):
        logger.warning('celery_task_router.py not found at %s', trigger_service_path)
        return
    with open(trigger_service_path, 'r', encoding='utf-8') as f:
        content = f.read()
    if create:
        updated_content = _add_trigger_service_methods(table_name, content)
    elif delete:
        updated_content = _remove_trigger_service_methods(table_name, content)
    else:
        updated_content = content
    if updated_content != content:
        with open(trigger_service_path, 'w', encoding='utf-8') as wf:
            wf.write(updated_content)
        logger.info("celery_task_router.py updated for table '%s'.", table_name)
    else:
        logger.info("No changes made to celery_task_router.py for '%s'.", table_name)

def _add_trigger_service_methods(table_name: str, content: str) -> str:
    region_pattern = f'# region.*?{table_name.upper()} TRIGGERS'
    if re.search(region_pattern, content, flags=re.DOTALL):
        logger.warning(f"Trigger region for '{table_name}' already exists. Skipping creation.")
        return content
    code_block = _build_trigger_service_methods(table_name)
    updated_content = content.strip() + f'\n\n{code_block}\n'
    logger.info("Added new trigger region for '%s' to celery_task_router.py", table_name)
    return updated_content

def _build_trigger_service_methods(table_name: str) -> str:
    capitalized = _table_name_to_class_name(table_name)
    return f'# region 🏷️ {table_name.upper()} TRIGGERS\ndef {table_name}_trigger_on_create(self, record_id: int):\n    self.logger.info(f"Handling newly created {capitalized} with record_id={{record_id}}.")\n    # Insert real logic here (e.g. checking references, foreign keys, etc.)\n\ndef {table_name}_trigger_on_update(self, record_id: int):\n    self.logger.info(f"Handling updated {capitalized} with record_id={{record_id}}.")\n    # Insert logic for handling updates\n\ndef {table_name}_trigger_on_delete(self, record_id: int):\n    self.logger.info(f"Handling deleted {capitalized} with record_id={{record_id}}.")\n    # Insert logic for cleaning up references\n# endregion\n'

def _remove_trigger_service_methods(table_name: str, content: str) -> str:
    region_pattern = f'# region.*?{table_name.upper()} TRIGGERS.*?# endregion'
    updated = re.sub(region_pattern, '', content, flags=re.DOTALL)
    if updated != content:
        logger.info("Removed region block for '%s' triggers from celery_task_router.py", table_name)
        return updated
    pattern = f'def\\s+{table_name}_trigger_on_create\\(.*?\\n\\ndef\\s+{table_name}_trigger_on_update\\(.*?\\n\\ndef\\s+{table_name}_trigger_on_delete\\(.*?\\n\\n'
    updated = re.sub(pattern, '', content, flags=re.DOTALL)
    if updated == content:
        logger.warning("No trigger methods found to remove for '%s' in celery_task_router.py.", table_name)
    else:
        logger.info("Removed trigger methods for '%s' from celery_task_router.py", table_name)
    return updated

def update_celery_tasks_file(table_name: str, create=False, delete=False, celery_file_path: str='../server_celery/celery_tasks.py'):
    """
    If create=True, add tasks at bottom. If delete=True, remove them.
    """
    if not os.path.exists(celery_file_path):
        logger.warning('celery_tasks.py not found at %s', celery_file_path)
        return
    with open(celery_file_path, 'r', encoding='utf-8') as f:
        content = f.read()
    if create:
        updated_content = _add_celery_tasks(table_name, content)
    elif delete:
        updated_content = _remove_celery_tasks(table_name, content)
    else:
        updated_content = content
    if updated_content != content:
        with open(celery_file_path, 'w', encoding='utf-8') as wf:
            wf.write(updated_content)
        logger.info("celery_tasks.py updated for table '%s'.", table_name)
    else:
        logger.info("No changes made to celery_tasks.py for table '%s'.", table_name)

def _add_celery_tasks(table_name: str, content: str) -> str:
    region_pattern = f'# region.*?{table_name.upper()} TASKS.*?# endregion'
    if re.search(region_pattern, content, flags=re.DOTALL):
        logger.warning("Celery tasks region for '%s' already exists. Skipping creation.", table_name)
        return content
    tasks_pattern = f'def\\s+process_{table_name}_create\\(.*?\\)|def\\s+process_{table_name}_update\\(.*?\\)|def\\s+process_{table_name}_delete\\(.*?\\)'
    if re.search(tasks_pattern, content, flags=re.DOTALL):
        logger.warning("Celery tasks for '%s' already exist. Skipping creation.", table_name)
        return content
    new_celery_tasks = _build_celery_tasks(table_name)
    updated_content = content.strip() + f'\n\n{new_celery_tasks}\n'
    return updated_content

def _build_celery_tasks(table_name: str) -> str:
    capitalized = _table_name_to_class_name(table_name)
    return f'''# region 🍔 {table_name.upper()} TASKS\n########################################\n# {table_name.upper()} TASKS\n########################################\n@shared_task\ndef process_{table_name}_create(record_id: int):\n    """\n    The Celery task for newly created {capitalized}.\n    """\n    try:\n        from utilities.config import Config\n        from db_util import initialize_database\n        from database_trigger_service import database_trigger_service\n        import logging\n\n        logger = logging.getLogger("admin_logger")\n        logger.info(f"🚀 Handling newly created {capitalized} with record_id={{record_id}}.")\n\n        config = Config()\n        db_settings = config.get_database_settings(config.USE_LOCAL)\n        initialize_database(db_settings['url'])\n        logger.info("DB initialization is done.")\n\n        database_trigger_service.{table_name}_trigger_on_create(record_id)\n\n        logger.info(f"✅ Done with newly created {capitalized} #{{record_id}}.")\n        return f"{capitalized} {{record_id}} creation processed!"\n    except Exception as e:\n        logger.error(f"💥 Problem in process_{table_name}_create({{record_id}}): {{e}}", exc_info=True)\n        raise\n\n@shared_task\ndef process_{table_name}_update(record_id: int):\n    """\n    The Celery task for updated {capitalized}.\n    """\n    try:\n        from utilities.config import Config\n        from db_util import initialize_database\n        from database_trigger_service import database_trigger_service\n        import logging\n\n        logger = logging.getLogger("admin_logger")\n        logger.info(f"🔄 Handling updated {capitalized} with record_id={{record_id}}.")\n\n        config = Config()\n        db_settings = config.get_database_settings(config.USE_LOCAL)\n        initialize_database(db_settings['url'])\n        logger.info("DB initialization is done.")\n\n        database_trigger_service.{table_name}_trigger_on_update(record_id)\n\n        logger.info(f"✅ {capitalized} #{{record_id}} update handled.")\n        return f"{capitalized} {{record_id}} updated!"\n    except Exception as e:\n        logger.error(f"💥 Problem in process_{table_name}_update({{record_id}}): {{e}}", exc_info=True)\n        raise\n\n@shared_task\ndef process_{table_name}_delete(record_id: int):\n    """\n    The Celery task for deleted {capitalized}.\n    """\n    try:\n        from utilities.config import Config\n        from db_util import initialize_database\n        from database_trigger_service import database_trigger_service\n        import logging\n\n        logger = logging.getLogger("admin_logger")\n        logger.info(f"🗑️ Handling deleted {capitalized} with record_id={{record_id}}.")\n\n        config = Config()\n        db_settings = config.get_database_settings(config.USE_LOCAL)\n        initialize_database(db_settings['url'])\n        logger.info("DB initialization is done.")\n\n        database_trigger_service.{table_name}_trigger_on_delete(record_id)\n\n        logger.info(f"✅ {capitalized} #{{record_id}} deletion handled.")\n        return f"{capitalized} {{record_id}} deletion processed!"\n    except Exception as e:\n        logger.error(f"💥 Problem in process_{table_name}_delete({{record_id}}): {{e}}", exc_info=True)\n        raise\n# endregion\n'''

def _remove_celery_tasks(table_name: str, content: str) -> str:
    region_pattern = f'# region.*?{table_name.upper()} TASKS.*?# endregion'
    updated = re.sub(region_pattern, '', content, flags=re.DOTALL)
    if updated != content:
        logger.info("Removed region block for '%s' tasks from celery_tasks.py", table_name)
        return updated
    pattern = f'@shared_task\\s+def\\s+process_{table_name}_create\\(.*?\\n\\n@shared_task\\s+def\\s+process_{table_name}_update\\(.*?\\n\\n@shared_task\\s+def\\s+process_{table_name}_delete\\(.*?\\n\\n'
    updated = re.sub(pattern, '', content, flags=re.DOTALL)
    if updated == content:
        logger.warning("No Celery tasks found to remove for '%s'.", table_name)
    else:
        logger.info("Removed Celery tasks for table '%s'.", table_name)
    return updated