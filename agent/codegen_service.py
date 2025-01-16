#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import logging
import os
import re
from typing import Optional, Dict, Any, List

from sqlalchemy import text
from sqlalchemy.dialects.mysql import (
    INTEGER as MYSQL_INTEGER,
    DECIMAL as MYSQL_DECIMAL,
)

from db_util import get_db_session

logger = logging.getLogger("agent_logger")


# region 🍏 AI LOG CAPTURE HANDLER
class AiLogCaptureHandler(logging.Handler):
    """
    A custom logging handler that stores all log records in-memory
    so your AI agent can retrieve and summarize them later
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


# Attach the AI log capture handler to the logger
_ai_log_handler = AiLogCaptureHandler()
_ai_log_handler.setLevel(logging.DEBUG)
formatter = logging.Formatter("[%(levelname)s] %(name)s - %(message)s")
_ai_log_handler.setFormatter(formatter)
logger.addHandler(_ai_log_handler)
logger.setLevel(logging.DEBUG)
# endregion


# region 🛠️ APPLY SQL SCRIPT
########################################
# APPLY SQL SCRIPT
########################################
def apply_sql_script(sql_script: str):
    """
    Execute multi-statement SQL script against the DB using the provided session factory.
    For foreign key constraints, ensure the referencing and referenced tables use same engine (InnoDB).
    """
    logger.info("[apply_sql_script] Running SQL:\n%s", sql_script)
    with get_db_session() as session:
        statements = sql_script.strip().split(";")
        for stmt in statements:
            s = stmt.strip()
            if s:
                try:
                    session.execute(text(s))
                except Exception as e:
                    # Possibly foreign-key constraints or syntax error
                    logger.error(f"SQL error when executing statement '{s}': {e}", exc_info=True)
                    raise
        session.commit()
# endregion


# region 🏗 UPDATE SQLALCHEMY MODELS
########################################
# UPDATE SQLALCHEMY MODELS
########################################
def update_sqlalchemy_models(
    table_name: str,
    columns_or_updates: Any,
    create=False,
    delete=False,
    models_file_path: str = "../database/models.py"
):
    """
    - If create=True, insert a new class into models.py (at bottom).
    - If delete=True, remove existing definition for that table.
    - Otherwise, do partial updates.
    """
    if not os.path.exists(models_file_path):
        logger.error("Models file not found: %s", models_file_path)
        return

    with open(models_file_path, "r", encoding="utf-8") as f:
        content = f.read()

    class_name = _table_name_to_class_name(table_name)
    if create:
        # Check if class is already present
        class_pattern = re.compile(rf"class\s+{class_name}\(Base\)")
        if re.search(class_pattern, content):
            logger.warning("Class definition for table '%s' already exists. Skipping creation.", table_name)
            updated_content = content
        else:
            new_class = _build_sqlalchemy_class(table_name, columns_or_updates)
            updated_content = content.strip() + f"\n\n{new_class}\n"
            logger.info("Inserted new model class for table '%s' into %s", table_name, models_file_path)
    elif delete:
        updated_content = _remove_model_class(table_name, content)
    else:
        updated_content = _update_model_class(table_name, content, columns_or_updates)

    if updated_content != content:
        with open(models_file_path, "w", encoding="utf-8") as wf:
            wf.write(updated_content)
        logger.info("Successfully updated models file: %s", models_file_path)
    else:
        logger.info("No changes made to models file for table '%s'.", table_name)


def _build_sqlalchemy_class(table_name: str, columns: Any) -> str:
    class_name = _table_name_to_class_name(table_name)
    lines = []

    if isinstance(columns, list):
        for col in columns:
            lines.append(_parse_column_def(col))
    elif isinstance(columns, dict):
        col_defs = columns.get("col_defs", [])
        for raw_col in col_defs:
            lines.append(_parse_column_def(raw_col))
    else:
        lines.append("# Add your column definitions here")

    col_defs = "\n    ".join(lines)

    return f"""# region 🚧 {class_name.upper()} TABLE
class {class_name}(Base):
    __tablename__ = '{table_name}'

    id = Column(MYSQL_INTEGER(unsigned=True), primary_key=True, autoincrement=True)
    {col_defs}

    def to_dict(self):
        return {{c.name: getattr(self, c.name) for c in self.__table__.columns}}
# endregion
""".strip()


def _parse_column_def(column_string: str) -> str:
    tokens = column_string.strip().split(None, 1)
    col_name = tokens[0]
    remainder = tokens[1].upper() if len(tokens) > 1 else ""

    # DECIMAL(...) check
    dec_match = re.search(r"DECIMAL\s*\(\s*(\d+)\s*,\s*(\d+)\s*\)", remainder)
    if dec_match:
        precision = dec_match.group(1)
        scale = dec_match.group(2)
        return f"{col_name} = Column(MYSQL_DECIMAL({precision}, {scale}), nullable=True)"

    # INT or INT(...)
    int_match = re.search(r"INT", remainder)
    if int_match:
        return f"{col_name} = Column(MYSQL_INTEGER(unsigned=True), nullable=True)"

    # fallback
    return f"{col_name} = Column(MYSQL_INTEGER(unsigned=True), nullable=True)"


def _remove_model_class(table_name: str, file_content: str) -> str:
    class_name = _table_name_to_class_name(table_name)
    region_pattern = rf"# region.*?{class_name.upper()} TABLE.*?# endregion"
    updated = re.sub(region_pattern, "", file_content, flags=re.DOTALL)

    if updated == file_content:
        pattern = rf"class\s+{class_name}\(Base\).*?\n# endregion"
        updated = re.sub(pattern, "", updated, flags=re.DOTALL)

    if updated == file_content:
        logger.warning("No matching class definition found for %s. Nothing removed.", table_name)
    else:
        logger.info("Removed model class (and region) for %s.", table_name)
    return updated


def _update_model_class(table_name: str, file_content: str, updates: Dict[str, Any]) -> str:
    class_name = _table_name_to_class_name(table_name)
    pattern = rf"(class\s+{class_name}\(Base\).*?)(\nclass|\Z)"
    matches = list(re.finditer(pattern, file_content, flags=re.DOTALL))
    if not matches:
        logger.warning("No existing model class found for %s. Can't update.", table_name)
        return file_content

    first_match = matches[0]
    class_block = first_match.group(1)

    note = "\n    # --- Updates below ---\n"
    add_cols = updates.get("add_columns", [])
    for col in add_cols:
        note += f"    # ADD COLUMN: {col}\n"
    drop_cols = updates.get("drop_columns", [])
    for col in drop_cols:
        note += f"    # DROP COLUMN: {col}\n"

    updated_class_block = class_block + note
    start, end = first_match.span(1)
    new_content = file_content[:start] + updated_class_block + file_content[end:]
    logger.info("Appended update notes to class '%s'.", class_name)
    return new_content


def _table_name_to_class_name(table_name: str) -> str:
    return "".join(word.capitalize() for word in table_name.split("_"))
# endregion


# region ⚙️ UPDATE DATABASE_UTIL.PY
########################################
def update_database_util_files(
    table_name: str,
    create=False,
    delete=False,
    db_util_path: str = "../database/database_util.py"
):
    """
    If create=True, we append new CRUD stubs at bottom. If delete=True, remove them.
    """
    if not os.path.exists(db_util_path):
        logger.warning("database_util.py not found at %s", db_util_path)
        return

    with open(db_util_path, "r", encoding="utf-8") as f:
        content = f.read()

    if create:
        region_re = rf"# region.*?{table_name}.*?# endregion"
        if re.search(region_re, content, flags=re.DOTALL):
            logger.warning("CRUD region for '%s' already exists. Skipping creation.", table_name)
            updated_content = content
        else:
            crud_pattern = (
                rf"def\s+search_{table_name}\(.*?\)"
                rf"|def\s+create_{table_name}\(.*?\)"
                rf"|def\s+update_{table_name}\(.*?\)"
            )
            if re.search(crud_pattern, content, flags=re.DOTALL):
                logger.warning("CRUD methods for '%s' already exist. Skipping creation.", table_name)
                updated_content = content
            else:
                new_methods = _build_crud_methods(table_name)
                updated_content = content.strip() + f"\n\n{new_methods}\n"
                logger.info("Added new CRUD methods for table '%s' to database_util.py", table_name)
    elif delete:
        updated_content = _remove_crud_methods(table_name, content)
    else:
        updated_content = content

    if updated_content != content:
        with open(db_util_path, "w", encoding="utf-8") as wf:
            wf.write(updated_content)
        logger.info("Successfully updated database_util.py for table '%s'.", table_name)
    else:
        logger.info("No changes made to database_util.py for table '%s'.", table_name)


def _build_crud_methods(table_name: str) -> str:
    class_name = _table_name_to_class_name(table_name)
    return f"""
    # region 🧪 {table_name}
    def search_{table_name}(self, column_names=None, values=None):
        \"\"\"
        Search {table_name} records.
        \"\"\"
        from database.models import {class_name}
        return self._search_records({class_name}, column_names, values)

    def create_{table_name}(self, **kwargs):
        \"\"\"
        Create a new {table_name} record.
        \"\"\"
        from database.models import {class_name}
        unique_lookup = {{}}
        return self._create_record({class_name}, unique_lookup=unique_lookup, **kwargs)

    def update_{table_name}(self, record_id, **kwargs):
        \"\"\"
        Update an existing {table_name} record by ID.
        \"\"\"
        from database.models import {class_name}
        return self._update_record({class_name}, record_id, **kwargs)
# endregion
""".strip()


def _remove_crud_methods(table_name: str, content: str) -> str:
    region_pattern = rf"# region.*?{table_name}.*?# endregion"
    updated = re.sub(region_pattern, "", content, flags=re.DOTALL)
    if updated != content:
        logger.info("Removed region for '%s' from database_util.py", table_name)
        return updated

    pattern = (
        rf"def\s+search_{table_name}\(.*?\n\n"
        rf"|def\s+create_{table_name}\(.*?\n\n"
        rf"|def\s+update_{table_name}\(.*?\n\n"
    )
    updated = re.sub(pattern, "", content, flags=re.DOTALL)
    if updated == content:
        logger.warning("No existing CRUD stubs found for '%s'. Nothing removed.", table_name)
    else:
        logger.info("Removed existing CRUD methods for '%s'.", table_name)
    return updated
# endregion


# region 🏮 UPDATE DATABASE_TRIGGER.PY
########################################
def update_database_trigger_file(
    table_name: str,
    create=False,
    delete=False,
    triggers_file_path: str = "../database_trigger.py"
):
    """
    If create=True, add references to TASK_ROUTING or create it.
    Could also add an import for process_{table_name}_ tasks if needed.
    """
    if not os.path.exists(triggers_file_path):
        logger.warning("database_trigger.py not found at %s", triggers_file_path)
        return

    with open(triggers_file_path, "r", encoding="utf-8") as f:
        content = f.read()

    updated_content = content
    # Optionally: add an import for new tasks if not present
    updated_content = _add_import_for_celery_tasks(table_name, updated_content)

    if create:
        updated_content = _add_trigger_routing(table_name, updated_content)
    elif delete:
        updated_content = _remove_trigger_routing(table_name, updated_content)

    if updated_content != content:
        with open(triggers_file_path, "w", encoding="utf-8") as wf:
            wf.write(updated_content)
        logger.info("database_trigger.py updated for table '%s'.", table_name)
    else:
        logger.info("No changes made to database_trigger.py for table '%s'.", table_name)


def _add_import_for_celery_tasks(table_name: str, content: str) -> str:
    # Insert new lines into existing 'from celery_tasks import ( ... )' if found, or create one
    import_pattern = r"(from\s+celery_tasks\s+import\s*\([\s\S]*?\))"
    match = re.search(import_pattern, content)
    new_imports = f"process_{table_name}_create, process_{table_name}_update, process_{table_name}_delete,"

    if match:
        original_block = match.group(1)
        if f"process_{table_name}_create" in original_block:
            logger.warning("Celery tasks import for '%s' already present. Skipping import addition.", table_name)
            return content

        insertion_point = original_block.rfind(")")
        updated_block = (
            original_block[:insertion_point]
            + f"\n    {new_imports}\n"
            + original_block[insertion_point:]
        )
        return content.replace(original_block, updated_block)
    else:
        # No block found, add one near top
        lines = content.split("\n")
        new_import_block = (
            f"from celery_tasks import (\n"
            f"    {new_imports}\n"
            f")\n"
        )
        # Insert after the top imports
        insert_index = 0
        for i, line in enumerate(lines):
            if "import logging" in line:
                insert_index = i + 1
                break
        lines.insert(insert_index, new_import_block)
        return "\n".join(lines)


def _add_trigger_routing(table_name: str, content: str) -> str:
    routing_pattern = r"TASK_ROUTING\s*=\s*\{\s*\(.*?\}\s*\}"
    match = re.search(routing_pattern, content, flags=re.DOTALL)

    new_entries = f"""    ("{table_name}", "INSERT"): lambda rid: process_{table_name}_create.delay(rid),
    ("{table_name}", "UPDATE"): lambda rid: process_{table_name}_update.delay(rid),
    ("{table_name}", "DELETE"): lambda rid: process_{table_name}_delete.delay(rid),"""

    if match:
        original_block = match.group(0)
        insertion_point = original_block.rfind("}")
        exist_pat = rf'\("{table_name}",\s*"INSERT"'
        if re.search(exist_pat, original_block):
            logger.warning("Routing for table '%s' already exists. Skipping creation.", table_name)
            return content

        updated_block = (
            original_block[:insertion_point] + "\n"
            + new_entries + "\n"
            + original_block[insertion_point:]
        )
        return content.replace(original_block, updated_block)
    else:
        # create at bottom
        logger.warning("No TASK_ROUTING dict found; creating one at bottom for table '%s'.", table_name)
        new_block = (
            f"\n\nTASK_ROUTING = {{\n"
            f"{new_entries}\n"
            f"}}\n"
        )
        return content.strip() + new_block


def _remove_trigger_routing(table_name: str, content: str) -> str:
    pattern = (
        rf'\(\s*"{table_name}"\s*,\s*"INSERT"\s*\).*?\n'
        rf'|\(\s*"{table_name}"\s*,\s*"UPDATE"\s*\).*?\n'
        rf'|\(\s*"{table_name}"\s*,\s*"DELETE"\s*\).*?\n'
    )
    updated = re.sub(pattern, "", content)
    if updated == content:
        logger.warning("No references found in database_trigger.py for %s. Nothing removed.", table_name)
    else:
        logger.info("Removed references for '%s' from database_trigger.py", table_name)
    return updated
# endregion


# region 🏮 UPDATE DATABASE_TRIGGER_SERVICE.PY
########################################
def update_database_trigger_service_file(
    table_name: str,
    create=False,
    delete=False,
    trigger_service_path: str = "../celery_files/database_trigger_service.py"
):
    """
    If create=True, add new <table_name>_trigger_on_create/update/delete at bottom.
    If delete=True, remove them.
    """
    if not os.path.exists(trigger_service_path):
        logger.warning("database_trigger_service.py not found at %s", trigger_service_path)
        return

    with open(trigger_service_path, "r", encoding="utf-8") as f:
        content = f.read()

    if create:
        updated_content = _add_trigger_service_methods(table_name, content)
    elif delete:
        updated_content = _remove_trigger_service_methods(table_name, content)
    else:
        updated_content = content

    if updated_content != content:
        with open(trigger_service_path, "w", encoding="utf-8") as wf:
            wf.write(updated_content)
        logger.info("database_trigger_service.py updated for table '%s'.", table_name)
    else:
        logger.info("No changes made to database_trigger_service.py for '%s'.", table_name)


def _add_trigger_service_methods(table_name: str, content: str) -> str:
    region_pattern = rf"# region.*?{table_name.upper()} TRIGGERS"
    if re.search(region_pattern, content, flags=re.DOTALL):
        logger.warning(f"Trigger region for '{table_name}' already exists. Skipping creation.")
        return content

    code_block = _build_trigger_service_methods(table_name)
    updated_content = content.strip() + f"\n\n{code_block}\n"
    logger.info("Added new trigger region for '%s' to database_trigger_service.py", table_name)
    return updated_content


def _build_trigger_service_methods(table_name: str) -> str:
    capitalized = _table_name_to_class_name(table_name)
    return f"""# region 🏷️ {table_name.upper()} TRIGGERS
def {table_name}_trigger_on_create(self, record_id: int):
    self.logger.info(f"Handling newly created {capitalized} with record_id={{record_id}}.")
    # Insert real logic here (e.g. checking references, foreign keys, etc.)

def {table_name}_trigger_on_update(self, record_id: int):
    self.logger.info(f"Handling updated {capitalized} with record_id={{record_id}}.")
    # Insert logic for handling updates

def {table_name}_trigger_on_delete(self, record_id: int):
    self.logger.info(f"Handling deleted {capitalized} with record_id={{record_id}}.")
    # Insert logic for cleaning up references
# endregion
"""


def _remove_trigger_service_methods(table_name: str, content: str) -> str:
    region_pattern = rf"# region.*?{table_name.upper()} TRIGGERS.*?# endregion"
    updated = re.sub(region_pattern, "", content, flags=re.DOTALL)
    if updated != content:
        logger.info("Removed region block for '%s' triggers from database_trigger_service.py", table_name)
        return updated

    pattern = (
        rf"def\s+{table_name}_trigger_on_create\(.*?\n\n"
        rf"def\s+{table_name}_trigger_on_update\(.*?\n\n"
        rf"def\s+{table_name}_trigger_on_delete\(.*?\n\n"
    )
    updated = re.sub(pattern, "", content, flags=re.DOTALL)
    if updated == content:
        logger.warning("No trigger methods found to remove for '%s' in database_trigger_service.py.", table_name)
    else:
        logger.info("Removed trigger methods for '%s' from database_trigger_service.py", table_name)
    return updated
# endregion


# region 🌀 UPDATE CELERY_TASKS.PY
########################################
def update_celery_tasks_file(
    table_name: str,
    create=False,
    delete=False,
    celery_file_path: str = "../celery_files/celery_tasks.py"
):
    """
    If create=True, add tasks at bottom. If delete=True, remove them.
    """
    if not os.path.exists(celery_file_path):
        logger.warning("celery_tasks.py not found at %s", celery_file_path)
        return

    with open(celery_file_path, "r", encoding="utf-8") as f:
        content = f.read()

    if create:
        updated_content = _add_celery_tasks(table_name, content)
    elif delete:
        updated_content = _remove_celery_tasks(table_name, content)
    else:
        updated_content = content

    if updated_content != content:
        with open(celery_file_path, "w", encoding="utf-8") as wf:
            wf.write(updated_content)
        logger.info("celery_tasks.py updated for table '%s'.", table_name)
    else:
        logger.info("No changes made to celery_tasks.py for table '%s'.", table_name)


def _add_celery_tasks(table_name: str, content: str) -> str:
    region_pattern = rf"# region.*?{table_name.upper()} TASKS.*?# endregion"
    if re.search(region_pattern, content, flags=re.DOTALL):
        logger.warning("Celery tasks region for '%s' already exists. Skipping creation.", table_name)
        return content

    tasks_pattern = (
        rf"def\s+process_{table_name}_create\(.*?\)"
        rf"|def\s+process_{table_name}_update\(.*?\)"
        rf"|def\s+process_{table_name}_delete\(.*?\)"
    )
    if re.search(tasks_pattern, content, flags=re.DOTALL):
        logger.warning("Celery tasks for '%s' already exist. Skipping creation.", table_name)
        return content

    new_celery_tasks = _build_celery_tasks(table_name)
    updated_content = content.strip() + f"\n\n{new_celery_tasks}\n"
    return updated_content


def _build_celery_tasks(table_name: str) -> str:
    capitalized = _table_name_to_class_name(table_name)
    return f"""# region 🍔 {table_name.upper()} TASKS
########################################
# {table_name.upper()} TASKS
########################################
@shared_task
def process_{table_name}_create(record_id: int):
    \"\"\"
    The Celery task for newly created {capitalized}.
    \"\"\"
    try:
        from utilities.config import Config
        from db_util import initialize_database
        from database_trigger_service import database_trigger_service
        import logging

        logger = logging.getLogger("celery_logger")
        logger.info(f"🚀 Handling newly created {capitalized} with record_id={{record_id}}.")

        config = Config()
        db_settings = config.get_database_settings(config.USE_LOCAL)
        initialize_database(db_settings['url'])
        logger.info("DB initialization is done.")

        database_trigger_service.{table_name}_trigger_on_create(record_id)

        logger.info(f"✅ Done with newly created {capitalized} #{{record_id}}.")
        return f"{capitalized} {{record_id}} creation processed!"
    except Exception as e:
        logger.error(f"💥 Problem in process_{table_name}_create({{record_id}}): {{e}}", exc_info=True)
        raise

@shared_task
def process_{table_name}_update(record_id: int):
    \"\"\"
    The Celery task for updated {capitalized}.
    \"\"\"
    try:
        from utilities.config import Config
        from db_util import initialize_database
        from database_trigger_service import database_trigger_service
        import logging

        logger = logging.getLogger("celery_logger")
        logger.info(f"🔄 Handling updated {capitalized} with record_id={{record_id}}.")

        config = Config()
        db_settings = config.get_database_settings(config.USE_LOCAL)
        initialize_database(db_settings['url'])
        logger.info("DB initialization is done.")

        database_trigger_service.{table_name}_trigger_on_update(record_id)

        logger.info(f"✅ {capitalized} #{{record_id}} update handled.")
        return f"{capitalized} {{record_id}} updated!"
    except Exception as e:
        logger.error(f"💥 Problem in process_{table_name}_update({{record_id}}): {{e}}", exc_info=True)
        raise

@shared_task
def process_{table_name}_delete(record_id: int):
    \"\"\"
    The Celery task for deleted {capitalized}.
    \"\"\"
    try:
        from utilities.config import Config
        from db_util import initialize_database
        from database_trigger_service import database_trigger_service
        import logging

        logger = logging.getLogger("celery_logger")
        logger.info(f"🗑️ Handling deleted {capitalized} with record_id={{record_id}}.")

        config = Config()
        db_settings = config.get_database_settings(config.USE_LOCAL)
        initialize_database(db_settings['url'])
        logger.info("DB initialization is done.")

        database_trigger_service.{table_name}_trigger_on_delete(record_id)

        logger.info(f"✅ {capitalized} #{{record_id}} deletion handled.")
        return f"{capitalized} {{record_id}} deletion processed!"
    except Exception as e:
        logger.error(f"💥 Problem in process_{table_name}_delete({{record_id}}): {{e}}", exc_info=True)
        raise
# endregion
"""


def _remove_celery_tasks(table_name: str, content: str) -> str:
    region_pattern = rf"# region.*?{table_name.upper()} TASKS.*?# endregion"
    updated = re.sub(region_pattern, "", content, flags=re.DOTALL)
    if updated != content:
        logger.info("Removed region block for '%s' tasks from celery_tasks.py", table_name)
        return updated

    pattern = (
        rf"@shared_task\s+def\s+process_{table_name}_create\(.*?\n\n"
        rf"@shared_task\s+def\s+process_{table_name}_update\(.*?\n\n"
        rf"@shared_task\s+def\s+process_{table_name}_delete\(.*?\n\n"
    )
    updated = re.sub(pattern, "", content, flags=re.DOTALL)
    if updated == content:
        logger.warning("No Celery tasks found to remove for '%s'.", table_name)
    else:
        logger.info("Removed Celery tasks for table '%s'.", table_name)
    return updated
# endregion