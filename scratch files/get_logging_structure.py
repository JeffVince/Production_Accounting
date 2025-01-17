import os
import ast
import json
TARGET_FOLDERS = {'database', 'files_dropbox', 'files_monday', 'files_xero', 'files_invoiceorchestration', 'files_po_log', 'server_celery', 'server_webhook', 'server_agent', 'server_triggerfiles_budget'}
ROOT_FILES = {'main.py', 'webhook_main.py', 'celery_server.py', 'database_trigger', 'agent_app'}

def is_target_file(relative_path, file_name):
    """
    Checks if the file should be included by either:
      - Being in one of the target folders
      - Being at root level and in ROOT_FILES
    """
    path_parts = relative_path.split(os.sep)
    if len(path_parts) > 1:
        folder = path_parts[0]
        if folder in TARGET_FOLDERS:
            return True
    elif file_name in ROOT_FILES:
        return True
    return False

class LoggingVisitor(ast.NodeVisitor):
    """
    Collects:
      - Logging imports
      - Logger creation calls
      - Logger usage calls (e.g., logger.info(...), logger.debug(...))
    """

    def __init__(self):
        super().__init__()
        self.logging_imports = []
        self.logger_creations = []
        self.logger_calls = []

    def visit_Import(self, node):
        """
        Looks for 'import logging'
        """
        for alias in node.names:
            if alias.name == 'logging':
                self.logging_imports.append({'type': 'import_logging', 'lineno': node.lineno, 'code': f'import {alias.name} as {alias.asname}' if alias.asname else f'import {alias.name}'})
        self.generic_visit(node)

    def visit_ImportFrom(self, node):
        """
        Looks for 'from logging import ...'
        """
        if node.module == 'logging':
            for alias in node.names:
                self.logging_imports.append({'type': 'from_logging_import', 'lineno': node.lineno, 'code': f'from logging import {alias.name} as {alias.asname}' if alias.asname else f'from logging import {alias.name}'})
        self.generic_visit(node)

    def visit_Call(self, node):
        """
        Looks for:
          - logging.getLogger(...)
          - logger.info(...), logger.debug(...), etc.
        """
        if isinstance(node.func, ast.Attribute):
            if isinstance(node.func.value, ast.Name) and node.func.value.id == 'logging' and (node.func.attr == 'getLogger'):
                self.logger_creations.append({'lineno': node.lineno, 'code': ast.get_source_segment(self.source_code, node)})
            elif node.func.attr in ['debug', 'info', 'warning', 'error', 'critical']:
                caller_name = None
                if isinstance(node.func.value, ast.Name):
                    caller_name = node.func.value.id
                elif isinstance(node.func.value, ast.Attribute):
                    caller_name = f'{ast.get_source_segment(self.source_code, node.func.value)}'
                self.logger_calls.append({'lineno': node.lineno, 'func': node.func.attr, 'caller': caller_name, 'code': ast.get_source_segment(self.source_code, node)})
        elif isinstance(node.func, ast.Name) and node.func.id == 'getLogger':
            self.logger_creations.append({'lineno': node.lineno, 'code': ast.get_source_segment(self.source_code, node)})
        self.generic_visit(node)

def analyze_file_for_logging(file_path):
    """
    Parse a single Python file and extract logging-related usage info.
    Returns a dict with { imports, logger_creations, logger_calls }.
    """
    with open(file_path, 'r', encoding='utf-8') as f:
        source = f.read()
    try:
        tree = ast.parse(source, file_path)
    except SyntaxError:
        return None
    visitor = LoggingVisitor()
    visitor.source_code = source
    visitor.visit(tree)
    return {'logging_imports': visitor.logging_imports, 'logger_creations': visitor.logger_creations, 'logger_calls': visitor.logger_calls}

def traverse_project_for_logging(directory):
    """
    Recursively walks the project directory, looking for Python files
    within TARGET_FOLDERS or ROOT_FILES, then analyzes each file for logging usage.
    Returns a summary dict for all files.
    """
    project_logging_summary = []
    for (root, dirs, files) in os.walk(directory):
        current_folder = os.path.basename(root)
        if current_folder not in TARGET_FOLDERS and root != directory:
            dirs[:] = []
            continue
        for file_name in files:
            if file_name.endswith('.py'):
                full_path = os.path.join(root, file_name)
                relative_path = os.path.relpath(full_path, directory)
                if is_target_file(relative_path, file_name):
                    logging_info = analyze_file_for_logging(full_path)
                    if logging_info is not None:
                        file_data = {'file_path': relative_path, 'logging_data': logging_info}
                        project_logging_summary.append(file_data)
    return project_logging_summary

def save_logging_summary(summary, output_file='logging_summary.json'):
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(summary, f, indent=4)
    print(f'Logging summary saved to {output_file}')
if __name__ == '__main__':
    project_directory = '/Users/Haske107/PycharmProjects/Dropbox Listener'
    if not os.path.isdir(project_directory):
        print('Invalid directory path. Please update project_directory in this script.')
    else:
        summary = traverse_project_for_logging(project_directory)
        save_logging_summary(summary)