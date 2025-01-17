import os
import ast
import json
from collections import defaultdict
TARGET_FOLDERS = {'database', 'files_dropbox', 'files_monday', 'files_xero', 'files_invoiceorchestration', 'files_po_log', 'server_celery', 'server_webhook', 'server_agent', 'server_triggerfiles_budget'}
ROOT_FILES = {'main.py', 'celery_server.py', 'database_trigger.py'}

class FunctionCallVisitor(ast.NodeVisitor):
    """
    AST Node Visitor to collect function calls within a function's body.
    """

    def __init__(self, current_file_functions):
        self.called_functions = set()
        self.current_file_functions = current_file_functions

    def visit_Call(self, node):
        if isinstance(node.func, ast.Name):
            self.called_functions.add(node.func.id)
        elif isinstance(node.func, ast.Attribute):
            self.called_functions.add(node.func.attr)
        self.generic_visit(node)

def extract_info_from_file(file_path, all_functions):
    """
    Parses a Python file to gather info:
      - All functions (name, args, decorators, calls, docstring, and body)
      - All classes (name, decorators, methods, etc.)
      - Top-level variables (assigned or annotated).
    """
    with open(file_path, 'r', encoding='utf-8') as file:
        try:
            tree = ast.parse(file.read(), filename=file_path)
        except SyntaxError as e:
            print(f'SyntaxError in {file_path}: {e}')
            return None
    file_info = {'file_path': file_path, 'functions': [], 'classes': [], 'variables': []}
    current_file_functions = set()
    for node in ast.iter_child_nodes(tree):
        if isinstance(node, ast.FunctionDef):
            current_file_functions.add(node.name)
        elif isinstance(node, ast.ClassDef):
            for class_node in node.body:
                if isinstance(class_node, ast.FunctionDef):
                    current_file_functions.add(class_node.name)
    for node in ast.iter_child_nodes(tree):
        if isinstance(node, ast.FunctionDef):
            func_info = {'name': node.name, 'args': [arg.arg for arg in node.args.args], 'decorators': [get_decorator_name(decorator) for decorator in node.decorator_list], 'calls': [], 'docstring': None, 'function_body': None}
            func_docstring = ast.get_docstring(node)
            if func_docstring:
                func_info['docstring'] = func_docstring
            try:
                start_line = node.lineno - 1
                end_line = node.body[-1].lineno
                with open(file_path, 'r', encoding='utf-8') as source_file:
                    all_lines = source_file.read().split('\n')
                    function_body_lines = all_lines[start_line:end_line]
                func_info['function_body'] = '\n'.join(function_body_lines)
            except Exception as e:
                print(f'Error extracting function body in {file_path}: {e}')
            visitor = FunctionCallVisitor(all_functions)
            visitor.visit(node)
            func_info['calls'] = list(visitor.called_functions.intersection(all_functions))
            file_info['functions'].append(func_info)
        elif isinstance(node, ast.ClassDef):
            class_info = {'name': node.name, 'methods': [], 'decorators': [get_decorator_name(decorator) for decorator in node.decorator_list]}
            for class_node in node.body:
                if isinstance(class_node, ast.FunctionDef):
                    method_info = {'name': class_node.name, 'args': [arg.arg for arg in class_node.args.args], 'decorators': [get_decorator_name(decorator) for decorator in class_node.decorator_list], 'calls': [], 'docstring': None, 'function_body': None}
                    method_docstring = ast.get_docstring(class_node)
                    if method_docstring:
                        method_info['docstring'] = method_docstring
                    try:
                        start_line = class_node.lineno - 1
                        end_line = class_node.body[-1].lineno
                        with open(file_path, 'r', encoding='utf-8') as source_file:
                            all_lines = source_file.read().split('\n')
                            method_body_lines = all_lines[start_line:end_line]
                        method_info['function_body'] = '\n'.join(method_body_lines)
                    except Exception as e:
                        print(f'Error extracting method body in {file_path}: {e}')
                    method_visitor = FunctionCallVisitor(all_functions)
                    method_visitor.visit(class_node)
                    method_info['calls'] = list(method_visitor.called_functions.intersection(all_functions))
                    class_info['methods'].append(method_info)
            file_info['classes'].append(class_info)
        elif isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name):
                    var_info = {'name': target.id, 'value': ast.unparse(node.value) if hasattr(ast, 'unparse') else ast.dump(node.value)}
                    file_info['variables'].append(var_info)
        elif isinstance(node, ast.AnnAssign):
            if isinstance(node.target, ast.Name):
                var_info = {'name': node.target.id, 'annotation': ast.unparse(node.annotation) if hasattr(ast, 'unparse') else ast.dump(node.annotation), 'value': ast.unparse(node.value) if node.value and hasattr(ast, 'unparse') else ast.dump(node.value) if node.value else None}
                file_info['variables'].append(var_info)
    return file_info

def get_decorator_name(decorator):
    if isinstance(decorator, ast.Name):
        return decorator.id
    elif isinstance(decorator, ast.Attribute):
        return f'{get_decorator_name(decorator.value)}.{decorator.attr}'
    elif isinstance(decorator, ast.Call):
        func_name = get_decorator_name(decorator.func)
        args = [ast.unparse(arg) if hasattr(ast, 'unparse') else ast.dump(arg) for arg in decorator.args]
        return f"{func_name}({', '.join(args)})"
    else:
        return ast.dump(decorator)

def traverse_project(directory):
    """
    First pass:
      - Collect all function names across all files (top-level + class methods).
    Second pass:
      - Extract detailed info including docstrings, function bodies, etc.
    """
    project_summary = []
    all_functions = set()
    for (root, dirs, files) in os.walk(directory):
        current_folder = os.path.basename(root)
        if current_folder not in TARGET_FOLDERS and root != directory:
            dirs[:] = []
            continue
        for file in files:
            if file.endswith('.py'):
                full_path = os.path.join(root, file)
                relative_path = os.path.relpath(full_path, directory)
                if is_target_file(relative_path, file):
                    with open(full_path, 'r', encoding='utf-8') as f:
                        try:
                            tree = ast.parse(f.read(), filename=full_path)
                            for node in ast.iter_child_nodes(tree):
                                if isinstance(node, ast.FunctionDef):
                                    all_functions.add(node.name)
                                elif isinstance(node, ast.ClassDef):
                                    for class_node in node.body:
                                        if isinstance(class_node, ast.FunctionDef):
                                            all_functions.add(class_node.name)
                        except SyntaxError:
                            continue
    for (root, dirs, files) in os.walk(directory):
        current_folder = os.path.basename(root)
        if current_folder not in TARGET_FOLDERS and root != directory:
            dirs[:] = []
            continue
        for file in files:
            if file.endswith('.py'):
                full_path = os.path.join(root, file)
                relative_path = os.path.relpath(full_path, directory)
                if is_target_file(relative_path, file):
                    info = extract_info_from_file(full_path, all_functions)
                    if info:
                        project_summary.append(info)
    return project_summary

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

def save_summary(summary, output_file='project_summary.json'):
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(summary, f, indent=4)
    print(f'Project summary saved to {output_file}')
if __name__ == '__main__':
    project_directory = '/Users/haske107/PycharmProjects/Dropbox Listener/'
    if not os.path.isdir(project_directory):
        print('Invalid directory path.')
    else:
        summary = traverse_project(project_directory)
        save_summary(summary)