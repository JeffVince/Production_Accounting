#!/usr/bin/env python3
import os
import sys
import ast

TARGET_FOLDERS = {
    'database', 'files_dropbox', 'files_monday', 'files_xero',
    'files_invoice', 'orchestration', 'files_po_log', 'server_celery',
    'server_agent', 'server_trigger', 'files_budget'
}


def get_file_info(file_path):
    """
    Parses a Python file to extract function names, class names, and imported modules.

    Returns:
        A dict with keys 'functions', 'classes', and 'imports', each being a list of names.
    """
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            content = f.read()
        tree = ast.parse(content, filename=file_path)
    except Exception:
        return {"functions": [], "classes": [], "imports": []}

    functions = []
    classes = []
    imports = []
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            functions.append(node.name)
        elif isinstance(node, ast.ClassDef):
            classes.append(node.name)
        elif isinstance(node, ast.Import):
            for alias in node.names:
                imports.append(alias.name)
        elif isinstance(node, ast.ImportFrom):
            module = node.module if node.module else ""
            for alias in node.names:
                if module:
                    imports.append(f"{module}.{alias.name}")
                else:
                    imports.append(alias.name)

    # Remove duplicates while preserving order.
    functions = list(dict.fromkeys(functions))
    classes = list(dict.fromkeys(classes))
    imports = list(dict.fromkeys(imports))
    return {"functions": functions, "classes": classes, "imports": imports}


def print_tree(root_path, output, prefix="", is_root=False):
    """
    Recursively writes the directory tree starting at root_path to the provided output file.
    For each Python file, it also writes the functions, classes, and imports found in that file.

    Only directories whose names are in TARGET_FOLDERS are recursed into (except for the provided root).
    """
    try:
        items = sorted(os.listdir(root_path))
    except PermissionError:
        return

    for index, item in enumerate(items):
        path = os.path.join(root_path, item)
        is_last = (index == len(items) - 1)
        connector = "└── " if is_last else "├── "

        if os.path.isdir(path):
            # For subdirectories (i.e. not the provided root), only check target folders.
            if not is_root and item not in TARGET_FOLDERS:
                continue
            output.write(prefix + connector + item + "/\n")
            extension = "    " if is_last else "│   "
            # Once inside the project root, all recursive calls use is_root=False.
            print_tree(path, output, prefix + extension, is_root=False)
        else:
            output.write(prefix + connector + item + "\n")
            # If it's a Python file, extract and write its functions, classes, and imports.
            if item.endswith(".py"):
                info = get_file_info(path)
                sub_prefix = prefix + ("    " if is_last else "│   ")
                groups = []
                if info["classes"]:
                    groups.append(("Classes", info["classes"]))
                if info["functions"]:
                    groups.append(("Functions", info["functions"]))
                if info["imports"]:
                    groups.append(("Imports", info["imports"]))
                for i, (label, items_list) in enumerate(groups):
                    connector_line = "└── " if i == len(groups) - 1 else "├── "
                    output.write(sub_prefix + connector_line + f"{label}: " + ", ".join(items_list) + "\n")


def main():
    directory = "/Users/haske107/PycharmProjects/Dropbox Listener"
    if not os.path.isdir(directory):
        print(f"Error: {directory} is not a valid directory")
        sys.exit(1)

    output_file = "tree.txt"

    with open(output_file, "w", encoding="utf-8") as f:
        # Print the absolute path of the provided root directory.
        f.write(os.path.abspath(directory) + "/scratch_files/\n")
        # For children of the root, we only process directories that are in TARGET_FOLDERS.
        print_tree(directory, f, prefix="", is_root=False)

    print(f"Tree structure saved to {output_file}")


if __name__ == '__main__':
    main()