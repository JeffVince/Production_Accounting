#!/usr/bin/env python3
import os
import sys
import ast


def get_file_info(file_path):
    """
    Parses a Python file to extract function names and imported modules.

    Returns:
        A dict with keys 'functions' and 'imports', each being a list of names.
    """
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            content = f.read()
        tree = ast.parse(content, filename=file_path)
    except Exception as e:
        # If there's an error reading/parsing, return empty info.
        return {"functions": [], "imports": []}

    functions = []
    imports = []
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            functions.append(node.name)
        elif isinstance(node, ast.Import):
            for alias in node.names:
                imports.append(alias.name)
        elif isinstance(node, ast.ImportFrom):
            module = node.module if node.module else ""
            for alias in node.names:
                # Format as "module.name" if module is provided.
                if module:
                    imports.append(f"{module}.{alias.name}")
                else:
                    imports.append(alias.name)

    # Remove duplicates while preserving order.
    functions = list(dict.fromkeys(functions))
    imports = list(dict.fromkeys(imports))
    return {"functions": functions, "imports": imports}


def print_tree(root_path, output, prefix=""):
    """
    Recursively writes the directory tree starting at root_path to the provided output file.
    For each Python file, it also writes the functions and imports found in that file.
    """
    try:
        items = sorted(os.listdir(root_path))
    except PermissionError:
        # In case we do not have permission to list the directory, skip it.
        return

    for index, item in enumerate(items):
        path = os.path.join(root_path, item)
        is_last = (index == len(items) - 1)
        connector = "└── " if is_last else "├── "

        if os.path.isdir(path):
            output.write(prefix + connector + item + "/\n")
            extension = "    " if is_last else "│   "
            print_tree(path, output, prefix + extension)
        else:
            output.write(prefix + connector + item + "\n")
            # If the file is a Python file, parse and write its functions and imports.
            if item.endswith(".py"):
                info = get_file_info(path)
                sub_prefix = prefix + ("    " if is_last else "│   ")
                if info["functions"]:
                    output.write(sub_prefix + "├── Functions: " + ", ".join(info["functions"]) + "\n")
                if info["imports"]:
                    # Use a different connector if functions were printed.
                    connector_line = "└── " if info["functions"] else "├── "
                    output.write(sub_prefix + connector_line + "Imports: " + ", ".join(info["imports"]) + "\n")
def main():




    # Print the root folder and then its tree.

    directory = ("/Users/haske107/PycharmProjects/Dropbox Listener/")

    output_file = sys.argv[2] if len(sys.argv) >= 3 else "tree.txt"

    with open(output_file, "w", encoding="utf-8") as f:
        f.write(os.path.abspath(directory) + "/scratch files/tree.txt")
        print_tree(directory, f)

    print(f"Tree structure saved to {output_file}")

if __name__ == '__main__':
    main()