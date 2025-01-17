#!/usr/bin/env python3

import os

def count_python_lines(root_path):
    excluded_dirs = {'venv', 'env', 'site-packages', '__pycache__'}
    total_lines = 0

    for root, dirs, files in os.walk(root_path):
        # Exclude the directories you don't want to scan
        dirs[:] = [d for d in dirs if d not in excluded_dirs]

        # Count lines only in .py files
        for file_name in files:
            if file_name.endswith('.py'):
                file_path = os.path.join(root, file_name)
                with open(file_path, 'r', encoding='utf-8') as f:
                    total_lines += sum(1 for _ in f)

    return total_lines

if __name__ == "__main__":
    root_directory = "."  # current directory
    lines_of_code = count_python_lines(root_directory)
    print(f"Total lines of Python code: {lines_of_code}")