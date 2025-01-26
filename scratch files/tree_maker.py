import os

# Define target folders and root files
TARGET_FOLDERS = {'database', 'files_dropbox', 'files_monday', 'files_xero', 'files_invoiceorchestration',
                  'files_po_log', 'server_celery', 'server_webhook', 'server_agent', 'server_triggerfiles_budget'}
ROOT_FILES = {'main.py', 'webhook_main.py', 'celery_server.py', 'database_trigger', 'agent_app'}


# Function to generate the tree structure3
def generate_tree_with_lines(root_path):
    def tree_dir(path, prefix=""):
        entries = sorted(os.listdir(path))
        entries = [e for e in entries if os.path.isfile(os.path.join(path, e)) or os.path.isdir(os.path.join(path, e))]
        entries_count = len(entries)

        for index, entry in enumerate(entries):
            connector = "├── " if index < entries_count - 1 else "└── "
            yield f"{prefix}{connector}{entry}"

            entry_path = os.path.join(path, entry)
            if os.path.isdir(entry_path):
                extension = "│   " if index < entries_count - 1 else "    "
                yield from tree_dir(entry_path, prefix + extension)

    tree = []

    # Add root files to the tree
    root_files = [file for file in ROOT_FILES if os.path.isfile(os.path.join(root_path, file))]
    for index, file in enumerate(root_files):
        connector = "├── " if index < len(root_files) - 1 else "└── "
        tree.append(f"{connector}{file}")

    # Add folders and their contents
    for folder in sorted(TARGET_FOLDERS):
        folder_path = os.path.join(root_path, folder)
        if os.path.isdir(folder_path):
            tree.append(f"├── {folder}")
            tree.extend([f"    {line}" for line in tree_dir(folder_path)])

    return "\n".join(tree)


# Main execution block
if __name__ == "__main__":
    # Get the root directory (one level up from the current directory)
    root_path = os.path.abspath(os.path.join(os.getcwd(), ".."))

    # Generate and print the ASCII tree with lines
    tree = generate_tree_with_lines(root_path)
    print("Root Directory Structure:")
    print(tree)
