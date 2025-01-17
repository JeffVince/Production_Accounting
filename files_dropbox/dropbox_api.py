# integrations/dropbox_api.py
import json
import logging
import os
from typing import Optional, List, Dict
import re

import dropbox
from dropbox import common
from dropbox.files import FolderMetadata, FileMetadata

from files_dropbox.dropbox_client import dropbox_client
from dropbox.exceptions import ApiError

from singleton import SingletonMeta
from utilities.config import Config


class DropboxAPI(metaclass=SingletonMeta):
    def __init__(self):
        if not hasattr(self, '_initialized'):
            self.logger = logging.getLogger("dropbox_logger")
            self.dbx_client = dropbox_client
            self.dbx = self.dbx_client.dbx
            self.TAX_FORM_REGEX = r"(?i)\b(w9)|(w8-ben)|(w8-bene)|(w8-ben-e)\b"

    def upload_file(self, file_path: str, destination_path: str):
        """Uploads a file to Dropbox."""
        with open(file_path, 'rb') as f:
            try:
                self.dbx.files_upload(f.read(), destination_path)
                return True
            except ApiError as e:
                print(f"Error uploading file: {e}")
                return False

    def download_file(self, file_path: str, local_destination: str):
        """Downloads a file from Dropbox."""
        try:
            metadata, res = self.dbx.files_download(file_path)
            with open(local_destination, 'wb') as f:
                f.write(res.content)
            return True
        except ApiError as e:
            print(f"Error downloading file: {e}")
            return False

    def get_file_metadata(self, file_path: str):
        """Retrieves metadata for a file."""
        try:
            metadata = self.dbx.files_get_metadata(file_path)
            return metadata
        except ApiError as e:
            print(f"Error getting file metadata: {e}")
            return None

    def list_folder_contents(self, folder_path: str):
        """Lists the contents of a folder."""
        try:
            result = self.dbx.files_list_folder(folder_path)
            entries = result.entries
            while result.has_more:
                result = self.dbx.files_list_folder_continue(result.cursor)
                entries.extend(result.entries)
            items_in_folder = []
            for entry in entries:
                items_in_folder.append(os.path.basename(entry.path_display))
            return items_in_folder
        except ApiError as e:
            print(f"Error listing folder contents: {e}")
            return []

    def create_folder(self, folder_path: str):
        """Creates a folder in Dropbox."""
        try:
            self.dbx.files_create_folder_v2(folder_path)
            return True
        except ApiError as e:
            print(f"Error creating folder: {e}")
            return False

    def delete_file_or_folder(self, path: str):
        """Deletes a file or folder in Dropbox."""
        try:
            self.dbx.files_delete_v2(path)
            return True
        except ApiError as e:
            print(f"Error deleting file or folder: {e}")
            return False

    def list_files_in_folder(self, folder_path: str) -> list:
        """
        Lists all files under the given folder_path and returns a list of dicts
        containing 'file_name' and 'file_link' for each file.
        """
        self.logger.debug(f"📂 Listing files under '{folder_path}'")
        files_data = []

        try:
            result = self.dbx.files_list_folder(path=folder_path, recursive=False)
            entries = result.entries

            while result.has_more:
                result = self.dbx.files_list_folder_continue(result.cursor)
                entries.extend(result.entries)

            for entry in entries:
                if isinstance(entry, FileMetadata):
                    files_data.append({
                        'file_name': entry.name,
                    })

        except Exception as e:
            self.logger.exception(f"💥 Error listing files in '{folder_path}': {e}", exc_info=True)

        return files_data

    def get_po_tax_form_link(self,
         project_number: Optional[str] = None,
         po_number: Optional[str] = None
     ) -> List[Dict[str, str]]:
        """
        Retrieves PO folders and their shared links based on provided parameters.

        Args:
            project_number (str, optional): The ID of the project. Defaults to None.
            po_number (str, optional): The number of the PO. Defaults to None.

        Returns:
            List[Dict[str, str]]: A list of dictionaries containing 'po_folder_name',
                                    'po_folder_path', and 'po_folder_link'.
        """
        project_po_data = []

        try:
            if project_number is None:
                # Scenario 1: No arguments provided - Retrieve all PO folders in the '2024' namespace
                self.logger.info("📁 No project_number provided. Retrieving all PO folders under the '2024' namespace.")
                all_projects = self.list_all_projects(namespace="2024")
                if not all_projects:
                    self.logger.warning("⚠️ No projects found under the '2024' namespace.")
                    return []

                for project in all_projects:
                    current_project_number = project['id']
                    project_folder_path = project['path']
                    po_base_path = f"{project_folder_path}/1. Purchase Orders"

                    po_folders = self.list_project_po_folders(po_base_path)
                    if not po_folders:
                        self.logger.info(f"ℹ️ No PO folders found for project_number={current_project_number} at '{po_base_path}'")
                        continue

                    for po_folder in po_folders:
                        # check if there's a tax form
                        files = self.list_files_in_folder(po_folder["path"])
                        tax_form_link = ""
                        form_type = ""
                        for file in files:
                            match = re.search(self.TAX_FORM_REGEX, file['file_name'].lower(), re.IGNORECASE)
                            if match:
                                self.logger.info(f"💼 Identified as tax form: {file['file_name']}")
                                # get the share link
                                tax_form_link = self.create_share_link(po_folder["path"])
                                if match.group(1):
                                    form_type = "W-9"
                                elif match.group(2):
                                    form_type = "W-8BEN"
                                elif match.group(3):
                                    form_type = "W-8BEN-E"
                                elif match.group(4):
                                    form_type = "W-8BEN-E"

                                self.logger.info(f"💼 Identified as tax form: {file['file_name']} ({form_type})")
                        project_po_data.append({
                            'po_folder_name': po_folder['name'],
                            'po_folder_path': po_folder['path'],
                            'po_tax_form_link': tax_form_link,
                            'form_type': form_type
                        })

            elif project_number and po_number is None:
                # Scenario 2: Only project_number provided - Retrieve all PO folders for the specified project
                self.logger.info(f"📁 Retrieving PO folders for project_number={project_number}")
                project_folder_path = self.find_project_folder(project_number)
                if not project_folder_path:
                    self.logger.warning(f"⚠️ Unable to find project folder for project_number={project_number}")
                    return []

                po_base_path = f"{project_folder_path}/1. Purchase Orders"
                po_folders = self.list_project_po_folders(po_base_path)
                if not po_folders:
                    self.logger.info(f"ℹ️ No PO folders found for project_number={project_number} at '{po_base_path}'")
                    return []

                for po_folder in po_folders:
                    # check if there's a tax form
                    files = self.list_files_in_folder(po_folder["path"])
                    tax_form_link = ""
                    form_type = ""
                    for file in files:
                        match = re.search(self.TAX_FORM_REGEX, file['file_name'].lower(), re.IGNORECASE)
                        if match:
                            self.logger.info(f"💼 Identified as tax form: {file['file_name']}")
                            # get the share link
                            tax_form_link = self.create_share_link(po_folder["path"])
                            if match.group(1):
                                form_type = "W-9"
                            elif match.group(2):
                                form_type = "W-8BEN"
                            elif match.group(3):
                                form_type = "W-8BEN-E"
                            elif match.group(4):
                                form_type = "W-8BEN-E"

                            self.logger.info(f"💼 Identified as tax form: {file['file_name']} ({form_type})")
                    project_po_data.append({
                        'po_folder_name': po_folder['name'],
                        'po_folder_path': po_folder['path'],
                        'po_tax_form_link': tax_form_link,
                        'form_type': form_type
                    })

            elif project_number and po_number:
                # Scenario 3: Both project_number and po_number provided - Retrieve specific PO folder
                self.logger.info(f"📁 Retrieving PO folder for project_number={project_number} and po_number={po_number}")
                project_folder_path = self.find_project_folder(project_number)
                if not project_folder_path:
                    self.logger.warning(f"⚠️ Unable to find project folder for project_number={project_number}")
                    return []

                # Ensure po_number has leading zero if it's a single digit
                formatted_po_number = f"{int(po_number):02}"
                self.logger.debug(f"📌 Formatted po_number: {formatted_po_number}")

                po_base_path = f"{project_folder_path}/1. Purchase Orders"
                po_folders = self.list_project_po_folders(po_base_path)
                if not po_folders:
                    self.logger.info(f"ℹ️ No PO folders found for project_number={project_number} at '{po_base_path}'")
                    return []

                # Search for the specific PO folder
                specific_po_folder = next(
                    (po for po in po_folders if po['name'].startswith(f"{project_number}_{formatted_po_number}")),
                    None
                )
                po_path = specific_po_folder['path'] + "/"
                files = self.list_files_in_folder(po_path)
                tax_form_link = ""
                form_type = ""
                for file in files:
                    match = re.search(self.TAX_FORM_REGEX, file["file_name"], re.IGNORECASE)
                    if match:
                        self.logger.info(f"💼 Identified as tax form: {file['file_name']}")
                        # get the share link
                        tax_form_link = self.create_share_link(po_path + file["file_name"])
                        if match.group(1):
                            form_type = "W-9"
                        elif match.group(2):
                            form_type = "W-8BEN"
                        elif match.group(3):
                            form_type = "W-8BEN-E"
                        elif match.group(4):
                            form_type = "W-8BEN-E"

                        self.logger.info(f"💼 Identified as tax form: {file['file_name']} ({form_type})")
                        continue
                project_po_data.append({
                    'po_folder_name': specific_po_folder["name"],
                    'po_folder_path': specific_po_folder["path"],
                    'po_tax_form_link': tax_form_link,
                    'form_type': form_type
                })
            else:
                self.logger.error("❗ Invalid combination of parameters provided.")
                return []

        except Exception as e:
            self.logger.exception(f"💥 An error occurred while retrieving PO folders: {e}")
            return []

        self.logger.info(f"✅ Retrieved {len(project_po_data)} PO folders based on the provided parameters.")
        return project_po_data

    def get_project_po_folders_with_link(
        self,
        project_number: Optional[str] = None,
        po_number: Optional[str] = None
    ) -> List[Dict[str, str]]:
        """
        Retrieves PO folders and their shared links based on provided parameters.

        Args:
            project_number (str, optional): The ID of the project. Defaults to None.
            po_number (str, optional): The number of the PO. Defaults to None.

        Returns:
            List[Dict[str, str]]: A list of dictionaries containing 'po_folder_name',
                                    'po_folder_path', and 'po_folder_link'.
        """
        project_po_data = []

        try:
            if project_number is None:
                # Scenario 1: No arguments provided - Retrieve all PO folders in the '2024' namespace
                self.logger.info("📁 No project_number provided. Retrieving all PO folders under the '2024' namespace.")
                all_projects = self.list_all_projects(namespace="2024")
                if not all_projects:
                    self.logger.warning("⚠️ No projects found under the '2024' namespace.")
                    return []

                for project in all_projects:
                    current_project_number = project['id']
                    project_folder_path = project['path']
                    po_base_path = f"{project_folder_path}/1. Purchase Orders"

                    po_folders = self.list_project_po_folders(po_base_path)
                    if not po_folders:
                        self.logger.info(f"ℹ️ No PO folders found for project_number={current_project_number} at '{po_base_path}'")
                        continue

                    for po_folder in po_folders:
                        po_link = self.create_share_link(po_folder["path"])
                        project_po_data.append({
                            'po_folder_name': po_folder['name'],
                            'po_folder_path': po_folder['path'],
                            'po_folder_link': po_link
                        })

            elif project_number and po_number is None:
                # Scenario 2: Only project_number provided - Retrieve all PO folders for the specified project
                self.logger.info(f"📁 Retrieving PO folders for project_number={project_number}")
                project_folder_path = self.find_project_folder(project_number)
                if not project_folder_path:
                    self.logger.warning(f"⚠️ Unable to find project folder for project_number={project_number}")
                    return []

                po_base_path = f"{project_folder_path}/1. Purchase Orders"
                po_folders = self.list_project_po_folders(po_base_path)
                if not po_folders:
                    self.logger.info(f"ℹ️ No PO folders found for project_number={project_number} at '{po_base_path}'")
                    return []

                for po_folder in po_folders:
                    po_link = self.create_share_link(po_folder["path"])
                    project_po_data.append({
                        'po_folder_name': po_folder['name'],
                        'po_folder_path': po_folder['path'],
                        'po_folder_link': po_link
                    })

            elif project_number and po_number:
                # Scenario 3: Both project_number and po_number provided - Retrieve specific PO folder
                self.logger.info(f"📁 Retrieving PO folder for project_number={project_number} and po_number={po_number}")
                project_folder_path = self.find_project_folder(project_number)
                if not project_folder_path:
                    self.logger.warning(f"⚠️ Unable to find project folder for project_number={project_number}")
                    return []

                # Ensure po_number has leading zero if it's a single digit
                formatted_po_number = f"{int(po_number):02}"
                self.logger.debug(f"📌 Formatted po_number: {formatted_po_number}")

                po_base_path = f"{project_folder_path}/1. Purchase Orders"
                po_folders = self.list_project_po_folders(po_base_path)
                if not po_folders:
                    self.logger.info(f"ℹ️ No PO folders found for project_number={project_number} at '{po_base_path}'")
                    return []

                # Search for the specific PO folder
                specific_po_folder = next(
                    (po for po in po_folders if po['name'].startswith(f"{project_number}_{formatted_po_number}")),
                    None
                )

                if specific_po_folder:
                    po_link = self.create_share_link(specific_po_folder["path"])
                    project_po_data.append({
                        'po_folder_name': specific_po_folder['name'],
                        'po_folder_path': specific_po_folder['path'],
                        'po_folder_link': po_link
                    })
                else:
                    self.logger.warning(f"⚠️ PO folder with po_number={formatted_po_number} not found in project_number={project_number}")
                    return []

            else:
                self.logger.error("❗ Invalid combination of parameters provided.")
                return []

        except Exception as e:
            self.logger.exception(f"💥 An error occurred while retrieving PO folders: {e}")
            return []

        self.logger.info(f"✅ Retrieved {len(project_po_data)} PO folders based on the provided parameters.")
        return project_po_data

    def list_all_projects(self, namespace: str) -> List[Dict[str, str]]:
        """
        Lists all project folders under the specified namespace.

        Args:
            namespace (str): The namespace (e.g., '2024') under which all projects reside.

        Returns:
            List[Dict[str, str]]: A list of projects, each dict with 'id' and 'path'.
        """
        # Construct the path root for the namespace
        dbx_namespaced = self.dbx.with_path_root(common.PathRoot.namespace_id(self.dbx_client.namespace_id))
        base_path = ""

        try:
            # List all folders at the namespace root
            result = dbx_namespaced.files_list_folder(base_path, recursive=False)
            entries = result.entries
            while result.has_more:
                result = dbx_namespaced.files_list_folder_continue(result.cursor)
                entries.extend(result.entries)

            projects = []
            for entry in entries:
                if isinstance(entry, FolderMetadata):
                    # We assume the project_number is in the folder name, e.g., "2416_ProjectName"
                    # If it's not, adjust accordingly.
                    folder_path = entry.path_lower
                    folder_name = entry.name
                    # Try extracting project_number from folder_name if needed
                    # For simplicity, assume the project_number is exactly the folder name or part of it.
                    # If the project naming convention isn't straightforward,
                    # you may need a more specific logic to find the project_number.
                    # Example: if folder_name starts with digits (project_number), parse them:
                    # project_number_str = re.match(r"(\d+)", folder_name).group(1)
                    # Use folder_name directly or parse based on your naming convention.
                    # In this example, we just return all folders as "projects"
                    projects.append({
                        "id": folder_name,
                        "path": folder_path
                    })

            return projects

        except ApiError as e:
            self.logger.error(f"❌ API error while listing projects: {e}")
            return []
        except Exception as e:
            self.logger.exception(f"💥 Error listing projects under '{namespace}': {e}")
            return []

    def find_project_folder(self, project_number: str, namespace: str = "2024") -> Optional[str]:
        """
        Searches for a folder whose name (or metadata) contains the given project_number under the specified namespace.

        Args:
            project_number (str): The ID of the project.
            namespace (str): The namespace under which projects are stored. Defaults to '2024'.

        Returns:
            Optional[str]: The path_lower of the matched project folder if found, else None.
        """
        self.logger.info(f"🔍 Searching for project folder with project_number='{project_number}' in namespace='{namespace}'.")

        # First, list all projects under the given namespace
        all_projects = self.list_all_projects(namespace=namespace)
        if not all_projects:
            self.logger.warning(f"⚠️ No projects found in namespace='{namespace}'.")
            return None

        # Attempt to find a project folder that matches the given project_number
        # Assuming that `project_number` is either exactly the folder name or a substring of the folder name.
        for project in all_projects:
            if str(project_number) in project['id']:
                self.logger.debug(f"✅ Found project folder: '{project['id']}' at '{project['path']}'")
                return project['path']

        self.logger.warning(f"⚠️ Project folder with project_number='{project_number}' not found in namespace='{namespace}'.")
        return None

    def create_share_link(self, dropbox_path: str) -> Optional[str]:
        """
        Creates a shared link for the specified Dropbox path within the team namespace.

        Args:
            dropbox_path (str): The path in Dropbox for which to create a share link.

        Returns:
            Optional[str]: The shared link if successful, else None.
        """
        try:
            normalized_path = dropbox_path.lstrip("")
            dbx_sharing = self.dbx.with_path_root(common.PathRoot.namespace_id(self.dbx_client.namespace_id))

            # Check if there's an existing shared link for this path
            existing_links = dbx_sharing.sharing_list_shared_links(
                path=normalized_path,
                direct_only=True
            ).links
            if existing_links:
                self.logger.info(f"Existing shared link found for '{normalized_path}': {existing_links[0].url}")
                return existing_links[0].url

            # If no link exists, create a new one
            settings = dropbox.sharing.SharedLinkSettings(
                requested_visibility=dropbox.sharing.RequestedVisibility.public
            )
            shared_link = dbx_sharing.sharing_create_shared_link_with_settings(normalized_path, settings)
            self.logger.info(f"Created new shared link for '{normalized_path}': {shared_link.url}")
            return shared_link.url

        except dropbox.exceptions.ApiError as e:
            self.logger.error(f"Error creating shared link for '{dropbox_path}': {e}")
            return None
        except Exception as e:
            self.logger.error(f"Error creating shared link for '{dropbox_path}': {e}")
            return None

    def list_project_po_folders(self, po_base_path: str) -> list:
        """
        Lists all PO folders under the given project's Purchase Orders directory.
        This method was already provided, but in case it wasn't implemented, here is a sample implementation.

        Args:
            po_base_path (str): The path to the project's "1. Purchase Orders" directory in Dropbox.

        Returns:
            list: A list of dictionaries representing PO folders, each dict containing 'name' and 'path'.
        """
        self.logger.debug(f"📂 Listing PO folders under '{po_base_path}'")
        all_entries = []
        dbx_namespaced = self.dbx.with_path_root(common.PathRoot.namespace_id(self.dbx_client.namespace_id))

        try:
            # Initial list_folder call
            result = dbx_namespaced.files_list_folder(path=po_base_path, recursive=False)

            # Accumulate all entries
            all_entries.extend(result.entries)

            # If there's more, continue listing
            while result.has_more:
                result = dbx_namespaced.files_list_folder_continue(result.cursor)
                all_entries.extend(result.entries)

        except ApiError as e:
            self.logger.exception(f"💥 Error listing PO folders in Dropbox under {po_base_path}: {e}", exc_info=True)
            return []
        except Exception as e:
            self.logger.exception(f"💥 Unexpected error listing PO folders under {po_base_path}: {e}", exc_info=True)
            return []

        # Filter only folder metadata and build the return list
        folders = []
        for entry in all_entries:
            if isinstance(entry, FolderMetadata):
                folder_path = entry.path_lower if entry.path_lower else entry.path_display
                folders.append({
                    "name": entry.name,
                    "path": folder_path
                })

        self.logger.debug(f"✅ Found {len(folders)} folders under '{po_base_path}'.")
        return folders

    def _update_monday_tax_form_link(self, pulse_id, new_link):
        """
        Update Monday contact's tax_form_link column with the new link.
        """
        if not pulse_id:
            self.logger.warning("No pulse_id to update Monday link.")
            return

        # Monday typically wants a JSON string: {"url": "...", "text": "..."}
        link_value = {"url": new_link, "text": "Tax Form"}
        column_values = {
            "tax_form_link_column": json.dumps(link_value)
        }
        try:
            self.monday_api.update_item(
                item_id=str(pulse_id),
                column_values=column_values,
                type="Contacts"  # Or however you designate the board
            )
            self.logger.info(
                f"✅ Updated Monday contact (pulse_id={pulse_id}) tax_form_link='{new_link}'."
            )
        except Exception as e:
            self.logger.exception(
                f"Failed to update Monday contact (pulse_id={pulse_id}) with link '{new_link}': {e}",
                exc_info=True
            )
dropbox_api = DropboxAPI()