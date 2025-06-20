"""
This module provides a comprehensive suite of utility functions for managing files and directories in a filesystem. It is designed to support common operations such as metadata extraction, listing contents, renaming, copying, moving, deleting, compressing, and extracting files or folders. The module is particularly useful for scripting, automation, and system analysis tasks.

Key Functionalities:
--------------------
1. **Metadata Retrieval**:
   - `get_metadata(path: str) -> dict`: Returns detailed metadata for a given file or directory, including timestamps, permissions, ownership, and size.

2. **Directory Listing**:
   - `list_folder(folder: str) -> list[dict]`: Lists all items in a directory and returns their metadata.

3. **File/Folder Renaming**:
   - `rename(path: str, new_name: str) -> dict`: Renames a file or folder and returns updated metadata.

4. **Copying**:
   - `copy(path: str, destination: str) -> dict`: Copies a file or directory to a new location, handling name conflicts and directory recursion.

5. **Moving**:
   - `move(path: str, destination: str) -> dict`: Moves a file or directory to a new location, with conflict resolution.

6. **Deletion**:
   - `delete(path: str) -> None`: Deletes a file or directory, including recursive deletion for folders.

7. **Archiving**:
   - `create_archive(archive_name: str, folder_path: str, archive_path: str, format: str = 'zip') -> dict`: Compresses a folder into an archive using supported formats like zip, tar, gztar, etc.

8. **Extraction**:
   - `extract_archive(archive_path: str, destination: str) -> None`: Extracts the contents of an archive to a specified directory.

Error Handling:
---------------
Each function includes robust error handling and raises appropriate exceptions such as:
- `FileNotFoundError` for missing paths.
- `OSError` for operational failures like permission issues or invalid operations.

Dependencies:
-------------
- `os`: Provides functions for interacting with the operating system, including file path manipulation and metadata access.
- `shutil`: Offers high-level operations on files and collections of files, such as copying, moving, and archiving.
- `datetime`: Used for converting and formatting timestamps into human-readable strings.

Usage Example:
--------------
```python
from file_utils import get_metadata, list_folder

metadata = get_metadata('/path/to/file.txt')
print(metadata)

contents = list_folder('/path/to/folder')
for item in contents:
    print(item['name'], item['size_in_bytes'])
```
"""

import os
import shutil
from datetime import datetime


def get_metadata(path: str) -> dict:
    """
    Get metadata of the specified path.

    Args:
        path (str): The location of the file/folder.

    Returns:
        dict: A dictionary containing the file/folder metadata with the following fields:
            - name: The name of the file/folder.
            - path: The absolute path of the file/folder.
            - extension: The file extension (empty for folders).
            - size_in_bytes: The size of the file in bytes.
            - creation_time: The creation time of the file/folder.
            - creation_time_str: The creation time as a formatted string.
            - modification_time: The last modification time of the file/folder.
            - modification_time_str: The last modification time as a formatted string.
            - access_time: The last access time of the file/folder.
            - access_time_str: The last access time as a formatted string.
            - mode: The mode (permissions) of the file/folder.
            - inode: The inode number of the file/folder.
            - device: The device number of the file/folder.
            - nlink: The number of hard links to the file/folder.
            - uid: The user ID of the file/folder owner.
            - gid: The group ID of the file/folder owner.
            - is_directory: Boolean indicating if the path is a directory.

    Raises:
        FileNotFoundError: If the file does not exist.
    """
    if not os.path.exists(path):
        raise FileNotFoundError(f"The file at {path} does not exist.")

    stat_info = os.stat(path)
    metadata = {
        "name": os.path.basename(path),
        "path": os.path.abspath(path),
        "extension": os.path.splitext(path)[1],
        "size_in_bytes": datetime.fromtimestamp(stat_info.st_size),
        "creation_time": datetime.fromtimestamp(stat_info.st_birthtime),
        "creation_time_str": datetime.fromtimestamp(stat_info.st_birthtime).strftime(
            "%Y-%m-%d %H:%M:%S.%f"
        ),
        "modification_time": datetime.fromtimestamp(stat_info.st_mtime),
        "modification_time_str": datetime.fromtimestamp(stat_info.st_mtime).strftime(
            "%Y-%m-%d %H:%M:%S.%f"
        ),
        "access_time": datetime.fromtimestamp(stat_info.st_atime),
        "access_time_str": datetime.fromtimestamp(stat_info.st_atime).strftime(
            "%Y-%m-%d %H:%M:%S.%f"
        ),
        "mode": stat_info.st_mode,
        "inode": stat_info.st_ino,
        "device": stat_info.st_dev,
        "nlink": stat_info.st_nlink,
        "uid": stat_info.st_uid,
        "gid": stat_info.st_gid,
        "is_directory": os.path.isdir(path),
    }

    return metadata


def list_folder(folder: str) -> list[dict]:
    """
    List all files and folders in the specified folder.

    Args:
        folder (str): The folder to list files and folders from.

    Returns:
        list: A list of dictionaries containing metadata of each item.

    Returns:
        list: A list of dictionaries containing metadata of each item.
            dict:
                - name: The name of the file/folder.
                - path: The absolute path of the file/folder.
                - extension: The file extension (empty for folders).
                - size_in_bytes: The size of the file in bytes.
                - creation_time: The creation time of the file/folder.
                - creation_time_str: The creation time as a formatted string.
                - modification_time: The last modification time of the file/folder.
                - modification_time_str: The last modification time as a formatted string.
                - access_time: The last access time of the file/folder.
                - access_time_str: The last access time as a formatted string.
                - mode: The mode (permissions) of the file/folder.
                - inode: The inode number of the file/folder.
                - device: The device number of the file/folder.
                - nlink: The number of hard links to the file/folder.
                - uid: The user ID of the file/folder owner.
                - gid: The group ID of the file/folder owner.
                - is_directory: Boolean indicating if the path is a directory.

    Raises:
        FileNotFoundError: If the folder does not exist.
    """
    if not os.path.exists(folder):
        raise FileNotFoundError(f"The folder at {folder} does not exist.")

    return [get_metadata(os.path.join(folder, item)) for item in os.listdir(folder)]


def rename(path: str, new_name: str) -> dict:
    """
    Rename the specified path.

    Args:
        path (str): The location of the file/folder to be renamed.
        new_name (str): The new name of the file/folder.

    Returns:
        dict: A dictionary containing the file/folder metadata with the following fields:
            - name: The name of the file/folder.
            - path: The absolute path of the file/folder.
            - extension: The file extension (empty for folders).
            - size_in_bytes: The size of the file in bytes.
            - creation_time: The creation time of the file/folder.
            - creation_time_str: The creation time as a formatted string.
            - modification_time: The last modification time of the file/folder.
            - modification_time_str: The last modification time as a formatted string.
            - access_time: The last access time of the file/folder.
            - access_time_str: The last access time as a formatted string.
            - mode: The mode (permissions) of the file/folder.
            - inode: The inode number of the file/folder.
            - device: The device number of the file/folder.
            - nlink: The number of hard links to the file/folder.
            - uid: The user ID of the file/folder owner.
            - gid: The group ID of the file/folder owner.
            - is_directory: Boolean indicating if the path is a directory.

    Raises:
        FileNotFoundError: If the file does not exist.
        OSError: If the file/folder cannot be renamed.
    """
    if not os.path.exists(path):
        raise FileNotFoundError(f"The file/folder at {path} does not exist.")

    try:
        new_location = os.path.join(os.path.dirname(path), new_name)
        os.rename(path, new_location)
    except OSError as e:
        raise OSError(
            f"Failed to rename the file/folder {path} to {new_name}. Error: {e}"
        ) from e

    return get_metadata(new_location)


def copy(path: str, destination: str) -> dict:
    """
    Copy a file or folder to the destination.

    Args:
        path (str): The path to the file or folder to copy.
        destination (str): The destination path (can be a folder or a full file path).

    Returns:
        dict: A dictionary containing the file/folder metadata with the following fields:
            - name: The name of the file/folder.
            - path: The absolute path of the file/folder.
            - extension: The file extension (empty for folders).
            - size_in_bytes: The size of the file in bytes.
            - creation_time: The creation time of the file/folder.
            - creation_time_str: The creation time as a formatted string.
            - modification_time: The last modification time of the file/folder.
            - modification_time_str: The last modification time as a formatted string.
            - access_time: The last access time of the file/folder.
            - access_time_str: The last access time as a formatted string.
            - mode: The mode (permissions) of the file/folder.
            - inode: The inode number of the file/folder.
            - device: The device number of the file/folder.
            - nlink: The number of hard links to the file/folder.
            - uid: The user ID of the file/folder owner.
            - gid: The group ID of the file/folder owner.
            - is_directory: Boolean indicating if the path is a directory.

    Raises:
        FileNotFoundError: If the source path does not exist.
        OSError: If the copy operation fails.
    """
    if not os.path.exists(path):
        raise FileNotFoundError(f"The path {path} does not exist.")

    try:
        if os.path.isfile(path):
            if os.path.isdir(destination):
                destination = os.path.join(destination, os.path.basename(path))
            if os.path.exists(destination):
                base, ext = os.path.splitext(destination)
                destination = f"{base}_copy{ext}"
            shutil.copy(path, destination)
        elif os.path.isdir(path):
            abs_path = os.path.abspath(path)
            abs_dest = os.path.abspath(destination)
            if abs_dest.startswith(abs_path):
                raise OSError("Cannot copy a folder into one of its subdirectories.")
            if os.path.exists(destination):
                destination = f"{destination}_copy"
            shutil.copytree(path, destination)
        else:
            raise OSError(f"The path {path} is neither a file nor a directory.")
    except OSError as e:
        raise OSError(f"Failed to copy {path} to {destination}. Error: {e}") from e

    return get_metadata(destination)


def move(path: str, destination: str) -> dict:
    """
    Move the specified file/folder to the destination folder.

    Args:
        path (str): The location of the file/folder to be moved.
        destination (str): The folder where the file/folder will be moved.

    Returns:
        dict: A dictionary containing the file/folder metadata with the following fields:
            - name: The name of the file/folder.
            - path: The absolute path of the file/folder.
            - extension: The file extension (empty for folders).
            - size_in_bytes: The size of the file in bytes.
            - creation_time: The creation time of the file/folder.
            - creation_time_str: The creation time as a formatted string.
            - modification_time: The last modification time of the file/folder.
            - modification_time_str: The last modification time as a formatted string.
            - access_time: The last access time of the file/folder.
            - access_time_str: The last access time as a formatted string.
            - mode: The mode (permissions) of the file/folder.
            - inode: The inode number of the file/folder.
            - device: The device number of the file/folder.
            - nlink: The number of hard links to the file/folder.
            - uid: The user ID of the file/folder owner.
            - gid: The group ID of the file/folder owner.
            - is_directory: Boolean indicating if the path is a directory.

    Raises:
        FileNotFoundError: If the file/folder does not exist.
        OSError: If the file/folder cannot be moved.
    """
    if not os.path.exists(path):
        raise FileNotFoundError(f"The file/folder at {path} does not exist.")

    try:
        if os.path.isdir(destination):
            destination = os.path.join(destination, os.path.basename(path))
        if os.path.exists(destination):
            base, ext = os.path.splitext(destination)
            destination = f"{base}_copy{ext}"
        os.makedirs(os.path.dirname(destination), exist_ok=True)
        shutil.move(path, destination)
    except OSError as e:
        raise OSError(
            f"Failed to move the file/folder {path} to {destination}. Error: {e}"
        ) from e

    return get_metadata(destination)


def delete(path: str) -> None:
    """
    Delete the specified file/folder.

    Args:
        path (str): The location of the file/folder to be deleted.

    Raises:
        FileNotFoundError: If the file/folder does not exist.
        OSError: If the file/folder cannot be deleted.
    """
    if not os.path.exists(path):
        raise FileNotFoundError(f"The file/folder at {path} does not exist.")

    try:
        if os.path.isdir(path):
            shutil.rmtree(path)
        else:
            os.remove(path)
    except OSError as e:
        raise OSError(f"Failed to delete the file/folder at {path}. Error: {e}") from e


def create_archive(
    archive_name: str, folder_path: str, archive_path: str, archive_format: str = "zip"
) -> dict:
    """
    Compress the specified folder into an archive file.

    Args:
        archive_name (str): The name of the archive file (without extension).
        folder_path (str): The path of the folder to be compressed.
        archive_path (str): The directory where the archive will be saved.
        archive_format (str): The archive format ('zip', 'tar', 'gztar', 'bztar', or 'xztar'). Defaults to 'zip'.

    Returns:
        dict: A dictionary containing the file/folder metadata with the following fields:
            - name: The name of the file/folder.
            - path: The absolute path of the file/folder.
            - extension: The file extension (empty for folders).
            - size_in_bytes: The size of the file in bytes.
            - creation_time: The creation time of the file/folder.
            - creation_time_str: The creation time as a formatted string.
            - modification_time: The last modification time of the file/folder.
            - modification_time_str: The last modification time as a formatted string.
            - access_time: The last access time of the file/folder.
            - access_time_str: The last access time as a formatted string.
            - mode: The mode (permissions) of the file/folder.
            - inode: The inode number of the file/folder.
            - device: The device number of the file/folder.
            - nlink: The number of hard links to the file/folder.
            - uid: The user ID of the file/folder owner.
            - gid: The group ID of the file/folder owner.
            - is_directory: Boolean indicating if the path is a directory.

    Raises:
        FileNotFoundError: If the folder does not exist.
        OSError: If the folder cannot be compressed.
    """
    if not os.path.exists(folder_path):
        raise FileNotFoundError(f"The folder at {folder_path} does not exist.")

    try:
        os.makedirs(archive_path, exist_ok=True)
        full_archive_base = os.path.join(archive_path, archive_name)
        if os.path.exists(f"{full_archive_base}.{archive_format}"):
            full_archive_base = f"{full_archive_base}_copy"
        shutil.make_archive(full_archive_base, archive_format, folder_path)
    except OSError as e:
        raise OSError(
            f"Failed to compress the folder {folder_path} into {full_archive_base}.{archive_format}. Error: {e}"
        ) from e

    return get_metadata(f"{full_archive_base}.{archive_format}")


def extract_archive(archive_path: str, destination: str) -> None:
    """
    Extract the contents of an archive file to the specified destination folder.

    Args:
        archive_path (str): The location of the archive file.
        destination (str): The folder where the contents will be extracted.

    Raises:
        FileNotFoundError: If the archive file does not exist.
        OSError: If the contents cannot be extracted.
    """
    if not os.path.exists(archive_path):
        raise FileNotFoundError(f"The archive file at {archive_path} does not exist.")

    try:
        os.makedirs(destination, exist_ok=True)
        shutil.unpack_archive(archive_path, destination)
    except (shutil.ReadError, OSError) as e:
        raise OSError(
            f"Failed to extract the contents of {archive_path} to {destination}. Error: {e}"
        ) from e
