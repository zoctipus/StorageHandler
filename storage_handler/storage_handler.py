"""
*******************************************************************************
*                                                                             *
*  Private and Confidential                                                   *
*                                                                             *
*  Unauthorized copying of this file, via any medium is strictly prohibited.  *
*  Proprietary and confidential.                                              *
*                                                                             *
*  © 2024 OctiLab. All rights reserved.                             *
*                                                                             *
*******************************************************************************
"""


from abc import ABC, abstractmethod
from pathlib import PosixPath
from typing import List, Union, Generator, Optional

class StorageHandler(ABC):
    """
    Abstract base class for storage handlers.
    Defines the interface for all storage operations.
    """
    
    @property
    def base_path(self) -> PosixPath:
        """
        Return the base path all path are relative to

        Returns:
            PosixPath: A PosixPath all (relative) path relative to.
        """
        pass

    @abstractmethod
    def list_files(self, prefix: Union[PosixPath, str] = "", relative: bool = True) -> List[str]:
        """
        List files under a given prefix.

        Args:
            prefix (Union[PosixPath, str]): The prefix to filter files.
            relative (bool): Whether the prefix is relative to the base path.

        Returns:
            List[str]: A list of file paths.
        """
        pass

    @abstractmethod
    def list_files_recursive(self, prefix: Union[PosixPath, str] = "", relative: bool = True) -> List[str]:
        """
        Recursively list files under a given prefix.

        Args:
            prefix (Union[PosixPath, str]): The prefix to filter files.
            relative (bool): Whether the prefix is relative to the base path.

        Returns:
            List[str]: A list of file paths.
        """
        pass

    @abstractmethod
    def glob_files(self, pattern: Union[PosixPath, str], relative: bool = True) -> List[str]:
        """
        Glob files matching a pattern.

        Args:
            pattern (Union[PosixPath, str]): The glob pattern to match files.
            relative (bool): Whether the pattern is relative to the base path.

        Returns:
            List[str]: A list of file paths matching the pattern.
        """
        pass

    @abstractmethod
    def upload_file(self, local_path: Union[PosixPath, str], remote_path: Union[PosixPath, str], relative: bool = True) -> None:
        """
        Upload a local file to remote storage.

        Args:
            local_path (Union[PosixPath, str]): Path to the local file.
            remote_path (Union[PosixPath, str]): Path to the remote file.
            relative (bool): Whether the remote path is relative to the base path.
        """
        pass

    @abstractmethod
    def download_file(self, remote_path: Union[PosixPath, str], local_path: Union[PosixPath, str], relative: bool = True) -> None:
        """
        Download a remote file to local storage.

        Args:
            remote_path (Union[PosixPath, str]): Path to the remote file.
            local_path (Union[PosixPath, str]): Path to save the downloaded file locally.
            relative (bool): Whether the remote path is relative to the base path.
        """
        pass

    @abstractmethod
    def read_file(self, remote_path: Union[PosixPath, str], relative: bool = True) -> bytes:
        """
        Read a remote file and return its content as bytes.

        Args:
            remote_path (Union[PosixPath, str]): Path to the remote file.
            relative (bool): Whether the remote path is relative to the base path.

        Returns:
            bytes: Content of the remote file.
        """
        pass

    @abstractmethod
    def write_file(self, remote_path: Union[PosixPath, str], data: bytes, relative: bool = True) -> None:
        """
        Write data to a remote file.

        Args:
            remote_path (Union[PosixPath, str]): Path to the remote file.
            data (bytes): Data to write to the file.
            relative (bool): Whether the remote path is relative to the base path.
        """
        pass

    @abstractmethod
    def delete_file(self, remote_path: Union[PosixPath, str], relative: bool = True) -> None:
        """
        Delete a remote file.

        Args:
            remote_path (Union[PosixPath, str]): Path to the remote file.
            relative (bool): Whether the remote path is relative to the base path.
        """
        pass
    
    def delete_directory(self, remote_path: Union[PosixPath, str], relative: bool = True, recursive: bool = True) -> None:
        """
        Delete a remote directory.

        Args:
            remote_path (Union[PosixPath, str]): Path to the remote directory.
            relative (bool): Whether the path is relative to `base_path`.
            recursive (bool): Whether to delete the directory recursively, including all contents.

        Raises:
            Exception: For any exceptions that occur during the deletion.
        """
        pass

    @abstractmethod
    def exists(self, remote_path: Union[PosixPath, str], relative: bool = True) -> bool:
        """
        Check if a remote file exists.

        Args:
            remote_path (Union[PosixPath, str]): Path to the remote file.
            relative (bool): Whether the remote path is relative to the base path.

        Returns:
            bool: True if the file exists, False otherwise.
        """
        pass

    @abstractmethod
    def rename(self, old_remote_path: Union[PosixPath, str], new_remote_path: Union[PosixPath, str], relative: bool = True) -> None:
        """
        Rename a remote file.

        Args:
            old_remote_path (Union[PosixPath, str]): Current path to the remote file.
            new_remote_path (Union[PosixPath, str]): New path for the remote file.
            relative (bool): Whether the paths are relative to the base path.
        """
        pass

    @abstractmethod
    def copy(self, source_remote_path: Union[PosixPath, str], destination_remote_path: Union[PosixPath, str], relative: bool = True) -> None:
        """
        Copy a remote file to another location within the same storage.

        Args:
            source_remote_path (Union[PosixPath, str]): Path to the source remote file.
            destination_remote_path (Union[PosixPath, str]): Path to the destination remote file.
            relative (bool): Whether the paths are relative to the base path.
        """
        pass

    @abstractmethod
    def create_directory(self, remote_path: Union[PosixPath, str], relative: bool = True) -> None:
        """
        Create a directory in remote storage.

        Args:
            remote_path (Union[PosixPath, str]): Path to the directory.
            relative (bool): Whether the path is relative to the base path.
        """
        pass

    @abstractmethod
    def get_file_metadata(self, remote_path: Union[PosixPath, str], relative: bool = True) -> Optional[dict]:
        """
        Retrieve metadata of a remote file.

        Args:
            remote_path (Union[PosixPath, str]): Path to the remote file.
            relative (bool): Whether the path is relative to the base path.

        Returns:
            Optional[dict]: Metadata of the file if it exists, else None.
        """
        pass

    @abstractmethod
    def stream_read(self, remote_path: Union[PosixPath, str], chunk_size: int = 1024 * 1024, relative: bool = True) -> Generator[bytes, None, None]:
        """
        Stream read a remote file in chunks.

        Args:
            remote_path (Union[PosixPath, str]): Path to the remote file.
            chunk_size (int): Size of each chunk in bytes.
            relative (bool): Whether the path is relative to the base path.

        Yields:
            bytes: Chunks of data read from the file.
        """
        pass

    @abstractmethod
    def stream_write(self, remote_path: Union[PosixPath, str], data_generator: Generator[bytes, None, None], relative: bool = True) -> None:
        """
        Stream write data to a remote file in chunks.

        Args:
            remote_path (Union[PosixPath, str]): Path to the remote file.
            data_generator (Generator[bytes, None, None]): Generator yielding chunks of data.
            relative (bool): Whether the path is relative to the base path.
        """
        pass

    @abstractmethod
    def generate_presigned_url(self, remote_path: Union[PosixPath, str], expiration: int = 3600, relative: bool = True) -> str:
        """
        Generate a presigned URL for accessing the remote file.

        Args:
            remote_path (Union[PosixPath, str]): Path to the remote file.
            expiration (int): Time in seconds for the URL to remain valid.
            relative (bool): Whether the path is relative to the base path.

        Returns:
            str: Presigned URL.
        """
        pass

    @abstractmethod
    def set_permissions(self, remote_path: Union[PosixPath, str], acl: str, relative: bool = True) -> None:
        """
        Set permissions for a remote file.

        Args:
            remote_path (Union[PosixPath, str]): Path to the remote file.
            acl (str): Access Control List policy (e.g., 'public-read').
            relative (bool): Whether the path is relative to the base path.
        """
        pass

    @abstractmethod
    def sync_from_local(self, local_dir: Union[PosixPath, str], remote_dir: Union[PosixPath, str], relative: bool = True) -> None:
        """
        Synchronize a local directory to remote storage.

        Args:
            local_dir (Union[PosixPath, str]): Path to the local directory.
            remote_dir (Union[PosixPath, str]): Path to the remote directory.
            relative (bool): Whether the remote path is relative to the base path.
        """
        pass

    @abstractmethod
    def sync_to_local(self, remote_dir: Union[PosixPath, str], local_dir: Union[PosixPath, str], relative: bool = True) -> None:
        """
        Synchronize a remote directory to local storage.

        Args:
            remote_dir (Union[PosixPath, str]): Path to the remote directory.
            local_dir (Union[PosixPath, str]): Path to the local directory.
            relative (bool): Whether the remote path is relative to the base path.
        """
        pass

    @abstractmethod
    def compress_and_upload(self, local_path: Union[PosixPath, str], remote_path: Union[PosixPath, str], relative: bool = True) -> None:
        """
        Compress a local file and upload it to remote storage.

        Args:
            local_path (Union[PosixPath, str]): Path to the local file.
            remote_path (Union[PosixPath, str]): Path to the remote file.
            relative (bool): Whether the remote path is relative to the base path.
        """
        pass

    @abstractmethod
    def download_and_decompress(self, remote_path: Union[PosixPath, str], local_path: Union[PosixPath, str], relative: bool = True) -> None:
        """
        Download a compressed remote file and decompress it locally.

        Args:
            remote_path (Union[PosixPath, str]): Path to the remote compressed file.
            local_path (Union[PosixPath, str]): Path to save the decompressed file locally.
            relative (bool): Whether the remote path is relative to the base path.
        """
        pass

    @abstractmethod
    def safe_write_file(self, remote_path: Union[PosixPath, str], data: bytes, relative: bool = True) -> None:
        """
        Safely write data to a remote file using file locking.

        Args:
            remote_path (Posix): Path to the remote file.
            data (bytes): Data to write.
            relative (bool): Whether the path is relative to the base path.
        """
        pass
