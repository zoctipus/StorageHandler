"""
*******************************************************************************
*                                                                             *
*  Private and Confidential                                                   *
*                                                                             *
*  Unauthorized copying of this file, via any medium is strictly prohibited.  *
*  Proprietary and confidential.                                              *
*                                                                             *
*  © 2024 OctiLab. All rights reserved.                                       *
*                                                                             *
*******************************************************************************
"""


from typing import List, Optional, Generator
import fsspec
import os
import gzip
import logging
from pathlib import PosixPath
from filelock import FileLock
from .storage_handler import StorageHandler

# Configure logging
logging.basicConfig(
    level=logging.INFO,  # Set to DEBUG for more detailed logs
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

class UnifiedStorageHandler(StorageHandler):
    """
    A unified handler for interacting with various storage backends using fsspec.

    Supports protocols like local filesystem, SFTP, Google Cloud Storage, etc.
    """

    SUPPORTED_PROTOCOLS = ['file', 'sftp', 'gs', 's3']  # Extend as needed

    def __init__(self, storage_url: str, **kwargs):
        """
        Initialize the filesystem based on the storage URL.

        Args:
            storage_url (str): The URL specifying the storage backend.
                                Examples:
                                - 'file:///path/to/local/storage'
                                - 'gs://your-gcs-bucket/'
                                - 'sftp://user@host/path/on/remote/server'
            **kwargs: Additional configuration parameters.
                      For example, for SFTP:
                      - username
                      - password
                      - key_filename
                      - port
        """
        self.storage_url = storage_url
        protocol, _, path = storage_url.partition("://")
        if protocol not in self.SUPPORTED_PROTOCOLS:
            raise ValueError(f"Unsupported protocol: {protocol}")

        fs_kwargs = {}
        if protocol == 'gs':
            fs_kwargs['project'] = kwargs.get('project', 'My First Project')
            fs_kwargs['token'] = kwargs.get('token') or os.environ.get('GOOGLE_APPLICATION_CREDENTIALS')
        elif protocol == 'sftp':
            fs_kwargs['host'] = kwargs.get('host')
            if not fs_kwargs['host']:
                raise ValueError("Host must be provided for SFTP protocol.")
            fs_kwargs['username'] = kwargs.get('username')
            fs_kwargs['password'] = kwargs.get('password')
            fs_kwargs['port'] = kwargs.get('port', 22)
        elif protocol == 's3':
            fs_kwargs['key'] = kwargs.get('aws_access_key_id') or os.getenv('AWS_ACCESS_KEY_ID')
            fs_kwargs['secret'] = kwargs.get('aws_secret_access_key') or os.getenv('AWS_SECRET_ACCESS_KEY')
            fs_kwargs['token'] = kwargs.get('token') or os.getenv('AWS_SESSION_TOKEN')
            fs_kwargs['client_kwargs'] = kwargs.get('client_kwargs', {})
        elif protocol == 'file':
            pass  # Local filesystem doesn't require additional kwargs
        else:
            raise ValueError(f"Unsupported protocol: {protocol}")

        self.fs = fsspec.filesystem(protocol, **fs_kwargs)
        self.base_path = path.rstrip('/')

        logger.info(f"Initialized UnifiedStorageHandler with protocol '{protocol}' and base path '{self.base_path}'")

    def _prepare_remote_path(self, remote_path: PosixPath, relative: bool = True) -> str:
        """
        Constructs the full remote path and ensures the remote directory exists.

        Args:
            remote_path (PosixPath): The path in remote storage.
            relative (bool): Whether the path is relative to `base_path`.

        Returns:
            str: The full remote path.

        Raises:
            FileNotFoundError: If the remote directory cannot be created.
        """
        # Use base_path only if path is relative
        target_path = self.base_path / remote_path if relative else remote_path

        # Determine the remote directory using posixpath
        remote_dir = target_path.parent

        # Ensure the remote directory exists
        if not self.fs.exists(str(remote_dir)):
            try:
                self.fs.makedirs(str(remote_dir), exist_ok=True)
                logger.info(f"Created remote directory '{remote_dir}'.")
            except Exception as e:
                logger.error(f"Failed to create remote directory '{remote_dir}': {e}")
                raise FileNotFoundError(f"Could not create remote directory '{remote_dir}'.") from e

        return target_path

    def list_files(self, prefix: PosixPath = PosixPath(''), relative: bool = True) -> List[PosixPath]:
        full_path = self.base_path / prefix if relative else prefix
        return [PosixPath(f) for f in self.fs.ls(str(full_path), detail=False)]

    def list_files_recursive(self, prefix: PosixPath = PosixPath(''), relative: bool = True) -> List[PosixPath]:
        full_path = self.base_path / prefix if relative else prefix
        return [PosixPath(f) for f in self.fs.find(str(full_path))]

    def glob_files(self, pattern: PosixPath, relative: bool = True) -> List[PosixPath]:
        full_pattern = self.base_path / pattern if relative else pattern
        return [PosixPath(f) for f in self.fs.glob(str(full_pattern))]

    def upload_file(self, local_path: PosixPath, remote_path: PosixPath, relative: bool = True) -> None:
        """
        Upload a local file to remote storage, ensuring that the target directory exists.

        Args:
            local_path (PosixPath): Path to the local file.
            remote_path (PosixPath): Path to the remote file.
            relative (bool): Whether the path is relative to `base_path`.

        Raises:
            FileNotFoundError: If the local file does not exist or remote directory cannot be created.
            Exception: For any other exceptions that occur during the upload.
        """
        # Check if local file exists
        if not local_path.exists():
            logger.error(f"Local file '{local_path}' does not exist.")
            raise FileNotFoundError(f"Local file '{local_path}' does not exist.")

        # Prepare the full remote path and ensure directory exists
        target_path = self._prepare_remote_path(remote_path, relative)

        try:
            # Upload the file
            self.fs.put_file(str(local_path), str(target_path))
            logger.info(f"Uploaded '{local_path}' to '{target_path}'.")
        except Exception as e:
            logger.error(f"Failed to upload '{local_path}' to '{target_path}': {e}")
            raise

    def download_file(self, remote_path: PosixPath, local_path: PosixPath, relative: bool = True) -> None:
        """
        Download a remote file to local storage.

        Args:
            remote_path (PosixPath): Path to the remote file.
            local_path (PosixPath): Path to save the downloaded file locally.
            relative (bool): Whether the path is relative to `base_path`.

        Raises:
            FileNotFoundError: If the remote file does not exist.
            Exception: For any other exceptions that occur during the download.
        """
        source_path = self.base_path / remote_path if relative else remote_path
        try:
            self.fs.get_file(str(source_path), str(local_path))
            logger.info(f"Downloaded '{source_path}' to '{local_path}'.")
        except FileNotFoundError as e:
            logger.error(f"The remote file '{source_path}' does not exist.")
            raise FileNotFoundError(f"The remote file '{source_path}' does not exist.") from e
        except Exception as e:
            logger.error(f"Failed to download '{source_path}' to '{local_path}': {e}")
            raise

    def read_file(self, remote_path: PosixPath, relative: bool = True) -> bytes:
        """
        Read a remote file and return its content as bytes.

        Args:
            remote_path (PosixPath): Path to the remote file.
            relative (bool): Whether the path is relative to `base_path`.

        Returns:
            bytes: Content of the remote file.

        Raises:
            FileNotFoundError: If the remote file does not exist.
            Exception: For any other exceptions that occur during the read operation.
        """
        source_path = self.base_path / remote_path if relative else remote_path
        try:
            with self.fs.open(str(source_path), 'rb') as f:
                data = f.read()
            logger.info(f"Read file '{source_path}'.")
            return data
        except FileNotFoundError as e:
            logger.error(f"The file '{source_path}' does not exist.")
            raise FileNotFoundError(f"The file '{source_path}' does not exist.") from e
        except Exception as e:
            logger.error(f"An error occurred while reading '{source_path}': {e}")
            raise

    def write_file(self, remote_path: PosixPath, data: bytes, relative: bool = True) -> None:
        """
        Write data directly to a remote file, ensuring that the target directory exists.

        Args:
            remote_path (str): The path in remote storage where the data will be written.
            data (bytes): The data to write to the remote file.
            relative (bool): Whether the path is relative to `base_path`.

        Raises:
            FileNotFoundError: If the remote directory cannot be created.
            Exception: For any other exceptions that occur during the write operation.
        """
        # Prepare the full remote path and ensure directory exists
        target_path = self._prepare_remote_path(remote_path, relative)

        try:
            with self.fs.open(str(target_path), 'wb') as f:
                f.write(data)
            logger.info(f"Wrote data to '{target_path}'.")
        except Exception as e:
            logger.error(f"Failed to write data to '{target_path}': {e}")
            raise

    def delete_file(self, remote_path: PosixPath, relative: bool = True) -> None:
        """
        Delete a remote file.

        Args:
            remote_path (PosixPath): Path to the remote file.
            relative (bool): Whether the path is relative to `base_path`.

        Raises:
            Exception: For any exceptions that occur during the deletion.
        """
        target_path = self.base_path / remote_path if relative else remote_path
        try:
            self.fs.rm_file(str(target_path))
            logger.info(f"Deleted '{target_path}'.")
        except FileNotFoundError:
            logger.warning(f"The file '{target_path}' does not exist.")
        except Exception as e:
            logger.error(f"Failed to delete '{target_path}': {e}")
            raise

    def exists(self, remote_path: PosixPath, relative: bool = True) -> bool:
        """
        Check if a remote file exists.

        Args:
            remote_path (PosixPath): Path to the remote file.
            relative (bool): Whether the path is relative to `base_path`.

        Returns:
            bool: True if the file exists, False otherwise.
        """
        target_path = self.base_path / remote_path if relative else remote_path
        exists = self.fs.exists(str(target_path))
        logger.info(f"Checked existence for '{target_path}': {exists}")
        return exists

    def rename(self, old_remote_path: str, new_remote_path: str, relative: bool = True) -> None:
        """
        Rename a remote file.

        Args:
            old_remote_path (str): Current path to the remote file.
            new_remote_path (str): New path for the remote file.
            relative (bool): Whether the paths are relative to `base_path`.

        Raises:
            Exception: For any exceptions that occur during the rename.
        """
        old_path = self.base_path / old_remote_path if relative else old_remote_path
        new_path = self.base_path / new_remote_path if relative else new_remote_path

        try:
            self.fs.mv(str(old_path), str(new_path))
            logger.info(f"Renamed '{old_path}' to '{new_path}'.")
        except Exception as e:
            logger.error(f"Failed to rename '{old_path}' to '{new_path}': {e}")
            raise

    def copy(self, source_remote_path: str, destination_remote_path: str, relative: bool = True) -> None:
        """
        Copy a remote file to another location within the same storage.

        Args:
            source_remote_path (str): Path to the source remote file.
            destination_remote_path (str): Path to the destination remote file.
            relative (bool): Whether the paths are relative to `base_path`.

        Raises:
            Exception: For any exceptions that occur during the copy.
        """
        source_path = self.base_path / source_remote_path if relative else source_remote_path
        destination_path = self.base_path, destination_remote_path if relative else destination_remote_path
        try:
            self.fs.copy(str(source_path), str(destination_path))
            logger.info(f"Copied '{source_path}' to '{destination_path}'.")
        except Exception as e:
            logger.error(f"Failed to copy '{source_path}' to '{destination_path}': {e}")
            raise

    def create_directory(self, remote_path: PosixPath, relative: bool = True) -> None:
        """
        Create a directory in remote storage.

        Args:
            remote_path (PosixPath): Path to the directory.
            relative (bool): Whether the path is relative to `base_path`.

        Raises:
            Exception: For any exceptions that occur during directory creation.
        """
        target_path = self.base_path / remote_path if relative else remote_path
        try:
            self.fs.makedirs(str(target_path), exist_ok=True)
            logger.info(f"Created directory '{target_path}'.")
        except Exception as e:
            logger.error(f"Failed to create directory '{target_path}': {e}")
            raise

    def get_file_metadata(self, remote_path: str, relative: bool = True) -> Optional[dict]:
        """
        Retrieve metadata of a remote file.

        Args:
            remote_path (str): Path to the remote file.
            relative (bool): Whether the path is relative to `base_path`.

        Returns:
            Optional[dict]: Metadata of the file if it exists, else None.
        """
        target_path = self.base_path / remote_path if relative else remote_path
        if not self.fs.exists(str(target_path)):
            logger.warning(f"The file '{target_path}' does not exist.")
            return None
        try:
            info = self.fs.info(str(target_path))
            metadata = {
                'size': info.get('size'),
                'type': info.get('type'),
                'name': info.get('name'),
                'modified': info.get('mtime')  # mtime is a standard key for modification time
            }
            logger.info(f"Retrieved metadata for '{target_path}': {metadata}")
            return metadata
        except Exception as e:
            logger.error(f"Failed to retrieve metadata for '{target_path}': {e}")
            raise

    def stream_read(self, remote_path: PosixPath, chunk_size: int = 1024 * 1024, relative: bool = True) -> Generator[bytes, None, None]:
        """
        Stream read a remote file in chunks.

        Args:
            remote_path (PosixPath): Path to the remote file.
            chunk_size (int, optional): Size of each chunk in bytes. Defaults to 1MB.
            relative (bool): Whether the path is relative to `base_path`.

        Yields:
            bytes: Chunks of data read from the file.

        Raises:
            Exception: For any exceptions that occur during streaming.
        """
        source_path = self.base_path / remote_path if relative else remote_path
        try:
            with self.fs.open(str(source_path), 'rb') as f:
                while True:
                    chunk = f.read(chunk_size)
                    if not chunk:
                        break
                    yield chunk
            logger.info(f"Streamed read for '{source_path}'.")
        except Exception as e:
            logger.error(f"Failed to stream read '{source_path}': {e}")
            raise

    def stream_write(self, remote_path: PosixPath, data_generator: Generator[bytes, None, None], relative: bool = True) -> None:
        """
        Stream write data to a remote file in chunks.

        Args:
            remote_path (PosixPath): Path to the remote file.
            data_generator (Generator[bytes, None, None]): Generator yielding chunks of data.
            relative (bool): Whether the path is relative to `base_path`.

        Raises:
            Exception: For any exceptions that occur during streaming.
        """
        target_path = self._prepare_remote_path(remote_path, relative)

        try:
            with self.fs.open(str(target_path), 'wb') as f:
                for chunk in data_generator:
                    f.write(chunk)
            logger.info(f"Streamed write to '{target_path}'.")
        except Exception as e:
            logger.error(f"Failed to stream write to '{target_path}': {e}")
            raise

    def generate_presigned_url(self, remote_path: PosixPath, expiration: int = 3600, relative: bool = True) -> str:
        """
        Generate a presigned URL for accessing the remote file.

        Args:
            remote_path (PosixPath): Path to the remote file.
            expiration (int, optional): Time in seconds for the URL to remain valid. Defaults to 1 hour.
            relative (bool): Whether the path is relative to `base_path`.

        Returns:
            str: Presigned URL.

        Raises:
            NotImplementedError: If the protocol does not support presigned URLs.
            Exception: For any exceptions that occur during URL generation.
        """
        target_path = self.base_path / remote_path if relative else remote_path
        try:
            if self.fs.protocol == 'gs':
                # Assuming self.fs is a gcsfs filesystem
                return self.fs.url(str(target_path), method='GET', expires=expiration)
            elif self.fs.protocol == 's3':
                # Assuming self.fs is an s3fs filesystem
                return self.fs.url(str(target_path), expires=expiration)
            else:
                raise NotImplementedError("Presigned URLs are not supported for this protocol.")
        except Exception as e:
            logger.error(f"Failed to generate presigned URL for '{target_path}': {e}")
            raise

    def set_permissions(self, remote_path: PosixPath, acl: str, relative: bool = True) -> None:
        """
        Set permissions for a remote file.

        Args:
            remote_path (PosixPath): Path to the remote file.
            acl (str): Access Control List policy (e.g., 'public-read').
            relative (bool): Whether the path is relative to `base_path`.

        Raises:
            NotImplementedError: If the protocol does not support setting permissions.
            Exception: For any exceptions that occur during permission setting.
        """
        target_path = self.base_path / remote_path if relative else remote_path
        try:
            if self.fs.protocol == 'gs':
                self.fs.setxattr(str(target_path), 'acl', acl)
                logger.info(f"Set permissions for '{target_path}' to '{acl}'.")
            elif self.fs.protocol == 's3':
                self.fs.setxattr(str(target_path), 'ACL', acl)
                logger.info(f"Set permissions for '{target_path}' to '{acl}'.")
            else:
                raise NotImplementedError("Setting permissions is not supported for this protocol.")
        except Exception as e:
            logger.error(f"Failed to set permissions for '{target_path}': {e}")
            raise

    def sync_from_local(self, local_dir: PosixPath, remote_dir: PosixPath, relative: bool = True) -> None:
        """
        Synchronize a local directory to remote storage.

        Args:
            local_dir (PosixPath): Path to the local directory.
            remote_dir (PosixPath): Path to the remote directory.
            relative (bool): Whether the remote path is relative to `base_path`.

        Raises:
            Exception: For any exceptions that occur during synchronization.
        """
        target_path = self.base_path / remote_dir if relative else remote_dir
        try:
            self.fs.put(str(local_dir), str(target_path), recursive=True)
            logger.info(f"Synchronized from local '{local_dir}' to remote '{target_path}'.")
        except Exception as e:
            logger.error(f"Failed to synchronize from '{local_dir}' to '{target_path}': {e}")
            raise

    def sync_to_local(self, remote_dir: PosixPath, local_dir: PosixPath, relative: bool = True) -> None:
        """
        Synchronize a remote directory to local storage.

        Args:
            remote_dir (PosixPath): Path to the remote directory.
            local_dir (PosixPath): Path to the local directory.
            relative (bool): Whether the remote path is relative to `base_path`.

        Raises:
            Exception: For any exceptions that occur during synchronization.
        """
        source_path = self.base_path / remote_dir if relative else remote_dir
        try:
            self.fs.get(str(source_path), str(local_dir), recursive=True)
            logger.info(f"Synchronized from remote '{source_path}' to local '{local_dir}'.")
        except Exception as e:
            logger.error(f"Failed to synchronize from '{source_path}' to '{local_dir}': {e}")
            raise

    def compress_and_upload(self, local_path: PosixPath, remote_path: PosixPath, relative: bool = True) -> None:
        """
        Compress a local file using gzip and upload it to remote storage.

        Args:
            local_path (PosixPath): Path to the local file.
            remote_path (PosixPath): Path to the remote file.
            relative (bool): Whether the path is relative to `base_path`.

        Raises:
            Exception: For any exceptions that occur during compression or upload.
        """
        target_path = self.base_path / remote_path if relative else remote_path
        compressed_path = f"{local_path}.gz"
        try:
            with open(str(local_path), 'rb') as f_in, gzip.open(compressed_path, 'wb') as f_out:
                f_out.writelines(f_in)
            self.upload_file(compressed_path, target_path, relative=False)
            logger.info(f"Compressed and uploaded '{local_path}' to '{target_path}'.")
        except Exception as e:
            logger.error(f"Failed to compress and upload '{local_path}' to '{target_path}': {e}")
            raise
        finally:
            if os.path.exists(compressed_path):
                os.remove(compressed_path)

    def download_and_decompress(self, remote_path: PosixPath, local_path: PosixPath, relative: bool = True) -> None:
        """
        Download a compressed remote file and decompress it locally.

        Args:
            remote_path (PosixPath): Path to the remote compressed file.
            local_path (PosixPath): Path to save the decompressed file locally.
            relative (bool): Whether the path is relative to `base_path`.

        Raises:
            Exception: For any exceptions that occur during download or decompression.
        """
        source_path = self.base_path / remote_path if relative else remote_path
        compressed_local_path = f"{str(local_path)}.gz"
        try:
            self.download_file(str(source_path), compressed_local_path, relative=False)
            with gzip.open(compressed_local_path, 'rb') as f_in, open(str(local_path), 'wb') as f_out:
                f_out.writelines(f_in)
            logger.info(f"Downloaded and decompressed '{source_path}' to '{local_path}'.")
        except Exception as e:
            logger.error(f"Failed to download and decompress '{source_path}' to '{local_path}': {e}")
            raise
        finally:
            if os.path.exists(compressed_local_path):
                os.remove(compressed_local_path)

    def safe_write_file(self, remote_path: PosixPath, data: bytes, relative: bool = True) -> None:
        """
        Safely write data to a remote file using file locking to prevent race conditions.

        Args:
            remote_path (PosixPath): Path to the remote file.
            data (bytes): Data to write.
            relative (bool): Whether the path is relative to `base_path`.

        Raises:
            NotImplementedError: If file locking is not supported for the protocol.
            Exception: For any exceptions that occur during the write operation.
        """
        target_path = self.base_path / remote_path if relative else remote_path
        if self.fs.protocol != 'file':
            logger.error("File locking is not supported for remote filesystems.")
            raise NotImplementedError("File locking is only supported for local filesystems.")
        lock_path = f"{str(target_path)}.lock"
        try:
            with FileLock(lock_path):
                self.write_file(remote_path, data, relative=relative)
            logger.info(f"Safely wrote data to '{target_path}' with locking.")
        except Exception as e:
            logger.error(f"Failed to safely write data to '{target_path}': {e}")
            raise
