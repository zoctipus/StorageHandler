# Unified Storage Handler

**Private and Confidential**

This package provides a unified interface for interacting with various storage backends using `fsspec`. It supports protocols like local filesystem, SFTP, Google Cloud Storage, Amazon S3, and more.

## Table of Contents

- [Introduction](#introduction)
- [Installation](#installation)
- [Usage](#usage)
  - [Initialization](#initialization)
  - [Methods Overview](#methods-overview)
- [Supported Protocols](#supported-protocols)
- [License](#license)

## Introduction

The Unified Storage Handler simplifies file operations across different storage backends by providing a consistent API. It's designed for internal use within your organization to facilitate data management tasks.

## Installation

Since this is a private package, you can install it directly from your internal repository or file system.

### From a Git Repository

```bash
pip install git+ssh://git@your-repo-url/your-package.git
```

### From a Git Repository

## Usage

### Initialization

```
from your_package.unified_storage_handler import UnifiedStorageHandler

# For local filesystem
storage_handler = UnifiedStorageHandler('file:///path/to/local/storage')

# For Google Cloud Storage
storage_handler = UnifiedStorageHandler(
    'gs://your-gcs-bucket/',
    project='your-gcp-project',
    token='path/to/credentials.json'
)

# For SFTP
storage_handler = UnifiedStorageHandler(
    'sftp://user@host/',
    host='sftp.example.com',
    username='your-username',
    password='your-password',
    port=22
)

# For Amazon S3
storage_handler = UnifiedStorageHandler(
    's3://your-s3-bucket/',
    aws_access_key_id='your-access-key-id',
    aws_secret_access_key='your-secret-access-key'
)

```

### Method Overview

#### File Operations
- list_files(prefix: str = "", relative: bool = True) -> List[str]
  - list_files_recursive(prefix: str = "", relative: bool = True) -> List[str]
  - upload_file(local_path: str, remote_path: str, relative: bool = True) -> None
  - download_file(remote_path: str, local_path: str, relative: bool = True) -> None
  - read_file(remote_path: str, relative: bool = True) -> bytes
  - write_file(remote_path: str, data: bytes, relative: bool = True) -> None
  - delete_file(remote_path: str, relative: bool = True) -> None
  - exists(remote_path: str, relative: bool = True) -> bool
  - rename(old_remote_path: str, new_remote_path: str, relative: bool = True) -> None
  - copy(source_remote_path: str, destination_remote_path: str, relative: bool = True) -> None
  - get_file_metadata(remote_path: str, relative: bool = True) -> Optional[dict]

- Directory Operations

  - create_directory(remote_path: str, relative: bool = True) -> None
  - sync_from_local(local_dir: str, remote_dir: str, relative: bool = True) -> None
  - sync_to_local(remote_dir: str, local_dir: str, relative: bool = True) -> None

- Advanced Operations

  - stream_read(remote_path: str, chunk_size: int = 1024 * 1024, relative: bool = True) -> Generator[bytes, None, None]
  - stream_write(remote_path: str, data_generator: Generator[bytes, None, None], relative: bool = True) -> None
  - compress_and_upload(local_path: str, remote_path: str, relative: bool = True) -> None
  - download_and_decompress(remote_path: str, local_path: str, relative: bool = True) -> None
  - generate_presigned_url(remote_path: str, expiration: int = 3600, relative: bool = True) -> str
  - set_permissions(remote_path: str, acl: str, relative: bool = True) -> None

Note: Refer to the code docstrings for detailed usage of each method.

## Supported Protocols
- Local Filesystem (file)
- Secure FTP (sftp)
- Google Cloud Storage (gs)
- Amazon S3 (s3)

## License
This package is proprietary and confidential. Unauthorized copying of this package, via any medium, is strictly prohibited. All rights reserved.