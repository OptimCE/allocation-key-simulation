"""Object storage facade.

Wraps an S3-compatible client (MinIO in dev/staging/production) behind a tiny
function-shaped API so callers in ``api/generation/service.py`` and
``worker/dispatcher.py`` don't need to know about aiobotocore directly.

Exposed surface:
- ``upload(key, content, content_type=None)`` — put_object
- ``upload_file(key, path, content_type=None)`` — put_object, streamed from a file
- ``download(key) -> bytes`` — get_object
- ``iter_object(key, chunk_bytes=..., max_bytes=None)`` — get_object, yielded in chunks
- ``delete(key)`` — best-effort delete_object
- ``ObjectNotFound`` — raised by ``download`` / ``iter_object`` when the key does not exist
- ``TransientStorageError`` — raised by ``download`` / ``iter_object`` for retryable errors
- ``ObjectTooLarge`` — raised by ``iter_object`` for an object over ``max_bytes``
"""

from core.storage.client import (
    ObjectNotFound,
    ObjectTooLarge,
    TransientStorageError,
    delete,
    download,
    iter_object,
    upload,
    upload_file,
)

__all__ = [
    "ObjectNotFound",
    "ObjectTooLarge",
    "TransientStorageError",
    "delete",
    "download",
    "iter_object",
    "upload",
    "upload_file",
]
