"""aiobotocore-backed S3 client used for source-file storage.

The service uploads user-provided source data files to ``STORAGE_BUCKET``
(MinIO in dev). The worker reads them back during generation and deletes
them once the row reaches a terminal state. The shape is deliberately
function-based — there's no client singleton to wire into the FastAPI
lifespan or the worker startup. aiobotocore clients are async-context
managers; opening one per operation is cheap because the underlying
aiohttp connector pool is shared across the asyncio loop.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator, Iterator
from contextlib import asynccontextmanager, contextmanager
from typing import cast

from aiobotocore.session import get_session
from botocore.exceptions import (
    ClientError,
    EndpointConnectionError,
    ReadTimeoutError,
    ResponseStreamingError,
)
from botocore.exceptions import (
    ConnectionError as BotoConnectionError,
)

from core.config import settings

logger = logging.getLogger(__name__)


class ObjectNotFound(Exception):  # noqa: N818  # public exception name; raised in dispatcher, caught in worker
    """Raised by ``download`` when the requested key is absent from the bucket.

    Treat as a deterministic failure: the file is gone, no amount of retry
    will bring it back. Callers should mark the generation FAILED and ack.
    """


class TransientStorageError(Exception):
    """Raised by ``download`` for retryable errors (5xx, timeouts, transport).

    Callers should let JetStream redeliver the message rather than write a
    terminal status.
    """


class ObjectTooLarge(Exception):  # noqa: N818  # public exception name, like ObjectNotFound
    """Raised by ``iter_object`` when the object is larger than the caller's ``max_bytes``."""

    def __init__(self, key: str, size: int) -> None:
        super().__init__(f"{key} holds {size} bytes")
        self.size = size


# The set of botocore error codes we treat as "the object isn't there".
# NoSuchKey is the canonical S3 code; AWS sometimes returns 404 without one.
_NOT_FOUND_CODES = {"NoSuchKey", "NoSuchBucket", "404"}

# What ``iter_object`` asks the response body for at a time; a read may return less.
STREAM_CHUNK_BYTES = 1024 * 1024


@asynccontextmanager
async def _client() -> AsyncIterator:
    """Yield a fresh aiobotocore S3 client bound to the configured endpoint.

    ``aiobotocore`` requires its clients to be used as async context managers
    so the underlying aiohttp connector is closed deterministically.
    """
    session = get_session()
    async with session.create_client(
        "s3",
        endpoint_url=settings.STORAGE_ENDPOINT or None,
        region_name=settings.STORAGE_REGION,
        aws_access_key_id=settings.STORAGE_ACCESS_KEY,
        aws_secret_access_key=settings.STORAGE_SECRET_KEY,
    ) as client:
        yield client


async def upload(key: str, content: bytes, content_type: str | None = None) -> None:
    """Upload ``content`` as the object at ``key`` in ``STORAGE_BUCKET``.

    Raises whatever ``put_object`` raises — the service layer is responsible
    for translating those into HTTP errors and rolling back. No retries: the
    caller decides whether to surface the failure or swallow it.
    """
    extra: dict[str, str] = {}
    if content_type:
        extra["ContentType"] = content_type
    async with _client() as s3:
        await s3.put_object(
            Bucket=settings.STORAGE_BUCKET,
            Key=key,
            Body=content,
            **extra,
        )


async def upload_file(key: str, path: str, content_type: str | None = None) -> None:
    """Upload the file at ``path`` as the object at ``key``, without reading it into memory.

    aiobotocore streams an open file as the request body; botocore reads it in
    chunks to sign the payload, then rewinds it. For results too large to hold
    as one ``bytes``. Raises like ``upload``.
    """
    extra: dict[str, str] = {}
    if content_type:
        extra["ContentType"] = content_type
    body = await asyncio.to_thread(open, path, "rb")
    with body:
        async with _client() as s3:
            await s3.put_object(
                Bucket=settings.STORAGE_BUCKET,
                Key=key,
                Body=body,
                **extra,
            )


@contextmanager
def _read_errors(key: str) -> Iterator[None]:
    """Classify an error reading the object at ``key``.

    Two buckets the worker dispatcher cares about: ``ObjectNotFound`` for
    missing keys (deterministic) and ``TransientStorageError`` for everything
    that smells like a network / server hiccup (retryable). Unexpected errors
    propagate so the dispatcher's catch-all path can mark the row as
    ``unhandled_worker_error``.
    """
    try:
        yield
    except ClientError as exc:
        error_code = exc.response.get("Error", {}).get("Code", "")
        status = exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
        if error_code in _NOT_FOUND_CODES or status == 404:
            raise ObjectNotFound(key) from exc
        if status is not None and 500 <= status < 600:
            raise TransientStorageError(f"storage {status} for {key}") from exc
        raise
    except (
        TimeoutError,
        EndpointConnectionError,
        BotoConnectionError,
        ReadTimeoutError,
        ResponseStreamingError,
    ) as exc:
        raise TransientStorageError(f"storage transport: {exc}") from exc


async def download(key: str) -> bytes:
    """Fetch the object at ``key`` and return its bytes.

    Errors are classified by ``_read_errors``.
    """
    with _read_errors(key):
        async with _client() as s3:
            response = await s3.get_object(Bucket=settings.STORAGE_BUCKET, Key=key)
            async with response["Body"] as stream:
                return cast(bytes, await stream.read())


async def iter_object(
    key: str, chunk_bytes: int = STREAM_CHUNK_BYTES, max_bytes: int | None = None
) -> AsyncIterator[bytes]:
    """Yield the object at ``key`` in chunks of at most ``chunk_bytes``, without holding it whole.

    For objects too large to read as one ``bytes``. Errors are classified by
    ``_read_errors``, the object's own (``ObjectNotFound``) included, and are
    raised by the first ``anext``: a caller that must answer them before
    sending anything awaits the first chunk itself. So is ``ObjectTooLarge``,
    for an object over ``max_bytes``, raised before any of it is read.
    """
    with _read_errors(key):
        async with _client() as s3:
            response = await s3.get_object(Bucket=settings.STORAGE_BUCKET, Key=key)
            # Read through aiobotocore's StreamingBody, which takes a size and
            # checks Content-Length at the end; ``async with`` it only releases
            # the connection (it enters as the raw aiohttp response, whose
            # read() takes everything).
            body = response["Body"]
            async with body:
                size = response.get("ContentLength")
                if max_bytes is not None and size is not None and size > max_bytes:
                    raise ObjectTooLarge(key, size)
                while chunk := await body.read(chunk_bytes):
                    yield chunk


async def delete(key: str) -> None:
    """Delete the object at ``key``. Idempotent and best-effort.

    Never raises: a leaked file is a soft failure compared to a stuck row.
    Missing keys are logged at DEBUG; other errors at WARN.
    """
    try:
        async with _client() as s3:
            await s3.delete_object(Bucket=settings.STORAGE_BUCKET, Key=key)
    except ClientError as exc:
        error_code = exc.response.get("Error", {}).get("Code", "")
        if error_code in _NOT_FOUND_CODES:
            logger.debug("delete: object already gone key=%s", key)
            return
        logger.warning("delete failed for key=%s code=%s; leaking object", key, error_code)
    except Exception:
        logger.warning("delete failed for key=%s; leaking object", key, exc_info=True)
