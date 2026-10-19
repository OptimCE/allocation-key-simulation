"""core/storage/client.py reads, against a stand-in for the aiobotocore client."""

from contextlib import asynccontextmanager

import pytest
from botocore.exceptions import ClientError, EndpointConnectionError, ReadTimeoutError

from core.storage import client as storage_client


class _RawResponse:
    """What aiobotocore's StreamingBody enters as: the aiohttp response."""

    def __init__(self, body: "_Body"):
        self.body = body

    async def read(self) -> bytes:  # aiohttp's ClientResponse.read() takes no size
        return await self.body.read()


class _Body:
    """aiobotocore's StreamingBody: read(amt) on the body itself."""

    def __init__(self, content: bytes, fail_after: int | None = None):
        self.content = content
        self.position = 0
        self.fail_after = fail_after  # raise a read timeout once this many bytes are read
        self.closed = False
        self.reads: list[int | None] = []

    async def __aenter__(self):
        return _RawResponse(self)

    async def __aexit__(self, *exc_info):
        self.closed = True

    async def read(self, amt: int | None = None) -> bytes:
        self.reads.append(amt)
        if self.fail_after is not None and self.position >= self.fail_after:
            raise ReadTimeoutError(endpoint_url="http://minio:9000")
        end = len(self.content) if amt is None else self.position + amt
        chunk = self.content[self.position : end]
        self.position += len(chunk)
        return chunk


class _S3:
    def __init__(self, body: _Body | None = None, error: Exception | None = None):
        self.body = body
        self.error = error
        self.requested: list[tuple[str, str]] = []

    async def get_object(self, Bucket: str, Key: str):  # noqa: N803 - botocore's names
        self.requested.append((Bucket, Key))
        if self.error is not None:
            raise self.error
        assert self.body is not None
        return {"Body": self.body, "ContentLength": len(self.body.content)}


@pytest.fixture
def s3(monkeypatch):
    def install(**kwargs) -> _S3:
        fake = _S3(**kwargs)

        @asynccontextmanager
        async def client():
            yield fake

        monkeypatch.setattr(storage_client, "_client", client)
        return fake

    return install


def _client_error(code: str, status: int) -> ClientError:
    return ClientError(
        {"Error": {"Code": code}, "ResponseMetadata": {"HTTPStatusCode": status}}, "GetObject"
    )


async def test_iter_object_yields_the_object_in_chunks(s3):
    content = bytes(range(256)) * 40
    fake = s3(body=_Body(content))

    chunks = [chunk async for chunk in storage_client.iter_object("a/key", chunk_bytes=1000)]

    assert b"".join(chunks) == content
    assert [len(chunk) for chunk in chunks] == [1000] * 10 + [240]
    assert set(fake.body.reads) == {1000}
    assert fake.body.closed
    assert fake.requested[0][1] == "a/key"


@pytest.mark.parametrize(
    ("error", "raised"),
    [
        (_client_error("NoSuchKey", 404), storage_client.ObjectNotFound),
        (_client_error("", 404), storage_client.ObjectNotFound),
        (_client_error("SlowDown", 503), storage_client.TransientStorageError),
        (
            EndpointConnectionError(endpoint_url="http://minio:9000"),
            storage_client.TransientStorageError,
        ),
    ],
)
async def test_iter_object_classifies_errors_on_the_first_chunk(s3, error, raised):
    s3(error=error)
    chunks = storage_client.iter_object("a/key")

    with pytest.raises(raised):
        await anext(chunks)


async def test_iter_object_passes_an_unexpected_client_error_through(s3):
    s3(error=_client_error("AccessDenied", 403))

    with pytest.raises(ClientError):
        await anext(storage_client.iter_object("a/key"))


async def test_iter_object_classifies_a_timeout_in_the_middle(s3):
    s3(body=_Body(b"x" * 5000, fail_after=2000))
    chunks = storage_client.iter_object("a/key", chunk_bytes=1000)

    assert await anext(chunks) == b"x" * 1000
    assert await anext(chunks) == b"x" * 1000
    with pytest.raises(storage_client.TransientStorageError):
        await anext(chunks)


async def test_download_classifies_as_before(s3):
    s3(error=_client_error("NoSuchKey", 404))
    with pytest.raises(storage_client.ObjectNotFound):
        await storage_client.download("a/key")

    s3(body=_Body(b"whole object"))
    assert await storage_client.download("a/key") == b"whole object"


async def test_iter_object_refuses_an_object_over_max_bytes_before_reading_it(s3):
    fake = s3(body=_Body(b"x" * 5000))

    with pytest.raises(storage_client.ObjectTooLarge) as raised:
        await anext(storage_client.iter_object("a/key", max_bytes=4999))

    assert raised.value.size == 5000
    assert fake.body.reads == []
    assert fake.body.closed


async def test_iter_object_streams_an_object_of_max_bytes(s3):
    s3(body=_Body(b"x" * 5000))

    chunks = [
        chunk
        async for chunk in storage_client.iter_object("a/key", chunk_bytes=4096, max_bytes=5000)
    ]

    assert b"".join(chunks) == b"x" * 5000
