"""Unit tests for the upload body-size cap (api/simulation/service._read_within_cap).

RequestLimitsMiddleware only screens the Content-Length header, so a chunked
request (no Content-Length) reaches the handler unbounded. ``_read_within_cap``
is the second line of defence: it reads the upload in bounded chunks and rejects
at the cap before the whole body is pulled into memory. These tests exercise the
cap logic directly (no DB / ASGI stack) so the boundary behaviour is pinned down
deterministically.
"""

from __future__ import annotations

import pytest

from api.simulation.service import _read_within_cap
from core.errors.errors import ErrorException
from shared.custom_errors import errors


class _FakeUpload:
    """Minimal stand-in for ``starlette.UploadFile`` that streams ``payload``.

    ``read(size)`` hands back at most ``size`` bytes per call and an empty bytes
    at EOF — exactly the contract ``_read_within_cap`` relies on, including for a
    chunked body that arrives in many small reads.
    """

    def __init__(self, payload: bytes):
        self._payload = payload
        self._pos = 0

    async def read(self, size: int = -1) -> bytes:
        if size is None or size < 0:
            chunk = self._payload[self._pos :]
        else:
            chunk = self._payload[self._pos : self._pos + size]
        self._pos += len(chunk)
        return chunk


async def test_body_over_cap_is_rejected_with_413():
    cap = 1024
    upload = _FakeUpload(b"A" * (cap + 1))  # one byte over

    with pytest.raises(ErrorException) as exc_info:
        await _read_within_cap(upload, cap)

    assert exc_info.value.status_code == 413
    assert exc_info.value.error is errors.simulation.FILE_TOO_LARGE


async def test_body_exactly_at_cap_is_accepted():
    cap = 1024
    payload = b"B" * cap
    upload = _FakeUpload(payload)

    assert await _read_within_cap(upload, cap) == payload


async def test_body_under_cap_returns_full_content():
    payload = b"production,C0\n1000,200\n"
    upload = _FakeUpload(payload)

    assert await _read_within_cap(upload, max_bytes=1024) == payload


async def test_many_small_chunks_summing_over_cap_are_rejected():
    # A chunked upload arrives in many small reads; the cap is enforced on the
    # running total, not on any single read.
    from api.simulation import service

    cap = 4 * service._UPLOAD_READ_CHUNK_BYTES  # spans several read() calls
    upload = _FakeUpload(b"x" * (cap + 1))

    with pytest.raises(ErrorException) as exc_info:
        await _read_within_cap(upload, cap)

    assert exc_info.value.status_code == 413
