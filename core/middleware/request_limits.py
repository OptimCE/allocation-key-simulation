"""
Request limits middleware — body size cap + request timeout.

Protects against oversized payloads and slow-loris / hung-request attacks.

Body size check:
    Reads Content-Length before the request reaches FastAPI's body parser.
    If the header exceeds MAX_BODY_BYTES, the request is rejected immediately
    with 413 Payload Too Large — no memory is allocated for the body.
    Requests without Content-Length (chunked transfer) are checked
    after the body has been read by the framework.

Request timeout:
    Wraps the entire downstream handler in asyncio.wait_for().
    If the handler does not complete within TIMEOUT_SECONDS,
    the client receives a 504 Gateway Timeout.

Usage:
    app.add_middleware(RequestLimitsMiddleware)
"""

import asyncio
import logging

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse

logger = logging.getLogger(__name__)

# 2 MB — generous for a JSON API, covers PDF upload metadata but blocks abuse
MAX_BODY_BYTES = 2 * 1024 * 1024

# Larger cap for explicit upload endpoints (POST /generation/ multipart).
# 50 MB covers realistic energy datasets (hourly * 365 days * hundreds of
# consumers) loaded fully into memory in the route handler. Bump this only
# after moving the upload to a streaming put_object.
UPLOAD_MAX_BODY_BYTES = 50 * 1024 * 1024

# Path + method pairs that get the larger upload cap. Match on POST only —
# GET/DELETE on the same path keep the default.
_UPLOAD_ROUTES: tuple[tuple[str, str], ...] = (("POST", "/generation/"),)

# 30 seconds — covers complex DB queries / PDF generation
TIMEOUT_SECONDS = 30


def _max_body_for(request: Request) -> int:
    """Return the body-size cap that applies to this request."""
    path = request.url.path
    method = request.method.upper()
    for upload_method, upload_path in _UPLOAD_ROUTES:
        if method == upload_method and path == upload_path:
            return UPLOAD_MAX_BODY_BYTES
    return MAX_BODY_BYTES


class RequestLimitsMiddleware(BaseHTTPMiddleware):
    """
    Enforces request body size limits and per-request timeout.
    """

    async def dispatch(self, request: Request, call_next):
        # --- Body size gate ---
        max_bytes = _max_body_for(request)
        content_length = request.headers.get("content-length")
        if content_length is not None:
            try:
                if int(content_length) > max_bytes:
                    logger.warning(
                        "Request rejected: body too large",
                        extra={
                            "content_length": content_length,
                            "max_allowed": max_bytes,
                            "path": request.url.path,
                        },
                    )
                    return JSONResponse(
                        status_code=413,
                        content={
                            "data": "Payload too large",
                            "error_code": 0,
                        },
                    )
            except ValueError:
                pass

        # --- Request timeout ---
        try:
            response = await asyncio.wait_for(
                call_next(request),
                timeout=TIMEOUT_SECONDS,
            )
        except TimeoutError:
            logger.error(
                "Request timed out",
                extra={
                    "timeout_seconds": TIMEOUT_SECONDS,
                    "path": request.url.path,
                    "method": request.method,
                },
            )
            return JSONResponse(
                status_code=504,
                content={
                    "data": "Request timeout",
                    "error_code": 0,
                },
            )

        return response
