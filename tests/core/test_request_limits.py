"""Per-route body-size cap tests for the request-limits middleware.

The simulation upload is ``POST /`` (simulation_routes is mounted without a
prefix), so it must receive the larger ``UPLOAD_MAX_BODY_BYTES`` cap; every
other route keeps the conservative ``MAX_BODY_BYTES`` default.
"""

from __future__ import annotations

from types import SimpleNamespace

from core.middleware import request_limits


def _request(method: str, path: str) -> SimpleNamespace:
    return SimpleNamespace(method=method, url=SimpleNamespace(path=path))


def test_upload_route_gets_large_cap():
    req = _request("POST", "/")
    assert request_limits._max_body_for(req) == request_limits.UPLOAD_MAX_BODY_BYTES


def test_get_root_keeps_default_cap():
    # The upload cap is POST-only; a GET on the same path keeps the default.
    req = _request("GET", "/")
    assert request_limits._max_body_for(req) == request_limits.MAX_BODY_BYTES


def test_other_post_route_keeps_default_cap():
    req = _request("POST", "/health/readiness")
    assert request_limits._max_body_for(req) == request_limits.MAX_BODY_BYTES
