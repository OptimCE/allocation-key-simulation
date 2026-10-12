"""Integration tests for api/simulation/routes.py.

Full ASGI stack against a real Postgres. NATS + storage are mocked at the
import location in ``api.simulation.service`` so no broker/MinIO is required.

Auth follows the gateway pattern: send x-user-id / x-community-id / x-user-role
headers; GatewayScopeMiddleware turns them into ContextVars. Every test creates
an active ``simulation`` subscription, else require_feature returns 403.
"""

import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from sqlalchemy import select

from api.simulation import service as service_module
from api.simulation.schemas import SimulationTimeseries
from core import storage
from core.api_response import ApiResponse
from core.database.models import Community
from shared import problem_size
from shared.const import SimulationStatus
from shared.custom_errors import errors
from shared.models.local_models import SimulationModel
from tests.factories.simulation_factory import (
    create_crm_key,
    create_simulation,
    create_simulation_with_result,
)
from tests.factories.subscription_factory import create_community, create_subscription


def _admin_headers(community: Community) -> dict[str, str]:
    return {
        "x-user-id": "test|admin",
        "x-community-id": community.auth_community_id,
        "x-user-role": "ADMIN",
    }


async def _community_with_subscription(db_session) -> Community:
    community = await create_community(db_session)
    await create_subscription(db_session, id_community=community.id, is_active=True)
    return community


_DEFAULT_FILE = ("data.csv", b"production,C0,C1\n1000,200,300\n", "text/csv")


def _multipart_payload(*, name="my sim", id_key: int, injection_name="production", file=None):
    return {
        "files": {"file": file or _DEFAULT_FILE},
        "data": {"name": name, "id_key": str(id_key), "injection_name": injection_name},
    }


# ---------------------------------------------------------------------------
# GET /  (list)
# ---------------------------------------------------------------------------


async def test_list_empty_returns_pagination_zero(client, db_session):
    community = await _community_with_subscription(db_session)
    response = await client.get("/", headers=_admin_headers(community))
    assert response.status_code == 200
    body = response.json()
    assert body["data"] == []
    assert body["pagination"]["total"] == 0


async def test_list_returns_only_current_community(client, db_session):
    community_a = await _community_with_subscription(db_session)
    community_b = await _community_with_subscription(db_session)
    await create_simulation(db_session, id_community=community_a.id, name="A row")
    await create_simulation(db_session, id_community=community_b.id, name="B row")

    response = await client.get("/", headers=_admin_headers(community_a))
    body = response.json()
    assert body["pagination"]["total"] == 1
    assert body["data"][0]["name"] == "A row"


async def test_list_filter_by_status(client, db_session):
    community = await _community_with_subscription(db_session)
    await create_simulation(db_session, id_community=community.id, status=SimulationStatus.PENDING)
    success = await create_simulation(
        db_session, id_community=community.id, status=SimulationStatus.SUCCESS, name="done"
    )

    response = await client.get("/?status=1", headers=_admin_headers(community))
    body = response.json()
    assert body["pagination"]["total"] == 1
    assert body["data"][0]["id"] == success.id


# ---------------------------------------------------------------------------
# POST /  (start) — NATS + storage mocked
# ---------------------------------------------------------------------------


@patch("api.simulation.service.storage.delete", new_callable=AsyncMock)
@patch("api.simulation.service.storage.upload", new_callable=AsyncMock)
@patch("api.simulation.service.get_jetstream", return_value=MagicMock())
@patch("api.simulation.service.send_event", new_callable=AsyncMock)
async def test_start_simulation_publishes_event_and_returns_pending(
    mock_send, mock_js, mock_upload, mock_delete, client, db_session
):
    community = await _community_with_subscription(db_session)
    key = await create_crm_key(db_session, id_community=community.id, name="My key")

    response = await client.post(
        "/", headers=_admin_headers(community), **_multipart_payload(id_key=key.id)
    )

    assert response.status_code == 200
    body = response.json()
    assert body["data"]["status"] == int(SimulationStatus.PENDING)
    new_id = body["data"]["id"]

    mock_upload.assert_awaited_once()
    uploaded_key = mock_upload.await_args.args[0]
    assert uploaded_key.startswith(f"simulations/{community.id}/")
    assert uploaded_key.endswith("/data.csv")

    mock_send.assert_awaited_once()
    args, _ = mock_send.call_args
    assert args[1] == "optimce.simulation.run"
    assert args[2].type == "simulation.requested"
    assert args[2].data == {"simulation_id": new_id}

    row = (
        await db_session.execute(select(SimulationModel).where(SimulationModel.id == new_id))
    ).scalar_one()
    assert row.status == SimulationStatus.PENDING
    assert row.id_key == key.id
    assert row.key_name == "My key"
    assert row.file_storage_key == uploaded_key
    mock_delete.assert_not_awaited()


@patch("api.simulation.service.storage.upload", new_callable=AsyncMock)
@patch("api.simulation.service.get_jetstream", return_value=MagicMock())
@patch("api.simulation.service.send_event", new_callable=AsyncMock)
async def test_start_simulation_refuses_a_run_too_large_before_storing_the_file(
    mock_send, mock_js, mock_upload, client, db_session, monkeypatch
):
    # 2 participants x 3 data rows x 3 iterations = 18 cells.
    monkeypatch.setattr(problem_size, "MAX_SIMULATION_CELLS", 17)
    community = await _community_with_subscription(db_session)
    key = await create_crm_key(db_session, id_community=community.id, iterations=3)
    csv = ("data.csv", b"production,C0,C1\n1,2,3\n4,5,6\n7,8,9\n", "text/csv")

    response = await client.post(
        "/", headers=_admin_headers(community), **_multipart_payload(id_key=key.id, file=csv)
    )

    assert response.status_code == 422
    assert response.json()["error_code"] == errors.simulation.SIMULATION_TOO_LARGE.code
    mock_upload.assert_not_awaited()
    mock_send.assert_not_awaited()
    rows = (
        await db_session.execute(
            select(SimulationModel).where(SimulationModel.id_community == community.id)
        )
    ).all()
    assert rows == []


@patch("api.simulation.service.storage.upload", new_callable=AsyncMock)
@patch("api.simulation.service.get_jetstream", return_value=MagicMock())
@patch("api.simulation.service.send_event", new_callable=AsyncMock)
async def test_start_simulation_sizes_the_run_by_the_keys_participants_not_the_columns(
    mock_send, mock_js, mock_upload, client, db_session, monkeypatch
):
    # The worker ignores columns the key does not name: 2 participants x 1 row,
    # not 5 participant-looking columns.
    monkeypatch.setattr(problem_size, "MAX_SIMULATION_CELLS", 2)
    community = await _community_with_subscription(db_session)
    key = await create_crm_key(db_session, id_community=community.id)
    csv = ("data.csv", b"production,C0,C1,X1,X2,X3\n1,2,3,4,5,6\n", "text/csv")

    response = await client.post(
        "/", headers=_admin_headers(community), **_multipart_payload(id_key=key.id, file=csv)
    )

    assert response.status_code == 200
    mock_upload.assert_awaited_once()


@patch("api.simulation.service.storage.delete", new_callable=AsyncMock)
@patch("api.simulation.service.storage.upload", new_callable=AsyncMock)
@patch("api.simulation.service.get_jetstream", return_value=MagicMock())
@patch("api.simulation.service.send_event", new_callable=AsyncMock)
async def test_start_simulation_unknown_key_returns_404(
    mock_send, mock_js, mock_upload, mock_delete, client, db_session
):
    community = await _community_with_subscription(db_session)
    response = await client.post(
        "/", headers=_admin_headers(community), **_multipart_payload(id_key=999999)
    )
    assert response.status_code == 404
    assert response.json()["error_code"] == 2103  # KEY_NOT_FOUND
    mock_upload.assert_not_awaited()
    mock_send.assert_not_awaited()


@patch("api.simulation.service.storage.delete", new_callable=AsyncMock)
@patch("api.simulation.service.storage.upload", new_callable=AsyncMock)
@patch("api.simulation.service.get_jetstream", return_value=MagicMock())
@patch("api.simulation.service.send_event", new_callable=AsyncMock)
async def test_start_simulation_foreign_key_returns_404(
    mock_send, mock_js, mock_upload, mock_delete, client, db_session
):
    community_a = await _community_with_subscription(db_session)
    community_b = await _community_with_subscription(db_session)
    key_b = await create_crm_key(db_session, id_community=community_b.id)

    response = await client.post(
        "/", headers=_admin_headers(community_a), **_multipart_payload(id_key=key_b.id)
    )
    assert response.status_code == 404
    assert response.json()["error_code"] == 2103  # cross-tenant key is invisible
    mock_upload.assert_not_awaited()


def _multipart_body(
    boundary: str,
    *,
    name: str,
    id_key: int,
    injection_name: str,
    filename: str,
    file_bytes: bytes,
    content_type: str = "text/csv",
) -> bytes:
    """Hand-build a multipart/form-data body so it can be streamed without a
    Content-Length (httpx only omits the header when given an async iterator)."""
    bnd = f"--{boundary}".encode()

    def text_field(field_name: str, value: str) -> bytes:
        return (
            bnd
            + f'\r\nContent-Disposition: form-data; name="{field_name}"\r\n\r\n{value}\r\n'.encode()
        )

    file_field = (
        bnd
        + f'\r\nContent-Disposition: form-data; name="file"; filename="{filename}"\r\n'.encode()
        + f"Content-Type: {content_type}\r\n\r\n".encode()
        + file_bytes
        + b"\r\n"
    )
    return (
        text_field("name", name)
        + text_field("id_key", str(id_key))
        + text_field("injection_name", injection_name)
        + file_field
        + bnd
        + b"--\r\n"
    )


@patch("api.simulation.service.storage.upload", new_callable=AsyncMock)
async def test_start_simulation_chunked_body_over_cap_returns_413(
    mock_upload, monkeypatch, client, db_session
):
    """A chunked upload (no Content-Length) over the cap is rejected.

    RequestLimitsMiddleware can only screen the Content-Length header, so a
    chunked request slips past it; the handler's bounded read must catch it.
    The cap is shrunk so the test body stays tiny.
    """
    community = await _community_with_subscription(db_session)
    key = await create_crm_key(db_session, id_community=community.id, name="My key")

    # Patch the binding the service actually reads (it imported the name).
    monkeypatch.setattr("api.simulation.service.UPLOAD_MAX_BODY_BYTES", 64)

    boundary = "testboundaryDEADBEEF"
    body = _multipart_body(
        boundary,
        name="big sim",
        id_key=key.id,
        injection_name="production",
        filename="big.csv",
        file_bytes=b"A" * 256,  # well over the 64-byte cap
    )

    async def _chunked_stream():
        # Two yields => httpx uses Transfer-Encoding: chunked with no
        # Content-Length, which is exactly the case the middleware can't screen.
        mid = len(body) // 2
        yield body[:mid]
        yield body[mid:]

    response = await client.post(
        "/",
        headers={
            **_admin_headers(community),
            "Content-Type": f"multipart/form-data; boundary={boundary}",
        },
        content=_chunked_stream(),
    )

    assert response.status_code == 413
    assert response.json()["error_code"] == 2110  # FILE_TOO_LARGE
    mock_upload.assert_not_awaited()  # rejected before the storage write


@patch("api.simulation.service.storage.upload", new_callable=AsyncMock)
async def test_start_simulation_empty_file_returns_422(mock_upload, client, db_session):
    community = await _community_with_subscription(db_session)
    key = await create_crm_key(db_session, id_community=community.id)
    response = await client.post(
        "/",
        headers=_admin_headers(community),
        files={"file": ("empty.csv", b"", "text/csv")},
        data={"name": "empty", "id_key": str(key.id), "injection_name": "production"},
    )
    assert response.status_code == 422
    assert response.json()["error_code"] == 2104  # INVALID_FILE
    mock_upload.assert_not_awaited()


@patch(
    "api.simulation.service.storage.upload",
    new_callable=AsyncMock,
    side_effect=RuntimeError("minio down"),
)
async def test_start_simulation_storage_failure_returns_502(mock_upload, client, db_session):
    community = await _community_with_subscription(db_session)
    key = await create_crm_key(db_session, id_community=community.id)
    response = await client.post(
        "/", headers=_admin_headers(community), **_multipart_payload(id_key=key.id, name="x")
    )
    assert response.status_code == 502
    assert response.json()["error_code"] == 2105  # STORAGE_UPLOAD_FAILED
    rows = (
        (await db_session.execute(select(SimulationModel).where(SimulationModel.name == "x")))
        .scalars()
        .all()
    )
    assert rows == []


@patch("api.simulation.service.SimulationService._mark_failed_to_queue", new_callable=AsyncMock)
@patch("api.simulation.service.storage.delete", new_callable=AsyncMock)
@patch("api.simulation.service.storage.upload", new_callable=AsyncMock)
@patch("api.simulation.service.get_jetstream", return_value=MagicMock())
@patch(
    "api.simulation.service.send_event",
    new_callable=AsyncMock,
    side_effect=RuntimeError("nats down"),
)
async def test_start_simulation_publish_failure_returns_500_and_cleans_up(
    mock_send, mock_js, mock_upload, mock_delete, mock_mark_failed, client, db_session
):
    community = await _community_with_subscription(db_session)
    key = await create_crm_key(db_session, id_community=community.id)

    response = await client.post(
        "/", headers=_admin_headers(community), **_multipart_payload(id_key=key.id)
    )
    assert response.status_code == 500
    assert response.json()["error_code"] == 2106  # START_SIMULATION
    mock_mark_failed.assert_awaited_once()
    mock_upload.assert_awaited_once()
    uploaded_key = mock_upload.await_args.args[0]
    mock_delete.assert_awaited_once_with(uploaded_key)


# ---------------------------------------------------------------------------
# GET /{id}  (detail)
# ---------------------------------------------------------------------------


async def test_get_simulation_returns_result_tree(client, db_session):
    community = await _community_with_subscription(db_session)
    sim = await create_simulation_with_result(db_session, id_community=community.id)

    response = await client.get(f"/{sim.id}", headers=_admin_headers(community))
    assert response.status_code == 200
    data = response.json()["data"]
    assert data["id"] == sim.id
    assert data["status"] == int(SimulationStatus.SUCCESS)
    assert data["has_timeseries"] is True
    assert data["key_result"]["sharing_rate_total"] == 0.8
    assert len(data["key_result"]["iterations"]) == 1
    assert len(data["key_result"]["iterations"][0]["consumers"]) == 1


async def test_get_simulation_unknown_returns_404(client, db_session):
    community = await _community_with_subscription(db_session)
    response = await client.get("/999999", headers=_admin_headers(community))
    assert response.status_code == 404
    assert response.json()["error_code"] == 2102  # SIMULATION_NOT_FOUND


async def test_get_simulation_for_other_community_returns_404(client, db_session):
    community_a = await _community_with_subscription(db_session)
    community_b = await _community_with_subscription(db_session)
    sim_b = await create_simulation(db_session, id_community=community_b.id)
    response = await client.get(f"/{sim_b.id}", headers=_admin_headers(community_a))
    assert response.status_code == 404


# ---------------------------------------------------------------------------
# GET /{id}/timeseries
# ---------------------------------------------------------------------------


# As the worker stores it (worker/timeseries.py): two iterations of five
# timesteps, NaN where an empty cell of an uploaded file used to end up.
_STORED_TIMESERIES = (
    b'{"iterations": [{"number": 1, "consumption": [1.0, NaN, 3.0, 4.0, 5.0], '
    b'"energy_allocated": [2.0, 2.0, 2.0, 2.0, 2.0], '
    b'"energy_allocated_consumed": [1.0, 0.0, 2.0, 2.0, 2.0], '
    b'"residual_volume": [0.0, 0.0, 1.0, 2.0, 3.0], "surplus": [1.0, 2.0, 0.0, 0.0, 0.0], '
    b'"sharing_rate": [0.5, 0.0, 1.0, 1.0, 1.0], '
    b'"self_sufficiency_rate": [1.0, 0.0, 0.67, 0.5, 0.4], '
    b'"consumers": [{"name": "C\\"0", "consumption": [1.0, NaN, 3.0, 4.0, 5.0], '
    b'"energy_allocated": [2.0, 2.0, 2.0, 2.0, 2.0], '
    b'"energy_allocated_consumed": [1.0, 0.0, 2.0, 2.0, 2.0], '
    b'"residual_volume": [0.0, 0.0, 1.0, 2.0, 3.0], '
    b'"surplus": [1.0, 2.0, 0.0, 0.0, 0.0]}]}, '
    b'{"number": 2, "consumption": [0.0, 0.0, 1.0, 2.0, 3.0], '
    b'"energy_allocated": [1.0, 2.0, 0.0, 0.0, 0.0], '
    b'"energy_allocated_consumed": [0.0, 0.0, 0.0, 0.0, 0.0], '
    b'"residual_volume": [0.0, 0.0, 1.0, 2.0, 3.0], "surplus": [1.0, 2.0, 0.0, 0.0, 0.0], '
    b'"sharing_rate": [0.0, 0.0, 0.0, 0.0, 0.0], '
    b'"self_sufficiency_rate": [0.0, 0.0, 0.0, 0.0, 0.0], "consumers": []}]}'
)


def _stored_object(content: bytes, chunk_bytes: int = 7):
    """A stand-in for storage.iter_object that serves ``content`` in small chunks."""
    keys: list[str] = []
    max_bytes_seen: list[int | None] = []

    async def iter_object(key: str, chunk_bytes_: int = 0, max_bytes: int | None = None):
        keys.append(key)
        max_bytes_seen.append(max_bytes)
        if max_bytes is not None and len(content) > max_bytes:
            raise storage.ObjectTooLarge(key, len(content))
        for start in range(0, len(content), chunk_bytes):
            yield content[start : start + chunk_bytes]

    iter_object.keys = keys  # type: ignore[attr-defined]
    iter_object.max_bytes = max_bytes_seen  # type: ignore[attr-defined]
    return iter_object


async def _missing_object(key: str, chunk_bytes: int = 0, max_bytes: int | None = None):
    raise storage.ObjectNotFound(key)
    yield b""  # pragma: no cover - makes this an async generator, like iter_object


async def test_get_timeseries_streams_the_stored_object(client, db_session, monkeypatch):
    community = await _community_with_subscription(db_session)
    sim = await create_simulation_with_result(
        db_session, id_community=community.id, result_storage_key="simulations/9/result.json"
    )
    stored = _stored_object(_STORED_TIMESERIES)
    monkeypatch.setattr("api.simulation.service.storage.iter_object", stored)

    response = await client.get(f"/{sim.id}/timeseries", headers=_admin_headers(community))

    assert response.status_code == 200
    assert response.headers["content-type"] == "application/json"
    assert stored.keys == ["simulations/9/result.json"]
    assert stored.max_bytes == [service_module.FULL_TIMESERIES_MAX_BYTES]
    # Strict JSON: the stored NaN went out as null.
    body = json.loads(response.content, parse_constant=lambda token: pytest.fail(token))
    expected = json.loads(_STORED_TIMESERIES.replace(b"NaN", b"null"))
    assert body == {"data": expected, "error_code": 0}
    data = ApiResponse[SimulationTimeseries].model_validate_json(response.content).data
    assert data.stride == 1
    assert data.iterations[0].consumers[0].name == 'C"0'
    assert data.iterations[0].consumers[0].consumption[1] is None


async def test_get_timeseries_max_points_returns_the_iteration_series_sampled(
    client, db_session, monkeypatch
):
    community = await _community_with_subscription(db_session)
    sim = await create_simulation_with_result(db_session, id_community=community.id)
    monkeypatch.setattr(
        "api.simulation.service.storage.iter_object", _stored_object(_STORED_TIMESERIES)
    )

    response = await client.get(
        f"/{sim.id}/timeseries?max_points=2", headers=_admin_headers(community)
    )

    assert response.status_code == 200
    data = response.json()["data"]
    assert data["stride"] == 3  # 5 timesteps, at most 2 points: timesteps 0 and 3
    first, second = data["iterations"]
    assert first["number"] == 1
    assert first["consumption"] == [1.0, 4.0]
    assert first["self_sufficiency_rate"] == [1.0, 0.5]
    assert first["consumers"] == []
    assert second["number"] == 2
    assert second["residual_volume"] == [0.0, 2.0]


async def test_get_timeseries_max_points_writes_null_for_nan(client, db_session, monkeypatch):
    community = await _community_with_subscription(db_session)
    sim = await create_simulation_with_result(db_session, id_community=community.id)
    monkeypatch.setattr(
        "api.simulation.service.storage.iter_object", _stored_object(_STORED_TIMESERIES)
    )

    response = await client.get(
        f"/{sim.id}/timeseries?max_points=5", headers=_admin_headers(community)
    )

    assert response.status_code == 200
    data = response.json()["data"]
    assert data["stride"] == 1
    assert data["iterations"][0]["consumption"] == [1.0, None, 3.0, 4.0, 5.0]


@pytest.mark.parametrize("max_points", ["0", "10001", "many"])
async def test_get_timeseries_rejects_an_invalid_max_points(max_points, client, db_session):
    community = await _community_with_subscription(db_session)
    sim = await create_simulation_with_result(db_session, id_community=community.id)

    response = await client.get(
        f"/{sim.id}/timeseries?max_points={max_points}", headers=_admin_headers(community)
    )

    assert response.status_code == 422


async def test_get_timeseries_refuses_to_send_a_large_object_whole(client, db_session, monkeypatch):
    # The gateway decodes a JSON response whole, at ~10 bytes per byte.
    community = await _community_with_subscription(db_session)
    sim = await create_simulation_with_result(db_session, id_community=community.id)
    monkeypatch.setattr(
        "api.simulation.service.storage.iter_object", _stored_object(_STORED_TIMESERIES)
    )
    monkeypatch.setattr(service_module, "FULL_TIMESERIES_MAX_BYTES", len(_STORED_TIMESERIES) - 1)

    whole = await client.get(f"/{sim.id}/timeseries", headers=_admin_headers(community))
    sampled = await client.get(
        f"/{sim.id}/timeseries?max_points=1500", headers=_admin_headers(community)
    )

    assert whole.status_code == 422
    assert whole.json()["error_code"] == 2120  # TIMESERIES_TOO_LARGE
    assert sampled.status_code == 200
    assert len(sampled.json()["data"]["iterations"]) == 2


@pytest.mark.parametrize("query", ["", "?max_points=1500"])
async def test_get_timeseries_missing_object_returns_404(query, client, db_session, monkeypatch):
    community = await _community_with_subscription(db_session)
    sim = await create_simulation_with_result(db_session, id_community=community.id)
    monkeypatch.setattr("api.simulation.service.storage.iter_object", _missing_object)

    response = await client.get(f"/{sim.id}/timeseries{query}", headers=_admin_headers(community))

    assert response.status_code == 404
    assert response.json()["error_code"] == 2108  # RESULT_NOT_FOUND


@pytest.mark.parametrize("query", ["", "?max_points=1500"])
async def test_get_timeseries_for_other_community_returns_404(
    query, client, db_session, monkeypatch
):
    community_a = await _community_with_subscription(db_session)
    community_b = await _community_with_subscription(db_session)
    sim_b = await create_simulation_with_result(db_session, id_community=community_b.id)
    stored = _stored_object(_STORED_TIMESERIES)
    monkeypatch.setattr("api.simulation.service.storage.iter_object", stored)

    response = await client.get(
        f"/{sim_b.id}/timeseries{query}", headers=_admin_headers(community_a)
    )

    assert response.status_code == 404
    assert response.json()["error_code"] == 2102  # SIMULATION_NOT_FOUND
    assert stored.keys == []


async def test_get_timeseries_missing_result_returns_404(client, db_session):
    community = await _community_with_subscription(db_session)
    sim = await create_simulation(db_session, id_community=community.id)  # no result_storage_key
    response = await client.get(f"/{sim.id}/timeseries", headers=_admin_headers(community))
    assert response.status_code == 404
    assert response.json()["error_code"] == 2108  # RESULT_NOT_FOUND


# ---------------------------------------------------------------------------
# DELETE /{id}
# ---------------------------------------------------------------------------


@patch("api.simulation.service.storage.delete", new_callable=AsyncMock)
async def test_delete_simulation_removes_row(mock_delete, client, db_session):
    community = await _community_with_subscription(db_session)
    sim = await create_simulation(db_session, id_community=community.id)
    response = await client.delete(f"/{sim.id}", headers=_admin_headers(community))
    assert response.status_code == 200
    remaining = (
        await db_session.execute(select(SimulationModel).where(SimulationModel.id == sim.id))
    ).scalar_one_or_none()
    assert remaining is None
    mock_delete.assert_awaited()  # source file cleanup


async def test_delete_simulation_unknown_returns_404(client, db_session):
    community = await _community_with_subscription(db_session)
    response = await client.delete("/999999", headers=_admin_headers(community))
    assert response.status_code == 404
    assert response.json()["error_code"] == 2102


# ---------------------------------------------------------------------------
# Feature gating
# ---------------------------------------------------------------------------


async def test_list_without_subscription_returns_403(client, db_session):
    community = await create_community(db_session)  # no subscription
    response = await client.get("/", headers=_admin_headers(community))
    assert response.status_code == 403
    assert response.json()["error_code"] == 1003  # NOT_SUBSCRIBED
