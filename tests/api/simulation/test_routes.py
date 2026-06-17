"""Integration tests for api/simulation/routes.py.

Full ASGI stack against a real Postgres. NATS + storage are mocked at the
import location in ``api.simulation.service`` so no broker/MinIO is required.

Auth follows the gateway pattern: send x-user-id / x-community-id / x-user-role
headers; GatewayScopeMiddleware turns them into ContextVars. Every test creates
an active ``simulation`` subscription, else require_feature returns 403.
"""

from unittest.mock import AsyncMock, MagicMock, patch

from sqlalchemy import select

from core.database.models import Community
from shared.const import SimulationStatus
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


@patch("api.simulation.service.storage.download", new_callable=AsyncMock)
async def test_get_timeseries_returns_series(mock_download, client, db_session):
    community = await _community_with_subscription(db_session)
    sim = await create_simulation_with_result(db_session, id_community=community.id)
    mock_download.return_value = (
        b'{"iterations":[{"number":0,"consumption":[100.0],"energy_allocated":[100.0],'
        b'"energy_allocated_consumed":[80.0],"residual_volume":[20.0],"surplus":[20.0],'
        b'"sharing_rate":[0.8],"self_sufficiency_rate":[0.8],'
        b'"consumers":[{"name":"C0","consumption":[100.0],"energy_allocated":[100.0],'
        b'"energy_allocated_consumed":[80.0],"residual_volume":[20.0],"surplus":[20.0]}]}]}'
    )

    response = await client.get(f"/{sim.id}/timeseries", headers=_admin_headers(community))
    assert response.status_code == 200
    data = response.json()["data"]
    assert data["iterations"][0]["sharing_rate"] == [0.8]
    assert data["iterations"][0]["consumers"][0]["name"] == "C0"


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
