"""Integration tests for the CRM-sourced simulation routes.

Full ASGI stack against a real Postgres, same conventions as test_routes.py.
Nothing here touches MinIO — that is the point of the CRM source.

The behaviour these tests are really about is the participant match: the
simulated key names participants, the CRM holds meters, and the two must line
up before a run is allowed.
"""

import datetime
from unittest.mock import AsyncMock, MagicMock, patch

from sqlalchemy import select

from core.database.models import Community
from shared.const import DataSource, SimulationStatus
from shared.custom_errors import errors
from shared.models.local_models import SimulationModel
from tests.factories.meter_factory import (
    create_meter,
    create_readings,
    create_sharing_operation,
)
from tests.factories.simulation_factory import create_crm_key
from tests.factories.subscription_factory import create_community, create_subscription

_EAN_A = "541448000000000001"
_EAN_PV = "541448000000000002"
_PERIOD = {"period_start": "2025-02-01", "period_end": "2025-02-28"}


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


async def _operation_with_data(
    db_session,
    community: Community,
    *,
    consumer_skip: set[int] | None = None,
    duplicate: bool = False,
    injection: float = 40.0,
) -> int:
    op = await create_sharing_operation(db_session, id_community=community.id)
    await create_meter(db_session, ean=_EAN_A, id_community=community.id)
    await create_meter(db_session, ean=_EAN_PV, id_community=community.id)

    await create_readings(
        db_session,
        ean=_EAN_A,
        id_community=community.id,
        id_sharing_operation=op,
        gross=10.0,
        inj_gross=0.0,
        skip=consumer_skip,
    )
    if duplicate:
        await create_readings(
            db_session,
            ean=_EAN_A,
            id_community=community.id,
            id_sharing_operation=op,
            gross=10.0,
            inj_gross=0.0,
            skip=consumer_skip,
        )
    await create_readings(
        db_session,
        ean=_EAN_PV,
        id_community=community.id,
        id_sharing_operation=op,
        gross=0.0,
        inj_gross=injection,
    )
    return op


def _body(id_key: int, op: int, **overrides) -> dict:
    body = {
        "name": "february sim",
        "id_key": id_key,
        "id_sharing_operation": op,
        **_PERIOD,
    }
    body.update(overrides)
    return body


# ---------------------------------------------------------------------------
# 1. GET /crm-data-preview — the participant match
# ---------------------------------------------------------------------------


async def test_preview_reports_matched_participants(client, db_session):
    community = await _community_with_subscription(db_session)
    op = await _operation_with_data(db_session, community)
    key = await create_crm_key(db_session, id_community=community.id, consumer_names=[_EAN_A])

    response = await client.get(
        "/crm-data-preview",
        params={"id_key": key.id, "id_sharing_operation": op, **_PERIOD},
        headers=_admin_headers(community),
    )

    assert response.status_code == 200
    data = response.json()["data"]
    assert data["can_simulate"] is True
    assert data["matched_participants"] == [_EAN_A]
    assert data["unmatched_participants"] == []
    assert data["reading_count"] == 8
    assert data["blockers"] == []


async def test_preview_is_reachable_and_not_swallowed_by_the_id_route(client, db_session):
    # `GET /{id}` is declared after this route; if the order regressed, the path
    # would be parsed as an integer id and 422.
    community = await _community_with_subscription(db_session)
    op = await _operation_with_data(db_session, community)
    key = await create_crm_key(db_session, id_community=community.id, consumer_names=[_EAN_A])

    response = await client.get(
        "/crm-data-preview",
        params={"id_key": key.id, "id_sharing_operation": op, **_PERIOD},
        headers=_admin_headers(community),
    )

    assert response.status_code == 200


async def test_preview_flags_participants_with_no_matching_meter(client, db_session):
    community = await _community_with_subscription(db_session)
    op = await _operation_with_data(db_session, community)
    # The default factory names participants C0/C1 — the shape of a key built
    # from a spreadsheet whose columns were not EANs.
    key = await create_crm_key(db_session, id_community=community.id)

    response = await client.get(
        "/crm-data-preview",
        params={"id_key": key.id, "id_sharing_operation": op, **_PERIOD},
        headers=_admin_headers(community),
    )

    data = response.json()["data"]
    assert data["can_simulate"] is False
    assert data["unmatched_participants"] == ["C0", "C1"]
    codes = {b["error_code"] for b in data["blockers"]}
    assert errors.simulation.KEY_CONSUMERS_NOT_MATCHED.code in codes


async def test_preview_matches_participant_names_after_trimming(client, db_session):
    community = await _community_with_subscription(db_session)
    op = await _operation_with_data(db_session, community)
    key = await create_crm_key(
        db_session, id_community=community.id, consumer_names=[f" {_EAN_A} "]
    )

    response = await client.get(
        "/crm-data-preview",
        params={"id_key": key.id, "id_sharing_operation": op, **_PERIOD},
        headers=_admin_headers(community),
    )

    data = response.json()["data"]
    assert data["can_simulate"] is True
    assert data["matched_participants"] == [_EAN_A]


async def test_preview_reports_gaps_as_warnings_without_blocking(client, db_session):
    community = await _community_with_subscription(db_session)
    op = await _operation_with_data(db_session, community, consumer_skip={1, 2})
    key = await create_crm_key(db_session, id_community=community.id, consumer_names=[_EAN_A])

    response = await client.get(
        "/crm-data-preview",
        params={"id_key": key.id, "id_sharing_operation": op, **_PERIOD},
        headers=_admin_headers(community),
    )

    data = response.json()["data"]
    assert data["can_simulate"] is True, "a gap must warn, not block"
    assert data["incomplete_meters"] == [
        {"ean": _EAN_A, "readings": 2, "expected": 4, "missing": 2}
    ]


async def test_preview_blocks_on_duplicate_readings(client, db_session):
    community = await _community_with_subscription(db_session)
    op = await _operation_with_data(db_session, community, duplicate=True)
    key = await create_crm_key(db_session, id_community=community.id, consumer_names=[_EAN_A])

    response = await client.get(
        "/crm-data-preview",
        params={"id_key": key.id, "id_sharing_operation": op, **_PERIOD},
        headers=_admin_headers(community),
    )

    data = response.json()["data"]
    assert data["can_simulate"] is False
    codes = {b["error_code"] for b in data["blockers"]}
    assert errors.simulation.CRM_DUPLICATE_READINGS.code in codes


async def test_preview_rejects_inverted_period(client, db_session):
    community = await _community_with_subscription(db_session)
    op = await _operation_with_data(db_session, community)
    key = await create_crm_key(db_session, id_community=community.id, consumer_names=[_EAN_A])

    response = await client.get(
        "/crm-data-preview",
        params={
            "id_key": key.id,
            "id_sharing_operation": op,
            "period_start": "2025-02-28",
            "period_end": "2025-02-01",
        },
        headers=_admin_headers(community),
    )

    assert response.status_code == 422
    assert response.json()["error_code"] == errors.simulation.INVALID_PERIOD.code


async def test_preview_cannot_reach_another_communitys_operation(client, db_session):
    owner = await _community_with_subscription(db_session)
    op = await _operation_with_data(db_session, owner)
    intruder = await _community_with_subscription(db_session)
    key = await create_crm_key(db_session, id_community=intruder.id, consumer_names=[_EAN_A])

    response = await client.get(
        "/crm-data-preview",
        params={"id_key": key.id, "id_sharing_operation": op, **_PERIOD},
        headers=_admin_headers(intruder),
    )

    assert response.status_code == 404
    assert response.json()["error_code"] == errors.simulation.SHARING_OPERATION_NOT_FOUND.code


async def test_preview_cannot_reach_another_communitys_key(client, db_session):
    owner = await _community_with_subscription(db_session)
    key = await create_crm_key(db_session, id_community=owner.id, consumer_names=[_EAN_A])
    intruder = await _community_with_subscription(db_session)
    op = await _operation_with_data(db_session, intruder)

    response = await client.get(
        "/crm-data-preview",
        params={"id_key": key.id, "id_sharing_operation": op, **_PERIOD},
        headers=_admin_headers(intruder),
    )

    assert response.status_code == 404
    assert response.json()["error_code"] == errors.simulation.KEY_NOT_FOUND.code


# ---------------------------------------------------------------------------
# 2. POST /from-crm
# ---------------------------------------------------------------------------


@patch("api.simulation.service.get_jetstream", MagicMock())
@patch("api.simulation.service.send_event", new_callable=AsyncMock)
async def test_start_from_crm_persists_a_crm_sourced_row(send_event, client, db_session):
    community = await _community_with_subscription(db_session)
    op = await _operation_with_data(db_session, community)
    key = await create_crm_key(db_session, id_community=community.id, consumer_names=[_EAN_A])

    response = await client.post(
        "/from-crm", json=_body(key.id, op), headers=_admin_headers(community)
    )

    assert response.status_code == 200, response.text
    assert response.json()["data"]["status"] == SimulationStatus.PENDING

    row = (
        await db_session.execute(
            select(SimulationModel).where(SimulationModel.id == response.json()["data"]["id"])
        )
    ).scalar_one()
    assert row.source == DataSource.CRM
    assert row.id_sharing_operation == op
    assert row.period_start == datetime.date(2025, 2, 1)
    assert row.period_end == datetime.date(2025, 2, 28)
    assert row.id_key == key.id
    # No file was uploaded, so the file columns stay empty — which the
    # ck_simulation_source CHECK only permits for source = CRM.
    assert row.file_storage_key is None
    assert row.injection_name is None
    send_event.assert_awaited_once()


@patch("api.simulation.service.get_jetstream", MagicMock())
@patch("api.simulation.service.send_event", new_callable=AsyncMock)
async def test_start_from_crm_persists_gap_warnings(send_event, client, db_session):
    community = await _community_with_subscription(db_session)
    op = await _operation_with_data(db_session, community, consumer_skip={1})
    key = await create_crm_key(db_session, id_community=community.id, consumer_names=[_EAN_A])

    response = await client.post(
        "/from-crm", json=_body(key.id, op), headers=_admin_headers(community)
    )

    assert response.status_code == 200, response.text
    row = (
        await db_session.execute(
            select(SimulationModel).where(SimulationModel.id == response.json()["data"]["id"])
        )
    ).scalar_one()
    assert row.data_warnings == {
        "incomplete_meters": [{"ean": _EAN_A, "readings": 3, "expected": 4, "missing": 1}]
    }


@patch("api.simulation.service.get_jetstream", MagicMock())
@patch("api.simulation.service.send_event", new_callable=AsyncMock)
async def test_start_from_crm_refuses_unmatched_participants(send_event, client, db_session):
    community = await _community_with_subscription(db_session)
    op = await _operation_with_data(db_session, community)
    key = await create_crm_key(db_session, id_community=community.id)  # C0 / C1

    response = await client.post(
        "/from-crm", json=_body(key.id, op), headers=_admin_headers(community)
    )

    assert response.status_code == 422
    assert response.json()["error_code"] == errors.simulation.KEY_CONSUMERS_NOT_MATCHED.code
    send_event.assert_not_awaited()


@patch("api.simulation.service.get_jetstream", MagicMock())
@patch("api.simulation.service.send_event", new_callable=AsyncMock)
async def test_start_from_crm_refuses_duplicate_readings(send_event, client, db_session):
    community = await _community_with_subscription(db_session)
    op = await _operation_with_data(db_session, community, duplicate=True)
    key = await create_crm_key(db_session, id_community=community.id, consumer_names=[_EAN_A])

    response = await client.post(
        "/from-crm", json=_body(key.id, op), headers=_admin_headers(community)
    )

    assert response.status_code == 422
    assert response.json()["error_code"] == errors.simulation.CRM_DUPLICATE_READINGS.code
    send_event.assert_not_awaited()


@patch("api.simulation.service.get_jetstream", MagicMock())
@patch("api.simulation.service.send_event", new_callable=AsyncMock)
async def test_start_from_crm_cannot_use_another_communitys_operation(
    send_event, client, db_session
):
    owner = await _community_with_subscription(db_session)
    op = await _operation_with_data(db_session, owner)
    intruder = await _community_with_subscription(db_session)
    key = await create_crm_key(db_session, id_community=intruder.id, consumer_names=[_EAN_A])

    response = await client.post(
        "/from-crm", json=_body(key.id, op), headers=_admin_headers(intruder)
    )

    assert response.status_code == 404
    assert response.json()["error_code"] == errors.simulation.SHARING_OPERATION_NOT_FOUND.code
    send_event.assert_not_awaited()


async def test_start_from_crm_body_is_json_not_query_params(client, db_session):
    # Guards the with_default_error / `from __future__ import annotations` trap:
    # if the route module ever gains that import, the Pydantic body is demoted
    # to query params and every well-formed request 422s with loc=[query, body].
    community = await _community_with_subscription(db_session)
    op = await _operation_with_data(db_session, community)
    key = await create_crm_key(db_session, id_community=community.id, consumer_names=[_EAN_A])

    with (
        patch("api.simulation.service.get_jetstream", MagicMock()),
        patch("api.simulation.service.send_event", new_callable=AsyncMock),
    ):
        response = await client.post(
            "/from-crm", json=_body(key.id, op), headers=_admin_headers(community)
        )

    assert response.status_code != 422, response.text
