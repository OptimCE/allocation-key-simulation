"""Realtime emission from the worker's terminal transitions.

The single most important assertion here is ORDER: the CRM commit must appear
before the emit. Publishing pre-commit does not merely lose an event — it tells
the browser to refetch and read PRE-COMMIT state, and because the transport is
fire-and-forget there is no second event, ever. The result is a permanently
stale UI behind a 200, with no error anywhere.

``patched_sessions`` routes both session factories at the test's
savepoint-joined session, so ``commit()`` is a savepoint release the conftest
rolls back.
"""

from unittest.mock import AsyncMock

import pytest_asyncio

from core.realtime import Tier
from shared.const import SimulationStatus
from tests.factories.simulation_factory import create_simulation
from tests.worker.test_persistence import _result
from worker import persistence


@pytest_asyncio.fixture
async def patched_sessions(db_session, monkeypatch):
    """Route both session factories at the test's savepoint-joined session.

    Defined here rather than imported from test_persistence: importing a fixture
    and then naming a parameter after it shadows the import, which ruff flags as
    a redefinition and which breaks the moment the other module renames it.
    """

    class _Ctx:
        async def __aenter__(self):
            return db_session

        async def __aexit__(self, exc_type, exc, tb):
            return False

    class _Factory:
        def __call__(self):
            return _Ctx()

    monkeypatch.setattr(persistence, "AsyncSessionLocalFactory", _Factory())
    monkeypatch.setattr(persistence, "AsyncSessionCRMFactory", _Factory())
    monkeypatch.setattr(persistence.storage, "upload", AsyncMock())
    yield


@pytest_asyncio.fixture
async def trace(db_session, monkeypatch):
    """Record commits and emits into ONE ordered list."""
    events: list[str] = []

    original_commit = db_session.commit

    async def _commit():
        events.append("commit")
        await original_commit()

    async def _emit(**kwargs):
        events.append("emit")
        events.append(kwargs)

    monkeypatch.setattr(db_session, "commit", _commit)
    monkeypatch.setattr(persistence, "emit", AsyncMock(side_effect=_emit))
    return events


async def test_save_success_emits_after_the_commit(db_session, patched_sessions, trace):
    sim = await create_simulation(db_session, id_community=7)

    await persistence.save_success(sim.id, _result())

    assert "emit" in trace, "no realtime hint was published for a successful simulation"
    # THE assertion. Not "did it emit" but "did it emit after the write landed".
    assert trace.index("emit") > trace.index("commit")


async def test_save_success_addresses_the_community_manager_tier(
    db_session, patched_sessions, trace
):
    sim = await create_simulation(db_session, id_community=7)

    await persistence.save_success(sim.id, _result())

    payload = trace[trace.index("emit") + 1]
    assert payload["topic"] == "simulation.finished"
    assert payload["hint"] == {"status": "success"}
    assert payload["resource"] == ("simulation", sim.id)
    # The audience is the community's MANAGER tier, not a user: `simulation`
    # carries only id_community, and the hub route is manager-gated. This is what
    # lets a worker with no request context reach the right people with zero
    # lookups — and it is only non-leaking because of that gating.
    assert payload["audience"].community_id == 7
    assert payload["audience"].tier is Tier.MANAGER


async def test_save_failure_emits_after_the_commit_with_a_failed_status(
    db_session, patched_sessions, trace
):
    sim = await create_simulation(db_session, id_community=7)

    await persistence.save_failure(sim.id, "boom")

    assert trace.index("emit") > trace.index("commit")
    payload = trace[trace.index("emit") + 1]
    assert payload["hint"] == {"status": "failed"}
    # No error message on the wire: the envelope is a hint, and the client
    # refetches through the gateway, which re-authorizes the read.
    assert "boom" not in str(payload)


async def test_no_emit_when_the_row_is_already_terminal(db_session, patched_sessions, trace):
    # JetStream redelivery: the conditional UPDATE ... WHERE status=PENDING guard
    # returns early, so a second delivery must not re-toast every open hub.
    sim = await create_simulation(db_session, id_community=7, status=SimulationStatus.SUCCESS)

    await persistence.save_success(sim.id, _result())

    assert "emit" not in trace


async def test_double_delivery_emits_exactly_once(db_session, patched_sessions, trace):
    sim = await create_simulation(db_session, id_community=7)

    await persistence.save_success(sim.id, _result())
    await persistence.save_success(sim.id, _result())

    assert trace.count("emit") == 1


async def test_missing_row_emits_nothing(db_session, patched_sessions, trace):
    await persistence.save_success(999_999, _result())
    assert "emit" not in trace
