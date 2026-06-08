"""Idempotency tests for worker.persistence.

The conditional ``UPDATE ... WHERE status=PENDING`` guard makes the persistence
layer the last line of defense against JetStream redelivery: only one of two
concurrent calls flips the row; the loser writes nothing.

``patched_factory`` routes ``persistence.AsyncSessionLocalFactory`` and
``AsyncSessionCRMFactory`` to the test's per-test session (savepoint join mode),
so every commit becomes a savepoint release the conftest rolls back. ``storage``
is mocked so no MinIO is contacted.
"""

from unittest.mock import AsyncMock

import pytest_asyncio
from sqlalchemy import select

from shared.const import SimulationStatus
from shared.models.local_models import SimulationKeyResultModel, SimulationModel
from simulation.result import ConsumerSimResult, IterationSimResult, KeySimResult
from tests.factories.simulation_factory import create_simulation
from worker import persistence


def _result() -> KeySimResult:
    consumer = ConsumerSimResult(
        name="C0",
        energy_allocated_percentage=0.5,
        consumption=[100.0],
        consumption_total=100.0,
        energy_allocated=[100.0],
        energy_allocated_total=100.0,
        energy_allocated_consumed=[80.0],
        energy_allocated_consumed_total=80.0,
        residual_volume=[20.0],
        residual_volume_total=20.0,
        surplus=[20.0],
        surplus_total=20.0,
    )
    iteration = IterationSimResult(
        number=0,
        energy_allocated_percentage=1.0,
        consumption=[100.0],
        consumption_total=100.0,
        energy_allocated=[100.0],
        energy_allocated_total=100.0,
        energy_allocated_consumed=[80.0],
        energy_allocated_consumed_total=80.0,
        residual_volume=[20.0],
        residual_volume_total=20.0,
        surplus=[20.0],
        surplus_total=20.0,
        sharing_rate=[0.8],
        sharing_rate_total=0.8,
        self_sufficiency_rate=[0.8],
        self_sufficiency_rate_total=0.8,
        consumers=[consumer],
    )
    return KeySimResult(
        name="K",
        description="d",
        consumption_total=100.0,
        energy_allocated_total=100.0,
        energy_allocated_consumed_total=80.0,
        residual_volume_total=20.0,
        surplus_total=20.0,
        self_sufficiency_rate_total=0.8,
        sharing_rate_total=0.8,
        iterations=[iteration],
    )


@pytest_asyncio.fixture
async def patched_factory(db_session, monkeypatch):
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


async def _count_results(db_session, simulation_id: int) -> int:
    rows = await db_session.execute(
        select(SimulationKeyResultModel).where(
            SimulationKeyResultModel.id_simulation == simulation_id
        )
    )
    return len(rows.scalars().all())


async def test_save_success_writes_tree_and_flips_status(db_session, patched_factory):
    sim = await create_simulation(db_session, id_community=1)

    await persistence.save_success(sim.id, _result())

    await db_session.refresh(sim)
    assert sim.status == SimulationStatus.SUCCESS
    assert sim.result_storage_key == f"simulations/1/{sim.id}/result.json"
    assert await _count_results(db_session, sim.id) == 1
    persistence.storage.upload.assert_awaited_once()


async def test_save_success_no_op_when_already_succeeded(db_session, patched_factory):
    sim = await create_simulation(db_session, id_community=1, status=SimulationStatus.SUCCESS)
    await persistence.save_success(sim.id, _result())
    assert await _count_results(db_session, sim.id) == 0


async def test_save_success_idempotent_on_double_call(db_session, patched_factory):
    sim = await create_simulation(db_session, id_community=1)
    await persistence.save_success(sim.id, _result())
    await persistence.save_success(sim.id, _result())
    assert await _count_results(db_session, sim.id) == 1


async def test_save_success_no_op_for_missing_row(db_session, patched_factory):
    await persistence.save_success(999_999, _result())
    rows = await db_session.execute(select(SimulationModel))
    assert rows.scalars().all() == []


async def test_save_failure_flips_status_when_pending(db_session, patched_factory):
    sim = await create_simulation(db_session, id_community=1)
    await persistence.save_failure(sim.id, "boom")
    await db_session.refresh(sim)
    assert sim.status == SimulationStatus.FAILED
    assert sim.error_message == "boom"


async def test_save_failure_no_op_when_already_succeeded(db_session, patched_factory):
    sim = await create_simulation(db_session, id_community=1, status=SimulationStatus.SUCCESS)
    await persistence.save_failure(sim.id, "ignored")
    await db_session.refresh(sim)
    assert sim.status == SimulationStatus.SUCCESS
    assert sim.error_message is None


async def test_save_failure_truncates_long_message(db_session, patched_factory):
    sim = await create_simulation(db_session, id_community=1)
    await persistence.save_failure(sim.id, "x" * 5000)
    await db_session.refresh(sim)
    assert sim.error_message is not None
    assert len(sim.error_message) == 2000
