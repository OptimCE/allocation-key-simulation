import datetime
from typing import cast

import factory

from shared.const import SimulationStatus
from shared.models.crm_models import AllocationKeyModel, ConsumerModel, IterationModel
from shared.models.local_models import (
    SimulationConsumerResultModel,
    SimulationIterationResultModel,
    SimulationKeyResultModel,
    SimulationModel,
)


def _now() -> datetime.datetime:
    return datetime.datetime.now(datetime.UTC).replace(tzinfo=None)


class SimulationFactory(factory.Factory):
    class Meta:
        model = SimulationModel

    name = factory.Sequence(lambda n: f"Simulation {n}")
    file_storage_key = factory.Sequence(lambda n: f"simulations/1/test-{n}/data.csv")
    file_name = "data.csv"
    injection_name = "production"
    id_key = factory.Sequence(lambda n: n + 1)
    key_name = factory.Sequence(lambda n: f"Key {n}")
    status = SimulationStatus.PENDING
    created_at = factory.LazyFunction(_now)
    updated_at = factory.LazyFunction(_now)


# ---------------------------------------------------------------------------
# async helpers — flush only, never commit (the db_session fixture owns the
# outer transaction and rolls it back at end of test).
# ---------------------------------------------------------------------------


async def create_simulation(session, *, id_community: int, **kwargs) -> SimulationModel:
    sim = cast(SimulationModel, SimulationFactory.build(id_community=id_community, **kwargs))
    session.add(sim)
    await session.flush()
    return sim


async def create_crm_key(
    session,
    *,
    id_community: int,
    name: str = "CRM Key",
    description: str = "a key",
    iterations: int = 1,
    consumers_per_iteration: int = 2,
    energy_allocated_percentage: float = 0.5,
    consumer_names: list[str] | None = None,
) -> AllocationKeyModel:
    """Seed a CRM allocation_key tree (consumer names stable across iterations).

    The simulation API validates ``id_key`` against this CRM table, and the
    worker reads the tree from here.

    ``consumer_names`` overrides the default ``C0``/``C1`` labels. CRM-sourced
    runs need them to be meter EANs, since that is the (unenforced) convention
    the platform matches ``consumer.name`` against.
    """
    names = consumer_names or [f"C{j}" for j in range(consumers_per_iteration)]
    key = AllocationKeyModel(name=name, description=description, id_community=id_community)
    session.add(key)
    await session.flush()
    for i in range(iterations):
        iteration = IterationModel(
            number=i,
            energy_allocated_percentage=1.0,
            id_key=key.id,
            id_community=id_community,
        )
        session.add(iteration)
        await session.flush()
        for consumer_name in names:
            session.add(
                ConsumerModel(
                    name=consumer_name,
                    energy_allocated_percentage=energy_allocated_percentage,
                    id_iteration=iteration.id,
                    id_community=id_community,
                )
            )
    await session.flush()
    return key


async def create_simulation_with_result(session, *, id_community: int, **kwargs) -> SimulationModel:
    """Create a SUCCESS simulation with a full scalar result tree."""
    result_storage_key = kwargs.pop("result_storage_key", "simulations/1/result.json")
    sim = await create_simulation(
        session,
        id_community=id_community,
        status=SimulationStatus.SUCCESS,
        result_storage_key=result_storage_key,
        **kwargs,
    )
    key_result = SimulationKeyResultModel(
        name="K",
        description="d",
        consumption_total=100.0,
        energy_allocated_total=100.0,
        energy_allocated_consumed_total=80.0,
        residual_volume_total=20.0,
        surplus_total=20.0,
        self_sufficiency_rate_total=0.8,
        sharing_rate_total=0.8,
        id_simulation=sim.id,
        id_community=id_community,
    )
    session.add(key_result)
    await session.flush()
    iteration = SimulationIterationResultModel(
        number=0,
        energy_allocated_percentage=1.0,
        consumption_total=100.0,
        energy_allocated_total=100.0,
        energy_allocated_consumed_total=80.0,
        residual_volume_total=20.0,
        surplus_total=20.0,
        sharing_rate_total=0.8,
        self_sufficiency_rate_total=0.8,
        id_key_result=key_result.id,
        id_community=id_community,
    )
    session.add(iteration)
    await session.flush()
    session.add(
        SimulationConsumerResultModel(
            name="C0",
            energy_allocated_percentage=0.5,
            consumption_total=100.0,
            energy_allocated_total=100.0,
            energy_allocated_consumed_total=80.0,
            residual_volume_total=20.0,
            surplus_total=20.0,
            id_iteration_result=iteration.id,
            id_community=id_community,
        )
    )
    await session.flush()
    return sim
