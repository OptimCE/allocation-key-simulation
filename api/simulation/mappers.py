from api.simulation.schemas import (
    ConsumerResult,
    IterationResult,
    KeyResult,
    Simulation,
    SimulationDetail,
)
from shared.models.local_models import (
    SimulationConsumerResultModel,
    SimulationIterationResultModel,
    SimulationKeyResultModel,
    SimulationModel,
)


def to_simulation_schema(simulation: SimulationModel) -> Simulation:
    return Simulation(
        id=simulation.id,
        name=simulation.name,
        status=simulation.status,
        id_key=simulation.id_key,
        key_name=simulation.key_name,
    )


def to_consumer_result_schema(consumer: SimulationConsumerResultModel) -> ConsumerResult:
    return ConsumerResult(
        name=consumer.name,
        energy_allocated_percentage=consumer.energy_allocated_percentage,
        consumption_total=consumer.consumption_total,
        energy_allocated_total=consumer.energy_allocated_total,
        energy_allocated_consumed_total=consumer.energy_allocated_consumed_total,
        residual_volume_total=consumer.residual_volume_total,
        surplus_total=consumer.surplus_total,
    )


def to_iteration_result_schema(iteration: SimulationIterationResultModel) -> IterationResult:
    return IterationResult(
        number=iteration.number,
        energy_allocated_percentage=iteration.energy_allocated_percentage,
        consumption_total=iteration.consumption_total,
        energy_allocated_total=iteration.energy_allocated_total,
        energy_allocated_consumed_total=iteration.energy_allocated_consumed_total,
        residual_volume_total=iteration.residual_volume_total,
        surplus_total=iteration.surplus_total,
        sharing_rate_total=iteration.sharing_rate_total,
        self_sufficiency_rate_total=iteration.self_sufficiency_rate_total,
        consumers=[to_consumer_result_schema(c) for c in iteration.consumers],
    )


def to_key_result_schema(key_result: SimulationKeyResultModel) -> KeyResult:
    return KeyResult(
        name=key_result.name,
        description=key_result.description,
        consumption_total=key_result.consumption_total,
        energy_allocated_total=key_result.energy_allocated_total,
        energy_allocated_consumed_total=key_result.energy_allocated_consumed_total,
        residual_volume_total=key_result.residual_volume_total,
        surplus_total=key_result.surplus_total,
        self_sufficiency_rate_total=key_result.self_sufficiency_rate_total,
        sharing_rate_total=key_result.sharing_rate_total,
        # iterations are ordered by `number` for stable charting/display.
        iterations=[
            to_iteration_result_schema(i)
            for i in sorted(key_result.iterations, key=lambda it: it.number)
        ],
    )


def to_simulation_detail(simulation: SimulationModel) -> SimulationDetail:
    return SimulationDetail(
        id=simulation.id,
        name=simulation.name,
        status=simulation.status,
        id_key=simulation.id_key,
        key_name=simulation.key_name,
        error_message=simulation.error_message,
        has_timeseries=simulation.result_storage_key is not None,
        key_result=(to_key_result_schema(simulation.key_result) if simulation.key_result else None),
    )
