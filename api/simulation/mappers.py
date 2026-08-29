from api.simulation.schemas import (
    ConsumerResult,
    CrmDataPreview,
    IncompleteMeter,
    IterationResult,
    KeyResult,
    PreviewBlocker,
    Simulation,
    SimulationDetail,
)
from core.i18n import translate
from shared.crm_preflight import Preflight
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


def to_crm_data_preview(preflight: Preflight, locale: str) -> CrmDataPreview:
    """Render a pre-flight verdict for the manager's screen.

    Blocker messages are localised here rather than in the service so that
    translation stays at the API edge — the worker reaches the same verdict via
    ``Preflight`` and needs no locale at all.
    """
    summary = preflight.summary
    return CrmDataPreview(
        can_simulate=preflight.ok,
        matched_participants=preflight.matched,
        unmatched_participants=preflight.unmatched,
        meter_count=len(summary.eans),
        reading_count=summary.total_rows,
        first_timestamp=summary.first_timestamp,
        last_timestamp=summary.last_timestamp,
        total_consumption_kwh=summary.total_consumption_kwh,
        total_injection_kwh=summary.total_injection_kwh,
        # Only participants are reported: a gap in a meter this key ignores
        # changes nothing about the simulation.
        incomplete_meters=[
            IncompleteMeter(
                ean=e.ean,
                readings=e.distinct_ts,
                expected=summary.grid_size,
                missing=summary.grid_size - e.distinct_ts,
            )
            for e in summary.incomplete
            if e.ean in set(preflight.matched)
        ],
        blockers=[
            PreviewBlocker(
                error_code=b.error.code,
                message=translate(b.error.key, locale=locale),
                detail=b.detail,
            )
            for b in preflight.blockers
        ],
    )
