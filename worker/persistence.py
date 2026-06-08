"""Persistence helpers for the worker process.

Database + object-storage I/O — neither NATS nor the math live here. The
dispatcher calls ``save_success`` after a successful run and ``save_failure``
on any deterministic failure.

Each helper opens its own ``AsyncSessionLocalFactory`` session so it's safe to
call from any branch of the dispatcher.

Idempotency under JetStream redelivery
--------------------------------------
A long delivery can exceed ack_wait, causing a concurrent second delivery.
Both helpers perform a conditional ``UPDATE simulation SET status=... WHERE
id=? AND status=PENDING`` and use the affected-row count as a lock: only the
worker whose UPDATE actually flipped the row proceeds. The per-timestep result
object is written under a deterministic key (``simulations/{community}/{id}/
result.json``), so even a re-run overwrites in place rather than orphaning.
"""

from __future__ import annotations

import json
import logging

from sqlalchemy import update

from core import metrics as app_metrics
from core import storage
from core.audit_log import AuditActions, AuditLogInput, AuditLogService
from core.database.database import AsyncSessionCRMFactory, AsyncSessionLocalFactory
from shared.const import SimulationStatus
from shared.models.local_models import (
    SimulationConsumerResultModel,
    SimulationIterationResultModel,
    SimulationKeyResultModel,
    SimulationModel,
)
from simulation.result import KeySimResult

logger = logging.getLogger(__name__)

_ERROR_MESSAGE_MAX_LEN = 2000


def _timeseries_bytes(result: KeySimResult) -> bytes:
    """Serialise the per-timestep series to the SimulationTimeseries JSON shape."""
    payload = {
        "iterations": [
            {
                "number": it.number,
                "consumption": it.consumption,
                "energy_allocated": it.energy_allocated,
                "energy_allocated_consumed": it.energy_allocated_consumed,
                "residual_volume": it.residual_volume,
                "surplus": it.surplus,
                "sharing_rate": it.sharing_rate,
                "self_sufficiency_rate": it.self_sufficiency_rate,
                "consumers": [
                    {
                        "name": c.name,
                        "consumption": c.consumption,
                        "energy_allocated": c.energy_allocated,
                        "energy_allocated_consumed": c.energy_allocated_consumed,
                        "residual_volume": c.residual_volume,
                        "surplus": c.surplus,
                    }
                    for c in it.consumers
                ],
            }
            for it in result.iterations
        ]
    }
    return json.dumps(payload).encode()


def _build_key_result(
    result: KeySimResult, simulation_id: int, community_id: int
) -> SimulationKeyResultModel:
    """Build the scalar result tree (key -> iterations -> consumers)."""
    iteration_rows: list[SimulationIterationResultModel] = []
    for it in result.iterations:
        consumer_rows = [
            SimulationConsumerResultModel(
                name=c.name,
                energy_allocated_percentage=c.energy_allocated_percentage,
                consumption_total=c.consumption_total,
                energy_allocated_total=c.energy_allocated_total,
                energy_allocated_consumed_total=c.energy_allocated_consumed_total,
                residual_volume_total=c.residual_volume_total,
                surplus_total=c.surplus_total,
                id_community=community_id,
            )
            for c in it.consumers
        ]
        iteration_rows.append(
            SimulationIterationResultModel(
                number=it.number,
                energy_allocated_percentage=it.energy_allocated_percentage,
                consumption_total=it.consumption_total,
                energy_allocated_total=it.energy_allocated_total,
                energy_allocated_consumed_total=it.energy_allocated_consumed_total,
                residual_volume_total=it.residual_volume_total,
                surplus_total=it.surplus_total,
                sharing_rate_total=it.sharing_rate_total,
                self_sufficiency_rate_total=it.self_sufficiency_rate_total,
                id_community=community_id,
                consumers=consumer_rows,
            )
        )
    return SimulationKeyResultModel(
        name=result.name,
        description=result.description,
        consumption_total=result.consumption_total,
        energy_allocated_total=result.energy_allocated_total,
        energy_allocated_consumed_total=result.energy_allocated_consumed_total,
        residual_volume_total=result.residual_volume_total,
        surplus_total=result.surplus_total,
        self_sufficiency_rate_total=result.self_sufficiency_rate_total,
        sharing_rate_total=result.sharing_rate_total,
        id_simulation=simulation_id,
        id_community=community_id,
        iterations=iteration_rows,
    )


async def save_success(simulation_id: int, result: KeySimResult) -> None:
    """Persist a successful run: claim the row, write the tree + time-series.

    Order: claim (UPDATE ... WHERE status=PENDING) → build tree → upload the
    time-series object → commit. The upload happens inside the transaction so a
    storage failure rolls the claim back (status returns to PENDING) and the
    message is redelivered; the deterministic object key keeps re-runs orphan-free.

    Idempotent: a lost-ack redelivery finds the row non-PENDING and no-ops.
    """
    async with AsyncSessionLocalFactory() as session:
        simulation = await session.get(SimulationModel, simulation_id)
        if simulation is None:
            logger.warning("save_success called for missing simulation id=%d", simulation_id)
            return

        community_id = simulation.id_community
        result_storage_key = f"simulations/{community_id}/{simulation_id}/result.json"

        claim = await session.execute(
            update(SimulationModel)
            .where(
                SimulationModel.id == simulation_id,
                SimulationModel.status == SimulationStatus.PENDING,
            )
            .values(
                status=SimulationStatus.SUCCESS,
                error_message=None,
                result_storage_key=result_storage_key,
            )
        )
        if claim.rowcount == 0:
            logger.info("save_success no-op for simulation %d (already non-PENDING)", simulation_id)
            return

        session.add(_build_key_result(result, simulation_id, community_id))
        await session.flush()

        # Upload the per-timestep series (deterministic key, overwrite-safe).
        await storage.upload(
            result_storage_key, _timeseries_bytes(result), content_type="application/json"
        )

        await session.commit()
        app_metrics.simulations_completed.add(1, {"status": "success"})

    async with AsyncSessionCRMFactory() as crm_session:
        await AuditLogService(crm_session).log(
            AuditLogInput(
                action=AuditActions.SIMULATION_SUCCEEDED,
                entity_type="simulation",
                entity_id=str(simulation_id),
                payload={"iteration_count": len(result.iterations)},
            ),
            id_community=community_id,
        )
        await crm_session.commit()


async def save_failure(simulation_id: int, error_message: str) -> None:
    """Mark a simulation FAILED with the given message (idempotent)."""
    async with AsyncSessionLocalFactory() as session:
        simulation = await session.get(SimulationModel, simulation_id)
        if simulation is None:
            logger.warning("save_failure called for missing simulation id=%d", simulation_id)
            return

        claim = await session.execute(
            update(SimulationModel)
            .where(
                SimulationModel.id == simulation_id,
                SimulationModel.status == SimulationStatus.PENDING,
            )
            .values(
                status=SimulationStatus.FAILED,
                error_message=error_message[:_ERROR_MESSAGE_MAX_LEN],
            )
        )
        if claim.rowcount == 0:
            logger.info("save_failure no-op for simulation %d (already non-PENDING)", simulation_id)
            return

        await session.commit()
        id_community = simulation.id_community
        app_metrics.simulations_completed.add(1, {"status": "failed"})

    async with AsyncSessionCRMFactory() as crm_session:
        await AuditLogService(crm_session).log(
            AuditLogInput(
                action=AuditActions.SIMULATION_FAILED,
                entity_type="simulation",
                entity_id=str(simulation_id),
                payload={"error_message": error_message[:_ERROR_MESSAGE_MAX_LEN]},
            ),
            id_community=id_community,
        )
        await crm_session.commit()
