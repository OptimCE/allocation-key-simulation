import logging
from uuid import uuid4

from fastapi import UploadFile

from api.simulation.mappers import to_simulation_detail, to_simulation_schema
from api.simulation.repository import SimulationRepository
from api.simulation.schemas import (
    SimulateRequest,
    SimulateResponse,
    Simulation,
    SimulationDetail,
    SimulationTimeseries,
)
from core import metrics as app_metrics
from core import storage
from core.api_response import Pagination
from core.audit_log import AuditActions, AuditLogInput, AuditLogService
from core.database.database import AsyncSessionCRMFactory, AsyncSessionLocalFactory
from core.errors.errors import ErrorException
from core.queue.helper import Event, send_event
from core.queue.init import get_jetstream
from shared.const import SIMULATION_SUBJECT, SimulationStatus
from shared.crm_repository import CRMRepository
from shared.custom_errors import errors
from shared.models.local_models import SimulationModel

logger = logging.getLogger(__name__)


class SimulationService:
    def __init__(self, local_session, crm_session):
        self.local_session = local_session
        self.crm_session = crm_session
        self.repository = SimulationRepository(local_session)
        self.crm_repository = CRMRepository(crm_session)
        self.audit_log_service = AuditLogService(crm_session)

    async def get_simulations(
        self, page: int, page_size: int, query_param: dict
    ) -> tuple[list[Simulation], Pagination]:
        rows, total = await self.repository.get_list_simulations(page, page_size, query_param)
        data = [to_simulation_schema(n) for n in rows]
        pagination = Pagination(
            page=page, limit=page_size, total=total, total_pages=-(-total // page_size)
        )
        return data, pagination

    async def get_simulation(self, id: int) -> SimulationDetail:
        sim = await self.repository.get_simulation(id)
        if not sim:
            raise ErrorException(error=errors.simulation.SIMULATION_NOT_FOUND, status_code=404)
        return to_simulation_detail(sim)

    async def get_timeseries(self, id: int) -> SimulationTimeseries:
        sim = await self.repository.get_simulation_row(id)
        if not sim:
            raise ErrorException(error=errors.simulation.SIMULATION_NOT_FOUND, status_code=404)
        if not sim.result_storage_key:
            raise ErrorException(error=errors.simulation.RESULT_NOT_FOUND, status_code=404)
        try:
            content = await storage.download(sim.result_storage_key)
        except storage.ObjectNotFound as exc:
            raise ErrorException(
                error=errors.simulation.RESULT_NOT_FOUND, status_code=404
            ) from exc
        return SimulationTimeseries.model_validate_json(content)

    async def start_simulation(
        self, req: SimulateRequest, file: UploadFile, community_id: int
    ) -> SimulateResponse:
        """Validate the CRM key, upload the file, persist a PENDING row, enqueue.

        Ordering mirrors allocation-key-generation: upload-then-commit-then-publish.
        Uploading first means a DB failure leaves only a transient MinIO orphan
        (cleaned up on the rollback path); committing first would leave a row
        pointing at a non-existent object.
        """
        # 1. The simulated key must exist and belong to the caller's community.
        key = await self.crm_repository.get_allocation_key(req.id_key, community_id)
        if key is None:
            raise ErrorException(error=errors.simulation.KEY_NOT_FOUND, status_code=404)

        # 2. Read the upload body. Reject empty files so the worker doesn't waste
        # a slot on a guaranteed parse failure.
        content = await file.read()
        if not content:
            raise ErrorException(error=errors.simulation.INVALID_FILE, status_code=422)
        file_name = file.filename or "uploaded-file"

        # 3. Upload to MinIO. community_id keeps keys browsable per tenant; the
        # UUID guarantees no collisions when two requests reuse a filename.
        storage_key = f"simulations/{community_id}/{uuid4()}/{file_name}"
        try:
            await storage.upload(storage_key, content, content_type=file.content_type)
        except Exception as exc:
            logger.exception(
                "Storage upload failed for community %d key=%s", community_id, storage_key
            )
            raise ErrorException(
                error=errors.simulation.STORAGE_UPLOAD_FAILED, status_code=502
            ) from exc

        # 4. Persist the simulation row. If the DB write fails, the uploaded
        # object is orphaned — clean it up.
        model = SimulationModel(
            name=req.name,
            id_community=community_id,
            file_storage_key=storage_key,
            file_name=file_name,
            injection_name=req.injection_name,
            id_key=req.id_key,
            key_name=key.name,
            status=SimulationStatus.PENDING,
        )
        try:
            await self.repository.create_simulation(model)
            await self.local_session.commit()
        except Exception:
            await _best_effort_delete(storage_key)
            raise
        simulation_id = model.id
        app_metrics.simulations_created.add(1)
        await self.audit_log_service.log(
            AuditLogInput(
                action=AuditActions.SIMULATION_CREATED,
                entity_type="simulation",
                entity_id=str(simulation_id),
                payload={
                    "name": req.name,
                    "id_key": req.id_key,
                    "key_name": key.name,
                    "file_name": file_name,
                    "injection_name": req.injection_name,
                },
            )
        )

        # 5. Publish the run event. On failure, mark the row FAILED in a fresh
        # transaction and delete the orphan object.
        event = Event(type="simulation.requested", data={"simulation_id": simulation_id})
        try:
            await send_event(get_jetstream(), SIMULATION_SUBJECT, event)
        except Exception as exc:
            logger.exception(
                "Failed to publish simulation %d to %s", simulation_id, SIMULATION_SUBJECT
            )
            await self._mark_failed_to_queue(simulation_id, str(exc))
            await _best_effort_delete(storage_key)
            raise ErrorException(
                error=errors.simulation.START_SIMULATION, status_code=500
            ) from exc

        return SimulateResponse(id=simulation_id, status=SimulationStatus.PENDING)

    @staticmethod
    async def _mark_failed_to_queue(simulation_id: int, reason: str) -> None:
        """Mark a simulation FAILED after a publish failure, in a fresh session."""
        id_community: int | None = None
        async with AsyncSessionLocalFactory() as session:
            row = await session.get(SimulationModel, simulation_id)
            if row is None:
                return
            row.status = SimulationStatus.FAILED
            row.error_message = f"failed_to_queue: {reason}"[:2000]
            await session.commit()
            id_community = row.id_community
            app_metrics.simulations_completed.add(1, {"status": "failed"})

        async with AsyncSessionCRMFactory() as crm_session:
            await AuditLogService(crm_session).log(
                AuditLogInput(
                    action=AuditActions.SIMULATION_QUEUE_FAILED,
                    entity_type="simulation",
                    entity_id=str(simulation_id),
                    payload={"reason": reason[:500]},
                ),
                id_community=id_community,
            )
            await crm_session.commit()

    async def delete_simulation(self, id_simulation: int) -> None:
        simulation = await self.repository.get_simulation_row(id_simulation)
        if not simulation:
            raise ErrorException(error=errors.simulation.SIMULATION_NOT_FOUND, status_code=404)
        status_name = SimulationStatus(simulation.status).name
        file_storage_key = simulation.file_storage_key
        result_storage_key = simulation.result_storage_key
        await self.repository.delete_simulation(simulation)
        await self.local_session.commit()
        # Best-effort cleanup of storage objects (source already gone on success).
        await _best_effort_delete(file_storage_key)
        if result_storage_key:
            await _best_effort_delete(result_storage_key)
        await self.audit_log_service.log(
            AuditLogInput(
                action=AuditActions.SIMULATION_DELETED,
                entity_type="simulation",
                entity_id=str(id_simulation),
                payload={"status": status_name},
            )
        )


async def _best_effort_delete(storage_key: str) -> None:
    """Wrap ``storage.delete`` for rollback/cleanup paths (already idempotent)."""
    await storage.delete(storage_key)
