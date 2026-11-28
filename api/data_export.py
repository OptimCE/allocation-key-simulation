"""The simulation-key part of a data export (monorepo contracts/export/README.md, §7).

User scope: nothing. A simulation belongs to its community, not to a person, so the part answers
with no file (``files: []``), as the contract says.

Community scope (an ADMIN, re-checked in crm_db by the kit): every simulation the community ran
and its results, table by table (contract §6: simulations, key, iteration and consumer results),
codes by name, plus each simulation's stored files: the consumption file it was run on (a FILE
source) and its per-timestep series (``result.json``).

The module's subscription is not checked: an export is not a use of the module. A stored file
whose object is gone stays described with ``file_missing: true`` and the ``files_missing``
counter instead of failing every export of the community.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import math
from collections.abc import AsyncIterator, Awaitable, Callable, Mapping
from contextlib import AbstractAsyncContextManager
from dataclasses import dataclass, field
from enum import IntEnum
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from core.config import settings
from core.export import (
    ExportHandler,
    ExportStorageSettings,
    PartContext,
    S3PartStore,
    SqlCrmLookup,
    resolve_requester,
    safe_file_name,
)
from shared.const import DataSource, SimulationStatus
from shared.models.local_models import (
    SimulationConsumerResultModel,
    SimulationIterationResultModel,
    SimulationKeyResultModel,
    SimulationModel,
)

logger = logging.getLogger(__name__)

SERVICE = "simulation-key"
PAGE = 500

SessionFactory = Callable[[], AbstractAsyncContextManager[AsyncSession]]
# The size of an object of the service's bucket, None when there is none.
SizeOf = Callable[[str], Awaitable[int | None]]


@dataclass(frozen=True)
class FileSpec:
    """A stored file a row points to: the attribute holding its key, where it goes in the
    archive, and the name of the item's field that describes it."""

    attribute: str
    field: str
    path: Callable[[Any], str]
    content_type: str | None = None


@dataclass(frozen=True)
class Table:
    """One exported table: its rows, minus ``id_community`` (the scope), codes by name."""

    entity: str
    model: type[Any]
    columns: tuple[str, ...]
    enums: Mapping[str, type[IntEnum]] = field(default_factory=dict)
    files: tuple[FileSpec, ...] = ()
    key: str = "id"


TOTALS = (
    "consumption_total",
    "energy_allocated_total",
    "energy_allocated_consumed_total",
    "residual_volume_total",
    "surplus_total",
)

TABLES: tuple[Table, ...] = (
    Table(
        "simulations",
        SimulationModel,
        (
            "id",
            "name",
            "source",
            "file_name",
            "injection_name",
            "id_sharing_operation",
            "period_start",
            "period_end",
            "id_key",
            "key_name",
            "status",
            "error_message",
            "data_warnings",
            "created_at",
            "updated_at",
        ),
        {"source": DataSource, "status": SimulationStatus},
        files=(
            FileSpec(
                "file_storage_key",
                "input_file",
                lambda row: f"files/{row.id}-input-{safe_file_name(row.file_name or 'input')}",
            ),
            FileSpec(
                "result_storage_key",
                "timeseries_file",
                lambda row: f"files/{row.id}-timeseries.json",
                "application/json",
            ),
        ),
    ),
    Table(
        "key-results",
        SimulationKeyResultModel,
        (
            "id",
            "id_simulation",
            "name",
            "description",
            *TOTALS,
            "self_sufficiency_rate_total",
            "sharing_rate_total",
            "created_at",
            "updated_at",
        ),
    ),
    Table(
        "iteration-results",
        SimulationIterationResultModel,
        (
            "id",
            "id_key_result",
            "number",
            "energy_allocated_percentage",
            *TOTALS,
            "sharing_rate_total",
            "self_sufficiency_rate_total",
            "created_at",
            "updated_at",
        ),
    ),
    Table(
        "consumer-results",
        SimulationConsumerResultModel,
        (
            "id",
            "id_iteration_result",
            "name",
            "energy_allocated_percentage",
            *TOTALS,
            "created_at",
            "updated_at",
        ),
    ),
)


def code(value: Any, enum: type[IntEnum] | None) -> Any:
    """A stored value as the archive writes it: a code by name; a NaN or infinite float, which
    JSON cannot carry, as null."""
    if value is None:
        return None
    if enum is not None:
        try:
            return enum(int(value)).name
        except ValueError:
            # A code retired since keeps its number rather than failing the export.
            return int(value)
    if isinstance(value, float) and not math.isfinite(value):
        return None
    return value


class _Part:
    def __init__(
        self, ctx: PartContext, *, local: AsyncSession, size_of: SizeOf, bucket: str
    ) -> None:
        self.ctx = ctx
        self.local = local
        self.size_of = size_of
        self.bucket = bucket
        self.files = 0
        self.missing = 0

    async def file(
        self, key: str | None, path: str, content_type: str | None
    ) -> dict[str, Any] | None:
        """References a stored object, or describes it as missing. None when the row has none."""
        if not key:
            return None
        self.ctx.checkpoint()
        size = await self.size_of(key)
        if size is None:
            self.missing += 1
            return {"path": None, "file_missing": True}
        self.ctx.add_reference(
            bucket=self.bucket, key=key, path=path, size=size, content_type=content_type
        )
        self.files += 1
        return {"path": f"{SERVICE}/{path}", "file_missing": False}

    async def community_export(self, id_community: int) -> None:
        for table in TABLES:
            written = await self.ctx.write_json(table.entity, self._rows(table, id_community))
            if table.entity == "simulations":
                self.ctx.count("simulations", written)
        self.ctx.count("files", self.files)
        if self.missing:
            self.ctx.count("files_missing", self.missing)

    async def _rows(self, table: Table, id_community: int) -> AsyncIterator[dict[str, Any]]:
        key = getattr(table.model, table.key)
        last: Any = None
        while True:
            self.ctx.checkpoint()
            query = select(table.model).where(table.model.id_community == id_community)
            if last is not None:
                query = query.where(key > last)
            rows = list((await self.local.execute(query.order_by(key).limit(PAGE))).scalars())
            for row in rows:
                item = {
                    name: code(getattr(row, name), table.enums.get(name)) for name in table.columns
                }
                for spec in table.files:
                    item[spec.field] = await self.file(
                        getattr(row, spec.attribute), spec.path(row), spec.content_type
                    )
                yield item
            if len(rows) < PAGE:
                return
            last = getattr(rows[-1], table.key)


def export_handler(
    *, local: SessionFactory, crm: SessionFactory, size_of: SizeOf, bucket: str
) -> ExportHandler:
    """The handler the kit calls for each part of this service."""

    async def handle(ctx: PartContext) -> None:
        async with crm() as crm_session:
            requester = await resolve_requester(SqlCrmLookup(crm_session), ctx.request)
        if requester.community_id is None:
            # A user export: nothing of this service is a person's own (files: []).
            return
        async with local() as local_session:
            part = _Part(ctx, local=local_session, size_of=size_of, bucket=bucket)
            await part.community_export(requester.community_id)

    return handle


async def _s3_size(key: str) -> int | None:
    from core.storage import size

    return await size(key)


def start_export_consumer(stop: asyncio.Event) -> asyncio.Task[None] | None:
    """Starts this service's export consumer, or logs why it cannot (and the API runs on).

    Needs the export account (EXPORT_STORAGE_*), never the service's usual storage account.
    """
    from core.database.database import AsyncSessionCRMFactory, AsyncSessionLocalFactory
    from core.export.consumer import run_export_consumer
    from core.queue.init import get_jetstream

    try:
        storage = ExportStorageSettings.from_env()
        js = get_jetstream()
    except RuntimeError as err:
        logger.warning(
            "Data export disabled: %s",
            err,
            extra={"operation": "export:consumer_disabled", "service": SERVICE},
        )
        return None
    handler = export_handler(
        local=AsyncSessionLocalFactory,
        crm=AsyncSessionCRMFactory,
        size_of=_s3_size,
        bucket=settings.STORAGE_BUCKET,
    )
    return asyncio.create_task(
        run_export_consumer(
            js,
            service=SERVICE,
            handler=handler,
            store=S3PartStore(storage),
            stop=stop,
            expected_bucket=storage.bucket,
        ),
        name="simulation-key-export-consumer",
    )


async def stop_export_consumer(
    task: asyncio.Task[None] | None, stop: asyncio.Event, grace: float = 10.0
) -> None:
    """Stops pulling; a part in progress is aborted and nak'ed (redelivered later)."""
    stop.set()
    if task is None:
        return
    try:
        await asyncio.wait_for(task, timeout=grace)
    except TimeoutError:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task
