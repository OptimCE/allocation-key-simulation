"""NATS subscription + message handler for simulation runs.

A single durable push subscription on ``SIMULATION_SUBJECT`` within a queue
group of the same name; multiple worker replicas share work via JetStream.

Message handling follows a strict failure-class matrix:

* **Deterministic failures** (object missing, key deleted, bad file, bad
  consumer columns, compute error) — mark the simulation row FAILED with an
  error message and ack the message. JetStream must not redeliver a poison
  pill that will fail the same way every time.
* **Transient failures** (storage 5xx/timeout, CRM/local DB hiccup, result
  upload failure) — nak the message so JetStream redelivers, leaving the row
  PENDING.

The dispatcher never holds a DB session while running the computation — each
step opens its own short-lived session.

File-storage cleanup runs only on terminal outcomes (SUCCESS, deterministic
FAILURE), never on transient NAKs (the next delivery still needs the file).
"""

from __future__ import annotations

import asyncio
import dataclasses
import datetime
import logging
import time
from collections.abc import Awaitable, Callable
from concurrent.futures import BrokenExecutor, Executor

from nats.aio.msg import Msg
from nats.js import JetStreamContext
from nats.js.api import ConsumerConfig

from core import metrics as app_metrics
from core import storage
from core.database.database import AsyncSessionCRMFactory, AsyncSessionLocalFactory
from core.queue.helper import Event
from shared import crm_preflight, crm_timeseries, data_loading
from shared.const import SIMULATION_SUBJECT, DataSource, SimulationStatus
from shared.crm_meter_repository import CrmMeterRepository
from shared.crm_repository import CRMRepository
from shared.models.local_models import SimulationModel
from simulation.compute import SimulationInputError, run_simulation
from simulation.inputs import (
    SimulationConsumerInput,
    SimulationIterationInput,
    SimulationKeyInput,
)
from simulation.key_mapping import consumer_names_of, from_crm_allocation_key
from simulation.result import KeySimResult
from worker import persistence

logger = logging.getLogger(__name__)

# The solve runs off-loop in a process pool and several solves share the
# available cores, so an individual solve's wall-clock can stretch under load.
# 10 minutes gives generous headroom against redelivering a still-running solve.
# JetStream will not redeliver until ack_wait elapses, so this also caps how
# long a silent crash stalls the queue.
_ACK_WAIT_SECONDS = 10 * 60

# How long JetStream waits before redelivering after a NAK.
_NAK_RETRY_DELAY_SECONDS = 30

_DURABLE = "worker-simulation"


@dataclasses.dataclass(frozen=True)
class _SimulationSnapshot:
    """Per-message snapshot of the row, captured before the session closes."""

    id: int
    source: DataSource
    # FILE only — None on a CRM-sourced row.
    file_storage_key: str | None
    file_name: str | None
    injection_name: str | None
    # CRM only — None on a file-sourced row.
    id_sharing_operation: int | None
    period_start: datetime.date | None
    period_end: datetime.date | None
    id_key: int
    id_community: int
    status: int


@dataclasses.dataclass(frozen=True)
class _Terminal:
    """Returned by ``_process`` for terminal outcomes; handler acks + cleans up."""

    storage_key: str | None


class _TransientError(Exception):
    """Raised inside ``_process`` to signal the message should be NAK'd.

    Raised (not returned) so the handler leaves the file in place for the
    redelivered message.
    """


async def subscribe(
    js: JetStreamContext,
    *,
    executor: Executor | None = None,
    semaphore: asyncio.Semaphore | None = None,
    inflight: set[asyncio.Task] | None = None,
    max_ack_pending: int | None = None,
):
    """Subscribe the worker to the simulation subject. Returns the subscription.

    ``executor`` / ``semaphore`` / ``inflight`` enable the off-loop solver pool:
    when set, the callback spawns each solve as a tracked background task
    (bounded by the semaphore) and returns, so the listener never blocks. Left
    unset, the callback handles each message inline to completion — the
    single-process fallback the handler unit tests rely on.

    ``max_ack_pending`` caps how many messages JetStream delivers before we ack,
    so the broker never hands us more than the pool can actively solve.
    """
    config_kwargs: dict = {"ack_wait": _ACK_WAIT_SECONDS}
    if max_ack_pending is not None:
        config_kwargs["max_ack_pending"] = max_ack_pending
    sub = await js.subscribe(
        subject=SIMULATION_SUBJECT,
        durable=_DURABLE,
        queue=_DURABLE,
        manual_ack=True,
        cb=_make_handler(executor=executor, semaphore=semaphore, inflight=inflight),
        config=ConsumerConfig(**config_kwargs),
    )
    logger.info("Subscribed to %s (durable=%s)", SIMULATION_SUBJECT, _DURABLE)
    return sub


def _make_handler(
    *,
    executor: Executor | None = None,
    semaphore: asyncio.Semaphore | None = None,
    inflight: set[asyncio.Task] | None = None,
) -> Callable[[Msg], Awaitable[None]]:
    """Build the per-message callback.

    nats-py serialises a subscription's callback — it ``await``\\ s one message
    before pulling the next. So to solve several messages concurrently we spawn
    the work as a background task and return immediately, letting the next
    message be delivered while this one solves. ``inflight`` tracks those tasks
    for graceful drain on shutdown.

    When ``inflight`` is None the message is handled inline to completion — the
    shape the handler unit tests rely on, and a safe single-process fallback.
    """

    def _on_task_done(task: asyncio.Task) -> None:
        if inflight is not None:
            inflight.discard(task)
        if not task.cancelled() and task.exception() is not None:
            logger.error("Simulation handler task crashed: %r", task.exception())

    async def handle(msg: Msg) -> None:
        if inflight is None:
            await _handle_message(msg, executor=executor, semaphore=semaphore)
            return
        task = asyncio.create_task(_handle_message(msg, executor=executor, semaphore=semaphore))
        inflight.add(task)
        task.add_done_callback(_on_task_done)

    return handle


async def _handle_message(
    msg: Msg,
    *,
    executor: Executor | None = None,
    semaphore: asyncio.Semaphore | None = None,
) -> None:
    try:
        event = Event.decode(msg.data)
    except Exception:
        logger.exception("Failed to decode event on %s; acking and dropping", SIMULATION_SUBJECT)
        await msg.ack()
        app_metrics.worker_messages.add(1, {"outcome": "drop_decode"})
        return

    simulation_id = event.data.get("simulation_id") if isinstance(event.data, dict) else None
    if not isinstance(simulation_id, int):
        logger.error(
            "Event on %s has missing/invalid simulation_id: %r",
            SIMULATION_SUBJECT,
            event.data,
        )
        await msg.ack()
        app_metrics.worker_messages.add(1, {"outcome": "drop_invalid_id"})
        return

    try:
        outcome = await _process(simulation_id, msg, executor=executor, semaphore=semaphore)
    except _TransientError as exc:
        logger.warning(
            "Transient failure for simulation %d, will redeliver: %s", simulation_id, exc
        )
        await msg.nak(delay=_NAK_RETRY_DELAY_SECONDS)
        app_metrics.worker_messages.add(1, {"outcome": "nak"})
        return
    except Exception:
        # Catch-all so the subscription never dies. Treat as deterministic:
        # mark FAILED rather than redeliver indefinitely.
        logger.exception("Unhandled error processing simulation %d", simulation_id)
        await persistence.save_failure(simulation_id, "unhandled_worker_error")
        await msg.ack()
        app_metrics.worker_messages.add(1, {"outcome": "ack_unhandled"})
        return

    await msg.ack()
    if outcome.storage_key is not None:
        await _delete_safely(outcome.storage_key)
    app_metrics.worker_messages.add(1, {"outcome": "ack"})


def _run_simulation_subprocess(
    key: SimulationKeyInput, raw: data_loading.SimulationRawData
) -> KeySimResult:
    """Top-level, picklable entry executed inside a solver pool process.

    The pool initializer (``worker.main._init_solver_process``) has already
    imported ``simulation.compute`` in this process. ``run_simulation`` is pure
    synchronous CPU work, so there is nothing async to drive here. ``key``
    (pydantic), ``raw`` (numpy arrays + names) and the ``KeySimResult`` are all
    picklable, so they cross the process boundary cleanly.
    """
    return run_simulation(key, raw.C, raw.VA, raw.consumer_names)


async def _run_simulation(
    key: SimulationKeyInput,
    raw: data_loading.SimulationRawData,
    *,
    executor: Executor | None,
    semaphore: asyncio.Semaphore | None,
) -> KeySimResult:
    """Run the solve, off the event loop when a process pool is configured.

    With ``executor`` set, the CPU-bound solve runs in a separate process so the
    event loop stays responsive (NATS liveness + heartbeat) and several solves
    can use several cores. ``semaphore`` caps how many run at once. Without an
    executor (unit tests / single-process fallback) it runs inline, preserving
    the original behaviour and the ``run_simulation`` monkeypatch seam the tests
    use.
    """
    if executor is None:
        return run_simulation(key, raw.C, raw.VA, raw.consumer_names)
    loop = asyncio.get_running_loop()
    try:
        if semaphore is None:
            return await loop.run_in_executor(executor, _run_simulation_subprocess, key, raw)
        async with semaphore:
            return await loop.run_in_executor(executor, _run_simulation_subprocess, key, raw)
    except BrokenExecutor as exc:
        # A pool worker died abnormally (OOM, native crash). Treat as transient —
        # redeliver rather than wrongly mark this simulation FAILED. The pool
        # rebuilds itself on the next submit (see worker.main._SolverPool), so the
        # redelivery lands on a fresh worker.
        raise _TransientError(f"solver pool broken: {exc}") from exc


async def _process(
    simulation_id: int,
    msg: Msg,
    *,
    executor: Executor | None = None,
    semaphore: asyncio.Semaphore | None = None,
) -> _Terminal:
    # ---- Step 1: snapshot the row, then close the session --------------
    snapshot = await _snapshot_simulation(simulation_id)
    if snapshot is None:
        logger.warning("Simulation %d not found in DB; acking message", simulation_id)
        return _Terminal(storage_key=None)

    if snapshot.status != SimulationStatus.PENDING:
        logger.info(
            "Simulation %d already in status %d; acking redelivery",
            simulation_id,
            snapshot.status,
        )
        return _Terminal(storage_key=snapshot.file_storage_key)

    # ---- Step 2: read the simulated key from the CRM DB ----------------
    # Ahead of the source read because both paths need the key's participant
    # names: the file path matches columns against them, the CRM path matches
    # meter EANs against them.
    try:
        key_model = await _read_crm_key(snapshot.id_key, snapshot.id_community)
    except Exception as exc:
        # CRM read failures are transient (connection/timeout) — redeliver.
        raise _TransientError(f"crm read: {exc}") from exc

    if key_model is None:
        # The key was deleted between enqueue and processing. Deterministic.
        await persistence.save_failure(simulation_id, f"key_not_found: {snapshot.id_key}")
        return _Terminal(storage_key=snapshot.file_storage_key)

    key_input = from_crm_allocation_key(key_model)
    if snapshot.source is DataSource.CRM:
        # Match EANs the way the rest of the platform does, with TRIM. The key
        # itself is normalised (not just the comparison) so that run_simulation's
        # own name -> percentage lookup still resolves: it keys off the input
        # model's names, which must therefore be the same strings as the matrix
        # column labels. The file path is deliberately left untrimmed — changing
        # its matching would alter existing behaviour.
        key_input = _with_trimmed_participants(key_input)
    consumer_names = consumer_names_of(key_input)

    # ---- Step 3: obtain the (C, VA, names) triple ----------------------
    # Two sources, one output. Everything downstream is identical.
    if snapshot.source is DataSource.CRM:
        loaded = await _load_from_crm(snapshot, consumer_names)
    else:
        loaded = await _load_from_file(snapshot, consumer_names)
    if isinstance(loaded, _Terminal):
        return loaded
    raw = loaded

    # ---- Step 5: run the simulation off the event loop ----------------
    start = time.perf_counter()
    try:
        result = await _run_simulation(key_input, raw, executor=executor, semaphore=semaphore)
    except _TransientError:
        # Broken solver pool (a worker died). Redeliver — don't mark FAILED.
        app_metrics.simulation_duration.record(time.perf_counter() - start, {"status": "failed"})
        raise
    except SimulationInputError as exc:
        app_metrics.simulation_duration.record(time.perf_counter() - start, {"status": "failed"})
        await persistence.save_failure(simulation_id, f"simulation_failed: {exc}")
        return _Terminal(storage_key=snapshot.file_storage_key)
    except Exception as exc:
        app_metrics.simulation_duration.record(time.perf_counter() - start, {"status": "failed"})
        logger.exception("Simulation %d raised", simulation_id)
        await persistence.save_failure(simulation_id, f"simulation_failed: {exc}")
        return _Terminal(storage_key=snapshot.file_storage_key)
    app_metrics.simulation_duration.record(time.perf_counter() - start, {"status": "success"})

    # ---- Step 6: persist the result ------------------------------------
    try:
        await persistence.save_success(simulation_id, result)
    except Exception as exc:
        # DB/storage failures during persist are transient — redeliver. Do NOT
        # delete the source file here.
        raise _TransientError(f"persist failed: {exc}") from exc

    logger.info("Simulation %d processed successfully", simulation_id)
    return _Terminal(storage_key=snapshot.file_storage_key)


def _with_trimmed_participants(key: SimulationKeyInput) -> SimulationKeyInput:
    """Return the key with every consumer name trimmed.

    ``allocation_key.consumer.name`` is free text with no FK to ``meter``; the
    platform-wide convention that it holds the EAN is enforced only by
    comparison, and crm-backend compares with ``TRIM``. Normalising the whole
    key (rather than only the comparison) keeps one set of strings in play
    across matching, the matrix column labels, and the result rows.
    """
    return SimulationKeyInput(
        name=key.name,
        description=key.description,
        iterations=[
            SimulationIterationInput(
                number=it.number,
                energy_allocated_percentage=it.energy_allocated_percentage,
                consumers=[
                    SimulationConsumerInput(
                        name=crm_preflight.normalize_participant(c.name),
                        energy_allocated_percentage=c.energy_allocated_percentage,
                    )
                    for c in it.consumers
                ],
            )
            for it in key.iterations
        ],
    )


async def _load_from_file(
    snapshot: _SimulationSnapshot, consumer_names: list[str]
) -> data_loading.SimulationRawData | _Terminal:
    """Download the uploaded object and parse it. The historical path."""
    if snapshot.file_storage_key is None or snapshot.file_name is None:
        # Unreachable through the API (ck_simulation_source enforces it), but a
        # row written directly to the DB could get here.
        await persistence.save_failure(snapshot.id, "file_source_incomplete")
        return _Terminal(storage_key=None)

    try:
        content = await storage.download(snapshot.file_storage_key)
    except storage.ObjectNotFound:
        await persistence.save_failure(snapshot.id, "storage_object_missing")
        return _Terminal(storage_key=None)
    except storage.TransientStorageError as exc:
        raise _TransientError(f"storage download: {exc}") from exc

    try:
        return data_loading.load(
            content, snapshot.file_name, snapshot.injection_name or "", consumer_names
        )
    except (
        data_loading.InvalidInjectionColumnError,
        data_loading.UnsupportedFileFormatError,
        data_loading.ConsumerColumnsError,
    ) as exc:
        await persistence.save_failure(snapshot.id, f"parse_failed: {exc}")
        return _Terminal(storage_key=snapshot.file_storage_key)
    except Exception as exc:
        await persistence.save_failure(snapshot.id, f"parse_failed_unexpected: {exc}")
        return _Terminal(storage_key=snapshot.file_storage_key)


async def _load_from_crm(
    snapshot: _SimulationSnapshot, consumer_names: list[str]
) -> data_loading.SimulationRawData | _Terminal:
    """Read meter_consumption for this row's sharing operation and period.

    The pre-flight is re-run here rather than trusted from creation time: the
    data can have changed since the run was queued, and this is the read that
    actually feeds the simulation. In particular the participant-to-EAN match is
    re-checked, so a meter deleted in the meantime fails loudly.

    Failure classification follows the module's existing matrix — a CRM read
    error is transient (NAK, redeliver), while rejected or unpivotable data is
    deterministic (FAILED, ack). There is never an object to delete.
    """
    if (
        snapshot.id_sharing_operation is None
        or snapshot.period_start is None
        or snapshot.period_end is None
    ):
        await persistence.save_failure(snapshot.id, "crm_source_incomplete")
        return _Terminal(storage_key=None)

    try:
        async with AsyncSessionCRMFactory() as crm_session:
            repository = CrmMeterRepository(crm_session)
            # The worker has no request context, so the community is passed
            # explicitly; with_community_scope would degrade to WHERE false.
            summary = await repository.summarize(
                id_community=snapshot.id_community,
                id_sharing_operation=snapshot.id_sharing_operation,
                period_start=snapshot.period_start,
                period_end=snapshot.period_end,
            )
            preflight = crm_preflight.evaluate(summary, consumer_names)
            # Skip the expensive read when the period is already rejected.
            rows = (
                await repository.fetch_rows(
                    id_community=snapshot.id_community,
                    id_sharing_operation=snapshot.id_sharing_operation,
                    period_start=snapshot.period_start,
                    period_end=snapshot.period_end,
                )
                if preflight.ok
                else []
            )
    except Exception as exc:
        raise _TransientError(f"crm read: {exc}") from exc

    if preflight.blockers:
        detail = "; ".join(b.detail for b in preflight.blockers)
        await persistence.save_failure(snapshot.id, f"crm_data_rejected: {detail}")
        return _Terminal(storage_key=None)

    try:
        frame = crm_timeseries.build_dataframe(rows, preflight.participants)
        # The same converter the file path uses — the frame is deliberately
        # shaped like a parsed upload so nothing below this line differs.
        return data_loading.to_simulation_raw_data(
            frame, crm_timeseries.INJECTION_COLUMN, preflight.participants
        )
    except (
        crm_timeseries.CrmPivotError,
        data_loading.InvalidInjectionColumnError,
        data_loading.ConsumerColumnsError,
    ) as exc:
        await persistence.save_failure(snapshot.id, f"crm_pivot_failed: {exc}")
        return _Terminal(storage_key=None)
    except Exception as exc:
        await persistence.save_failure(snapshot.id, f"crm_pivot_failed_unexpected: {exc}")
        return _Terminal(storage_key=None)


async def _read_crm_key(id_key: int, id_community: int):
    """Load the simulated key from the CRM DB in a short-lived session.

    Extracted so tests can patch the CRM read without standing up a session.
    """
    async with AsyncSessionCRMFactory() as crm_session:
        return await CRMRepository(crm_session).get_allocation_key(id_key, id_community)


async def _delete_safely(key: str) -> None:
    """Best-effort delete; ``storage.delete`` already swallows its own errors."""
    await storage.delete(key)


async def _snapshot_simulation(simulation_id: int) -> _SimulationSnapshot | None:
    """Read the row in a short-lived session and return a frozen snapshot."""
    async with AsyncSessionLocalFactory() as session:
        row = await session.get(SimulationModel, simulation_id)
        if row is None:
            return None
        return _SimulationSnapshot(
            id=row.id,
            source=DataSource(row.source),
            file_storage_key=row.file_storage_key,
            file_name=row.file_name,
            injection_name=row.injection_name,
            id_sharing_operation=row.id_sharing_operation,
            period_start=row.period_start,
            period_end=row.period_end,
            id_key=row.id_key,
            id_community=row.id_community,
            status=int(row.status),
        )
