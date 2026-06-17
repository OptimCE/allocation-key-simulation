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
from shared import data_loading
from shared.const import SIMULATION_SUBJECT, SimulationStatus
from shared.crm_repository import CRMRepository
from shared.models.local_models import SimulationModel
from simulation.compute import SimulationInputError, run_simulation
from simulation.inputs import SimulationKeyInput
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
    file_storage_key: str
    file_name: str
    injection_name: str
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

    # ---- Step 2: download the source file ------------------------------
    try:
        content = await storage.download(snapshot.file_storage_key)
    except storage.ObjectNotFound:
        await persistence.save_failure(simulation_id, "storage_object_missing")
        return _Terminal(storage_key=None)
    except storage.TransientStorageError as exc:
        raise _TransientError(f"storage download: {exc}") from exc

    # ---- Step 3: read the simulated key from the CRM DB ----------------
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
    consumer_names = consumer_names_of(key_input)

    # ---- Step 4: parse the file (consumer columns matched by name) -----
    try:
        raw = data_loading.load(
            content, snapshot.file_name, snapshot.injection_name, consumer_names
        )
    except (
        data_loading.InvalidInjectionColumnError,
        data_loading.UnsupportedFileFormatError,
        data_loading.ConsumerColumnsError,
    ) as exc:
        await persistence.save_failure(simulation_id, f"parse_failed: {exc}")
        return _Terminal(storage_key=snapshot.file_storage_key)
    except Exception as exc:
        await persistence.save_failure(simulation_id, f"parse_failed_unexpected: {exc}")
        return _Terminal(storage_key=snapshot.file_storage_key)

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
            file_storage_key=row.file_storage_key,
            file_name=row.file_name,
            injection_name=row.injection_name,
            id_key=row.id_key,
            id_community=row.id_community,
            status=int(row.status),
        )
