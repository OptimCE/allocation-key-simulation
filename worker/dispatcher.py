"""NATS subscription + message handler for simulation runs.

A single durable push subscription on ``SIMULATION_SUBJECT`` within a queue
group of the same name; multiple worker replicas share work via JetStream.

Message handling follows a strict failure-class matrix:

* **Deterministic failures** (object missing, key deleted, bad file, bad
  consumer columns, run too large, compute error) — mark the simulation row
  FAILED with an error message and ack the message. JetStream must not
  redeliver a poison pill that will fail the same way every time.
* **Transient failures** (storage 5xx/timeout, CRM/local DB hiccup, result
  upload failure) — nak the message so JetStream redelivers, leaving the row
  PENDING.
* **Solver crashes** (a solver pool process died: OOM kill, native crash)
  — nak while the message's delivery count is below
  ``_MAX_SOLVER_CRASH_DELIVERIES``, then mark the row FAILED
  (``solver_crashed``) and ack. A crash breaks the whole pool, so with more
  than one solver process the simulations solving beside the one that caused
  it see it too and are retried as well. A crash that is deterministic (input
  too large for the container) repeats on every delivery and must not loop
  forever.

Long runs
---------
A simulation can outlast the consumer's ack_wait: it may first wait for a
solver slot (the semaphore is shared by every message the worker holds), and
the largest inputs (5 M CRM readings) take about a minute to read, solve and
store. While ``_process`` works, the handler sends JetStream an in-progress ack
every ``_IN_PROGRESS_INTERVAL_SECONDS``, which restarts the message's ack_wait
clock. The message is redelivered only once those stop: the worker died, lost
NATS, or cancelled the simulation on shutdown.

The dispatcher never holds a DB session while running the computation — each
step opens its own short-lived session.

Memory
------
This process must not be the one that runs out of memory: its death takes the
whole worker down, the in-progress acks stop, and the message comes back after
ack_wait with nothing counting how often (``_MAX_SOLVER_CRASH_DELIVERIES``
counts only solver crashes). So the run's size is checked before the solve
(``shared.problem_size``), and its result, whose per-timestep series dominate,
never comes back here whole: the solver process writes the series to a JSON
file (``worker.timeseries``), returns the totals, and ``persistence`` streams
the file to storage. What grows here is the input: the CRM read (bounded by
``crm_preflight.MAX_READING_ROWS``) or the parsed upload (bounded by the upload
size limit). The solver processes volunteer as the kernel's first victim when
the container runs out (``worker.main._init_solver_process``).

File-storage cleanup runs only on terminal outcomes (SUCCESS, deterministic
FAILURE), never on transient NAKs (the next delivery still needs the file).
"""

from __future__ import annotations

import asyncio
import contextlib
import dataclasses
import datetime
import logging
import time
from collections.abc import AsyncIterator, Awaitable, Callable
from concurrent.futures import BrokenExecutor, Executor

from nats.aio.msg import Msg
from nats.js import JetStreamContext
from nats.js.api import ConsumerConfig

from core import metrics as app_metrics
from core import storage
from core.database.database import AsyncSessionCRMFactory, AsyncSessionLocalFactory
from core.queue.helper import Event, subscribe_durable
from shared import crm_preflight, crm_timeseries, data_loading, problem_size
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
from worker import persistence, timeseries

logger = logging.getLogger(__name__)

# JetStream redelivers a message that is neither acked nor marked in progress
# within this window. The handler keeps resetting it with in-progress acks
# (``_IN_PROGRESS_INTERVAL_SECONDS``), so what it still bounds is how long a
# simulation whose worker died or lost NATS waits before another delivery.
# Like max_ack_pending, it only applies when the durable is created: nats-py
# reuses an existing consumer's config as is, so a consumer created before
# this was raised from 2 minutes (the dev one, from 2026-06-05) still
# redelivers after 120 s.
_ACK_WAIT_SECONDS = 10 * 60

# How often a simulation still being processed sends an in-progress ack. Well
# under even the 2-minute ack_wait of older consumers, so a few lost or late
# acks never let it lapse. Late ones happen: pivoting the largest CRM reads
# holds the event loop for about 9 s.
_IN_PROGRESS_INTERVAL_SECONDS = 20

# How long JetStream waits before redelivering after a NAK.
_NAK_RETRY_DELAY_SECONDS = 30

# The delivery on which a solver crash stops being retried and the simulation
# is marked FAILED. Counts every delivery of the message, including ones that
# ended for another reason (a storage NAK, an expired ack_wait). Small, because
# a deterministic crash costs a full run each time; above 1, because the crash
# may have come from another simulation sharing the pool, or from memory
# pressure that has since passed.
_MAX_SOLVER_CRASH_DELIVERIES = 3

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


@dataclasses.dataclass(frozen=True)
class _Solved:
    """What a solve hands back to this process.

    ``result`` has its totals but no per-timestep series
    (``timeseries.without_series``); those are in the JSON file at
    ``timeseries_path``, which ``_process`` deletes once they are stored.
    """

    result: KeySimResult
    timeseries_path: str


class _TransientError(Exception):
    """Raised inside ``_process`` to signal the message should be NAK'd.

    Raised (not returned) so the handler leaves the file in place for the
    redelivered message.
    """


class _SolverCrashError(Exception):
    """Raised by ``_run_simulation`` when a solver pool process died mid-solve.

    The pool reports that as ``BrokenExecutor`` (``BrokenProcessPool``),
    typically the kernel's OOM killer or a native crash. ``_process`` decides
    from the delivery count whether it is retried.
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

    ``subscribe_durable`` writes ``ack_wait`` and ``max_ack_pending`` onto a
    durable an earlier build created, which ``js.subscribe`` alone does not.
    """
    config_kwargs: dict = {"ack_wait": _ACK_WAIT_SECONDS}
    if max_ack_pending is not None:
        config_kwargs["max_ack_pending"] = max_ack_pending
    sub = await subscribe_durable(
        js,
        subject=SIMULATION_SUBJECT,
        durable=_DURABLE,
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
        async with _in_progress_until_done(msg, simulation_id):
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


@contextlib.asynccontextmanager
async def _in_progress_until_done(msg: Msg, simulation_id: int) -> AsyncIterator[None]:
    """Keep JetStream from redelivering ``msg`` while the block runs.

    Covers all of ``_process``, not only the solve: waiting for a solver slot
    can also outlast ack_wait (the semaphore is shared by every message the
    worker holds), and a redelivered copy would become a second task that runs
    the whole simulation again. Stops on any exit, normal, raised or cancelled
    (the shutdown drain), before the handler acks or naks.
    """
    heartbeat = asyncio.create_task(_send_in_progress(msg, simulation_id))
    try:
        yield
    finally:
        heartbeat.cancel()
        # Wait for it to stop, so no in-progress ack follows the ack or nak.
        # asyncio.wait neither raises the heartbeat's CancelledError nor
        # swallows a cancellation of this task.
        await asyncio.wait({heartbeat})


async def _send_in_progress(msg: Msg, simulation_id: int) -> None:
    """Send an in-progress ack every ``_IN_PROGRESS_INTERVAL_SECONDS``, forever."""
    while True:
        await asyncio.sleep(_IN_PROGRESS_INTERVAL_SECONDS)
        try:
            await msg.in_progress()
        except Exception as exc:
            # Not worth failing the simulation over: the next one may get
            # through, and if none do, the message is redelivered after
            # ack_wait as if it had no heartbeat.
            logger.warning("In-progress ack for simulation %d failed: %s", simulation_id, exc)


def _delivery_count(msg: Msg) -> int | None:
    """JetStream's delivery count for ``msg`` (1 on the first delivery), or None."""
    try:
        return int(msg.metadata.num_delivered)
    except Exception:
        return None


def _run_simulation_subprocess(
    key: SimulationKeyInput, raw: data_loading.SimulationRawData
) -> _Solved:
    """Top-level, picklable entry executed inside a solver pool process.

    The pool initializer (``worker.main._init_solver_process``) has already
    imported ``simulation.compute`` in this process. ``run_simulation`` is pure
    synchronous CPU work, so there is nothing async to drive here. ``key``
    (pydantic), ``raw`` (numpy arrays + names) and the returned ``_Solved`` are
    all picklable, so they cross the process boundary cleanly.

    The per-timestep series are written to a JSON file here, in the solver
    process, and only the totals go back: as Python floats the series take 160
    bytes per participant, timestep and iteration, and would otherwise be
    pickled here and rebuilt in the main process.
    """
    result = run_simulation(key, raw.C, raw.VA, raw.consumer_names)
    path = timeseries.to_temp_file(result)
    try:
        return _Solved(result=timeseries.without_series(result), timeseries_path=path)
    except BaseException:
        timeseries.remove(path)
        raise


async def _run_simulation(
    key: SimulationKeyInput,
    raw: data_loading.SimulationRawData,
    *,
    executor: Executor | None,
    semaphore: asyncio.Semaphore | None,
) -> _Solved:
    """Run the solve, off the event loop when a process pool is configured.

    With ``executor`` set, the CPU-bound solve runs in a separate process so the
    event loop stays responsive (NATS liveness + heartbeat) and several solves
    can use several cores. ``semaphore`` caps how many run at once. Without an
    executor (unit tests / single-process fallback) it runs inline, preserving
    the original behaviour and the ``run_simulation`` monkeypatch seam the tests
    use.
    """
    if executor is None:
        return _run_simulation_subprocess(key, raw)
    loop = asyncio.get_running_loop()
    try:
        if semaphore is None:
            return await loop.run_in_executor(executor, _run_simulation_subprocess, key, raw)
        async with semaphore:
            return await loop.run_in_executor(executor, _run_simulation_subprocess, key, raw)
    except BrokenExecutor as exc:
        # A pool worker died abnormally (OOM kill, native crash). That breaks the
        # whole pool, so every solve running in it lands here, not only the one
        # that crashed. The pool rebuilds itself on the next submit (see
        # worker.main._SolverPool); whether this simulation gets that next
        # submit is _after_solver_crash's call.
        raise _SolverCrashError(str(exc)) from exc


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
        loaded = await _load_from_crm(
            snapshot, consumer_names, iterations=len(key_input.iterations)
        )
    else:
        loaded = await _load_from_file(snapshot, consumer_names)
    if isinstance(loaded, _Terminal):
        return loaded
    raw = loaded

    # ---- Step 4: refuse a run too large to produce ---------------------
    # The CRM pre-flight and the upload route check this before queueing, but
    # the upload count is approximate (and skipped for a file it cannot read).
    # This is the exact parsed shape, checked before any solver memory is spent.
    num_participants, num_timesteps = raw.C.shape
    oversize = problem_size.too_large(num_participants, num_timesteps, len(key_input.iterations))
    if oversize:
        await persistence.save_failure(simulation_id, f"simulation_too_large: {oversize}")
        return _Terminal(storage_key=snapshot.file_storage_key)

    # ---- Step 5: run the simulation off the event loop ----------------
    start = time.perf_counter()
    try:
        solved = await _run_simulation(key_input, raw, executor=executor, semaphore=semaphore)
    except _SolverCrashError as exc:
        app_metrics.simulation_duration.record(time.perf_counter() - start, {"status": "failed"})
        return await _after_solver_crash(simulation_id, msg, exc, snapshot.file_storage_key)
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
        await persistence.save_success(simulation_id, solved.result, solved.timeseries_path)
    except Exception as exc:
        # DB/storage failures during persist are transient — redeliver. Do NOT
        # delete the source file here.
        raise _TransientError(f"persist failed: {exc}") from exc
    finally:
        # A redelivery solves again and writes a new file.
        timeseries.remove(solved.timeseries_path)

    logger.info("Simulation %d processed successfully", simulation_id)
    return _Terminal(storage_key=snapshot.file_storage_key)


async def _after_solver_crash(
    simulation_id: int, msg: Msg, exc: _SolverCrashError, storage_key: str | None
) -> _Terminal:
    """Retry a solver crash on a later delivery, up to a point.

    Raises ``_TransientError`` (NAK) while the delivery count is below
    ``_MAX_SOLVER_CRASH_DELIVERIES``: the crash may have come from another
    simulation sharing the pool, or from memory pressure that has passed.
    From that delivery on (or when the count cannot be read, so a crash can
    never loop) the simulation is marked FAILED and its file released.
    """
    delivered = _delivery_count(msg)
    if delivered is not None and delivered < _MAX_SOLVER_CRASH_DELIVERIES:
        raise _TransientError(
            f"solver crashed on delivery {delivered} of {_MAX_SOLVER_CRASH_DELIVERIES}: {exc}"
        ) from exc
    delivery = "unknown" if delivered is None else str(delivered)
    logger.error(
        "Solver crashed for simulation %d on delivery %s; not retrying: %s",
        simulation_id,
        delivery,
        exc,
    )
    await persistence.save_failure(
        simulation_id, f"solver_crashed (likely out of memory) on delivery {delivery}: {exc}"
    )
    return _Terminal(storage_key=storage_key)


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
        data_loading.NonFiniteValueError,
    ) as exc:
        await persistence.save_failure(snapshot.id, f"parse_failed: {exc}")
        return _Terminal(storage_key=snapshot.file_storage_key)
    except Exception as exc:
        await persistence.save_failure(snapshot.id, f"parse_failed_unexpected: {exc}")
        return _Terminal(storage_key=snapshot.file_storage_key)


async def _load_from_crm(
    snapshot: _SimulationSnapshot, consumer_names: list[str], *, iterations: int
) -> data_loading.SimulationRawData | _Terminal:
    """Read meter_consumption for this row's sharing operation and period.

    The pre-flight is re-run here rather than trusted from creation time: the
    data can have changed since the run was queued, and this is the read that
    actually feeds the simulation. In particular the participant-to-EAN match is
    re-checked, so a meter deleted in the meantime fails loudly, and the run's
    size (``iterations`` is the key's) is checked before the readings are read.

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
            preflight = crm_preflight.evaluate(summary, consumer_names, iterations=iterations)
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
        data_loading.NonFiniteValueError,
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
