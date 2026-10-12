"""When JetStream redelivers a simulation, and when it must not.

* **Long runs.** A simulation can outlast the consumer's ack_wait: it may wait
  for the solver slot behind other simulations, and the largest inputs take
  minutes to read, solve and store. A consumer created before ack_wait went to
  10 minutes (the dev one) still redelivers after 2. While the simulation is
  processed the handler sends in-progress acks, so JetStream does not hand the
  message out a second time and the worker does not run it twice.
* **Solver crashes.** A solver process that dies (OOM kill, native crash)
  breaks the whole pool. The simulation is redelivered while its delivery
  count is below ``_MAX_SOLVER_CRASH_DELIVERIES``, then marked FAILED, so a
  crash that repeats on every delivery cannot loop.
* **Shutdown.** The drain in ``worker.main`` cancels simulations that outlast
  it; cancelling one stops its in-progress acks and settles nothing, so the
  message is redelivered once ack_wait runs out.

A ThreadPoolExecutor stands in for the solver process pool (a real one only
where a process has to die), and the in-progress interval is shortened so a
"long" run takes a few hundredths of a second. The solve blocks its thread
until the test releases it, as a real solve blocks its process.
"""

from __future__ import annotations

import asyncio
import multiprocessing
import os
import threading
from concurrent.futures import Executor, Future, ProcessPoolExecutor, ThreadPoolExecutor
from concurrent.futures.process import BrokenProcessPool
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import numpy as np
import pytest
from nats.errors import ConnectionClosedError, NotJSMessageError

from core.queue.helper import Event
from shared.const import DataSource, SimulationStatus
from shared.data_loading import SimulationRawData
from simulation.inputs import (
    SimulationConsumerInput,
    SimulationIterationInput,
    SimulationKeyInput,
)
from simulation.result import KeySimResult
from worker import dispatcher
from worker import main as worker_main

# Short enough that a test sees several in-progress acks within a fraction of
# a second, long enough that the event loop is never flooded.
_FAST_INTERVAL_SECONDS = 0.01

# What a consumer created before ack_wait went from 2 to 10 minutes still runs
# with: nats-py never updates an existing durable's config (the dev one has it).
_OLDEST_ACK_WAIT_SECONDS = 2 * 60

_STORAGE_KEY = "simulations/1/test-uuid/data.csv"

# What ProcessPoolExecutor reports when one of its processes is killed.
_POOL_BROKEN = (
    "A process in the process pool was terminated abruptly while the future was "
    "running or pending."
)


# ---------------------------------------------------------------------------
# Fakes
# ---------------------------------------------------------------------------


class FakeMsg:
    """``nats.aio.msg.Msg`` stand-in that records every settle call in order.

    ``events`` holds "in_progress", "ack" and "nak" as they happen, so a test
    can check that no in-progress ack follows the ack or nak.
    ``num_delivered`` feeds ``metadata.num_delivered``; None makes ``metadata``
    raise, as it does for a message that did not come from JetStream.
    """

    def __init__(
        self,
        data: bytes,
        *,
        num_delivered: int | None = 1,
        in_progress_error: Exception | None = None,
    ) -> None:
        self.data = data
        self.events: list[str] = []
        self.nak_delay: int | None = None
        self._num_delivered = num_delivered
        self._in_progress_error = in_progress_error
        self._progressed = asyncio.Event()

    @property
    def metadata(self) -> SimpleNamespace:
        if self._num_delivered is None:
            raise NotJSMessageError
        return SimpleNamespace(num_delivered=self._num_delivered)

    @property
    def acked(self) -> bool:
        return "ack" in self.events

    @property
    def naked(self) -> bool:
        return "nak" in self.events

    @property
    def in_progress_count(self) -> int:
        return self.events.count("in_progress")

    async def ack(self) -> None:
        self.events.append("ack")

    async def nak(self, delay: int | None = None) -> None:
        self.events.append("nak")
        self.nak_delay = delay

    async def in_progress(self) -> None:
        self.events.append("in_progress")
        self._progressed.set()
        if self._in_progress_error is not None:
            raise self._in_progress_error

    async def wait_for_in_progress(self, count: int) -> None:
        """Return once ``count`` in-progress acks were sent; fail after 5 s."""
        async with asyncio.timeout(5):
            while self.in_progress_count < count:
                self._progressed.clear()
                await self._progressed.wait()


class _CrashingPool(Executor):
    """A pool whose solves all fail as if their process had been killed."""

    def submit(self, fn, /, *args, **kwargs):
        future: Future = Future()
        future.set_exception(BrokenProcessPool(_POOL_BROKEN))
        return future


def _key() -> SimulationKeyInput:
    return SimulationKeyInput(
        name="k",
        description="d",
        iterations=[
            SimulationIterationInput(
                number=1,
                energy_allocated_percentage=1.0,
                consumers=[SimulationConsumerInput(name="A", energy_allocated_percentage=-1.0)],
            )
        ],
    )


def _raw() -> SimulationRawData:
    return SimulationRawData(C=np.zeros((1, 4)), VA=np.ones((1, 4)), consumer_names=["A"])


def _result() -> KeySimResult:
    return KeySimResult(
        name="k",
        description="d",
        consumption_total=0.0,
        energy_allocated_total=0.0,
        energy_allocated_consumed_total=0.0,
        residual_volume_total=0.0,
        surplus_total=0.0,
        self_sufficiency_rate_total=0.0,
        sharing_rate_total=0.0,
        iterations=[],
    )


def _quick_solve(key, c, va, names) -> KeySimResult:
    return _result()


def _blocking_solve(release: threading.Event, error: Exception | None = None):
    """A solve that blocks its thread until ``release`` is set."""

    def solve(key, c, va, names) -> KeySimResult:
        if not release.wait(timeout=10):
            raise TimeoutError("the test never released the solve")
        if error is not None:
            raise error
        return _result()

    return solve


def _make_msg(simulation_id: int = 1, **kwargs) -> FakeMsg:
    data = Event(type="simulation.requested", data={"simulation_id": simulation_id}).encode()
    return FakeMsg(data, **kwargs)


def _make_snapshot(source: DataSource = DataSource.FILE) -> dispatcher._SimulationSnapshot:
    file_source = source is DataSource.FILE
    return dispatcher._SimulationSnapshot(
        id=1,
        source=source,
        file_storage_key=_STORAGE_KEY if file_source else None,
        file_name="data.csv" if file_source else None,
        injection_name="production" if file_source else None,
        id_sharing_operation=None if file_source else 7,
        period_start=None,
        period_end=None,
        id_key=10,
        id_community=1,
        status=int(SimulationStatus.PENDING),
    )


async def _quiet_for_a_few_intervals() -> None:
    """Long enough for a heartbeat that was still running to send again."""
    await asyncio.sleep(_FAST_INTERVAL_SECONDS * 5)


@pytest.fixture
def fast_heartbeat(monkeypatch):
    monkeypatch.setattr(dispatcher, "_IN_PROGRESS_INTERVAL_SECONDS", _FAST_INTERVAL_SECONDS)


@pytest.fixture
def patched_save(monkeypatch):
    save_success = AsyncMock()
    save_failure = AsyncMock()
    monkeypatch.setattr(dispatcher.persistence, "save_success", save_success)
    monkeypatch.setattr(dispatcher.persistence, "save_failure", save_failure)
    return save_success, save_failure


@pytest.fixture
def patched_delete(monkeypatch):
    """Stub the row snapshot, the key read and the file I/O; returns the
    storage.delete spy."""
    monkeypatch.setattr(
        dispatcher, "_snapshot_simulation", AsyncMock(return_value=_make_snapshot())
    )
    monkeypatch.setattr(dispatcher, "_read_crm_key", AsyncMock(return_value=MagicMock()))
    monkeypatch.setattr(dispatcher, "from_crm_allocation_key", MagicMock(return_value=_key()))
    monkeypatch.setattr(dispatcher.storage, "download", AsyncMock(return_value=b"file-bytes"))
    monkeypatch.setattr(dispatcher.data_loading, "load", MagicMock(return_value=_raw()))
    delete = AsyncMock()
    monkeypatch.setattr(dispatcher.storage, "delete", delete)
    return delete


def _use_solve(monkeypatch, solve) -> None:
    # Looked up as a module global inside the solver (thread here, process in
    # production).
    monkeypatch.setattr(dispatcher, "run_simulation", solve)


@pytest.fixture
def solver_thread():
    with ThreadPoolExecutor(max_workers=1) as executor:
        yield executor


# ---------------------------------------------------------------------------
# Long runs: in-progress acks while the simulation is processed
# ---------------------------------------------------------------------------


async def test_a_long_solve_sends_in_progress_acks_until_it_is_acked(
    monkeypatch, fast_heartbeat, patched_save, patched_delete, solver_thread
):
    save_success, save_failure = patched_save
    release = threading.Event()
    _use_solve(monkeypatch, _blocking_solve(release))
    msg = _make_msg()

    task = asyncio.create_task(dispatcher._handle_message(msg, executor=solver_thread))
    try:
        await msg.wait_for_in_progress(3)
        assert not msg.acked
        assert not msg.naked
    finally:
        release.set()
    await task

    assert msg.events[-1] == "ack"
    assert msg.events.count("ack") == 1
    assert not msg.naked
    save_success.assert_awaited_once()
    save_failure.assert_not_awaited()
    patched_delete.assert_awaited_once_with(_STORAGE_KEY)
    # Stopped with the simulation: nothing more after the ack.
    await _quiet_for_a_few_intervals()
    assert msg.events[-1] == "ack"


async def test_waiting_for_a_solver_slot_also_sends_in_progress_acks(
    monkeypatch, fast_heartbeat, patched_save, patched_delete, solver_thread
):
    """The semaphore is shared by every message the worker holds, and the dev
    consumer hands out up to 1000 at once to a one-process pool, so a
    simulation can wait behind others for longer than ack_wait. A redelivery
    then would run it twice."""
    save_success, _save_failure = patched_save
    _use_solve(monkeypatch, _quick_solve)
    slot = asyncio.Semaphore(1)
    await slot.acquire()  # another simulation is solving
    msg = _make_msg()

    task = asyncio.create_task(
        dispatcher._handle_message(msg, executor=solver_thread, semaphore=slot)
    )
    try:
        await msg.wait_for_in_progress(3)
        save_success.assert_not_awaited()
    finally:
        slot.release()
    await task

    assert msg.events[-1] == "ack"
    save_success.assert_awaited_once()
    await _quiet_for_a_few_intervals()
    assert msg.events[-1] == "ack"


async def test_storing_a_large_result_also_sends_in_progress_acks(
    monkeypatch, fast_heartbeat, patched_save, patched_delete, solver_thread
):
    """Writing the result is part of the run: at the size limit
    (shared/problem_size.py) its JSON is about 400 MB to upload."""
    save_success, _save_failure = patched_save
    _use_solve(monkeypatch, _quick_solve)
    stored = asyncio.Event()

    async def slow_save(simulation_id, result, timeseries_path) -> None:
        await stored.wait()

    save_success.side_effect = slow_save
    msg = _make_msg()

    task = asyncio.create_task(dispatcher._handle_message(msg, executor=solver_thread))
    try:
        await msg.wait_for_in_progress(3)
        assert not msg.acked
    finally:
        stored.set()
    await task

    assert msg.events[-1] == "ack"
    await _quiet_for_a_few_intervals()
    assert msg.events[-1] == "ack"


async def test_in_progress_acks_stop_when_the_simulation_fails(
    monkeypatch, fast_heartbeat, patched_save, patched_delete, solver_thread
):
    save_success, save_failure = patched_save
    release = threading.Event()
    _use_solve(monkeypatch, _blocking_solve(release, error=RuntimeError("boom")))
    msg = _make_msg(simulation_id=12)

    task = asyncio.create_task(dispatcher._handle_message(msg, executor=solver_thread))
    try:
        await msg.wait_for_in_progress(2)
    finally:
        release.set()
    await task

    assert msg.events[-1] == "ack"
    save_success.assert_not_awaited()
    save_failure.assert_awaited_once_with(12, "simulation_failed: boom")
    await _quiet_for_a_few_intervals()
    assert msg.events[-1] == "ack"


async def test_in_progress_acks_stop_before_a_transient_nak(
    monkeypatch, fast_heartbeat, patched_save, patched_delete, solver_thread
):
    save_success, _save_failure = patched_save
    release = threading.Event()
    _use_solve(monkeypatch, _blocking_solve(release))
    save_success.side_effect = ConnectionError("database gone")
    msg = _make_msg()

    task = asyncio.create_task(dispatcher._handle_message(msg, executor=solver_thread))
    try:
        await msg.wait_for_in_progress(2)
    finally:
        release.set()
    await task

    assert msg.events[-1] == "nak"
    assert not msg.acked
    patched_delete.assert_not_awaited()
    await _quiet_for_a_few_intervals()
    assert msg.events[-1] == "nak"


async def test_a_failing_in_progress_ack_does_not_fail_the_simulation(
    monkeypatch, fast_heartbeat, patched_save, patched_delete, solver_thread
):
    """A lost in-progress ack only risks a redelivery later; the simulation
    itself carries on and is stored."""
    save_success, save_failure = patched_save
    release = threading.Event()
    _use_solve(monkeypatch, _blocking_solve(release))
    msg = _make_msg(in_progress_error=ConnectionClosedError())

    task = asyncio.create_task(dispatcher._handle_message(msg, executor=solver_thread))
    try:
        # Keeps trying after a failure.
        await msg.wait_for_in_progress(3)
    finally:
        release.set()
    await task

    assert msg.events[-1] == "ack"
    save_success.assert_awaited_once()
    save_failure.assert_not_awaited()


async def test_a_simulation_shorter_than_the_interval_sends_no_in_progress_ack(
    monkeypatch, patched_save, patched_delete, solver_thread
):
    save_success, _save_failure = patched_save
    _use_solve(monkeypatch, _quick_solve)
    msg = _make_msg()

    await dispatcher._handle_message(msg, executor=solver_thread)

    assert msg.events == ["ack"]
    save_success.assert_awaited_once()


def test_in_progress_acks_come_several_times_per_ack_wait():
    """A few lost or late in-progress acks must not let ack_wait lapse, even on
    a consumer that still has the 2-minute ack_wait of older workers."""
    ack_wait = min(dispatcher._ACK_WAIT_SECONDS, _OLDEST_ACK_WAIT_SECONDS)
    assert ack_wait >= 5 * dispatcher._IN_PROGRESS_INTERVAL_SECONDS


# ---------------------------------------------------------------------------
# Shutdown: the drain in worker.main
# ---------------------------------------------------------------------------


async def test_shutdown_drain_lets_a_solve_that_ends_in_time_ack(
    monkeypatch, fast_heartbeat, patched_save, patched_delete, solver_thread
):
    save_success, _save_failure = patched_save
    monkeypatch.setattr(worker_main, "_INFLIGHT_DRAIN_TIMEOUT_SECONDS", 5)
    release = threading.Event()
    _use_solve(monkeypatch, _blocking_solve(release))
    inflight: set[asyncio.Task] = set()
    handler = dispatcher._make_handler(executor=solver_thread, inflight=inflight)
    msg = _make_msg()

    await handler(msg)
    try:
        await msg.wait_for_in_progress(1)
        asyncio.get_running_loop().call_later(_FAST_INTERVAL_SECONDS * 3, release.set)
        await worker_main._drain_inflight(inflight)
    finally:
        release.set()

    assert msg.events[-1] == "ack"
    save_success.assert_awaited_once()
    patched_delete.assert_awaited_once_with(_STORAGE_KEY)


async def test_shutdown_drain_cancels_a_straggler_and_stops_its_in_progress_acks(
    monkeypatch, fast_heartbeat, patched_save, patched_delete, solver_thread
):
    """Cancelled at the drain timeout: neither acked nor naked, no row
    written, file kept, and no more in-progress acks, so JetStream redelivers
    it once ack_wait runs out."""
    save_success, save_failure = patched_save
    monkeypatch.setattr(worker_main, "_INFLIGHT_DRAIN_TIMEOUT_SECONDS", _FAST_INTERVAL_SECONDS)
    release = threading.Event()
    _use_solve(monkeypatch, _blocking_solve(release))
    inflight: set[asyncio.Task] = set()
    handler = dispatcher._make_handler(executor=solver_thread, inflight=inflight)
    msg = _make_msg()

    await handler(msg)
    (task,) = inflight
    try:
        await msg.wait_for_in_progress(2)
        await worker_main._drain_inflight(inflight)
        await asyncio.wait({task})
        beats_at_cancel = msg.in_progress_count
        await _quiet_for_a_few_intervals()
    finally:
        release.set()

    assert task.cancelled()
    assert msg.events == ["in_progress"] * beats_at_cancel
    save_success.assert_not_awaited()
    save_failure.assert_not_awaited()
    patched_delete.assert_not_awaited()


async def test_shutdown_drain_does_nothing_without_inflight_work():
    await worker_main._drain_inflight(set())


# ---------------------------------------------------------------------------
# Solver crashes: retried on a few deliveries, then FAILED
# ---------------------------------------------------------------------------


async def test_run_simulation_reports_a_broken_pool_as_a_solver_crash():
    slot = asyncio.Semaphore(1)

    with pytest.raises(dispatcher._SolverCrashError, match="terminated abruptly"):
        await dispatcher._run_simulation(_key(), _raw(), executor=_CrashingPool(), semaphore=slot)

    assert slot.locked() is False


async def test_a_killed_solver_process_is_reported_as_a_solver_crash():
    """The real thing: a pool process that dies surfaces as a solver crash.

    The process exits in the pool initializer, before it can take the work, so
    nothing needs to be importable inside it. "spawn" on every platform: fork
    from a multi-threaded test process warns, and warnings fail the suite.
    """
    pool = ProcessPoolExecutor(
        max_workers=1,
        mp_context=multiprocessing.get_context("spawn"),
        initializer=os._exit,
        initargs=(1,),
    )
    try:
        with pytest.raises(dispatcher._SolverCrashError):
            await asyncio.wait_for(
                dispatcher._run_simulation(_key(), _raw(), executor=pool, semaphore=None),
                timeout=60,
            )
    finally:
        pool.shutdown(wait=True, cancel_futures=True)


@pytest.mark.parametrize("num_delivered", [1, dispatcher._MAX_SOLVER_CRASH_DELIVERIES - 1])
async def test_a_solver_crash_before_the_last_delivery_is_redelivered(
    patched_save, patched_delete, num_delivered
):
    save_success, save_failure = patched_save
    msg = _make_msg(num_delivered=num_delivered)

    await dispatcher._handle_message(msg, executor=_CrashingPool())

    assert msg.events == ["nak"]
    assert msg.nak_delay == dispatcher._NAK_RETRY_DELAY_SECONDS
    save_success.assert_not_awaited()
    save_failure.assert_not_awaited()
    # The redelivery still needs the file.
    patched_delete.assert_not_awaited()


@pytest.mark.parametrize(
    "num_delivered",
    # The cap itself, and a message already redelivered past it for other
    # reasons (an expired ack_wait, storage NAKs).
    [dispatcher._MAX_SOLVER_CRASH_DELIVERIES, dispatcher._MAX_SOLVER_CRASH_DELIVERIES + 4],
)
async def test_a_solver_crash_on_the_last_delivery_marks_the_simulation_failed(
    patched_save, patched_delete, num_delivered
):
    save_success, save_failure = patched_save
    msg = _make_msg(simulation_id=21, num_delivered=num_delivered)

    await dispatcher._handle_message(msg, executor=_CrashingPool())

    assert msg.events == ["ack"]
    save_success.assert_not_awaited()
    save_failure.assert_awaited_once_with(
        21,
        f"solver_crashed (likely out of memory) on delivery {num_delivered}: {_POOL_BROKEN}",
    )
    patched_delete.assert_awaited_once_with(_STORAGE_KEY)


async def test_a_crash_on_every_delivery_stops_at_the_cap(patched_save, patched_delete):
    """The loop the cap is for: an input too big for the container crashes
    the solver on every delivery. NAKed until the cap, then FAILED and acked,
    which ends the redeliveries."""
    _save_success, save_failure = patched_save

    settled = []
    for delivery in range(1, dispatcher._MAX_SOLVER_CRASH_DELIVERIES + 1):
        msg = _make_msg(simulation_id=25, num_delivered=delivery)
        await dispatcher._handle_message(msg, executor=_CrashingPool())
        settled.append(msg.events)

    assert settled == [["nak"]] * (dispatcher._MAX_SOLVER_CRASH_DELIVERIES - 1) + [["ack"]]
    save_failure.assert_awaited_once()
    patched_delete.assert_awaited_once_with(_STORAGE_KEY)


async def test_a_solver_crash_with_an_unreadable_delivery_count_is_not_retried(
    patched_save, patched_delete
):
    """Without a count there is no way to stop a retry loop, so fail."""
    _save_success, save_failure = patched_save
    msg = _make_msg(simulation_id=22, num_delivered=None)

    await dispatcher._handle_message(msg, executor=_CrashingPool())

    assert msg.events == ["ack"]
    save_failure.assert_awaited_once_with(
        22, f"solver_crashed (likely out of memory) on delivery unknown: {_POOL_BROKEN}"
    )
    patched_delete.assert_awaited_once_with(_STORAGE_KEY)


async def test_a_solver_crash_on_a_crm_sourced_simulation_has_no_file_to_release(
    monkeypatch, patched_save, patched_delete
):
    _save_success, save_failure = patched_save
    monkeypatch.setattr(
        dispatcher,
        "_snapshot_simulation",
        AsyncMock(return_value=_make_snapshot(DataSource.CRM)),
    )
    monkeypatch.setattr(dispatcher, "_load_from_crm", AsyncMock(return_value=_raw()))
    msg = _make_msg(simulation_id=24, num_delivered=dispatcher._MAX_SOLVER_CRASH_DELIVERIES)

    await dispatcher._handle_message(msg, executor=_CrashingPool())

    assert msg.events == ["ack"]
    save_failure.assert_awaited_once_with(
        24,
        "solver_crashed (likely out of memory) on delivery "
        f"{dispatcher._MAX_SOLVER_CRASH_DELIVERIES}: {_POOL_BROKEN}",
    )
    patched_delete.assert_not_awaited()


async def test_a_simulation_exception_in_the_pool_is_not_retried_like_a_crash(
    monkeypatch, patched_save, patched_delete, solver_thread
):
    """Only a dead solver process is retried; an exception the simulation
    raises fails it on its first delivery."""
    _save_success, save_failure = patched_save
    release = threading.Event()
    release.set()
    _use_solve(
        monkeypatch,
        _blocking_solve(release, error=dispatcher.SimulationInputError("missing consumer")),
    )
    msg = _make_msg(simulation_id=23, num_delivered=1)

    await dispatcher._handle_message(msg, executor=solver_thread)

    assert msg.events == ["ack"]
    save_failure.assert_awaited_once_with(23, "simulation_failed: missing consumer")
