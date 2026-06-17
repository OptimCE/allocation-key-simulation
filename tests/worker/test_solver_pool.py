"""Tests for the solver process-pool offload added to the worker.

These cover the moving parts introduced to keep the CPU-bound solve off the
event loop (so NATS PING/PONG + the liveness heartbeat never starve) and to
run several simulations in parallel:

* ``worker.main._detect_available_cpus`` / ``_solver_pool_size`` — sizing the
  pool from the CPUs the container is actually allowed to use (cgroup quota),
  not the host core count.
* ``worker.main._SolverPool`` — rebuilds itself when a pool worker dies.
* ``worker.dispatcher._run_simulation`` / ``_run_simulation_subprocess`` — the
  inline-vs-executor branch, the picklable subprocess entry point, and the
  broken-pool → transient mapping.
* ``worker.dispatcher._make_handler`` spawning path — when an ``inflight`` set
  is provided the callback returns immediately and the work runs as a tracked
  background task (inline-to-completion when it is not).

A ThreadPoolExecutor / fakes stand in for the ProcessPoolExecutor so the tests
stay fast and cross-platform (no spawn/pickle round-trip); picklability of the
payloads is asserted separately.
"""

from __future__ import annotations

import asyncio
import pickle
from concurrent.futures import Executor, ThreadPoolExecutor
from concurrent.futures.process import BrokenProcessPool

import numpy as np
import pytest
from nats.js.api import ConsumerConfig

from core.queue.helper import Event
from shared.data_loading import SimulationRawData
from simulation.inputs import (
    SimulationConsumerInput,
    SimulationIterationInput,
    SimulationKeyInput,
)
from simulation.result import KeySimResult
from worker import dispatcher
from worker import main as worker_main

# ---------------------------------------------------------------------------
# Fakes / helpers
# ---------------------------------------------------------------------------


def _key() -> SimulationKeyInput:
    return SimulationKeyInput(
        name="k",
        description="d",
        iterations=[
            SimulationIterationInput(
                number=1,
                energy_allocated_percentage=-1.0,
                consumers=[SimulationConsumerInput(name="A", energy_allocated_percentage=-1.0)],
            )
        ],
    )


def _raw() -> SimulationRawData:
    return SimulationRawData(C=np.zeros((1, 3)), VA=np.ones((1, 3)), consumer_names=["A"])


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


class FakeMsg:
    def __init__(self, data: bytes) -> None:
        self.data = data
        self.acked = False
        self.naked = False

    async def ack(self) -> None:
        self.acked = True

    async def nak(self, delay: int | None = None) -> None:
        self.naked = True


def _event_bytes(simulation_id: int = 1) -> bytes:
    return Event(type="simulation.requested", data={"simulation_id": simulation_id}).encode()


# ---------------------------------------------------------------------------
# CPU detection / pool sizing
# ---------------------------------------------------------------------------


def test_detect_cpus_reads_cgroup_v2_quota(monkeypatch):
    """cgroup v2 "quota period" (microseconds) → quota/period cores."""

    def fake_read_text(self, *args, **kwargs):
        if self.as_posix() == "/sys/fs/cgroup/cpu.max":
            return "200000 100000"
        raise FileNotFoundError(self)

    monkeypatch.setattr(worker_main.pathlib.Path, "read_text", fake_read_text)
    assert worker_main._detect_available_cpus() == 2


def test_detect_cpus_cgroup_v2_unlimited_falls_through(monkeypatch):
    """ "max" quota means unlimited → fall back to a sane positive count."""

    def fake_read_text(self, *args, **kwargs):
        if self.as_posix() == "/sys/fs/cgroup/cpu.max":
            return "max 100000"
        raise FileNotFoundError(self)

    monkeypatch.setattr(worker_main.pathlib.Path, "read_text", fake_read_text)
    assert worker_main._detect_available_cpus() >= 1


def test_detect_cpus_reads_cgroup_v1_quota(monkeypatch):
    """cgroup v1 splits quota / period across two files."""

    def fake_read_text(self, *args, **kwargs):
        posix = self.as_posix()
        if posix == "/sys/fs/cgroup/cpu/cpu.cfs_quota_us":
            return "300000"
        if posix == "/sys/fs/cgroup/cpu/cpu.cfs_period_us":
            return "100000"
        raise FileNotFoundError(self)

    monkeypatch.setattr(worker_main.pathlib.Path, "read_text", fake_read_text)
    assert worker_main._detect_available_cpus() == 3


def test_solver_pool_size_reserves_one_core(monkeypatch):
    monkeypatch.setattr(worker_main, "_detect_available_cpus", lambda: 4)
    assert worker_main._solver_pool_size() == 3


def test_solver_pool_size_floor_is_one(monkeypatch):
    monkeypatch.setattr(worker_main, "_detect_available_cpus", lambda: 1)
    assert worker_main._solver_pool_size() == 1


# ---------------------------------------------------------------------------
# _SolverPool — rebuilds itself when a worker dies abnormally
# ---------------------------------------------------------------------------


def test_solver_pool_rebuilds_after_broken_worker(monkeypatch):
    """A poisoned pool (BrokenProcessPool on submit) is rebuilt transparently.

    The underlying ProcessPoolExecutor is faked so no real processes spawn.
    """
    created: list = []

    class _FakePool:
        def __init__(self, *args, **kwargs):
            self.broken = False
            created.append(self)

        def submit(self, fn, *args, **kwargs):
            if self.broken:
                raise BrokenProcessPool("worker died")
            return ("future", self)

        def shutdown(self, *args, **kwargs):
            pass

    monkeypatch.setattr(worker_main, "ProcessPoolExecutor", _FakePool)

    pool = worker_main._SolverPool(2, lambda: None)
    assert len(created) == 1

    # Healthy submit goes to the original pool.
    assert pool.submit(len, [1, 2])[1] is created[0]

    # Poison the live pool: the next submit must rebuild and use a fresh one.
    created[0].broken = True
    result = pool.submit(len, [1, 2, 3])
    assert len(created) == 2
    assert result[1] is created[1]


# ---------------------------------------------------------------------------
# Payload picklability — required to cross the process-pool boundary
# ---------------------------------------------------------------------------


def test_solve_payloads_survive_pickling():
    key = _key()
    raw = _raw()
    res = _result()

    # Trusted, self-produced data — round-trip just proves picklability.
    assert pickle.loads(pickle.dumps(key)).name == "k"  # noqa: S301
    raw_rt = pickle.loads(pickle.dumps(raw))  # noqa: S301
    assert raw_rt.C.shape == (1, 3)
    assert raw_rt.consumer_names == ["A"]
    assert isinstance(pickle.loads(pickle.dumps(res)), KeySimResult)  # noqa: S301


# ---------------------------------------------------------------------------
# _run_simulation — inline vs executor branch
# ---------------------------------------------------------------------------


async def test_run_simulation_inline_when_no_executor(monkeypatch):
    monkeypatch.setattr(dispatcher, "run_simulation", lambda key, c, va, names: _result())
    result = await dispatcher._run_simulation(_key(), _raw(), executor=None, semaphore=None)
    assert isinstance(result, KeySimResult)


async def test_run_simulation_uses_executor_and_semaphore(monkeypatch):
    """Executor branch runs the solve off-loop and releases the semaphore."""
    monkeypatch.setattr(dispatcher, "run_simulation", lambda key, c, va, names: _result())
    semaphore = asyncio.Semaphore(1)
    with ThreadPoolExecutor(max_workers=1) as ex:
        result = await dispatcher._run_simulation(_key(), _raw(), executor=ex, semaphore=semaphore)
    assert isinstance(result, KeySimResult)
    # Semaphore released after the solve so the pool slot is reusable.
    assert semaphore.locked() is False


async def test_run_simulation_broken_pool_is_transient():
    """A BrokenProcessPool from the executor is mapped to a transient failure
    (so the message is redelivered rather than wrongly marked FAILED)."""

    class _BrokenExecutor(Executor):
        def submit(self, fn, /, *args, **kwargs):
            raise BrokenProcessPool("worker died")

    with pytest.raises(dispatcher._TransientError):
        await dispatcher._run_simulation(_key(), _raw(), executor=_BrokenExecutor(), semaphore=None)


def test_run_simulation_subprocess_calls_run_simulation(monkeypatch):
    """The picklable entry point forwards to run_simulation with the raw arrays."""
    captured: dict = {}

    def fake_run(key, c, va, names):
        captured["names"] = names
        return _result()

    monkeypatch.setattr(dispatcher, "run_simulation", fake_run)
    result = dispatcher._run_simulation_subprocess(_key(), _raw())
    assert isinstance(result, KeySimResult)
    assert captured["names"] == ["A"]


# ---------------------------------------------------------------------------
# _make_handler — spawning vs inline
# ---------------------------------------------------------------------------


async def test_handler_spawns_tracked_task_when_inflight_provided(monkeypatch):
    """With an inflight set, the callback returns immediately and the work
    runs as a tracked task that completes (and de-registers) afterwards."""
    started = asyncio.Event()

    async def _fake_handle(msg, *, executor=None, semaphore=None):
        started.set()
        await msg.ack()

    monkeypatch.setattr(dispatcher, "_handle_message", _fake_handle)

    inflight: set[asyncio.Task] = set()
    handler = dispatcher._make_handler(inflight=inflight)
    msg = FakeMsg(_event_bytes())

    await handler(msg)

    # Returned without waiting for the work: exactly one tracked task, and
    # _fake_handle hasn't necessarily run yet.
    assert len(inflight) == 1
    assert msg.acked is False

    await asyncio.gather(*list(inflight))
    await asyncio.sleep(0)  # let the done-callback run

    assert started.is_set()
    assert msg.acked is True
    assert len(inflight) == 0  # done-callback discarded the finished task


async def test_handler_runs_inline_when_no_inflight(monkeypatch):
    """Default (inflight=None) path processes to completion before returning —
    the shape the existing failure-path tests rely on."""
    calls: list[bool] = []

    async def _fake_handle(msg, *, executor=None, semaphore=None):
        await msg.ack()
        calls.append(True)

    monkeypatch.setattr(dispatcher, "_handle_message", _fake_handle)

    handler = dispatcher._make_handler()
    msg = FakeMsg(_event_bytes())

    await handler(msg)

    assert calls == [True]
    assert msg.acked is True


# ---------------------------------------------------------------------------
# subscribe — consumer config wiring
# ---------------------------------------------------------------------------


async def test_subscribe_sets_ack_wait_and_max_ack_pending():
    captured: dict = {}

    class _FakeJS:
        async def subscribe(self, **kwargs):
            captured.update(kwargs)
            return object()

    await dispatcher.subscribe(_FakeJS(), max_ack_pending=3)

    config = captured["config"]
    assert config.ack_wait == dispatcher._ACK_WAIT_SECONDS
    assert config.max_ack_pending == 3


async def test_subscribe_omits_max_ack_pending_when_unset():
    """Backwards-compatible default: no explicit max_ack_pending override."""
    captured: dict = {}

    class _FakeJS:
        async def subscribe(self, **kwargs):
            captured.update(kwargs)
            return object()

    await dispatcher.subscribe(_FakeJS())

    config = captured["config"]
    assert config.ack_wait == dispatcher._ACK_WAIT_SECONDS
    # Left at the ConsumerConfig default rather than forced to a value.
    default = ConsumerConfig(ack_wait=dispatcher._ACK_WAIT_SECONDS)
    assert config.max_ack_pending == default.max_ack_pending
