"""Worker process entry point.

Bootstraps logging + tracing, connects to NATS JetStream, and subscribes the
dispatcher to the single simulation consumer. Runs until SIGINT/SIGTERM, then
drains the subscription and disposes resources cleanly.

Run locally:

    python -m worker.main
"""

from __future__ import annotations

import asyncio
import contextlib
import importlib
import logging
import math
import os
import pathlib
import signal
import sys
from collections.abc import Callable
from concurrent.futures import Executor, ProcessPoolExecutor
from concurrent.futures.process import BrokenProcessPool

from core import metrics as app_metrics
from core.database.database import crm_engine, local_engine
from core.logging import configure_logging
from core.queue.init import close_nats, get_jetstream, init_nats
from core.realtime import log_realtime_state
from core.tracing import setup_tracer_provider
from shared.const import SIMULATION_STREAM
from worker import dispatcher

logger = logging.getLogger(__name__)

# Bounded retry on NATS connect so a slow-to-start broker doesn't crash the
# worker container immediately (~10 minutes of exponential backoff, then give
# up and let the orchestrator recreate it).
_NATS_CONNECT_MAX_ATTEMPTS = 10
_NATS_CONNECT_BASE_DELAY_SECONDS = 2
_NATS_CONNECT_MAX_DELAY_SECONDS = 30

_QUEUE_DEPTH_POLL_INTERVAL_SECONDS = 15

# Durable consumer name must match worker.dispatcher._DURABLE.
_CONSUMER_NAME = "worker-simulation"

# Touched by the queue-depth poller whenever a NATS consumer_info round-trip
# succeeds. Container HEALTHCHECK reads this file's mtime to tell a live worker
# from a hung one.
_HEARTBEAT_PATH = pathlib.Path("/tmp/worker.alive")  # noqa: S108 — dedicated container, non-root app user

# How long shutdown waits for in-flight solves to finish + ack before giving up
# on them (they redeliver after ack_wait — the persistence idempotency guard
# makes that safe). Kept comfortably under a typical orchestrator SIGTERM grace
# period so we don't get SIGKILL'd mid-drain.
_INFLIGHT_DRAIN_TIMEOUT_SECONDS = 25


def _detect_available_cpus() -> int:
    """Best-effort count of CPUs this process is actually allowed to use.

    ``os.cpu_count()`` reports host cores and ignores a container CPU quota
    (Docker ``cpus:`` / k8s limits), which would make us spawn a pool far
    larger than the quota and thrash. Prefer the cgroup quota, then CPU
    affinity, then the host count.
    """
    # cgroup v2: "<quota> <period>" in microseconds, or "max" for unlimited.
    try:
        parts = pathlib.Path("/sys/fs/cgroup/cpu.max").read_text().split()
        if parts and parts[0] != "max":
            quota = int(parts[0])
            period = int(parts[1]) if len(parts) > 1 else 100_000
            if quota > 0 and period > 0:
                return max(1, math.floor(quota / period))
    except (OSError, ValueError):
        pass
    # cgroup v1: separate quota / period files.
    try:
        quota = int(pathlib.Path("/sys/fs/cgroup/cpu/cpu.cfs_quota_us").read_text())
        period = int(pathlib.Path("/sys/fs/cgroup/cpu/cpu.cfs_period_us").read_text())
        if quota > 0 and period > 0:
            return max(1, math.floor(quota / period))
    except (OSError, ValueError):
        pass
    # Respect cpuset/taskset pinning if present (not available on Windows).
    if hasattr(os, "sched_getaffinity"):
        try:
            return max(1, len(os.sched_getaffinity(0)))
        except OSError:
            pass
    return max(1, os.cpu_count() or 1)


def _solver_pool_size() -> int:
    """Number of solver processes: all available CPUs bar one.

    Reserve ~1 core of scheduling headroom for the event loop / NATS client
    so the listener never starves while solves run.
    """
    return max(1, _detect_available_cpus() - 1)


def _init_solver_process() -> None:
    """Pool-worker initializer — runs once per solver process at spawn.

    Pays the heavy numpy import cost once per worker (rather than per solve) by
    importing the compute module here. On a fork start method the child already
    inherits it; on a spawn start method (dev on Windows/macOS) this makes the
    import explicit so the first solve isn't slowed by it.
    """
    importlib.import_module("simulation.compute")


class _SolverPool(Executor):
    """ProcessPoolExecutor that rebuilds itself if a worker dies abnormally.

    A native crash or OOM kill in one pool worker poisons the entire
    ProcessPoolExecutor: every subsequent ``submit`` raises
    ``BrokenProcessPool``. Left unhandled, the worker would keep running and
    look healthy while silently failing every solve. Rebuilding on the next
    submit restores capacity; the simulation that tripped the break is
    redelivered (``dispatcher._run_simulation`` maps the break to a transient
    failure). ``submit`` is only ever called from the event-loop thread (via
    ``loop.run_in_executor``), so the rebuild needs no extra locking.
    """

    def __init__(self, max_workers: int, initializer: Callable[[], None]) -> None:
        self._max_workers = max_workers
        self._initializer = initializer
        self._pool = self._new_pool()

    def _new_pool(self) -> ProcessPoolExecutor:
        return ProcessPoolExecutor(max_workers=self._max_workers, initializer=self._initializer)

    def submit(self, fn, /, *args, **kwargs):
        try:
            return self._pool.submit(fn, *args, **kwargs)
        except BrokenProcessPool:
            logger.error(
                "Solver pool broken (worker died); rebuilding with %d worker(s)",
                self._max_workers,
            )
            self._pool = self._new_pool()
            return self._pool.submit(fn, *args, **kwargs)

    def shutdown(self, wait: bool = True, *, cancel_futures: bool = False) -> None:
        self._pool.shutdown(wait=wait, cancel_futures=cancel_futures)


async def _connect_nats_with_retry() -> None:
    for attempt in range(1, _NATS_CONNECT_MAX_ATTEMPTS + 1):
        try:
            await init_nats()
            return
        except Exception as exc:
            if attempt == _NATS_CONNECT_MAX_ATTEMPTS:
                logger.error(
                    "NATS connect attempt %d/%d failed: %s; aborting worker startup",
                    attempt,
                    _NATS_CONNECT_MAX_ATTEMPTS,
                    exc,
                )
                raise
            delay = min(
                _NATS_CONNECT_BASE_DELAY_SECONDS * (2 ** (attempt - 1)),
                _NATS_CONNECT_MAX_DELAY_SECONDS,
            )
            logger.warning(
                "NATS connect attempt %d/%d failed: %s; retrying in %.1fs",
                attempt,
                _NATS_CONNECT_MAX_ATTEMPTS,
                exc,
                delay,
            )
            await asyncio.sleep(delay)


async def _poll_queue_depth(js, shutdown_event: asyncio.Event) -> None:
    """Refresh ``app_metrics.queue_depth_snapshot`` and the liveness heartbeat."""
    while not shutdown_event.is_set():
        any_success = False
        try:
            info = await js.consumer_info(SIMULATION_STREAM, _CONSUMER_NAME)
            app_metrics.queue_depth_snapshot["simulation"] = int(info.num_pending)
            any_success = True
        except Exception as exc:
            logger.debug("queue depth poll failed: %s", exc)
        if any_success:
            try:
                await asyncio.to_thread(_HEARTBEAT_PATH.touch)
            except OSError as exc:
                logger.debug("heartbeat touch failed: %s", exc)
        try:
            await asyncio.wait_for(
                shutdown_event.wait(), timeout=_QUEUE_DEPTH_POLL_INTERVAL_SECONDS
            )
        except TimeoutError:
            continue


async def main() -> None:
    configure_logging()
    # Absence of this line means the image predates the realtime feature —
    # see core/realtime/bus.py. Must come after configure_logging().
    log_realtime_state("simulation-key-worker")
    setup_tracer_provider()

    await _connect_nats_with_retry()
    js = get_jetstream()

    shutdown_event = asyncio.Event()
    _install_signal_handlers(shutdown_event)

    # Solver process pool: keeps the CPU-bound solve off the event loop so the
    # loop stays free for NATS PING/PONG + the heartbeat poller, and lets several
    # simulations solve on several cores at once. Sized from the CPUs the
    # container is actually allowed to use.
    pool_size = _solver_pool_size()
    solver_pool = _SolverPool(pool_size, _init_solver_process)
    solve_semaphore = asyncio.Semaphore(pool_size)
    inflight: set[asyncio.Task] = set()
    logger.info("Solver process pool ready: %d worker(s)", pool_size)

    sub = None
    queue_depth_task: asyncio.Task | None = None
    try:
        sub = await dispatcher.subscribe(
            js,
            executor=solver_pool,
            semaphore=solve_semaphore,
            inflight=inflight,
            max_ack_pending=pool_size,
        )
        queue_depth_task = asyncio.create_task(
            _poll_queue_depth(js, shutdown_event), name="queue-depth-poller"
        )

        logger.info("Worker ready — listening on the simulation queue")
        await shutdown_event.wait()
        logger.info("Shutdown signal received; draining subscription...")
    finally:
        # Stop the queue-depth poller before draining so it doesn't race against
        # a closing JetStream connection.
        if queue_depth_task is not None:
            queue_depth_task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await queue_depth_task

        # Let in-flight solves finish + ack while NATS is still up. The callback
        # returns as soon as it spawns the task, so sub.drain() no longer covers
        # this work — await it explicitly, bounded so a long solve can't stall
        # shutdown past the SIGTERM grace. Stragglers redeliver after ack_wait
        # (the persistence idempotency guard makes that safe).
        if inflight:
            logger.info("Waiting for %d in-flight simulation(s) to finish...", len(inflight))
            _done, pending = await asyncio.wait(
                set(inflight), timeout=_INFLIGHT_DRAIN_TIMEOUT_SECONDS
            )
            if pending:
                logger.warning(
                    "%d simulation(s) still running after %ds; cancelling — they will be "
                    "redelivered after ack_wait",
                    len(pending),
                    _INFLIGHT_DRAIN_TIMEOUT_SECONDS,
                )
                for task in pending:
                    task.cancel()

        if sub is not None:
            try:
                await sub.drain()
            except Exception:
                logger.exception("Error draining subscription")

        # Don't block shutdown on a still-running subprocess solve (its future
        # isn't cancellable once started); cancel_futures drops any queued work.
        # Workers are reaped on process exit (tini in prod).
        try:
            solver_pool.shutdown(wait=False, cancel_futures=True)
        except Exception:
            logger.exception("Error shutting down solver pool")

        try:
            await close_nats()
        except Exception:
            logger.exception("Error closing NATS connection")

        try:
            await local_engine.dispose()
        except Exception:
            logger.exception("Error disposing local DB engine")

        try:
            await crm_engine.dispose()
        except Exception:
            logger.exception("Error disposing CRM DB engine")

        logger.info("Worker shutdown complete")


def _install_signal_handlers(shutdown_event: asyncio.Event) -> None:
    """Wire SIGINT/SIGTERM to set ``shutdown_event`` (POSIX + Windows)."""
    loop = asyncio.get_running_loop()

    def _set_event() -> None:
        if not shutdown_event.is_set():
            shutdown_event.set()

    if sys.platform == "win32":
        signal.signal(signal.SIGINT, lambda *_: _set_event())
        signal.signal(signal.SIGTERM, lambda *_: _set_event())
        return

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _set_event)
        except NotImplementedError:
            signal.signal(sig, lambda *_: _set_event())


if __name__ == "__main__":
    asyncio.run(main())
