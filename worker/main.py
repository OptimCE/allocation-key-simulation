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
import logging
import pathlib
import signal
import sys

from core import metrics as app_metrics
from core.database.database import crm_engine, local_engine
from core.logging import configure_logging
from core.queue.init import close_nats, get_jetstream, init_nats
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
    setup_tracer_provider()

    await _connect_nats_with_retry()
    js = get_jetstream()

    shutdown_event = asyncio.Event()
    _install_signal_handlers(shutdown_event)

    sub = None
    queue_depth_task: asyncio.Task | None = None
    try:
        sub = await dispatcher.subscribe(js)
        queue_depth_task = asyncio.create_task(
            _poll_queue_depth(js, shutdown_event), name="queue-depth-poller"
        )

        logger.info("Worker ready — listening on the simulation queue")
        await shutdown_event.wait()
        logger.info("Shutdown signal received; draining subscription...")
    finally:
        if queue_depth_task is not None:
            queue_depth_task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await queue_depth_task

        if sub is not None:
            try:
                await sub.drain()
            except Exception:
                logger.exception("Error draining subscription")

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
