"""Application metrics for the simulation-key service.

All instruments are created at module import. The OTel API ships a
``_ProxyMeterProvider`` until ``metrics.set_meter_provider(...)`` runs
(inside ``core.tracing.setup_tracer_provider``), and instruments created
against the proxy rebind to the real provider when it is installed —
so import order between this module and tracing setup does not matter.

In LOCAL ``setup_tracer_provider`` returns early and the proxy stays a
no-op, meaning every ``.add(...)``/``.record(...)`` call below becomes
a cheap function dispatch with no side effects. Tests that want to
observe values use ``InMemoryMetricReader`` + a fresh ``MeterProvider``
and re-fetch instruments from that provider.

Naming follows OTel semantic conventions: dotted lowercase, ``.total``
suffix on monotonically increasing counters, units in seconds for
duration histograms. The backend translates these to
``simulations_created_total`` etc.
"""

from __future__ import annotations

from collections.abc import Iterable

from opentelemetry import metrics
from opentelemetry.metrics import CallbackOptions, Observation

_meter = metrics.get_meter("simulation-key")

simulations_created = _meter.create_counter(
    name="simulations.created.total",
    description="Simulations enqueued by the API after commit",
    unit="1",
)

simulations_completed = _meter.create_counter(
    name="simulations.completed.total",
    description="Simulations finalized by the worker, labelled by status",
    unit="1",
)

simulation_duration = _meter.create_histogram(
    name="simulation.duration.seconds",
    description="Wall-clock time spent inside the simulation computation",
    unit="s",
)

worker_messages = _meter.create_counter(
    name="worker.messages.total",
    description="NATS message handler outcomes, labelled by outcome",
    unit="1",
)

health_checks = _meter.create_counter(
    name="health.checks.total",
    description="Readiness probe component outcomes",
    unit="1",
)

# Latest queue depth per subject. Mutated by the worker's background poller
# (see worker.main._poll_queue_depth). The observable gauge below reads from
# this dict on every metric collection cycle.
queue_depth_snapshot: dict[str, int] = {}


def _queue_depth_callback(
    options: CallbackOptions,
) -> Iterable[Observation]:
    """Emit one observation per subject with its current pending count."""
    return [
        Observation(value, {"subject": subject})
        for subject, value in queue_depth_snapshot.items()
    ]


_meter.create_observable_gauge(
    name="nats.queue.depth",
    callbacks=[_queue_depth_callback],
    description="JetStream consumer num_pending for the simulation subject",
    unit="1",
)
