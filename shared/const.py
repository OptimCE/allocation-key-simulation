from enum import IntEnum, StrEnum


class SimulationStatus(IntEnum):
    PENDING = 0
    SUCCESS = 1
    FAILED = 2


class FeatureName(StrEnum):
    SIMULATION = "simulation"


# NATS JetStream stream + subject the API publishes to and the worker
# subscribes to for simulation runs (declared in core/queue/streams.json).
SIMULATION_STREAM = "SIMULATIONS"
SIMULATION_SUBJECT = "optimce.simulation.run"


class DataSource(IntEnum):
    """Where a simulation's input timeseries comes from.

    FILE is the historical path (a CSV/XLSX uploaded by the manager). CRM
    reads the same numbers straight out of the core database's
    ``meter_consumption`` for one sharing operation over a date range.
    """

    FILE = 1
    CRM = 2
