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
