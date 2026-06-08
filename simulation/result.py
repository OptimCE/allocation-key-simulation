"""Pure result of a simulation run.

Per-timestep series are plain ``list[float]``; totals/rates are ``float``.
No numpy / no DB types, so the API layer can import these for response
mapping. The worker persists the scalar totals to the local DB and writes the
per-timestep series to object storage.
"""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class ConsumerSimResult:
    name: str
    energy_allocated_percentage: float

    consumption: list[float]
    consumption_total: float
    energy_allocated: list[float]
    energy_allocated_total: float
    energy_allocated_consumed: list[float]
    energy_allocated_consumed_total: float
    residual_volume: list[float]
    residual_volume_total: float
    surplus: list[float]
    surplus_total: float


@dataclass
class IterationSimResult:
    number: int
    energy_allocated_percentage: float

    consumption: list[float]
    consumption_total: float
    energy_allocated: list[float]
    energy_allocated_total: float
    energy_allocated_consumed: list[float]
    energy_allocated_consumed_total: float
    residual_volume: list[float]
    residual_volume_total: float
    surplus: list[float]
    surplus_total: float
    sharing_rate: list[float]
    sharing_rate_total: float
    self_sufficiency_rate: list[float]
    self_sufficiency_rate_total: float

    consumers: list[ConsumerSimResult] = field(default_factory=list)


@dataclass
class KeySimResult:
    name: str
    description: str

    # Headline roll-ups across the whole simulation (see compute.run_simulation).
    consumption_total: float
    energy_allocated_total: float
    energy_allocated_consumed_total: float
    residual_volume_total: float
    surplus_total: float
    self_sufficiency_rate_total: float
    sharing_rate_total: float

    iterations: list[IterationSimResult] = field(default_factory=list)
