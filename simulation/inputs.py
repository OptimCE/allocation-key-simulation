"""The allocation key as consumed by the simulation.

Built by the API/worker mapper from the CRM ``allocation_key`` tree
(``AllocationKeyModel`` -> ``IterationModel`` -> ``ConsumerModel``). Kept
numpy-free so the API layer can import it without the worker's heavy deps.

A consumer's ``energy_allocated_percentage`` is either a FIXED fraction
(``>= 0``) or the sentinel ``-1`` for PRORATA (allocation proportional to the
consumer's share of consumption at each timestep).
"""

from __future__ import annotations

from pydantic import BaseModel


class SimulationConsumerInput(BaseModel):
    name: str
    energy_allocated_percentage: float


class SimulationIterationInput(BaseModel):
    number: int
    energy_allocated_percentage: float
    consumers: list[SimulationConsumerInput]


class SimulationKeyInput(BaseModel):
    name: str
    description: str
    iterations: list[SimulationIterationInput]
