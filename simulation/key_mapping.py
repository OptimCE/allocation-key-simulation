"""Map a CRM allocation key into the simulation input model.

Numpy-free and duck-typed on the CRM ORM models (``AllocationKeyModel`` ->
``IterationModel`` -> ``ConsumerModel``) so it can live in the worker without
pulling SQLAlchemy typing into the compute core.
"""

from __future__ import annotations

from simulation.inputs import (
    SimulationConsumerInput,
    SimulationIterationInput,
    SimulationKeyInput,
)


def from_crm_allocation_key(key) -> SimulationKeyInput:
    """Build a ``SimulationKeyInput`` from a CRM allocation key tree.

    Iterations are ordered by ``number`` so the cascade (iteration N feeding
    N+1) runs in the intended order regardless of CRM row order.
    """
    iterations = sorted(key.iterations, key=lambda it: it.number)
    return SimulationKeyInput(
        name=key.name,
        description=key.description,
        iterations=[
            SimulationIterationInput(
                number=it.number,
                energy_allocated_percentage=it.energy_allocated_percentage,
                consumers=[
                    SimulationConsumerInput(
                        name=c.name,
                        energy_allocated_percentage=c.energy_allocated_percentage,
                    )
                    for c in it.consumers
                ],
            )
            for it in iterations
        ],
    )


def consumer_names_of(key: SimulationKeyInput) -> list[str]:
    """Canonical consumer order, taken from the first iteration.

    The file's consumer columns are matched/ordered against this list, and the
    consumption matrix rows follow it.
    """
    if not key.iterations:
        return []
    return [c.name for c in key.iterations[0].consumers]
