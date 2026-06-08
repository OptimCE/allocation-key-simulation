"""Audit log action codes.

Action codes follow the ``domain.entity.verb`` convention used by
``crm-backend`` (e.g. ``crm.allocation_key.created``). They are stored as
``VARCHAR(128)`` and the ``AuditAction`` type stays open-ended so call sites
can introduce new codes without round-tripping this module.
"""

from typing import Final

AuditAction = str


class AuditActions:
    """Known action codes emitted by ``simulation-key``."""

    SIMULATION_CREATED: Final[AuditAction] = "simulation_key.simulation.created"
    SIMULATION_QUEUE_FAILED: Final[AuditAction] = "simulation_key.simulation.queue_failed"
    SIMULATION_SUCCEEDED: Final[AuditAction] = "simulation_key.simulation.succeeded"
    SIMULATION_FAILED: Final[AuditAction] = "simulation_key.simulation.failed"
    SIMULATION_DELETED: Final[AuditAction] = "simulation_key.simulation.deleted"
