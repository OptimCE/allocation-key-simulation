"""How large a simulation may be: participants x timesteps x iterations.

Call one participant at one timestep in one iteration a cell. The result holds
five values per cell (its per-timestep series), so the memory needed to
produce, store and serve it grows with the number of cells. One limit, checked
in three places so that a run which cannot fit is refused before it is queued
whenever possible:

* the CRM pre-flight (``shared.crm_preflight.evaluate``), which feeds the
  preview, ``POST /from-crm`` and the worker's re-read of the period, before the
  readings themselves are fetched;
* ``POST /`` (file upload), from a cheap count of the file's data rows
  (``api.simulation.upload_shape``);
* the worker, on the exact parsed matrix, before the solve, as the backstop for
  an upload whose rows could not be counted up front.

Framework-free and numpy-free: the API imports it.
"""

# Measured 2026-10-09 in the worker image (README, "Resource sizing"): at this
# limit the solver process peaks at 1.5 GB (1.2 GB when the cells are spread
# over three iterations) and the whole worker at 1.9 GB, page cache included,
# inside its 2.5 GB mem_limit. The same result costs the API about 460 bytes
# per cell, and about 2.4 s per million cells, each time GET /{id}/timeseries
# reads it back. It equals MAX_READING_ROWS: one iteration over every reading
# the CRM read allows, or a third of that over three. Raise it only together
# with the worker's mem_limit.
MAX_SIMULATION_CELLS = 5_000_000


def too_large(participants: int, timesteps: int, iterations: int) -> str | None:
    """Return the detail to report when the run exceeds the limit, else ``None``."""
    cells = participants * timesteps * iterations
    if cells <= MAX_SIMULATION_CELLS:
        return None
    return (
        f"{participants} participants x {timesteps} timesteps x {iterations} iterations "
        f"= {cells} cells exceed the {MAX_SIMULATION_CELLS} limit; shorten the period, "
        "or simulate a key with fewer participants or iterations"
    )
