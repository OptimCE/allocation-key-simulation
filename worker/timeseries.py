"""A run's per-timestep series, as the JSON object storage keeps for it.

``GET /{id}/timeseries`` reads the object back as ``SimulationTimeseries``. It
is by far the largest thing a run produces: five values per participant,
timestep and iteration, 70 to 80 bytes of JSON and 160 as the ``list[float]``
series of a ``KeySimResult``. So it never reaches the worker's main process:

* the solver process (``dispatcher._run_simulation_subprocess``) writes the
  JSON to a temporary file, one series at a time (``to_temp_file``), and hands
  back the result without its series (``without_series``), only its totals;
* ``persistence.save_success`` streams that file to storage, and the
  dispatcher deletes it.

Before this, the main process unpickled the whole result and serialised it with
one ``json.dumps``: over 5 GB for 142 meters x 35,040 quarter-hours x 3
iterations, with the event loop held for about half a minute.

The bytes are exactly those of ``json.dumps(payload).encode()`` on the payload
the worker used to build, so objects written before and after read back alike.
"""

from __future__ import annotations

import contextlib
import dataclasses
import glob
import json
import os
import tempfile
from typing import BinaryIO

from simulation.result import KeySimResult

# Each worker container runs one worker, so at startup every file with this
# prefix is a leftover of a process that died between solve and upload.
TEMP_FILE_PREFIX = "simulation-timeseries-"

# Member order of SimulationTimeseries (api/simulation/schemas.py): an
# iteration's number, these series, then its consumers.
_ITERATION_SERIES = (
    "consumption",
    "energy_allocated",
    "energy_allocated_consumed",
    "residual_volume",
    "surplus",
    "sharing_rate",
    "self_sufficiency_rate",
)
# A consumer's name, then these series.
_CONSUMER_SERIES = (
    "consumption",
    "energy_allocated",
    "energy_allocated_consumed",
    "residual_volume",
    "surplus",
)


def write_json(result: KeySimResult, out: BinaryIO) -> None:
    """Write ``result``'s series to ``out`` as one SimulationTimeseries JSON document.

    Each series goes through ``json.dumps`` on its own, so memory beyond the
    result itself stays at one series' text.
    """
    out.write(b'{"iterations": [')
    for index, iteration in enumerate(result.iterations):
        if index:
            out.write(b", ")
        out.write(b'{"number": ' + _encode(iteration.number))
        for name in _ITERATION_SERIES:
            _write_member(out, name, getattr(iteration, name))
        out.write(b', "consumers": [')
        for position, consumer in enumerate(iteration.consumers):
            if position:
                out.write(b", ")
            out.write(b'{"name": ' + _encode(consumer.name))
            for name in _CONSUMER_SERIES:
                _write_member(out, name, getattr(consumer, name))
            out.write(b"}")
        out.write(b"]}")
    out.write(b"]}")


def _write_member(out: BinaryIO, name: str, value: object) -> None:
    out.write(b", " + _encode(name) + b": " + _encode(value))


def _encode(value: object) -> bytes:
    # json.dumps's defaults, as the former single json.dumps call used them.
    return json.dumps(value).encode()


def to_temp_file(result: KeySimResult) -> str:
    """Write the series JSON to a new temporary file and return its path.

    The caller owns the file and deletes it with ``remove``. Nothing is left
    behind when writing fails.
    """
    fd, path = tempfile.mkstemp(prefix=TEMP_FILE_PREFIX, suffix=".json")
    try:
        with os.fdopen(fd, "wb") as out:
            write_json(result, out)
    except BaseException:
        remove(path)
        raise
    return path


def without_series(result: KeySimResult) -> KeySimResult:
    """``result`` with every per-timestep series emptied, the totals unchanged.

    What crosses back to the main process: ``persistence`` stores only the
    totals in the database.
    """
    return dataclasses.replace(
        result,
        iterations=[
            dataclasses.replace(
                iteration,
                consumption=[],
                energy_allocated=[],
                energy_allocated_consumed=[],
                residual_volume=[],
                surplus=[],
                sharing_rate=[],
                self_sufficiency_rate=[],
                consumers=[
                    dataclasses.replace(
                        consumer,
                        consumption=[],
                        energy_allocated=[],
                        energy_allocated_consumed=[],
                        residual_volume=[],
                        surplus=[],
                    )
                    for consumer in iteration.consumers
                ],
            )
            for iteration in result.iterations
        ],
    )


def remove(path: str) -> None:
    """Delete a file ``to_temp_file`` wrote; already gone is fine."""
    with contextlib.suppress(FileNotFoundError):
        os.remove(path)


def remove_leftovers() -> int:
    """Delete the temporary files a previous worker process left behind.

    A process killed between the solve and the upload leaves its file, and a
    container restart keeps the filesystem. Call only at startup, before any
    solve: it removes every file with ``TEMP_FILE_PREFIX``. Returns how many.
    """
    paths = glob.glob(os.path.join(tempfile.gettempdir(), f"{TEMP_FILE_PREFIX}*.json"))
    for path in paths:
        remove(path)
    return len(paths)
