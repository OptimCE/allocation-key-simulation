"""Shared data loading for the simulation.

Converts an uploaded tabular file (CSV or XLSX) — one column per consumer plus
one "injection" column holding the shared production profile — into the
``(C, VA, consumer_names)`` triple consumed by ``simulation.compute``.

Consumer columns are matched to the allocation key BY NAME: the caller passes
the ordered ``consumer_names`` taken from the CRM key, and the file must contain
exactly those columns (extra columns are ignored). A missing consumer column or
a missing injection column is a deterministic failure.
"""

from __future__ import annotations

from dataclasses import dataclass
from io import BytesIO
from typing import Any

import numpy as np
import pandas as pd


class InvalidInjectionColumnError(ValueError):
    """Raised when the configured production column is absent from the file."""


class UnsupportedFileFormatError(ValueError):
    """Raised when the file extension is not one of csv / xlsx / xls."""


class ConsumerColumnsError(ValueError):
    """Raised when the file columns don't match the key's consumer names."""


class NonFiniteValueError(ValueError):
    """Raised when a consumer or injection cell is empty or not a finite number.

    pandas reads an empty cell as NaN (and "inf" as infinity). One of them made
    every total of the run NaN, a result the API could not send: JSON has no
    NaN.
    """


@dataclass(frozen=True)
class SimulationRawData:
    """Pre-parsed input data passed to ``simulation.compute.run_simulation``.

    ``C`` / ``VA`` are 2D numpy arrays of shape ``(n_consumers, T)``. Typed as
    ``Any`` to keep importers free of a hard numpy dependency at type level.
    """

    C: Any  # consumption matrix, shape (n_consumers, T)
    VA: Any  # production matrix, shape (n_consumers, T)
    consumer_names: list[str]


def parse_file(content: bytes, file_name: str) -> pd.DataFrame:
    """Parse raw file bytes into a pandas DataFrame.

    Supports CSV and Excel (xlsx / xls); the parser is chosen by extension.
    """
    extension = file_name.rsplit(".", 1)[-1].lower()
    if extension == "csv":
        return pd.read_csv(BytesIO(content))
    if extension in ("xlsx", "xls"):
        return pd.read_excel(BytesIO(content), engine="openpyxl")
    raise UnsupportedFileFormatError(f"Unsupported file extension: {extension!r}")


def to_simulation_raw_data(
    dataframe: pd.DataFrame,
    injection_name: str,
    consumer_names: list[str],
) -> SimulationRawData:
    """Convert a parsed DataFrame into ``SimulationRawData``.

    ``consumer_names`` is the ordered list of consumers from the CRM key; the
    consumption matrix rows follow that order so they align with the key.
    """
    if not consumer_names:
        raise ConsumerColumnsError("no consumer columns requested")

    # Coerce column labels to str so matching against the key's (string)
    # consumer names is reliable even when pandas infers numeric headers.
    dataframe = dataframe.rename(columns=str)

    if injection_name not in dataframe.columns:
        raise InvalidInjectionColumnError(f"Injection column {injection_name!r} not found in file")

    missing = [name for name in consumer_names if name not in dataframe.columns]
    if missing:
        raise ConsumerColumnsError(f"file is missing consumer column(s): {missing}")

    # Select consumer columns in the requested (key) order.
    consumption = dataframe[list(consumer_names)].to_numpy(dtype=np.float64).transpose()
    production_series = dataframe[injection_name].to_numpy(dtype=np.float64)

    _require_finite(consumption, list(consumer_names))
    _require_finite(production_series[np.newaxis, :], [injection_name])

    VA = np.tile(production_series, (len(consumer_names), 1))
    return SimulationRawData(
        C=consumption,
        VA=VA,
        consumer_names=[str(c) for c in consumer_names],
    )


def _require_finite(series: np.ndarray, columns: list[str]) -> None:
    """Refuse an empty or non-finite cell; ``series`` holds one row per column."""
    invalid = ~np.isfinite(series)
    if not invalid.any():
        return
    for column, column_invalid in zip(columns, invalid, strict=True):
        if column_invalid.any():
            raise NonFiniteValueError(
                f"column {column!r} has {int(column_invalid.sum())} empty or non-numeric"
                f" cell(s), the first in data row {int(np.argmax(column_invalid)) + 1}"
            )


def load(
    content: bytes,
    file_name: str,
    injection_name: str,
    consumer_names: list[str],
) -> SimulationRawData:
    """One-shot helper: parse bytes and convert to ``SimulationRawData``."""
    dataframe = parse_file(content, file_name)
    return to_simulation_raw_data(dataframe, injection_name, consumer_names)
