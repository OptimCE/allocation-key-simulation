"""Pivot CRM metering rows into the wide frame the algorithms already expect.

The whole point of this module is that it produces a DataFrame **shaped exactly
like a parsed upload** — one column per participant plus a single injection
column — so the existing converter in ``shared/data_loading`` consumes it
unchanged. Nothing downstream needs to know the data came from the database
rather than from a file.

Imports pandas, and is therefore **worker-only**: ``requirements/api.txt``
carries no pandas, exactly as for ``shared/data_loading.py``.
"""

from __future__ import annotations

from collections.abc import Sequence

import pandas as pd

from shared.crm_meter_repository import ConsumptionRow

# Column name standing in for the file-based ``injection_name``. EANs are digit
# strings, so a dunder label cannot collide with a participant column.
INJECTION_COLUMN = "__injection__"


class CrmPivotError(ValueError):
    """Raised when the fetched rows cannot form a rectangular timeseries."""


def build_dataframe(
    rows: Sequence[ConsumptionRow],
    consumer_eans: Sequence[str],
) -> pd.DataFrame:
    """Build ``[timestamp x consumer EAN] + __injection__`` from raw readings.

    ``consumer_eans`` selects (and orders) the participant columns — meters that
    actually drew energy over the period. Injection is summed over **every** EAN
    in ``rows``, including pure production sites that are not participants, and
    is taken from the same rows as the consumption so the two series can never
    disagree in length.

    Timestamps present for some meters but not others are zero-filled; the
    caller has already reported those gaps to the manager.
    """
    if not rows:
        raise CrmPivotError("no readings to pivot")
    if not consumer_eans:
        raise CrmPivotError("no consumer EANs requested")
    if INJECTION_COLUMN in consumer_eans:
        raise CrmPivotError(f"{INJECTION_COLUMN!r} is reserved and cannot be a participant")

    frame = pd.DataFrame(
        {
            "timestamp": [r.timestamp for r in rows],
            "ean": [r.ean for r in rows],
            "gross": [r.gross for r in rows],
            "inj_gross": [r.inj_gross for r in rows],
        }
    )

    # Duplicate (ean, timestamp) pairs are refused upstream (they would double a
    # participant's energy). Pivoting defensively rather than aggregating means a
    # regression surfaces as a loud failure instead of silently inflated volumes.
    try:
        consumption = frame.pivot(index="timestamp", columns="ean", values="gross")
    except ValueError as exc:
        raise CrmPivotError(f"duplicate (ean, timestamp) readings: {exc}") from exc

    # A requested participant with no reading at all in the period would be
    # silently reindexed into an all-zero column — for simulation that means a
    # key participant whose EAN does not exist quietly gets a 0 kWh profile
    # instead of an error. Refuse instead; the pre-flight normally catches this
    # first and reports it far more helpfully, so reaching here is a backstop.
    missing = [ean for ean in consumer_eans if ean not in consumption.columns]
    if missing:
        raise CrmPivotError(f"no readings for requested participant(s): {missing}")

    # Select the requested participants in order, dropping injection-only meters.
    consumption = consumption.reindex(columns=list(consumer_eans))
    # Remaining NaNs are per-timestamp gaps in an otherwise present meter.
    consumption = consumption.fillna(0.0).astype(float)

    # One shared production profile, summed across every meter at each instant.
    injection = frame.groupby("timestamp")["inj_gross"].sum()
    consumption[INJECTION_COLUMN] = injection.reindex(consumption.index).fillna(0.0).astype(float)

    # Chronological order is the only ordering guarantee the algorithms have:
    # row t of every column must be contemporaneous.
    return consumption.sort_index().reset_index(drop=True)
