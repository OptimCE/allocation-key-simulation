"""Unit tests for the CRM rows -> wide DataFrame pivot.

No database: the pivot is a pure function over ``ConsumptionRow`` values, and
these tests pin the three behaviours the rest of the feature relies on — a
common timestamp grid, zero-filled gaps, and a single summed injection series.
"""

import datetime

import numpy as np
import pandas as pd
import pytest

from shared import crm_timeseries, data_loading
from shared.crm_meter_repository import ConsumptionRow

_TZ = datetime.UTC


def _ts(index: int) -> datetime.datetime:
    return datetime.datetime(2025, 2, 1, 0, 0, tzinfo=_TZ) + datetime.timedelta(minutes=15 * index)


def _rows(spec: dict[str, list[tuple[int, float, float]]]) -> list[ConsumptionRow]:
    """Build rows from {ean: [(timestamp_index, gross, inj_gross), ...]}."""
    return [
        ConsumptionRow(timestamp=_ts(i), ean=ean, gross=gross, inj_gross=inj)
        for ean, entries in spec.items()
        for (i, gross, inj) in entries
    ]


# ---------------------------------------------------------------------------
# 1. Shape and ordering
# ---------------------------------------------------------------------------


def test_columns_are_participants_plus_injection_in_requested_order():
    rows = _rows({"B": [(0, 2.0, 0.0)], "A": [(0, 1.0, 0.0)]})
    frame = crm_timeseries.build_dataframe(rows, ["A", "B"])
    assert list(frame.columns) == ["A", "B", crm_timeseries.INJECTION_COLUMN]


def test_rows_are_ordered_chronologically_regardless_of_input_order():
    # Row t of every column must be contemporaneous; ordering is the only
    # alignment guarantee the algorithms have.
    rows = [
        ConsumptionRow(timestamp=_ts(2), ean="A", gross=30.0, inj_gross=3.0),
        ConsumptionRow(timestamp=_ts(0), ean="A", gross=10.0, inj_gross=1.0),
        ConsumptionRow(timestamp=_ts(1), ean="A", gross=20.0, inj_gross=2.0),
    ]
    frame = crm_timeseries.build_dataframe(rows, ["A"])
    assert frame["A"].tolist() == [10.0, 20.0, 30.0]


# ---------------------------------------------------------------------------
# 2. Gaps — the "warn but allow" behaviour
# ---------------------------------------------------------------------------


def test_missing_timestamps_are_zero_filled_onto_the_common_grid():
    # A is present at 0,1,2; B only at 0 and 2. The grid is the union, so B
    # gets a 0.0 at index 1 rather than the frame collapsing to A's shape.
    rows = _rows(
        {
            "A": [(0, 1.0, 0.0), (1, 1.0, 0.0), (2, 1.0, 0.0)],
            "B": [(0, 5.0, 0.0), (2, 5.0, 0.0)],
        }
    )
    frame = crm_timeseries.build_dataframe(rows, ["A", "B"])
    assert len(frame) == 3
    assert frame["B"].tolist() == [5.0, 0.0, 5.0]


# ---------------------------------------------------------------------------
# 3. Injection
# ---------------------------------------------------------------------------


def test_injection_sums_every_meter_including_non_participants():
    # C injects but never consumes, so it is not a participant — its production
    # must still reach the shared profile.
    rows = _rows(
        {
            "A": [(0, 1.0, 2.0)],
            "C": [(0, 0.0, 10.0)],
        }
    )
    frame = crm_timeseries.build_dataframe(rows, ["A"])
    assert list(frame.columns) == ["A", crm_timeseries.INJECTION_COLUMN]
    assert frame[crm_timeseries.INJECTION_COLUMN].tolist() == [12.0]


def test_non_participant_meters_are_not_consumer_columns():
    rows = _rows({"A": [(0, 1.0, 0.0)], "C": [(0, 0.0, 9.0)]})
    frame = crm_timeseries.build_dataframe(rows, ["A"])
    assert "C" not in frame.columns


# ---------------------------------------------------------------------------
# 4. Refusals
# ---------------------------------------------------------------------------


def test_requested_participant_with_no_readings_is_refused():
    # Reindexing would otherwise invent an all-zero column, which for a
    # simulation means a key participant silently gets a 0 kWh profile.
    rows = _rows({"A": [(0, 1.0, 1.0)]})
    with pytest.raises(crm_timeseries.CrmPivotError, match="GHOST"):
        crm_timeseries.build_dataframe(rows, ["A", "GHOST"])


def test_duplicate_ean_timestamp_pairs_are_refused():
    rows = [
        ConsumptionRow(timestamp=_ts(0), ean="A", gross=1.0, inj_gross=1.0),
        ConsumptionRow(timestamp=_ts(0), ean="A", gross=1.0, inj_gross=1.0),
    ]
    with pytest.raises(crm_timeseries.CrmPivotError):
        crm_timeseries.build_dataframe(rows, ["A"])


def test_empty_inputs_are_refused():
    with pytest.raises(crm_timeseries.CrmPivotError):
        crm_timeseries.build_dataframe([], ["A"])
    with pytest.raises(crm_timeseries.CrmPivotError):
        crm_timeseries.build_dataframe(_rows({"A": [(0, 1.0, 1.0)]}), [])


def test_injection_column_name_cannot_be_a_participant():
    rows = _rows({"A": [(0, 1.0, 1.0)]})
    with pytest.raises(crm_timeseries.CrmPivotError, match="reserved"):
        crm_timeseries.build_dataframe(rows, [crm_timeseries.INJECTION_COLUMN])


# ---------------------------------------------------------------------------
# 5. The whole point: the frame drops into the existing file-path converter
# ---------------------------------------------------------------------------


def test_frame_feeds_the_existing_simulation_converter_unchanged():
    rows = _rows(
        {
            "541448000000000001": [(0, 10.0, 0.0), (1, 11.0, 0.0)],
            "541448000000000002": [(0, 5.0, 0.0), (1, 6.0, 0.0)],
            "541448000000000003": [(0, 0.0, 100.0), (1, 0.0, 200.0)],
        }
    )
    participants = ["541448000000000001", "541448000000000002"]

    frame = crm_timeseries.build_dataframe(rows, participants)
    raw = data_loading.to_simulation_raw_data(frame, crm_timeseries.INJECTION_COLUMN, participants)

    assert raw.consumer_names == participants
    assert raw.C.shape == (2, 2)
    np.testing.assert_array_equal(raw.C, np.array([[10.0, 11.0], [5.0, 6.0]]))
    # VA is the single production series broadcast across every consumer row.
    assert raw.VA.shape == (2, 2)
    np.testing.assert_array_equal(raw.VA, np.array([[100.0, 200.0], [100.0, 200.0]]))


def test_frame_is_shaped_like_a_parsed_upload():
    # The contract that lets the converter stay untouched: a plain RangeIndex
    # and float columns, exactly what pd.read_csv would produce.
    rows = _rows({"A": [(0, 1.0, 1.0), (1, 2.0, 2.0)]})
    frame = crm_timeseries.build_dataframe(rows, ["A"])
    assert isinstance(frame.index, pd.RangeIndex)
    assert all(pd.api.types.is_float_dtype(dtype) for dtype in frame.dtypes)
