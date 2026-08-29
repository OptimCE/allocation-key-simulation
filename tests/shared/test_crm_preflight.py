"""Unit tests for the CRM pre-flight verdict.

No database — ``evaluate`` is a pure function of a ``CrmDataSummary`` plus the
key's participant names. The rule this file exists to pin is the one the feature
was asked for: **a simulated key's participant names must match meter EANs.**
"""

import datetime

from shared.crm_meter_repository import CrmDataSummary, EanCoverage
from shared.crm_preflight import MAX_READING_ROWS, evaluate, normalize_participant
from shared.custom_errors import errors

_A = "541448000000000001"
_B = "541448000000000002"
_PV = "541448000000000003"


def _coverage(
    ean: str,
    *,
    row_count: int = 4,
    distinct_ts: int = 4,
    consumption_kwh: float = 10.0,
    injection_kwh: float = 0.0,
) -> EanCoverage:
    return EanCoverage(
        ean=ean,
        row_count=row_count,
        distinct_ts=distinct_ts,
        consumption_kwh=consumption_kwh,
        injection_kwh=injection_kwh,
    )


def _summary(eans: list[EanCoverage], *, grid_size: int = 4) -> CrmDataSummary:
    return CrmDataSummary(
        eans=eans,
        grid_size=grid_size,
        first_timestamp=datetime.datetime(2025, 2, 1, tzinfo=datetime.UTC),
        last_timestamp=datetime.datetime(2025, 2, 28, tzinfo=datetime.UTC),
    )


def _codes(preflight) -> set[int]:
    return {b.error.code for b in preflight.blockers}


_DEFAULT = [_coverage(_A), _coverage(_PV, consumption_kwh=0.0, injection_kwh=50.0)]


# ---------------------------------------------------------------------------
# 1. Participant matching — the requested behaviour
# ---------------------------------------------------------------------------


def test_key_participants_matching_eans_are_accepted():
    result = evaluate(_summary(_DEFAULT), [_A])
    assert result.ok
    assert result.matched == [_A]
    assert result.unmatched == []


def test_participant_with_no_matching_meter_blocks():
    result = evaluate(_summary(_DEFAULT), [_A, "NOT-AN-EAN"])
    assert not result.ok
    assert errors.simulation.KEY_CONSUMERS_NOT_MATCHED.code in _codes(result)
    assert result.unmatched == ["NOT-AN-EAN"]
    # The manager needs to know *which* participant, not just that one failed.
    blocker = next(
        b for b in result.blockers if b.error is errors.simulation.KEY_CONSUMERS_NOT_MATCHED
    )
    assert "NOT-AN-EAN" in blocker.detail


def test_participant_names_are_trimmed_before_matching():
    # crm-backend matches with TRIM(cons.name); a key with a stray space
    # resolves in the CRM's own views and must resolve here too.
    result = evaluate(_summary(_DEFAULT), [f"  {_A} "])
    assert result.ok
    assert result.matched == [_A]
    assert result.participants == [_A]


def test_injection_only_meter_can_be_a_participant():
    # A key may legitimately include a producer that never draws; matching is
    # against every meter with a reading, not just the consuming ones.
    result = evaluate(_summary(_DEFAULT), [_A, _PV])
    assert result.ok
    assert result.matched == [_A, _PV]


def test_participant_order_follows_the_key_not_the_database():
    # Matrix rows are built in this order and must line up with the key.
    result = evaluate(
        _summary(
            [_coverage(_B), _coverage(_A), _coverage(_PV, consumption_kwh=0.0, injection_kwh=50.0)]
        ),
        [_B, _A],
    )
    assert result.participants == [_B, _A]


def test_normalize_participant_trims_only():
    # EANs are digits, so there is deliberately no case folding.
    assert normalize_participant("  541448  ") == "541448"


# ---------------------------------------------------------------------------
# 2. Other blocking findings
# ---------------------------------------------------------------------------


def test_empty_period_blocks_with_a_single_message():
    result = evaluate(_summary([], grid_size=0), [_A])
    assert _codes(result) == {errors.simulation.CRM_NO_DATA.code}
    assert result.unmatched == [_A]


def test_duplicate_readings_block():
    result = evaluate(
        _summary(
            [
                _coverage(_A, row_count=8, distinct_ts=4),
                _coverage(_PV, consumption_kwh=0.0, injection_kwh=50.0),
            ]
        ),
        [_A],
    )
    assert errors.simulation.CRM_DUPLICATE_READINGS.code in _codes(result)


def test_no_injection_blocks():
    result = evaluate(_summary([_coverage(_A)]), [_A])
    assert errors.simulation.CRM_NO_INJECTION.code in _codes(result)


def test_oversized_period_blocks():
    big = MAX_READING_ROWS + 1
    result = evaluate(
        _summary(
            [_coverage(_A, row_count=big, distinct_ts=big, injection_kwh=1.0)],
            grid_size=big,
        ),
        [_A],
    )
    assert errors.simulation.CRM_RANGE_TOO_LARGE.code in _codes(result)


# ---------------------------------------------------------------------------
# 3. Gaps warn but do not block
# ---------------------------------------------------------------------------


def test_gaps_warn_but_do_not_block():
    result = evaluate(
        _summary(
            [
                _coverage(_A, row_count=2, distinct_ts=2),
                _coverage(_PV, consumption_kwh=0.0, injection_kwh=50.0),
            ]
        ),
        [_A],
    )
    assert result.ok, "a gap must warn, not block"
    assert result.warnings == {
        "incomplete_meters": [{"ean": _A, "readings": 2, "expected": 4, "missing": 2}]
    }


def test_gaps_in_meters_the_key_ignores_are_not_reported():
    # A gap in a meter this key does not simulate changes nothing.
    result = evaluate(
        _summary(
            [
                _coverage(_A),
                _coverage(_B, row_count=1, distinct_ts=1),
                _coverage(_PV, consumption_kwh=0.0, injection_kwh=50.0),
            ]
        ),
        [_A],
    )
    assert result.ok
    assert result.warnings is None
