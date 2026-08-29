"""Worker tests for the CRM-sourced branch of ``_process``.

Same style as test_dispatcher.py: drive ``_load_from_crm`` directly with
patched collaborators, so no live NATS, Postgres or MinIO is needed.

Two things matter here beyond the allocation service's equivalent:

* the failure classification — a CRM **read** error is transient (NAK) while
  **rejected data** is deterministic (FAILED, ack);
* the trimmed-participant round trip — the matrix column labels and the key's
  own consumer names must be the same strings, or ``run_simulation``'s internal
  name -> percentage lookup raises on a key that matched perfectly.
"""

from __future__ import annotations

import datetime
from unittest.mock import AsyncMock, MagicMock

import pytest

from shared.const import DataSource
from shared.crm_meter_repository import ConsumptionRow, CrmDataSummary, EanCoverage
from simulation.inputs import (
    SimulationConsumerInput,
    SimulationIterationInput,
    SimulationKeyInput,
)
from simulation.key_mapping import consumer_names_of
from worker import dispatcher

_TZ = datetime.UTC
_EAN_A = "541448000000000001"
_EAN_PV = "541448000000000002"


def _snapshot(
    *,
    simulation_id: int = 1,
    id_sharing_operation: int | None = 7,
    period_start: datetime.date | None = datetime.date(2025, 2, 1),
    period_end: datetime.date | None = datetime.date(2025, 2, 28),
) -> dispatcher._SimulationSnapshot:
    return dispatcher._SimulationSnapshot(
        id=simulation_id,
        source=DataSource.CRM,
        file_storage_key=None,
        file_name=None,
        injection_name=None,
        id_sharing_operation=id_sharing_operation,
        period_start=period_start,
        period_end=period_end,
        id_key=10,
        id_community=42,
        status=0,
    )


def _coverage(ean: str, *, consumption: float, injection: float, dup: bool = False) -> EanCoverage:
    return EanCoverage(
        ean=ean,
        row_count=8 if dup else 4,
        distinct_ts=4,
        consumption_kwh=consumption,
        injection_kwh=injection,
    )


def _summary(eans: list[EanCoverage]) -> CrmDataSummary:
    return CrmDataSummary(
        eans=eans,
        grid_size=4,
        first_timestamp=datetime.datetime(2025, 2, 1, tzinfo=_TZ),
        last_timestamp=datetime.datetime(2025, 2, 1, 0, 45, tzinfo=_TZ),
    )


_HEALTHY = [
    _coverage(_EAN_A, consumption=40.0, injection=0.0),
    _coverage(_EAN_PV, consumption=0.0, injection=100.0),
]


def _rows() -> list[ConsumptionRow]:
    base = datetime.datetime(2025, 2, 1, tzinfo=_TZ)
    out: list[ConsumptionRow] = []
    for i in range(4):
        ts = base + datetime.timedelta(minutes=15 * i)
        out.append(ConsumptionRow(timestamp=ts, ean=_EAN_A, gross=10.0, inj_gross=0.0))
        out.append(ConsumptionRow(timestamp=ts, ean=_EAN_PV, gross=0.0, inj_gross=25.0))
    return out


def _key(*names: str) -> SimulationKeyInput:
    return SimulationKeyInput(
        name="K",
        description="d",
        iterations=[
            SimulationIterationInput(
                number=0,
                energy_allocated_percentage=1.0,
                consumers=[
                    SimulationConsumerInput(name=n, energy_allocated_percentage=0.5) for n in names
                ],
            )
        ],
    )


@pytest.fixture
def patched_save(monkeypatch):
    save_failure = AsyncMock()
    monkeypatch.setattr(dispatcher.persistence, "save_failure", save_failure)
    return save_failure


def _patch_crm(monkeypatch, *, summary, rows=None, raises: Exception | None = None):
    repository = MagicMock()
    if raises is not None:
        repository.summarize = AsyncMock(side_effect=raises)
    else:
        repository.summarize = AsyncMock(return_value=summary)
    repository.fetch_rows = AsyncMock(return_value=rows or [])

    session_cm = MagicMock()
    session_cm.__aenter__ = AsyncMock(return_value=MagicMock())
    session_cm.__aexit__ = AsyncMock(return_value=False)

    monkeypatch.setattr(dispatcher, "AsyncSessionCRMFactory", MagicMock(return_value=session_cm))
    monkeypatch.setattr(dispatcher, "CrmMeterRepository", MagicMock(return_value=repository))
    return repository


# ---------------------------------------------------------------------------
# 1. Happy path
# ---------------------------------------------------------------------------


async def test_returns_raw_data_aligned_to_the_key(monkeypatch, patched_save):
    repository = _patch_crm(monkeypatch, summary=_summary(_HEALTHY), rows=_rows())

    result = await dispatcher._load_from_crm(_snapshot(), [_EAN_A])

    assert not isinstance(result, dispatcher._Terminal)
    assert result.consumer_names == [_EAN_A]
    assert result.C.tolist() == [[10.0, 10.0, 10.0, 10.0]]
    # The PV site is not a participant but still supplies the shared profile.
    assert result.VA.tolist() == [[25.0, 25.0, 25.0, 25.0]]
    patched_save.assert_not_awaited()
    assert repository.summarize.await_args.kwargs["id_community"] == 42


async def test_participant_order_follows_the_key(monkeypatch, patched_save):
    _patch_crm(
        monkeypatch,
        summary=_summary(
            [
                _coverage(_EAN_A, consumption=40.0, injection=0.0),
                _coverage(_EAN_PV, consumption=40.0, injection=100.0),
            ]
        ),
        rows=_rows(),
    )

    result = await dispatcher._load_from_crm(_snapshot(), [_EAN_PV, _EAN_A])

    # Rows must line up with the key's order, not the database's.
    assert result.consumer_names == [_EAN_PV, _EAN_A]
    assert result.C[0].tolist() == [0.0, 0.0, 0.0, 0.0]
    assert result.C[1].tolist() == [10.0, 10.0, 10.0, 10.0]


# ---------------------------------------------------------------------------
# 2. Trimming — the key and the matrix must agree on the strings
# ---------------------------------------------------------------------------


def test_trimming_the_key_keeps_it_consistent_with_the_matrix_labels():
    trimmed = dispatcher._with_trimmed_participants(_key(f"  {_EAN_A} "))
    assert consumer_names_of(trimmed) == [_EAN_A]
    # run_simulation resolves percentages off these names, so they must be the
    # same strings the pivot used as column labels.
    assert trimmed.iterations[0].consumers[0].energy_allocated_percentage == 0.5


async def test_padded_participant_name_still_loads(monkeypatch, patched_save):
    _patch_crm(monkeypatch, summary=_summary(_HEALTHY), rows=_rows())
    trimmed = dispatcher._with_trimmed_participants(_key(f" {_EAN_A}  "))

    result = await dispatcher._load_from_crm(_snapshot(), consumer_names_of(trimmed))

    assert not isinstance(result, dispatcher._Terminal)
    assert result.consumer_names == [_EAN_A]


# ---------------------------------------------------------------------------
# 3. Transient: the CRM is unreachable
# ---------------------------------------------------------------------------


async def test_crm_read_failure_is_transient(monkeypatch, patched_save):
    _patch_crm(monkeypatch, summary=None, raises=OSError("connection reset"))

    with pytest.raises(dispatcher._TransientError, match="crm read"):
        await dispatcher._load_from_crm(_snapshot(), [_EAN_A])

    patched_save.assert_not_awaited()


# ---------------------------------------------------------------------------
# 4. Deterministic: the data itself is unusable
# ---------------------------------------------------------------------------


async def test_unmatched_participant_fails_deterministically(monkeypatch, patched_save):
    repository = _patch_crm(monkeypatch, summary=_summary(_HEALTHY))

    result = await dispatcher._load_from_crm(_snapshot(), ["C0"])

    assert isinstance(result, dispatcher._Terminal)
    assert result.storage_key is None
    detail = patched_save.await_args.args[1]
    assert "crm_data_rejected" in detail
    assert "C0" in detail
    repository.fetch_rows.assert_not_awaited()


async def test_empty_period_fails_deterministically(monkeypatch, patched_save):
    _patch_crm(monkeypatch, summary=_summary([]))

    result = await dispatcher._load_from_crm(_snapshot(), [_EAN_A])

    assert isinstance(result, dispatcher._Terminal)
    assert "crm_data_rejected" in patched_save.await_args.args[1]


async def test_duplicate_readings_fail_deterministically(monkeypatch, patched_save):
    _patch_crm(
        monkeypatch,
        summary=_summary(
            [
                _coverage(_EAN_A, consumption=40.0, injection=0.0, dup=True),
                _coverage(_EAN_PV, consumption=0.0, injection=100.0),
            ]
        ),
    )

    result = await dispatcher._load_from_crm(_snapshot(), [_EAN_A])

    assert isinstance(result, dispatcher._Terminal)
    assert "imported twice" in patched_save.await_args.args[1]


async def test_no_injection_fails_deterministically(monkeypatch, patched_save):
    _patch_crm(monkeypatch, summary=_summary([_coverage(_EAN_A, consumption=40.0, injection=0.0)]))

    result = await dispatcher._load_from_crm(_snapshot(), [_EAN_A])

    assert isinstance(result, dispatcher._Terminal)
    assert "nothing to share" in patched_save.await_args.args[1]


async def test_incomplete_crm_columns_fail_deterministically(monkeypatch, patched_save):
    _patch_crm(monkeypatch, summary=_summary([]))

    result = await dispatcher._load_from_crm(_snapshot(id_sharing_operation=None), [_EAN_A])

    assert isinstance(result, dispatcher._Terminal)
    patched_save.assert_awaited_once_with(1, "crm_source_incomplete")


# ---------------------------------------------------------------------------
# 5. Gaps still run
# ---------------------------------------------------------------------------


async def test_gaps_do_not_stop_the_run_and_are_zero_filled(monkeypatch, patched_save):
    rows = [r for r in _rows() if not (r.ean == _EAN_A and r.timestamp.minute >= 30)]
    _patch_crm(
        monkeypatch,
        summary=_summary(
            [
                EanCoverage(
                    _EAN_A, row_count=2, distinct_ts=2, consumption_kwh=20.0, injection_kwh=0.0
                ),
                _coverage(_EAN_PV, consumption=0.0, injection=100.0),
            ]
        ),
        rows=rows,
    )

    result = await dispatcher._load_from_crm(_snapshot(), [_EAN_A])

    assert not isinstance(result, dispatcher._Terminal)
    assert result.C.tolist() == [[10.0, 10.0, 0.0, 0.0]]
    patched_save.assert_not_awaited()
