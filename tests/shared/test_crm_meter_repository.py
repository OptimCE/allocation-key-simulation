"""``CrmMeterRepository.fetch_rows`` against a real Postgres.

The worker's dispatcher tests replace the repository with a mock, so this is
where the query itself, and the way its rows are read, are pinned: ordering,
COALESCE, the local-midnight period bounds, tenant scope, and reading the
cursor in partitions.
"""

import datetime

from shared import crm_meter_repository
from shared.crm_meter_repository import CrmMeterRepository
from tests.factories.meter_factory import create_meter, create_readings, create_sharing_operation
from tests.factories.subscription_factory import create_community

# February is CET (UTC+1), as in tests/factories/meter_factory.py.
_CET = datetime.timezone(datetime.timedelta(hours=1))
_A = "541448000000000001"
_B = "541448000000000002"
_FEBRUARY = {"period_start": datetime.date(2025, 2, 1), "period_end": datetime.date(2025, 2, 28)}


async def _operation(db_session) -> tuple[int, int]:
    community = await create_community(db_session)
    operation = await create_sharing_operation(db_session, id_community=community.id)
    for ean in (_A, _B):
        await create_meter(db_session, ean=ean, id_community=community.id)
    return community.id, operation


async def _fetch(db_session, community: int, operation: int):
    return await CrmMeterRepository(db_session).fetch_rows(
        id_community=community, id_sharing_operation=operation, **_FEBRUARY
    )


async def test_rows_come_back_by_instant_then_meter_with_nulls_as_zero(db_session):
    community, operation = await _operation(db_session)
    # B first, so that the order is the query's rather than the insertion's.
    await create_readings(
        db_session, ean=_B, id_community=community, id_sharing_operation=operation, gross=2.0
    )
    await create_readings(
        db_session,
        ean=_A,
        id_community=community,
        id_sharing_operation=operation,
        gross=None,
        inj_gross=None,
    )

    rows = await _fetch(db_session, community, operation)

    start = datetime.datetime(2025, 2, 1, tzinfo=_CET)
    instants = [start + datetime.timedelta(minutes=15 * i) for i in range(4)]
    assert [(r.timestamp, r.ean) for r in rows] == [(t, e) for t in instants for e in (_A, _B)]
    assert {(r.ean, r.gross, r.inj_gross) for r in rows} == {(_A, 0.0, 0.0), (_B, 2.0, 0.0)}


async def test_period_runs_from_local_midnight_to_local_midnight(db_session):
    community, operation = await _operation(db_session)
    edges = [
        datetime.datetime(2025, 1, 31, 23, 45, tzinfo=_CET),  # before
        datetime.datetime(2025, 2, 1, 0, 0, tzinfo=_CET),  # first quarter
        datetime.datetime(2025, 2, 28, 23, 45, tzinfo=_CET),  # last quarter
        datetime.datetime(2025, 3, 1, 0, 0, tzinfo=_CET),  # after
    ]
    for instant in edges:
        await create_readings(
            db_session,
            ean=_A,
            id_community=community,
            id_sharing_operation=operation,
            start=instant,
            count=1,
        )

    rows = await _fetch(db_session, community, operation)

    assert [r.timestamp for r in rows] == edges[1:3]


async def test_other_operations_and_communities_are_not_read(db_session):
    community, operation = await _operation(db_session)
    other_operation = await create_sharing_operation(db_session, id_community=community)
    other_community, foreign_operation = await _operation(db_session)
    for owner, op in (
        (community, operation),
        (community, other_operation),
        (other_community, foreign_operation),
    ):
        await create_readings(db_session, ean=_A, id_community=owner, id_sharing_operation=op)

    rows = await _fetch(db_session, community, operation)

    assert len(rows) == 4
    assert await _fetch(db_session, community, foreign_operation) == []


async def test_rows_spread_over_several_partitions_all_arrive_in_order(db_session, monkeypatch):
    monkeypatch.setattr(crm_meter_repository, "_FETCH_PARTITION_ROWS", 3)
    community, operation = await _operation(db_session)
    for ean in (_A, _B):
        await create_readings(
            db_session, ean=ean, id_community=community, id_sharing_operation=operation, count=5
        )

    rows = await _fetch(db_session, community, operation)

    assert len(rows) == 10
    assert [(r.timestamp, r.ean) for r in rows] == sorted((r.timestamp, r.ean) for r in rows)


async def test_rows_share_one_ean_string_per_meter_and_one_timestamp_per_instant(db_session):
    # What keeps the read at the size crm_preflight.MAX_READING_ROWS allows: the
    # driver hands back a new string and datetime for every row.
    community, operation = await _operation(db_session)
    for ean in (_A, _B):
        await create_readings(
            db_session, ean=ean, id_community=community, id_sharing_operation=operation
        )

    rows = await _fetch(db_session, community, operation)

    assert len(rows) == 8
    assert len({id(r.ean) for r in rows}) == 2
    assert len({id(r.timestamp) for r in rows}) == 4
