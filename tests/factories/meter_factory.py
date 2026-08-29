"""Factories for the CRM metering tables.

These services map no ORM models for ``meter`` / ``meter_consumption`` — the
production code reads them with raw ``text()`` SQL through
``shared/crm_meter_repository.py`` — so the factories insert with raw SQL too.
That is deliberate: a test that went through an ORM model would stop exercising
the column names the real query actually depends on.

Like the other factories here they flush and never commit; ``conftest`` wraps
each test in a transaction that is rolled back.
"""

import datetime

from sqlalchemy import text

# Brussels local midnight, expressed as the UTC instant Postgres stores. February
# is CET (UTC+1), so 00:00 local is 23:00 UTC the previous day. Hard-coding the
# offset rather than importing ZoneInfo keeps the fixture obvious about which
# instant it means.
_CET = datetime.timezone(datetime.timedelta(hours=1))


async def create_sharing_operation(session, *, id_community: int, name: str = "Test op") -> int:
    result = await session.execute(
        text(
            """
            INSERT INTO sharing_operation (name, type, is_public, id_community)
            VALUES (:name, 1, FALSE, :cid)
            RETURNING id
            """
        ),
        {"name": name, "cid": id_community},
    )
    await session.flush()
    return int(result.scalar_one())


async def create_meter(session, *, ean: str, id_community: int) -> str:
    await session.execute(
        text(
            """
            INSERT INTO meter (ean, meter_number, id_community)
            VALUES (:ean, :num, :cid)
            ON CONFLICT (ean) DO NOTHING
            """
        ),
        {"ean": ean, "num": f"M-{ean}", "cid": id_community},
    )
    await session.flush()
    return ean


async def create_readings(
    session,
    *,
    ean: str,
    id_community: int,
    id_sharing_operation: int,
    start: datetime.datetime | None = None,
    count: int = 4,
    gross: float | None = 1.0,
    inj_gross: float | None = 0.0,
    step_minutes: int = 15,
    skip: set[int] | None = None,
) -> list[datetime.datetime]:
    """Insert ``count`` quarter-hourly readings, optionally skipping some.

    ``skip`` holds indices to omit, which is how a gap is produced: the meter is
    then missing from those timestamps of the operation-wide grid and the
    pre-flight reports it as incomplete.
    """
    start = start or datetime.datetime(2025, 2, 1, 0, 0, tzinfo=_CET)
    skip = skip or set()
    written: list[datetime.datetime] = []
    for i in range(count):
        if i in skip:
            continue
        ts = start + datetime.timedelta(minutes=step_minutes * i)
        await session.execute(
            text(
                """
                INSERT INTO meter_consumption
                    (ean, id_sharing_operation, timestamp, gross, inj_gross, id_community)
                VALUES (:ean, :op, :ts, :gross, :inj, :cid)
                """
            ),
            {
                "ean": ean,
                "op": id_sharing_operation,
                "ts": ts,
                "gross": gross,
                "inj": inj_gross,
                "cid": id_community,
            },
        )
        written.append(ts)
    await session.flush()
    return written
