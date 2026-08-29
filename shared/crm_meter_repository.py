"""Read-only access to the CRM core's metering tables.

This is the single place coupled to the CRM ``meter_consumption`` layout, in
the same spirit as ``billing/ports/crm_core_sqlalchemy.py``. Every statement is
SELECT-only and runs on a CRM ``AsyncSession``.

Two deliberate constraints on this module:

* **No pandas.** The API imports it for the pre-flight/preview and
  ``requirements/api.txt`` carries no numpy/pandas. The pivot into a DataFrame
  lives in ``shared/crm_timeseries.py``, which only the worker imports.
* **No fastapi.** The worker imports it too, and ``Dockerfile.worker``
  installs no HTTP stack.

Community scope is passed **explicitly** on every call rather than read from a
ContextVar: the worker has no request context, and
``core.database.with_community.with_community_scope`` would silently degrade to
``WHERE false`` there.

Period boundaries are interpreted in Belgian local time, so a month aligns to
local midnights and is DST-safe. Windows are half-open:
``timestamp >= start AND timestamp < end_exclusive``.
"""

import datetime
from dataclasses import dataclass
from datetime import date
from zoneinfo import ZoneInfo

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

# Metering timestamps are absolute instants (timestamptz); a period expressed
# as local dates is bounded at Belgian local midnights. Mirrors
# crm-backend's CONSUMPTION_TIMEZONE.
_METERING_TZ = ZoneInfo("Europe/Brussels")


def period_bounds(
    period_start: date, period_end: date
) -> tuple[datetime.datetime, datetime.datetime]:
    """Half-open instant bounds ``[start, end_exclusive)`` for an inclusive date range."""
    start = datetime.datetime.combine(period_start, datetime.time.min, tzinfo=_METERING_TZ)
    end_exclusive = datetime.datetime.combine(
        period_end + datetime.timedelta(days=1), datetime.time.min, tzinfo=_METERING_TZ
    )
    return start, end_exclusive


@dataclass(frozen=True)
class EanCoverage:
    """Per-EAN aggregate over the requested period."""

    ean: str
    row_count: int
    distinct_ts: int
    consumption_kwh: float
    injection_kwh: float

    @property
    def has_duplicate_rows(self) -> bool:
        """True when the same (ean, timestamp) appears more than once.

        ``meter_consumption`` has no unique constraint on (ean, timestamp), so a
        workbook imported twice lands twice and silently doubles this meter's
        energy. Callers treat this as fatal, not as a warning.
        """
        return self.row_count != self.distinct_ts

    @property
    def is_consumer(self) -> bool:
        """A meter that actually drew energy over the period.

        A pure injection point would otherwise become an all-zero participant
        holding a 0% share of the generated key.
        """
        return self.consumption_kwh > 0


@dataclass(frozen=True)
class CrmDataSummary:
    """Everything the pre-flight and the preview screen need, in two queries."""

    eans: list[EanCoverage]
    # Distinct timestamps across the whole operation — the length of the grid
    # every meter is reindexed onto.
    grid_size: int
    first_timestamp: datetime.datetime | None
    last_timestamp: datetime.datetime | None

    @property
    def total_rows(self) -> int:
        return sum(e.row_count for e in self.eans)

    @property
    def total_consumption_kwh(self) -> float:
        return sum(e.consumption_kwh for e in self.eans)

    @property
    def total_injection_kwh(self) -> float:
        return sum(e.injection_kwh for e in self.eans)

    @property
    def all_eans(self) -> list[str]:
        """Every meter with a reading in the period, consumers and injectors alike."""
        return [e.ean for e in self.eans]

    @property
    def consumer_eans(self) -> list[str]:
        return [e.ean for e in self.eans if e.is_consumer]

    @property
    def duplicate_eans(self) -> list[str]:
        return [e.ean for e in self.eans if e.has_duplicate_rows]

    @property
    def incomplete(self) -> list[EanCoverage]:
        """Meters missing at least one timestamp of the common grid.

        These are allowed through (the gaps are zero-filled) but are reported to
        the manager both before the run and on the finished run.
        """
        return [e for e in self.eans if e.distinct_ts < self.grid_size]


@dataclass(frozen=True)
class ConsumptionRow:
    """One metering reading, as consumed by the pivot."""

    timestamp: datetime.datetime
    ean: str
    gross: float
    inj_gross: float


class CrmMeterRepository:
    """SELECT-only reader over the CRM metering tables."""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def sharing_operation_exists(
        self, *, id_community: int, id_sharing_operation: int
    ) -> bool:
        """Tenant gate: does this operation belong to the caller's community?

        Checked explicitly so a cross-tenant id yields a clean "not found"
        rather than an indistinguishable "no data in this period".
        """
        result = await self._session.execute(
            text(
                """
                SELECT EXISTS (
                    SELECT 1 FROM sharing_operation so
                    WHERE so.id = :op AND so.id_community = :cid
                ) AS present
                """
            ),
            {"cid": id_community, "op": id_sharing_operation},
        )
        return bool(result.scalar())

    async def summarize(
        self,
        *,
        id_community: int,
        id_sharing_operation: int,
        period_start: date,
        period_end: date,
    ) -> CrmDataSummary:
        """Aggregate the period without transferring the readings themselves.

        Deliberately cheap: ``RequestLimitsMiddleware.TIMEOUT_SECONDS`` caps
        every API request at 30 s, and this runs on the request path.
        """
        start, end_exclusive = period_bounds(period_start, period_end)
        params = {
            "cid": id_community,
            "op": id_sharing_operation,
            "start": start,
            "end_excl": end_exclusive,
        }

        per_ean = await self._session.execute(
            text(
                """
                SELECT mc.ean                        AS ean,
                       COUNT(*)                      AS row_count,
                       COUNT(DISTINCT mc.timestamp)  AS distinct_ts,
                       COALESCE(SUM(mc.gross), 0)    AS consumption_kwh,
                       COALESCE(SUM(mc.inj_gross), 0) AS injection_kwh
                FROM meter_consumption mc
                WHERE mc.id_community = :cid
                  AND mc.id_sharing_operation = :op
                  AND mc.timestamp >= :start AND mc.timestamp < :end_excl
                GROUP BY mc.ean
                ORDER BY mc.ean
                """
            ),
            params,
        )
        eans = [
            EanCoverage(
                ean=row["ean"],
                row_count=int(row["row_count"]),
                distinct_ts=int(row["distinct_ts"]),
                consumption_kwh=float(row["consumption_kwh"]),
                injection_kwh=float(row["injection_kwh"]),
            )
            for row in per_ean.mappings()
        ]

        grid = await self._session.execute(
            text(
                """
                SELECT COUNT(DISTINCT mc.timestamp) AS grid_size,
                       MIN(mc.timestamp)            AS first_ts,
                       MAX(mc.timestamp)            AS last_ts
                FROM meter_consumption mc
                WHERE mc.id_community = :cid
                  AND mc.id_sharing_operation = :op
                  AND mc.timestamp >= :start AND mc.timestamp < :end_excl
                """
            ),
            params,
        )
        grid_row = grid.mappings().one()

        return CrmDataSummary(
            eans=eans,
            grid_size=int(grid_row["grid_size"] or 0),
            first_timestamp=grid_row["first_ts"],
            last_timestamp=grid_row["last_ts"],
        )

    async def fetch_rows(
        self,
        *,
        id_community: int,
        id_sharing_operation: int,
        period_start: date,
        period_end: date,
    ) -> list[ConsumptionRow]:
        """The readings themselves, ordered for a stable pivot.

        Only ever called from the worker: a year of quarter-hours across a few
        dozen meters is well past what belongs on a 30-second request path.
        """
        start, end_exclusive = period_bounds(period_start, period_end)
        result = await self._session.execute(
            text(
                """
                SELECT mc.timestamp                 AS ts,
                       mc.ean                       AS ean,
                       COALESCE(mc.gross, 0)        AS gross,
                       COALESCE(mc.inj_gross, 0)    AS inj_gross
                FROM meter_consumption mc
                WHERE mc.id_community = :cid
                  AND mc.id_sharing_operation = :op
                  AND mc.timestamp >= :start AND mc.timestamp < :end_excl
                ORDER BY mc.timestamp, mc.ean
                """
            ),
            {
                "cid": id_community,
                "op": id_sharing_operation,
                "start": start,
                "end_excl": end_exclusive,
            },
        )
        return [
            ConsumptionRow(
                timestamp=row["ts"],
                ean=row["ean"],
                gross=float(row["gross"]),
                inj_gross=float(row["inj_gross"]),
            )
            for row in result.mappings()
        ]
