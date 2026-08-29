"""Decide whether a CRM-sourced period can be simulated against a given key.

One definition of "blocking", used in three places: the preview endpoint (so the
manager sees the problem before launching), ``POST /from-crm`` (so a stale
preview cannot slip a bad run through), and the worker (which re-reads the data
at execution time and must reach the same verdict).

The simulation-specific rule lives here: **the key's participant names must be
meter EANs present in the period.** ``allocation_key.consumer.name`` is free
text with no foreign key to ``meter``; the convention that it holds the EAN is
enforced platform-wide only by comparison, and crm-backend compares it with
``TRIM(cons.name) IN (:eans)``. This module matches that exactly, so a key that
resolves in the CRM's own views also resolves here.

Framework-free and pandas-free on purpose — the API, which has neither pandas
nor a request-scoped community, imports this too.
"""

from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import Any

from core.errors.errors import Error
from shared.crm_meter_repository import CrmDataSummary
from shared.custom_errors import errors

# A year of quarter-hours across ~60 meters is ~2.1 M rows, which the worker
# pivots comfortably. Well past that we would rather refuse than risk the
# worker being OOM-killed mid-run, which reads to the manager as a silent hang.
MAX_READING_ROWS = 5_000_000


def normalize_participant(name: str) -> str:
    """Canonical form for comparing a key participant against a meter EAN.

    ``TRIM`` only — EANs are digits, so there is no case to fold. Mirrors
    ``crm-backend/src/modules/me/infra/me.repository.ts::getKeyConsumersForEans``.
    Note the file-upload path does *not* trim, so a key with a trailing space
    works here and fails there; this is the more forgiving of the two.
    """
    return name.strip()


@dataclass(frozen=True)
class Blocker:
    """A reason this period cannot be used, with the detail the manager needs."""

    error: Error
    detail: str


@dataclass(frozen=True)
class Preflight:
    summary: CrmDataSummary
    # The key's participants, trimmed, in key order. The pivot's column order
    # follows this so matrix rows line up with the key.
    participants: list[str]
    matched: list[str]
    unmatched: list[str]
    blockers: list[Blocker]
    warnings: dict[str, Any] | None = field(default=None)

    @property
    def ok(self) -> bool:
        return not self.blockers


def evaluate(
    summary: CrmDataSummary,
    key_consumer_names: Sequence[str],
) -> Preflight:
    """Classify a period's metering data against the key being simulated.

    Gaps are deliberately **not** blocking: missing quarters are zero-filled and
    reported. Duplicates are, because there is no unique constraint on
    ``meter_consumption(ean, timestamp)`` and a repeated import would inflate a
    participant's volume with no visible symptom.
    """
    participants = [normalize_participant(n) for n in key_consumer_names]

    if not summary.eans:
        # Nothing else can be said about an empty period; return early so the
        # manager gets one clear message rather than four derived ones.
        return Preflight(
            summary=summary,
            participants=participants,
            matched=[],
            unmatched=participants,
            blockers=[
                Blocker(
                    error=errors.simulation.CRM_NO_DATA,
                    detail="no readings for this sharing operation over this period",
                )
            ],
            warnings=None,
        )

    # Matching is against every meter with a reading, not just the consuming
    # ones: a key may legitimately include a participant that only injected.
    available = set(summary.all_eans)
    matched = [p for p in participants if p in available]
    unmatched = [p for p in participants if p not in available]

    blockers: list[Blocker] = []

    if summary.total_rows > MAX_READING_ROWS:
        blockers.append(
            Blocker(
                error=errors.simulation.CRM_RANGE_TOO_LARGE,
                detail=(
                    f"{summary.total_rows} readings exceed the {MAX_READING_ROWS} limit; "
                    "choose a shorter period"
                ),
            )
        )

    duplicates = summary.duplicate_eans
    if duplicates:
        blockers.append(
            Blocker(
                error=errors.simulation.CRM_DUPLICATE_READINGS,
                detail=(
                    "the same timestamp appears more than once for meter(s) "
                    f"{', '.join(duplicates)} — the data was most likely imported twice"
                ),
            )
        )

    if unmatched:
        blockers.append(
            Blocker(
                error=errors.simulation.KEY_CONSUMERS_NOT_MATCHED,
                detail=(
                    "the key names participant(s) with no matching meter in this period: "
                    f"{', '.join(unmatched)}"
                ),
            )
        )

    if summary.total_injection_kwh <= 0:
        blockers.append(
            Blocker(
                error=errors.simulation.CRM_NO_INJECTION,
                detail="no energy was injected over this period — there is nothing to share",
            )
        )

    # Only the participants actually being simulated are worth warning about;
    # a gap in a meter the key ignores changes nothing.
    participant_set = set(matched)
    incomplete = [e for e in summary.incomplete if e.ean in participant_set]
    warnings: dict[str, Any] | None = None
    if incomplete:
        warnings = {
            "incomplete_meters": [
                {
                    "ean": e.ean,
                    "readings": e.distinct_ts,
                    "expected": summary.grid_size,
                    "missing": summary.grid_size - e.distinct_ts,
                }
                for e in incomplete
            ]
        }

    return Preflight(
        summary=summary,
        participants=participants,
        matched=matched,
        unmatched=unmatched,
        blockers=blockers,
        warnings=warnings,
    )
