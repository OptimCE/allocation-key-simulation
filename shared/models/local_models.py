import datetime
from typing import Any

from sqlalchemy import TIMESTAMP, Date, Float, ForeignKey, Integer, SmallInteger, String, Text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship

from core.database.database import LocalBase
from shared.const import DataSource, SimulationStatus


class SimulationModel(LocalBase):
    """One row per simulation request.

    Holds the source file reference, a snapshot of the CRM allocation key
    being stress-tested (``id_key`` + ``key_name``), the execution status,
    and — once successful — the object key of the per-timestep time-series
    result written to storage. The scalar result tree
    (``SimulationKeyResultModel`` and children) is linked by ``id_simulation``
    with ON DELETE CASCADE.
    """

    __tablename__ = "simulation"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)

    # --- Display + ownership ---
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    id_community: Mapped[int] = mapped_column(Integer, nullable=False)

    # --- Source data ---
    # Which of the two column groups below is populated. The DB-level
    # ck_simulation_source CHECK enforces the pairing; these columns cannot be
    # NOT NULL any more because a CRM-sourced run has no file.
    source: Mapped[DataSource] = mapped_column(
        SmallInteger, nullable=False, default=DataSource.FILE
    )

    # FILE only. file_storage_key is the object key inside STORAGE_BUCKET
    # (MinIO). The service uploads the user-supplied file at creation time; the
    # worker deletes it once the row reaches SUCCESS or FAILED.
    file_storage_key: Mapped[str | None] = mapped_column(String(512), nullable=True)
    file_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    # Name of the column inside the uploaded file that holds the shared
    # production profile (the "injection").
    injection_name: Mapped[str | None] = mapped_column(String(255), nullable=True)

    # CRM only. The sharing operation and the inclusive Brussels-local date
    # range read out of meter_consumption. Plain columns, never FKs -- the CRM
    # is a separate database.
    id_sharing_operation: Mapped[int | None] = mapped_column(Integer, nullable=True)
    period_start: Mapped[datetime.date | None] = mapped_column(Date, nullable=True)
    period_end: Mapped[datetime.date | None] = mapped_column(Date, nullable=True)

    # --- Simulated key snapshot ---
    # The CRM ``allocation_key`` being stress-tested. Snapshotted by id (no FK:
    # the key lives in a separate CRM database) and name (for display without a
    # cross-DB join). The full key tree is re-read from CRM by the worker.
    id_key: Mapped[int] = mapped_column(Integer, nullable=False)
    key_name: Mapped[str] = mapped_column(String(255), nullable=False)

    # --- Execution state ---
    status: Mapped[SimulationStatus] = mapped_column(
        Integer, nullable=False, default=SimulationStatus.PENDING
    )
    # Populated by the worker on failure so the UI can surface the cause
    # without needing to hit logs. Nullable because success rows have none.
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)

    # Object key (inside STORAGE_BUCKET) of the per-timestep time-series JSON
    # the worker writes on success. Null until SUCCESS. The API streams it back
    # for charting (GET /simulation/{id}/timeseries).
    result_storage_key: Mapped[str | None] = mapped_column(String(512), nullable=True)

    # Non-blocking findings from the CRM pre-flight -- currently the
    # participants whose meter had gaps in the period and were zero-filled.
    # Persisted rather than only shown before launch, so the manager can still
    # see it on the finished run.
    data_warnings: Mapped[dict[str, Any] | None] = mapped_column(JSONB, nullable=True)

    key_result: Mapped["SimulationKeyResultModel | None"] = relationship(
        "SimulationKeyResultModel",
        lazy="select",
        back_populates="simulation",
        cascade="all, delete-orphan",
        uselist=False,
    )

    created_at: Mapped[datetime.datetime] = mapped_column(
        TIMESTAMP(timezone=True),
        nullable=False,
        default=lambda: datetime.datetime.now(datetime.UTC),
    )
    updated_at: Mapped[datetime.datetime] = mapped_column(
        TIMESTAMP(timezone=True),
        nullable=False,
        default=lambda: datetime.datetime.now(datetime.UTC),
        onupdate=lambda: datetime.datetime.now(datetime.UTC),
    )


class SimulationKeyResultModel(LocalBase):
    """Key-level result of a simulation (1:1 with ``SimulationModel``).

    Carries the headline roll-up metrics across all iterations so list/summary
    views never need to load the iteration subtree.
    """

    __tablename__ = "simulation_key_result"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    description: Mapped[str] = mapped_column(String(255), nullable=False)

    # Key-level roll-ups (headline metrics). Energy totals are summed across
    # iterations; the two rates are recomputed from those summed totals.
    consumption_total: Mapped[float] = mapped_column(Float, nullable=False)
    energy_allocated_total: Mapped[float] = mapped_column(Float, nullable=False)
    energy_allocated_consumed_total: Mapped[float] = mapped_column(Float, nullable=False)
    residual_volume_total: Mapped[float] = mapped_column(Float, nullable=False)
    surplus_total: Mapped[float] = mapped_column(Float, nullable=False)
    self_sufficiency_rate_total: Mapped[float] = mapped_column(Float, nullable=False)
    sharing_rate_total: Mapped[float] = mapped_column(Float, nullable=False)

    id_simulation: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("simulation.id", ondelete="CASCADE"),
        nullable=False,
    )
    id_community: Mapped[int] = mapped_column(Integer, nullable=False)

    simulation: Mapped["SimulationModel"] = relationship(
        "SimulationModel",
        lazy="select",
        back_populates="key_result",
    )
    iterations: Mapped[list["SimulationIterationResultModel"]] = relationship(
        "SimulationIterationResultModel",
        lazy="select",
        back_populates="key_result",
        cascade="all, delete-orphan",
    )

    created_at: Mapped[datetime.datetime] = mapped_column(
        TIMESTAMP(timezone=True),
        nullable=False,
        default=lambda: datetime.datetime.now(datetime.UTC),
    )
    updated_at: Mapped[datetime.datetime] = mapped_column(
        TIMESTAMP(timezone=True),
        nullable=False,
        default=lambda: datetime.datetime.now(datetime.UTC),
        onupdate=lambda: datetime.datetime.now(datetime.UTC),
    )


class SimulationIterationResultModel(LocalBase):
    """Per-iteration aggregated metrics."""

    __tablename__ = "simulation_iteration_result"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    number: Mapped[int] = mapped_column(Integer, nullable=False)
    energy_allocated_percentage: Mapped[float] = mapped_column(Float, nullable=False)

    consumption_total: Mapped[float] = mapped_column(Float, nullable=False)
    energy_allocated_total: Mapped[float] = mapped_column(Float, nullable=False)
    energy_allocated_consumed_total: Mapped[float] = mapped_column(Float, nullable=False)
    residual_volume_total: Mapped[float] = mapped_column(Float, nullable=False)
    surplus_total: Mapped[float] = mapped_column(Float, nullable=False)
    sharing_rate_total: Mapped[float] = mapped_column(Float, nullable=False)
    self_sufficiency_rate_total: Mapped[float] = mapped_column(Float, nullable=False)

    id_key_result: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("simulation_key_result.id", ondelete="CASCADE"),
        nullable=False,
    )
    id_community: Mapped[int] = mapped_column(Integer, nullable=False)

    key_result: Mapped["SimulationKeyResultModel"] = relationship(
        "SimulationKeyResultModel",
        lazy="select",
        back_populates="iterations",
    )
    consumers: Mapped[list["SimulationConsumerResultModel"]] = relationship(
        "SimulationConsumerResultModel",
        lazy="select",
        back_populates="iteration",
        cascade="all, delete-orphan",
    )

    created_at: Mapped[datetime.datetime] = mapped_column(
        TIMESTAMP(timezone=True),
        nullable=False,
        default=lambda: datetime.datetime.now(datetime.UTC),
    )
    updated_at: Mapped[datetime.datetime] = mapped_column(
        TIMESTAMP(timezone=True),
        nullable=False,
        default=lambda: datetime.datetime.now(datetime.UTC),
        onupdate=lambda: datetime.datetime.now(datetime.UTC),
    )


class SimulationConsumerResultModel(LocalBase):
    """Per-consumer aggregated metrics within an iteration."""

    __tablename__ = "simulation_consumer_result"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    energy_allocated_percentage: Mapped[float] = mapped_column(Float, nullable=False)

    consumption_total: Mapped[float] = mapped_column(Float, nullable=False)
    energy_allocated_total: Mapped[float] = mapped_column(Float, nullable=False)
    energy_allocated_consumed_total: Mapped[float] = mapped_column(Float, nullable=False)
    residual_volume_total: Mapped[float] = mapped_column(Float, nullable=False)
    surplus_total: Mapped[float] = mapped_column(Float, nullable=False)

    id_iteration_result: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("simulation_iteration_result.id", ondelete="CASCADE"),
        nullable=False,
    )
    id_community: Mapped[int] = mapped_column(Integer, nullable=False)

    iteration: Mapped["SimulationIterationResultModel"] = relationship(
        "SimulationIterationResultModel",
        lazy="select",
        back_populates="consumers",
    )

    created_at: Mapped[datetime.datetime] = mapped_column(
        TIMESTAMP(timezone=True),
        nullable=False,
        default=lambda: datetime.datetime.now(datetime.UTC),
    )
    updated_at: Mapped[datetime.datetime] = mapped_column(
        TIMESTAMP(timezone=True),
        nullable=False,
        default=lambda: datetime.datetime.now(datetime.UTC),
        onupdate=lambda: datetime.datetime.now(datetime.UTC),
    )
