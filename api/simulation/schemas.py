import datetime

from pydantic import BaseModel, Field

from shared.const import SimulationStatus


class Simulation(BaseModel):
    """List-row view of a simulation run."""

    id: int = Field(..., description="Unique ID of the simulation.")
    name: str = Field(..., description="User-facing label for the simulation.")
    status: SimulationStatus = Field(..., description="Execution status.")
    id_key: int = Field(..., description="CRM allocation key that was simulated.")
    key_name: str = Field(..., description="Name of the simulated allocation key.")


class ConsumerResult(BaseModel):
    name: str
    energy_allocated_percentage: float
    consumption_total: float
    energy_allocated_total: float
    energy_allocated_consumed_total: float
    residual_volume_total: float
    surplus_total: float


class IterationResult(BaseModel):
    number: int
    energy_allocated_percentage: float
    consumption_total: float
    energy_allocated_total: float
    energy_allocated_consumed_total: float
    residual_volume_total: float
    surplus_total: float
    sharing_rate_total: float
    self_sufficiency_rate_total: float
    consumers: list[ConsumerResult]


class KeyResult(BaseModel):
    name: str
    description: str
    consumption_total: float
    energy_allocated_total: float
    energy_allocated_consumed_total: float
    residual_volume_total: float
    surplus_total: float
    self_sufficiency_rate_total: float
    sharing_rate_total: float
    iterations: list[IterationResult]


class SimulationDetail(Simulation):
    """Full view of a simulation: scalar result tree + time-series availability."""

    error_message: str | None = Field(default=None, description="Failure cause, if FAILED.")
    has_timeseries: bool = Field(
        default=False,
        description="Whether per-timestep series are available via GET /{id}/timeseries.",
    )
    key_result: KeyResult | None = Field(
        default=None, description="Scalar result tree (null until SUCCESS)."
    )


class ConsumerTimeseries(BaseModel):
    name: str
    consumption: list[float]
    energy_allocated: list[float]
    energy_allocated_consumed: list[float]
    residual_volume: list[float]
    surplus: list[float]


class IterationTimeseries(BaseModel):
    number: int
    consumption: list[float]
    energy_allocated: list[float]
    energy_allocated_consumed: list[float]
    residual_volume: list[float]
    surplus: list[float]
    sharing_rate: list[float]
    self_sufficiency_rate: list[float]
    consumers: list[ConsumerTimeseries]


class SimulationTimeseries(BaseModel):
    """Per-timestep series for charting, stored as one object per run in storage."""

    iterations: list[IterationTimeseries]


class SimulateRequest(BaseModel):
    """Internal carrier for the parsed start-simulation request.

    POST /simulation/ is multipart/form-data (the file is uploaded alongside
    the metadata), so this is not the FastAPI body model — the route binds each
    form field individually and assembles this object for the service layer.
    """

    name: str = Field(..., description="User-facing label for the simulation.")
    id_key: int = Field(..., description="CRM allocation key id to stress-test.")
    injection_name: str = Field(
        ..., description="Name of the injection (production) column in the source file."
    )


class SimulateResponse(BaseModel):
    id: int = Field(..., description="ID of the freshly created simulation row.")
    status: SimulationStatus = Field(..., description="Initial status (PENDING on success).")


# ---------------------------------------------------------------------------
# CRM-sourced simulation (source = DataSource.CRM)
# ---------------------------------------------------------------------------


class SimulateFromCrmRequest(BaseModel):
    """Body of ``POST /from-crm``.

    Unlike ``SimulateRequest`` this *is* the FastAPI body model: with no file
    part there is nothing forcing multipart, so the request is plain JSON.

    ``injection_name`` is deliberately absent — the production profile is summed
    from the meters themselves, which is the whole reason this path is simpler
    for the user than uploading a file.
    """

    name: str = Field(..., min_length=1, description="User-facing label for the simulation.")
    id_key: int = Field(..., description="CRM allocation key id to stress-test.")
    id_sharing_operation: int = Field(..., description="CRM sharing operation to read meters from.")
    period_start: datetime.date = Field(..., description="First day of the period (inclusive).")
    period_end: datetime.date = Field(..., description="Last day of the period (inclusive).")


class IncompleteMeter(BaseModel):
    """A participant's meter missing part of the period. Zero-filled, not fatal."""

    ean: str
    readings: int = Field(..., description="Distinct timestamps this meter actually has.")
    expected: int = Field(..., description="Distinct timestamps across the whole operation.")
    missing: int = Field(..., description="expected - readings.")


class PreviewBlocker(BaseModel):
    """A reason the period cannot be used, already localised."""

    error_code: int = Field(..., description="Matches the error_code of the eventual 4xx.")
    message: str = Field(..., description="Localised, manager-facing explanation.")
    detail: str = Field(..., description="Which meters/participants triggered it.")


class CrmDataPreview(BaseModel):
    """What ``GET /crm-data-preview`` shows before the manager commits to a run.

    ``matched`` / ``unmatched`` are the heart of it: the key's participant names
    must be meter EANs, and this is where the manager finds out that they are
    not, rather than after a failed run.
    """

    can_simulate: bool
    matched_participants: list[str]
    unmatched_participants: list[str]
    meter_count: int = Field(..., description="Meters with readings in the period.")
    reading_count: int
    first_timestamp: datetime.datetime | None
    last_timestamp: datetime.datetime | None
    total_consumption_kwh: float
    total_injection_kwh: float
    incomplete_meters: list[IncompleteMeter]
    blockers: list[PreviewBlocker]
