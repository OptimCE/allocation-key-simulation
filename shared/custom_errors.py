from core.errors.errors import Error


# ---------------------------------------------------------------------------
# Auth (no domain code — use xxx)
# ---------------------------------------------------------------------------
class _AuthErrors:
    UNAUTHORIZED = Error(code=1, key="ERRORS.AUTH.UNAUTHORIZED")
    FORBIDDEN = Error(code=2, key="ERRORS.AUTH.FORBIDDEN")
    RATE_LIMITED = Error(code=3, key="ERRORS.AUTH.RATE_LIMITED")
    AUTHORIZATION_MISSING = Error(code=4, key="ERRORS.AUTH.AUTHORIZATION_MISSING")


class _SubscriptionErrors:
    NOT_SUBSCRIBED = Error(code=1003, key="ERRORS.SUBSCRIPTION.NOT_SUBSCRIBED")


class _SimulationErrors:
    GET_SIMULATIONS = Error(code=2100, key="ERRORS.SIMULATION.GET_SIMULATIONS")
    GET_SIMULATION = Error(code=2101, key="ERRORS.SIMULATION.GET_SIMULATION")
    SIMULATION_NOT_FOUND = Error(code=2102, key="ERRORS.SIMULATION.SIMULATION_NOT_FOUND")
    # The CRM allocation key referenced by ``id_key`` does not exist or is not
    # owned by the caller's community.
    KEY_NOT_FOUND = Error(code=2103, key="ERRORS.SIMULATION.KEY_NOT_FOUND")
    INVALID_FILE = Error(code=2104, key="ERRORS.SIMULATION.INVALID_FILE")
    STORAGE_UPLOAD_FAILED = Error(code=2105, key="ERRORS.SIMULATION.STORAGE_UPLOAD_FAILED")
    START_SIMULATION = Error(code=2106, key="ERRORS.SIMULATION.START_SIMULATION")
    DELETE_SIMULATION = Error(code=2107, key="ERRORS.SIMULATION.DELETE_SIMULATION")
    # The per-timestep time-series result object is missing from storage.
    RESULT_NOT_FOUND = Error(code=2108, key="ERRORS.SIMULATION.RESULT_NOT_FOUND")
    GET_TIMESERIES = Error(code=2109, key="ERRORS.SIMULATION.GET_TIMESERIES")
    # The uploaded file exceeds UPLOAD_MAX_BODY_BYTES. Raised by the upload
    # handler's bounded read, which catches oversized bodies the request-limits
    # middleware can't pre-screen (chunked / no Content-Length). Maps to 413.
    FILE_TOO_LARGE = Error(code=2110, key="ERRORS.SIMULATION.FILE_TOO_LARGE")

    # --- CRM-sourced input (source = DataSource.CRM) -----------------------
    # Raised by the pre-flight, which runs on the preview endpoint, again on
    # POST /from-crm, and a third time in the worker against the data as it is
    # at execution time.
    INVALID_PERIOD = Error(code=2111, key="ERRORS.SIMULATION.INVALID_PERIOD")
    SHARING_OPERATION_NOT_FOUND = Error(
        code=2112, key="ERRORS.SIMULATION.SHARING_OPERATION_NOT_FOUND"
    )
    CRM_NO_DATA = Error(code=2113, key="ERRORS.SIMULATION.CRM_NO_DATA")
    # No unique constraint on meter_consumption(ean, timestamp): a repeated
    # import silently doubles a participant's energy, so this is fatal rather
    # than a warning.
    CRM_DUPLICATE_READINGS = Error(code=2114, key="ERRORS.SIMULATION.CRM_DUPLICATE_READINGS")
    CRM_NO_INJECTION = Error(code=2115, key="ERRORS.SIMULATION.CRM_NO_INJECTION")
    CRM_RANGE_TOO_LARGE = Error(code=2116, key="ERRORS.SIMULATION.CRM_RANGE_TOO_LARGE")
    # The simulated key names participants that no meter in the period matches.
    # allocation_key.consumer.name has no FK to meter.ean — the platform-wide
    # convention is that the name IS the EAN, compared after TRIM.
    KEY_CONSUMERS_NOT_MATCHED = Error(code=2117, key="ERRORS.SIMULATION.KEY_CONSUMERS_NOT_MATCHED")
    GET_CRM_PREVIEW = Error(code=2118, key="ERRORS.SIMULATION.GET_CRM_PREVIEW")


class _Errors:
    auth = _AuthErrors()
    subscription = _SubscriptionErrors()
    simulation = _SimulationErrors()


errors = _Errors()
