-- ============================================================================
-- Local database schema (LOCAL_DATABASE_URL).
--
-- This file is the single source of truth for the LOCAL database only.
-- CRM tables (allocation_key, iteration, consumer) live in a separate DB
-- hosted elsewhere and are NOT declared here (the simulation reads keys from
-- the CRM DB by id; see shared/crm_repository.py).
--
-- Mirrors shared/models/local_models.py. When changing models, update
-- this file and add a migration under scripts/sql/migrations/.
-- ============================================================================

-- ---- Shared utilities ------------------------------------------------------
CREATE OR REPLACE FUNCTION set_updated_at()
RETURNS trigger AS $$
BEGIN
  NEW.updated_at := CURRENT_TIMESTAMP;
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TABLE IF NOT EXISTS schema_version (
    version      INTEGER     PRIMARY KEY,
    description  TEXT        NOT NULL,
    applied_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

INSERT into schema_version (version, description) VALUES(
       1, 'First version'
) ON CONFLICT DO NOTHING;

INSERT into schema_version (version, description) VALUES(
       2, 'Allow CRM-sourced simulations (source, sharing operation, period)'
) ON CONFLICT DO NOTHING;


-- ---- simulation ------------------------------------------------------------
-- One row per simulation request. Holds the source file reference, a snapshot
-- of the CRM allocation key being stress-tested (id_key + key_name), the
-- execution status, and — once successful — the object key of the per-timestep
-- time-series result. Result tree (simulation_key_result and children) is
-- linked by id_simulation with ON DELETE CASCADE.
CREATE TABLE IF NOT EXISTS simulation (
    id                 INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name               VARCHAR(255) NOT NULL,
    id_community       INTEGER NOT NULL,

    -- Source data: 1=FILE (uploaded CSV/XLSX), 2=CRM (meter_consumption).
    -- Exactly one of the two column groups below is populated; the
    -- ck_simulation_source CHECK is what enforces that, now that the file
    -- columns can no longer be NOT NULL.
    source             SMALLINT     NOT NULL DEFAULT 1,

    -- FILE only. file_storage_key is the object key inside STORAGE_BUCKET
    -- (MinIO). The service uploads on creation; the worker deletes on terminal
    -- outcomes. injection_name names the production column inside the file.
    file_storage_key   VARCHAR(512) NULL,
    file_name          VARCHAR(255) NULL,
    injection_name     VARCHAR(255) NULL,

    -- CRM only. The sharing operation and the inclusive local date range read
    -- from meter_consumption. No FK: the CRM lives in a separate database.
    id_sharing_operation INTEGER    NULL,
    period_start       DATE         NULL,
    period_end         DATE         NULL,

    -- Simulated key snapshot (the CRM allocation_key; no FK — separate DB)
    id_key             INTEGER      NOT NULL,
    key_name           VARCHAR(255) NOT NULL,

    -- Execution state: 0=PENDING, 1=SUCCESS, 2=FAILED
    status             INTEGER      NOT NULL DEFAULT 0,
    error_message      TEXT         NULL,

    -- Object key of the per-timestep time-series JSON written on success.
    result_storage_key VARCHAR(512) NULL,

    -- Non-blocking findings from the CRM pre-flight (participants whose meter
    -- has gaps, which are zero-filled). Persisted so the warning outlives the
    -- preview screen.
    data_warnings      JSONB        NULL,

    created_at         TIMESTAMPTZ  NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at         TIMESTAMPTZ  NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT ck_simulation_source CHECK (
        (source = 1
            AND file_storage_key IS NOT NULL
            AND file_name        IS NOT NULL
            AND injection_name   IS NOT NULL)
     OR (source = 2
            AND id_sharing_operation IS NOT NULL
            AND period_start         IS NOT NULL
            AND period_end           IS NOT NULL
            AND period_start <= period_end)
    )
);

CREATE INDEX IF NOT EXISTS idx_simulation_id_community ON simulation (id_community);
CREATE INDEX IF NOT EXISTS idx_simulation_status       ON simulation (status);
CREATE INDEX IF NOT EXISTS idx_simulation_id_key       ON simulation (id_key);

DROP TRIGGER IF EXISTS trg_simulation_set_updated_at ON simulation;
CREATE TRIGGER trg_simulation_set_updated_at
    BEFORE UPDATE ON simulation
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();


-- ---- simulation_key_result -------------------------------------------------
-- Key-level result (1:1 with simulation). Carries the headline roll-up metrics
-- across all iterations so list/summary views skip the iteration subtree.
CREATE TABLE IF NOT EXISTS simulation_key_result (
    id                              INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name                            VARCHAR(255) NOT NULL,
    description                     VARCHAR(255) NOT NULL,

    consumption_total               DOUBLE PRECISION NOT NULL,
    energy_allocated_total          DOUBLE PRECISION NOT NULL,
    energy_allocated_consumed_total DOUBLE PRECISION NOT NULL,
    residual_volume_total           DOUBLE PRECISION NOT NULL,
    surplus_total                   DOUBLE PRECISION NOT NULL,
    self_sufficiency_rate_total     DOUBLE PRECISION NOT NULL,
    sharing_rate_total              DOUBLE PRECISION NOT NULL,

    id_simulation                   INTEGER NOT NULL REFERENCES simulation (id) ON DELETE CASCADE,
    id_community                    INTEGER NOT NULL,

    created_at                      TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at                      TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_simulation_key_result_simulation
    ON simulation_key_result (id_simulation);
CREATE INDEX IF NOT EXISTS idx_simulation_key_result_community
    ON simulation_key_result (id_community);

DROP TRIGGER IF EXISTS trg_simulation_key_result_set_updated_at ON simulation_key_result;
CREATE TRIGGER trg_simulation_key_result_set_updated_at
    BEFORE UPDATE ON simulation_key_result
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();


-- ---- simulation_iteration_result -------------------------------------------
CREATE TABLE IF NOT EXISTS simulation_iteration_result (
    id                              INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    number                          INTEGER NOT NULL,
    energy_allocated_percentage     DOUBLE PRECISION NOT NULL,

    consumption_total               DOUBLE PRECISION NOT NULL,
    energy_allocated_total          DOUBLE PRECISION NOT NULL,
    energy_allocated_consumed_total DOUBLE PRECISION NOT NULL,
    residual_volume_total           DOUBLE PRECISION NOT NULL,
    surplus_total                   DOUBLE PRECISION NOT NULL,
    sharing_rate_total              DOUBLE PRECISION NOT NULL,
    self_sufficiency_rate_total     DOUBLE PRECISION NOT NULL,

    id_key_result                   INTEGER NOT NULL REFERENCES simulation_key_result (id) ON DELETE CASCADE,
    id_community                    INTEGER NOT NULL,

    created_at                      TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at                      TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_simulation_iteration_result_key_result
    ON simulation_iteration_result (id_key_result);
CREATE INDEX IF NOT EXISTS idx_simulation_iteration_result_community
    ON simulation_iteration_result (id_community);

DROP TRIGGER IF EXISTS trg_simulation_iteration_result_set_updated_at ON simulation_iteration_result;
CREATE TRIGGER trg_simulation_iteration_result_set_updated_at
    BEFORE UPDATE ON simulation_iteration_result
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();


-- ---- simulation_consumer_result --------------------------------------------
CREATE TABLE IF NOT EXISTS simulation_consumer_result (
    id                              INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name                            VARCHAR(255) NOT NULL,
    energy_allocated_percentage     DOUBLE PRECISION NOT NULL,

    consumption_total               DOUBLE PRECISION NOT NULL,
    energy_allocated_total          DOUBLE PRECISION NOT NULL,
    energy_allocated_consumed_total DOUBLE PRECISION NOT NULL,
    residual_volume_total           DOUBLE PRECISION NOT NULL,
    surplus_total                   DOUBLE PRECISION NOT NULL,

    id_iteration_result             INTEGER NOT NULL REFERENCES simulation_iteration_result (id) ON DELETE CASCADE,
    id_community                    INTEGER NOT NULL,

    created_at                      TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at                      TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_simulation_consumer_result_iteration
    ON simulation_consumer_result (id_iteration_result);
CREATE INDEX IF NOT EXISTS idx_simulation_consumer_result_community
    ON simulation_consumer_result (id_community);

DROP TRIGGER IF EXISTS trg_simulation_consumer_result_set_updated_at ON simulation_consumer_result;
CREATE TRIGGER trg_simulation_consumer_result_set_updated_at
    BEFORE UPDATE ON simulation_consumer_result
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();
