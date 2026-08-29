-- Test-only DDL for the CRM tables this service interacts with.
--
-- The real CRM schema is owned by another service. The migration files in
-- scripts/sql/migrations/ are applied manually against the production CRM DB
-- and are not part of scripts/sql/schema.sql (which only declares the local
-- DB tables). Tests run against a single Postgres instance, so we mirror the
-- minimum CRM DDL needed by the suite here.
--
-- Mirrors core/database/models.py::Community and ::CommunitySubscription.

CREATE TABLE IF NOT EXISTS community (
    id                INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name              VARCHAR(255) NOT NULL UNIQUE,
    auth_community_id VARCHAR(255) NOT NULL UNIQUE,
    created_at        TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at        TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS community_subscription (
    id           INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    id_community INTEGER     NOT NULL,
    feature      VARCHAR(64) NOT NULL,
    is_active    BOOLEAN     NOT NULL DEFAULT FALSE,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_community_subscription_community_feature
        UNIQUE (id_community, feature)
);

CREATE INDEX IF NOT EXISTS idx_community_subscription_id_community
    ON community_subscription (id_community);


-- Mirrors shared/models/crm_models.py::AllocationKeyModel / IterationModel /
-- ConsumerModel. Used by POST /generation/save tests, which copy a generated
-- key tree from the Local DB into the CRM DB via to_allocation_key_crm.

CREATE TABLE IF NOT EXISTS allocation_key (
    id           INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name         VARCHAR(255) NOT NULL,
    description  TEXT         NOT NULL,
    id_community INTEGER      NOT NULL,
    created_at   TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at   TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS iteration (
    id                          INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    number                      INTEGER NOT NULL,
    energy_allocated_percentage DOUBLE PRECISION NOT NULL,
    id_key                      INTEGER NOT NULL REFERENCES allocation_key(id),
    id_community                INTEGER NOT NULL,
    created_at                  TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at                  TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS consumer (
    id                          INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name                        VARCHAR(255) NOT NULL,
    energy_allocated_percentage DOUBLE PRECISION NOT NULL,
    id_iteration                INTEGER NOT NULL REFERENCES iteration(id),
    id_community                INTEGER NOT NULL,
    created_at                  TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at                  TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);


-- Mirrors shared/models/crm_models.py::AppUserModel. Only the columns the
-- audit log service reads — auth_user_id -> (id, email) — are present.

CREATE TABLE IF NOT EXISTS app_user (
    id            INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    auth_user_id  VARCHAR(255) NOT NULL UNIQUE,
    email         VARCHAR(256) NOT NULL
);


-- Mirrors shared/models/crm_models.py::AuditLogModel and the production DDL
-- in crm-backend/database_script/2026-05-27_audit_log.sql. Append-only by
-- convention. Indexes from the production migration are omitted here — they
-- exist only to keep production reads fast and don't affect test correctness.

CREATE TABLE IF NOT EXISTS audit_log (
    id           BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    id_community INTEGER REFERENCES community(id) ON DELETE CASCADE,
    timestamp    TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    action       VARCHAR(128) NOT NULL,
    source       VARCHAR(32)  NOT NULL,
    entity_type  VARCHAR(64)  NOT NULL,
    entity_id    VARCHAR(64),
    user_id      INTEGER,
    user_email   VARCHAR(256),
    payload      JSONB        NOT NULL DEFAULT '{}'::jsonb
);


-- ---- Metering tables -------------------------------------------------------
-- Mirrors crm-backend/database_script/init.sql (meter / meter_data /
-- meter_consumption / sharing_operation), trimmed to the columns this service
-- actually SELECTs. Adapted from billing/tests/sql/crm_test_schema.sql, which
-- carries the same block for the same reason.
--
-- Two production properties are reproduced deliberately, because the code under
-- test depends on both:
--   * meter_consumption has NO unique constraint on (ean, timestamp) -- that is
--     what makes a double import possible and CRM_DUPLICATE_READINGS necessary.
--   * every measure column is nullable, so COALESCE in the queries is load-
--     bearing rather than defensive.

CREATE TABLE IF NOT EXISTS sharing_operation (
    id           INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name         VARCHAR(255) NOT NULL,
    type         INTEGER      NOT NULL DEFAULT 1,
    is_public    BOOLEAN      NOT NULL DEFAULT FALSE,
    id_community INTEGER      NOT NULL REFERENCES community (id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS meter (
    ean               VARCHAR(64) PRIMARY KEY,
    meter_number      VARCHAR(255),
    tarif_group       INTEGER,
    phases_number     INTEGER,
    reading_frequency INTEGER,
    id_community      INTEGER NOT NULL REFERENCES community (id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS meter_data (
    id                   INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    ean                  VARCHAR(64) NOT NULL REFERENCES meter (ean) ON DELETE CASCADE,
    id_member            INTEGER,
    id_sharing_operation INTEGER REFERENCES sharing_operation (id),
    status               INTEGER,   -- 1=ACTIVE
    client_type          INTEGER,   -- 1=Residentiel, 2=Professionnel, 3=Industriel
    injection_status     INTEGER,
    production_chain     INTEGER,
    start_date           DATE,
    end_date             DATE,
    id_community         INTEGER NOT NULL REFERENCES community (id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS meter_consumption (
    id                   INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    ean                  VARCHAR(64) NOT NULL REFERENCES meter (ean) ON DELETE CASCADE,
    id_sharing_operation INTEGER REFERENCES sharing_operation (id),
    timestamp            TIMESTAMPTZ NOT NULL,
    gross                DOUBLE PRECISION,
    net                  DOUBLE PRECISION,
    shared               DOUBLE PRECISION,
    inj_gross            DOUBLE PRECISION,
    inj_shared           DOUBLE PRECISION,
    inj_net              DOUBLE PRECISION,
    id_community         INTEGER NOT NULL REFERENCES community (id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_meter_consumption_lookup
    ON meter_consumption (id_sharing_operation, timestamp);
