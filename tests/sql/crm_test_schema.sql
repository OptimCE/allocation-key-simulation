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
