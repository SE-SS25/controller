CREATE
EXTENSION "uuid-ossp";
CREATE TABLE worker_metric
(
    id              UUID PRIMARY KEY,
    last_heartbeat  TIMESTAMP,
    uptime INTERVAL,
    req_per_sec     INT,
    write_per_sec   INT,
    read_per_sec    INT,
    req_total       BIGINT,
    req_failed      BIGINT,
    db_availability REAL
);
CREATE TABLE controller_status
(
    scaling        BOOL NOT NULL DEFAULT false,
    last_heartbeat TIMESTAMP
);
CREATE TABLE migration_worker
(
    id              UUID PRIMARY KEY,
    last_heartbeat  TIMESTAMP,
    uptime INTERVAL,
    working_on_from TEXT,
    working_on_to   TEXT
);
CREATE TABLE monitor
(
    id             UUID PRIMARY KEY,
    last_heartbeat TIMESTAMP,
    url            TEXT NOT NULL
);
CREATE TABLE db_mapping
(
    id     UUID PRIMARY KEY,
    url    TEXT NOT NULL,
    "from" TEXT NOT NULL UNIQUE,
    "to"   TEXT NOT NULL UNIQUE
);
CREATE TABLE db_conn_err
(
    worker_id UUID REFERENCES worker_metric (id),
    db_id     UUID REFERENCES db_mapping (id),
    fail_time TIMESTAMP NOT NULL,
    PRIMARY KEY (worker_id, db_id, fail_time)
);
CREATE TABLE db_migration
(
    id          UUID PRIMARY KEY,
    m_worker_id UUID NOT NULL UNIQUE references migration_worker(id),
    url         TEXT NOT NULL,
    "from"      TEXT NOT NULL UNIQUE,
    "to"        TEXT NOT NULL UNIQUE
);
