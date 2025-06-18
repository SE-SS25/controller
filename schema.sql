CREATE TABLE WorkerMetrics
(
    uuid            UUID PRIMARY KEY,
    last_heartbeat  TIMESTAMP,
    uptime          INTERVAL,
    req_per_sec     INT,
    write_per_sec   INT,
    read_per_sec    INT,
    req_total       BIGINT,
    req_failed      BIGINT,
    db_availability REAL
);

CREATE TABLE ControllerStatus
(
    scaling        BOOL NOT NULL DEFAULT false,
    last_heartbeat TIMESTAMP
);

CREATE TABLE MigrationWorkers
(
    uuid            UUID PRIMARY KEY,
    last_heartbeat  TIMESTAMP,
    uptime          INTERVAL,
    working_on_from TEXT,
    working_on_to   TEXT
);

CREATE TABLE Monitors
(
    "uuid"         UUID PRIMARY KEY,
    last_heartbeat TIMESTAMP,
    url            TEXT NOT NULL
);

CREATE TABLE DbConnError
(
    worker_id    UUID REFERENCES WorkerMetrics (uuid),
    db_id        UUID REFERENCES DatabaseMetrics (uuid),
    failure_time TIMESTAMP,
    PRIMARY KEY (worker_id, db_id, failure_time)
);

CREATE TABLE DatabaseMapping
(
    url    TEXT,
    "from" TEXT UNIQUE,
    "to"   TEXT UNIQUE,
    PRIMARY KEY ("from", "to")
);

CREATE TABLE Migrations
(
    url    TEXT,
    "from" TEXT UNIQUE,
    "to"   TEXT UNIQUE,
    PRIMARY KEY ("from", "to")
);