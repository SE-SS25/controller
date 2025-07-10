-- name: GetAllWorkerState :many
SELECT *
FROM worker_metric;

-- name: GetAllMWorkerState :many
SELECT *
FROM migration_worker;

-- name: GetSingleWorkerState :one
SELECT *
FROM worker_metric
WHERE worker_metric.id = $1
LIMIT 1;

-- name: GetControllerState :one
SELECT *
FROM controller_status
LIMIT 1;

-- name: GetDatabaseCount :one
SELECT DISTINCT COUNT(*)
FROM db_instance;

-- name: GetWorkerCount :one
SELECT COUNT(id)
FROM worker_metric;

-- name: GetAllDbConnErrors :many
SELECT *
FROM db_conn_err;

-- name: GetFreeMigrationWorker :one
(SELECT id
 FROM migration_worker
 EXCEPT
 SELECT m_worker_id
 FROM db_migration)
    LIMIT 1;

-- name: GetAllDbInstances :many
SELECT *
FROM db_instance;

-- name: GetAllDbMappings :many
SELECT *
FROM db_mapping;


-- name: DeleteWorker :execresult
DELETE
FROM worker_metric
WHERE id = $1;

-- name: AddMigrationWorker :execresult
INSERT INTO migration_worker (id, last_heartbeat, uptime, working_on_from, working_on_to)
VALUES ($1, $2, $3, $4, $5);

-- name: DeleteMigrationWorker :execresult
DELETE
FROM migration_worker
WHERE id = $1;

-- name: CreateMapping :execresult
INSERT INTO db_mapping(id, url, "from", size)
VALUES ($1, $2, $3, 0);

-- name: CreateMigrationJob :execresult
INSERT INTO db_migration (id, url, m_worker_id, "from", "to", status)
SELECT db_mapping.id,
       db_mapping.url,
       $4, -- m_worker_id parameter
       db_mapping."from",
       $3, -- "to" parameter
       'waiting'
FROM db_mapping
WHERE db_mapping.url = $1
  AND db_mapping."from" = $2;

-- name: DeleteDBConnError :execresult
DELETE
FROM db_conn_err
WHERE db_url = $1
  AND worker_id = $2
  AND fail_time = $3;

-- name: DeleteOldControllerHeartbeat :execresult
DELETE
FROM controller_status;

-- name: CreateNewControllerHeartbeat :execresult
INSERT INTO controller_status(scaling, last_heartbeat)
VALUES ($1, $2);
