-- name: GetAllWorkerState :many
SELECT *
FROM worker_metric;

-- name: GetSingleWorkerState :one
SELECT *
FROM worker_metric
WHERE worker_metric.id = $1 LIMIT 1;

-- name: GetControllerState :one
SELECT *
FROM controller_status LIMIT 1;

-- name: GetDatabaseCount :one
SELECT DISTINCT COUNT(url)
FROM db_mapping;

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


-- name: DeleteWorker :exec
DELETE
FROM worker_metric
WHERE id = $1;

-- name: CreateMapping :exec
INSERT INTO db_mapping(id, url, "from")
VALUES ($1, $2, $3);

-- name: CreateMigrationJob :exec
INSERT INTO db_migration (id, url, m_worker_id, "from", "to", status)
SELECT db_mapping.id,
       db_mapping.url,
       $4, -- m_worker_id parameter
       db_mapping."from",
       $3, -- "to" parameter (fixed comma)
       'waiting'
FROM db_mapping
WHERE db_mapping.url = $1 AND db_mapping."from" = $2;

-- name: DeleteDBConnError :exec
DELETE
FROM db_conn_err
WHERE db_url = $1
  AND worker_id = $2
  AND fail_time = $3;

-- name: DeleteOldControllerHeartbeat :exec
DELETE
FROM controller_status;

-- name: CreateNewControllerHeartbeat :exec
INSERT INTO controller_status(scaling, last_heartbeat)
VALUES ($1, $2);
