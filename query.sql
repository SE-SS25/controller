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
INSERT INTO db_migration (id, url, m_worker_id, "from", status)
SELECT db_mapping.id,
       $2, -- user input for 'url'
       $3, -- user input for 'm_worker_id'
       db_mapping."from",
       'waiting'  -- initial status
FROM db_mapping
WHERE db_mapping.id = $1;

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
