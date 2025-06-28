-- name: GetAllWorkerState :many
SELECT * FROM worker_metric;

-- name: GetSingleWorkerState :one
SELECT * FROM worker_metric
WHERE worker_metric.id = $1
LIMIT 1;

-- name: GetControllerState :one
SELECT * FROM controller_status
LIMIT 1;

-- name: GetDatabaseCount :one
SELECT DISTINCT COUNT(url) FROM db_mapping;

-- name: GetWorkerCount :one
SELECT COUNT(id) FROM worker_metric;

-- name: GetAllDbConnErrors :many
SELECT * FROM db_conn_err;




-- name: DeleteWorker :exec
DELETE FROM worker_metric WHERE id=$1;

-- name: CreateMapping :exec
INSERT INTO db_mapping(id, url, "from")
VALUES ($1, $2, $3);

-- name: CreateMigrationJob :exec
INSERT INTO db_migration(id, m_worker_id, url, "from", "to")
SELECT db_mapping.id, $2, db_mapping.url, db_mapping."from", db_mapping."to" FROM db_mapping
WHERE db_mapping.id=$1;

-- name: DeleteDBConnError :exec
DELETE FROM db_conn_err
WHERE db_url = $1 AND worker_id = $2 AND fail_time = $3;