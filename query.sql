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




-- name: DeleteWorker :exec
DELETE FROM worker_metric WHERE id=$1;

-- name: CreateMapping :exec
INSERT INTO db_mapping(id, url, "from", "to")
VALUES ($1, $2, $3, $4);

-- name: CreateMigrationJob :exec
INSERT INTO db_migration(id, m_worker_id, url, "from", "to")
SELECT db_mapping.id, $2, db_mapping.url, db_mapping."from", db_mapping."to" FROM db_mapping
WHERE db_mapping.id=$1;
