-- name: GetWorkerState :many
SELECT * FROM WorkerMetrics;

-- name: GetControllerState :one
SELECT * FROM ControllerStatus
LIMIT 1;

-- name: GetDatabaseCount :one
SELECT DISTINCT COUNT(url) FROM DatabaseMapping;

-- name: GetWorkerCount :one
SELECT COUNT(uuid) FROM WorkerMetrics;

-- name: DeleteWorker :exec
DELETE FROM WorkerMetrics WHERE uuid=$1;