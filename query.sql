-- name: GetWorkerState :many
SELECT * FROM WorkerMetrics;

-- name: GetControllerState :one
SELECT * FROM ControllerStatus
LIMIT 1;


-- name: DeleteWorker :exec
DELETE FROM WorkerMetrics WHERE uuid=$1;