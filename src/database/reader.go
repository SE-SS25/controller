package database

import (
	"context"
	database "controller/src/database/sqlc"
	"controller/utils"
	"fmt"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/zap"
)

type Reader struct {
	Pool   *pgxpool.Pool
	Logger *zap.Logger
}

func (r *Reader) Ping(ctx context.Context) error {

	r.Logger.Debug("Trying to ping database")

	err := r.Pool.Ping(ctx)
	if err != nil {
		return err
	}

	return nil
}

func (r *Reader) GetAllWorkerState(ctx context.Context) ([]database.WorkerMetric, error) {

	conn, err := utils.AcquireLock(ctx, r.Pool)
	if err != nil {
		return nil, fmt.Errorf("acquiring lock failed: %w", err)
	}

	defer conn.Release()

	q := database.New(conn)
	workers, err := q.GetAllWorkerState(ctx)
	if err != nil {
		return nil, fmt.Errorf("getting all worker states failed: %w", err)
	}

	r.Logger.Debug("successfully got workers state")
	return workers, nil
}

func (r *Reader) GetSingleWorkerState(ctx context.Context, workerID string) (database.WorkerMetric, error) {

	conn, err := utils.AcquireLock(ctx, r.Pool)
	if err != nil {
		return database.WorkerMetric{}, fmt.Errorf("acquiring lock failed: %w", err)
	}

	defer conn.Release()

	q := database.New(conn)
	worker, err := q.GetSingleWorkerState(ctx, pgtype.UUID{
		Bytes: [16]byte([]byte(workerID)),
		Valid: true,
	})
	if err != nil {
		return database.WorkerMetric{}, fmt.Errorf("getting single worker state failed: %w", err)
	}

	r.Logger.Debug("successfully got workers state")
	return worker, nil
}

func (r *Reader) GetControllerState(ctx context.Context) (database.ControllerStatus, error) {

	conn, err := utils.AcquireLock(ctx, r.Pool)
	if err != nil {
		return database.ControllerStatus{}, fmt.Errorf("acquiring lock failed: %w", err)
	}

	defer conn.Release()

	q := database.New(conn)
	state, err := q.GetControllerState(ctx)
	if err != nil {
		return database.ControllerStatus{}, fmt.Errorf("getting controller state failed: %w", err)
	}

	r.Logger.Debug("successfully got controller state")

	return state, nil
}

func (r *Reader) GetWorkerCount(ctx context.Context) (int, error) {
	conn, err := r.Pool.Acquire(ctx)
	if err != nil {
		return 0, fmt.Errorf("acquiring lock failed: %w", err)
	}

	defer conn.Release()

	q := database.New(conn)
	count, err := q.GetWorkerCount(ctx)
	if err != nil {
		return 0, fmt.Errorf("getting worker count failed: %w", err)
	}

	r.Logger.Debug("successfully got worker count", zap.Int64("count", count))

	return int(count), nil
}

func (r *Reader) GetDBCount(ctx context.Context) (int, error) {
	conn, err := r.Pool.Acquire(ctx)
	if err != nil {
		return 0, fmt.Errorf("acquiring lock failed: %w", err)
	}

	defer conn.Release()

	q := database.New(conn)
	count, err := q.GetDatabaseCount(ctx)
	if err != nil {
		return 0, fmt.Errorf("getting db count failed: %w", err)
	}

	return int(count), nil
}

func (r *Reader) GetDBConnErrors(ctx context.Context) ([]database.DbConnErr, error) {

	conn, err := r.Pool.Acquire(ctx)
	if err != nil {
		return nil, fmt.Errorf("acquiring lock failed: %w", err)
	}

	defer conn.Release()

	q := database.New(conn)
	connectionErrors, err := q.GetAllDbConnErrors(ctx)
	if err != nil {
		return nil, fmt.Errorf("getting db_conn_errors failed: %w", err)
	}

	return connectionErrors, nil
}
