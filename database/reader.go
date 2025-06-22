package database

import (
	"context"
	"controller/dbutils"
	db "controller/sqlc"
	"fmt"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/zap"
)

type Reader struct {
	Pool   *pgxpool.Pool
	Logger *zap.Logger
}

func (r *Reader) GetWorkerState(ctx context.Context) ([]db.WorkerMetric, error) {

	conn, err := dbutils.AcquireLock(ctx, r.Pool)
	if err != nil {
		return nil, fmt.Errorf("getting worker state failed: %w", err)
	}

	defer conn.Release()

	q := db.New(conn)
	workers, err := q.GetWorkerState(ctx)
	if err != nil {
		return nil, fmt.Errorf("getting worker state failed: %w", err)
	}

	return workers, nil
}

func (r *Reader) GetControllerState(ctx context.Context) (db.ControllerStatus, error) {

	conn, err := dbutils.AcquireLock(ctx, r.Pool)
	if err != nil {
		return db.ControllerStatus{}, fmt.Errorf("getting controller state failed: %w", err)
	}

	defer conn.Release()

	q := db.New(conn)
	state, err := q.GetControllerState(ctx)
	if err != nil {
		return db.ControllerStatus{}, fmt.Errorf("getting controller state failed: %w", err)
	}

	return state, nil
}

func (r *Reader) GetWorkerCount(ctx context.Context) (int, error) {
	conn, err := r.Pool.Acquire(ctx)
	if err != nil {
		return 0, fmt.Errorf("getting worker count failed: %w", err)
	}

	defer conn.Release()

	q := db.New(conn)
	count, err := q.GetWorkerCount(ctx)
	if err != nil {
		return 0, fmt.Errorf("getting worker count failed: %w", err)
	}

	return int(count), nil
}

func (r *Reader) GetDBCount(ctx context.Context) (int, error) {
	conn, err := r.Pool.Acquire(ctx)
	if err != nil {
		return 0, fmt.Errorf("getting db count failed: %w", err)
	}

	defer conn.Release()

	q := db.New(conn)
	count, err := q.GetDatabaseCount(ctx)
	if err != nil {
		return 0, fmt.Errorf("getting db count failed: %w", err)
	}

	return int(count), nil
}
