package reader

import (
	"context"
	"controller/dbutils"
	database "controller/sqlc"
	"fmt"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/zap"
)

type Reader struct {
	Pool   *pgxpool.Pool
	Logger *zap.Logger
}

func (r *Reader) GetWorkerState(ctx context.Context) ([]database.Workermetric, error) {

	conn, err := dbutils.AcquireLock(ctx, r)
	if err != nil {
		return nil, fmt.Errorf("getting worker state failed: %w", err)
	}

	defer conn.Release()

	q := database.New(conn)
	workers, err := q.GetWorkerState(ctx)
	if err != nil {
		return nil, fmt.Errorf("getting worker state failed: %w", err)
	}

	return workers, nil
}

func (r *Reader) GetControllerState(ctx context.Context) (database.Controllerstatus, error) {

	conn, err := dbutils.AcquireLock(ctx, r)
	if err != nil {
		return database.Controllerstatus{}, fmt.Errorf("getting controller state failed: %w", err)
	}

	defer conn.Release()

	q := database.New(conn)
	state, err := q.GetControllerState(ctx)
	if err != nil {
		return database.Controllerstatus{}, fmt.Errorf("getting controller state failed: %w", err)
	}

	return state, nil
}

func (r *Reader) GetWorkerCount(ctx context.Context) (int, error) {
	conn, err := r.Pool.Acquire(ctx)
	if err != nil {
		return 0, fmt.Errorf("getting worker count failed: %w", err)
	}

	defer conn.Release()

	q := database.New(conn)
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

	q := database.New(conn)
	count, err := q.GetDatabaseCount(ctx)
	if err != nil {
		return 0, fmt.Errorf("getting db count failed: %w", err)
	}

	return int(count), nil
}
