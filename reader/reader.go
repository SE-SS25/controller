package reader

import (
	"context"
	database "controller/sqlc"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/zap"
)

type Reader struct {
	Pool   *pgxpool.Pool
	Logger *zap.Logger
}

func (r *Reader) GetWorkerState(ctx context.Context) []database.Workermetric {

	conn, err := r.Pool.Acquire(ctx)
	if err != nil {
		r.Logger.Error("Could not get database connection") //TODO
	}

	defer conn.Release()

	q := database.New(conn)
	workers, err := q.GetWorkerState(ctx)
	if err != nil {
		r.Logger.Error("Could not retrieve worker state from db", zap.Error(err))
	}

	return workers
}

func (r *Reader) GetControllerState(ctx context.Context) database.Controllerstatus {

	conn, err := r.Pool.Acquire(ctx)
	if err != nil {
		r.Logger.Error("Could not get database connection") //TODO
	}

	defer conn.Release()

	q := database.New(conn)
	state, err := q.GetControllerState(ctx)
	if err != nil {
		r.Logger.Error("Could not retrieve controller state", zap.Error(err))
	}

	return state
}
