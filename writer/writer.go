package writer

import (
	"context"
	database "controller/sqlc"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/zap"
)

type Writer struct {
	Logger *zap.Logger
	Pool   *pgxpool.Pool
}

func (w *Writer) RemoveWorker(ctx context.Context, uuid pgtype.UUID) {

	conn, err := w.Pool.Acquire(ctx)
	if err != nil {
		w.Logger.Error("Could not get database connection")
	}

	q := database.New(conn)
	err = q.DeleteWorker(ctx, uuid)
	if err != nil {
		w.Logger.Error("Could not delete worker from table", zap.Error(err))
	}

	return

}
