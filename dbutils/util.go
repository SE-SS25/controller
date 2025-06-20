package dbutils

import (
	"context"
	"controller/reader"
	"github.com/jackc/pgx/v5/pgxpool"
)

func AcquireLock(ctx context.Context, r *reader.Reader) (*pgxpool.Conn, error) {

	conn, err := r.Pool.Acquire(ctx)
	return conn, err
}
