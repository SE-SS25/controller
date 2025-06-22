package dbutils

import (
	"context"
	"github.com/jackc/pgx/v5/pgxpool"
)

func AcquireLock(ctx context.Context, pool *pgxpool.Pool) (*pgxpool.Conn, error) {

	conn, err := pool.Acquire(ctx)
	return conn, err
}
