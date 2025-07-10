package utils

import (
	"context"
	oe "controller/src/errors"
	"fmt"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	goutils "github.com/linusgith/goutils/pkg/env_utils"
	"go.uber.org/zap"
)

func SetupDBConn(logger *zap.Logger, ctx context.Context) (*pgxpool.Pool, error) {

	pgConn := goutils.Log().ParseEnvStringPanic("PG_CONN", logger)
	logger.Debug("Connecting to database", zap.String("conn_string", pgConn))

	pool, err := pgxpool.New(ctx, pgConn)
	if err != nil {
		logger.Error("Unable to connect to database", zap.Error(err))
		return nil, err
	}

	if err = pool.Ping(ctx); err != nil {
		logger.Error("Unable to ping database", zap.Error(err))
		return nil, err
	}

	logger.Info("Connected to PG database", zap.String("conn", pgConn))

	return pool, nil
}

func Must(execRes pgconn.CommandTag, execErr error) oe.DbError {
	if execErr != nil {
		return oe.DbError{
			Err:          fmt.Errorf("execution error occurred: %w", execErr),
			Reconcilable: true,
		}
	}
	if execRes.RowsAffected() == 0 {
		return oe.DbError{
			Err:          fmt.Errorf("no execution error but no rows affected: %s", execRes.String()),
			Reconcilable: false,
		}
	}

	return oe.DbError{Err: nil}
}
