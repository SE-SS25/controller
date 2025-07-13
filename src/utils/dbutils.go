package utils

import (
	"context"
	oe "controller/src/errors"
	"errors"
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
		var pgErr *pgconn.PgError
		if errors.As(execErr, &pgErr) {
			switch pgErr.Code {
			case "23505":
				return oe.DbError{Err: fmt.Errorf("UNIQUE violation - code 23505: %w", pgErr), Reconcilable: false}
			case "23503":
				return oe.DbError{Err: fmt.Errorf("FOREIGN KEY violation - code 23503: %w", pgErr), Reconcilable: false}
			case "23502":
				return oe.DbError{Err: fmt.Errorf("NOT NULL violation - code 23502: %w", pgErr), Reconcilable: false}
			default:
				return oe.DbError{Err: fmt.Errorf("unknown pg error occurred: %w", execErr), Reconcilable: true}
			}
		} else {
			return oe.DbError{
				Err:          fmt.Errorf("unknown execution error occurred: %w", execErr),
				Reconcilable: true,
			}
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
