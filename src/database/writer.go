package database

import (
	"context"
	database "controller/src/database/sqlc"
	"fmt"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/zap"
)

type Writer struct {
	Logger *zap.Logger
	Pool   *pgxpool.Pool
}

func (w *Writer) RemoveWorker(ctx context.Context, uuid pgtype.UUID) error {

	conn, err := w.Pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("removing worker failed: %w", err)
	}

	q := database.New(conn)
	err = q.DeleteWorker(ctx, uuid)
	if err != nil {
		return fmt.Errorf("removing worker failed: %w", err)
	}

	return nil

}

func (w *Writer) AddDatabaseMapping(from, url string, ctx context.Context) error {

	conn, err := w.Pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("adding database mapping failed: %w", err)
	}

	q := database.New(conn)
	params := database.CreateMappingParams{
		ID: pgtype.UUID{
			Bytes: uuid.New(),
			Valid: true,
		},
		Url:  url,
		From: from,
	}
	err = q.CreateMapping(ctx, params)
	if err != nil {
		return fmt.Errorf("adding database mapping failed: %w", err)
	}

	return nil
}

// AddMigrationJob takes one range with a given id from the mapping table and transfers it into to migrations-table, marking it to be migrated by the migration worker specified through the id
func (w *Writer) AddMigrationJob(ctx context.Context, rangeId, mWorkerId string) error {

	conn, err := w.Pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("adding migration job to database failed %w", err)
	}

	q := database.New(conn)
	params := database.CreateMigrationJobParams{
		ID: pgtype.UUID{
			Bytes: [16]byte([]byte(rangeId)),
			Valid: true,
		}, //id of the range
		MWorkerID: pgtype.UUID{
			Bytes: [16]byte([]byte(mWorkerId)),
			Valid: true,
		}, //id of the worker responsible
	}
	err = q.CreateMigrationJob(ctx, params)
	if err != nil {
		return fmt.Errorf("adding migration job to database failed %w", err)
	}

	return nil
}

func (w *Writer) DeleteDbConnErrors(ctx context.Context, dbUrl pgtype.Text, workerId pgtype.UUID, failTime pgtype.Timestamp) error {
	conn, err := w.Pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("removing outdated dbConnErr from database failed %w", err)
	}

	q := database.New(conn)
	params := database.DeleteDBConnErrorParams{
		DbUrl:    dbUrl,
		WorkerID: workerId,
		FailTime: failTime,
	}

	deletionErr := q.DeleteDBConnError(ctx, params)
	if deletionErr != nil {
		return fmt.Errorf("removing outdated dbConnErr from database failed %w", err)
	}

	return nil
}
