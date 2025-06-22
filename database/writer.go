package database

import (
	"context"
	db "controller/sqlc"
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

	q := db.New(conn)
	err = q.DeleteWorker(ctx, uuid)
	if err != nil {
		return fmt.Errorf("removing worker failed: %w", err)
	}

	return nil

}

func (w *Writer) AddDatabaseMapping(ctx context.Context, url, from, to string) error {

	conn, err := w.Pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("adding database mapping failed: %w", err)
	}

	q := db.New(conn)
	params := db.CreateMappingParams{
		ID: pgtype.UUID{
			Bytes: uuid.New(),
			Valid: true,
		},
		Url:  url,
		From: from,
		To:   to,
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

	q := db.New(conn)
	params := db.CreateMigrationJobParams{
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
