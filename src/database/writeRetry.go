package database

import (
	"context"
	"github.com/jackc/pgx/v5/pgtype"
	"go.uber.org/zap"
)

type WriterPerfectionist struct {
	writer     Writer
	maxRetries int
}

func NewWriterPerfectionist(writer Writer, maxRetries int) *WriterPerfectionist {
	return &WriterPerfectionist{
		writer:     writer,
		maxRetries: maxRetries,
	}
}

func (w *WriterPerfectionist) RemoveWorker(uuid pgtype.UUID, ctx context.Context) error {

	var err error

	for i := 1; i <= w.maxRetries; i++ {
		err := w.writer.RemoveWorker(ctx, uuid)
		if err == nil {
			return nil
		}

		if i < w.maxRetries {
			w.writer.Logger.Warn("getting workers state failed; retrying...", zap.Int("try", i), zap.Error(err))
		}
	}

	w.writer.Logger.Error("getting workers state failed, retry limit reached", zap.Error(err))
	return err

}

func (w *WriterPerfectionist) AddDatabaseMapping(from, url string, ctx context.Context) error {
	var err error

	for i := 1; i <= w.maxRetries; i++ {
		err = w.writer.AddDatabaseMapping(from, url, ctx)
		if err == nil {
			return nil
		}

		if i < w.maxRetries {
			w.writer.Logger.Warn("getting workers state failed; retrying...", zap.Int("try", i), zap.Error(err))
		}
	}

	w.writer.Logger.Error("getting workers state failed, retry limit reached", zap.Error(err))
	return err
}

func (w *WriterPerfectionist) AddMigrationJob(ctx context.Context, rangeID string, migrationWorkerID string) error {

	var err error

	for i := 1; i <= w.maxRetries; i++ {
		err = w.writer.AddMigrationJob(ctx, rangeID, migrationWorkerID)
		if err == nil {
			return nil
		}

		if i < w.maxRetries {
			w.writer.Logger.Warn("adding migration job failed; retrying...", zap.Int("try", i), zap.Error(err))
		}
	}

	w.writer.Logger.Error("adding migration job failed, retry limit reached", zap.Error(err))
	return err
}

func (w *WriterPerfectionist) DeleteDBConnErrors(ctx context.Context, dbUrl pgtype.Text, workerId pgtype.UUID, timestamp pgtype.Timestamp) error {

	var err error

	for i := 1; i <= w.maxRetries; i++ {
		err = w.writer.DeleteDbConnErrors(ctx, dbUrl, workerId, timestamp)
		if err == nil {
			return nil
		}

		if i < w.maxRetries {
			w.writer.Logger.Warn("deleting outdated dbConnError failed; retrying...", zap.Int("try", i), zap.Error(err))
		}
	}

	w.writer.Logger.Error("deleting outdated dbConnError failed, retry limit reached", zap.Error(err))
	return err

}
