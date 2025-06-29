package database

import (
	"context"
	"controller/utils"
	"github.com/jackc/pgx/v5/pgtype"
	"go.uber.org/zap"
	"time"
)

type WriterPerfectionist struct {
	writer         *Writer
	maxRetries     int
	initialBackoff time.Duration
	backoffType    string
}

func NewWriterPerfectionist(writer *Writer, maxRetries int) *WriterPerfectionist {

	//TODO ugly with the loggers

	//15 ms in exp backoff gives us [15,225, 3375] ms as backoff intervals
	//we shouldn't allow a long backoff for the controller since shit can hit the fan fast
	initBackoff := utils.ParseEnvDuration("INIT_RETRY_BACKOFF", 15*time.Millisecond, writer.Logger)

	defaultBackoffStrategy := "exp"

	backoffTypeInput := utils.ParseEnvStringWithDefault("BACKOFF_TYPE", defaultBackoffStrategy, writer.Logger)

	var backoffType string

	switch backoffTypeInput {
	case "exp":
		backoffType = "exponential"
	case "lin":
		backoffType = "linear"
	default:
		writer.Logger.Warn("invalid backoff strategy provided, setting default", zap.String("provided", backoffTypeInput))
		backoffType = defaultBackoffStrategy
	}

	return &WriterPerfectionist{
		writer:         writer,
		maxRetries:     maxRetries,
		initialBackoff: initBackoff,
		backoffType:    backoffType,
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

			utils.CalculateAndExecuteBackoff(i, w.initialBackoff)
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

			utils.CalculateAndExecuteBackoff(i, w.initialBackoff)
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

			utils.CalculateAndExecuteBackoff(i, w.initialBackoff)
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

			utils.CalculateAndExecuteBackoff(i, w.initialBackoff)
		}
	}

	w.writer.Logger.Error("deleting outdated dbConnError failed, retry limit reached", zap.Error(err))
	return err

}

func (w *WriterPerfectionist) Heartbeat(ctx context.Context) error {

	var err error

	for i := 1; i <= w.maxRetries; i++ {
		err = w.writer.Heartbeat(ctx)
		if err == nil {
			return nil
		}

		if i < w.maxRetries {
			w.writer.Logger.Warn("heartbeat failed; retrying...", zap.Int("try", i), zap.Error(err))

			utils.CalculateAndExecuteBackoff(i, w.initialBackoff)
		}
	}

	w.writer.Logger.Error("heartbeat failed, retry limit reached", zap.Error(err))

	return err
}
