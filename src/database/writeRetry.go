package database

import (
	"context"
	oe "controller/src/errors"
	utils "controller/src/utils"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	goutils "github.com/linusgith/goutils/pkg/env_utils"
	"go.uber.org/zap"
	"time"
)

// WriterPerfectionist is a wrapper around the Writer that retries operations with a backoff strategy
// and handles reconcilable errors. It is used to ensure that operations are retried
// in case of temporary failures, while also allowing for a maximum number of retries.
type WriterPerfectionist struct {
	writer         *Writer
	maxRetries     int
	initialBackoff time.Duration
	backoffType    string
}

func NewWriterPerfectionist(writer *Writer) *WriterPerfectionist {

	//15 ms in exp backoff gives us [15,225, 3375] ms as backoff intervals

	initBackoff := goutils.Log().ParseEnvDurationDefault("INIT_RETRY_BACKOFF", 15*time.Millisecond, writer.Logger)

	maxRetries := goutils.Log().ParseEnvIntDefault("MAX_RETRIES", 3, writer.Logger)

	defaultBackoffStrategy := "exp"

	backoffTypeInput := goutils.Log().ParseEnvStringDefault("BACKOFF_TYPE", defaultBackoffStrategy, writer.Logger)

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

// RemoveWorker removes a worker from the database with retries and backoff.
func (w *WriterPerfectionist) RemoveWorker(uuid pgtype.UUID, ctx context.Context) error {

	var err oe.DbError

	for i := 1; i <= w.maxRetries; i++ {
		err = w.writer.RemoveWorker(ctx, uuid)
		if err.Err == nil {
			return nil
		}

		if !err.Reconcilable {
			return err
		}

		if i < w.maxRetries {
			w.writer.Logger.Warn("removing worker failed; retrying...", zap.Int("try", i), zap.Error(err))

			utils.CalculateAndExecuteBackoff(i, w.initialBackoff)
		}
	}

	w.writer.Logger.Error("removing worker failed, retry limit reached", zap.Error(err))
	return err

}

// AddMigrationWorker adds a migration worker to the database with retries and backoff.
func (w *WriterPerfectionist) AddMigrationWorker(uuid, from, to string, ctx context.Context) error {

	var err oe.DbError

	for i := 1; i <= w.maxRetries; i++ {
		err = w.writer.AddMigrationWorker(ctx, uuid, from, to)
		if err.Err == nil {
			return nil
		}

		if !err.Reconcilable {
			return err
		}

		if i < w.maxRetries {
			w.writer.Logger.Warn("removing migration worker failed; retrying...", zap.Int("try", i), zap.Error(err))

			utils.CalculateAndExecuteBackoff(i, w.initialBackoff)
		}
	}

	w.writer.Logger.Error("removing migration worker failed, retry limit reached", zap.Error(err))
	return err

}

// RemoveMigrationWorker removes a migration worker from the database with retries and backoff.
func (w *WriterPerfectionist) RemoveMigrationWorker(uuid string, ctx context.Context) error {

	var err oe.DbError

	for i := 1; i <= w.maxRetries; i++ {
		err = w.writer.RemoveMigrationWorker(ctx, uuid)
		if err.Err == nil {
			return nil
		}

		if !err.Reconcilable {
			return err
		}

		if i < w.maxRetries {
			w.writer.Logger.Warn("removing migration worker failed; retrying...", zap.Int("try", i), zap.Error(err))

			utils.CalculateAndExecuteBackoff(i, w.initialBackoff)
		}
	}

	w.writer.Logger.Error("removing migration worker failed, retry limit reached", zap.Int("retry", w.maxRetries), zap.Error(err))
	return err

}

// AddWorkerJobJoin adds a join from a migration worker to a job with retries and backoff.
func (w *WriterPerfectionist) AddWorkerJobJoin(ctx context.Context, workerId, migrationId string) error {

	var err oe.DbError

	for i := 1; i <= w.maxRetries; i++ {
		err = w.writer.AddWorkerJobJoin(ctx, workerId, migrationId)
		if err.Err == nil {
			return nil
		}

		if !err.Reconcilable {
			return err
		}

		if i < w.maxRetries {
			w.writer.Logger.Warn("adding join from migration worker and job failed; retrying...", zap.Int("try", i), zap.Error(err))

			utils.CalculateAndExecuteBackoff(i, w.initialBackoff)
		}
	}

	w.writer.Logger.Error("adding join from migration worker and job failed, retry limit reached", zap.Int("retry", w.maxRetries), zap.Error(err))
	return err

}

// RemoveMWorkerAndJobs removes a migration worker and its associated jobs with retries and backoff.
func (w *WriterPerfectionist) RemoveMWorkerAndJobs(ctx context.Context, workerId string) error {

	var err oe.DbError

	for i := 1; i <= w.maxRetries; i++ {
		err = w.writer.RemoveMWorkerAndJobs(ctx, workerId)
		if err.Err == nil {
			return nil
		}

		if !err.Reconcilable {
			return err
		}

		if i < w.maxRetries {
			w.writer.Logger.Warn("removing migration worker failed; retrying...", zap.Int("try", i), zap.Error(err))

			utils.CalculateAndExecuteBackoff(i, w.initialBackoff)
		}
	}

	w.writer.Logger.Error("removing migration worker failed, retry limit reached", zap.Int("retry", w.maxRetries), zap.Error(err))
	return err

}

// AddDatabaseMapping adds a database mapping with retries and backoff.
func (w *WriterPerfectionist) AddDatabaseMapping(from, url string, ctx context.Context) error {
	var err oe.DbError

	for i := 1; i <= w.maxRetries; i++ {
		err = w.writer.AddDatabaseMapping(from, url, ctx)
		if err.Err == nil {
			return nil
		}

		if !err.Reconcilable {
			return err
		}

		if i < w.maxRetries {
			w.writer.Logger.Warn("adding database mapping failed; retrying...", zap.Int("try", i), zap.Error(err))

			utils.CalculateAndExecuteBackoff(i, w.initialBackoff)
		}
	}

	w.writer.Logger.Error("adding database mapping failed, retry limit reached", zap.Error(err))
	return err
}

// AddMigrationJob adds a migration job with retries and backoff.
func (w *WriterPerfectionist) AddMigrationJob(ctx context.Context, addReq MigrationJobAddReq, migrationId uuid.UUID) error {

	var err oe.DbError

	for i := 1; i <= w.maxRetries; i++ {
		err = w.writer.AddMigrationJob(ctx, addReq, migrationId)
		if err.Err == nil {
			return nil
		}

		if !err.Reconcilable {
			return err
		}

		if i < w.maxRetries {
			w.writer.Logger.Warn("adding migration job failed; retrying...", zap.Int("try", i), zap.Error(err))

			utils.CalculateAndExecuteBackoff(i, w.initialBackoff)
		}
	}

	w.writer.Logger.Error("adding migration job failed, retry limit reached", zap.Error(err))
	return err
}

// DeleteDBConnErrors deletes outdated database connection errors with retries and backoff.
func (w *WriterPerfectionist) DeleteDBConnErrors(ctx context.Context, dbUrl pgtype.Text, workerId pgtype.UUID, timestamp pgtype.Timestamptz) error {

	var err oe.DbError

	for i := 1; i <= w.maxRetries; i++ {
		err = w.writer.DeleteDbConnErrors(ctx, dbUrl, workerId, timestamp)
		if err.Err == nil {
			return nil
		}

		if !err.Reconcilable {
			return err
		}

		if i < w.maxRetries {
			w.writer.Logger.Warn("deleting outdated dbConnError failed; retrying...", zap.Int("try", i), zap.Error(err))

			utils.CalculateAndExecuteBackoff(i, w.initialBackoff)
		}
	}

	w.writer.Logger.Error("deleting outdated dbConnError failed, retry limit reached", zap.Error(err))
	return err

}

// Heartbeat sends a heartbeat signal to the database with retries and backoff.
func (w *WriterPerfectionist) Heartbeat(ctx context.Context) error {

	var err oe.DbError

	for i := 1; i <= w.maxRetries; i++ {
		err = w.writer.Heartbeat(ctx)
		if err.Err == nil {
			return nil
		}

		if !err.Reconcilable {
			return err
		}

		if i < w.maxRetries {
			w.writer.Logger.Warn("heartbeat failed; retrying...", zap.Int("try", i), zap.Error(err))

			utils.CalculateAndExecuteBackoff(i, w.initialBackoff)
		}
	}

	w.writer.Logger.Error("heartbeat failed, retry limit reached", zap.Error(err))

	return err
}

// RegisterController registers a controller with the database with retries and backoff.
func (w *WriterPerfectionist) RegisterController(ctx context.Context) error {

	var err oe.DbError

	for i := 1; i <= w.maxRetries; i++ {
		err = w.writer.RegisterController(ctx)
		if err.Err == nil {
			return nil
		}

		if !err.Reconcilable {
			return err
		}

		if i < w.maxRetries {
			w.writer.Logger.Warn("registering controller failed; retrying...", zap.Int("try", i), zap.Error(err))

			utils.CalculateAndExecuteBackoff(i, w.initialBackoff)
		}
	}

	w.writer.Logger.Error("registering controller failed, retry limit reached", zap.Error(err))

	return err
}
