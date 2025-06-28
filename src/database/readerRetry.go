package database

import (
	"context"
	sqlc "controller/src/database/sqlc"
	"go.uber.org/zap"
)

type ReaderPerfectionist struct {
	reader     Reader
	maxRetries int
}

func NewReaderPerfectionist(reader Reader, maxRetries int) *ReaderPerfectionist {
	return &ReaderPerfectionist{
		reader:     reader,
		maxRetries: maxRetries,
	}
}

func (r *ReaderPerfectionist) Ping(ctx context.Context) error {

	var err error

	for i := 1; i <= r.maxRetries; i++ {
		err := r.reader.Ping(ctx)
		if err == nil {
			return nil
		}

		if i < r.maxRetries {
			r.reader.Logger.Warn("pinging database failed; retrying...", zap.Int("try", i), zap.Error(err))
		}
	}

	r.reader.Logger.Error("getting controller state failed, retry limit reached", zap.Error(err))
	return err

}

func (r *ReaderPerfectionist) GetControllerState(ctx context.Context) (sqlc.ControllerStatus, error) {

	var err error

	for i := 1; i <= r.maxRetries; i++ {
		state, err := r.reader.GetControllerState(ctx)
		if err == nil {
			return state, nil
		}

		if i < r.maxRetries {
			r.reader.Logger.Warn("getting controller state failed; retrying...", zap.Int("try", i), zap.Error(err))
		}
	}

	r.reader.Logger.Error("getting controller state failed, retry limit reached", zap.Error(err))
	return sqlc.ControllerStatus{}, err

}

func (r *ReaderPerfectionist) GetAllWorkerState(ctx context.Context) ([]sqlc.WorkerMetric, error) {

	var err error

	for i := 1; i <= r.maxRetries; i++ {
		state, err := r.reader.GetAllWorkerState(ctx)
		if err == nil {
			return state, nil
		}

		if i < r.maxRetries {
			r.reader.Logger.Warn("getting workers state failed; retrying...", zap.Int("try", i), zap.Error(err))
		}
	}

	r.reader.Logger.Error("getting workers state failed, retry limit reached", zap.Error(err))
	return nil, err
}

func (r *ReaderPerfectionist) GetSingleWorkerState(ctx context.Context, workerID string) (sqlc.WorkerMetric, error) {

	var err error

	for i := 1; i <= r.maxRetries; i++ {
		state, err := r.reader.GetSingleWorkerState(ctx, workerID)
		if err == nil {
			return state, nil
		}

		if i < r.maxRetries {
			r.reader.Logger.Warn("getting workers state failed; retrying...", zap.Int("try", i), zap.Error(err))
		}
	}

	r.reader.Logger.Error("getting worker state failed, retry limit reached", zap.String("workerID", workerID), zap.Error(err))
	return sqlc.WorkerMetric{}, err
}

func (r *ReaderPerfectionist) GetDBCount(ctx context.Context) (int, error) {
	var err error

	for i := 1; i <= r.maxRetries; i++ {
		count, err := r.reader.GetDBCount(ctx)
		if err == nil {
			return count, nil
		}

		if i < r.maxRetries {
			r.reader.Logger.Warn("getting workers state failed; retrying...", zap.Int("try", i), zap.Error(err))
		}
	}

	r.reader.Logger.Error("getting workers state failed, retry limit reached", zap.Error(err))
	return 0, err
}

func (r *ReaderPerfectionist) GetDBConnErrors(ctx context.Context) ([]sqlc.DbConnErr, error) {
	var err error

	for i := 1; i <= r.maxRetries; i++ {
		connErrors, err := r.reader.GetDBConnErrors(ctx)
		if err == nil {
			return connErrors, nil
		}

		if i < r.maxRetries {
			r.reader.Logger.Warn("getting db connection errors failed; retrying...", zap.Int("try", i), zap.Error(err))
		}
	}

	r.reader.Logger.Error("getting db connection errors failed, retry limit reached", zap.Error(err))
	return nil, err
}
