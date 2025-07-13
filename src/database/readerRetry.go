package database

import (
	"context"
	sqlc "controller/src/database/sqlc"
	"controller/src/utils"
	"github.com/jackc/pgx/v5/pgtype"
	goutils "github.com/linusgith/goutils/pkg/env_utils"
	"go.uber.org/zap"
	"time"
)

// ReaderPerfectionist is a wrapper around Reader that implements retry logic with backoff strategies
// for various database operations. It retries operations up to a maximum number of times
// with an initial backoff duration that can be configured. The backoff strategy can be either
// exponential or linear, also configurable via environment variables.
// It is designed to handle transient errors gracefully, allowing the application to recover
// from temporary issues without crashing or losing data.
type ReaderPerfectionist struct {
	reader         *Reader
	maxRetries     int
	initialBackoff time.Duration
	backoffType    string
}

func NewReaderPerfectionist(reader *Reader) *ReaderPerfectionist {

	//15 ms in exp backoff gives us [15,225, 3375] ms as backoff intervals

	initBackoff := goutils.Log().ParseEnvDurationDefault("INIT_RETRY_BACKOFF", 15*time.Millisecond, reader.Logger)

	maxRetries := goutils.Log().ParseEnvIntDefault("MAX_RETRIES", 3, reader.Logger)

	defaultBackoffStrategy := "exp"

	backoffTypeInput := goutils.Log().ParseEnvStringDefault("BACKOFF_TYPE", defaultBackoffStrategy, reader.Logger)

	var backoffType string

	switch backoffTypeInput {
	case "exp":
		backoffType = "exponential"
	case "lin":
		backoffType = "linear"
	default:
		reader.Logger.Warn("invalid backoff strategy provided, setting default", zap.String("provided", backoffTypeInput))
		backoffType = defaultBackoffStrategy
	}

	return &ReaderPerfectionist{
		reader:         reader,
		maxRetries:     maxRetries,
		initialBackoff: initBackoff,
		backoffType:    backoffType,
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

			utils.CalculateAndExecuteBackoff(i, r.initialBackoff)
		}
	}

	r.reader.Logger.Error("pinging database failed, retry limit reached", zap.Error(err))
	return err

}

// GetControllerState retrieves the current state of the controller.
func (r *ReaderPerfectionist) GetControllerState(ctx context.Context) (sqlc.ControllerStatus, error) {

	var err error

	for i := 1; i <= r.maxRetries; i++ {
		state, err := r.reader.GetControllerState(ctx)
		if err == nil {
			return state, nil
		}

		if i < r.maxRetries {
			r.reader.Logger.Warn("getting controller state failed; retrying...", zap.Int("try", i), zap.Error(err))

			utils.CalculateAndExecuteBackoff(i, r.initialBackoff)
		}
	}

	r.reader.Logger.Error("getting controller state failed, retry limit reached", zap.Error(err))
	return sqlc.ControllerStatus{}, err

}

// GetAllWorkerState retrieves the state of all workers.
func (r *ReaderPerfectionist) GetAllWorkerState(ctx context.Context) ([]sqlc.WorkerMetric, error) {

	var err error

	for i := 1; i <= r.maxRetries; i++ {
		state, err := r.reader.GetAllWorkerState(ctx)
		if err == nil {
			return state, nil
		}

		if i < r.maxRetries {
			r.reader.Logger.Warn("getting all worker states failed; retrying...", zap.Int("try", i), zap.Error(err))

			utils.CalculateAndExecuteBackoff(i, r.initialBackoff)
		}
	}

	r.reader.Logger.Error("getting all worker states failed, retry limit reached", zap.Error(err))
	return nil, err
}

// GetAllMWorkerState retrieves the state of all migration workers.
func (r *ReaderPerfectionist) GetAllMWorkerState(ctx context.Context) ([]sqlc.MigrationWorker, error) {

	var err error

	for i := 1; i <= r.maxRetries; i++ {
		state, err := r.reader.GetAllMWorkerState(ctx)
		if err == nil {
			return state, nil
		}

		if i < r.maxRetries {
			r.reader.Logger.Warn("getting all worker states failed; retrying...", zap.Int("try", i), zap.Error(err))

			utils.CalculateAndExecuteBackoff(i, r.initialBackoff)
		}
	}

	r.reader.Logger.Error("getting all worker states failed, retry limit reached", zap.Error(err))
	return nil, err
}

// GetSingleWorkerState retrieves the state of a single worker identified by workerID
func (r *ReaderPerfectionist) GetSingleWorkerState(ctx context.Context, workerID string) (sqlc.WorkerMetric, error) {

	var err error

	for i := 1; i <= r.maxRetries; i++ {
		state, err := r.reader.GetSingleWorkerState(ctx, workerID)
		if err == nil {
			return state, nil
		}

		if i < r.maxRetries {
			r.reader.Logger.Warn("getting single worker state failed; retrying...", zap.Int("try", i), zap.Error(err))

			utils.CalculateAndExecuteBackoff(i, r.initialBackoff)
		}
	}

	r.reader.Logger.Error("getting single worker state failed, retry limit reached", zap.String("workerID", workerID), zap.Error(err))
	return sqlc.WorkerMetric{}, err
}

// GetDBCount retrieves the count of databases in the system.
func (r *ReaderPerfectionist) GetDBCount(ctx context.Context) (int, error) {
	var err error

	for i := 1; i <= r.maxRetries; i++ {
		count, err := r.reader.GetDBCount(ctx)
		if err == nil {
			return count, nil
		}

		if i < r.maxRetries {
			r.reader.Logger.Warn("getting db count failed; retrying...", zap.Int("try", i), zap.Error(err))

			utils.CalculateAndExecuteBackoff(i, r.initialBackoff)
		}
	}

	r.reader.Logger.Error("getting db count failed, retry limit reached", zap.Error(err))
	return 0, err
}

// GetDBConnErrors retrieves the database connection errors.
func (r *ReaderPerfectionist) GetDBConnErrors(ctx context.Context) ([]sqlc.DbConnErr, error) {
	var err error

	for i := 1; i <= r.maxRetries; i++ {
		connErrors, err := r.reader.GetDBConnErrors(ctx)
		if err == nil {
			return connErrors, nil
		}

		if i < r.maxRetries {
			r.reader.Logger.Warn("getting db connection errors failed; retrying...", zap.Int("try", i), zap.Error(err))

			utils.CalculateAndExecuteBackoff(i, r.initialBackoff)
		}
	}

	r.reader.Logger.Error("getting db connection errors failed, retry limit reached", zap.Error(err))
	return nil, err
}

// GetFreeMigrationWorker retrieves a free migration worker from the database.
func (r *ReaderPerfectionist) GetFreeMigrationWorker(ctx context.Context) (pgtype.UUID, error) {
	var err error

	for i := 1; i <= r.maxRetries; i++ {
		workerUUID, err := r.reader.GetFreeMigrationWorker(ctx)
		if err == nil {
			return workerUUID, nil
		}

		if i < r.maxRetries {
			r.reader.Logger.Warn("getting available migration worker failed; retrying...", zap.Int("try", i), zap.Error(err))

			utils.CalculateAndExecuteBackoff(i, r.initialBackoff)
		}
	}

	r.reader.Logger.Error("getting available migration worker failed, retry limit reached", zap.Error(err))
	return pgtype.UUID{}, err
}

// GetAllDbInstanceInfo retrieves information about all database instances.
func (r *ReaderPerfectionist) GetAllDbInstanceInfo(ctx context.Context) ([]sqlc.DbInstance, error) {

	var err error

	for i := 1; i <= r.maxRetries; i++ {
		dbInstances, err := r.reader.GetAllDbInstanceInfo(ctx)
		if err == nil {
			return dbInstances, nil
		}

		if i < r.maxRetries {
			r.reader.Logger.Warn("getting info on all db instances failed; retrying...", zap.Int("try", i), zap.Error(err))

			utils.CalculateAndExecuteBackoff(i, r.initialBackoff)
		}
	}

	r.reader.Logger.Error("getting info on all db instances failed, retry limit reached", zap.Error(err))
	return nil, err

}

// GetAllDbMappingInfo retrieves information about all database mappings.
func (r *ReaderPerfectionist) GetAllDbMappingInfo(ctx context.Context) ([]sqlc.DbMapping, error) {

	var err error

	for i := 1; i <= r.maxRetries; i++ {
		dbInstances, err := r.reader.GetAllDbMappingInfo(ctx)
		if err == nil {
			return dbInstances, nil
		}

		if i < r.maxRetries {
			r.reader.Logger.Warn("getting all db mappings failed; retrying...", zap.Int("try", i), zap.Error(err))

			utils.CalculateAndExecuteBackoff(i, r.initialBackoff)
		}
	}

	r.reader.Logger.Error("getting db mappings failed, retry limit reached", zap.Error(err))
	return nil, err

}

// GetDBMappingInfoByUrlFrom retrieves the database mapping information for a specific URL and from a given source.
func (r *ReaderPerfectionist) GetDBMappingInfoByUrlFrom(ctx context.Context, url, from string) (sqlc.DbMapping, error) {

	var err error

	for i := 1; i <= r.maxRetries; i++ {
		mapping, err := r.reader.GetDBMappingInfoByUrlFrom(ctx, url, from)
		if err == nil {
			return mapping, nil
		}

		if i < r.maxRetries {
			r.reader.Logger.Warn("getting all db mappings failed; retrying...", zap.Int("try", i), zap.Error(err))

			utils.CalculateAndExecuteBackoff(i, r.initialBackoff)
		}
	}

	r.reader.Logger.Error("getting db mappings failed, retry limit reached", zap.Error(err))
	return sqlc.DbMapping{}, err

}
