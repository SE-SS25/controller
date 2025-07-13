package database

import (
	"context"
	sqlc "controller/src/database/sqlc"
	"fmt"
	guuid "github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/zap"
	"time"
)

// The Reader struct provides methods to read data from the database.
// It uses a pgxpool.Pool for database connections and a zap.Logger for logging.
// All methods are similar to another and the names are self-explanatory.
type Reader struct {
	Pool   *pgxpool.Pool
	Logger *zap.Logger
}

func (r *Reader) Ping(ctx context.Context) error {

	err := r.Pool.Ping(ctx)
	if err != nil {
		return err
	}

	r.Logger.Debug("successfully pinged the database", zap.Time("timestamp", time.Now()))

	return nil
}

// GetAllWorkerState retrieves the state of all workers
// Returns a slice of WorkerMetric and an error if the operation fails.
func (r *Reader) GetAllWorkerState(ctx context.Context) ([]sqlc.WorkerMetric, error) {

	tx, err := r.Pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, fmt.Errorf("beeginning transaction failed: %w", err)
	}

	defer tx.Rollback(ctx)

	q := sqlc.New(tx)
	workers, err := q.GetAllWorkerState(ctx)
	if err != nil {
		return nil, fmt.Errorf("getting all worker states failed: %w", err)
	}

	commitErr := tx.Commit(ctx)
	if commitErr != nil {
		return nil, fmt.Errorf("committing transaction failed: %w", commitErr)
	}

	r.Logger.Debug("successfully got workers state")
	return workers, nil
}

// GetAllMWorkerState retrieves the state of all migration workers
func (r *Reader) GetAllMWorkerState(ctx context.Context) ([]sqlc.MigrationWorker, error) {

	tx, err := r.Pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, fmt.Errorf("beeginning transaction failed: %w", err)
	}

	defer tx.Rollback(ctx)

	q := sqlc.New(tx)
	workers, err := q.GetAllMWorkerState(ctx)
	if err != nil {
		return nil, fmt.Errorf("getting all migration workers failed: %w", err)
	}

	commitErr := tx.Commit(ctx)
	if commitErr != nil {
		return nil, fmt.Errorf("committing transaction failed: %w", commitErr)
	}

	r.Logger.Debug("successfully got migration workers", zap.Int("count", len(workers)))
	return workers, nil

}

// GetSingleWorkerState retrieves the state of a single worker identified by workerID
// Returns the WorkerMetric of the worker and an error if the operation fails.
func (r *Reader) GetSingleWorkerState(ctx context.Context, workerID string) (sqlc.WorkerMetric, error) {

	tx, err := r.Pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return sqlc.WorkerMetric{}, fmt.Errorf("beginning transaction failed: %w", err)
	}

	defer tx.Rollback(ctx)

	parsed, err := guuid.Parse(workerID)
	if err != nil {
		return sqlc.WorkerMetric{}, fmt.Errorf("could not parse uuid")
	}

	q := sqlc.New(tx)
	worker, err := q.GetSingleWorkerState(ctx, pgtype.UUID{
		Bytes: parsed,
		Valid: true,
	})
	if err != nil {
		return sqlc.WorkerMetric{}, fmt.Errorf("getting single worker state failed: %w", err)
	}

	commitErr := tx.Commit(ctx)
	if commitErr != nil {
		return sqlc.WorkerMetric{}, fmt.Errorf("committing transaction failed: %w", commitErr)
	}

	r.Logger.Debug("successfully got workers state")
	return worker, nil
}

// GetControllerState retrieves the current state of the controller
// Returns the ControllerStatus and an error if the operation fails.
func (r *Reader) GetControllerState(ctx context.Context) (sqlc.ControllerStatus, error) {

	tx, err := r.Pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return sqlc.ControllerStatus{}, fmt.Errorf("beginning transaction failed: %w", err)
	}

	defer tx.Rollback(ctx)

	q := sqlc.New(tx)
	state, err := q.GetControllerState(ctx)
	if err != nil {
		return sqlc.ControllerStatus{}, fmt.Errorf("getting controller state failed: %w", err)
	}

	commitErr := tx.Commit(ctx)
	if commitErr != nil {
		return sqlc.ControllerStatus{}, fmt.Errorf("committing transaction failed: %w", commitErr)
	}

	r.Logger.Debug("successfully got controller state")
	return state, nil
}

// GetWorkerCount retrieves the total number of workers
// Returns the count as an int and an error if the operation fails.
func (r *Reader) GetWorkerCount(ctx context.Context) (int, error) {

	tx, err := r.Pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return 0, fmt.Errorf("beginning transaction failed: %w", err)
	}

	defer tx.Rollback(ctx)

	q := sqlc.New(tx)
	count, err := q.GetWorkerCount(ctx)
	if err != nil {
		return 0, fmt.Errorf("getting worker count failed: %w", err)
	}

	commitErr := tx.Commit(ctx)
	if commitErr != nil {
		return 0, fmt.Errorf("committing transaction failed: %w", commitErr)
	}

	r.Logger.Debug("successfully got worker count", zap.Int64("count", count))
	return int(count), nil
}

// GetDBCount retrieves the total number of databases
// Returns the count as an int and an error if the operation fails.
func (r *Reader) GetDBCount(ctx context.Context) (int, error) {

	tx, err := r.Pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return 0, fmt.Errorf("beginning transaction failed: %w", err)
	}

	defer tx.Rollback(ctx)

	q := sqlc.New(tx)
	count, err := q.GetDatabaseCount(ctx)
	if err != nil {
		return 0, fmt.Errorf("getting db count failed: %w", err)
	}

	commitErr := tx.Commit(ctx)
	if commitErr != nil {
		return 0, fmt.Errorf("committing transaction failed: %w", commitErr)
	}

	r.Logger.Debug("successfully got db count", zap.Int64("count", count))
	return int(count), nil
}

// GetDBConnErrors retrieves all sqlc connection error records
// Returns a slice of errors and logs the operation. Returns an error if the operation fails.
func (r *Reader) GetDBConnErrors(ctx context.Context) ([]sqlc.DbConnErr, error) {

	tx, err := r.Pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, fmt.Errorf("beginning transaction failed: %w", err)
	}

	defer tx.Rollback(ctx)

	q := sqlc.New(tx)
	connectionErrors, err := q.GetAllDbConnErrors(ctx)
	if err != nil {
		return nil, fmt.Errorf("getting db_conn_errors failed: %w", err)
	}

	commitErr := tx.Commit(ctx)
	if commitErr != nil {
		return nil, fmt.Errorf("committing transaction failed: %w", commitErr)
	}

	r.Logger.Debug("successfully got db conn errors")
	return connectionErrors, nil
}

// GetFreeMigrationWorker fetches a UUID for a free migration worker
// Returns the worker UUID and an error if the operation fails.
func (r *Reader) GetFreeMigrationWorker(ctx context.Context) (pgtype.UUID, error) {
	tx, err := r.Pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return pgtype.UUID{}, fmt.Errorf("beginning transaction failed: %w", err)
	}

	defer tx.Rollback(ctx)

	q := sqlc.New(tx)
	workerUUID, queryErr := q.GetFreeMigrationWorker(ctx)
	if queryErr != nil {
		return pgtype.UUID{}, fmt.Errorf("getting available migration worker failed: %w", queryErr)
	}

	commitErr := tx.Commit(ctx)
	if commitErr != nil {
		return pgtype.UUID{}, fmt.Errorf("committing transaction failed: %w", commitErr)
	}

	r.Logger.Debug("successfully got available worker", zap.String("workerID", workerUUID.String()))
	return workerUUID, nil
}

// GetAllDbInstanceInfo retrieves information about all database instances
func (r *Reader) GetAllDbInstanceInfo(ctx context.Context) ([]sqlc.DbInstance, error) {

	tx, err := r.Pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, fmt.Errorf("beginning transaction failed: %w", err)
	}

	defer tx.Rollback(ctx)

	q := sqlc.New(tx)
	dbInstances, queryErr := q.GetAllDbInstances(ctx)
	if queryErr != nil {
		return nil, fmt.Errorf("getting data on all db instances failed: %w", queryErr)
	}

	commitErr := tx.Commit(ctx)
	if commitErr != nil {
		return nil, fmt.Errorf("committing transaction failed: %w", commitErr)
	}

	r.Logger.Debug("successfully got info an all db instances", zap.Int("count", len(dbInstances)))
	return dbInstances, nil

}

// GetAllDbMappingInfo retrieves information about all database mappings
func (r *Reader) GetAllDbMappingInfo(ctx context.Context) ([]sqlc.DbMapping, error) {

	tx, err := r.Pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, fmt.Errorf("beginning transaction failed: %w", err)
	}

	defer tx.Rollback(ctx)

	q := sqlc.New(tx)
	mappings, queryErr := q.GetAllDbMappings(ctx)
	if queryErr != nil {
		return nil, fmt.Errorf("getting data on all db mappings failed: %w", queryErr)
	}

	commitErr := tx.Commit(ctx)
	if commitErr != nil {
		return nil, fmt.Errorf("committing transaction failed: %w", commitErr)
	}

	r.Logger.Debug("successfully got info an all db mappings", zap.Int("count", len(mappings)))
	return mappings, nil

}

// GetDBMappingInfoByUrlFrom retrieves a specific database mapping by URL and from attribute
func (r *Reader) GetDBMappingInfoByUrlFrom(ctx context.Context, url, from string) (sqlc.DbMapping, error) {

	tx, err := r.Pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return sqlc.DbMapping{}, fmt.Errorf("beginning transaction failed: %w", err)
	}

	defer tx.Rollback(ctx)

	q := sqlc.New(tx)
	args := sqlc.GetMappingByUrlFromParams{
		Url:  url,
		From: from,
	}
	mapping, queryErr := q.GetMappingByUrlFrom(ctx, args)
	if queryErr != nil {
		return sqlc.DbMapping{}, fmt.Errorf("getting data on specific db mapping failed: %w", queryErr)
	}

	commitErr := tx.Commit(ctx)
	if commitErr != nil {
		return sqlc.DbMapping{}, fmt.Errorf("committing transaction failed: %w", commitErr)
	}

	r.Logger.Debug("successfully got db mapping with attributes", zap.String("from", from), zap.String("url", url))
	return mapping, nil

}
