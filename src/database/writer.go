package database

import (
	"context"
	database "controller/src/database/sqlc"
	"errors"
	"fmt"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/zap"
	"time"
)

type Writer struct {
	Logger *zap.Logger
	Pool   *pgxpool.Pool
}

// RemoveWorker removes a worker from the database by UUID within a transaction.
// Logs the operation and returns an error if the operation fails.
func (w *Writer) RemoveWorker(ctx context.Context, uuid pgtype.UUID) error {

	tx, err := w.Pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("beginning transaction: %w", err)
	}

	defer tx.Rollback(ctx)

	q := database.New(tx)
	err = q.DeleteWorker(ctx, uuid)
	if err != nil {
		return fmt.Errorf("removing worker failed: %w", err)
	}

	commitErr := tx.Commit(ctx)
	if commitErr != nil {
		return fmt.Errorf("committing transaction failed: %w", commitErr)
	}

	w.Logger.Debug("successfully removed worker", zap.String("worker_uuid", uuid.String()))
	return nil
}

// AddDatabaseMapping adds a new mapping for a range to a database URL in the mapping table.
// Executes within a transaction and logs the result. Returns an error if the operation fails.
func (w *Writer) AddDatabaseMapping(from, url string, ctx context.Context) error {

	tx, err := w.Pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("beginning transaction failed: %w", err)
	}

	defer tx.Rollback(ctx)

	q := database.New(tx)
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

	commitErr := tx.Commit(ctx)
	if commitErr != nil {
		return fmt.Errorf("committing transaction failed: %w", commitErr)
	}

	w.Logger.Debug("successfully added database mapping", zap.String("from", from), zap.String("url", url))
	return nil
}

// AddMigrationJob takes a range with a given id from the mapping table and transfers it into the migrations table,
// marking it to be migrated by the migration worker specified through the id. Executes within a transaction.
// Returns an error if the operation fails.
func (w *Writer) AddMigrationJob(ctx context.Context, rangeId, url, mWorkerId string) error {

	tx, err := w.Pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("beginning transaction failed: %w", err)
	}

	defer tx.Rollback(ctx)

	q := database.New(tx)
	params := database.CreateMigrationJobParams{
		ID: pgtype.UUID{
			Bytes: [16]byte([]byte(rangeId)),
			Valid: true,
		}, //id of the range
		Url: url, //url of the db instance we want to migrate to
		MWorkerID: pgtype.UUID{
			Bytes: [16]byte([]byte(mWorkerId)),
			Valid: true,
		}, //id of the worker responsible
	}
	err = q.CreateMigrationJob(ctx, params)
	if err != nil {
		return fmt.Errorf("adding migration job to database failed: %w", err)
	}

	commitErr := tx.Commit(ctx)
	if commitErr != nil {
		return fmt.Errorf("committing transaction failed: %w", commitErr)
	}

	w.Logger.Debug("successfully added migration job", zap.String("range_id", rangeId), zap.String("worker_id", mWorkerId))
	return nil
}

// DeleteDbConnErrors deletes database connection error records for a given database URL, worker ID, and failure time.
// Executes within a transaction and logs the result. Returns an error if the operation fails.
func (w *Writer) DeleteDbConnErrors(ctx context.Context, dbUrl pgtype.Text, workerId pgtype.UUID, failTime pgtype.Timestamptz) error {

	tx, err := w.Pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("beginning transaction failed: %w", err)
	}

	defer tx.Rollback(ctx)

	q := database.New(tx)
	params := database.DeleteDBConnErrorParams{
		DbUrl:    dbUrl,
		WorkerID: workerId,
		FailTime: failTime,
	}

	deletionErr := q.DeleteDBConnError(ctx, params)
	if deletionErr != nil {
		return fmt.Errorf("removing outdated dbConnErr from database failed: %w", deletionErr)
	}

	commitErr := tx.Commit(ctx)
	if commitErr != nil {
		return fmt.Errorf("committing transaction failed: %w", commitErr)
	}

	w.Logger.Debug("successfully deleted db connection errors", zap.String("db_url", dbUrl.String), zap.String("worker_id", workerId.String()), zap.Time("fail_time", failTime.Time))
	return nil
}

// Heartbeat updates the controller's heartbeat in the database, carrying over the scaling state.
// Deletes the old heartbeat and creates a new one in a transaction. Returns an error if the operation fails.
func (w *Writer) Heartbeat(ctx context.Context) error {

	w.Logger.Debug("attempting to update heartbeat", zap.Time("timestamp", time.Now()))

	tx, err := w.Pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("beginning transaction failed: %w", err)
	}

	defer tx.Rollback(ctx)

	q := database.New(tx)

	state, queryErr := q.GetControllerState(ctx)
	if queryErr != nil {
		return fmt.Errorf("getting old controller state failed: %w", queryErr)
	}

	delErr := q.DeleteOldControllerHeartbeat(ctx)
	if delErr != nil {
		return fmt.Errorf("deleting old controller state failed: %w", delErr)
	}

	//carry over the old state of whether the controller is currently scaling or not, we do not want to keep this state locally as the controller can crash at any time
	params := database.CreateNewControllerHeartbeatParams{
		Scaling: state.Scaling,
		LastHeartbeat: pgtype.Timestamptz{
			Time:             time.Now(),
			InfinityModifier: 0,
			Valid:            true,
		},
	}

	creationErr := q.CreateNewControllerHeartbeat(ctx, params)
	if creationErr != nil {
		return fmt.Errorf("creating new heartbeat failed: %w", creationErr)
	}

	commitErr := tx.Commit(ctx)
	if commitErr != nil {
		return fmt.Errorf("committing transaction failed: %w", commitErr)
	}

	w.Logger.Debug("successfully updated controller heartbeat", zap.Bool("scaling", state.Scaling), zap.Time("last_heartbeat", params.LastHeartbeat.Time))
	return nil
}

// RegisterController registers a new controller instance. Handles controller takeover or first-time registration,
// updates the heartbeat, and logs the event. Returns an error if the operation fails.
func (w *Writer) RegisterController(ctx context.Context) error {

	tx, err := w.Pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("beginning transaction failed: %w", err)
	}

	defer tx.Rollback(ctx)

	q := database.New(tx)

	state, queryErr := q.GetControllerState(ctx)
	switch {
	case queryErr == nil:
		// Controller takeover: delete the old heartbeat
		if delErr := q.DeleteOldControllerHeartbeat(ctx); delErr != nil {
			return fmt.Errorf("deleting old controller state failed: %w", delErr)
		}
	case errors.Is(queryErr, pgx.ErrNoRows):
		// No previous controller found
		w.Logger.Debug("there has not been a controller before, starting the bloodline")
		state.Scaling = false
	case queryErr != nil:
		// Unexpected error
		return fmt.Errorf("getting controller state failed, but err was not 'no rows': %w", queryErr)
	}

	//carry over the old state of whether the controller is currently scaling or not, we do not want to keep this state locally as the controller can crash at any time
	params := database.CreateNewControllerHeartbeatParams{
		Scaling: state.Scaling,
		LastHeartbeat: pgtype.Timestamptz{
			Time:             time.Now(),
			InfinityModifier: 0,
			Valid:            true,
		},
	}

	creationErr := q.CreateNewControllerHeartbeat(ctx, params)
	if creationErr != nil {
		return fmt.Errorf("creating new heartbeat failed: %w", creationErr)
	}

	commitErr := tx.Commit(ctx)
	if commitErr != nil {
		return fmt.Errorf("committing transaction failed: %w", commitErr)
	}

	w.Logger.Debug("successfully updated controller heartbeat for new controller", zap.Bool("scaling", state.Scaling), zap.Time("last_heartbeat", params.LastHeartbeat.Time))
	return nil

}
