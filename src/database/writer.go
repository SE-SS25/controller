package database

import (
	"context"
	database "controller/src/database/sqlc"
	oe "controller/src/errors"
	"controller/src/utils"
	"errors"
	"fmt"
	"github.com/google/uuid"
	guuid "github.com/google/uuid"
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
func (w *Writer) RemoveWorker(ctx context.Context, uuid pgtype.UUID) oe.DbError {

	tx, err := w.Pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return oe.DbError{Err: fmt.Errorf("beginning transaction: %w", err), Reconcilable: true}
	}

	defer tx.Rollback(ctx)

	q := database.New(tx)
	execRes, execErr := q.DeleteWorker(ctx, uuid)
	if oeErr := utils.Must(execRes, execErr); oeErr.Err != nil {
		return oeErr
	}

	commitErr := tx.Commit(ctx)
	if commitErr != nil {
		return oe.DbError{Err: fmt.Errorf("committing transaction failed: %w", commitErr), Reconcilable: true}
	}

	w.Logger.Debug("successfully removed worker", zap.String("worker_uuid", uuid.String()))
	return oe.DbError{Err: nil}
}

func (w *Writer) AddMigrationWorker(ctx context.Context, uuid, from, to string) oe.DbError {

	tx, err := w.Pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return oe.DbError{Err: fmt.Errorf("beginning transaction: %w", err), Reconcilable: true}
	}

	defer tx.Rollback(ctx)

	parsed, err := guuid.Parse(uuid)
	if err != nil {
		return oe.DbError{Err: fmt.Errorf("could not parse uuid"), Reconcilable: false}
	}

	q := database.New(tx)
	args := database.AddMigrationWorkerParams{
		ID: pgtype.UUID{
			Bytes: parsed,
			Valid: true,
		},
		LastHeartbeat: pgtype.Timestamptz{
			Time:             time.Now(),
			InfinityModifier: 0,
			Valid:            true,
		},
		Uptime: pgtype.Interval{
			Microseconds: 0,
			Days:         0,
			Months:       0,
			Valid:        true,
		},
		WorkingOnFrom: pgtype.Text{
			String: from,
			Valid:  true,
		},
		WorkingOnTo: pgtype.Text{
			String: to,
			Valid:  true,
		},
	}
	execRes, execErr := q.AddMigrationWorker(ctx, args)
	if oeErr := utils.Must(execRes, execErr); oeErr.Err != nil {
		return oeErr
	}
	w.Logger.Debug("changed rows", zap.Int64("count", execRes.RowsAffected()))

	commitErr := tx.Commit(ctx)
	if commitErr != nil {
		return oe.DbError{Err: fmt.Errorf("committing transaction failed: %w", commitErr), Reconcilable: true}
	}

	select {}

	w.Logger.Debug("successfully added migration worker", zap.String("worker_uuid", uuid))
	return oe.DbError{Err: nil}

}

func (w *Writer) RemoveMigrationWorker(ctx context.Context, uuid string) oe.DbError {

	tx, err := w.Pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return oe.DbError{Err: fmt.Errorf("beginning transaction: %w", err), Reconcilable: true}
	}

	defer tx.Rollback(ctx)

	parsed, err := guuid.Parse(uuid)
	if err != nil {
		return oe.DbError{Err: fmt.Errorf("could not parse uuid"), Reconcilable: false}
	}

	q := database.New(tx)
	execRes, execErr := q.DeleteMigrationWorker(ctx, pgtype.UUID{
		Bytes: parsed,
		Valid: true,
	})
	if oeErr := utils.Must(execRes, execErr); oeErr.Err != nil {
		return oeErr
	}

	commitErr := tx.Commit(ctx)
	if commitErr != nil {
		return oe.DbError{Err: fmt.Errorf("committing transaction failed: %w", commitErr), Reconcilable: true}
	}

	w.Logger.Debug("successfully removed migration worker", zap.String("worker_uuid", uuid))
	return oe.DbError{Err: nil}

}

// AddDatabaseMapping adds a new mapping for a range to a database URL in the mapping table.
// Executes within a transaction and logs the result. Returns an error if the operation fails.
func (w *Writer) AddDatabaseMapping(from, url string, ctx context.Context) oe.DbError {

	tx, err := w.Pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return oe.DbError{Err: fmt.Errorf("beginning transaction: %w", err), Reconcilable: true}
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
	execRes, execErr := q.CreateMapping(ctx, params)
	if oeErr := utils.Must(execRes, execErr); oeErr.Err != nil {
		return oeErr
	}

	commitErr := tx.Commit(ctx)
	if commitErr != nil {
		return oe.DbError{Err: fmt.Errorf("committing transaction failed: %w", commitErr), Reconcilable: true}
	}

	w.Logger.Debug("successfully added database mapping", zap.String("from", from), zap.String("url", url))
	return oe.DbError{Err: nil}
}

type MigrationJobAddReq struct {
	From, To, Url, MWorkerId string
}

// AddMigrationJob takes a range with a given id from the mapping table and transfers it into the migrations table,
// marking it to be migrated by the migration worker specified through the id. Executes within a transaction.
// Returns an error if the operation fails.
func (w *Writer) AddMigrationJob(ctx context.Context, addReq MigrationJobAddReq) oe.DbError {

	tx, err := w.Pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return oe.DbError{Err: fmt.Errorf("beginning transaction: %w", err), Reconcilable: true}
	}

	defer tx.Rollback(ctx)

	parsed, err := guuid.Parse(addReq.MWorkerId)
	if err != nil {
		return oe.DbError{Err: fmt.Errorf("could not parse uuid"), Reconcilable: false}
	}

	q := database.New(tx)
	params := database.CreateMigrationJobParams{
		Url:  addReq.Url,
		From: addReq.From,
		To:   addReq.To,
		MWorkerID: pgtype.UUID{
			Bytes: parsed,
			Valid: true,
		},
	}
	execRes, execErr := q.CreateMigrationJob(ctx, params)
	if oeErr := utils.Must(execRes, execErr); oeErr.Err != nil {
		return oeErr
	}

	commitErr := tx.Commit(ctx)
	if commitErr != nil {
		return oe.DbError{Err: fmt.Errorf("committing transaction failed: %w", commitErr), Reconcilable: true}
	}

	w.Logger.Info("successfully added migration job", zap.String("from", addReq.From), zap.String("to", addReq.To), zap.String("worker_id", addReq.MWorkerId))
	return oe.DbError{Err: nil}
}

// DeleteDbConnErrors deletes database connection error records for a given database URL, worker ID, and failure time.
// Executes within a transaction and logs the result. Returns an error if the operation fails.
func (w *Writer) DeleteDbConnErrors(ctx context.Context, dbUrl pgtype.Text, workerId pgtype.UUID, failTime pgtype.Timestamptz) oe.DbError {

	tx, err := w.Pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return oe.DbError{Err: fmt.Errorf("beginning transaction: %w", err), Reconcilable: true}
	}

	defer tx.Rollback(ctx)

	q := database.New(tx)
	params := database.DeleteDBConnErrorParams{
		DbUrl:    dbUrl,
		WorkerID: workerId,
		FailTime: failTime,
	}

	execRes, execErr := q.DeleteDBConnError(ctx, params)
	if oeErr := utils.Must(execRes, execErr); oeErr.Err != nil {
		return oeErr
	}

	commitErr := tx.Commit(ctx)
	if commitErr != nil {
		return oe.DbError{Err: fmt.Errorf("committing transaction failed: %w", commitErr), Reconcilable: true}
	}

	w.Logger.Debug("successfully deleted db connection errors", zap.String("db_url", dbUrl.String), zap.String("worker_id", workerId.String()), zap.Time("fail_time", failTime.Time))
	return oe.DbError{Err: nil}
}

// Heartbeat updates the controller's heartbeat in the database, carrying over the scaling state.
// Deletes the old heartbeat and creates a new one in a transaction. Returns an error if the operation fails.
func (w *Writer) Heartbeat(ctx context.Context) oe.DbError {

	w.Logger.Debug("attempting to update heartbeat", zap.Time("timestamp", time.Now()))

	tx, err := w.Pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return oe.DbError{Err: fmt.Errorf("beginning transaction: %w", err), Reconcilable: true}
	}

	defer tx.Rollback(ctx)

	q := database.New(tx)

	state, queryErr := q.GetControllerState(ctx)
	if queryErr != nil {
		return oe.DbError{Err: fmt.Errorf("getting old controller state failed: %w", queryErr), Reconcilable: true}
	}

	execRes, execErr := q.DeleteOldControllerHeartbeat(ctx)
	if oeErr := utils.Must(execRes, execErr); oeErr.Err != nil {
		return oeErr
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

	execRes, execErr = q.CreateNewControllerHeartbeat(ctx, params)
	if oeErr := utils.Must(execRes, execErr); oeErr.Err != nil {
		return oeErr
	}

	commitErr := tx.Commit(ctx)
	if commitErr != nil {
		return oe.DbError{Err: fmt.Errorf("committing transaction failed: %w", commitErr), Reconcilable: true}
	}

	w.Logger.Debug("successfully updated controller heartbeat", zap.Bool("scaling", state.Scaling), zap.Time("last_heartbeat", params.LastHeartbeat.Time))
	return oe.DbError{Err: nil}
}

// RegisterController registers a new controller instance. Handles controller takeover or first-time registration,
// updates the heartbeat, and logs the event. Returns an error if the operation fails.
func (w *Writer) RegisterController(ctx context.Context) oe.DbError {

	tx, err := w.Pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return oe.DbError{Err: fmt.Errorf("beginning transaction: %w", err), Reconcilable: true}
	}

	defer tx.Rollback(ctx)

	q := database.New(tx)

	state, queryErr := q.GetControllerState(ctx)
	switch {
	case queryErr == nil:
		// Controller takeover: delete the old heartbeat
		execRes, execErr := q.DeleteOldControllerHeartbeat(ctx)
		if oeErr := utils.Must(execRes, execErr); oeErr.Err != nil {
			return oeErr
		}

	case errors.Is(queryErr, pgx.ErrNoRows):
		// No previous controller found
		w.Logger.Debug("there has not been a controller before, starting the bloodline")
		state.Scaling = false
	case queryErr != nil:
		// Unexpected error
		return oe.DbError{Err: fmt.Errorf("getting controller state failed, but err was not 'no rows': %w", queryErr), Reconcilable: true}
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

	execRes, execErr := q.CreateNewControllerHeartbeat(ctx, params)
	if oeErr := utils.Must(execRes, execErr); oeErr.Err != nil {
		return oeErr
	}

	commitErr := tx.Commit(ctx)
	if commitErr != nil {
		return oe.DbError{Err: fmt.Errorf("committing transaction failed: %w", commitErr), Reconcilable: true}
	}

	w.Logger.Debug("successfully updated controller heartbeat for new controller", zap.Bool("scaling", state.Scaling), zap.Time("last_heartbeat", params.LastHeartbeat.Time))
	return oe.DbError{Err: nil}

}
