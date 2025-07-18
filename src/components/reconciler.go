package components

import (
	"context"
	"controller/src/database"
	"controller/src/docker"
	ownErrors "controller/src/errors"
	"fmt"
	"github.com/jackc/pgx/v5/pgtype"
	goutils "github.com/linusgith/goutils/pkg/env_utils"
	"go.uber.org/zap"
	"time"
)

// Reconciler handles all tasks concerning the health of the overall system.
// Meaning it checks for controller, worker, migration_worker, monitor health and reconciles when there is a failure
type Reconciler struct {
	logger     *zap.Logger
	reader     *database.Reader
	readerPerf *database.ReaderPerfectionist
	writer     *database.Writer
	writerPerf *database.WriterPerfectionist
	dInterface docker.DInterface
}

func NewReconciler(logger *zap.Logger, dbReader *database.Reader, readerPerf *database.ReaderPerfectionist, dbWriter *database.Writer, writerPerf *database.WriterPerfectionist, dInterface docker.DInterface) Reconciler {
	return Reconciler{
		logger:     logger,
		reader:     dbReader,
		readerPerf: readerPerf,
		writer:     dbWriter,
		writerPerf: writerPerf,
		dInterface: dInterface,
	}
}

func (r *Reconciler) PingDB(ctx context.Context) error {

	err := r.readerPerf.Ping(ctx)
	if err != nil {
		//We kill the controller so the shadow can step in
		r.logger.Fatal("pinging the database failed; deactivating as leader", zap.Error(err))
	}

	return nil
}

func (r *Reconciler) Heartbeat(ctx context.Context) error {

	//We don't make the controller terminate here since its trying to ping the database every second anyway, and if that fails it will shut off
	heartbeatErr := r.writerPerf.Heartbeat(ctx)
	if heartbeatErr != nil {
		return heartbeatErr
	}

	return nil
}

func (r *Reconciler) RegisterController(ctx context.Context) error {

	if err := r.writerPerf.RegisterController(ctx); err != nil {
		return err
	}

	return nil

}

// CheckControllerUp checks if the controller has a valid heartbeat and if not, activates the shadow as the new controller
func (r *Reconciler) CheckControllerUp(ctx context.Context) error {

	timeout := goutils.Log().ParseEnvDurationDefault("CONTROLLER_HEARTBEAT_TIMEOUT", 10*time.Second, r.logger)

	state, err := r.readerPerf.GetControllerState(ctx)
	if err != nil {
		errW := fmt.Errorf("checking if controller is running failed: %w", err)
		return errW
	}

	timeSinceHeartbeat := time.Now().Sub(state.LastHeartbeat.Time)

	r.logger.Debug("time since last heartbeat from controller", zap.Float64("seconds", timeSinceHeartbeat.Seconds()))

	if timeSinceHeartbeat > timeout {
		r.logger.Warn("Controller has surpassed heartbeat timeout, activating shadow", zap.Duration("timeout", timeout))

		return ownErrors.ErrControllerCrashed
	}

	return nil

}

// EvaluateWorkerState evaluates if all workers have a valid heartbeat and uptime;
// if that is not the case, the workers are removed from the "workers" table and hence no longer belong to the system
// This function should be called in a goroutine to be executed in the background
func (r *Reconciler) EvaluateWorkerState(ctx context.Context, timeout time.Duration) error {

	state, err := r.readerPerf.GetControllerState(ctx)
	if err != nil {
		return err
	}

	isScaling := state.Scaling

	var minimumUptime time.Duration

	if !isScaling {
		minimumUptime = goutils.Log().ParseEnvDurationDefault("MINIMUM_WORKER_UPTIME", 5*time.Second, r.logger)
	}

	workers, err := r.readerPerf.GetAllWorkerState(ctx)
	if err != nil {
		r.logger.Error("error evaluating worker state", zap.Error(err))
		return err
	}

	for _, worker := range workers {

		r.logger.Debug("Reading data for worker", zap.String("uuid", worker.ID.String()))

		//Delay = time_since_last_heartbeat - specified_heartbeat_frequency
		err = workerHeartbeatOK(worker.LastHeartbeat, timeout)
		if err != nil {

			go func() {
				r.logger.Warn("Detected delayed worker heartbeat, trying again", zap.Error(err), zap.String("workerId", worker.ID.String()))

				//Recheck the state of the worker to see if it still recovers, if it doesn't remove it
				workerState, dbErr := r.readerPerf.GetSingleWorkerState(ctx, worker.ID.String())
				if dbErr != nil || workerHeartbeatOK(workerState.LastHeartbeat, timeout) != nil {

					r.logger.Warn("worker did not recover or could not be fetched from db; removing...", zap.String("workerId", worker.ID.String()), zap.NamedError("dbErr", err), zap.Bool("heartBeatOk", workerHeartbeatOK(worker.LastHeartbeat, timeout) == nil))

					if removeErr := r.writerPerf.RemoveWorker(worker.ID, ctx); err != nil {
						r.logger.Error("could not remove non-functional worker from table", zap.String("workerId", worker.ID.String()), zap.Error(removeErr))
					}

				}
			}()

		}

		if !isScaling && worker.Uptime.Microseconds < minimumUptime.Microseconds() {

			r.logger.Warn("Detected worker with unusually low uptime, trying again", zap.String("workerID", worker.ID.String()), zap.Duration("retryTimeout", time.Second))

			go func() {
				workerState, dbErr := r.readerPerf.GetSingleWorkerState(ctx, worker.ID.String())
				if dbErr != nil || workerState.Uptime.Microseconds < minimumUptime.Microseconds() {

					r.logger.Warn("worker did not recover or could not be fetched from db; removing...", zap.String("workerId", worker.ID.String()), zap.NamedError("dbErr", err), zap.Bool("uptimeOk", !isScaling && workerState.Uptime.Microseconds < minimumUptime.Microseconds()))

					if removeErr := r.writerPerf.RemoveWorker(worker.ID, ctx); err != nil {
						r.logger.Error("could not remove non-functional worker from table", zap.String("workerId", worker.ID.String()), zap.Error(removeErr))
						return
					}
				}

				if removeErr := r.writerPerf.RemoveWorker(worker.ID, ctx); err != nil {
					r.logger.Error("could not remove non-functional worker from table", zap.String("workerId", worker.ID.String()), zap.Error(removeErr))
				}
			}()
		}

	}

	return nil

}

// EvaluateMigrationWorkerState evaluates the state of all migration workers in the system.
// It checks if the workers have a valid heartbeat and uptime, and removes them from the database if they do not.
// This function should be called in a goroutine to be executed in the background
func (r *Reconciler) EvaluateMigrationWorkerState(ctx context.Context) error {

	migrationWorkerState, err := r.readerPerf.GetAllMWorkerState(ctx)
	if err != nil {
		r.logger.Error("error getting the migration worker state")
		return err
	}

	maxAgeHeartbeat := goutils.Log().ParseEnvDurationDefault("WORKER_HEARTBEAT_TIMEOUT", 10*time.Second, r.logger)

	workersPresent := false
	for _, worker := range migrationWorkerState {

		workersPresent = true

		r.logger.Debug("time and migration worker heartbeat", zap.String("workerId", worker.ID.String()), zap.Time("current", time.Now()), zap.Time("heartbeat", worker.LastHeartbeat.Time))

		err = workerHeartbeatOK(worker.LastHeartbeat, maxAgeHeartbeat)
		if err != nil {
			r.logger.Warn("heartbeat for migration worker was not ok, removing from the database", zap.String("workerId", worker.ID.String()))

			err = r.writerPerf.RemoveMWorkerAndJobs(ctx, worker.ID.String())
			if err != nil {
				r.logger.Error("could not remove migration worker from the table", zap.Error(err))
			}

		}

	}

	if !workersPresent {
		r.logger.Debug("there are currently no migration workers running")
	}

	return nil
}

// CheckFailureRate queries all rows from the corresponding table in the database and runs some simple data aggregation to determine whether there is an unusually high failure rate in the last half hour (this goes for dbs, workers or db-worker-relationships)
func (r *Reconciler) CheckFailureRate(ctx context.Context) error {
	now := time.Now()

	r.logger.Debug("checking if there are unusually high failure rates in the last 30 minutes")

	connErrorStructList, err := r.readerPerf.GetDBConnErrors(ctx)
	if err != nil {
		return err
	}

	// Maps to track unique workers and databases
	workerToIndex := make(map[string]int)
	dbToIndex := make(map[string]int)
	workerList := []string{}
	dbList := []string{}

	// First pass: collect unique workers and databases from recent errors and delete old ones
	for _, connError := range connErrorStructList {
		failureTime := connError.FailTime.Time

		// Delete errors older than 30 minutes
		if !failureTime.After(now.Add(-30 * time.Minute)) {
			if err := r.writerPerf.DeleteDBConnErrors(ctx, connError.DbUrl, connError.WorkerID, connError.FailTime); err != nil {
				r.logger.Error("failed to delete old connection error", zap.Error(err))

				return err
			}
			continue
		}

		workerID := connError.WorkerID.String()
		dbURL := connError.DbUrl.String

		// Add worker if not seen before
		if _, exists := workerToIndex[workerID]; !exists {
			workerToIndex[workerID] = len(workerList)
			workerList = append(workerList, workerID)
		}

		// Add database if not seen before
		if _, exists := dbToIndex[dbURL]; !exists {
			dbToIndex[dbURL] = len(dbList)
			dbList = append(dbList, dbURL)
		}
	}

	// Initialize 2D slice with zeros
	errorToFrequency := make([][]int, len(workerList))
	for i := range errorToFrequency {
		errorToFrequency[i] = make([]int, len(dbList))
	}

	// Second pass: populate the 2D matrix with error counts
	for _, connError := range connErrorStructList {
		failureTime := connError.FailTime.Time

		// Skip errors that are not in the last 30 minutes
		if !failureTime.After(now.Add(-30 * time.Minute)) {
			continue
		}

		workerID := connError.WorkerID.String()
		dbURL := connError.DbUrl.String

		workerIdx := workerToIndex[workerID]
		dbIdx := dbToIndex[dbURL]

		errorToFrequency[workerIdx][dbIdx]++
	}

	// Collect all warnings for final report
	var warnings []string

	// Evaluate individual cells (worker-database combinations)
	for i, workerID := range workerList {
		for j, dbURL := range dbList {
			errorCount := errorToFrequency[i][j]
			if errorCount > 3 {
				warnings = append(warnings, fmt.Sprintf("Worker %s + Database %s: %d errors", workerID, dbURL, errorCount))
			}
		}
	}

	// Evaluate rows (per worker across all databases)
	for i, workerID := range workerList {
		rowSum := 0
		for j := 0; j < len(dbList); j++ {
			rowSum += errorToFrequency[i][j]
		}
		if rowSum > 3 {
			warnings = append(warnings, fmt.Sprintf("Worker %s (all databases): %d errors", workerID, rowSum))
		}
	}

	// Evaluate columns (per database across all workers)
	for j, dbURL := range dbList {
		colSum := 0
		for i := 0; i < len(workerList); i++ {
			colSum += errorToFrequency[i][j]
		}
		if colSum > 3 {
			warnings = append(warnings, fmt.Sprintf("Database %s (all workers): %d errors", dbURL, colSum))
		}
	}

	// Log final report
	if len(warnings) > 0 {
		r.logger.Warn("high failure rates detected in the last 30 minutes",
			zap.Strings("warnings", warnings))
	} else {
		r.logger.Info("no high failure rates detected in the last 30 minutes")
	}

	return nil
}

// workerHeartbeatOK checks if a worker's last heartbeat is valid and within the allowed timeout.
// Returns an error if the heartbeat is invalid or delayed beyond the timeout, otherwise returns nil.
func workerHeartbeatOK(heartbeat pgtype.Timestamptz, timeout time.Duration) error {

	if heartbeat.Valid == false {
		return fmt.Errorf("heartbeat of worker does not have a valid return from pg")
	}

	timeSinceHeartbeat := time.Now().Sub(heartbeat.Time)

	if timeSinceHeartbeat > timeout {
		delay := timeSinceHeartbeat - timeout
		return fmt.Errorf("delay of worker heartbeat is higher than maximum limit - delay: %v", delay)
	}

	return nil
}
