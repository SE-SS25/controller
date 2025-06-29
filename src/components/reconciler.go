package components

import (
	"context"
	"controller/src/database"
	sqlc "controller/src/database/sqlc"
	ownErrors "controller/src/errors"
	"controller/utils"
	"fmt"
	"go.uber.org/zap"
	"time"
)

type Reconciler struct {
	logger     *zap.Logger
	reader     *database.Reader
	readerPerf *database.ReaderPerfectionist
	writer     *database.Writer
	writerPerf *database.WriterPerfectionist
}

func NewReconciler(logger *zap.Logger, dbReader *database.Reader, readerPerf *database.ReaderPerfectionist, dbWriter *database.Writer, writerPerf *database.WriterPerfectionist) Reconciler {
	return Reconciler{
		logger:     logger,
		reader:     dbReader,
		readerPerf: readerPerf,
		writer:     dbWriter,
		writerPerf: writerPerf,
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
func (r *Reconciler) Heartbeat(ctx context.Context) {

	heartbeatInterval := utils.ParseEnvDuration("HEARTBEAT_BACKOFF", 5*time.Second, r.logger)

	start := time.Now()

	//We don't make the controller terminate here since its trying to ping the database every second anyway, and if that fails it will shut off
	heartbeatErr := r.writerPerf.Heartbeat(ctx)
	if heartbeatErr != nil {
		r.logger.Error("heartbeat failed", zap.Error(heartbeatErr))
	}

	end := time.Now()

	//calculate the time it took for the last heartbeat to be written to the database and then subtract that from the interval and sleep for the resulting amount of time -> this way the interval should always be the same length
	timeToSleep := heartbeatInterval - (end.Sub(start))

	time.Sleep(timeToSleep)
}

// CheckControllerUp checks if the controller has a valid heartbeat and if not, activates the shadow as the new controller
func (r *Reconciler) CheckControllerUp(ctx context.Context) error {

	timeout := utils.ParseEnvDuration("CHECK_ITER_TIMEOUT", time.Second, r.logger)

	state, err := r.readerPerf.GetControllerState(ctx)
	if err != nil {
		errW := fmt.Errorf("checking if controller is running failed: %w", err)
		return errW
	}

	timeSinceHeartbeat := time.Now().Sub(state.LastHeartbeat.Time)

	if timeSinceHeartbeat > timeout {
		r.logger.Warn("Controller has surpassed heartbeat timeout, activating shadow", zap.Duration("timeout", timeout))
		controllerErr := &ownErrors.ControllerCrashed{
			Err: fmt.Errorf("controller has not heartbeat in valid timeframe, restarting, : %s", timeout),
		}

		return controllerErr
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
		minimumUptime = utils.ParseEnvDuration("MINIMUM_WORKER_UPTIME", 5*time.Second, r.logger)
	}

	workers, err := r.readerPerf.GetAllWorkerState(ctx)
	if err != nil {
		r.logger.Error("error evaluating worker state", zap.Error(err))
		return err
	}

	//TODO I think this part can potentially cause big latency if we backoff with every retry
	//Thats problematic because as long as the function is stuck here, the rest of th workers arent checked
	//Maybe i can execute the remove retries in goroutines?
	//maybe refactor this ugly as hell
	for _, worker := range workers {

		//Delay = time_since_last_heartbeat - specified_heartbeat_frequency
		err = workerHeartbeatOK(worker, timeout, r.logger)
		if err != nil {

			r.logger.Warn("Detected delayed worker heartbeat, trying again", zap.Error(err))

			//Recheck the state of the worker to see if it still recovers, if it doesn't remove it
			workerState, dbErr := r.readerPerf.GetSingleWorkerState(ctx, worker.ID.String())
			if dbErr != nil || workerHeartbeatOK(workerState, timeout, r.logger) != nil {

				r.logger.Warn("worker did not recover or could not be fetched from db; removing...", zap.String("workerId", worker.ID.String()), zap.NamedError("dbErr", err), zap.Bool("heartBeatOk", workerHeartbeatOK(workerState, timeout, r.logger) == nil))

				if removeErr := r.writerPerf.RemoveWorker(worker.ID, ctx); err != nil {
					r.logger.Error("could not remove non-functional worker from table", zap.String("workerId", worker.ID.String()), zap.Error(removeErr))
					continue
				}

			}

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
					return
				}
			}()
		}

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
			if errorCount > 5 {
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
		if rowSum > 5 {
			warnings = append(warnings, fmt.Sprintf("Worker %s (all databases): %d errors", workerID, rowSum))
		}
	}

	// Evaluate columns (per database across all workers)
	for j, dbURL := range dbList {
		colSum := 0
		for i := 0; i < len(workerList); i++ {
			colSum += errorToFrequency[i][j]
		}
		if colSum > 5 {
			warnings = append(warnings, fmt.Sprintf("Database %s (all workers): %d errors", dbURL, colSum))
		}
	}

	// Log final report
	if len(warnings) > 0 {
		r.logger.Warn("high failure rates detected in the last 30 minutes",
			zap.Strings("warnings", warnings))
	} else {
		r.logger.Debug("no high failure rates detected in the last 30 minutes")
	}

	return nil
}

func workerHeartbeatOK(worker sqlc.WorkerMetric, timeout time.Duration, logger *zap.Logger) error {
	uuid := worker.ID

	logger.Debug("Reading data for worker", zap.String("uuid", uuid.String()))

	if worker.LastHeartbeat.Valid == false {
		return fmt.Errorf("heartbeat of worker does not have a valid return from pg: %s", uuid.String())
	}

	timeSinceHeartbeat := time.Now().Sub(worker.LastHeartbeat.Time)

	if timeSinceHeartbeat > timeout {
		delay := timeSinceHeartbeat - timeout
		return fmt.Errorf("delay of worker heartbeat is higher than maximum limit - delay: %v worker %s", delay, uuid.String())
	}

	return nil
}
