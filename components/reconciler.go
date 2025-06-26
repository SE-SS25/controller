package components

import (
	"context"
	"controller/database"
	sqlc "controller/database/sqlc"
	"controller/envutils"
	ownErrors "controller/errors"
	"fmt"
	"go.uber.org/zap"
	"os"
	"time"
)

type Reconciler struct {
	logger     *zap.Logger
	readerPerf *database.ReaderPerfectionist
	writerPerf *database.WriterPerfectionist
}

func NewReconciler(logger *zap.Logger, readerPerf *database.ReaderPerfectionist, writerPerf *database.WriterPerfectionist) Reconciler {
	return Reconciler{
		logger:     logger,
		readerPerf: readerPerf,
		writerPerf: writerPerf,
	}
}

// CheckControllerUp checks if the controller has a valid heartbeat and if not, activates the shadow as the new controller
func (r *Reconciler) CheckControllerUp(ctx context.Context) error {

	timeout := envutils.ParseEnvDuration(os.Getenv("CONTROLLER_CHECK_TIMEOUT"), time.Second, r.logger)

	for {

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

	}

}

// EvaluateWorkerState evaluates if all workers have a valid heartbeat and uptime;
// if that is not the case, the workers are removed from the "workers" table and hence no longer belong to the system
// This function should be called in a goroutine to be executed in the background
func (r *Reconciler) EvaluateWorkerState(ctx context.Context, iterationTimeout, timeout time.Duration) error {

	for {

		state, err := r.readerPerf.GetControllerState(ctx)
		if err != nil {
			r.logger.Error("error evaluating worker state", zap.Error(err)) //TODO Add retry logic; warn then Error
		}

		isScaling := state.Scaling

		var minimumUptime time.Duration

		if !isScaling {
			minimumUptime = envutils.ParseEnvDuration(os.Getenv("MINIMUM_WORKER_UPTIME"), 5*time.Second, r.logger)
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

				workerState, dbErr := r.readerPerf.GetSingleWorkerState(ctx, worker.ID.String())
				if dbErr != nil || !isScaling && workerState.Uptime.Microseconds < minimumUptime.Microseconds() {

					r.logger.Warn("worker did not recover or could not be fetched from db; removing...", zap.String("workerId", worker.ID.String()), zap.NamedError("dbErr", err), zap.Bool("uptimeOk", !isScaling && workerState.Uptime.Microseconds < minimumUptime.Microseconds()))

					if removeErr := r.writerPerf.RemoveWorker(worker.ID, ctx); err != nil {
						r.logger.Error("could not remove non-functional worker from table", zap.String("workerId", worker.ID.String()), zap.Error(removeErr))
						continue
					}
				}

				err = r.writerPerf.RemoveWorker(worker.ID, ctx)
			}

		}

		time.Sleep(iterationTimeout)
	}

	//TODO implement metrics aggregation here somehow???

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
