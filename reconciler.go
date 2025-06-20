package main

import (
	"context"
	"controller/reader"
	database "controller/sqlc"
	"controller/stringutils"
	"controller/writer"
	"fmt"
	"go.uber.org/zap"
	"os"
	"time"
)

type Reconciler struct {
	logger   *zap.Logger
	dbReader *reader.Reader
	dbWriter *writer.Writer
}

// CheckControllerUp checks if the controller has a valid heartbeat and if not, activates the shadow as the new controller
func (r *Reconciler) CheckControllerUp(ctx context.Context) {

	controllerCheckTimeout := os.Getenv("CONTROLLER_CHECK_TIMEOUT")
	timeout, err := stringutils.ParseEnvDuration(controllerCheckTimeout, r.logger)
	if err != nil {
		return
	}

	for {

		state, err := r.dbReader.GetControllerState(ctx)
		if err != nil {
			errW := fmt.Errorf("checking if controller is running failed: %w", err)
			r.logger.Error("error evaluating controller state", zap.Error(errW)) //TODO Add retry logic; warn then Error
		}

		timeSinceHeartbeat := time.Now().Sub(state.LastHeartbeat.Time)

		if timeSinceHeartbeat > timeout {
			r.logger.Warn("Controller has surpassed heartbeat timeout, activating shadow", zap.Duration("timeout", timeout))

			//TODO
		}

	}

}

// EvaluateWorkerState evaluates if all workers have a valid heartbeat and uptime; if that is not the case the workers are removed from the "workers" table and hence no longer belong to the system
func (r *Reconciler) EvaluateWorkerState(ctx context.Context) {

	iterationTimeout, err := stringutils.ParseEnvDuration(os.Getenv("ITER_TIMEOUT"), r.logger)
	if err != nil {
		return
	}
	timeout, err := stringutils.ParseEnvDuration("HEARTBEAT_TIMEOUT", r.logger)
	if err != nil {
		return
	}

	go func() {
		for {

			state, err := r.dbReader.GetControllerState(ctx)
			if err != nil {
				errW := fmt.Errorf("evaluating worker state failed: %w", err)
				r.logger.Error("error evaluating worker state", zap.Error(errW)) //TODO Add retry logic; warn then Error
			}

			isScaling := state.Scaling

			var minimumUptime time.Duration

			if !isScaling {
				minimumUptime, err = stringutils.ParseEnvDuration(os.Getenv("MINIMUM_WORKER_UPTIME"), r.logger)
				if err != nil {
					return
				}
			}

			workers, err := r.dbReader.GetWorkerState(ctx)
			if err != nil {
				errW := fmt.Errorf("evaluating worker state failed: %w", err)
				r.logger.Error("error evaluating worker state", zap.Error(errW)) //TODO Add retry logic; warn then Error
			}

			for _, worker := range workers {

				//Delay = time_since_last_heartbeat - specified_heartbeat_frequency
				ok, delay := workerHeartbeatOK(worker, timeout, r.logger)

				if !ok {
					r.logger.Warn("Detected delayed worker heartbeat, trying again", zap.String("workerID", worker.Uuid.String()), zap.Duration("delay", delay))

					r.dbWriter.RemoveWorker(ctx, worker.Uuid)
				}

				if !isScaling && worker.Uptime.Microseconds < minimumUptime.Microseconds() {
					r.logger.Warn("Detected worker with unusually low uptime, trying again", zap.String("workerID", worker.Uuid.String()), zap.Duration("retryTimeout", time.Second))

					r.dbWriter.RemoveWorker(ctx, worker.Uuid)
				}

			}

			time.Sleep(iterationTimeout)
		}
	}()

	//TODO implement metrics aggregation here somehow???

}

func workerHeartbeatOK(worker database.Workermetric, timeout time.Duration, logger *zap.Logger) (bool, time.Duration) {
	uuid := worker.Uuid

	logger.Debug("Reading data for worker", zap.String("uuid", uuid.String()))

	timeSinceHeartbeat := time.Now().Sub(worker.LastHeartbeat.Time)

	if timeSinceHeartbeat > timeout {
		delay := timeSinceHeartbeat - timeout
		return false, delay
	}

	return true, 0
}
