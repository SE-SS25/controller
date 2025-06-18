package main

import (
	"context"
	"controller/reader"
	database "controller/sqlc"
	"controller/stringutils"
	"controller/writer"
	"go.uber.org/zap"
	"os"
	"time"
)

type Scheduler struct {
	logger   *zap.Logger
	dbReader *reader.Reader
	dbWriter *writer.Writer
}

func (s *Scheduler) evaluateWorkerState(ctx context.Context) {

	iterationTimeout, err := stringutils.ParseEnvDuration(os.Getenv("ITER_TIMEOUT"), s.logger)
	if err != nil {
		return
	}
	timeout, err := stringutils.ParseEnvDuration("HEARTBEAT_TIMEOUT", s.logger)
	if err != nil {
		return
	}

	go func() {
		for {

			state := s.dbReader.GetControllerState(ctx)

			var minimumUptime time.Duration

			if !state.Scaling {
				minimumUptime, err = stringutils.ParseEnvDuration(os.Getenv("MINIMUM_WORKER_UPTIME"), s.logger)
				if err != nil {
					return
				}
			}

			workers := s.dbReader.GetWorkerState(ctx)

			for _, worker := range workers {

				if !workerHeartbeatOK(worker, timeout, s.logger) {
					s.dbWriter.RemoveWorker(ctx, worker.Uuid)
				}

				if !state.Scaling && worker.Uptime.Microseconds < minimumUptime.Microseconds() {
					s.logger.Warn("Detected worker with unusually low uptime, trying again", zap.String("workerID", worker.Uuid.String()), zap.Duration("retryTimeout", time.Second))

					//TODO
				}

			}

			time.Sleep(iterationTimeout)
		}
	}()

	if !state.Scaling {
		go func() {

		}()
	}

	//TODO implement metrics aggregation here somehow???

}

func workerHeartbeatOK(worker database.Workermetric, timeout time.Duration, logger *zap.Logger) bool {
	uuid := worker.Uuid

	logger.Debug("Reading data for worker", zap.String("uuid", uuid.String()))

	timeSinceHeartbeat := time.Now().Sub(worker.LastHeartbeat.Time)

	if timeSinceHeartbeat > timeout {
		return false
	}

	return true
}

func workerUptimeOK(worker database.Workermetric) {

	if !workersAreScaling() {

	}
}

func workersAreScaling() {

}
