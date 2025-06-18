package main

import (
	"context"
	"controller/reader"
	"controller/stringutils"
	"controller/writer"
	"go.uber.org/zap"
	"os"
	"time"
)

type Reconciler struct {
	logger   *zap.Logger
	dbReader *reader.Reader
	dbWriter *writer.Writer
}

func (r Reconciler) CheckControllerUp(ctx context.Context) {

	controllerCheckTimeout := os.Getenv("CONTROLLER_CHECK_TIMEOUT")
	timeout, err := stringutils.ParseEnvDuration(controllerCheckTimeout, r.logger)
	if err != nil {
		return
	}

	for {

		state := r.dbReader.GetControllerState(ctx)

		timeSinceHeartbeat := time.Now().Sub(state.LastHeartbeat.Time)

		if timeSinceHeartbeat > timeout {
			r.logger.Warn("Controller has surpassed heartbeat timeout, activating shadow", zap.Duration("timeout", timeout))

			//TODO
		}

	}

}
