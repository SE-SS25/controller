package main

import (
	"context"
	"controller/src/components"
	customErr "controller/src/errors"
	"controller/src/utils"
	"errors"
	"go.uber.org/zap"
	"os"
	"time"
)

type Controller struct {
	scheduler  components.Scheduler
	reconciler components.Reconciler
	logger     *zap.Logger
	isShadow   bool
}

// heartbeat periodically sends a heartbeat signal to indicate the controller is alive.
// It calls the reconciler's Heartbeat method and logs a fatal error if the heartbeat fails.
// The function sleeps for the configured heartbeat interval between each heartbeat.
func (c *Controller) heartbeat(ctx context.Context) {
	heartbeatInterval := utils.ParseEnvDuration("HEARTBEAT_BACKOFF", 5*time.Second, c.logger)

	for {
		start := time.Now()

		heartbeatErr := c.reconciler.Heartbeat(ctx)
		if heartbeatErr != nil {
			c.logger.Fatal("heartbeat failed", zap.Error(heartbeatErr))
		}

		end := time.Now()

		timeToSleep := heartbeatInterval - (end.Sub(start))

		time.Sleep(timeToSleep)
	}
}

// checkControllerUp runs a loop while the controller is in shadow mode.
// It periodically checks if the main controller is up by calling the reconciler's CheckControllerUp method.
// If the main controller is detected as crashed, this function takes over as the main controller,
// updates the environment variable and state, and starts the heartbeat loop.
// If another error occurs, it logs a fatal error and exits.
// Sleeps for the configured check interval between checks.
func (c *Controller) checkControllerUp(ctx context.Context) {
	for c.isShadow {

		checkInterval := utils.ParseEnvDuration("CHECK_CONTROLLER_BACKOFF", 3*time.Second, c.logger)

		start := time.Now()

		shadowErr := c.reconciler.CheckControllerUp(ctx)

		if shadowErr != nil {

			if !errors.Is(shadowErr, customErr.ErrControllerCrashed) {
				c.logger.Fatal("shadow reconciliation loop failed", zap.Error(shadowErr))
			} else {
				// If the controller crashed, take over as the shadow
				if setEnvErr := os.Setenv("SHADOW", "false"); setEnvErr != nil {
					c.logger.Warn("could not change `SHADOW` environment variable after taking over as controller")
				}
				c.isShadow = false //Put the shadow in control
				c.heartbeat(ctx)
			}
		}

		end := time.Now()

		timeToSleep := checkInterval - (end.Sub(start))

		time.Sleep(timeToSleep)
	}
}
