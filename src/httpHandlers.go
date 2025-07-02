package main

import (
	"controller/src/utils"
	"encoding/json"
	"go.uber.org/zap"
	"net/http"
	"os"
)

func (c *Controller) RunHttpServer() {
	http.Handle("/migrate", c.migrationHandler())
	http.Handle("/health", c.health())

	var port string
	var err error

	port = os.Getenv("BASE_HTTP_PORT")

	if c.isShadow {
		port, err = utils.SetShadowPort(port)
		if err != nil {
			c.logger.Warn("could not set appropriate http server port for shadow", zap.Error(err))
		}
	}
	httpServeErr := http.ListenAndServe("0.0.0.0"+":"+port, nil)
	if err != nil {
		c.logger.Error("serving http traffic failed", zap.Error(httpServeErr))
		return
	}
	c.logger.Info("Started http server", zap.String("port", port))
}

func (c *Controller) getSystemStateHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {

		if c.isShadow {
			c.logger.Warn("use tried sending a request to the shadow, tell him to stop pwease")
			w.WriteHeader(http.StatusForbidden)
			return
		}

		ctx := utils.GenerateCallTraceId(r.Context())

		migrationInfos, stateErr := c.scheduler.GetSystemState(ctx)
		if stateErr != nil {
			c.logger.Warn("could not get system state for user request", zap.Any("traceId", ctx.Value("traceId")), zap.Error(stateErr))

			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		jsonBytes, parseErr := json.Marshal(migrationInfos)
		if parseErr != nil {
			c.logger.Warn("could not parse migration infos to json", zap.Error(parseErr))
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusOK)
		_, writeErr := w.Write(jsonBytes)
		if writeErr != nil {
			c.logger.Warn("could not write json to http writer", zap.Error(writeErr))
		}
		return
	}
}

// migrationHandler returns an HTTP handler for triggering a database migration for a given range ID.
// If the controller is in shadow mode, responds with HTTP 403 Forbidden.
// Expects the rangeID as a query parameter. Generates a trace ID for the request context.
// Responds with HTTP 204 No Content on success, or HTTP 500 Internal Server Error on failure.
func (c *Controller) migrationHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {

		if c.isShadow {
			w.WriteHeader(http.StatusForbidden)
		}

		//Get the rangeId from the URL request, fuck request bodies
		rangeId := r.URL.Query().Get("rangeID")
		goalUrl := r.URL.Query().Get("goal_url")

		//generate a tracing id for the context received from the http call and save it in it
		ctx := utils.GenerateCallTraceId(r.Context())

		err := c.scheduler.RunMigration(ctx, rangeId, goalUrl)
		if err != nil {
			c.logger.Error("could not migrate db-range", zap.Error(err))
			w.Header().Set("Content-Type", "text/plain; charset=utf-8")
			w.WriteHeader(http.StatusInternalServerError)
			_, httpErr := w.Write([]byte(err.Error()))
			if httpErr != nil {
				c.logger.Warn("could not send http response code to client", zap.Error(httpErr), zap.Int("responseCode", http.StatusInternalServerError))
			}
			return
		}

		//Successful http code 204 = NoContent
		w.WriteHeader(http.StatusNoContent)
	}
}

// health returns an HTTP handler that checks the health of the controller by pinging the database.
// Responds with HTTP 200 if the database is reachable, otherwise responds with HTTP 424 (Failed Dependency).
func (c *Controller) health() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {

		c.logger.Debug("pinging the database")

		err := c.reconciler.PingDB(r.Context())
		if err != nil {
			w.WriteHeader(http.StatusFailedDependency)
			return
		}
		w.WriteHeader(http.StatusOK)
	}
}
