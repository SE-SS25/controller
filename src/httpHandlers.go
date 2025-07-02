package main

import (
	"context"
	"controller/src/utils"
	"github.com/google/uuid"
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

		//generate a tracing id for the context received from the http call and save it in it
		ctx := generateCallTraceId(r.Context())

		err := c.scheduler.RunMigration(ctx, rangeId)
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

// generateCallTraceId generates a new UUID and attaches it to the context under the key "traceID".
// Returns the new context with the trace ID included.
func generateCallTraceId(ctx context.Context) context.Context {
	traceUUID := uuid.New()

	return context.WithValue(ctx, "traceID", traceUUID)
}
