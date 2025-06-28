package main

import (
	"context"
	"controller/src/components"
	"controller/src/envutils"
	"github.com/google/uuid"
	"go.uber.org/zap"
	"net/http"
	"os"
)

type Controller struct {
	scheduler  components.Scheduler
	reconciler components.Reconciler
	logger     *zap.Logger
	isShadow   bool
}

func (c *Controller) RunHttpServer() {
	http.Handle("/migrate", c.migrationHandler())
	http.Handle("/health", c.health())

	var port string
	var err error

	port = os.Getenv("BASE_PORT")

	if c.isShadow {
		port, err = envutils.SetShadowPort(port)
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

// health function checks the only thing this component really does, which is accessing the database
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

func generateCallTraceId(ctx context.Context) context.Context {
	traceUUID := uuid.New()

	return context.WithValue(ctx, "traceID", traceUUID)
}
