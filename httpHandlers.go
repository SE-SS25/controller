package main

import (
	"context"
	"controller/components"
	"github.com/google/uuid"
	"go.uber.org/zap"
	"net/http"
)

type InfinityGauntlet struct {
	scheduler  components.Scheduler
	reconciler components.Reconciler
	logger     *zap.Logger
}

func (i *InfinityGauntlet) migrationHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {

		//Get the rangeId from the URL request, fuck request bodies
		rangeId := r.URL.Query().Get("rangeID")

		//generate a tracing id for the context received from the http call and save it in it
		ctx := generateCallTraceId(r.Context())

		err := i.scheduler.RunMigration(ctx, rangeId)
		if err != nil {
			i.logger.Error("could not migrate db-range", zap.Error(err))
			w.Header().Set("Content-Type", "text/plain; charset=utf-8")
			w.WriteHeader(http.StatusInternalServerError)
			_, httpErr := w.Write([]byte(err.Error()))
			if httpErr != nil {
				i.logger.Warn("could not send http response code to client", zap.Error(httpErr), zap.Int("responseCode", http.StatusInternalServerError))
			}
			return
		}

		//Successful http code 204 = NoContent
		w.WriteHeader(http.StatusNoContent)
	}
}

func generateCallTraceId(ctx context.Context) context.Context {
	traceUUID := uuid.New()

	return context.WithValue(ctx, "traceID", traceUUID)
}
