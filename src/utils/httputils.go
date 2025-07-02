package utils

import (
	"context"
	"github.com/google/uuid"
)

// GenerateCallTraceId generates a new UUID and attaches it to the context under the key "traceID".
// Returns the new context with the trace ID included.
func GenerateCallTraceId(ctx context.Context) context.Context {
	traceUUID := uuid.New()

	return context.WithValue(ctx, "traceID", traceUUID)
}
