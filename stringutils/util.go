package stringutils

import (
	"go.uber.org/zap"
	"os"
	"time"
)

func ParseEnvDuration(env string, logger *zap.Logger) (time.Duration, error) {

	timeout := os.Getenv(env)

	duration, err := time.ParseDuration(timeout)
	if err != nil {
		logger.Error("Could not parse from .env", zap.String("variable", env), zap.Error(err))
		return 0, err
	}

	return duration, nil
}
