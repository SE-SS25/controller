package envutils

import (
	"go.uber.org/zap"
	"os"
	"strconv"
	"time"
)

func ParseEnvDuration(env string, durationDefault time.Duration, logger *zap.Logger) time.Duration {

	timeout := os.Getenv(env)

	duration, err := time.ParseDuration(timeout)
	if err != nil {
		logger.Warn("Could not parse from .env, setting default", zap.String("variable", env), zap.Duration("default", durationDefault), zap.Error(err))
		return durationDefault
	}

	return duration
}

func ParseEnvInt(env string, intDefault int, logger *zap.Logger) int {

	retriesString := os.Getenv(env)

	retries, err := strconv.Atoi(retriesString)
	if err != nil {
		logger.Warn("Could not parse from .env, setting default", zap.String("variable", env), zap.Int("default", intDefault), zap.Error(err))
		return intDefault
	}

	return retries
}
