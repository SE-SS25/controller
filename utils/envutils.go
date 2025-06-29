package utils

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

	intString := os.Getenv(env)

	logger.Debug("got env variable", zap.String("variable", env), zap.String("value", intString))

	retries, err := strconv.Atoi(intString)
	if err != nil {
		logger.Warn("Could not parse from .env, setting default", zap.String("variable", env), zap.Int("default", intDefault), zap.Error(err))
		return intDefault
	}

	return retries
}

func ParseEnvStringWithDefault(env string, defaultValue string, logger *zap.Logger) string {
	envString := os.Getenv(env)

	logger.Debug("got env variable", zap.String("variable", env), zap.String("value", envString))

	if envString == "" {
		return defaultValue
	}

	return envString

}

func SetShadowPort(portString string) (string, error) {
	portInt, err := strconv.Atoi(portString)
	if err != nil {
		return "", err
	}

	portInt++

	return strconv.Itoa(portInt), nil
}
