package utils

import (
	"math"
	"strconv"
	"time"
)

func CalculateAndExecuteBackoff(iteration int, initBackoff time.Duration) {
	backoffInMs := float64(initBackoff.Milliseconds())
	backoffAsFloat := math.Pow(backoffInMs, float64(iteration))

	backoff := time.Duration(backoffAsFloat) * time.Millisecond

	time.Sleep(backoff)
}

func SetShadowPort(portString string) (string, error) {
	portInt, err := strconv.Atoi(portString)
	if err != nil {
		return "", err
	}

	portInt++

	return strconv.Itoa(portInt), nil
}
