package utils

import (
	"math"
	"time"
)

func CalculateAndExecuteBackoff(iteration int, initBackoff time.Duration) {
	backoffInMs := float64(initBackoff.Milliseconds())
	backoffAsFloat := math.Pow(backoffInMs, float64(iteration))

	backoff := time.Duration(backoffAsFloat) * time.Millisecond

	time.Sleep(backoff)
}
