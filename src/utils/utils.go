package utils

import (
	"controller/src/docker"
	"controller/src/errors"
	"math"
	"strconv"
	"time"
)

// CalculateAndExecuteBackoff calculates the backoff duration based on the iteration number and initial backoff duration.
func CalculateAndExecuteBackoff(iteration int, initBackoff time.Duration) {
	backoffInMs := float64(initBackoff.Milliseconds())
	backoffAsFloat := math.Pow(backoffInMs, float64(iteration))

	backoff := time.Duration(backoffAsFloat) * time.Millisecond

	time.Sleep(backoff)
}

// SetShadowPort increments the provided port string by 1 and returns the new port as a string.
// It returns an error if the port string cannot be converted to an integer.
// It is used to set a shadow port for a container, ensuring that the port is unique and does not conflict with existing ports.
func SetShadowPort(portString string) (string, error) {
	portInt, err := strconv.Atoi(portString)
	if err != nil {
		return "", err
	}

	portInt++

	return strconv.Itoa(portInt), nil
}

// ChanWihTimeout waits for a response from the CreateRequest's ResponseChan.
func ChanWihTimeout(cr docker.CreateRequest) error {
	select {
	case resp := <-cr.ResponseChan:
		return resp
	case <-time.After(5 * time.Second):
		return errors.ErrCreateTimeout
	}
}
