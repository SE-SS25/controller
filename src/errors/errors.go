package errors

import (
	"errors"
	"fmt"
)

var (
	ErrRetryLimitReached = errors.New("retry limit was reached")
	ErrControllerCrashed = errors.New("controller crashed")
	ErrWhatTheHelly      = errors.New("this error should not be possible")
	ErrCreateTimeout     = errors.New("request for container creation timed out")
)

type DbError struct {
	Err          error
	Reconcilable bool
}

func (d DbError) Error() string {
	if d.Reconcilable {
		return fmt.Sprintf(d.Err.Error() + " - is reconscilable")
	}

	return fmt.Sprintf(d.Err.Error() + " - is not reconscilable")
}
