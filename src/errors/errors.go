package errors

import "errors"

var (
	ErrRetryLimitReached = errors.New("retry limit was reached")
	ErrControllerCrashed = errors.New("controller crashed")
	ErrWhatTheHelly      = errors.New("this error should not be possible")
)
