package errors

type RetryLimitReached struct {
	Attr string
	Err  error
}

func (r *RetryLimitReached) Error() string {
	r.Attr = "retry limit was reached: "
	return r.Attr + r.Err.Error()
}

type ControllerCrashed struct {
	Err error
}

func (c *ControllerCrashed) Error() string {
	return c.Err.Error()
}

type WhatTheHelly struct {
	Attr string
	Err  error
}

func (w *WhatTheHelly) Error() string {
	w.Attr = "this error should not be possible: "
	return w.Attr + w.Err.Error()
}
