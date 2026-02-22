package errdefs

import "errors"

var ErrInvalidEnvelope = errors.New("invalid envelope")

type TransientError struct {
	Err error
}

func (e TransientError) Error() string {
	return e.Err.Error()
}

func (e TransientError) Unwrap() error {
	return e.Err
}

type PermanentError struct {
	Err error
}

func (e PermanentError) Error() string {
	return e.Err.Error()
}

func (e PermanentError) Unwrap() error {
	return e.Err
}

func IsTransient(err error) bool {
	var te TransientError
	return errors.As(err, &te)
}
