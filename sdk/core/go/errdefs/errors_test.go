package errdefs

import "testing"

func TestIsTransient(t *testing.T) {
	err := TransientError{Err: ErrInvalidEnvelope}
	if !IsTransient(err) {
		t.Fatalf("expected transient")
	}
	if IsTransient(ErrInvalidEnvelope) {
		t.Fatalf("expected non-transient")
	}
}
