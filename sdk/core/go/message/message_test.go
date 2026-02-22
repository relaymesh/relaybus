package message

import "testing"

func TestMessageZeroValue(t *testing.T) {
	var msg Message
	if msg.ID != "" || msg.Topic != "" {
		t.Fatalf("expected zero value fields")
	}
}
