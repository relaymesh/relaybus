package envelope

import (
	"testing"
	"time"

	"github.com/relaymesh/relaybus/sdk/core/go/message"
)

func TestEncodeDecodeEnvelope(t *testing.T) {
	msg := message.Message{
		ID:            "id",
		Topic:         "alpha",
		Timestamp:     time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
		ContentType:   "text/plain",
		Payload:       []byte("hello"),
		Metadata:      map[string]string{},
		SchemaVersion: "v1",
	}
	data, err := EncodeEnvelope(msg)
	if err != nil {
		t.Fatalf("EncodeEnvelope error: %v", err)
	}
	decoded, err := DecodeEnvelope(data)
	if err != nil {
		t.Fatalf("DecodeEnvelope error: %v", err)
	}
	if decoded.ID != msg.ID {
		t.Fatalf("expected id preserved")
	}
}
