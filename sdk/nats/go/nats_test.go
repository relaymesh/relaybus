package natsadapter_test

import (
	"context"
	"testing"
	"time"

	"github.com/relaymesh/relaybus/sdk/core/go/envelope"
	"github.com/relaymesh/relaybus/sdk/core/go/message"

	natsadapter "github.com/relaymesh/relaybus/sdk/nats/go"
)

type fakeConn struct {
	subject string
	data    []byte
}

func (f *fakeConn) Publish(subj string, data []byte) error {
	f.subject = subj
	f.data = data
	return nil
}

func (f *fakeConn) Close() {}

func TestPublishUsesSubjectPrefix(t *testing.T) {
	conn := &fakeConn{}
	pub, err := natsadapter.NewPublisher(natsadapter.Config{Conn: conn, SubjectPrefix: "events"})
	if err != nil {
		t.Fatalf("NewPublisher error: %v", err)
	}

	msg := message.Message{
		ID:            "id",
		Topic:         "alpha",
		Timestamp:     time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
		ContentType:   "text/plain",
		Payload:       []byte("hi"),
		Metadata:      map[string]string{},
		SchemaVersion: "v1",
	}
	if err := pub.Publish(context.Background(), "alpha", msg); err != nil {
		t.Fatalf("Publish error: %v", err)
	}

	if conn.subject != "events.alpha" {
		t.Fatalf("expected subject events.alpha, got %s", conn.subject)
	}
	decoded, err := envelope.DecodeEnvelope(conn.data)
	if err != nil {
		t.Fatalf("decode error: %v", err)
	}
	if decoded.ID != "id" {
		t.Fatalf("expected id preserved")
	}
}
