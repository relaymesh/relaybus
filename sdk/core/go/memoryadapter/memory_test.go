package memoryadapter

import (
	"context"
	"testing"
	"time"

	"github.com/relaymesh/relaybus/sdk/core/go/message"
)

func TestPublishStoresMessage(t *testing.T) {
	pub, err := NewPublisher(Config{})
	if err != nil {
		t.Fatalf("NewPublisher error: %v", err)
	}

	msg := message.Message{ID: "id", Topic: "t", Timestamp: time.Now(), ContentType: "text/plain", Metadata: map[string]string{}, SchemaVersion: "v1"}
	if err := pub.Publish(context.Background(), "t", msg); err != nil {
		t.Fatalf("Publish error: %v", err)
	}

	if got := pub.Messages(); len(got) != 1 {
		t.Fatalf("expected 1 message, got %d", len(got))
	}
}

func TestPublishUsesCallback(t *testing.T) {
	called := 0
	pub, err := NewPublisher(Config{OnPublish: func(topic string, msg message.Message) error {
		called++
		return nil
	}})
	if err != nil {
		t.Fatalf("NewPublisher error: %v", err)
	}

	msg := message.Message{ID: "id", Topic: "t", Timestamp: time.Now(), ContentType: "text/plain", Metadata: map[string]string{}, SchemaVersion: "v1"}
	if err := pub.Publish(context.Background(), "t", msg); err != nil {
		t.Fatalf("Publish error: %v", err)
	}
	if called != 1 {
		t.Fatalf("expected callback to be called")
	}
}
