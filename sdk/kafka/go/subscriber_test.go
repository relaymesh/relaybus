package kafkaadapter_test

import (
	"context"
	"testing"

	kafkaadapter "github.com/relaymesh/relaybus/sdk/kafka/go"

	"github.com/segmentio/kafka-go"

	"github.com/relaymesh/relaybus/sdk/core/go/message"
)

func TestSubscriberHandlesMessage(t *testing.T) {
	var got message.Message
	sub, err := kafkaadapter.NewSubscriber(kafkaadapter.SubscriberConfig{Handler: func(_ context.Context, msg message.Message) error {
		got = msg
		return nil
	}})
	if err != nil {
		t.Fatalf("NewSubscriber error: %v", err)
	}

	msg := kafka.Message{Value: []byte(`{"v":"v1","id":"id","topic":"alpha","ts":"2024-01-01T00:00:00Z","content_type":"text/plain","payload_b64":"aGVsbG8=","meta":{}}`)}
	if err := sub.HandleMessage(context.Background(), msg); err != nil {
		t.Fatalf("HandleMessage error: %v", err)
	}
	if got.Topic != "alpha" {
		t.Fatalf("expected topic")
	}
}

func TestSubscriberRejectsInvalidEnvelope(t *testing.T) {
	sub, err := kafkaadapter.NewSubscriber(kafkaadapter.SubscriberConfig{Handler: func(_ context.Context, _ message.Message) error {
		return nil
	}})
	if err != nil {
		t.Fatalf("NewSubscriber error: %v", err)
	}

	msg := kafka.Message{Value: []byte("{")}
	if err := sub.HandleMessage(context.Background(), msg); err == nil {
		t.Fatalf("expected error")
	}
}

func TestSubscriberRejectsInvalidBase64(t *testing.T) {
	sub, err := kafkaadapter.NewSubscriber(kafkaadapter.SubscriberConfig{Handler: func(_ context.Context, _ message.Message) error {
		return nil
	}})
	if err != nil {
		t.Fatalf("NewSubscriber error: %v", err)
	}

	msg := kafka.Message{Value: []byte(`{"v":"v1","id":"id","topic":"alpha","ts":"2024-01-01T00:00:00Z","content_type":"text/plain","payload_b64":"???","meta":{}}`)}
	if err := sub.HandleMessage(context.Background(), msg); err == nil {
		t.Fatalf("expected error")
	}
}
