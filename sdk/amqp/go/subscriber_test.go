package amqpadapter_test

import (
	"context"
	"testing"

	"github.com/relaymesh/relaybus/sdk/core/go/message"

	amqpadapter "github.com/relaymesh/relaybus/sdk/amqp/go"

	amqp "github.com/rabbitmq/amqp091-go"
)

func TestSubscriberHandlesDelivery(t *testing.T) {
	var got message.Message
	sub, err := amqpadapter.NewSubscriber(amqpadapter.SubscriberConfig{Handler: func(_ context.Context, msg message.Message) error {
		got = msg
		return nil
	}})
	if err != nil {
		t.Fatalf("NewSubscriber error: %v", err)
	}

	delivery := amqp.Delivery{Body: []byte(`{"v":"v1","id":"id","topic":"alpha","ts":"2024-01-01T00:00:00Z","content_type":"text/plain","payload_b64":"aGVsbG8=","meta":{}}`)}
	if err := sub.HandleDelivery(context.Background(), delivery); err != nil {
		t.Fatalf("HandleDelivery error: %v", err)
	}
	if got.Topic != "alpha" {
		t.Fatalf("expected topic")
	}
}

func TestSubscriberRejectsInvalidEnvelope(t *testing.T) {
	sub, err := amqpadapter.NewSubscriber(amqpadapter.SubscriberConfig{Handler: func(_ context.Context, _ message.Message) error {
		return nil
	}})
	if err != nil {
		t.Fatalf("NewSubscriber error: %v", err)
	}

	delivery := amqp.Delivery{Body: []byte("{")}
	if err := sub.HandleDelivery(context.Background(), delivery); err == nil {
		t.Fatalf("expected error")
	}
}

func TestSubscriberRejectsInvalidBase64(t *testing.T) {
	sub, err := amqpadapter.NewSubscriber(amqpadapter.SubscriberConfig{Handler: func(_ context.Context, _ message.Message) error {
		return nil
	}})
	if err != nil {
		t.Fatalf("NewSubscriber error: %v", err)
	}

	delivery := amqp.Delivery{Body: []byte(`{"v":"v1","id":"id","topic":"alpha","ts":"2024-01-01T00:00:00Z","content_type":"text/plain","payload_b64":"???","meta":{}}`)}
	if err := sub.HandleDelivery(context.Background(), delivery); err == nil {
		t.Fatalf("expected error")
	}
}
