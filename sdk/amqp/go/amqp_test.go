package amqpadapter_test

import (
	"context"
	"testing"
	"time"

	"relaybus/sdk/core/go/envelope"
	"relaybus/sdk/core/go/message"

	amqpadapter "relaybus/sdk/amqp/go"

	amqp "github.com/rabbitmq/amqp091-go"
)

type fakeChannel struct {
	exchange string
	key      string
	body     []byte
	headers  amqp.Table
}

func (f *fakeChannel) PublishWithContext(_ context.Context, exchange, key string, _ bool, _ bool, msg amqp.Publishing) error {
	f.exchange = exchange
	f.key = key
	f.body = msg.Body
	f.headers = msg.Headers
	return nil
}

func (f *fakeChannel) Close() error { return nil }

func TestPublishUsesRoutingTemplate(t *testing.T) {
	ch := &fakeChannel{}
	pub, err := amqpadapter.NewPublisher(amqpadapter.Config{Channel: ch, Exchange: "ex", RoutingKeyTemplate: "events.{topic}"})
	if err != nil {
		t.Fatalf("NewPublisher error: %v", err)
	}

	msg := message.Message{
		ID:            "id",
		Topic:         "alpha",
		Timestamp:     time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
		ContentType:   "text/plain",
		Payload:       []byte("hi"),
		Metadata:      map[string]string{"k": "v"},
		SchemaVersion: "v1",
	}
	if err := pub.Publish(context.Background(), "alpha", msg); err != nil {
		t.Fatalf("Publish error: %v", err)
	}

	if ch.exchange != "ex" {
		t.Fatalf("expected exchange ex, got %s", ch.exchange)
	}
	if ch.key != "events.alpha" {
		t.Fatalf("expected routing key events.alpha, got %s", ch.key)
	}
	if ch.headers["k"] != "v" {
		t.Fatalf("expected header k")
	}
	decoded, err := envelope.DecodeEnvelope(ch.body)
	if err != nil {
		t.Fatalf("decode error: %v", err)
	}
	if decoded.ID != "id" {
		t.Fatalf("expected id preserved")
	}
}
