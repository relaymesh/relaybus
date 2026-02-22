package natsadapter_test

import (
	"context"
	"testing"

	natsadapter "relaybus/sdk/nats/go"

	"github.com/nats-io/nats.go"

	"relaybus/sdk/core/go/message"
)

func TestSubscriberHandlesMsg(t *testing.T) {
	var got message.Message
	sub, err := natsadapter.NewSubscriber(natsadapter.SubscriberConfig{Handler: func(_ context.Context, msg message.Message) error {
		got = msg
		return nil
	}})
	if err != nil {
		t.Fatalf("NewSubscriber error: %v", err)
	}

	msg := &nats.Msg{Data: []byte(`{"v":"v1","id":"id","topic":"alpha","ts":"2024-01-01T00:00:00Z","content_type":"text/plain","payload_b64":"aGVsbG8=","meta":{}}`)}
	if err := sub.HandleMsg(context.Background(), msg); err != nil {
		t.Fatalf("HandleMsg error: %v", err)
	}
	if got.ID != "id" {
		t.Fatalf("expected id")
	}
}

func TestSubscriberRejectsInvalidEnvelope(t *testing.T) {
	sub, err := natsadapter.NewSubscriber(natsadapter.SubscriberConfig{Handler: func(_ context.Context, _ message.Message) error {
		return nil
	}})
	if err != nil {
		t.Fatalf("NewSubscriber error: %v", err)
	}

	msg := &nats.Msg{Data: []byte("{")}
	if err := sub.HandleMsg(context.Background(), msg); err == nil {
		t.Fatalf("expected error")
	}
}

func TestSubscriberRejectsMissingFields(t *testing.T) {
	sub, err := natsadapter.NewSubscriber(natsadapter.SubscriberConfig{Handler: func(_ context.Context, _ message.Message) error {
		return nil
	}})
	if err != nil {
		t.Fatalf("NewSubscriber error: %v", err)
	}

	msg := &nats.Msg{Data: []byte(`{"v":"v1"}`)}
	if err := sub.HandleMsg(context.Background(), msg); err == nil {
		t.Fatalf("expected error")
	}
}
