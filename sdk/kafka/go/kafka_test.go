package kafkaadapter_test

import (
	"context"
	"testing"
	"time"

	kafkaadapter "relaybus/sdk/kafka/go"

	"github.com/segmentio/kafka-go"

	"relaybus/sdk/core/go/envelope"
	"relaybus/sdk/core/go/message"
)

type fakeWriter struct {
	messages []kafka.Message
}

func (f *fakeWriter) WriteMessages(_ context.Context, msgs ...kafka.Message) error {
	f.messages = append(f.messages, msgs...)
	return nil
}

func (f *fakeWriter) Close() error { return nil }

func TestPublishWritesKafkaMessage(t *testing.T) {
	writer := &fakeWriter{}
	pub, err := kafkaadapter.NewPublisher(kafkaadapter.Config{Writer: writer, TopicPrefix: "rb-"})
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
		Key:           "k1",
	}
	if err := pub.Publish(context.Background(), "alpha", msg); err != nil {
		t.Fatalf("Publish error: %v", err)
	}

	if len(writer.messages) != 1 {
		t.Fatalf("expected 1 message, got %d", len(writer.messages))
	}
	kmsg := writer.messages[0]
	if kmsg.Topic != "rb-alpha" {
		t.Fatalf("expected topic rb-alpha, got %s", kmsg.Topic)
	}
	if string(kmsg.Key) != "k1" {
		t.Fatalf("expected key k1")
	}
	decoded, err := envelope.DecodeEnvelope(kmsg.Value)
	if err != nil {
		t.Fatalf("decode error: %v", err)
	}
	if decoded.Topic != "alpha" {
		t.Fatalf("expected topic preserved")
	}
}
