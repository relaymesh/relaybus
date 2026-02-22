package core

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"relaybus/sdk/core/go/envelope"
)

type fakePublisher struct {
	failures int
	calls    int
}

func (f *fakePublisher) Publish(_ context.Context, _ string, _ Message) error {
	f.calls++
	if f.calls <= f.failures {
		return TransientError{Err: errSentinel}
	}
	return nil
}

func (f *fakePublisher) PublishBatch(ctx context.Context, topic string, msgs []Message) error {
	for _, msg := range msgs {
		if err := f.Publish(ctx, topic, msg); err != nil {
			return err
		}
	}
	return nil
}

func (f *fakePublisher) Close() error { return nil }

var errSentinel = &testError{msg: "boom"}

type testError struct{ msg string }

func (e *testError) Error() string { return e.msg }

func TestNormalizeMessageDefaults(t *testing.T) {
	fixed := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	prevNow := nowFunc
	prevUUID := uuidFunc
	nowFunc = func() time.Time { return fixed }
	uuidFunc = func() string { return "uuid-123" }
	defer func() {
		nowFunc = prevNow
		uuidFunc = prevUUID
	}()

	msg := Message{Topic: "alpha"}
	if err := NormalizeMessage(&msg); err != nil {
		t.Fatalf("NormalizeMessage error: %v", err)
	}
	if msg.ID != "uuid-123" {
		t.Fatalf("expected id to be set")
	}
	if !msg.Timestamp.Equal(fixed) {
		t.Fatalf("expected timestamp set")
	}
	if msg.ContentType != DefaultContentType {
		t.Fatalf("expected content_type default")
	}
	if msg.Metadata == nil || len(msg.Metadata) != 0 {
		t.Fatalf("expected empty metadata map")
	}
	if msg.SchemaVersion != "v1" {
		t.Fatalf("expected schema version default")
	}
}

func TestEncodeDecodeRoundtrip(t *testing.T) {
	msg := Message{
		ID:            "msg-1",
		Topic:         "alpha",
		Timestamp:     time.Date(2024, 1, 2, 3, 4, 5, 6, time.UTC),
		ContentType:   "text/plain",
		Payload:       []byte("hello"),
		Metadata:      map[string]string{"k": "v"},
		SchemaVersion: "v1",
	}
	data, err := EncodeEnvelope(msg)
	if err != nil {
		t.Fatalf("EncodeEnvelope error: %v", err)
	}
	if err := ValidateEnvelopeJSON(data); err != nil {
		t.Fatalf("ValidateEnvelopeJSON error: %v", err)
	}
	decoded, err := DecodeEnvelope(data)
	if err != nil {
		t.Fatalf("DecodeEnvelope error: %v", err)
	}
	if string(decoded.Payload) != "hello" {
		t.Fatalf("expected payload preserved")
	}
	if decoded.Metadata["k"] != "v" {
		t.Fatalf("expected metadata preserved")
	}
}

func TestEncodeEnvelopeMissingTopic(t *testing.T) {
	msg := Message{ID: "id", SchemaVersion: "v1"}
	if _, err := EncodeEnvelope(msg); err == nil {
		t.Fatalf("expected error for missing topic")
	}
}

func TestDecodeEnvelopeInvalidJSON(t *testing.T) {
	if _, err := DecodeEnvelope([]byte("{")); err == nil {
		t.Fatalf("expected error for invalid json")
	}
}

func TestDecodeEnvelopeInvalidBase64(t *testing.T) {
	bad := []byte(`{"v":"v1","id":"id","topic":"t","ts":"2024-01-01T00:00:00Z","content_type":"text/plain","payload_b64":"???","meta":{}}`)
	if _, err := DecodeEnvelope(bad); err == nil {
		t.Fatalf("expected error for invalid base64")
	}
}

func TestValidateEnvelopeMissingFields(t *testing.T) {
	bad := []byte(`{"v":"v1"}`)
	if err := ValidateEnvelopeJSON(bad); err == nil {
		t.Fatalf("expected error for missing fields")
	}
}

func TestValidateEnvelopeCorpusSamples(t *testing.T) {
	root := filepath.Join("..", "..", "..")
	samplesDir := filepath.Join(root, "spec", "corpus", "samples")
	entries, err := os.ReadDir(samplesDir)
	if err != nil {
		t.Fatalf("read samples dir: %v", err)
	}
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		data, err := os.ReadFile(filepath.Join(samplesDir, entry.Name()))
		if err != nil {
			t.Fatalf("read sample: %v", err)
		}
		if err := ValidateEnvelopeJSON(data); err != nil {
			t.Fatalf("sample %s invalid: %v", entry.Name(), err)
		}
	}
}

func TestRetryPublisherTransient(t *testing.T) {
	fake := &fakePublisher{failures: 2}
	wrapper := &publisherWrapper{
		base:        fake,
		retry:       RetryPolicy{MaxAttempts: 3, BaseDelay: 0, MaxDelay: 0},
		destination: "memory",
	}
	msg := Message{ID: "id", Topic: "alpha", Timestamp: time.Now(), ContentType: "text/plain", Metadata: map[string]string{}, SchemaVersion: "v1"}
	if err := wrapper.Publish(context.Background(), "alpha", msg); err != nil {
		t.Fatalf("expected retry to succeed, got %v", err)
	}
	if fake.calls != 3 {
		t.Fatalf("expected 3 calls, got %d", fake.calls)
	}
}

func TestEncodeEnvelopeProducesJSON(t *testing.T) {
	msg := Message{ID: "id", Topic: "alpha", Timestamp: time.Now(), ContentType: "text/plain", Payload: []byte("x"), Metadata: map[string]string{}, SchemaVersion: "v1"}
	data, err := EncodeEnvelope(msg)
	if err != nil {
		t.Fatalf("EncodeEnvelope error: %v", err)
	}
	var envelopeMap map[string]any
	if err := json.Unmarshal(data, &envelopeMap); err != nil {
		t.Fatalf("expected json output: %v", err)
	}
	for _, field := range []string{"v", "id", "topic", "ts", "content_type", "payload_b64", "meta"} {
		if _, ok := envelopeMap[field]; !ok {
			t.Fatalf("missing field %s", field)
		}
	}
}

func TestEnvelopePackageValidation(t *testing.T) {
	msg := Message{ID: "id", Topic: "alpha", Timestamp: time.Now(), ContentType: "text/plain", Payload: []byte("x"), Metadata: map[string]string{}, SchemaVersion: "v1"}
	data, err := EncodeEnvelope(msg)
	if err != nil {
		t.Fatalf("EncodeEnvelope error: %v", err)
	}
	if err := envelope.ValidateEnvelopeJSON(data); err != nil {
		t.Fatalf("envelope.ValidateEnvelopeJSON error: %v", err)
	}
}
