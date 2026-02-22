package httpadapter_test

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"relaybus/sdk/core/go/message"

	httpadapter "relaybus/sdk/http/go"
)

func TestSubscriberHandlesEnvelope(t *testing.T) {
	var got message.Message
	sub, err := httpadapter.NewSubscriber(httpadapter.SubscriberConfig{Handler: func(_ context.Context, msg message.Message) error {
		got = msg
		return nil
	}})
	if err != nil {
		t.Fatalf("NewSubscriber error: %v", err)
	}

	body := []byte(`{"v":"v1","id":"id","topic":"alpha","ts":"2024-01-01T00:00:00Z","content_type":"text/plain","payload_b64":"aGVsbG8=","meta":{}}`)
	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(body))
	w := httptest.NewRecorder()

	sub.ServeHTTP(w, req)
	if w.Result().StatusCode != http.StatusNoContent {
		t.Fatalf("expected 204, got %d", w.Result().StatusCode)
	}
	if got.ID != "id" {
		t.Fatalf("expected message id")
	}
}

func TestSubscriberRejectsInvalidJSON(t *testing.T) {
	sub, err := httpadapter.NewSubscriber(httpadapter.SubscriberConfig{Handler: func(_ context.Context, _ message.Message) error {
		return nil
	}})
	if err != nil {
		t.Fatalf("NewSubscriber error: %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader([]byte("{")))
	w := httptest.NewRecorder()

	sub.ServeHTTP(w, req)
	if w.Result().StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Result().StatusCode)
	}
}

func TestSubscriberRejectsInvalidBase64(t *testing.T) {
	sub, err := httpadapter.NewSubscriber(httpadapter.SubscriberConfig{Handler: func(_ context.Context, _ message.Message) error {
		return nil
	}})
	if err != nil {
		t.Fatalf("NewSubscriber error: %v", err)
	}

	body := []byte(`{"v":"v1","id":"id","topic":"alpha","ts":"2024-01-01T00:00:00Z","content_type":"text/plain","payload_b64":"???","meta":{}}`)
	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(body))
	w := httptest.NewRecorder()

	sub.ServeHTTP(w, req)
	if w.Result().StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Result().StatusCode)
	}
}
