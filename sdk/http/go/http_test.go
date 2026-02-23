package httpadapter_test

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/relaymesh/relaybus/sdk/core/go/envelope"
	"github.com/relaymesh/relaybus/sdk/core/go/message"

	core "github.com/relaymesh/relaybus/sdk/core/go"
	httpadapter "github.com/relaymesh/relaybus/sdk/http/go"
)

func TestPublishSendsEnvelope(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		if r.URL.Path != "/v1/alpha" {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		if ct := r.Header.Get("Content-Type"); ct != "application/json" {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		if id := r.Header.Get("Idempotency-Key"); id != "msg-1" {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		body, _ := io.ReadAll(r.Body)
		msg, err := envelope.DecodeEnvelope(body)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		if msg.Topic != "alpha" {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	pub, err := httpadapter.NewPublisher(httpadapter.Config{Endpoint: server.URL + "/v1/{topic}"})
	if err != nil {
		t.Fatalf("NewPublisher error: %v", err)
	}

	msg := message.Message{
		ID:            "msg-1",
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
}

func TestPublishRetriesOn500(t *testing.T) {
	var calls int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		count := atomic.AddInt32(&calls, 1)
		if count == 1 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	pub, err := core.NewPublisher(core.Config{
		Destination: "http",
		Retry:       core.RetryPolicy{MaxAttempts: 2, BaseDelay: 0, MaxDelay: 0},
		HTTP:        httpadapter.Config{Endpoint: server.URL + "/{topic}"},
	})
	if err != nil {
		t.Fatalf("NewPublisher error: %v", err)
	}

	msg := core.Message{
		Topic:       "alpha",
		Payload:     []byte("data"),
		Metadata:    map[string]string{},
		ContentType: core.DefaultContentType,
	}
	if err := pub.Publish(context.Background(), "alpha", msg); err != nil {
		t.Fatalf("Publish error: %v", err)
	}
	if calls != 2 {
		t.Fatalf("expected 2 calls, got %d", calls)
	}
}

func TestPublishRetriesOn429(t *testing.T) {
	var calls int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		count := atomic.AddInt32(&calls, 1)
		if count == 1 {
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	pub, err := core.NewPublisher(core.Config{
		Destination: "http",
		Retry:       core.RetryPolicy{MaxAttempts: 2, BaseDelay: 0, MaxDelay: 0},
		HTTP:        httpadapter.Config{Endpoint: server.URL + "/{topic}"},
	})
	if err != nil {
		t.Fatalf("NewPublisher error: %v", err)
	}

	msg := core.Message{
		Topic:       "alpha",
		Payload:     []byte("data"),
		Metadata:    map[string]string{},
		ContentType: core.DefaultContentType,
	}
	if err := pub.Publish(context.Background(), "alpha", msg); err != nil {
		t.Fatalf("Publish error: %v", err)
	}
	if calls != 2 {
		t.Fatalf("expected 2 calls, got %d", calls)
	}
}
