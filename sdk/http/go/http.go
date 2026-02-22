package httpadapter

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"

	"relaybus/sdk/core/go/envelope"
	"relaybus/sdk/core/go/errdefs"
	"relaybus/sdk/core/go/message"
)

type Doer interface {
	Do(req *http.Request) (*http.Response, error)
}

type Config struct {
	Endpoint          string
	Client            Doer
	Headers           map[string]string
	IdempotencyHeader string
}

type Publisher struct {
	endpoint          string
	client            Doer
	headers           map[string]string
	idempotencyHeader string
}

func NewPublisher(cfg Config) (*Publisher, error) {
	if cfg.Endpoint == "" {
		return nil, fmt.Errorf("endpoint is required")
	}
	client := cfg.Client
	if client == nil {
		client = http.DefaultClient
	}
	header := cfg.IdempotencyHeader
	if header == "" {
		header = "Idempotency-Key"
	}

	return &Publisher{
		endpoint:          cfg.Endpoint,
		client:            client,
		headers:           cfg.Headers,
		idempotencyHeader: header,
	}, nil
}

func (p *Publisher) Publish(ctx context.Context, topic string, msg message.Message) error {
	body, err := envelope.EncodeEnvelope(msg)
	if err != nil {
		return err
	}

	endpoint, err := p.buildEndpoint(topic)
	if err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return errdefs.TransientError{Err: err}
	}

	req.Header.Set("Content-Type", "application/json")
	if msg.ID != "" {
		req.Header.Set(p.idempotencyHeader, msg.ID)
	} else if msg.Key != "" {
		req.Header.Set(p.idempotencyHeader, msg.Key)
	}
	for k, v := range p.headers {
		req.Header.Set(k, v)
	}

	resp, err := p.client.Do(req)
	if err != nil {
		return errdefs.TransientError{Err: err}
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)

	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return nil
	}

	statusErr := fmt.Errorf("http status %d", resp.StatusCode)
	if resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500 {
		return errdefs.TransientError{Err: statusErr}
	}
	return errdefs.PermanentError{Err: statusErr}
}

func (p *Publisher) PublishBatch(ctx context.Context, topic string, msgs []message.Message) error {
	for _, msg := range msgs {
		if err := p.Publish(ctx, topic, msg); err != nil {
			return err
		}
	}
	return nil
}

func (p *Publisher) Close() error {
	return nil
}

func (p *Publisher) buildEndpoint(topic string) (string, error) {
	if strings.Contains(p.endpoint, "{topic}") {
		return strings.ReplaceAll(p.endpoint, "{topic}", url.PathEscape(topic)), nil
	}
	if topic == "" {
		return p.endpoint, nil
	}
	return strings.TrimRight(p.endpoint, "/") + "/" + url.PathEscape(topic), nil
}
