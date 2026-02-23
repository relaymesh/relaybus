package httpadapter

import (
	"context"
	"errors"
	"io"
	"net/http"

	"github.com/relaymesh/relaybus/sdk/core/go/envelope"
	"github.com/relaymesh/relaybus/sdk/core/go/errdefs"
	"github.com/relaymesh/relaybus/sdk/core/go/message"
)

type SubscriberConfig struct {
	Address string
	Handler func(ctx context.Context, msg message.Message) error
}

type Subscriber struct {
	handler func(ctx context.Context, msg message.Message) error
	addr    string
}

func NewSubscriber(cfg SubscriberConfig) (*Subscriber, error) {
	if cfg.Handler == nil {
		return nil, errors.New("handler is required")
	}
	return &Subscriber{handler: cfg.Handler, addr: cfg.Address}, nil
}

func (s *Subscriber) Handle(ctx context.Context, body []byte) error {
	msg, err := envelope.DecodeEnvelope(body)
	if err != nil {
		return err
	}
	return s.handler(ctx, msg)
}

func (s *Subscriber) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	body, err := io.ReadAll(r.Body)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	if err := s.Handle(r.Context(), body); err != nil {
		if errors.Is(err, errdefs.ErrInvalidEnvelope) {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Subscriber) Start(ctx context.Context) error {
	if s.addr == "" {
		return errors.New("address is required")
	}
	server := &http.Server{Addr: s.addr, Handler: s}
	errCh := make(chan error, 1)
	go func() {
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			errCh <- err
			return
		}
		errCh <- nil
	}()
	select {
	case <-ctx.Done():
		_ = server.Shutdown(context.Background())
		return ctx.Err()
	case err := <-errCh:
		return err
	}
}
