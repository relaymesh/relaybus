package natsadapter

import (
	"context"
	"errors"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/relaymesh/relaybus/sdk/core/go/envelope"
	"github.com/relaymesh/relaybus/sdk/core/go/message"
)

type SubscriberConfig struct {
	URL           string
	SubjectPrefix string
	MaxMessages   int
	Handler       func(ctx context.Context, msg message.Message) error
}

type Subscriber struct {
	handler func(ctx context.Context, msg message.Message) error
	url     string
	prefix  string
	maxMsgs int
}

func NewSubscriber(cfg SubscriberConfig) (*Subscriber, error) {
	if cfg.Handler == nil {
		return nil, errors.New("handler is required")
	}
	return &Subscriber{
		handler: cfg.Handler,
		url:     cfg.URL,
		prefix:  cfg.SubjectPrefix,
		maxMsgs: cfg.MaxMessages,
	}, nil
}

func (s *Subscriber) HandleMsg(ctx context.Context, msg *nats.Msg) error {
	if msg == nil {
		return errors.New("msg is nil")
	}
	return s.HandleBody(ctx, msg.Data)
}

func (s *Subscriber) HandleBody(ctx context.Context, body []byte) error {
	decoded, err := envelope.DecodeEnvelope(body)
	if err != nil {
		return err
	}
	return s.handler(ctx, decoded)
}

func (s *Subscriber) Start(ctx context.Context, topic string) error {
	if s.url == "" {
		return errors.New("url is required")
	}
	nc, err := nats.Connect(s.url)
	if err != nil {
		return err
	}
	defer nc.Close()

	subject := joinSubject(s.prefix, topic)
	sub, err := nc.SubscribeSync(subject)
	if err != nil {
		return err
	}
	defer sub.Unsubscribe()

	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		msg, err := sub.NextMsg(500 * time.Millisecond)
		if err == nats.ErrTimeout {
			continue
		}
		if err != nil {
			return err
		}
		if err := s.HandleMsg(ctx, msg); err != nil {
			return err
		}
		if s.maxMsgs > 0 {
			s.maxMsgs--
			if s.maxMsgs == 0 {
				return nil
			}
		}
	}
}
