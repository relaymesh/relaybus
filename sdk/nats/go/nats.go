package natsadapter

import (
	"context"
	"errors"
	"strings"

	"github.com/relaymesh/relaybus/sdk/core/go/envelope"
	"github.com/relaymesh/relaybus/sdk/core/go/errdefs"
	"github.com/relaymesh/relaybus/sdk/core/go/message"

	"github.com/nats-io/nats.go"
)

type Conn interface {
	Publish(subj string, data []byte) error
	Close()
}

type Config struct {
	URL           string
	Conn          Conn
	SubjectPrefix string
}

type Publisher struct {
	conn   Conn
	owned  *nats.Conn
	prefix string
}

func NewPublisher(cfg Config) (*Publisher, error) {
	conn := cfg.Conn
	var owned *nats.Conn
	if conn == nil {
		if cfg.URL == "" {
			return nil, errors.New("conn or url is required")
		}
		var err error
		owned, err = nats.Connect(cfg.URL)
		if err != nil {
			return nil, err
		}
		conn = owned
	}
	return &Publisher{conn: conn, owned: owned, prefix: cfg.SubjectPrefix}, nil
}

func (p *Publisher) Publish(_ context.Context, topic string, msg message.Message) error {
	if topic == "" {
		return errors.New("topic is required")
	}
	body, err := envelope.EncodeEnvelope(msg)
	if err != nil {
		return err
	}
	subject := joinSubject(p.prefix, topic)
	if err := p.conn.Publish(subject, body); err != nil {
		return errdefs.TransientError{Err: err}
	}
	return nil
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
	p.conn.Close()
	if p.owned != nil {
		p.owned.Close()
	}
	return nil
}

func joinSubject(prefix, topic string) string {
	if prefix == "" {
		return topic
	}
	if strings.HasSuffix(prefix, ".") {
		return prefix + topic
	}
	return prefix + "." + topic
}
