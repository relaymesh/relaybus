package kafkaadapter

import (
	"context"
	"errors"

	"github.com/segmentio/kafka-go"

	"github.com/relaymesh/relaybus/sdk/core/go/envelope"
	"github.com/relaymesh/relaybus/sdk/core/go/errdefs"
	"github.com/relaymesh/relaybus/sdk/core/go/message"
)

type Writer interface {
	WriteMessages(ctx context.Context, msgs ...kafka.Message) error
	Close() error
}

type Config struct {
	Writer      Writer
	Brokers     []string
	Broker      string
	TopicPrefix string
}

type Publisher struct {
	writer Writer
	owned  bool
	prefix string
}

func NewPublisher(cfg Config) (*Publisher, error) {
	writer := cfg.Writer
	owned := false
	if writer == nil {
		brokers := cfg.Brokers
		if len(brokers) == 0 && cfg.Broker != "" {
			brokers = []string{cfg.Broker}
		}
		if len(brokers) == 0 {
			return nil, errors.New("writer or broker(s) are required")
		}
		writer = &kafka.Writer{
			Addr:     kafka.TCP(brokers...),
			Balancer: &kafka.LeastBytes{},
		}
		owned = true
	}
	return &Publisher{writer: writer, owned: owned, prefix: cfg.TopicPrefix}, nil
}

func (p *Publisher) Publish(ctx context.Context, topic string, msg message.Message) error {
	if topic == "" {
		return errors.New("topic is required")
	}
	body, err := envelope.EncodeEnvelope(msg)
	if err != nil {
		return err
	}

	kmsg := kafka.Message{
		Topic: p.prefix + topic,
		Value: body,
	}
	if msg.Key != "" {
		kmsg.Key = []byte(msg.Key)
	}
	if err := p.writer.WriteMessages(ctx, kmsg); err != nil {
		return errdefs.TransientError{Err: err}
	}
	return nil
}

func (p *Publisher) PublishBatch(ctx context.Context, topic string, msgs []message.Message) error {
	if topic == "" {
		return errors.New("topic is required")
	}
	if len(msgs) == 0 {
		return nil
	}

	batch := make([]kafka.Message, 0, len(msgs))
	for _, msg := range msgs {
		body, err := envelope.EncodeEnvelope(msg)
		if err != nil {
			return err
		}
		kmsg := kafka.Message{
			Topic: p.prefix + topic,
			Value: body,
		}
		if msg.Key != "" {
			kmsg.Key = []byte(msg.Key)
		}
		batch = append(batch, kmsg)
	}
	if err := p.writer.WriteMessages(ctx, batch...); err != nil {
		return errdefs.TransientError{Err: err}
	}
	return nil
}

func (p *Publisher) Close() error {
	if p.owned {
		return p.writer.Close()
	}
	return nil
}
