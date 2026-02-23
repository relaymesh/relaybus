package kafkaadapter

import (
	"context"
	"errors"

	"github.com/segmentio/kafka-go"

	"github.com/relaymesh/relaybus/sdk/core/go/envelope"
	"github.com/relaymesh/relaybus/sdk/core/go/message"
)

type SubscriberConfig struct {
	Brokers     []string
	Broker      string
	GroupID     string
	MaxMessages int
	Handler     func(ctx context.Context, msg message.Message) error
}

type Subscriber struct {
	handler     func(ctx context.Context, msg message.Message) error
	brokers     []string
	groupID     string
	maxMessages int
}

func NewSubscriber(cfg SubscriberConfig) (*Subscriber, error) {
	if cfg.Handler == nil {
		return nil, errors.New("handler is required")
	}
	brokers := cfg.Brokers
	if len(brokers) == 0 && cfg.Broker != "" {
		brokers = []string{cfg.Broker}
	}
	return &Subscriber{
		handler:     cfg.Handler,
		brokers:     brokers,
		groupID:     cfg.GroupID,
		maxMessages: cfg.MaxMessages,
	}, nil
}

func (s *Subscriber) HandleMessage(ctx context.Context, msg kafka.Message) error {
	return s.HandleBody(ctx, msg.Value)
}

func (s *Subscriber) HandleBody(ctx context.Context, body []byte) error {
	decoded, err := envelope.DecodeEnvelope(body)
	if err != nil {
		return err
	}
	return s.handler(ctx, decoded)
}

func (s *Subscriber) Start(ctx context.Context, topic string) error {
	if len(s.brokers) == 0 {
		return errors.New("broker(s) are required")
	}
	if s.groupID == "" {
		s.groupID = "relaybus"
	}
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: s.brokers,
		Topic:   topic,
		GroupID: s.groupID,
	})
	defer reader.Close()

	for {
		msg, err := reader.ReadMessage(ctx)
		if err != nil {
			return err
		}
		if err := s.HandleMessage(ctx, msg); err != nil {
			return err
		}
		if s.maxMessages > 0 {
			s.maxMessages--
			if s.maxMessages == 0 {
				return nil
			}
		}
	}
}
