package amqpadapter

import (
	"context"
	"errors"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/relaymesh/relaybus/sdk/core/go/envelope"
	"github.com/relaymesh/relaybus/sdk/core/go/message"
)

type SubscriberConfig struct {
	URL                string
	Exchange           string
	RoutingKeyTemplate string
	Queue              string
	AutoAck            bool
	MaxMessages        int
	Handler            func(ctx context.Context, msg message.Message) error
}

type Subscriber struct {
	handler            func(ctx context.Context, msg message.Message) error
	url                string
	exchange           string
	routingKeyTemplate string
	queue              string
	autoAck            bool
	maxMessages        int
}

func NewSubscriber(cfg SubscriberConfig) (*Subscriber, error) {
	if cfg.Handler == nil {
		return nil, errors.New("handler is required")
	}
	return &Subscriber{
		handler:            cfg.Handler,
		url:                cfg.URL,
		exchange:           cfg.Exchange,
		routingKeyTemplate: cfg.RoutingKeyTemplate,
		queue:              cfg.Queue,
		autoAck:            cfg.AutoAck,
		maxMessages:        cfg.MaxMessages,
	}, nil
}

func (s *Subscriber) HandleDelivery(ctx context.Context, delivery amqp.Delivery) error {
	return s.HandleBody(ctx, delivery.Body)
}

func (s *Subscriber) HandleBody(ctx context.Context, body []byte) error {
	msg, err := envelope.DecodeEnvelope(body)
	if err != nil {
		return err
	}
	return s.handler(ctx, msg)
}

func (s *Subscriber) Start(ctx context.Context, topic string) error {
	if s.url == "" {
		return errors.New("url is required")
	}
	conn, err := amqp.Dial(s.url)
	if err != nil {
		return err
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		return err
	}
	defer ch.Close()

	queueName := s.queue
	if queueName == "" {
		queueName = topic
	}
	q, err := ch.QueueDeclare(queueName, false, true, false, false, nil)
	if err != nil {
		return err
	}
	if s.exchange != "" {
		key := buildRoutingKey(s.routingKeyTemplate, topic)
		if err := ch.QueueBind(q.Name, key, s.exchange, false, nil); err != nil {
			return err
		}
	}

	msgs, err := ch.Consume(q.Name, "", s.autoAck, false, false, false, nil)
	if err != nil {
		return err
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case delivery, ok := <-msgs:
			if !ok {
				return nil
			}
			if err := s.HandleDelivery(ctx, delivery); err != nil {
				if !s.autoAck {
					_ = delivery.Nack(false, true)
				}
				return err
			}
			if !s.autoAck {
				_ = delivery.Ack(false)
			}
			if s.maxMessages > 0 {
				s.maxMessages--
				if s.maxMessages == 0 {
					return nil
				}
			}
		}
	}
}
