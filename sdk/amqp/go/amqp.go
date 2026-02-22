package amqpadapter

import (
	"context"
	"errors"
	"strings"

	amqp "github.com/rabbitmq/amqp091-go"

	"relaybus/sdk/core/go/envelope"
	"relaybus/sdk/core/go/errdefs"
	"relaybus/sdk/core/go/message"
)

type Channel interface {
	PublishWithContext(ctx context.Context, exchange, key string, mandatory, immediate bool, msg amqp.Publishing) error
	Close() error
}

type Config struct {
	URL                string
	Channel            Channel
	Exchange           string
	RoutingKeyTemplate string
	Mandatory          bool
	Immediate          bool
}

type Publisher struct {
	channel            Channel
	connection         *amqp.Connection
	exchange           string
	routingKeyTemplate string
	mandatory          bool
	immediate          bool
}

func NewPublisher(cfg Config) (*Publisher, error) {
	channel := cfg.Channel
	var conn *amqp.Connection
	if channel == nil {
		if cfg.URL == "" {
			return nil, errors.New("channel or url is required")
		}
		var err error
		conn, err = amqp.Dial(cfg.URL)
		if err != nil {
			return nil, err
		}
		ch, err := conn.Channel()
		if err != nil {
			conn.Close()
			return nil, err
		}
		channel = ch
	}
	return &Publisher{
		channel:            channel,
		connection:         conn,
		exchange:           cfg.Exchange,
		routingKeyTemplate: cfg.RoutingKeyTemplate,
		mandatory:          cfg.Mandatory,
		immediate:          cfg.Immediate,
	}, nil
}

func (p *Publisher) Publish(ctx context.Context, topic string, msg message.Message) error {
	if topic == "" {
		return errors.New("topic is required")
	}
	body, err := envelope.EncodeEnvelope(msg)
	if err != nil {
		return err
	}

	key := buildRoutingKey(p.routingKeyTemplate, topic)
	headers := amqp.Table{}
	for k, v := range msg.Metadata {
		headers[k] = v
	}

	pub := amqp.Publishing{
		MessageId:   msg.ID,
		Timestamp:   msg.Timestamp,
		ContentType: "application/json",
		Body:        body,
		Headers:     headers,
	}

	if err := p.channel.PublishWithContext(ctx, p.exchange, key, p.mandatory, p.immediate, pub); err != nil {
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
	if err := p.channel.Close(); err != nil {
		return err
	}
	if p.connection != nil {
		return p.connection.Close()
	}
	return nil
}

func buildRoutingKey(template, topic string) string {
	if template == "" {
		return topic
	}
	if strings.Contains(template, "{topic}") {
		return strings.ReplaceAll(template, "{topic}", topic)
	}
	return template
}
