package amqpadapter

import (
	"context"
	"errors"
	"strings"
	"sync"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/relaymesh/relaybus/sdk/core/go/envelope"
	"github.com/relaymesh/relaybus/sdk/core/go/errdefs"
	"github.com/relaymesh/relaybus/sdk/core/go/message"
)

type Channel interface {
	PublishWithContext(ctx context.Context, exchange, key string, mandatory, immediate bool, msg amqp.Publishing) error
	Close() error
}

type Config struct {
	URL                string
	Channel            Channel
	Exchange           string
	ExchangeType       string
	RoutingKeyTemplate string
	Queue              string
	Mandatory          bool
	Immediate          bool
}

type Publisher struct {
	channel            Channel
	connection         *amqp.Connection
	exchange           string
	exchangeType       string
	routingKeyTemplate string
	queue              string
	mandatory          bool
	immediate          bool
	mu                 sync.Mutex
	ensuredQueues       map[string]struct{}
	ensuredExchanges    map[string]struct{}
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
		exchangeType:       defaultExchangeType(cfg.ExchangeType),
		routingKeyTemplate: cfg.RoutingKeyTemplate,
		queue:              cfg.Queue,
		mandatory:          cfg.Mandatory,
		immediate:          cfg.Immediate,
		ensuredQueues:      map[string]struct{}{},
		ensuredExchanges:   map[string]struct{}{},
	}, nil
}

func (p *Publisher) Publish(ctx context.Context, topic string, msg message.Message) error {
	if topic == "" {
		return errors.New("topic is required")
	}
	if err := p.ensureInfrastructure(ctx, topic); err != nil {
		return err
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

type queueDeclarer interface {
	QueueDeclare(name string, durable, autoDelete, exclusive, noWait bool, args amqp.Table) (amqp.Queue, error)
}

type exchangeDeclarer interface {
	ExchangeDeclare(name, kind string, durable, autoDelete, internal, noWait bool, args amqp.Table) error
}

func (p *Publisher) ensureInfrastructure(ctx context.Context, topic string) error {
	if p.exchange != "" {
		if declarer, ok := p.channel.(exchangeDeclarer); ok {
			if p.markExchangeEnsured(p.exchange) {
				if err := declarer.ExchangeDeclare(p.exchange, p.exchangeType, false, false, false, false, nil); err != nil {
					return errdefs.TransientError{Err: err}
				}
			}
		}
	}

	queueName := p.queue
	if queueName == "" {
		queueName = topic
	}
	if queueName != "" {
		if declarer, ok := p.channel.(queueDeclarer); ok {
			if p.markQueueEnsured(queueName) {
				if _, err := declarer.QueueDeclare(queueName, false, true, false, false, nil); err != nil {
					return errdefs.TransientError{Err: err}
				}
			}
		}
	}
	return nil
}

func (p *Publisher) markQueueEnsured(name string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if _, exists := p.ensuredQueues[name]; exists {
		return false
	}
	p.ensuredQueues[name] = struct{}{}
	return true
}

func (p *Publisher) markExchangeEnsured(name string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if _, exists := p.ensuredExchanges[name]; exists {
		return false
	}
	p.ensuredExchanges[name] = struct{}{}
	return true
}

func defaultExchangeType(exchangeType string) string {
	if exchangeType != "" {
		return exchangeType
	}
	return "topic"
}
