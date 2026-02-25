package kafkaadapter

import (
	"context"
	"errors"
	"net"
	"strconv"
	"strings"
	"sync"

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
	brokers []string
	mu      sync.Mutex
	topics  map[string]struct{}
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
	brokers := cfg.Brokers
	if len(brokers) == 0 && cfg.Broker != "" {
		brokers = []string{cfg.Broker}
	}
	return &Publisher{
		writer:  writer,
		owned:   owned,
		prefix:  cfg.TopicPrefix,
		brokers: brokers,
		topics:  map[string]struct{}{},
	}, nil
}

func (p *Publisher) Publish(ctx context.Context, topic string, msg message.Message) error {
	if topic == "" {
		return errors.New("topic is required")
	}
	fullTopic := p.prefix + topic
	if err := p.ensureTopic(ctx, fullTopic); err != nil {
		return err
	}
	body, err := envelope.EncodeEnvelope(msg)
	if err != nil {
		return err
	}

	kmsg := kafka.Message{
		Topic: fullTopic,
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
	fullTopic := p.prefix + topic
	if err := p.ensureTopic(ctx, fullTopic); err != nil {
		return err
	}

	batch := make([]kafka.Message, 0, len(msgs))
	for _, msg := range msgs {
		body, err := envelope.EncodeEnvelope(msg)
		if err != nil {
			return err
		}
		kmsg := kafka.Message{
			Topic: fullTopic,
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

func (p *Publisher) ensureTopic(ctx context.Context, topic string) error {
	if topic == "" {
		return nil
	}
	if len(p.brokers) == 0 {
		return nil
	}
	if !p.markTopicEnsured(topic) {
		return nil
	}
	conn, err := kafka.DialContext(ctx, "tcp", p.brokers[0])
	if err != nil {
		p.unmarkTopic(topic)
		return errdefs.TransientError{Err: err}
	}
	defer conn.Close()

	controller, err := conn.Controller()
	if err != nil {
		p.unmarkTopic(topic)
		return errdefs.TransientError{Err: err}
	}
	controllerAddr := net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port))
	controllerConn, err := kafka.DialContext(ctx, "tcp", controllerAddr)
	if err != nil {
		p.unmarkTopic(topic)
		return errdefs.TransientError{Err: err}
	}
	defer controllerConn.Close()

	err = controllerConn.CreateTopics(kafka.TopicConfig{
		Topic:             topic,
		NumPartitions:     1,
		ReplicationFactor: 1,
	})
	if err != nil {
		if isTopicExistsError(err) {
			return nil
		}
		p.unmarkTopic(topic)
		return errdefs.TransientError{Err: err}
	}
	return nil
}

func (p *Publisher) markTopicEnsured(topic string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if _, exists := p.topics[topic]; exists {
		return false
	}
	p.topics[topic] = struct{}{}
	return true
}

func (p *Publisher) unmarkTopic(topic string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.topics, topic)
}

func isTopicExistsError(err error) bool {
	if err == nil {
		return false
	}
	lower := strings.ToLower(err.Error())
	return strings.Contains(lower, "already exists")
}
