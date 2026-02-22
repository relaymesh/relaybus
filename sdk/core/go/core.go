package core

import (
	"context"
	crand "crypto/rand"
	"errors"
	"fmt"
	"log"
	mrand "math/rand"
	"os"
	"time"

	"relaybus/sdk/core/go/envelope"
	"relaybus/sdk/core/go/errdefs"
	"relaybus/sdk/core/go/memoryadapter"
	"relaybus/sdk/core/go/message"

	amqpadapter "relaybus/sdk/amqp/go"
	httpadapter "relaybus/sdk/http/go"
	kafkaadapter "relaybus/sdk/kafka/go"
	natsadapter "relaybus/sdk/nats/go"
)

const DefaultContentType = "application/octet-stream"

var ErrInvalidEnvelope = errdefs.ErrInvalidEnvelope

type TransientError = errdefs.TransientError

type PermanentError = errdefs.PermanentError

func IsTransient(err error) bool {
	return errdefs.IsTransient(err)
}

type Message = message.Message

type Publisher interface {
	Publish(ctx context.Context, topic string, msg Message) error
	PublishBatch(ctx context.Context, topic string, msgs []Message) error
	Close() error
}

type RetryPolicy struct {
	MaxAttempts int
	BaseDelay   time.Duration
	MaxDelay    time.Duration
	Jitter      float64
}

type Hooks struct {
	OnPublished func(result PublishResult)
	OnFailed    func(result PublishResult, err error)
}

type PublishResult struct {
	Destination string
	Topic       string
	Message     Message
	Attempt     int
}

type Logger interface {
	Debug(msg string, args ...any)
	Info(msg string, args ...any)
	Warn(msg string, args ...any)
	Error(msg string, args ...any)
}

type NopLogger struct{}

func (NopLogger) Debug(string, ...any) {}
func (NopLogger) Info(string, ...any)  {}
func (NopLogger) Warn(string, ...any)  {}
func (NopLogger) Error(string, ...any) {}

type StdLogger struct {
	logger *log.Logger
}

func NewStdLogger() StdLogger {
	return StdLogger{logger: log.New(os.Stdout, "relaybus ", log.LstdFlags)}
}

func (l StdLogger) Debug(msg string, args ...any) { l.logger.Printf("DEBUG: "+msg, args...) }
func (l StdLogger) Info(msg string, args ...any)  { l.logger.Printf("INFO: "+msg, args...) }
func (l StdLogger) Warn(msg string, args ...any)  { l.logger.Printf("WARN: "+msg, args...) }
func (l StdLogger) Error(msg string, args ...any) { l.logger.Printf("ERROR: "+msg, args...) }

type Config struct {
	Destination string
	Retry       RetryPolicy
	Logger      Logger
	Hooks       Hooks
	HTTP        httpadapter.Config
	NATS        natsadapter.Config
	AMQP        amqpadapter.Config
	Kafka       kafkaadapter.Config
	Memory      memoryadapter.Config
}

var nowFunc = time.Now
var uuidFunc = newUUIDv4
var sleepFunc = time.Sleep

func NormalizeMessage(msg *Message) error {
	if msg == nil {
		return errors.New("message is nil")
	}
	if msg.ID == "" {
		msg.ID = uuidFunc()
		if msg.ID == "" {
			return errors.New("generated id is empty")
		}
	}
	if msg.Topic == "" {
		return errors.New("topic is required")
	}
	if msg.Timestamp.IsZero() {
		msg.Timestamp = nowFunc().UTC()
	}
	if msg.ContentType == "" {
		msg.ContentType = DefaultContentType
	}
	if msg.Metadata == nil {
		msg.Metadata = map[string]string{}
	}
	if msg.SchemaVersion == "" {
		msg.SchemaVersion = "v1"
	}
	return nil
}

func EncodeEnvelope(msg Message) ([]byte, error) {
	if err := NormalizeMessage(&msg); err != nil {
		return nil, err
	}
	return envelope.EncodeEnvelope(msg)
}

func DecodeEnvelope(data []byte) (Message, error) {
	return envelope.DecodeEnvelope(data)
}

func ValidateEnvelopeJSON(data []byte) error {
	return envelope.ValidateEnvelopeJSON(data)
}

func NewPublisher(cfg Config) (Publisher, error) {
	logger := cfg.Logger
	if logger == nil {
		logger = NopLogger{}
	}

	var base Publisher
	var err error

	switch cfg.Destination {
	case "http":
		base, err = httpadapter.NewPublisher(cfg.HTTP)
	case "nats":
		base, err = natsadapter.NewPublisher(cfg.NATS)
	case "amqp":
		base, err = amqpadapter.NewPublisher(cfg.AMQP)
	case "kafka":
		base, err = kafkaadapter.NewPublisher(cfg.Kafka)
	case "memory":
		base, err = memoryadapter.NewPublisher(cfg.Memory)
	default:
		return nil, fmt.Errorf("unknown destination %q", cfg.Destination)
	}
	if err != nil {
		return nil, err
	}

	return &publisherWrapper{
		base:        base,
		retry:       normalizeRetryPolicy(cfg.Retry),
		logger:      logger,
		hooks:       cfg.Hooks,
		destination: cfg.Destination,
	}, nil
}

type publisherWrapper struct {
	base        Publisher
	retry       RetryPolicy
	logger      Logger
	hooks       Hooks
	destination string
}

func (p *publisherWrapper) Publish(ctx context.Context, topic string, msg Message) error {
	if err := applyTopic(topic, &msg); err != nil {
		return err
	}
	if err := NormalizeMessage(&msg); err != nil {
		return err
	}

	attempt := 0
	for {
		attempt++
		err := p.base.Publish(ctx, msg.Topic, msg)
		result := PublishResult{Destination: p.destination, Topic: msg.Topic, Message: msg, Attempt: attempt}
		if err == nil {
			if p.hooks.OnPublished != nil {
				p.hooks.OnPublished(result)
			}
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if !errdefs.IsTransient(err) || attempt >= p.retry.MaxAttempts {
			if p.hooks.OnFailed != nil {
				p.hooks.OnFailed(result, err)
			}
			return err
		}
		delay := backoffDuration(p.retry, attempt)
		if err := sleepWithContext(ctx, delay); err != nil {
			return err
		}
	}
}

func (p *publisherWrapper) PublishBatch(ctx context.Context, topic string, msgs []Message) error {
	if len(msgs) == 0 {
		return nil
	}

	normalized := make([]Message, 0, len(msgs))
	for i := range msgs {
		msg := msgs[i]
		if err := applyTopic(topic, &msg); err != nil {
			return err
		}
		if err := NormalizeMessage(&msg); err != nil {
			return err
		}
		normalized = append(normalized, msg)
	}

	attempt := 0
	batchTopic := topic
	if batchTopic == "" {
		batchTopic = normalized[0].Topic
		for _, msg := range normalized[1:] {
			if msg.Topic != batchTopic {
				return errors.New("batch messages must share a topic")
			}
		}
	}

	for {
		attempt++
		err := p.base.PublishBatch(ctx, batchTopic, normalized)
		if err == nil {
			if p.hooks.OnPublished != nil {
				for _, msg := range normalized {
					p.hooks.OnPublished(PublishResult{Destination: p.destination, Topic: msg.Topic, Message: msg, Attempt: attempt})
				}
			}
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if !errdefs.IsTransient(err) || attempt >= p.retry.MaxAttempts {
			if p.hooks.OnFailed != nil {
				for _, msg := range normalized {
					p.hooks.OnFailed(PublishResult{Destination: p.destination, Topic: msg.Topic, Message: msg, Attempt: attempt}, err)
				}
			}
			return err
		}
		delay := backoffDuration(p.retry, attempt)
		if err := sleepWithContext(ctx, delay); err != nil {
			return err
		}
	}
}

func (p *publisherWrapper) Close() error {
	return p.base.Close()
}

func applyTopic(topic string, msg *Message) error {
	if msg.Topic == "" {
		msg.Topic = topic
	}
	if msg.Topic == "" {
		return errors.New("topic is required")
	}
	if topic != "" && msg.Topic != topic {
		return fmt.Errorf("topic mismatch: %q vs %q", msg.Topic, topic)
	}
	return nil
}

func normalizeRetryPolicy(policy RetryPolicy) RetryPolicy {
	if policy.MaxAttempts <= 0 {
		policy.MaxAttempts = 1
	}
	if policy.BaseDelay < 0 {
		policy.BaseDelay = 0
	}
	if policy.MaxDelay <= 0 {
		policy.MaxDelay = 2 * time.Second
	}
	if policy.MaxDelay < policy.BaseDelay {
		policy.MaxDelay = policy.BaseDelay
	}
	if policy.Jitter < 0 {
		policy.Jitter = 0
	}
	return policy
}

func backoffDuration(policy RetryPolicy, attempt int) time.Duration {
	if policy.BaseDelay <= 0 {
		return 0
	}
	delay := policy.BaseDelay
	for i := 1; i < attempt; i++ {
		delay *= 2
		if delay >= policy.MaxDelay {
			return policy.MaxDelay
		}
	}
	if policy.Jitter > 0 {
		jitter := 1 + mrand.Float64()*policy.Jitter
		delay = time.Duration(float64(delay) * jitter)
		if delay > policy.MaxDelay {
			return policy.MaxDelay
		}
	}
	return delay
}

func sleepWithContext(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
	}
	ch := make(chan struct{})
	go func() {
		sleepFunc(d)
		close(ch)
	}()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-ch:
		return nil
	}
}

func newUUIDv4() string {
	buf := make([]byte, 16)
	if _, err := crand.Read(buf); err != nil {
		return fmt.Sprintf("uuid-%d", time.Now().UnixNano())
	}
	buf[6] = (buf[6] & 0x0f) | 0x40
	buf[8] = (buf[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", buf[0:4], buf[4:6], buf[6:8], buf[8:10], buf[10:])
}
