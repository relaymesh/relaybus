package memoryadapter

import (
	"context"
	"sync"

	"relaybus/sdk/core/go/message"
)

type Config struct {
	OnPublish func(topic string, msg message.Message) error
}

type Publisher struct {
	onPublish func(topic string, msg message.Message) error
	mu        sync.Mutex
	messages  []message.Message
}

func NewPublisher(cfg Config) (*Publisher, error) {
	return &Publisher{onPublish: cfg.OnPublish}, nil
}

func (p *Publisher) Publish(_ context.Context, topic string, msg message.Message) error {
	if p.onPublish != nil {
		return p.onPublish(topic, msg)
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.messages = append(p.messages, msg)
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
	return nil
}

func (p *Publisher) Messages() []message.Message {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]message.Message, len(p.messages))
	copy(out, p.messages)
	return out
}
