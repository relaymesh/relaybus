package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/relaymesh/relaybus/sdk/core/go/message"

	core "github.com/relaymesh/relaybus/sdk/core/go"
	natsadapter "github.com/relaymesh/relaybus/sdk/nats/go"
)

func main() {
	mode := flag.String("mode", "sub", "sub|pub")
	url := flag.String("url", "nats://localhost:4222", "")
	subjectPrefix := flag.String("prefix", "relaybus", "")
	topic := flag.String("topic", "alpha", "")
	flag.Parse()

	if *mode == "sub" {
		runSubscriber(*url, *subjectPrefix, *topic)
		return
	}

	runPublisher(*url, *subjectPrefix, *topic)
}

func runSubscriber(url, prefix, topic string) {
	sub, err := natsadapter.NewSubscriber(natsadapter.SubscriberConfig{
		URL:           url,
		SubjectPrefix: prefix,
		MaxMessages:   1,
		Handler: func(_ context.Context, msg message.Message) error {
			fmt.Printf("received id=%s topic=%s payload=%s\n", msg.ID, msg.Topic, string(msg.Payload))
			return nil
		}})
	if err != nil {
		log.Fatalf("subscriber: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := sub.Start(ctx, topic); err != nil {
		log.Fatalf("start: %v", err)
	}
}

func runPublisher(url, prefix, topic string) {
	pub, err := core.NewPublisher(core.Config{
		Destination: "nats",
		NATS: natsadapter.Config{
			URL:           url,
			SubjectPrefix: prefix,
		},
	})
	if err != nil {
		log.Fatalf("publisher: %v", err)
	}
	defer pub.Close()

	msg := core.Message{
		Topic:    topic,
		Payload:  []byte("hello from go"),
		Metadata: map[string]string{"lang": "go"},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := pub.Publish(ctx, topic, msg); err != nil {
		log.Fatalf("publish: %v", err)
	}
	fmt.Println("published")
}

func joinSubject(prefix, topic string) string {
	if prefix == "" {
		return topic
	}
	if prefix[len(prefix)-1] == '.' {
		return prefix + topic
	}
	return prefix + "." + topic
}
