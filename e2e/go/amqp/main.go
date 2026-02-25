package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/relaymesh/relaybus/sdk/core/go/message"

	amqpadapter "github.com/relaymesh/relaybus/sdk/amqp/go"
	core "github.com/relaymesh/relaybus/sdk/core/go"
)

func main() {
	mode := flag.String("mode", "sub", "sub|pub")
	url := flag.String("url", "amqp://guest:guest@localhost:5672/", "")
	topic := flag.String("topic", "relaybus.alpha", "")
	exchange := flag.String("exchange", "relaybus.events", "")
	queue := flag.String("queue", "", "")
	flag.Parse()
	if *queue == "" {
		*queue = *topic + ".queue"
	}

	if *mode == "sub" {
		runSubscriber(*url, *topic, *exchange, *queue)
		return
	}

	runPublisher(*url, *topic, *exchange, *queue)
}

func runSubscriber(url, topic, exchange, queue string) {
	sub, err := amqpadapter.NewSubscriber(amqpadapter.SubscriberConfig{
		URL:                url,
		Exchange:           exchange,
		ExchangeType:       "topic",
		RoutingKeyTemplate: "{topic}",
		Queue:              queue,
		AutoAck:            false,
		MaxMessages:        1,
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

func runPublisher(url, topic, exchange, queue string) {
	pub, err := core.NewPublisher(core.Config{
		Destination: "amqp",
		AMQP: amqpadapter.Config{
			URL:                url,
			Exchange:           exchange,
			ExchangeType:       "topic",
			RoutingKeyTemplate: "{topic}",
			Queue:              queue,
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
