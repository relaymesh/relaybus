package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/relaymesh/relaybus/sdk/core/go/message"

	core "github.com/relaymesh/relaybus/sdk/core/go"
	httpadapter "github.com/relaymesh/relaybus/sdk/http/go"
)

func main() {
	mode := flag.String("mode", "sub", "sub|pub")
	addr := flag.String("addr", ":8088", "")
	endpoint := flag.String("endpoint", "http://localhost:8088/{topic}", "")
	topic := flag.String("topic", "relaybus.alpha", "")
	flag.Parse()

	if *mode == "sub" {
		runSubscriber(*addr)
		return
	}

	runPublisher(*endpoint, *topic)
}

func runSubscriber(addr string) {
	sub, err := httpadapter.NewSubscriber(httpadapter.SubscriberConfig{
		Address: addr,
		Handler: func(_ context.Context, msg message.Message) error {
			fmt.Printf("received id=%s topic=%s payload=%s\n", msg.ID, msg.Topic, string(msg.Payload))
			return nil
		}})
	if err != nil {
		log.Fatalf("subscriber: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := sub.Start(ctx); err != nil && err != context.DeadlineExceeded {
		log.Fatalf("start: %v", err)
	}
}

func runPublisher(endpoint, topic string) {
	pub, err := core.NewPublisher(core.Config{
		Destination: "http",
		HTTP: httpadapter.Config{
			Endpoint: endpoint,
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
