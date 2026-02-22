package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"time"

	core "relaybus/sdk/core/go"
	kafkaadapter "relaybus/sdk/kafka/go"

	"github.com/segmentio/kafka-go"

	"relaybus/sdk/core/go/message"
)

func main() {
	mode := flag.String("mode", "sub", "sub|pub")
	broker := flag.String("broker", "localhost:29092", "")
	topic := flag.String("topic", "relaybus.alpha", "")
	flag.Parse()

	if *mode == "sub" {
		runSubscriber(*broker, *topic)
		return
	}

	runPublisher(*broker, *topic)
}

func runSubscriber(broker, topic string) {
	ensureTopic(broker, topic)

	sub, err := kafkaadapter.NewSubscriber(kafkaadapter.SubscriberConfig{
		Broker:      broker,
		GroupID:     "relaybus-e2e",
		MaxMessages: 1,
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

func runPublisher(broker, topic string) {
	ensureTopic(broker, topic)
	pub, err := core.NewPublisher(core.Config{
		Destination: "kafka",
		Kafka: kafkaadapter.Config{
			Broker:      broker,
			TopicPrefix: "",
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

func ensureTopic(broker, topic string) {
	conn, err := kafka.Dial("tcp", broker)
	if err != nil {
		log.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	_ = conn.CreateTopics(kafka.TopicConfig{Topic: topic, NumPartitions: 1, ReplicationFactor: 1})
}
