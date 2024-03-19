package main

import (
	"context"
	"fmt"
	"github.com/ThreeDotsLabs/watermill"
	"github.com/pkg/errors"
	"github.com/redis/go-redis/v9"
	"github.com/stong1994/watermill-rediszset/pkg/rediszset"
	"time"
)

func main() {
	topic := "simple_topic"

	redisClient, err := redisClient()
	if err != nil {
		panic(err)
	}

	subscriber, err := rediszset.NewSubscriber(
		rediszset.SubscriberConfig{
			Client:       redisClient,
			Unmarshaller: rediszset.DefaultMarshallerUnmarshaller{},
			BlockMode:    rediszset.NotBlock,
			RestTime:     100 * time.Millisecond,
		},
		watermill.NewStdLogger(true, false),
	)
	if err != nil {
		panic(err)
	}
	messages, err := subscriber.Subscribe(context.Background(), topic)
	if err != nil {
		panic(err)
	}

	publisher, err := rediszset.NewPublisher(
		rediszset.PublisherConfig{
			Client:     redisClient,
			Marshaller: rediszset.DefaultMarshallerUnmarshaller{},
		},
		watermill.NewStdLogger(false, false),
	)
	if err != nil {
		panic(err)
	}

	msg := rediszset.NewMessage(watermill.NewShortUUID(), float64(0), []byte("hello"))

	err = publisher.Publish(topic, msg)
	if err != nil {
		panic(err)
	}

	received := <-messages
	score, err := rediszset.GetScore(received)
	if err != nil {
		panic(err)
	}
	msg.Ack()
	fmt.Printf("got msg: %s, score: %f\n", received.Payload, score)

}

func redisClient() (redis.UniversalClient, error) {
	client := redis.NewClient(&redis.Options{
		Addr:        "127.0.0.1:6379",
		DB:          0,
		ReadTimeout: -1,
		PoolTimeout: 10 * time.Minute,
	})
	err := client.Ping(context.Background()).Err()
	if err != nil {
		return nil, errors.Wrap(err, "redis simple connect fail")
	}
	return client, nil
}
