package main

import (
	"context"
	"fmt"
	"github.com/google/uuid"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-amqp/v2/pkg/amqp"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/stong1994/watermill-rediszset/pkg/rediszset"
)

const (
	redisTopic    = "redis_timer_topic"
	rabbitmqTopic = "rabbitmq_timer_topic"
)

func main() {
	redisPub, redisSub := redisPubSub()
	rabbitmqPub, rabbitmaSub := rabbitmqPubSub()
	router := run(redisSub, rabbitmqPub, rabbitmaSub)
	<-router.Running()
	now := time.Now()
	err := redisPub.Publish(redisTopic, rediszset.NewMessage(uuid.NewString(), float64(now.Add(time.Second*5).Unix()), []byte("hello world")))
	if err != nil {
		panic(err)
	}
	time.Sleep(time.Second * 10)
	router.Close()
}

func run(
	redisSub *rediszset.StrictSubscriber,
	rabbitmqPub message.Publisher,
	rabbitmqSub message.Subscriber,
) *message.Router {
	logger := watermill.NewStdLogger(false, false)
	router, err := message.NewRouter(message.RouterConfig{}, logger)
	if err != nil {
		panic(err)
	}

	router.AddHandler(
		"convert_data_from_redis_to_rabbitmq",
		redisTopic,
		redisSub,
		rabbitmqTopic,
		rabbitmqPub,
		func(msg *message.Message) ([]*message.Message, error) {
			logger.Info("get task from redis", map[string]any{"payload": string(msg.Payload)})
			tm, err := rediszset.GetScore(msg)
			if err != nil {
				return nil, err
			}
			if tm >= float64(time.Now().Unix()) {
				logger.Info("not at now", nil)
				return nil, fmt.Errorf("not at now, task time is %s", time.Unix(int64(tm), 0).Format(time.DateTime))
			}
			return []*message.Message{msg}, nil
		},
	)

	router.AddNoPublisherHandler(
		"handle_task",
		rabbitmqTopic,
		rabbitmqSub,
		func(msg *message.Message) error {
			logger.Info("get task from rabbitmq", map[string]any{"payload": string(msg.Payload), "recv time": time.Now().String()})
			return nil
		},
	)

	go func() {
		err = router.Run(context.Background())
		if err != nil {
			panic(err)
		}
	}()

	return router
}

func rabbitmqPubSub() (*amqp.Publisher, *amqp.Subscriber) {
	config := amqp.Config{
		Connection: amqp.ConnectionConfig{
			AmqpURI: "amqp://guest:guest@127.0.0.1:5672/",
		},
		Marshaler: amqp.DefaultMarshaler{},
		Exchange: amqp.ExchangeConfig{
			Type:         "topic",
			GenerateName: func(topic string) string { return topic },
		},
		Publish: amqp.PublishConfig{
			GenerateRoutingKey: func(topic string) string { return topic },
		},
		QueueBind: amqp.QueueBindConfig{
			GenerateRoutingKey: func(topic string) string { return topic },
		},
		Queue: amqp.QueueConfig{
			GenerateName: amqp.GenerateQueueNameTopicName,
		},
		TopologyBuilder: &amqp.DefaultTopologyBuilder{},
	}
	logger := watermill.NewStdLogger(false, false)
	pub, err := amqp.NewPublisher(config, logger)
	if err != nil {
		panic(err)
	}
	sub, err := amqp.NewSubscriber(config, logger)
	if err != nil {
		panic(err)
	}
	return pub, sub
}

func redisPubSub() (*rediszset.Publisher, *rediszset.StrictSubscriber) {
	logger := watermill.NewStdLogger(false, false)
	client := redis.NewClient(&redis.Options{
		Addr:        "127.0.0.1:6379",
		DB:          0,
		ReadTimeout: -1,
		PoolTimeout: 10 * time.Minute,
	})
	err := client.Ping(context.Background()).Err()
	if err != nil {
		panic(err)
	}
	pub, err := rediszset.NewPublisher(
		rediszset.PublisherConfig{
			Client:     client,
			Marshaller: rediszset.WithoutScoreMarshallerUnmarshaller{},
		},
		logger,
	)
	if err != nil {
		panic(err)
	}
	sub, err := rediszset.NewStrictSubscriber(
		rediszset.StrictSubscriberConfig{
			Client:          client,
			Unmarshaller:    rediszset.WithoutScoreMarshallerUnmarshaller{},
			RestTime:        time.Second,
			NackResendSleep: time.Second,
		},
		SimpleLocker{client: client},
		logger,
	)
	if err != nil {
		panic(err)
	}
	return pub, sub
}

type SimpleLocker struct {
	client redis.UniversalClient
}

func (l SimpleLocker) Lock(ctx context.Context) error {
	for {
		got, err := l.client.SetNX(ctx, "lock_key", 1, time.Second).Result()
		if err != nil {
			return err
		}
		if got {
			return nil
		}
	}
}

func (l SimpleLocker) Unlock(ctx context.Context) error {
	return l.client.Del(ctx, "lock_key").Err()
}
