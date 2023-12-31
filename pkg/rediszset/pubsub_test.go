package rediszset_test

import (
	"context"
	"github.com/stong1994/watermill-rediszset/pkg/rediszset"
	"github.com/stretchr/testify/assert"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/pubsub/tests"

	"github.com/pkg/errors"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

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

func redisClientOrFail(t *testing.T) redis.UniversalClient {
	client, err := redisClient()
	require.NoError(t, err)
	return client
}

func newPubSub(t *testing.T, subConfig *rediszset.SubscriberConfig) (message.Publisher, message.Subscriber) {
	logger := watermill.NewStdLogger(true, false)

	publisher, err := rediszset.NewPublisher(
		rediszset.PublisherConfig{
			Client:     redisClientOrFail(t),
			Marshaller: rediszset.DefaultMarshallerUnmarshaller{},
		},
		watermill.NewStdLogger(false, false),
	)
	require.NoError(t, err)

	subscriber, err := rediszset.NewSubscriber(*subConfig, logger)
	require.NoError(t, err)

	return publisher, subscriber
}

//func TestPublishSubscribe(t *testing.T) {
//	features := tests.Features{
//		ConsumerGroups:                      false,
//		ExactlyOnceDelivery:                 false,
//		GuaranteedOrder:                     false,
//		GuaranteedOrderWithSingleSubscriber: true,
//		Persistent:                          true,
//		RestartServiceCommand:               []string{"docker", "restart", "redis"},
//		RequireSingleInstance:               false,
//		NewSubscriberReceivesOldMessages:    false,
//	}
//
//	tests.TestPubSub(t, features, createPubSub, nil)
//}

func TestSubscriber(t *testing.T) {
	topic := watermill.NewShortUUID()

	subscriber, err := rediszset.NewSubscriber(
		notBlockSubConfig(t),
		watermill.NewStdLogger(true, false),
	)
	require.NoError(t, err)
	messages, err := subscriber.Subscribe(context.Background(), topic)
	require.NoError(t, err)

	publisher, err := rediszset.NewPublisher(
		rediszset.PublisherConfig{
			Client:     redisClientOrFail(t),
			Marshaller: rediszset.DefaultMarshallerUnmarshaller{},
		},
		watermill.NewStdLogger(false, false),
	)
	require.NoError(t, err)

	var sentMsgs message.Messages
	for i := 0; i < 50; i++ {
		msg := rediszset.NewMessage(watermill.NewShortUUID(), float64(i), []byte(strconv.Itoa(i)))

		require.NoError(t, publisher.Publish(topic, msg))
		sentMsgs = append(sentMsgs, msg)
	}

	var receivedMsgs message.Messages
	for i := 0; i < 50; i++ {
		msg := <-messages
		if msg == nil {
			t.Fatal("msg nil")
		}
		require.Equal(t, strconv.Itoa(i), string(msg.Payload))
		score, err := rediszset.GetScore(msg)
		require.NoError(t, err)
		assert.Equal(t, float64(i), score)
		receivedMsgs = append(receivedMsgs, msg)
		msg.Ack()
	}
	tests.AssertAllMessagesReceived(t, sentMsgs, receivedMsgs)

	require.NoError(t, publisher.Close())
	require.NoError(t, subscriber.Close())
}

func TestErrorMsg(t *testing.T) {
	publisher, err := rediszset.NewPublisher(
		rediszset.PublisherConfig{
			Client:     redisClientOrFail(t),
			Marshaller: rediszset.DefaultMarshallerUnmarshaller{},
		},
		watermill.NewStdLogger(false, false),
	)
	require.NoError(t, err)

	err = publisher.Publish("topic", message.NewMessage("abc", nil))
	require.Contains(t, err.Error(), "should use rediszset.NewMessage since zset need score")
}

func TestErrorResponse(t *testing.T) {
	topic := "topic"
	subscriber, err := rediszset.NewSubscriber(
		notBlockSubConfig(t),
		watermill.NewStdLogger(true, false),
	)
	require.NoError(t, err)
	messages, err := subscriber.Subscribe(context.Background(), topic)
	require.NoError(t, err)

	publisher, err := rediszset.NewPublisher(
		rediszset.PublisherConfig{
			Client:     redisClientOrFail(t),
			Marshaller: rediszset.DefaultMarshallerUnmarshaller{},
		},
		watermill.NewStdLogger(false, false),
	)
	require.NoError(t, err)

	err = publisher.Publish(topic, rediszset.NewMessage("abc", 10, []byte("abc")))
	require.NoError(t, err)

	for i := 0; i < 10; i++ {
		msg := <-messages
		score, err := rediszset.GetScore(msg)
		require.NoError(t, err)
		assert.Equal(t, float64(10), score)
		assert.Equal(t, "abc", string(msg.Payload))
		if i == 9 {
			msg.Ack()
		} else {
			msg.Nack()
		}
	}
}

func TestMultiConsumer(t *testing.T) {
	topic := "topic"
	subscriber1, err := rediszset.NewSubscriber(
		notBlockSubConfig(t),
		watermill.NewStdLogger(true, false),
	)
	require.NoError(t, err)
	messages1, err := subscriber1.Subscribe(context.Background(), topic)
	require.NoError(t, err)

	subscriber2, err := rediszset.NewSubscriber(
		notBlockSubConfig(t),
		watermill.NewStdLogger(true, false),
	)
	require.NoError(t, err)
	messages2, err := subscriber2.Subscribe(context.Background(), topic)
	require.NoError(t, err)

	publisher, err := rediszset.NewPublisher(
		rediszset.PublisherConfig{
			Client:     redisClientOrFail(t),
			Marshaller: rediszset.DefaultMarshallerUnmarshaller{},
		},
		watermill.NewStdLogger(false, false),
	)
	require.NoError(t, err)

	for i := 0; i < 100; i++ {
		err = publisher.Publish(topic, rediszset.NewMessage("abc", float64(i), []byte(strconv.Itoa(i))))
		require.NoError(t, err)
	}

	counter := new(atomic.Uint64)

	go func() {
		for m := range messages1 {
			counter.Add(1)
			m.Ack()
		}
	}()
	go func() {
		for m := range messages2 {
			counter.Add(1)
			m.Ack()
		}
	}()

	require.Eventually(
		t,
		func() bool {
			return counter.Load() == 100
		},
		time.Second*5,
		time.Millisecond*100,
	)
}

func notBlockSubConfig(t *testing.T) rediszset.SubscriberConfig {
	return rediszset.SubscriberConfig{
		Client:       redisClientOrFail(t),
		Unmarshaller: rediszset.DefaultMarshallerUnmarshaller{},
		BlockMode:    rediszset.NotBlock,
		RestTime:     100 * time.Millisecond,
	}
}
