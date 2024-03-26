package rediszset_test

import (
	"context"
	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/lithammer/shortuuid/v3"
	"github.com/stong1994/watermill-rediszset/pkg/rediszset"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestStrictSubscriber_NoDataLose(t *testing.T) {
	topic := shortuuid.New()
	locker := memoryLocker()
	subscriber1, err := rediszset.NewStrictSubscriber(
		strictSubConfig(t),
		locker,
		watermill.NewStdLogger(true, false),
	)
	messages1, err := subscriber1.Subscribe(context.Background(), topic)
	require.NoError(t, err)
	subscriber2, err := rediszset.NewStrictSubscriber(
		strictSubConfig(t),
		locker,
		watermill.NewStdLogger(true, false),
	)
	require.NoError(t, err)
	messages2, err := subscriber2.Subscribe(context.Background(), topic)
	require.NoError(t, err)

	publisher, err := rediszset.NewPublisher(
		rediszset.PublisherConfig{
			Client:     redisClientOrFail(t),
			Marshaller: rediszset.WithoutScoreMarshallerUnmarshaller{},
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

	require.Eventuallyf(
		t,
		func() bool {
			remain, err := redisClientOrFail(t).ZCard(context.Background(), topic).Result()
			require.NoError(t, err)
			return 0 == remain && counter.Load() == 100
		},
		time.Second*5,
		time.Millisecond*100,
		"want %d got %d",
		100,
		counter.Load(),
	)
}

func TestStrictSubscriber_NoConsumeBefore(t *testing.T) {
	topic := shortuuid.New()
	locker := memoryLocker()
	subscriber, err := rediszset.NewStrictSubscriber(
		rediszset.StrictSubscriberConfig{
			Client:       redisClientOrFail(t),
			Unmarshaller: rediszset.WithoutScoreMarshallerUnmarshaller{},
			RestTime:     100 * time.Millisecond,
			ConsumeFn: func(topic string) (start, end string, err error) {
				return "-inf", strconv.FormatInt(time.Now().Unix(), 10), nil
			},
		},
		locker,
		watermill.NewStdLogger(true, false),
	)
	messages1, err := subscriber.Subscribe(context.Background(), topic)
	require.NoError(t, err)

	publisher, err := rediszset.NewPublisher(
		rediszset.PublisherConfig{
			Client:     redisClientOrFail(t),
			Marshaller: rediszset.WithoutScoreMarshallerUnmarshaller{},
		},
		watermill.NewStdLogger(false, false),
	)
	require.NoError(t, err)

	err = publisher.Publish(topic, rediszset.NewMessage("abc", float64(time.Now().Add(time.Second*10).Unix()), []byte("hi")))
	require.NoError(t, err)

	counter := new(atomic.Uint64)

	go func() {
		for m := range messages1 {
			counter.Add(1)
			m.Ack()
		}
	}()

	require.Eventuallyf(
		t,
		func() bool {
			remain, err := redisClientOrFail(t).ZCard(context.Background(), topic).Result()
			require.NoError(t, err)
			t.Log("remain", remain, "counter", counter.Load())
			return 1 == remain && counter.Load() == 0
		},
		time.Second*5,
		time.Millisecond*100,
		"want 0 got %d",
		counter.Load(),
	)

	require.Eventuallyf(
		t,
		func() bool {
			remain, err := redisClientOrFail(t).ZCard(context.Background(), topic).Result()
			require.NoError(t, err)
			return 0 == remain && counter.Load() == 1
		},
		time.Second*10,
		time.Millisecond*100,
		"want 1 got %d",
		counter.Load(),
	)
}

func TestStrictSubscriber_Remove(t *testing.T) {
	topic := watermill.NewShortUUID()

	publisher, err := rediszset.NewPublisher(
		rediszset.PublisherConfig{
			Client:     redisClientOrFail(t),
			Marshaller: rediszset.WithoutScoreMarshallerUnmarshaller{},
		},
		watermill.NewStdLogger(true, true),
	)
	require.NoError(t, err)

	var sentMsgs message.Messages
	for i := 0; i < 50; i++ {
		msg := rediszset.NewMessage(strconv.Itoa(i), float64(i), []byte(strconv.Itoa(i)))

		require.NoError(t, publisher.Publish(topic, msg))
		sentMsgs = append(sentMsgs, msg)
	}

	var removedMsgs message.Messages
	for i := 0; i < 40; i++ {
		msg := rediszset.NewMessage(strconv.Itoa(i), float64(i), []byte(strconv.Itoa(i)))
		require.NoError(t, publisher.Remove(topic, msg))
		removedMsgs = append(removedMsgs, msg)
	}

	subscriber, err := rediszset.NewStrictSubscriber(
		rediszset.StrictSubscriberConfig{
			Client:       redisClientOrFail(t),
			Unmarshaller: rediszset.WithoutScoreMarshallerUnmarshaller{},
			RestTime:     100 * time.Millisecond,
		},
		memoryLocker(),
		watermill.NewStdLogger(true, true),
	)
	require.NoError(t, err)
	messages, err := subscriber.Subscribe(context.Background(), topic)
	require.NoError(t, err)

	var receivedMsgs message.Messages
	for i := 40; i < 50; i++ {
		msg := <-messages
		if msg == nil {
			t.Fatal("msg nil")
		}
		require.Equal(t, strconv.Itoa(i), string(msg.Payload))
		//score, err := rediszset.GetScore(msg)
		require.NoError(t, err)
		//assert.Equal(t, float64(i), score)
		receivedMsgs = append(receivedMsgs, msg)
		msg.Ack()
	}

	assert.Equal(t, 10, len(receivedMsgs))
	for i, v := range receivedMsgs {
		val, err := strconv.ParseFloat(string(v.Payload), 64)
		assert.NoError(t, err)
		assert.Equal(t, float64(40+i), val)
	}
	//for i, v := range receivedMsgs {
	//score, err := rediszset.GetScore(v)
	//assert.NoError(t, err)
	//assert.Equal(t, float64(40+i), score)
	//}

	require.NoError(t, publisher.Close())
	require.NoError(t, subscriber.Close())
}

func TestStrictSubscriber_Lock(t *testing.T) {
	topic := shortuuid.New()
	locker := memoryLocker()

	publisher, err := rediszset.NewPublisher(
		rediszset.PublisherConfig{
			Client:     redisClientOrFail(t),
			Marshaller: rediszset.WithoutScoreMarshallerUnmarshaller{},
		},
		watermill.NewStdLogger(false, false),
	)
	require.NoError(t, err)
	for i := 0; i < 2; i++ {
		err = publisher.Publish(topic, rediszset.NewMessage("abc", float64(i), []byte(strconv.Itoa(i))))
		require.NoError(t, err)
	}

	expire := time.Second * 10
	subscriber1, err := rediszset.NewStrictSubscriber(
		strictSubConfig(t),
		locker,
		watermill.NewStdLogger(true, false),
	)
	messages1, err := subscriber1.Subscribe(context.Background(), topic)
	require.NoError(t, err)

	go func() {
		for range messages1 {
			time.Sleep(expire)
			// will not ack
		}
	}()
	time.Sleep(time.Second) // make sure message1 have got the message

	subscriber2, err := rediszset.NewStrictSubscriber(
		strictSubConfig(t),
		locker,
		watermill.NewStdLogger(true, false),
	)
	require.NoError(t, err)
	messages2, err := subscriber2.Subscribe(context.Background(), topic)
	require.NoError(t, err)
	counter := new(atomic.Uint64)

	go func() {
		for range messages2 {
			counter.Add(1)
		}
	}()

	time.Sleep(expire / 2)
	require.Equal(t, uint64(0), counter.Load())
}

func TestStrictSubscriber_Nack(t *testing.T) {
	topic := shortuuid.New()
	locker := memoryLocker()

	publisher, err := rediszset.NewPublisher(
		rediszset.PublisherConfig{
			Client:     redisClientOrFail(t),
			Marshaller: rediszset.WithoutScoreMarshallerUnmarshaller{},
		},
		watermill.NewStdLogger(false, false),
	)
	require.NoError(t, err)
	err = publisher.Publish(topic, rediszset.NewMessage("abc", 0, []byte("abc")))
	require.NoError(t, err)

	subscriber, err := rediszset.NewStrictSubscriber(
		strictSubConfig(t),
		locker,
		watermill.NewStdLogger(true, false),
	)
	messages, err := subscriber.Subscribe(context.Background(), topic)
	require.NoError(t, err)
	count := 0
	for i := 0; i < 10; i++ {
		msg := <-messages
		require.Equal(t, "abc", string(msg.Payload))
		msg.Nack()
		count++
	}
	require.Equal(t, 10, count)
}

type memlocker struct {
	locker sync.Mutex
}

func (m *memlocker) Lock(ctx context.Context) (bool, error) {
	m.locker.Lock()
	return true, nil
}

func (m *memlocker) Unlock(ctx context.Context) error {
	m.locker.Unlock()
	return nil
}

func memoryLocker() rediszset.Locker {
	return &memlocker{}
}

func strictSubConfig(t *testing.T) rediszset.StrictSubscriberConfig {
	return rediszset.StrictSubscriberConfig{
		Client:       redisClientOrFail(t),
		Unmarshaller: rediszset.WithoutScoreMarshallerUnmarshaller{},
		RestTime:     100 * time.Millisecond,
	}
}
