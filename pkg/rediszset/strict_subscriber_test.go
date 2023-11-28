package rediszset_test

import (
	"context"
	"github.com/ThreeDotsLabs/watermill"
	"github.com/lithammer/shortuuid/v3"
	"github.com/stong1994/watermill-rediszset/pkg/rediszset"
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

func TestStrictSubscriber_Lock(t *testing.T) {
	topic := shortuuid.New()
	locker := memoryLocker()

	publisher, err := rediszset.NewPublisher(
		rediszset.PublisherConfig{
			Client:     redisClientOrFail(t),
			Marshaller: rediszset.DefaultMarshallerUnmarshaller{},
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

type memlocker struct {
	locker sync.Mutex
}

func (m *memlocker) Lock() error {
	m.locker.Lock()
	return nil
}

func (m *memlocker) Unlock() {
	m.locker.Unlock()
}

func memoryLocker() rediszset.Locker {
	return &memlocker{}
}

func strictSubConfig(t *testing.T) rediszset.StrictSubscriberConfig {
	return rediszset.StrictSubscriberConfig{
		Client:       redisClientOrFail(t),
		Unmarshaller: rediszset.DefaultMarshallerUnmarshaller{},
		RestTime:     100 * time.Millisecond,
	}
}
