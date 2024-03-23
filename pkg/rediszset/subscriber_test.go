package rediszset_test

import (
	"context"
	"github.com/ThreeDotsLabs/watermill"
	"github.com/stong1994/watermill-rediszset/pkg/rediszset"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestSubscriber_Timeout(t *testing.T) {
	topic := watermill.NewShortUUID()
	subscriber, err := rediszset.NewSubscriber(
		rediszset.SubscriberConfig{
			Client:       redisClientOrFail(t),
			Unmarshaller: rediszset.DefaultMarshallerUnmarshaller{},
			BlockMode:    rediszset.Block,
			BlockTime:    time.Second,
		},
		watermill.NewStdLogger(true, false),
	)
	require.NoError(t, err)

	msg, err := subscriber.Subscribe(context.Background(), topic)

	select {
	case <-time.After(time.Second * 10):
		t.Log("not receive any msg")
	case <-msg:
		t.Fatal("should receive nothing")
	}
}

func blockSubConfig(t *testing.T) rediszset.SubscriberConfig {
	return rediszset.SubscriberConfig{
		Client:       redisClientOrFail(t),
		Unmarshaller: rediszset.DefaultMarshallerUnmarshaller{},
		BlockMode:    rediszset.Block,
		BlockTime:    rediszset.DefaultBlockTime,
	}
}
