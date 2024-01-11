package rediszset

import (
	"context"
	"fmt"
	"sync"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/pkg/errors"
	"github.com/redis/go-redis/v9"
)

type Publisher struct {
	config PublisherConfig
	client redis.UniversalClient
	logger watermill.LoggerAdapter

	closed     bool
	closeMutex sync.Mutex
}

// NewPublisher creates a new redis stream Publisher.
func NewPublisher(config PublisherConfig, logger watermill.LoggerAdapter) (*Publisher, error) {
	config.setDefaults()

	if err := config.Validate(); err != nil {
		return nil, err
	}

	if logger == nil {
		logger = &watermill.NopLogger{}
	}

	return &Publisher{
		config: config,
		client: config.Client,
		logger: logger,
		closed: false,
	}, nil
}

type PublisherConfig struct {
	Client     redis.UniversalClient
	Marshaller Marshaller
}

func (c *PublisherConfig) setDefaults() {
	if c.Marshaller == nil {
		c.Marshaller = DefaultMarshallerUnmarshaller{}
	}
}

func (c *PublisherConfig) Validate() error {
	if c.Client == nil {
		return errors.New("redis client is empty")
	}
	return nil
}

// Publish publishes message to redis zset
//
// Publish is blocking and waits for redis response.
// When any of messages delivery fails - function is interrupted.
func (p *Publisher) Publish(topic string, msgs ...*message.Message) error {
	if p.closed {
		return errors.New("publisher closed")
	}

	logFields := make(watermill.LogFields, 3)
	logFields["topic"] = topic

	for _, msg := range msgs {
		logFields["message_uuid"] = msg.UUID
		p.logger.Trace("Sending message to redis", logFields)

		values, err := p.config.Marshaller.Marshal(topic, msg)
		if err != nil {
			return errors.Wrapf(err, "cannot marshal message %s", msg.UUID)
		}

		score, err := GetScore(msg)
		if err != nil {
			return errors.Wrapf(err, "cannot get score from message %s", msg.UUID)
		}

		logFields["zadd_score"] = score
		_, err = p.client.ZAdd(context.Background(), topic, redis.Z{
			Score:  score,
			Member: values,
		}).Result()
		if err != nil {
			return errors.Wrapf(err, "cannot zadd message %s", msg.UUID)
		}

		p.logger.Trace("Message sent to redis", logFields)
	}

	return nil
}

// Remove message from redis zset
// Pay attention to Marshaller. It should not contain trace info. You can use WithoutTraceMarshallerUnmarshaller.
func (p *Publisher) Remove(topic string, msgs ...*message.Message) error {
	if p.closed {
		return errors.New("publisher closed")
	}

	logFields := make(watermill.LogFields, 3)
	logFields["topic"] = topic

	for _, msg := range msgs {
		p.logger.Trace("Sending message to redis", logFields)

		values, err := p.config.Marshaller.Marshal(topic, msg)
		if err != nil {
			return errors.Wrapf(err, "cannot marshal message %s", msg.UUID)
		}
		logFields["zrem_member"] = string(values)

		cnt, err := p.client.ZRem(context.Background(), topic, string(values)).Result()
		if err != nil {
			return errors.Wrapf(err, "cannot rem message %s", string(values))
		}
		p.logger.Trace(fmt.Sprintf("Message removed from redis, cnt: %d", cnt), logFields)
	}
	return nil
}

func (p *Publisher) Close() error {
	p.closeMutex.Lock()
	defer p.closeMutex.Unlock()

	if p.closed {
		return nil
	}
	p.closed = true

	if err := p.client.Close(); err != nil {
		return err
	}

	return nil
}
