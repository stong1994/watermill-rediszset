package rediszset

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/pkg/errors"
	"github.com/redis/go-redis/v9"
)

type Locker interface {
	Lock() error
	Unlock()
}

type StrictSubscriber struct {
	config  StrictSubscriberConfig
	client  redis.UniversalClient
	logger  watermill.LoggerAdapter
	closing chan struct{}

	locker Locker

	closed     bool
	closeMutex sync.Mutex
}

// NewStrictSubscriber creates a new redis zset Subscriber.
func NewStrictSubscriber(config StrictSubscriberConfig, locker Locker, logger watermill.LoggerAdapter) (*StrictSubscriber, error) {
	config.setDefaults()

	if err := config.Validate(); err != nil {
		return nil, err
	}

	if logger == nil {
		logger = &watermill.NopLogger{}
	}
	if locker == nil {
		return nil, fmt.Errorf("locker can not be nil")
	}

	return &StrictSubscriber{
		config:  config,
		client:  config.Client,
		logger:  logger,
		locker:  locker,
		closing: make(chan struct{}),
	}, nil
}

type StrictSubscriberConfig struct {
	Client redis.UniversalClient

	Unmarshaller Unmarshaller

	// How long should we rest after got nothing
	RestTime time.Duration

	// After a failed consumption, the strictMessageHandler will receive a nack, and it is better to wait for some time before retrying.
	NackResendSleep time.Duration
}

func (sc *StrictSubscriberConfig) setDefaults() {
	if sc.Unmarshaller == nil {
		sc.Unmarshaller = DefaultMarshallerUnmarshaller{}
	}

	if sc.RestTime == 0 {
		sc.RestTime = DefaultRestTime
	}
	if sc.NackResendSleep == 0 {
		sc.NackResendSleep = NoSleep
	}
}

func (sc *StrictSubscriberConfig) Validate() error {
	if sc.Client == nil {
		return errors.New("redis client is empty")
	}
	return nil
}

func (s *StrictSubscriber) Subscribe(ctx context.Context, topic string) (<-chan *message.Message, error) {
	if s.closed {
		return nil, errors.New("subscriber closed")
	}

	logFields := watermill.LogFields{
		"provider": "redis",
		"topic":    topic,
	}
	s.logger.Info("Subscribing to redis zset topic", logFields)

	// we don't want to have buffered channel to not consume messsage from redis zset when consumer is not consuming
	output := make(chan *message.Message)

	consumeClosed, err := s.consumeMessages(ctx, topic, output, logFields)
	if err != nil {
		return nil, err
	}

	go func() {
		<-consumeClosed
		close(output)
	}()

	return output, nil
}

func (s *StrictSubscriber) consumeMessages(ctx context.Context, topic string, output chan *message.Message, logFields watermill.LogFields) (consumeMessageClosed chan struct{}, err error) {
	s.logger.Info("Starting consuming", logFields)

	ctx, cancel := context.WithCancel(ctx)
	go func() {
		select {
		case <-s.closing:
			s.logger.Debug("Closing subscriber, cancelling consumeMessages", logFields)
			cancel()
		case <-ctx.Done():
			// avoid goroutine leak
		}
	}()

	consumeMessageClosed, err = s.consumeZset(ctx, topic, output, logFields)
	if err != nil {
		s.logger.Debug(
			"Starting consume failed, cancelling context",
			logFields.Add(watermill.LogFields{"err": err}),
		)
		cancel()
		return nil, err
	}

	return consumeMessageClosed, nil
}

func (s *StrictSubscriber) consumeZset(ctx context.Context, topic string, output chan *message.Message, logFields watermill.LogFields) (chan struct{}, error) {
	consumeMessageClosed := make(chan struct{})

	go func() {
		defer close(consumeMessageClosed)

		for {
			select {
			case <-s.closing:
				return
			case <-ctx.Done():
				return
			default:
				if err := s.consume(ctx, topic, output, logFields); err != nil {
					return
				}
			}
		}
	}()

	return consumeMessageClosed, nil
}

func (s *StrictSubscriber) consume(ctx context.Context, topic string, output chan *message.Message, logFields watermill.LogFields) error {
	if err := s.locker.Lock(); err != nil {
		s.logger.Error("get lock failed", err, logFields)
		return err
	}
	defer s.locker.Unlock()
	data, err := s.getData(ctx, topic, logFields)
	if err != nil {
		return err
	}
	if len(data) == 0 {
		time.Sleep(s.config.RestTime)
		return nil
	}
	handler := s.createMessageHandler(output)
	if err = handler.processMessage(ctx, topic, data[0], logFields); err != nil {
		return err
	}
	return nil
}

func (s *StrictSubscriber) getData(ctx context.Context, topic string, logFields watermill.LogFields) ([]redis.Z, error) {
	data, err := s.client.ZRangeWithScores(ctx, topic, 0, 0).Result()
	if err != nil {
		s.logger.Error("read fail", err, logFields)
		return nil, err
	}
	return data, nil
}

func (s *StrictSubscriber) createMessageHandler(output chan *message.Message) strictMessageHandler {
	return strictMessageHandler{
		outputChannel:   output,
		rc:              s.client,
		unmarshaller:    s.config.Unmarshaller,
		nackResendSleep: s.config.NackResendSleep,
		logger:          s.logger,
		closing:         s.closing,
	}
}

func (s *StrictSubscriber) Close() error {
	s.closeMutex.Lock()
	defer s.closeMutex.Unlock()

	if s.closed {
		return nil
	}

	s.closed = true
	close(s.closing)

	if err := s.client.Close(); err != nil {
		return err
	}

	s.logger.Debug("Redis subscriber closed", nil)

	return nil
}

type strictMessageHandler struct {
	outputChannel chan<- *message.Message
	rc            redis.UniversalClient
	unmarshaller  Unmarshaller

	nackResendSleep time.Duration

	logger  watermill.LoggerAdapter
	closing chan struct{}
}

func (h *strictMessageHandler) processMessage(ctx context.Context, topic string, data redis.Z, messageLogFields watermill.LogFields) error {
	score, value := data.Score, data.Member.(string)
	receivedMsgLogFields := messageLogFields.Add(watermill.LogFields{
		"zscore": score,
		"topic":  topic,
	})

	h.logger.Trace("Received message from redis zset", receivedMsgLogFields)

	msg, err := h.unmarshaller.Unmarshal([]byte(value))
	if err != nil {
		return errors.Wrapf(err, "message unmarshal failed")
	}

	ctx, cancelCtx := context.WithCancel(ctx)
	msg.SetContext(ctx)
	defer cancelCtx()

	receivedMsgLogFields = receivedMsgLogFields.Add(watermill.LogFields{
		"message_uuid": msg.UUID,
	})

	select {
	case h.outputChannel <- msg:
		h.logger.Trace("Message sent to consumer", receivedMsgLogFields)
	case <-h.closing:
		h.logger.Trace("Closing, message discarded", receivedMsgLogFields)
		return nil
	case <-ctx.Done():
		h.logger.Trace("Closing, ctx cancelled before sent to consumer", receivedMsgLogFields)
		return nil
	}

	select {
	case <-msg.Acked():
		_, err := h.rc.ZRem(ctx, topic, value).Result()
		if err != nil {
			h.logger.Error("zrem faield", err, receivedMsgLogFields)
			return err
		}
		h.logger.Trace("Message Acked", receivedMsgLogFields)
	case <-msg.Nacked():
		h.logger.Trace("Message Nacked", receivedMsgLogFields)
		if h.nackResendSleep != NoSleep {
			time.Sleep(h.nackResendSleep)
		}
	case <-h.closing:
		h.logger.Trace("Closing, message discarded before ack", receivedMsgLogFields)
		return nil
	case <-ctx.Done():
		h.logger.Trace("Closing, ctx cancelled before ack", receivedMsgLogFields)
		return nil
	}
	return nil
}
