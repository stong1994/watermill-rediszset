package rediszset

import (
	"context"
	"sync"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/pkg/errors"
	"github.com/redis/go-redis/v9"
)

type BlockMode int

const (
	Block BlockMode = iota
	NotBlock
)

const (
	// NoSleep can be set to SubscriberConfig.NackResendSleep
	NoSleep time.Duration = -1

	DefaultBlockTime = time.Millisecond * 100

	DefaultRestTime = time.Millisecond * 100
)

type Subscriber struct {
	config        SubscriberConfig
	client        Client
	logger        watermill.LoggerAdapter
	closing       chan struct{}
	subscribersWg sync.WaitGroup

	closed     bool
	closeMutex sync.Mutex
}

// NewSubscriber creates a new redis zset Subscriber.
func NewSubscriber(config SubscriberConfig, logger watermill.LoggerAdapter) (*Subscriber, error) {
	config.setDefaults()

	if err := config.Validate(); err != nil {
		return nil, err
	}

	if logger == nil {
		logger = &watermill.NopLogger{}
	}
	result, err := config.Client.Info(context.Background()).Result()
	if err != nil {
		return nil, err
	}
	version, err := getVersion(result)
	if err != nil {
		return nil, err
	}
	client := NewClient(config.Client, version)
	return &Subscriber{
		config:  config,
		client:  client,
		logger:  logger,
		closing: make(chan struct{}),
	}, nil
}

type SubscriberConfig struct {
	Client redis.UniversalClient

	Unmarshaller Unmarshaller

	// Block if there are no members to pop from sorted set if BlockMode is Block
	BlockMode BlockMode

	// Block to wait next redis zset message, only works while BlockMode is Block
	BlockTime time.Duration

	// How long should we rest after got nothing, only works while BlockMode is NotBlock
	RestTime time.Duration

	// After a failed consumption, the messageHandler will receive a nack, and it is better to wait for some time before retrying.
	NackResendSleep time.Duration
}

func (sc *SubscriberConfig) setDefaults() {
	if sc.Unmarshaller == nil {
		sc.Unmarshaller = DefaultMarshallerUnmarshaller{}
	}

	if sc.BlockMode == Block {
		if sc.RestTime == 0 {
			sc.RestTime = DefaultRestTime
		}
	}

	if sc.BlockMode == Block {
		if sc.BlockTime == 0 {
			sc.BlockTime = DefaultBlockTime
		}
	}

	if sc.NackResendSleep == 0 {
		sc.NackResendSleep = NoSleep
	}
}

func (sc *SubscriberConfig) Validate() error {
	if sc.Client == nil {
		return errors.New("redis client is empty")
	}
	return nil
}

func (s *Subscriber) Subscribe(ctx context.Context, topic string) (<-chan *message.Message, error) {
	if s.closed {
		return nil, errors.New("subscriber closed")
	}

	s.subscribersWg.Add(1)

	logFields := watermill.LogFields{
		"provider": "redis",
		"topic":    topic,
	}
	s.logger.Info("Subscribing to redis zset topic", logFields)

	// we don't want to have buffered channel to not consume messsage from redis zset when consumer is not consuming
	output := make(chan *message.Message)

	consumeClosed, err := s.consumeMessages(ctx, topic, output, logFields)
	if err != nil {
		s.subscribersWg.Done()
		return nil, err
	}

	go func() {
		<-consumeClosed
		close(output)
		s.subscribersWg.Done()
	}()

	return output, nil
}

func (s *Subscriber) consumeMessages(ctx context.Context, topic string, output chan *message.Message, logFields watermill.LogFields) (consumeMessageClosed chan struct{}, err error) {
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

func (s *Subscriber) consumeZset(ctx context.Context, topic string, output chan *message.Message, logFields watermill.LogFields) (chan struct{}, error) {
	messageHandler := s.createMessageHandler(output)
	consumeMessageClosed := make(chan struct{})

	go func() {
		defer close(consumeMessageClosed)

		readChannel := make(chan redis.Z, 1)
		go s.read(ctx, topic, readChannel, logFields)

		for {
			select {
			case z, ok := <-readChannel:
				if !ok {
					s.logger.Debug("readChannel is closed, stopping", logFields)
					return
				}
				if err := messageHandler.processMessage(ctx, topic, z, logFields); err != nil {
					s.logger.Error("processMessage fail", err, logFields)
					return
				}
			case <-s.closing:
				s.logger.Debug("Subscriber is closing, stopping readZset", logFields)
				return
			case <-ctx.Done():
				s.logger.Debug("Ctx was cancelled, stopping readZset", logFields)
				return
			}
		}
	}()

	return consumeMessageClosed, nil
}

func (s *Subscriber) read(ctx context.Context, topic string, readChannel chan<- redis.Z, logFields watermill.LogFields) {
	defer close(readChannel)

	popFn := s.popFn(ctx, topic, logFields)

	for {
		select {
		case <-s.closing:
			return
		case <-ctx.Done():
			return
		default:
			zs, err := popFn()
			if err != nil {
				return
			}
			if len(zs) == 0 {
				if s.config.BlockMode == NotBlock {
					time.Sleep(s.config.RestTime) // rest for a while
				}
				continue
			}
			select {
			case <-s.closing:
				return
			case <-ctx.Done():
				return
			case readChannel <- zs[0]:
			}
		}
	}
}

func (s *Subscriber) popFn(ctx context.Context, topic string, logFields watermill.LogFields) func() ([]redis.Z, error) {
	switch s.config.BlockMode {
	case Block:
		return func() ([]redis.Z, error) {
			zWithKey, err := s.client.BZPopMin(ctx, s.config.BlockTime, topic)
			if err != nil {
				if errors.Is(err, redis.Nil) {
					return nil, nil
				}
				s.logger.Error("read fail", err, logFields)
				return nil, err
			}
			if zWithKey == nil {
				return nil, nil
			}
			return []redis.Z{zWithKey.Z}, nil
		}

	case NotBlock:
		return func() ([]redis.Z, error) {
			zs, err := s.client.ZPopMin(ctx, topic)
			if err != nil {
				if errors.Is(err, redis.Nil) {
					return nil, nil
				}
				s.logger.Error("read fail", err, logFields)
				return nil, nil
			}
			return zs, nil
		}
	default:
		panic("unkonwn block mode")
	}
}

func (s *Subscriber) createMessageHandler(output chan *message.Message) messageHandler {
	return messageHandler{
		outputChannel:   output,
		unmarshaller:    s.config.Unmarshaller,
		logger:          s.logger,
		closing:         s.closing,
		nackResendSleep: s.config.NackResendSleep,
	}
}

func (s *Subscriber) Close() error {
	s.closeMutex.Lock()
	defer s.closeMutex.Unlock()

	if s.closed {
		return nil
	}

	s.closed = true
	close(s.closing)
	s.subscribersWg.Wait()

	if err := s.client.Close(); err != nil {
		return err
	}

	s.logger.Debug("Redis zset subscriber closed", nil)

	return nil
}

type messageHandler struct {
	outputChannel chan<- *message.Message
	unmarshaller  Unmarshaller

	nackResendSleep time.Duration

	logger  watermill.LoggerAdapter
	closing chan struct{}
}

func (h *messageHandler) processMessage(ctx context.Context, topic string, z redis.Z, messageLogFields watermill.LogFields) error {
	receivedMsgLogFields := messageLogFields.Add(watermill.LogFields{
		"zscore": z.Score,
	})

	h.logger.Trace("Received message from redis zset", receivedMsgLogFields)

	msg, err := h.unmarshaller.Unmarshal([]byte(z.Member.(string)))
	if err != nil {
		return errors.Wrapf(err, "message unmarshal failed")
	}

	ctx, cancelCtx := context.WithCancel(ctx)
	msg.SetContext(ctx)
	defer cancelCtx()

	receivedMsgLogFields = receivedMsgLogFields.Add(watermill.LogFields{
		"message_uuid": msg.UUID,
		"topic":        topic,
	})

ResendLoop:
	for {
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
			h.logger.Trace("Message Acked", receivedMsgLogFields)
			break ResendLoop
		case <-msg.Nacked():
			h.logger.Trace("Message Nacked", receivedMsgLogFields)

			// reset acks, etc.
			msg = msg.Copy()
			if h.nackResendSleep != NoSleep {
				time.Sleep(h.nackResendSleep)
			}

			continue ResendLoop
		case <-h.closing:
			h.logger.Trace("Closing, message discarded before ack", receivedMsgLogFields)
			return nil
		case <-ctx.Done():
			h.logger.Trace("Closing, ctx cancelled before ack", receivedMsgLogFields)
			return nil
		}
	}

	return nil
}
