// Package inmemorykafka provides an in-memory Kafka implementation for testing.
package inmemorykafka

import (
	"context"
	"sync"
	"time"

	"github.com/bsv-blockchain/teranode/errors"
)

// Message represents a Kafka message.
type Message struct {
	Topic     string
	Key       []byte
	Value     []byte
	Offset    int64
	Partition int32
	Timestamp time.Time
}

// InMemoryBroker is the core in-memory message broker.
type InMemoryBroker struct {
	topics map[string]*Topic
	mu     sync.RWMutex
}

// Topic holds messages and consumer channels for a specific topic.
type Topic struct {
	messages  []*Message
	consumers []chan *Message
	mu        sync.RWMutex
}

// NewInMemoryBroker creates a new instance of the in-memory broker.
func NewInMemoryBroker() *InMemoryBroker {
	return &InMemoryBroker{
		topics: make(map[string]*Topic),
	}
}

// Produce sends a message to the specified topic and notifies consumers.
func (b *InMemoryBroker) Produce(ctx context.Context, topic string, key []byte, value []byte) error {
	b.mu.RLock()
	t, ok := b.topics[topic]
	b.mu.RUnlock()

	if !ok {
		b.mu.Lock()
		if _, exists := b.topics[topic]; !exists {
			b.topics[topic] = &Topic{
				messages:  make([]*Message, 0),
				consumers: make([]chan *Message, 0),
			}
		}
		t = b.topics[topic]
		b.mu.Unlock()
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	msg := &Message{
		Topic:     topic,
		Key:       key,
		Value:     value,
		Offset:    int64(len(t.messages)),
		Partition: 0,
		Timestamp: time.Now(),
	}
	t.messages = append(t.messages, msg)

	// Broadcast to all consumers
	for _, ch := range t.consumers {
		select {
		case ch <- msg:
		default:
		}
	}

	return nil
}

// Topics returns a list of topic names managed by the broker.
func (b *InMemoryBroker) Topics() []string {
	b.mu.Lock()
	defer b.mu.Unlock()
	topics := make([]string, 0, len(b.topics))
	for topic := range b.topics {
		topics = append(topics, topic)
	}
	return topics
}

// --- Shared Broker Singleton ---

var sharedBroker *InMemoryBroker
var brokerOnce sync.Once

// GetSharedBroker returns the singleton InMemoryBroker instance.
func GetSharedBroker() *InMemoryBroker {
	brokerOnce.Do(func() {
		sharedBroker = NewInMemoryBroker()
	})
	return sharedBroker
}

// --- Sync Producer ---

// InMemorySyncProducer implements a synchronous producer for testing.
type InMemorySyncProducer struct {
	broker *InMemoryBroker
}

// NewInMemorySyncProducer creates a new in-memory sync producer.
func NewInMemorySyncProducer(broker *InMemoryBroker) *InMemorySyncProducer {
	return &InMemorySyncProducer{broker: broker}
}

// Send sends a message to the broker.
func (p *InMemorySyncProducer) Send(topic string, key []byte, value []byte) error {
	return p.broker.Produce(context.Background(), topic, key, value)
}

// Close does nothing as there's no resource to clean up.
func (p *InMemorySyncProducer) Close() error {
	return nil
}

// --- Async Producer ---

// ProducerMessage represents a message to be produced.
type ProducerMessage struct {
	Topic string
	Key   []byte
	Value []byte
}

// ProducerError represents a producer error.
type ProducerError struct {
	Msg *ProducerMessage
	Err error
}

// InMemoryAsyncProducer implements an asynchronous producer for testing.
type InMemoryAsyncProducer struct {
	broker    *InMemoryBroker
	input     chan *ProducerMessage
	successes chan *ProducerMessage
	errors    chan *ProducerError
	close     chan struct{}
	wg        sync.WaitGroup
	closeOnce sync.Once
}

// NewInMemoryAsyncProducer creates a new in-memory async producer.
func NewInMemoryAsyncProducer(broker *InMemoryBroker, bufferSize int) *InMemoryAsyncProducer {
	if bufferSize <= 0 {
		bufferSize = 100
	}

	p := &InMemoryAsyncProducer{
		broker:    broker,
		input:     make(chan *ProducerMessage, bufferSize),
		successes: make(chan *ProducerMessage, bufferSize),
		errors:    make(chan *ProducerError, bufferSize),
		close:     make(chan struct{}),
	}

	p.wg.Add(1)
	go p.messageHandler()

	return p
}

// messageHandler reads from the input channel and routes messages.
func (p *InMemoryAsyncProducer) messageHandler() {
	defer p.wg.Done()

	for {
		select {
		case msg, ok := <-p.input:
			if !ok {
				return
			}

			err := p.broker.Produce(context.Background(), msg.Topic, msg.Key, msg.Value)
			if err != nil {
				p.errors <- &ProducerError{Msg: msg, Err: err}
			} else {
				p.successes <- msg
			}
		case <-p.close:
			return
		}
	}
}

// Produce sends a message asynchronously.
func (p *InMemoryAsyncProducer) Produce(topic string, key []byte, value []byte) {
	p.input <- &ProducerMessage{
		Topic: topic,
		Key:   key,
		Value: value,
	}
}

// Input returns the input channel for sending messages.
func (p *InMemoryAsyncProducer) Input() chan<- *ProducerMessage {
	return p.input
}

// Successes returns the success channel.
func (p *InMemoryAsyncProducer) Successes() <-chan *ProducerMessage {
	return p.successes
}

// Errors returns the error channel.
func (p *InMemoryAsyncProducer) Errors() <-chan *ProducerError {
	return p.errors
}

// Close shuts down the producer. Safe to call multiple times.
func (p *InMemoryAsyncProducer) Close() error {
	p.closeOnce.Do(func() {
		close(p.close)
		p.wg.Wait()
		close(p.successes)
		close(p.errors)
		close(p.input)
	})
	return nil
}

// --- Consumer Group ---

// ConsumerGroupSession represents a consumer group session.
type ConsumerGroupSession interface {
	Claims() map[string][]int32
	MemberID() string
	GenerationID() int32
	MarkOffset(topic string, partition int32, offset int64, metadata string)
	ResetOffset(topic string, partition int32, offset int64, metadata string)
	MarkMessage(msg *Message, metadata string)
	Context() context.Context
	Commit()
}

// ConsumerGroupClaim represents a consumer group claim.
type ConsumerGroupClaim interface {
	Topic() string
	Partition() int32
	InitialOffset() int64
	HighWaterMarkOffset() int64
	Messages() <-chan *Message
}

// ConsumerGroupHandler is the interface for handling consumer group events.
type ConsumerGroupHandler interface {
	Setup(ConsumerGroupSession) error
	Cleanup(ConsumerGroupSession) error
	ConsumeClaim(ConsumerGroupSession, ConsumerGroupClaim) error
}

// InMemoryConsumerGroup implements a consumer group for testing.
type InMemoryConsumerGroup struct {
	broker        *InMemoryBroker
	topic         string
	groupID       string
	errors        chan error
	closeOnce     sync.Once
	cancelConsume context.CancelFunc
	wg            sync.WaitGroup
	closed        chan struct{}
	isRunning     bool
	isPaused      bool
	mu            sync.Mutex
}

// NewInMemoryConsumerGroup creates a mock consumer group.
func NewInMemoryConsumerGroup(broker *InMemoryBroker, topic, groupID string) *InMemoryConsumerGroup {
	return &InMemoryConsumerGroup{
		broker:  broker,
		topic:   topic,
		groupID: groupID,
		errors:  make(chan error, 100),
		closed:  make(chan struct{}),
	}
}

// Errors returns the error channel.
func (mcg *InMemoryConsumerGroup) Errors() <-chan error {
	return mcg.errors
}

// Close stops the consumer group.
func (mcg *InMemoryConsumerGroup) Close() error {
	mcg.mu.Lock()
	if !mcg.isRunning {
		mcg.mu.Unlock()
		return nil
	}
	mcg.mu.Unlock()

	mcg.closeOnce.Do(func() {
		mcg.mu.Lock()
		if mcg.cancelConsume != nil {
			mcg.cancelConsume()
		}
		mcg.wg.Wait()
		close(mcg.errors)
		close(mcg.closed)
		mcg.isRunning = false
		mcg.mu.Unlock()
	})

	<-mcg.closed
	return nil
}

// Consume joins a cluster of consumers for a given list of topics.
func (mcg *InMemoryConsumerGroup) Consume(ctx context.Context, topics []string, handler ConsumerGroupHandler) error {
	mcg.mu.Lock()
	if mcg.isRunning {
		mcg.mu.Unlock()
		return errors.NewProcessingError("consumer group already running")
	}
	mcg.isRunning = true
	internalCtx, cancel := context.WithCancel(ctx)
	mcg.cancelConsume = cancel
	mcg.mu.Unlock()

	defer func() {
		if mcg.cancelConsume != nil {
			mcg.cancelConsume()
		}
		mcg.mu.Lock()
		mcg.isRunning = false
		mcg.cancelConsume = nil
		mcg.mu.Unlock()
	}()

	if len(topics) != 1 {
		return errors.NewConfigurationError("in-memory consumer group mock only supports exactly one topic")
	}

	topicToConsume := topics[0]

	// Create consumer channel
	mcg.broker.mu.RLock()
	t, ok := mcg.broker.topics[topicToConsume]
	mcg.broker.mu.RUnlock()

	if !ok {
		mcg.broker.mu.Lock()
		if _, exists := mcg.broker.topics[topicToConsume]; !exists {
			mcg.broker.topics[topicToConsume] = &Topic{
				messages:  make([]*Message, 0),
				consumers: make([]chan *Message, 0),
			}
		}
		t = mcg.broker.topics[topicToConsume]
		mcg.broker.mu.Unlock()
	}

	ch := make(chan *Message, 100)
	t.mu.Lock()
	t.consumers = append(t.consumers, ch)
	t.mu.Unlock()

	defer func() {
		t.mu.Lock()
		newConsumers := make([]chan *Message, 0, len(t.consumers))
		for _, c := range t.consumers {
			if c != ch {
				newConsumers = append(newConsumers, c)
			}
		}
		t.consumers = newConsumers
		t.mu.Unlock()
		close(ch)
	}()

	session := &InMemoryConsumerGroupSession{
		ctx:     internalCtx,
		broker:  mcg.broker,
		groupID: mcg.groupID,
	}

	claim := &InMemoryConsumerGroupClaim{
		topic:     topicToConsume,
		partition: 0,
		messages:  ch,
		isPausedFunc: func() bool {
			mcg.mu.Lock()
			defer mcg.mu.Unlock()
			return mcg.isPaused
		},
	}

	// Setup
	if err := handler.Setup(session); err != nil {
		return err
	}

	// Check context cancellation after Setup
	if internalCtx.Err() != nil {
		return internalCtx.Err()
	}

	// ConsumeClaim
	consumeErr := handler.ConsumeClaim(session, claim)

	// Cleanup
	cleanupErr := handler.Cleanup(session)
	if cleanupErr != nil && consumeErr == nil {
		return cleanupErr
	}

	return consumeErr
}

// PauseAll pauses consumption.
func (mcg *InMemoryConsumerGroup) PauseAll() {
	mcg.mu.Lock()
	defer mcg.mu.Unlock()
	mcg.isPaused = true
}

// ResumeAll resumes consumption.
func (mcg *InMemoryConsumerGroup) ResumeAll() {
	mcg.mu.Lock()
	defer mcg.mu.Unlock()
	mcg.isPaused = false
}

// Pause is a no-op for partition map; use PauseAll to pause.
func (mcg *InMemoryConsumerGroup) Pause(_ map[string][]int32) {}

// Resume is a no-op for partition map; use ResumeAll to resume.
func (mcg *InMemoryConsumerGroup) Resume(_ map[string][]int32) {}

// --- Session and Claim implementations ---

// InMemoryConsumerGroupSession implements ConsumerGroupSession.
type InMemoryConsumerGroupSession struct {
	ctx     context.Context
	broker  *InMemoryBroker
	groupID string
}

func (s *InMemoryConsumerGroupSession) Claims() map[string][]int32 {
	claims := make(map[string][]int32)
	topics := s.broker.Topics()
	for _, topic := range topics {
		claims[topic] = []int32{0}
	}
	return claims
}

func (s *InMemoryConsumerGroupSession) MemberID() string    { return "mock-member-" + s.groupID }
func (s *InMemoryConsumerGroupSession) GenerationID() int32 { return 1 }
func (s *InMemoryConsumerGroupSession) MarkOffset(topic string, partition int32, offset int64, metadata string) {
}
func (s *InMemoryConsumerGroupSession) Commit() {}
func (s *InMemoryConsumerGroupSession) ResetOffset(topic string, partition int32, offset int64, metadata string) {
}
func (s *InMemoryConsumerGroupSession) MarkMessage(msg *Message, metadata string) {}
func (s *InMemoryConsumerGroupSession) Context() context.Context                  { return s.ctx }

// InMemoryConsumerGroupClaim implements ConsumerGroupClaim.
type InMemoryConsumerGroupClaim struct {
	topic        string
	partition    int32
	messages     <-chan *Message
	isPausedFunc func() bool
}

func (c *InMemoryConsumerGroupClaim) Topic() string        { return c.topic }
func (c *InMemoryConsumerGroupClaim) Partition() int32     { return c.partition }
func (c *InMemoryConsumerGroupClaim) InitialOffset() int64 { return 0 }
func (c *InMemoryConsumerGroupClaim) HighWaterMarkOffset() int64 {
	return 0
}
func (c *InMemoryConsumerGroupClaim) Messages() <-chan *Message {
	// Create a filtered channel that respects pause state
	filtered := make(chan *Message, 100)
	go func() {
		defer close(filtered)
		for msg := range c.messages {
			// Wait while paused
			for c.isPausedFunc != nil && c.isPausedFunc() {
				time.Sleep(10 * time.Millisecond)
			}
			filtered <- msg
		}
	}()
	return filtered
}
