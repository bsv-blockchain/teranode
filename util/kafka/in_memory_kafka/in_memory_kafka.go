package inmemorykafka

import (
	"context"
	"errors" // nolint:depguard
	"fmt"
	"log"
	"sync"

	"github.com/IBM/sarama"
)

var errSessionClosed = errors.New("in-memory session closed")

// Message represents a simplified message structure.
type Message struct {
	Topic  string
	Value  []byte
	Offset int64
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
func (b *InMemoryBroker) Produce(ctx context.Context, topic string, value []byte) error {
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

	// Create message with next offset
	msg := &Message{
		Topic:  topic,
		Value:  value,
		Offset: int64(len(t.messages)),
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

// InMemorySyncProducer implements sarama.SyncProducer.
type InMemorySyncProducer struct {
	broker *InMemoryBroker
}

// NewInMemorySyncProducer creates a new in-memory sync producer.
func NewInMemorySyncProducer(broker *InMemoryBroker) sarama.SyncProducer {
	return &InMemorySyncProducer{broker: broker}
}

// SendMessage sends a message to the broker, returning dummy partition and offset.
func (p *InMemorySyncProducer) SendMessage(msg *sarama.ProducerMessage) (int32, int64, error) {
	value, err := msg.Value.Encode()
	if err != nil {
		return 0, 0, err
	}

	err = p.broker.Produce(context.Background(), msg.Topic, value)
	if err != nil {
		return 0, 0, err
	}

	// Return dummy partition (0) and offset (0) since we don’t manage these
	return 0, 0, nil
}

// SendMessages is required by SyncProducer but not implemented for simplicity.
func (p *InMemorySyncProducer) SendMessages(msgs []*sarama.ProducerMessage) error {
	return errors.New("SendMessages not implemented")
}

// Close does nothing as there’s no resource to clean up.
func (p *InMemorySyncProducer) Close() error {
	return nil
}

// BeginTxn is required by SyncProducer but not implemented.
func (p *InMemorySyncProducer) BeginTxn() error {
	return errors.New("BeginTxn not implemented")
}

// AddMessageToTxn is required by SyncProducer but not implemented.
func (p *InMemorySyncProducer) AddMessageToTxn(msg *sarama.ConsumerMessage, groupID string, metadata *string) error {
	return errors.New("AddMessageToTxn not implemented")
}

// AddOffsetsToTxn is required by SyncProducer but not implemented.
func (p *InMemorySyncProducer) AddOffsetsToTxn(offsets map[string][]*sarama.PartitionOffsetMetadata, groupID string) error {
	return errors.New("AddOffsetsToTxn not implemented")
}

// CommitTxn is required by SyncProducer but not implemented.
func (p *InMemorySyncProducer) CommitTxn() error {
	return errors.New("CommitTxn not implemented")
}

// AbortTxn is required by SyncProducer but not implemented.
func (p *InMemorySyncProducer) AbortTxn() error {
	return errors.New("AbortTxn not implemented")
}

// TxnStatus is required by SyncProducer but not implemented.
func (p *InMemorySyncProducer) TxnStatus() sarama.ProducerTxnStatusFlag {
	// Return a default status; assuming no transaction is ever active in this mock.
	return sarama.ProducerTxnFlagReady
}

// IsTransactional is required by SyncProducer. Returns false as this mock isn't transactional.
func (p *InMemorySyncProducer) IsTransactional() bool {
	return false
}

// InMemoryConsumer implements sarama.Consumer.
type InMemoryConsumer struct {
	broker    *InMemoryBroker
	topic     string
	ch        chan *Message
	closeOnce sync.Once // Added for safe closing
}

// NewInMemoryConsumer creates a new in-memory consumer for a topic.
func (b *InMemoryBroker) NewInMemoryConsumer(topic string) (sarama.Consumer, error) {
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

	ch := make(chan *Message, 100) // Buffered channel

	t.mu.Lock()
	t.consumers = append(t.consumers, ch)
	t.mu.Unlock()

	return &InMemoryConsumer{
		broker: b,
		topic:  topic,
		ch:     ch,
	}, nil
}

// ConsumePartition returns a PartitionConsumer for the topic.
func (c *InMemoryConsumer) ConsumePartition(topic string, partition int32, offset int64) (sarama.PartitionConsumer, error) {
	// Ignore partition and offset for simplicity; only support one partition (0)
	if partition != 0 {
		return nil, errors.New("only partition 0 is supported")
	}

	return &InMemoryPartitionConsumer{
		consumer: c,
	}, nil
}

// Topics is not implemented as it’s not needed for basic tests.
func (c *InMemoryConsumer) Topics() ([]string, error) {
	return nil, errors.New("Topics not implemented")
}

// Partitions is not implemented as we assume one partition.
func (c *InMemoryConsumer) Partitions(topic string) ([]int32, error) {
	return nil, errors.New("Partitions not implemented")
}

// Close removes the consumer’s channel from the broker and safely closes the channel once.
func (c *InMemoryConsumer) Close() error {
	c.closeOnce.Do(func() { // Ensure close logic runs only once
		c.broker.mu.RLock()
		t, ok := c.broker.topics[c.topic]
		c.broker.mu.RUnlock()

		if ok {
			t.mu.Lock()
			newConsumers := make([]chan *Message, 0, len(t.consumers)) // Safer removal

			for _, ch := range t.consumers {
				if ch != c.ch {
					newConsumers = append(newConsumers, ch)
				}
			}

			t.consumers = newConsumers
			t.mu.Unlock()
		}
		// Close the channel safely within the sync.Once block
		close(c.ch)
	})

	return nil
}

// HighWaterMarks is required by the sarama.Consumer interface but not implemented.
func (c *InMemoryConsumer) HighWaterMarks() map[string]map[int32]int64 {
	// Return nil as this mock doesn't track high water marks.
	return nil
}

// Pause is required by the sarama.Consumer interface but not implemented.
func (c *InMemoryConsumer) Pause(partitions map[string][]int32) {
	// This mock does not support pausing partitions.
}

// PauseAll is required by the sarama.Consumer interface but not implemented.
func (c *InMemoryConsumer) PauseAll() {
	// This mock does not support pausing.
}

// Resume is required by the sarama.Consumer interface but not implemented.
func (c *InMemoryConsumer) Resume(partitions map[string][]int32) {
	// This mock does not support resuming partitions.
}

// ResumeAll is required by the sarama.Consumer interface but not implemented.
func (c *InMemoryConsumer) ResumeAll() {
	// This mock does not support resuming.
}

// InMemoryPartitionConsumer implements sarama.PartitionConsumer.
type InMemoryPartitionConsumer struct {
	consumer *InMemoryConsumer
	messages chan *sarama.ConsumerMessage
	once     sync.Once
}

// Messages returns a channel of ConsumerMessages.
func (pc *InMemoryPartitionConsumer) Messages() <-chan *sarama.ConsumerMessage {
	pc.once.Do(func() {
		pc.messages = make(chan *sarama.ConsumerMessage, 100)
		go func() {
			defer close(pc.messages)

			for msg := range pc.consumer.ch {
				cm := &sarama.ConsumerMessage{
					Topic:  msg.Topic,
					Value:  msg.Value,
					Offset: msg.Offset,
				}
				select {
				case pc.messages <- cm:
				default:
				}
			}
		}()
	})

	return pc.messages
}

// Errors returns nil as error handling is simplified.
func (pc *InMemoryPartitionConsumer) Errors() <-chan *sarama.ConsumerError {
	return nil
}

// Close stops the partition consumer (in this mock, it does nothing specific).
func (pc *InMemoryPartitionConsumer) Close() error {
	// Previously called pc.consumer.Close(), causing double close.
	// PartitionConsumer closing should be independent.
	return nil
}

// AsyncClose performs a non-blocking close operation required by sarama.PartitionConsumer.
func (pc *InMemoryPartitionConsumer) AsyncClose() {
	// Trigger the close operation asynchronously.
	go pc.Close()
}

// HighWaterMarkOffset returns the high water mark offset. Required by sarama.PartitionConsumer.
func (pc *InMemoryPartitionConsumer) HighWaterMarkOffset() int64 {
	// This mock does not track offsets precisely. Return a dummy value or best guess.
	// Let's return the current count of messages in the topic as a proxy.
	pc.consumer.broker.mu.RLock()
	t, ok := pc.consumer.broker.topics[pc.consumer.topic]
	pc.consumer.broker.mu.RUnlock()

	if ok {
		t.mu.RLock()
		defer t.mu.RUnlock()

		return int64(len(t.messages))
	}

	return 0 // Default if topic not found or not tracking
}

// IsPaused returns whether this partition consumer is paused. Required by sarama.PartitionConsumer.
func (pc *InMemoryPartitionConsumer) IsPaused() bool {
	// This mock does not support pausing, so always return false.
	return false
}

// Pause is required by the sarama.PartitionConsumer interface but not implemented.
func (pc *InMemoryPartitionConsumer) Pause() {
	// This mock does not support pausing partitions.
}

// Resume is required by the sarama.PartitionConsumer interface but not implemented.
func (pc *InMemoryPartitionConsumer) Resume() {
	// This mock does not support resuming partitions.
}

// InMemoryAsyncProducer implements sarama.AsyncProducer.
type InMemoryAsyncProducer struct {
	broker    *InMemoryBroker
	input     chan *sarama.ProducerMessage
	successes chan *sarama.ProducerMessage
	errors    chan *sarama.ProducerError
	close     chan struct{}
	wg        sync.WaitGroup
}

// Ensure InMemoryAsyncProducer implements the interface
var _ sarama.AsyncProducer = (*InMemoryAsyncProducer)(nil)

// NewInMemoryAsyncProducer creates a new in-memory async producer.
// Buffer sizes can be adjusted as needed.
func NewInMemoryAsyncProducer(broker *InMemoryBroker, bufferSize int) *InMemoryAsyncProducer {
	if bufferSize <= 0 {
		bufferSize = 100 // Default buffer size
	}

	p := &InMemoryAsyncProducer{
		broker:    broker,
		input:     make(chan *sarama.ProducerMessage, bufferSize),
		successes: make(chan *sarama.ProducerMessage, bufferSize),
		errors:    make(chan *sarama.ProducerError, bufferSize),
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
				// Input channel closed, likely during Close()
				return
			}

			value, err := msg.Value.Encode()
			if err != nil {
				p.errors <- &sarama.ProducerError{Msg: msg, Err: err}
				continue // Skip to next message
			}

			// Use context.Background() for the mock Produce call
			err = p.broker.Produce(context.Background(), msg.Topic, value)
			if err != nil {
				p.errors <- &sarama.ProducerError{Msg: msg, Err: err}
			} else {
				// Simulate success: Assign dummy partition/offset if needed, although
				// broker.Produce doesn't return them. The original message is sufficient
				// for the Successes channel according to Sarama docs.
				p.successes <- msg
			}
		case <-p.close:
			// Close signal received
			return
		}
	}
}

// AsyncClose triggers a shutdown of the producer and waits for it to complete.
func (p *InMemoryAsyncProducer) AsyncClose() {
	close(p.close) // Signal handler goroutine to stop
}

// Close shuts down the producer and waits for currently processed messages to finish.
func (p *InMemoryAsyncProducer) Close() error {
	p.AsyncClose() // Signal handler to stop processing
	p.wg.Wait()    // Wait for handler goroutine to finish
	// Close output channels after the handler has finished to prevent sends on closed channels
	close(p.successes)
	close(p.errors)
	close(p.input) // Close input to signal no more messages

	return nil
}

// Input returns the input channel for sending messages.
func (p *InMemoryAsyncProducer) Input() chan<- *sarama.ProducerMessage {
	return p.input
}

// Successes returns the success channel.
func (p *InMemoryAsyncProducer) Successes() <-chan *sarama.ProducerMessage {
	return p.successes
}

// Errors returns the error channel.
func (p *InMemoryAsyncProducer) Errors() <-chan *sarama.ProducerError {
	return p.errors
}

// IsTransactional required by AsyncProducer interface. Always false for this mock.
func (p *InMemoryAsyncProducer) IsTransactional() bool {
	return false
}

// TxnStatus required by AsyncProducer interface. Mock implementation.
func (p *InMemoryAsyncProducer) TxnStatus() sarama.ProducerTxnStatusFlag {
	return sarama.ProducerTxnFlagReady
}

// BeginTxn required by AsyncProducer interface. Mock implementation.
func (p *InMemoryAsyncProducer) BeginTxn() error {
	return errors.New("transactions not supported by InMemoryAsyncProducer")
}

// CommitTxn required by AsyncProducer interface. Mock implementation.
func (p *InMemoryAsyncProducer) CommitTxn() error {
	return errors.New("transactions not supported by InMemoryAsyncProducer")
}

// AbortTxn required by AsyncProducer interface. Mock implementation.
func (p *InMemoryAsyncProducer) AbortTxn() error {
	return errors.New("transactions not supported by InMemoryAsyncProducer")
}

// AddMessageToTxn required by AsyncProducer interface. Mock implementation.
func (p *InMemoryAsyncProducer) AddMessageToTxn(msg *sarama.ConsumerMessage, groupID string, metadata *string) error {
	return errors.New("transactions not supported by InMemoryAsyncProducer")
}

// AddOffsetsToTxn required by AsyncProducer interface. Mock implementation.
func (p *InMemoryAsyncProducer) AddOffsetsToTxn(offsets map[string][]*sarama.PartitionOffsetMetadata, groupID string) error {
	return errors.New("transactions not supported by InMemoryAsyncProducer")
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

// --- InMemoryConsumerGroup Implementation ---

// InMemoryConsumerGroup implements sarama.ConsumerGroup
type InMemoryConsumerGroup struct {
	broker        *InMemoryBroker
	topic         string                      // Store topic from config
	groupID       string                      // Store groupID from config
	handler       sarama.ConsumerGroupHandler // Set during Consume call
	errors        chan error
	closeOnce     sync.Once
	cancelConsume context.CancelFunc
	wg            sync.WaitGroup
	closed        chan struct{}
	isRunning     bool
	mu            sync.Mutex
}

// Ensure InMemoryConsumerGroup implements the interface
var _ sarama.ConsumerGroup = (*InMemoryConsumerGroup)(nil)

// NewInMemoryConsumerGroup creates a mock consumer group.
// It needs the topic and groupID for potential internal logic or logging.
func NewInMemoryConsumerGroup(broker *InMemoryBroker, topic, groupID string) *InMemoryConsumerGroup {
	return &InMemoryConsumerGroup{
		broker:  broker,
		topic:   topic,
		groupID: groupID,
		errors:  make(chan error, 100), // Buffered error channel
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
		// Avoid double close or closing non-running group
		// Although closeOnce handles double close, this prevents unnecessary waits
		return nil
	}
	mcg.mu.Unlock()

	mcg.closeOnce.Do(func() {
		mcg.mu.Lock()
		// fmt.Printf("[DEBUG InMemoryConsumerGroup %s] Close called\n", mcg.groupID)
		if mcg.cancelConsume != nil {
			// fmt.Printf("[DEBUG InMemoryConsumerGroup %s] Calling cancelConsume\n", mcg.groupID)
			mcg.cancelConsume() // Signal Consume loop to stop
		}
		// fmt.Printf("[DEBUG InMemoryConsumerGroup %s] Waiting on WaitGroup\n", mcg.groupID)
		mcg.wg.Wait() // Wait for Consume loop to finish
		// fmt.Printf("[DEBUG InMemoryConsumerGroup %s] WaitGroup finished\n", mcg.groupID)
		close(mcg.errors) // Close errors channel last
		close(mcg.closed) // Signal that Close is complete
		mcg.isRunning = false
		mcg.mu.Unlock()
	})

	<-mcg.closed // Wait until close is fully done

	return nil
}

// Consume joins a cluster of consumers for a given list of topics and
// starts a *blocking* ConsumerGroupSession through the ConsumerGroupHandler.
// This implementation attempts to mimic the blocking behavior of the real Sarama ConsumerGroup.
// It runs Setup, ConsumeClaim, and Cleanup synchronously.
// ConsumeClaim is expected to block until the provided context is cancelled or the handler exits.
func (mcg *InMemoryConsumerGroup) Consume(ctx context.Context, topics []string, handler sarama.ConsumerGroupHandler) error {
	// fmt.Printf("[DEBUG InMemoryConsumerGroup %s] Consume called\n", mcg.groupID)
	// Ensure only one Consume call runs at a time for this group instance
	mcg.mu.Lock()
	if mcg.isRunning {
		mcg.mu.Unlock()
		// fmt.Printf("[DEBUG InMemoryConsumerGroup %s] Consume called but already running\n", mcg.groupID)
		return errAlreadyRunning // Or sarama.ErrConcurrentConsumerGroupSessions if defined/preferred
	}

	mcg.isRunning = true
	// Store the cancel function derived from the input context immediately under lock
	// This internal context is what gets passed down and cancelled by Close()
	internalCtx, cancel := context.WithCancel(ctx)
	mcg.cancelConsume = cancel
	mcg.mu.Unlock()

	mcg.handler = handler

	// Variables to hold resources that need cleanup
	var (
		consumer          sarama.Consumer
		partitionConsumer sarama.PartitionConsumer
		consumeErr        error // Stores the primary error to return
	)

	// Defer cleanup actions
	defer func() {
		// Call the stored cancel function ensures the context IS cancelled on exit
		// It's safe to call cancel multiple times.
		if mcg.cancelConsume != nil {
			mcg.cancelConsume()
		}

		// Close consumers if they were successfully created
		if partitionConsumer != nil {
			// fmt.Printf("[DEBUG InMemoryConsumerGroup %s] Closing partition consumer\n", mcg.groupID)
			// In a real scenario, closing partitionConsumer might need coordination
			// if its internal processing (Messages loop) runs in a goroutine.
			// For this mock, assuming direct close is sufficient.
			partitionConsumer.Close()
		}

		if consumer != nil {
			// fmt.Printf("[DEBUG InMemoryConsumerGroup %s] Closing main consumer\n", mcg.groupID)
			consumer.Close()
		}

		// Mark as not running and clear cancel func *after* cleanup
		mcg.mu.Lock()
		mcg.isRunning = false
		mcg.cancelConsume = nil // Clear the stored cancel func
		mcg.mu.Unlock()
	}()

	// This mock currently only supports consuming a single specified topic
	if len(topics) != 1 {
		consumeErr = errors.New("in-memory consumer group mock only supports exactly one topic")
		return consumeErr // Cleanup runs via defer
	}

	topicToConsume := topics[0]

	// fmt.Printf("[DEBUG InMemoryConsumerGroup %s] Creating new consumer for topic %s\n", mcg.groupID, topicToConsume)
	var err error

	consumer, err = mcg.broker.NewInMemoryConsumer(topicToConsume)
	if err != nil {
		consumeErr = fmt.Errorf("failed to create underlying consumer: %w", err) // nolint:forbidigo
		return consumeErr                                                        // Cleanup runs via defer
	}

	// fmt.Printf("[DEBUG InMemoryConsumerGroup %s] Creating partition consumer for topic %s partition 0\n", mcg.groupID, topicToConsume)
	// Assume partition 0 and oldest offset for simplicity in the mock
	partitionConsumer, err = consumer.ConsumePartition(topicToConsume, 0, sarama.OffsetOldest)
	if err != nil {
		consumeErr = fmt.Errorf("failed to create partition consumer: %w", err) // nolint:forbidigo
		return consumeErr                                                       // Cleanup runs via defer
	}

	// Prepare session and claim objects using the internalCtx
	session := &InMemoryConsumerGroupSession{ctx: internalCtx, broker: mcg.broker, groupID: mcg.groupID}
	claim := &InMemoryConsumerGroupClaim{
		topic:        topicToConsume,
		partition:    0,
		partConsumer: partitionConsumer,
	}

	// --- Start of Synchronous Lifecycle ---

	// 1. Call Setup
	// fmt.Printf("[DEBUG InMemoryConsumerGroup %s] Calling handler.Setup\n", mcg.groupID)
	err = handler.Setup(session)
	// Check context cancellation *after* Setup, as Setup might block briefly
	if internalCtx.Err() != nil {
		consumeErr = internalCtx.Err() // Prioritize context cancellation
		if err != nil {
			log.Printf("WARN: InMemoryConsumerGroup %s: handler.Setup failed (%v) but context was already cancelled (%v)", mcg.groupID, err, consumeErr)
		}

		return consumeErr
	}

	if err != nil && !errors.Is(err, errSessionClosed) {
		// fmt.Printf("[ERROR InMemoryConsumerGroup %s] Handler Setup failed: %v\n", mcg.groupID, err)
		consumeErr = fmt.Errorf("handler setup failed: %w", err) // nolint:forbidigo
		// Cleanup (including cancelling internalCtx) happens in defer
		return consumeErr
	}

	// 2. Call ConsumeClaim - This is the core blocking call.
	// It should block until the session's context (internalCtx) is cancelled
	// or the handler implementation decides to exit.
	// fmt.Printf("[DEBUG InMemoryConsumerGroup %s] Calling handler.ConsumeClaim (blocking)\n", mcg.groupID)
	consumeErr = handler.ConsumeClaim(session, claim) // Assign directly to consumeErr
	// fmt.Printf("[DEBUG InMemoryConsumerGroup %s] Handler ConsumeClaim returned: %v\n", mcg.groupID, consumeErr)

	// 3. Call Cleanup - This runs *after* ConsumeClaim returns, even if it errored or context was cancelled.
	cleanupErr := handler.Cleanup(session)
	if cleanupErr != nil {
		// fmt.Printf("[ERROR InMemoryConsumerGroup %s] Handler Cleanup failed: %v\n", mcg.groupID, cleanupErr)
		// If ConsumeClaim didn't error, the cleanup error becomes the primary error.
		// If ConsumeClaim did error, we prioritize that and just log the cleanup error.
		if consumeErr == nil {
			consumeErr = fmt.Errorf("handler cleanup failed: %w", cleanupErr) // nolint:forbidigo
		} else {
			log.Printf("WARN: InMemoryConsumerGroup %s: handler.Cleanup failed (%v) after ConsumeClaim error (%v)", mcg.groupID, cleanupErr, consumeErr)
		}
	}

	// --- End of Synchronous Lifecycle ---

	// Final cleanup happens in the defer block.
	// Return the primary error encountered (ConsumeClaim, context cancellation if propagated, or Cleanup error).
	return consumeErr
}

// PauseAll pauses consumption for all claimed partitions. No-op for mock.
func (mcg *InMemoryConsumerGroup) PauseAll() {
	// No-op
}

// ResumeAll resumes consumption for all paused partitions. No-op for mock.
func (mcg *InMemoryConsumerGroup) ResumeAll() {
	// No-op
}

// Pause pauses consumption for the given partitions. No-op for mock.
func (mcg *InMemoryConsumerGroup) Pause(partitions map[string][]int32) {
	// No-op: Pausing/Resuming is not actively simulated in this mock
}

// Resume resumes consumption for the given partitions. No-op for mock.
func (mcg *InMemoryConsumerGroup) Resume(partitions map[string][]int32) {
	// No-op: Pausing/Resuming is not actively simulated in this mock
}

// --- Mock Session and Claim (Needed for Consume) ---

// InMemoryConsumerGroupSession implements sarama.ConsumerGroupSession
type InMemoryConsumerGroupSession struct {
	ctx     context.Context
	broker  *InMemoryBroker
	groupID string
}

func (s *InMemoryConsumerGroupSession) Claims() map[string][]int32 {
	claims := make(map[string][]int32)
	topics := s.broker.Topics() // Call the new Topics() method (Fixes 82efbd65, c78a7c59)

	for _, topic := range topics {
		// Mock: Assume claim on partition 0 for all known topics
		claims[topic] = []int32{0}
	}

	return claims
}
func (s *InMemoryConsumerGroupSession) MemberID() string    { return "mock-member-" + s.groupID }
func (s *InMemoryConsumerGroupSession) GenerationID() int32 { return 1 } // Mocked generation ID
func (s *InMemoryConsumerGroupSession) MarkOffset(topic string, partition int32, offset int64, metadata string) {
}                                               // No-op mock
func (s *InMemoryConsumerGroupSession) Commit() {} // No-op mock
func (s *InMemoryConsumerGroupSession) ResetOffset(topic string, partition int32, offset int64, metadata string) {
}                                                                                                // No-op mock
func (s *InMemoryConsumerGroupSession) MarkMessage(msg *sarama.ConsumerMessage, metadata string) {} // No-op mock
func (s *InMemoryConsumerGroupSession) Context() context.Context                                 { return s.ctx }

// InMemoryConsumerGroupClaim implements sarama.ConsumerGroupClaim
type InMemoryConsumerGroupClaim struct {
	topic        string
	partition    int32
	partConsumer sarama.PartitionConsumer // The underlying partition consumer
}

func (c *InMemoryConsumerGroupClaim) Topic() string        { return c.topic }
func (c *InMemoryConsumerGroupClaim) Partition() int32     { return c.partition }
func (c *InMemoryConsumerGroupClaim) InitialOffset() int64 { return sarama.OffsetOldest } // Mocked
func (c *InMemoryConsumerGroupClaim) HighWaterMarkOffset() int64 {
	if c.partConsumer != nil {
		return c.partConsumer.HighWaterMarkOffset()
	}

	return 0
}
func (c *InMemoryConsumerGroupClaim) Messages() <-chan *sarama.ConsumerMessage {
	if c.partConsumer != nil {
		return c.partConsumer.Messages()
	}
	// Return a closed channel if partition consumer is nil to prevent blocking forever
	closedChan := make(chan *sarama.ConsumerMessage)
	close(closedChan)

	return closedChan
}

// --- Consumer Group Mock ---

var errAlreadyRunning = errors.New("consumer group already running")

// ConsumerGroupHandler represents the interface that needs to be implemented by users of a ConsumerGroup.
// This is copied directly from sarama for compatibility.
