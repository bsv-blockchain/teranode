package inmemorykafka

import (
	"bytes"
	"context"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInMemoryBrokerProduceConsume(t *testing.T) {
	broker := NewInMemoryBroker()
	topic := "test-topic"
	messageValue := []byte("hello world")

	producer := NewInMemorySyncProducer(broker)
	defer func() { _ = producer.Close() }()

	received := make(chan *Message, 1)
	handler := &captureHandler{received: received}
	cg := NewInMemoryConsumerGroup(broker, topic, "test-group")
	defer cg.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = cg.Consume(ctx, []string{topic}, handler) }()

	time.Sleep(50 * time.Millisecond)
	require.NoError(t, producer.Send(topic, nil, messageValue))

	select {
	case msg := <-received:
		assert.Equal(t, topic, msg.Topic)
		assert.True(t, bytes.Equal(msg.Value, messageValue))
		assert.Equal(t, int64(0), msg.Offset)
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for message")
	}
}

type captureHandler struct{ received chan *Message }

func (h *captureHandler) Setup(ConsumerGroupSession) error   { return nil }
func (h *captureHandler) Cleanup(ConsumerGroupSession) error { return nil }
func (h *captureHandler) ConsumeClaim(_ ConsumerGroupSession, claim ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		h.received <- msg
	}
	return nil
}

func TestInMemoryBrokerProduceToNewTopic(t *testing.T) {
	broker := NewInMemoryBroker()
	producer := NewInMemorySyncProducer(broker)
	defer producer.Close()
	require.NoError(t, producer.Send("new-topic", nil, []byte("message for new topic")))

	broker.mu.RLock()
	_, ok := broker.topics["new-topic"]
	broker.mu.RUnlock()
	assert.True(t, ok)
}

func TestInMemoryBrokerConsumeFromNewTopic(t *testing.T) {
	broker := NewInMemoryBroker()
	cg := NewInMemoryConsumerGroup(broker, "another-new-topic", "test-group")
	defer cg.Close()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := cg.Consume(ctx, []string{"another-new-topic"}, &captureHandler{received: make(chan *Message)})
	require.Error(t, err)
	assert.Equal(t, context.Canceled, err)

	broker.mu.RLock()
	_, ok := broker.topics["another-new-topic"]
	broker.mu.RUnlock()
	assert.True(t, ok)
}

func TestInMemorySyncProducerSend(t *testing.T) {
	broker := NewInMemoryBroker()
	producer := NewInMemorySyncProducer(broker)
	defer producer.Close()
	require.NoError(t, producer.Send("test-topic", []byte("key"), []byte("value")))
}

func TestInMemoryAsyncProducerProduceSuccess(t *testing.T) {
	broker := NewInMemoryBroker()
	topic := "async-test-success"
	messageValue := []byte("hello async world")
	received := make(chan *Message, 1)
	handler := &captureHandler{received: received}
	cg := NewInMemoryConsumerGroup(broker, topic, "test-group")
	defer cg.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = cg.Consume(ctx, []string{topic}, handler) }()
	time.Sleep(50 * time.Millisecond)

	producer := NewInMemoryAsyncProducer(broker, 1)
	defer producer.Close()

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		select {
		case <-producer.Successes():
		case err := <-producer.Errors():
			t.Errorf("expected success, got error: %v", err)
		case <-time.After(2 * time.Second):
			t.Error("timeout waiting for success")
		}
	}()
	go func() {
		defer wg.Done()
		select {
		case msg := <-received:
			assert.Equal(t, messageValue, msg.Value)
		case <-time.After(2 * time.Second):
			t.Error("timeout waiting for message")
		}
	}()
	producer.Produce(topic, nil, messageValue)
	wg.Wait()
}

func TestInMemoryAsyncProducerClose(t *testing.T) {
	broker := NewInMemoryBroker()
	producer := NewInMemoryAsyncProducer(broker, 10)
	defer producer.Close()
	producer.Produce("async-test-close", nil, []byte("msg1"))
	producer.Produce("async-test-close", nil, []byte("msg2"))
	time.Sleep(50 * time.Millisecond)
	require.NoError(t, producer.Close())
	for range producer.Successes() {
		// drain successes channel
	}
	for range producer.Errors() {
		// drain errors channel
	}
	_, okSuccess := <-producer.Successes()
	assert.False(t, okSuccess)
	_, okError := <-producer.Errors()
	assert.False(t, okError)
}

func TestBrokerTopics(t *testing.T) {
	broker := NewInMemoryBroker()
	assert.Empty(t, broker.Topics())
	producer := NewInMemorySyncProducer(broker)
	defer producer.Close()
	require.NoError(t, producer.Send("topic1", nil, []byte("msg1")))
	require.NoError(t, producer.Send("topic2", nil, []byte("msg2")))
	require.NoError(t, producer.Send("topic1", nil, []byte("msg3")))
	topics := broker.Topics()
	assert.Len(t, topics, 2)
	assert.Contains(t, topics, "topic1")
	assert.Contains(t, topics, "topic2")
}

func TestGetSharedBroker(t *testing.T) {
	assert.Equal(t, GetSharedBroker(), GetSharedBroker())
	assert.NotNil(t, GetSharedBroker())
}

func TestConsumerGroupBasicFunctionality(t *testing.T) {
	broker := NewInMemoryBroker()
	cg := NewInMemoryConsumerGroup(broker, "test-topic", "test-group")
	defer cg.Close()
	assert.NotNil(t, cg.Errors())
	cg.PauseAll()
	cg.ResumeAll()
	cg.Pause(nil)
	cg.Resume(nil)
}

func TestConsumerGroupCloseWithoutRunning(t *testing.T) {
	broker := NewInMemoryBroker()
	cg := NewInMemoryConsumerGroup(broker, "test-topic", "test-group")
	assert.NoError(t, cg.Close())
	assert.NoError(t, cg.Close())
}

func TestConsumerGroupConsumeMultipleTopics(t *testing.T) {
	broker := NewInMemoryBroker()
	cg := NewInMemoryConsumerGroup(broker, "test-topic", "test-group")
	defer cg.Close()
	handler := &captureHandler{received: make(chan *Message)}
	err := cg.Consume(context.Background(), []string{"topic1", "topic2"}, handler)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "only supports exactly one topic")
}

func TestConsumerGroupConsumeSetupError(t *testing.T) {
	broker := NewInMemoryBroker()
	cg := NewInMemoryConsumerGroup(broker, "test-topic", "test-group")
	defer cg.Close()
	handler := &errorSetupHandler{setupErr: errors.New(errors.ERR_UNKNOWN, "setup failed")}
	err := cg.Consume(context.Background(), []string{"test-topic"}, handler)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "setup failed")
	assert.True(t, handler.setupCalled)
}

type errorSetupHandler struct {
	setupCalled bool
	setupErr    error
}

func (h *errorSetupHandler) Setup(ConsumerGroupSession) error {
	h.setupCalled = true
	return h.setupErr
}
func (h *errorSetupHandler) Cleanup(ConsumerGroupSession) error                          { return nil }
func (h *errorSetupHandler) ConsumeClaim(ConsumerGroupSession, ConsumerGroupClaim) error { return nil }

func TestConsumerGroupConsumeCleanupError(t *testing.T) {
	broker := NewInMemoryBroker()
	cg := NewInMemoryConsumerGroup(broker, "test-topic", "test-group")
	defer cg.Close()
	handler := &errorCleanupHandler{
		consumeErr: errors.New(errors.ERR_UNKNOWN, "consume done"),
		cleanupErr: errors.New(errors.ERR_UNKNOWN, "cleanup failed"),
	}
	err := cg.Consume(context.Background(), []string{"test-topic"}, handler)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "consume done")
	assert.True(t, handler.cleanupCalled)
}

type errorCleanupHandler struct {
	consumeErr    error
	cleanupErr    error
	cleanupCalled bool
}

func (h *errorCleanupHandler) Setup(ConsumerGroupSession) error { return nil }
func (h *errorCleanupHandler) Cleanup(ConsumerGroupSession) error {
	h.cleanupCalled = true
	return h.cleanupErr
}
func (h *errorCleanupHandler) ConsumeClaim(ConsumerGroupSession, ConsumerGroupClaim) error {
	return h.consumeErr
}

func TestConsumerGroupConsumeContextCancel(t *testing.T) {
	broker := NewInMemoryBroker()
	cg := NewInMemoryConsumerGroup(broker, "test-topic", "test-group")
	defer cg.Close()
	handler := &captureHandler{received: make(chan *Message)}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := cg.Consume(ctx, []string{"test-topic"}, handler)
	assert.Error(t, err)
	assert.Equal(t, context.Canceled, err)
}

func TestConsumerGroupSessionMethods(t *testing.T) {
	broker := NewInMemoryBroker()
	producer := NewInMemorySyncProducer(broker)
	require.NoError(t, producer.Send("topic1", nil, []byte("test1")))
	require.NoError(t, producer.Send("topic2", nil, []byte("test2")))
	producer.Close()

	session := &InMemoryConsumerGroupSession{ctx: context.Background(), broker: broker, groupID: "test-group"}
	claims := session.Claims()
	assert.Len(t, claims, 2)
	assert.Contains(t, claims, "topic1")
	assert.Contains(t, claims, "topic2")
	assert.Equal(t, []int32{0}, claims["topic1"])
	assert.Equal(t, "mock-member-test-group", session.MemberID())
	assert.Equal(t, int32(1), session.GenerationID())
	assert.Equal(t, context.Background(), session.Context())
	session.MarkOffset("topic", 0, 0, "")
	session.Commit()
	session.ResetOffset("topic", 0, 0, "")
	session.MarkMessage(nil, "")
}

func TestConsumerGroupClaimMethods(t *testing.T) {
	ch := make(chan *Message, 1)
	close(ch)
	claim := &InMemoryConsumerGroupClaim{topic: "test-topic", partition: 0, messages: ch}
	assert.Equal(t, "test-topic", claim.Topic())
	assert.Equal(t, int32(0), claim.Partition())
	assert.Equal(t, int64(0), claim.InitialOffset())
	assert.Equal(t, int64(0), claim.HighWaterMarkOffset())
	assert.NotNil(t, claim.Messages())
}

func TestConsumerGroupSessionNoOpMethods(t *testing.T) {
	broker := NewInMemoryBroker()
	session := &InMemoryConsumerGroupSession{ctx: context.Background(), broker: broker, groupID: "test-group"}
	assert.NotPanics(t, func() {
		session.MarkOffset("topic", 0, 100, "metadata")
		session.Commit()
		session.ResetOffset("topic", 0, 50, "metadata")
		session.MarkMessage(nil, "metadata")
	})
}

func TestConsumerGroupPauseResumeMethods(t *testing.T) {
	broker := NewInMemoryBroker()
	cg := NewInMemoryConsumerGroup(broker, "test-topic", "test-group")
	defer cg.Close()
	partitions := map[string][]int32{"topic1": {0, 1}, "topic2": {0}}
	assert.NotPanics(t, func() {
		cg.PauseAll()
		cg.ResumeAll()
		cg.Pause(partitions)
		cg.Resume(partitions)
	})
}

func TestAsyncProducerNewWithZeroBuffer(t *testing.T) {
	broker := NewInMemoryBroker()
	producer := NewInMemoryAsyncProducer(broker, 0)
	defer producer.Close()
	assert.NotNil(t, producer.Input())
	assert.NotNil(t, producer.Successes())
	assert.NotNil(t, producer.Errors())
}

func TestConsumerGroupPauseResumeBehavior(t *testing.T) {
	broker := NewInMemoryBroker()
	topic := "pause-test-topic"
	cg := NewInMemoryConsumerGroup(broker, topic, "test-group")
	var receivedMessages []string
	var mu sync.Mutex
	handler := &PauseTestHandler{received: &receivedMessages, mu: &mu}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	consumeDone := make(chan error, 1)
	go func() { consumeDone <- cg.Consume(ctx, []string{topic}, handler) }()
	time.Sleep(100 * time.Millisecond)

	producer := NewInMemorySyncProducer(broker)
	defer producer.Close()
	require.NoError(t, producer.Send(topic, nil, []byte("message1")))
	require.NoError(t, producer.Send(topic, nil, []byte("message2")))
	time.Sleep(100 * time.Millisecond)
	mu.Lock()
	beforePauseCount := len(receivedMessages)
	mu.Unlock()
	assert.Equal(t, 2, beforePauseCount)

	cg.PauseAll()
	time.Sleep(50 * time.Millisecond)
	require.NoError(t, producer.Send(topic, nil, []byte("message3")))
	require.NoError(t, producer.Send(topic, nil, []byte("message4")))
	time.Sleep(200 * time.Millisecond)
	mu.Lock()
	pausedCount := len(receivedMessages)
	mu.Unlock()
	assert.Equal(t, 2, pausedCount)

	cg.ResumeAll()
	time.Sleep(200 * time.Millisecond)
	mu.Lock()
	afterResumeCount := len(receivedMessages)
	allMessages := make([]string, len(receivedMessages))
	for i, m := range receivedMessages {
		allMessages[i] = m
	}
	mu.Unlock()
	assert.Equal(t, 4, afterResumeCount)
	assert.Equal(t, []string{"message1", "message2", "message3", "message4"}, allMessages)
	cancel()
	select {
	case err := <-consumeDone:
		assert.Error(t, err)
		assert.Equal(t, context.Canceled, err)
	case <-time.After(2 * time.Second):
		t.Fatal("Consume did not exit after context cancel")
	}
}

type PauseTestHandler struct {
	received *[]string
	mu       *sync.Mutex
}

func (h *PauseTestHandler) Setup(ConsumerGroupSession) error   { return nil }
func (h *PauseTestHandler) Cleanup(ConsumerGroupSession) error { return nil }
func (h *PauseTestHandler) ConsumeClaim(session ConsumerGroupSession, claim ConsumerGroupClaim) error {
	msgs := claim.Messages()
	for {
		select {
		case <-session.Context().Done():
			return session.Context().Err()
		case msg, ok := <-msgs:
			if !ok {
				return nil
			}
			if msg == nil {
				return nil
			}
			h.mu.Lock()
			*h.received = append(*h.received, string(msg.Value))
			h.mu.Unlock()
		}
	}
}

// TestConsumerGroupCloseStopsIdleConsumeGoroutine is the E4 regression this
// fix targets (task-13-brief.md controller addendum E4, booked residual B6):
// against the ORIGINAL code, Close() on a consumer group whose topic never
// received a single message could not stop the underlying Consume goroutine
// at all — ConsumeClaim's `for message := range claim.Messages()` blocks on
// a channel nothing ever closes until Consume itself returns, and nothing
// inside that idle loop ever checked ctx. Close()'s own wg.Wait() was also a
// no-op, since nothing ever called wg.Add. So Close() returning proved
// nothing about whether the goroutine backing this consumer had exited.
//
// This test proves Consume itself observably returns after Close, on a
// topic with zero messages ever produced — the exact idle case the residual
// describes — by having Consume signal a channel when it returns and
// asserting that channel closes soon after Close() does.
func TestConsumerGroupCloseStopsIdleConsumeGoroutine(t *testing.T) {
	broker := NewInMemoryBroker()
	cg := NewInMemoryConsumerGroup(broker, "idle-topic", "idle-group")

	// rangeOnlyHandler mirrors the SHAPE of the production handler this
	// fake actually backs: util/kafka's inMemoryConsumerHandler.ConsumeClaim
	// is a plain `for message := range claim.Messages()`, with no select on
	// session.Context() of its own. Using a ctx-aware handler here (like
	// this file's own PauseTestHandler) would let the HANDLER's own select
	// mask a still-broken Messages() — this handler has no such escape
	// hatch, so it can only return if Messages() itself reacts to
	// cancellation.
	handler := &rangeOnlyHandler{}

	consumeReturned := make(chan struct{})

	go func() {
		_ = cg.Consume(context.Background(), []string{"idle-topic"}, handler)
		close(consumeReturned)
	}()

	// Give Consume a chance to actually register its consumer channel on the
	// broker and enter ConsumeClaim's idle read before Close races it.
	require.Eventually(t, func() bool {
		return broker.HasConsumer("idle-topic")
	}, time.Second, 5*time.Millisecond)

	require.NoError(t, cg.Close())

	select {
	case <-consumeReturned:
	case <-time.After(time.Second):
		t.Fatal("Consume did not return after Close on an idle topic — the idle consume goroutine is still blocked")
	}
}

// TestConsumerGroupCloseDoesNotDeadlockWithAMessageInFlight is the second
// bug this fix's own Wait()-becoming-real exposed: Close() originally held
// mcg.mu across mcg.wg.Wait(), which was harmless only because Wait() on an
// empty WaitGroup returns instantly. Once wg.Add/Done made Wait() genuinely
// block until Consume returns, that same lock became a real deadlock risk —
// Consume's own forwarding goroutine (Messages) takes mcg.mu on every
// message via its isPausedFunc closure, paused or not, so Close blocking in
// Wait() while holding mcg.mu could never let that goroutine finish, and
// Close could never return. Fixed by releasing mcg.mu before Wait().
//
// Reproduced directly (not via a timing race in a slower end-to-end test)
// by producing a message and calling Close immediately after: with the
// message already past the receive branch of Messages' select and heading
// toward its isPausedFunc check, Close's mu-guarded Wait() and that check
// contend for the same lock on every run.
func TestConsumerGroupCloseDoesNotDeadlockWithAMessageInFlight(t *testing.T) {
	for i := 0; i < 20; i++ {
		broker := NewInMemoryBroker()
		topic := "message-in-flight-topic"
		cg := NewInMemoryConsumerGroup(broker, topic, "test-group")

		consumeReturned := make(chan struct{})

		go func() {
			_ = cg.Consume(context.Background(), []string{topic}, &rangeOnlyHandler{})
			close(consumeReturned)
		}()

		require.Eventually(t, func() bool {
			return broker.HasConsumer(topic)
		}, time.Second, time.Millisecond)

		require.NoError(t, broker.Produce(context.Background(), topic, nil, []byte("x")))

		closeReturned := make(chan error, 1)

		go func() { closeReturned <- cg.Close() }()

		select {
		case err := <-closeReturned:
			require.NoError(t, err)
		case <-time.After(2 * time.Second):
			t.Fatalf("iteration %d: Close deadlocked with a message in flight", i)
		}

		select {
		case <-consumeReturned:
		case <-time.After(2 * time.Second):
			t.Fatalf("iteration %d: Consume never returned after Close", i)
		}
	}
}

// rangeOnlyHandler is the minimal handler shape this fake's real production
// caller (util/kafka's inMemoryConsumerHandler) actually uses: it drains
// Messages() with a plain range and never selects on the session's context.
type rangeOnlyHandler struct{}

func (rangeOnlyHandler) Setup(ConsumerGroupSession) error   { return nil }
func (rangeOnlyHandler) Cleanup(ConsumerGroupSession) error { return nil }
func (rangeOnlyHandler) ConsumeClaim(_ ConsumerGroupSession, claim ConsumerGroupClaim) error {
	for range claim.Messages() { //nolint:revive // draining only, mirrors inMemoryConsumerHandler
	}

	return nil
}

func TestConsumerGroupCloseNotRunning(t *testing.T) {
	broker := NewInMemoryBroker()
	cg := NewInMemoryConsumerGroup(broker, "test-topic", "test-group")
	assert.NoError(t, cg.Close())
	assert.NoError(t, cg.Close())
}

func TestAsyncProducerCloseChannelPath(t *testing.T) {
	broker := NewInMemoryBroker()
	producer := NewInMemoryAsyncProducer(broker, 1)
	assert.NoError(t, producer.Close())
}
