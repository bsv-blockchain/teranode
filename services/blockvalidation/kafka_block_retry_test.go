package blockvalidation

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	blobmemory "github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/kafka"
	kafkamessage "github.com/bsv-blockchain/teranode/util/kafka/kafka_message"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

// TestConsumerMessageHandler_UnparseableHashReportsPeer pins that a block message whose
// hash does not parse is committed and reported against the peer, rather than panicking
// on the nil hash and being committed by the consumer's panic barrier with no report.
func TestConsumerMessageHandler_UnparseableHashReportsPeer(t *testing.T) {
	ctx := context.Background()

	chain := &blockchain.Mock{}
	chain.On("ReportPeerFailure", mock.Anything, (*chainhash.Hash)(nil), "peerA", "block", mock.Anything).Return(nil).Once()

	server := &Server{logger: ulogger.TestLogger{}, blockchainClient: chain}
	shortenRetryDelays(&server.blockMessageRetries)

	value, err := proto.Marshal(&kafkamessage.KafkaBlockTopicMessage{Hash: "not-a-hash", URL: "http://peer", PeerId: "peerA"})
	require.NoError(t, err)

	require.NoError(t, server.consumerMessageHandler(ctx)(&kafka.KafkaMessage{Value: value}))
	chain.AssertExpectations(t)
}

func TestIsRecoverableBlockMessageError(t *testing.T) {
	require.True(t, isRecoverableBlockMessageError(errors.NewServiceError("x")))
	require.True(t, isRecoverableBlockMessageError(errors.NewStorageError("x")))
	require.True(t, isRecoverableBlockMessageError(errors.NewProcessingError("wrapped", errors.NewExternalError("x"))))

	// A ProcessingError with no peer code is local: retried, and the peer is not blamed.
	require.True(t, isRecoverableBlockMessageError(errors.NewProcessingError("x")))

	// A peer or consensus code in the chain is the peer's, even under a processing wrapper.
	require.False(t, isRecoverableBlockMessageError(errors.NewProcessingError("wrapped", errors.NewTxInvalidError("x"))))
	require.False(t, isRecoverableBlockMessageError(errors.NewBlockInvalidError("x")))
	require.False(t, isRecoverableBlockMessageError(errors.NewNetworkInvalidResponseError("x")))
}

// isPending reports whether a retry is in flight for hash, with no side effects.
func (r *blockMessageRetries) isPending(hash string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	_, ok := r.pending[hash]

	return ok
}

func blockMsg(hash, peerID string) *kafkamessage.KafkaBlockTopicMessage {
	return &kafkamessage.KafkaBlockTopicMessage{Hash: hash, URL: "http://" + peerID, PeerId: peerID}
}

// mustClaim claims a retry for msg and fails the test unless a new slot was claimed.
func mustClaim(t *testing.T, r *blockMessageRetries, msg *kafkamessage.KafkaBlockTopicMessage) *pendingBlockRetry {
	t.Helper()

	owner, outcome := r.claim(msg)
	require.Equal(t, retryClaimed, outcome, "claim of %s from %s", msg.Hash, msg.GetPeerId())
	require.NotNil(t, owner)

	return owner
}

// claimOutcome claims a retry for msg and returns only the outcome.
func claimOutcome(r *blockMessageRetries, msg *kafkamessage.KafkaBlockTopicMessage) blockRetryClaim {
	_, outcome := r.claim(msg)
	return outcome
}

// TestBlockMessageRetries_Claim pins the dedupe and the global cap on pending retries.
func TestBlockMessageRetries_Claim(t *testing.T) {
	var r blockMessageRetries

	owner := mustClaim(t, &r, blockMsg("a", "p"))
	require.Equal(t, retryAlreadyPending, claimOutcome(&r, blockMsg("a", "p")), "a hash already pending must not be queued twice")

	r.release("a", owner)
	mustClaim(t, &r, blockMsg("a", "p")) // a released hash can be queued again

	// Spread over peers so the per-peer cap does not trip first.
	for i := 1; i < blockMessageRetryMaxPending; i++ {
		h := chainhash.HashH([]byte{byte(i), byte(i >> 8)}).String()
		mustClaim(t, &r, blockMsg(h, string(rune('A'+i%40))))
	}

	require.Equal(t, retryQueueFull, claimOutcome(&r, blockMsg("overflow", "fresh-peer")), "the queue must refuse once full")
}

// TestBlockMessageRetries_PerPeerCap pins that one peer cannot hold more than its share
// of the queue, that other peers are still served, and that a release frees the share.
func TestBlockMessageRetries_PerPeerCap(t *testing.T) {
	var r blockMessageRetries

	owners := make([]*pendingBlockRetry, 0, blockMessageRetryMaxPendingPerPeer)
	for i := 0; i < blockMessageRetryMaxPendingPerPeer; i++ {
		owners = append(owners, mustClaim(t, &r, blockMsg(chainhash.HashH([]byte{byte(i)}).String(), "greedy")))
	}

	require.Equal(t, retryPeerFull, claimOutcome(&r, blockMsg("one-more", "greedy")), "a peer at its cap must be refused")
	mustClaim(t, &r, blockMsg("other", "honest")) // another peer must still get a slot

	r.release(chainhash.HashH([]byte{0}).String(), owners[0])
	mustClaim(t, &r, blockMsg("one-more", "greedy")) // a release must free the peer's share
}

// TestBlockMessageRetries_KeepsOtherSources pins that later announcers of a pending
// block are kept (once each, bounded) and handed back before the slot is released.
func TestBlockMessageRetries_KeepsOtherSources(t *testing.T) {
	var r blockMessageRetries

	owner := mustClaim(t, &r, blockMsg("h", "first"))
	require.Equal(t, retryKeptAsSource, claimOutcome(&r, blockMsg("h", "second")))
	require.Equal(t, retryAlreadyPending, claimOutcome(&r, blockMsg("h", "second")), "the same peer is kept once")
	require.Equal(t, retryAlreadyPending, claimOutcome(&r, blockMsg("h", "first")), "the claiming peer is not its own source")

	for i := 0; i < blockMessageRetryMaxSources; i++ {
		r.claim(blockMsg("h", string(rune('a'+i))))
	}

	require.Equal(t, retryAlreadyPending, claimOutcome(&r, blockMsg("h", "late")), "the sources are full")

	var got []string

	for {
		src, ok := r.nextSource("h", owner)
		if !ok {
			break
		}

		got = append(got, src.GetPeerId())
	}

	require.Len(t, got, blockMessageRetryMaxSources)
	require.Equal(t, "second", got[0])
	require.False(t, r.isPending("h"), "handing back the last source must release the slot")
	mustClaim(t, &r, blockMsg("h", "first")) // the peer's share must be freed too
}

// TestBlockMessageRetries_StaleReleaseKeepsReplacement pins slot ownership. The hand-off
// of a retry frees the slot when its sources run out. A new announcement can then claim
// the same hash before the deferred release of the first retry runs. That late release
// must not remove the replacement entry, its peer share, or its kept sources.
func TestBlockMessageRetries_StaleReleaseKeepsReplacement(t *testing.T) {
	var r blockMessageRetries

	first := mustClaim(t, &r, blockMsg("h", "peerA"))

	// The hand-off drains the sources and frees the slot.
	_, ok := r.nextSource("h", first)
	require.False(t, ok)
	require.False(t, r.isPending("h"))

	// A new recoverable announcement claims the same hash and keeps another source.
	replacement := mustClaim(t, &r, blockMsg("h", "peerB"))
	require.Equal(t, retryKeptAsSource, claimOutcome(&r, blockMsg("h", "peerC")))

	// The deferred release of the first retry runs late, then the first retry's
	// hand-off loop asks for a source once more.
	r.release("h", first)
	_, ok = r.nextSource("h", first)
	require.False(t, ok, "the first retry must not take the sources of the replacement")

	require.True(t, r.isPending("h"), "a late release must not remove the replacement")
	require.Equal(t, retryAlreadyPending, claimOutcome(&r, blockMsg("h", "peerC")), "a second concurrent retry for the hash must be refused")

	r.mu.Lock()
	peerBShare := r.perPeer["peerB"]
	r.mu.Unlock()
	require.Equal(t, 1, peerBShare, "a late release must not clear the share of the replacement's peer")

	src, ok := r.nextSource("h", replacement)
	require.True(t, ok, "the replacement must keep its sources")
	require.Equal(t, "peerC", src.GetPeerId())

	_, ok = r.nextSource("h", replacement)
	require.False(t, ok)
	require.False(t, r.isPending("h"), "the replacement frees the slot when its sources run out")

	r.mu.Lock()
	require.Empty(t, r.perPeer, "all peer shares are freed")
	r.mu.Unlock()
}

// TestRetryBlockMessage_StopsOnShutdown pins that a pending retry exits and frees its
// slot when the context ends, without running the handler.
func TestRetryBlockMessage_StopsOnShutdown(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	server := &Server{logger: ulogger.TestLogger{}}

	msg := &kafkamessage.KafkaBlockTopicMessage{Hash: "h"}
	server.scheduleBlockMessageRetry(ctx, msg)
	server.scheduleBlockMessageRetry(ctx, msg) // already pending: refused, no second goroutine
	cancel()

	require.Eventually(t, func() bool { return !server.blockMessageRetries.isPending("h") }, time.Second, 10*time.Millisecond)
}

// shortenRetryDelays makes the retry loop of r run in milliseconds.
func shortenRetryDelays(r *blockMessageRetries) {
	r.initialDelay, r.maxDelay = time.Millisecond, 2*time.Millisecond
}

// alwaysFailingExistsChain fails every GetBlockExists with a recoverable error and counts
// the calls, so a test can count the handler attempts.
type alwaysFailingExistsChain struct {
	blockchain.ClientI

	mu    sync.Mutex
	calls int
}

func (c *alwaysFailingExistsChain) GetBlockExists(context.Context, *chainhash.Hash) (bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.calls++

	return false, errors.NewServiceError("injected persistent blockchain failure")
}

func (c *alwaysFailingExistsChain) count() int {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.calls
}

// TestRetryBlockMessage_GivesUpAfterMaxAttempts pins that a fault that never clears is
// retried exactly blockMessageRetryMaxAttempts times, is not blamed on the peer, and
// frees its slot afterwards.
func TestRetryBlockMessage_GivesUpAfterMaxAttempts(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s, _, block := newProcessBlockFoundHarness(ctx, t)
	shortenRetryDelays(&s.blockMessageRetries)

	chain := &alwaysFailingExistsChain{ClientI: s.blockchainClient}
	s.blockValidation = NewBlockValidation(ctx, ulogger.TestLogger{}, s.settings, chain, blobmemory.New(), s.txStore, s.utxoStore, nil, nil)

	// ReportPeerFailure is not expected: a mock call without an expectation panics, and
	// the panic guard would then free the slot early with fewer attempts counted.
	s.blockchainClient = &blockchain.Mock{}

	hash := block.Hash().String()
	s.scheduleBlockMessageRetry(ctx, &kafkamessage.KafkaBlockTopicMessage{Hash: hash, URL: "http://peer", PeerId: "peerA"})

	require.Eventually(t, func() bool { return !s.blockMessageRetries.isPending(hash) }, 5*time.Second, 5*time.Millisecond)
	require.Equal(t, blockMessageRetryMaxAttempts, chain.count())
}

// TestRetryBlockMessage_UnrecoverableReportsPeer pins that a retry hitting an
// unrecoverable error blames the announcing peer, as the first attempt would have.
func TestRetryBlockMessage_UnrecoverableReportsPeer(t *testing.T) {
	hash := chainhash.HashH([]byte("retry-unrecoverable"))

	chain := &blockchain.Mock{}
	chain.On("ReportPeerFailure", mock.Anything, &hash, "peerA", "block", mock.Anything).Return(nil).Once()

	server := &Server{logger: ulogger.TestLogger{}, blockchainClient: chain}
	shortenRetryDelays(&server.blockMessageRetries)

	// A non-http URL fails blockHandler with an unrecoverable invalid-response error.
	server.scheduleBlockMessageRetry(context.Background(), &kafkamessage.KafkaBlockTopicMessage{Hash: hash.String(), URL: "ftp://peer", PeerId: "peerA"})

	require.Eventually(t, func() bool { return !server.blockMessageRetries.isPending(hash.String()) }, time.Second, 5*time.Millisecond)
	chain.AssertExpectations(t)
}

// TestRetryBlockMessage_RecoversFromPanic pins that a panic in the retry goroutine is
// contained and frees the slot. Without the guard the panic kills the test binary.
func TestRetryBlockMessage_RecoversFromPanic(t *testing.T) {
	hash := chainhash.HashH([]byte("retry-panic"))

	chain := &blockchain.Mock{}
	chain.On("ReportPeerFailure", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Run(func(mock.Arguments) { panic("injected panic") }).Once()

	server := &Server{logger: ulogger.TestLogger{}, blockchainClient: chain}
	shortenRetryDelays(&server.blockMessageRetries)
	server.scheduleBlockMessageRetry(context.Background(), &kafkamessage.KafkaBlockTopicMessage{Hash: hash.String(), URL: "ftp://peer", PeerId: "peerA"})

	require.Eventually(t, func() bool { return !server.blockMessageRetries.isPending(hash.String()) }, time.Second, 5*time.Millisecond)
	chain.AssertExpectations(t)
}

// TestRunBlockHandler_PanicBecomesServiceError pins that a panic in blockHandler is
// returned as a recoverable service error. Without the guard the panic kills the test
// binary.
func TestRunBlockHandler_PanicBecomesServiceError(t *testing.T) {
	// A zero BlockValidation has a nil exists cache, so GetBlockExists panics.
	server := &Server{logger: ulogger.TestLogger{}, blockValidation: &BlockValidation{}}

	hash := chainhash.HashH([]byte("handler-panic"))
	err := server.runBlockHandler(context.Background(), &kafkamessage.KafkaBlockTopicMessage{Hash: hash.String(), URL: "http://peer", PeerId: "peerA"})

	require.Error(t, err)
	require.True(t, errors.Is(err, errors.ErrServiceError), "got %v", err)
	require.True(t, isRecoverableBlockMessageError(err), "a panic is a local fault and must be retried, not blamed on the peer")
}

// TestConsumerMessageHandler_HandlerPanicIsRetried pins the end state for a panic on
// the first attempt: the message is committed, a retry is pending, and the peer is not
// reported.
func TestConsumerMessageHandler_HandlerPanicIsRetried(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// No ReportPeerFailure expectation: a report would make the mock panic.
	chain := &blockchain.Mock{}
	server := &Server{logger: ulogger.TestLogger{}, blockchainClient: chain, blockValidation: &BlockValidation{}}

	hash := chainhash.HashH([]byte("consumer-handler-panic"))
	value, err := proto.Marshal(&kafkamessage.KafkaBlockTopicMessage{Hash: hash.String(), URL: "http://peer", PeerId: "peerA"})
	require.NoError(t, err)

	require.NoError(t, server.consumerMessageHandler(ctx)(&kafka.KafkaMessage{Value: value}))
	require.True(t, server.blockMessageRetries.isPending(hash.String()), "a retry must be pending for the block")
	chain.AssertNotCalled(t, "ReportPeerFailure", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything)
}

// blockingExistsChain parks every GetBlockExists until its context ends, and records
// the error that context ended with.
type blockingExistsChain struct {
	blockchain.ClientI

	entered chan struct{}
	once    sync.Once
	sawErr  chan error
}

func (c *blockingExistsChain) GetBlockExists(ctx context.Context, _ *chainhash.Hash) (bool, error) {
	c.once.Do(func() { close(c.entered) })
	<-ctx.Done()
	c.sawErr <- ctx.Err()

	return false, ctx.Err()
}

// TestRetryBlockMessage_ShutdownCancelsActiveLookup pins that shutdown during a retry's
// existence lookup cancels the lookup itself, and that the slot is freed only after the
// handler has returned.
func TestRetryBlockMessage_ShutdownCancelsActiveLookup(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s, _, block := newProcessBlockFoundHarness(ctx, t)
	shortenRetryDelays(&s.blockMessageRetries)

	chain := &blockingExistsChain{ClientI: s.blockchainClient, entered: make(chan struct{}), sawErr: make(chan error, 1)}
	s.blockValidation = NewBlockValidation(ctx, ulogger.TestLogger{}, s.settings, chain, blobmemory.New(), s.txStore, s.utxoStore, nil, nil)

	retryCtx, stopRetries := context.WithCancel(ctx)
	hash := block.Hash().String()
	s.scheduleBlockMessageRetry(retryCtx, blockMsg(hash, "peerA"))

	select {
	case <-chain.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("the retry never reached the existence lookup")
	}

	stopRetries()

	select {
	case err := <-chain.sawErr:
		require.ErrorIs(t, err, context.Canceled, "the lookup must run on the retry's context")
	case <-time.After(5 * time.Second):
		t.Fatal("the lookup was not cancelled by shutdown")
	}

	require.Eventually(t, func() bool { return !s.blockMessageRetries.isPending(hash) }, 5*time.Second, 5*time.Millisecond)
}

// TestRetryBlockMessage_ShutdownCancelsActiveEnqueue pins that shutdown while a retry is
// handing its block to the validation queue cancels the hand-off, so nothing is queued
// after the retry has stopped.
func TestRetryBlockMessage_ShutdownCancelsActiveEnqueue(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s, _, block := newProcessBlockFoundHarness(ctx, t)
	shortenRetryDelays(&s.blockMessageRetries)

	chain := &gap003Chain{ClientI: s.blockchainClient, failHash: &chainhash.Hash{}}
	s.blockValidation = NewBlockValidation(ctx, ulogger.TestLogger{}, s.settings, chain, blobmemory.New(), s.txStore, s.utxoStore, nil, nil)

	// Unbuffered and unread: the handler blocks on the send.
	s.blockFoundCh = make(chan processBlockFound)

	retryCtx, stopRetries := context.WithCancel(ctx)
	hash := *block.Hash()
	s.scheduleBlockMessageRetry(retryCtx, blockMsg(hash.String(), "peerA"))

	require.Eventually(t, func() bool { return chain.count(hash) > 0 }, 5*time.Second, 5*time.Millisecond)
	time.Sleep(50 * time.Millisecond) // let the handler get from the lookup to the send

	stopRetries()

	require.Eventually(t, func() bool { return !s.blockMessageRetries.isPending(hash.String()) }, 5*time.Second, 5*time.Millisecond)

	select {
	case found := <-s.blockFoundCh:
		t.Fatalf("block %s was queued after the retry was cancelled", found.hash)
	case <-time.After(200 * time.Millisecond):
	}
}

// TestRetryBlockMessage_HandsOffOtherAnnouncers pins that a block announced by a second
// peer while the first announcement is being retried still reaches the validation queue
// from both peers, as it would have with no retry in between.
func TestRetryBlockMessage_HandsOffOtherAnnouncers(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s, _, block := newProcessBlockFoundHarness(ctx, t)
	shortenRetryDelays(&s.blockMessageRetries)

	hash := *block.Hash()
	chain := &gap003Chain{ClientI: s.blockchainClient, failHash: &chainhash.Hash{}}
	s.blockValidation = NewBlockValidation(ctx, ulogger.TestLogger{}, s.settings, chain, blobmemory.New(), s.txStore, s.utxoStore, nil, nil)
	s.blockFoundCh = make(chan processBlockFound, 4)
	s.blockMessageRetries.initialDelay = 100 * time.Millisecond

	s.scheduleBlockMessageRetry(ctx, blockMsg(hash.String(), "first"))
	s.scheduleBlockMessageRetry(ctx, blockMsg(hash.String(), "second")) // pending: kept as a source

	require.Eventually(t, func() bool { return !s.blockMessageRetries.isPending(hash.String()) }, 5*time.Second, 5*time.Millisecond)
	require.Len(t, s.blockFoundCh, 2)

	got := []string{(<-s.blockFoundCh).peerID, (<-s.blockFoundCh).peerID}
	require.Equal(t, []string{"first", "second"}, got)
}
