package blockvalidation

import (
	"bytes"
	"context"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/blockvalidation/blockvalidation_api"
	"github.com/bsv-blockchain/teranode/services/blockvalidation/testhelpers"
	"github.com/bsv-blockchain/teranode/services/p2p"
	blobmemory "github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/kafka"
	kafkamessage "github.com/bsv-blockchain/teranode/util/kafka/kafka_message"
	"github.com/jarcoal/httpmock"
	"github.com/jellydator/ttlcache/v3"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

const (
	gap054EnvVar   = "GAP_BV054_CHILD"
	gap054OKMarker = "GAP_BV054_CHILD_SCENARIO_OK"
)

// gap054PanicChain wraps a real blockchain client and panics on the first call to the chosen
// method (after signalling on `panicking`), then delegates normally. With always set, it panics
// on each call to that method.
type gap054PanicChain struct {
	blockchain.ClientI
	onHeader   bool // true: panic in GetBlockHeader; false: panic in GetBlockExists
	always     bool
	calls      atomic.Int32
	panicking  chan struct{}
	panicValue string
}

func (c *gap054PanicChain) maybePanic(isHeaderCall bool) {
	if isHeaderCall != c.onHeader {
		return
	}

	n := c.calls.Add(1)
	if n == 1 {
		close(c.panicking)
	}

	if n == 1 || c.always {
		panic(c.panicValue)
	}
}

func (c *gap054PanicChain) GetBlockHeader(ctx context.Context, h *chainhash.Hash) (*model.BlockHeader, *model.BlockHeaderMeta, error) {
	c.maybePanic(true)
	return c.ClientI.GetBlockHeader(ctx, h)
}

func (c *gap054PanicChain) GetBlockExists(ctx context.Context, h *chainhash.Hash) (bool, error) {
	c.maybePanic(false)
	return c.ClientI.GetBlockExists(ctx, h)
}

// gap054PeerReportCounter wraps a blockchain client and counts ReportPeerFailure calls.
type gap054PeerReportCounter struct {
	blockchain.ClientI
	reports atomic.Int32
}

func (c *gap054PeerReportCounter) ReportPeerFailure(context.Context, *chainhash.Hash, string, string, string) error {
	c.reports.Add(1)
	return nil
}

// gap054RunChild re-executes this test binary running only childTest with the env flag set, and
// fails the (parent) test if the child process died or did not reach the success marker. A panic
// in a goroutine kills the whole test binary, so this is what turns "process crashed" into a
// clean, reportable failure.
func gap054RunChild(t *testing.T, childTest, specID string) {
	t.Helper()

	cmd := exec.Command(os.Args[0], "-test.run", "^"+childTest+"$", "-test.count=1", "-test.v", "-test.timeout=90s")
	cmd.Env = append(os.Environ(), gap054EnvVar+"=1")

	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &out

	err := cmd.Run()
	output := out.String()

	if err != nil || !strings.Contains(output, gap054OKMarker) {
		tail := output
		if idx := strings.Index(output, "panic:"); idx >= 0 {
			tail = output[idx:]
		}

		if len(tail) > 1500 {
			tail = tail[:1500]
		}

		t.Fatalf("%s: the service process did not survive an injected panic (child exit: %v, scenario completed: %v)\n--- child output ---\n%s",
			specID, err, strings.Contains(output, gap054OKMarker), tail)
	}
}

func gap054ChildOnly(t *testing.T) {
	t.Helper()

	if os.Getenv(gap054EnvVar) != "1" {
		t.Skip("subprocess scenario; driven by its parent test")
	}
}

// TestGapBV054_PriorityWorkerSurvivesPanic proves GAP-BV-054 for the priority-queue worker.
//
// Spec: BV-STARTUP-007: a panic while processing an announcement MUST be recovered and the
// service MUST continue processing other announcements (AC-BV-STARTUP-007.1).
//
// A panic is injected into the validation stage run by blockProcessingWorker (via
// processBlockWithPriority -> processBlockFound -> blockchainClient.GetBlockHeader). The
// scenario runs in a subprocess; the parent fails if the child process dies, which is what an
// unrecovered panic in the worker does (bitcoin-sv/teranode#4985).
func TestGapBV054_PriorityWorkerSurvivesPanic(t *testing.T) {
	if os.Getenv(gap054EnvVar) == "1" {
		t.Skip("parent driver")
	}

	gap054RunChild(t, "TestGapBV054_PriorityWorkerSurvivesPanic_Child", "BV-STARTUP-007 (blockProcessingWorker)")
}

func TestGapBV054_PriorityWorkerSurvivesPanic_Child(t *testing.T) {
	gap054ChildOnly(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s, _, block := newProcessBlockFoundHarness(ctx, t)

	chain := &gap054PanicChain{ClientI: s.blockchainClient, onHeader: true, panicking: make(chan struct{}), panicValue: "injected panic in priority worker"}
	s.blockchainClient = chain // only processBlockFound (worker stage) reads this field

	require.NoError(t, s.Init(ctx))

	blockBytes, err := block.Bytes()
	require.NoError(t, err)

	httpmock.ActivateNonDefault(util.HTTPClient())
	defer httpmock.DeactivateAndReset()

	httpmock.RegisterResponder("GET", "http://test-peer/block/"+block.Hash().String(),
		httpmock.NewBytesResponder(200, blockBytes))

	announce := func() {
		_, callErr := s.BlockFound(ctx, &blockvalidation_api.BlockFoundRequest{
			Hash:    block.Hash()[:],
			BaseUrl: "http://test-peer",
			PeerId:  "peerA",
		})
		require.NoError(t, callErr)
	}

	announce()

	select {
	case <-chain.panicking:
	case <-time.After(10 * time.Second):
		t.Fatal("setup: the worker never reached the injected panic")
	}

	// The service must still be alive after the panic and process a later announcement. The
	// worker drops an announcement of a hash it is still processing, and the panic signal above
	// fires before the worker has unwound, so one announcement now could be dropped. Announce
	// again on each poll until the block is stored. That waits on the end state, with no fixed
	// delay. An unrecovered panic kills this child process, so the parent test still fails.
	require.Eventually(t, func() bool {
		_, callErr := s.BlockFound(ctx, &blockvalidation_api.BlockFoundRequest{
			Hash:    block.Hash()[:],
			BaseUrl: "http://test-peer",
			PeerId:  "peerA",
		})
		if callErr != nil {
			return false
		}

		exists, existsErr := s.blockValidation.GetBlockExists(ctx, block.Hash())

		return existsErr == nil && exists
	}, 10*time.Second, 100*time.Millisecond, "BV-STARTUP-007: a block announced after the panic was never processed")

	println(gap054OKMarker)
}

// TestGapBV054_CatchupConsumerSurvivesPanic proves GAP-BV-054 for the catchup consumer.
//
// Spec: BV-STARTUP-007 (AC-BV-STARTUP-007.1). A panic is injected into catchupFunc, which the
// Init catchup consumer goroutine runs via processCatchupChItem. A second catchup item must
// still be consumed afterwards, and the panicking block's in-progress marker must be cleared so
// a later announcement can retry it. Subprocess-isolated (bitcoin-sv/teranode#4985).
func TestGapBV054_CatchupConsumerSurvivesPanic(t *testing.T) {
	if os.Getenv(gap054EnvVar) == "1" {
		t.Skip("parent driver")
	}

	gap054RunChild(t, "TestGapBV054_CatchupConsumerSurvivesPanic_Child", "BV-STARTUP-007 (processCatchupChItem)")
}

func TestGapBV054_CatchupConsumerSurvivesPanic_Child(t *testing.T) {
	gap054ChildOnly(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s, _, block := newProcessBlockFoundHarness(ctx, t)

	var calls atomic.Int32

	panicking := make(chan struct{})
	secondCall := make(chan struct{})

	s.catchupFunc = func(_ context.Context, _ *model.Block, _, _ string) error {
		switch calls.Add(1) {
		case 1:
			close(panicking)
			panic("injected panic in catchup")
		case 2:
			close(secondCall)
		}

		return nil
	}

	require.NoError(t, s.Init(ctx))

	item := processBlockCatchup{block: block, baseURL: "http://test-peer", peerID: ""}

	// queueFetchedBlock sets this marker before sending to catchupCh; the recover must clear it.
	s.processBlockNotify.Set(*block.Hash(), true, ttlcache.DefaultTTL)
	s.catchupAlternatives.Set(*block.Hash(), []processBlockCatchup{{block: block, peerID: "altpeer"}}, ttlcache.DefaultTTL)

	s.catchupCh <- item

	select {
	case <-panicking:
	case <-time.After(10 * time.Second):
		t.Fatal("setup: the catchup consumer never ran catchupFunc")
	}

	// The panic signal fires before the recover runs. The recover counts the attempt and then
	// clears both markers, the alternatives last, so wait for that end state.
	require.Eventually(t, func() bool {
		return s.processBlockNotify.Get(*block.Hash()) == nil && s.catchupAlternatives.Get(*block.Hash()) == nil
	}, 10*time.Second, 10*time.Millisecond, "the in-progress marker and the cached alternatives must be cleared after the panic")

	attempts := s.blockCatchupAttempts.Get(*block.Hash())
	require.NotNil(t, attempts, "the panicked cycle must count toward the per-block catchup cap")
	require.Equal(t, 1, attempts.Value())

	s.catchupCh <- item

	select {
	case <-secondCall:
		println(gap054OKMarker)
	case <-time.After(10 * time.Second):
		t.Fatal("BV-STARTUP-007: the catchup consumer did not process the item queued after the panic")
	}
}

// TestGapBV054_KafkaHandlerSurvivesPanic proves GAP-BV-054 for consumerMessageHandler, which
// runs blockHandler through runBlockHandler. A panic is injected into the existence check
// blockHandler performs; the handler must return nil so the message is committed rather than
// redelivered by Kafka (the in-memory retry takes it from there), must not report the peer,
// must count the panic, and a following message must still be handled.
// Subprocess-isolated (bitcoin-sv/teranode#4985).
func TestGapBV054_KafkaHandlerSurvivesPanic(t *testing.T) {
	if os.Getenv(gap054EnvVar) == "1" {
		t.Skip("parent driver")
	}

	gap054RunChild(t, "TestGapBV054_KafkaHandlerSurvivesPanic_Child", "BV-STARTUP-007 (consumerMessageHandler goroutine)")
}

func TestGapBV054_KafkaHandlerSurvivesPanic_Child(t *testing.T) {
	gap054ChildOnly(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s, _, block := newProcessBlockFoundHarness(ctx, t)

	chain := &gap054PanicChain{ClientI: s.blockchainClient, onHeader: false, panicking: make(chan struct{}), panicValue: "injected panic in kafka handler"}
	// blockHandler's GetBlockExists goes through s.blockValidation's blockchain client.
	s.blockValidation = NewBlockValidation(ctx, ulogger.TestLogger{}, s.settings, chain, blobmemory.New(), s.txStore, s.utxoStore, nil, nil)

	msgBytes, err := proto.Marshal(&kafkamessage.KafkaBlockTopicMessage{
		Hash:   block.Hash().String(),
		URL:    "http://test-peer",
		PeerId: "peerA",
	})
	require.NoError(t, err)

	reporter := &gap054PeerReportCounter{ClientI: s.blockchainClient}
	s.blockchainClient = reporter

	handler := s.consumerMessageHandler(ctx)

	initPrometheusMetrics()
	panicsBefore := testutil.ToFloat64(prometheusRecoveredPanics.WithLabelValues(recoveredPanicPathKafka))

	call := func() chan error {
		ch := make(chan error, 1)
		go func() { ch <- handler(&kafka.KafkaMessage{Value: msgBytes}) }()

		return ch
	}

	select {
	case herr := <-call():
		require.NoError(t, herr, "a panicking message must be committed, not redelivered")
		require.Zero(t, reporter.reports.Load(), "a panic in this node must not be reported as a peer failure")
		require.Equal(t, panicsBefore+1, testutil.ToFloat64(prometheusRecoveredPanics.WithLabelValues(recoveredPanicPathKafka)), "the recover must count the panic")
	case <-time.After(10 * time.Second):
		t.Fatal("the handler did not return for the panicking message")
	}

	select {
	case herr := <-call():
		require.NoError(t, herr)
		require.Equal(t, 1, len(s.blockFoundCh), "the message after the panic must be handled and queued")
		println(gap054OKMarker)
	case <-time.After(10 * time.Second):
		t.Fatal("the handler did not return for the message after the panic")
	}
}

// gap054ReportPanicChain wraps a blockchain client and panics in ReportPeerFailure.
type gap054ReportPanicChain struct {
	blockchain.ClientI
}

func (c *gap054ReportPanicChain) ReportPeerFailure(context.Context, *chainhash.Hash, string, string, string) error {
	panic("injected panic in ReportPeerFailure")
}

// TestGapBV054_CatchupPanicInRealCatchupReleasesLock runs the real catchup() (not an injected
// catchupFunc) and panics in it after acquireCatchupLock, in the block-existence check that
// catchupGetBlockHeaders makes on the consumer goroutine. The recover in processCatchupChItem
// must see catchup's deferred releaseCatchupLock run first: the catchup flag is clear, the
// active catchup context is gone, the attempt is counted one time and the markers are cleared.
// releaseCatchupLock must also record the cycle as a local failure, not as a success: the health
// check reads lastCatchupResult false, catchupSuccesses does not change, and the dashboard
// attempt has the error type local_panic, which no peer is charged for.
func TestGapBV054_CatchupPanicInRealCatchupReleasesLock(t *testing.T) {
	initPrometheusMetrics()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s, _, block := newProcessBlockFoundHarness(ctx, t)

	chain := &gap054PanicChain{ClientI: s.blockchainClient, onHeader: false, panicking: make(chan struct{}), panicValue: "injected panic in catchup"}
	s.blockValidation = NewBlockValidation(ctx, ulogger.TestLogger{}, s.settings, chain, blobmemory.New(), s.txStore, s.utxoStore, nil, nil)

	s.processBlockNotify.Set(*block.Hash(), true, ttlcache.DefaultTTL)
	s.catchupAlternatives.Set(*block.Hash(), []processBlockCatchup{{block: block, peerID: "altpeer"}}, ttlcache.DefaultTTL)

	// A success from an earlier cycle, so the test sees the value change to false.
	s.catchupStatsMu.Lock()
	s.lastCatchupResult = true
	s.catchupStatsMu.Unlock()

	successesBefore := s.catchupSuccesses.Load()
	panicsBefore := testutil.ToFloat64(prometheusRecoveredPanics.WithLabelValues(recoveredPanicPathCatchup))

	s.processCatchupChItem(ctx, processBlockCatchup{block: block, baseURL: "http://test-peer", peerID: ""})

	select {
	case <-chain.panicking:
	default:
		t.Fatal("setup: catchup never reached the injected panic")
	}

	require.False(t, s.isCatchingUp.Load(), "catchup must release its lock during the unwind")

	s.activeCatchupCtxMu.RLock()
	active := s.activeCatchupCtx
	previous := s.previousCatchupAttempt
	s.activeCatchupCtxMu.RUnlock()
	require.Nil(t, active, "the active catchup context must be cleared during the unwind")

	require.Nil(t, s.processBlockNotify.Get(*block.Hash()))
	require.Nil(t, s.catchupAlternatives.Get(*block.Hash()))

	attempts := s.blockCatchupAttempts.Get(*block.Hash())
	require.NotNil(t, attempts)
	require.Equal(t, 1, attempts.Value())

	s.catchupStatsMu.RLock()
	lastResult := s.lastCatchupResult
	s.catchupStatsMu.RUnlock()
	require.False(t, lastResult, "a panicked catchup must not read as a success in the health check")
	require.Equal(t, successesBefore, s.catchupSuccesses.Load(), "a panicked catchup must not count as a success")

	require.NotNil(t, previous, "a panicked catchup must be recorded as a failed attempt")
	require.Equal(t, "local_panic", previous.ErrorType, "a panic is local and must not be scored against the peer")

	require.Equal(t, panicsBefore+1, testutil.ToFloat64(prometheusRecoveredPanics.WithLabelValues(recoveredPanicPathCatchup)))
}

// TestGapBV054_CatchupPanicBeforeLockCountsAttempt panics before any catchup takes the lock, while
// blocksValidated still holds a positive count from an earlier cycle. The recover must count the
// attempt, not read the stale count as progress and clear the counter.
func TestGapBV054_CatchupPanicBeforeLockCountsAttempt(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s, _, block := newProcessBlockFoundHarness(ctx, t)

	s.blocksValidated.Store(5)
	s.blockCatchupAttempts.Set(*block.Hash(), 1, ttlcache.DefaultTTL)

	s.catchupFunc = func(context.Context, *model.Block, string, string) error {
		panic("injected panic before the catchup lock")
	}

	s.processCatchupChItem(ctx, processBlockCatchup{block: block, baseURL: "http://test-peer", peerID: "peerA"})

	attempts := s.blockCatchupAttempts.Get(*block.Hash())
	require.NotNil(t, attempts, "a panic before the lock must not clear the attempt counter")
	require.Equal(t, 2, attempts.Value(), "a panic before the lock must count toward the per-block catchup cap")
}

// gap054SignalLogger wraps a logger and closes seen on the first Debugf call whose format holds
// match, so a test can wait for a code path that has no other observable signal.
type gap054SignalLogger struct {
	ulogger.Logger
	match string
	seen  chan struct{}
	once  sync.Once
}

func (l *gap054SignalLogger) Debugf(format string, args ...interface{}) {
	if strings.Contains(format, l.match) {
		l.once.Do(func() { close(l.seen) })
	}

	l.Logger.Debugf(format, args...)
}

// gap054RunOnce runs runOncePerBlock in a goroutine and sends its error, or the recovered panic
// value, on the returned channels.
func gap054RunOnce(bv *BlockValidation, hash *chainhash.Hash, validate func(*ValidateBlockOptions) error) (chan error, chan any) {
	errCh := make(chan error, 1)
	panicCh := make(chan any, 1)

	go func() {
		defer func() {
			if r := recover(); r != nil {
				panicCh <- r
			}
		}()

		errCh <- bv.runOncePerBlock(hash, &ValidateBlockOptions{}, validate)
	}()

	return errCh, panicCh
}

// TestGapBV054_PanicInValidateReleasesWaiters panics inside the validate function of
// runOncePerBlock while a second caller waits for the same hash. The panic must continue to the
// first caller, the waiter must get an ERR_PROCESSING error, the entry must go after the grace
// window, and a later validation of the hash must run again.
func TestGapBV054_PanicInValidateReleasesWaiters(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s, _, block := newProcessBlockFoundHarness(ctx, t)
	waiting := &gap054SignalLogger{Logger: ulogger.TestLogger{}, match: "waiting for concurrent validation", seen: make(chan struct{})}
	bv := NewBlockValidation(ctx, waiting, s.settings, s.blockchainClient, blobmemory.New(), s.txStore, s.utxoStore, nil, nil)
	hash := block.Hash()

	started := make(chan struct{})
	release := make(chan struct{})

	_, firstPanic := gap054RunOnce(bv, hash, func(*ValidateBlockOptions) error {
		close(started)
		<-release
		panic("injected panic in validate")
	})

	<-started

	waiterErr, waiterPanic := gap054RunOnce(bv, hash, func(*ValidateBlockOptions) error {
		return errors.NewProcessingError("the waiter must not run its own validation")
	})

	// Release the first caller only once the waiter has found the entry and waits on it.
	// runOncePerBlock logs "waiting for concurrent validation" after that lookup, so the waiter
	// gets the stored result even if it reaches the done channel after the panic closed it.
	select {
	case <-waiting.seen:
	case <-time.After(5 * time.Second):
		t.Fatal("setup: the second caller never waited on the first caller's entry")
	}

	close(release)

	select {
	case r := <-firstPanic:
		require.Equal(t, "injected panic in validate", r, "the panic must continue to the first caller")
	case <-time.After(5 * time.Second):
		t.Fatal("the first caller did not panic")
	}

	select {
	case err := <-waiterErr:
		require.Error(t, err)
		require.True(t, errors.Is(err, errors.ErrProcessing), "the waiter must get ERR_PROCESSING, got %v", err)
		require.True(t, isLocalPanic(err), "the waiter's error must carry the local-panic marker, got %v", err)
		require.NotContains(t, err.Error(), "must not run", "the waiter must get the stored result, not run validate")
	case r := <-waiterPanic:
		t.Fatalf("the waiter panicked: %v", r)
	case <-time.After(5 * time.Second):
		t.Fatal("the waiter for the same hash is wedged after the panic")
	}

	require.Eventually(t, func() bool {
		_, ok := bv.blocksCurrentlyValidating.Get(*hash)
		return !ok
	}, 5*time.Second, 10*time.Millisecond, "the entry must go after the grace window")

	var ran atomic.Bool

	laterErr, _ := gap054RunOnce(bv, hash, func(*ValidateBlockOptions) error {
		ran.Store(true)
		return nil
	})

	select {
	case err := <-laterErr:
		require.NoError(t, err)
		require.True(t, ran.Load(), "a later validation of the hash must run again")
	case <-time.After(5 * time.Second):
		t.Fatal("a later validation of the hash is wedged after the panic")
	}
}

// TestGapBV054_PanicInValidateBlockDoesNotWedgeHash runs the real ValidateBlockWithOptions and
// panics in its first blockchain lookup, which is inside the runOncePerBlock closure. After the
// panic, a new validation of the same hash must complete and must reach the blockchain again.
func TestGapBV054_PanicInValidateBlockDoesNotWedgeHash(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s, _, block := newProcessBlockFoundHarness(ctx, t)

	chain := &gap054PanicChain{ClientI: s.blockchainClient, onHeader: false, panicking: make(chan struct{}), panicValue: "injected panic in ValidateBlock"}
	bv := NewBlockValidation(ctx, ulogger.TestLogger{}, s.settings, chain, blobmemory.New(), s.txStore, s.utxoStore, nil, nil)

	require.PanicsWithValue(t, "injected panic in ValidateBlock", func() {
		_ = bv.ValidateBlockWithOptions(ctx, block, "http://test-peer", nil)
	})

	require.Eventually(t, func() bool {
		_, ok := bv.blocksCurrentlyValidating.Get(*block.Hash())
		return !ok
	}, 5*time.Second, 10*time.Millisecond, "the entry must go after the grace window")

	done := make(chan struct{})

	go func() {
		defer close(done)
		_ = bv.ValidateBlockWithOptions(ctx, block, "http://test-peer", nil)
	}()

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("a validation of the hash after the panic is wedged")
	}

	require.GreaterOrEqual(t, chain.calls.Load(), int32(2), "the second validation must run, not get a stored result")
}

// TestGapBV054_WorkerPanicCapSuppressesRedelivery panics on each delivery of one block from one
// peer, at the parent-header lookup that follows the download. After MaxCorruptAttemptsPerBlock
// panics, processBlockFound must stop that peer's deliveries before the download, with an error
// that does not requeue the block. A different peer must keep its own budget.
func TestGapBV054_WorkerPanicCapSuppressesRedelivery(t *testing.T) {
	initPrometheusMetrics()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s, _, block := newProcessBlockFoundHarness(ctx, t)
	s.settings.BlockValidation.MaxCorruptAttemptsPerBlock = 2

	chain := &gap054PanicChain{ClientI: s.blockchainClient, onHeader: true, always: true, panicking: make(chan struct{}), panicValue: "injected panic on each delivery"}
	s.blockchainClient = chain

	blockBytes, err := block.Bytes()
	require.NoError(t, err)

	httpmock.ActivateNonDefault(util.HTTPClient())
	defer httpmock.DeactivateAndReset()

	httpmock.RegisterResponder("GET", "http://test-peer/block/"+block.Hash().String(),
		httpmock.NewBytesResponder(200, blockBytes))

	deliver := func(peerID string) error {
		return s.processBlockWithPriorityRecovered(ctx, processBlockFound{hash: block.Hash(), baseURL: "http://test-peer", peerID: peerID})
	}

	panicsBefore := testutil.ToFloat64(prometheusRecoveredPanics.WithLabelValues(recoveredPanicPathWorker))

	for i := 1; i <= 2; i++ {
		err := deliver("peerA")
		require.Error(t, err)
		require.Contains(t, err.Error(), "see the PANIC log line", "delivery %d must panic", i)
		require.Equal(t, int32(i), chain.calls.Load())
	}

	require.Equal(t, panicsBefore+2, testutil.ToFloat64(prometheusRecoveredPanics.WithLabelValues(recoveredPanicPathWorker)))

	downloadsBefore := httpmock.GetTotalCallCount()

	err = deliver("peerA")
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.ErrProcessing), "a capped delivery must be ERR_PROCESSING, got %v", err)
	require.Contains(t, err.Error(), "recovered-panic cap reached")
	require.False(t, errors.IsBlockCorrupt(err))
	require.False(t, errors.IsNetworkError(err), "a capped delivery must not requeue as a fetch failure: %v", err)
	require.False(t, errors.IsMaliciousResponseError(err), "a capped delivery must not requeue as a fetch failure: %v", err)
	require.Equal(t, int32(2), chain.calls.Load(), "a capped delivery must stop before the stage that panics")
	require.Equal(t, downloadsBefore, httpmock.GetTotalCallCount(), "a capped delivery must stop before the download")
	require.Equal(t, panicsBefore+2, testutil.ToFloat64(prometheusRecoveredPanics.WithLabelValues(recoveredPanicPathWorker)))

	err = deliver("peerB")
	require.Error(t, err)
	require.Contains(t, err.Error(), "see the PANIC log line", "a different peer must keep its own budget")
	require.Equal(t, int32(3), chain.calls.Load())
}

// TestGapBV054_CatchupPanicAfterCountDoesNotCountTwice makes the cycle fail with ErrExternal, which
// counts the attempt, and then panics in the ReportPeerFailure call that follows the count. The
// recover must not count the same cycle a second time.
func TestGapBV054_CatchupPanicAfterCountDoesNotCountTwice(t *testing.T) {
	initPrometheusMetrics()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s, _, block := newProcessBlockFoundHarness(ctx, t)

	s.blockchainClient = &gap054ReportPanicChain{ClientI: s.blockchainClient}
	s.catchupFunc = func(context.Context, *model.Block, string, string) error {
		return errors.NewExternalError("all peers failed to serve subtree data")
	}

	s.processBlockNotify.Set(*block.Hash(), true, ttlcache.DefaultTTL)

	s.processCatchupChItem(ctx, processBlockCatchup{block: block, baseURL: "http://test-peer", peerID: "peerA"})

	attempts := s.blockCatchupAttempts.Get(*block.Hash())
	require.NotNil(t, attempts)
	require.Equal(t, 1, attempts.Value(), "a panic after the cycle was counted must not count it again")
	require.Nil(t, s.processBlockNotify.Get(*block.Hash()))
}

// TestGapBV054_WorkerPanicErrorIsNotAFetchFailure panics with a value that holds words the
// substring classifiers look for. The worker reads the recovered error with IsNetworkError and
// IsMaliciousResponseError, and a match requeues the block as a fetch failure. The recovered
// error must match neither.
func TestGapBV054_WorkerPanicErrorIsNotAFetchFailure(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s, _, block := newProcessBlockFoundHarness(ctx, t)

	chain := &gap054PanicChain{ClientI: s.blockchainClient, onHeader: false, panicking: make(chan struct{}), panicValue: "http connection timeout: corrupt malformed response"}
	s.blockValidation = NewBlockValidation(ctx, ulogger.TestLogger{}, s.settings, chain, blobmemory.New(), s.txStore, s.utxoStore, nil, nil)

	initPrometheusMetrics()
	panicsBefore := testutil.ToFloat64(prometheusRecoveredPanics.WithLabelValues(recoveredPanicPathWorker))

	err := s.processBlockWithPriorityRecovered(ctx, processBlockFound{hash: block.Hash(), baseURL: "http://test-peer", peerID: "peerA"})

	require.Equal(t, panicsBefore+1, testutil.ToFloat64(prometheusRecoveredPanics.WithLabelValues(recoveredPanicPathWorker)), "the recover must count the panic")

	select {
	case <-chain.panicking:
	default:
		t.Fatal("setup: processing never reached the injected panic")
	}

	require.Error(t, err)
	require.True(t, errors.Is(err, errors.ErrProcessing), "a recovered panic must be ERR_PROCESSING, got %v", err)
	require.False(t, errors.IsBlockCorrupt(err))
	require.False(t, errors.IsNetworkError(err), "a recovered panic must not read as a network error: %v", err)
	require.False(t, errors.IsMaliciousResponseError(err), "a recovered panic must not read as a malicious response: %v", err)
}

// gap054AltHarness sets up a block whose parent-header lookup panics on each call, a primary peer
// whose download fails with a network error, and an alternative peer that serves the block. The
// cap is 2 panics per (hash, peerID).
func gap054AltHarness(ctx context.Context, t *testing.T) (*Server, *model.Block, *gap054PanicChain, string) {
	t.Helper()

	initPrometheusMetrics()

	s, _, block := newProcessBlockFoundHarness(ctx, t)
	s.settings.BlockValidation.MaxCorruptAttemptsPerBlock = 2

	chain := &gap054PanicChain{ClientI: s.blockchainClient, onHeader: true, always: true, panicking: make(chan struct{}), panicValue: "injected panic from the alternative's block"}
	s.blockchainClient = chain

	blockBytes, err := block.Bytes()
	require.NoError(t, err)

	httpmock.ActivateNonDefault(util.HTTPClient())
	t.Cleanup(httpmock.DeactivateAndReset)

	httpmock.RegisterResponder("GET", "http://primary-peer/block/"+block.Hash().String(),
		httpmock.NewErrorResponder(errors.NewNetworkError("network error")))
	httpmock.RegisterResponder("GET", "http://alt-peer/block/"+block.Hash().String(),
		httpmock.NewBytesResponder(200, blockBytes))

	return s, block, chain, "GET http://primary-peer/block/" + block.Hash().String()
}

// TestGapBV054_AlternativePanicChargesAlternative has a primary whose download fails with a network
// error and an alternative whose block panics. processBlockWithPriority walks to the alternative.
// The panic must count for the alternative, which is capped after the limit. The primary must keep
// its budget and still reach its download.
func TestGapBV054_AlternativePanicChargesAlternative(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s, block, chain, primaryKey := gap054AltHarness(ctx, t)
	hash := block.Hash()

	primary := processBlockFound{hash: hash, baseURL: "http://primary-peer", peerID: "primaryPeer"}
	alternative := processBlockFound{hash: hash, baseURL: "http://alt-peer", peerID: "altPeer"}

	// A newer queue entry for the hash. The alternative sources hang off it.
	s.blockPriorityQueue.Add(processBlockFound{hash: hash, baseURL: "http://other-peer", peerID: "otherPeer"}, PriorityChainExtending, block.Height)

	for i := 1; i <= 3; i++ {
		s.blockPriorityQueue.Add(alternative, PriorityChainExtending, block.Height)

		primaryCallsBefore := httpmock.GetCallCountInfo()[primaryKey]

		err := s.processBlockWithPriorityRecovered(ctx, primary)
		require.Error(t, err)
		require.True(t, errors.IsNetworkError(err), "round %d: the primary's fetch error must come back, got %v", i, err)
		require.Equal(t, primaryCallsBefore+1, httpmock.GetCallCountInfo()[primaryKey], "round %d: the primary must not be capped", i)
	}

	require.Equal(t, int32(2), chain.calls.Load(), "the third alternative delivery must stop at the cap, before the stage that panics")
	require.True(t, s.panicAttemptsExhausted(hash, "altPeer"), "the alternative whose block panics must be capped")
	require.False(t, s.panicAttemptsExhausted(hash, "primaryPeer"), "the primary must keep its budget")
	require.Nil(t, s.blockPanicAttempts.Get(blockAttemptKey{hash: *hash, peerID: "primaryPeer"}), "no panic may count for the primary")
}

// TestGapBV054_RetryPanicChargesAlternative runs a retry item, which has an empty peerID, while the
// queue holds an alternative whose block panics. The retry path processes the alternative, and the
// panic must count for the alternative, not for the empty peerID of the retry item.
func TestGapBV054_RetryPanicChargesAlternative(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s, block, chain, _ := gap054AltHarness(ctx, t)
	hash := block.Hash()

	s.blockPriorityQueue.Add(processBlockFound{hash: hash, baseURL: "http://other-peer", peerID: "otherPeer"}, PriorityChainExtending, block.Height)

	for i := 1; i <= 2; i++ {
		s.blockPriorityQueue.Add(processBlockFound{hash: hash, baseURL: "http://alt-peer", peerID: "altPeer"}, PriorityChainExtending, block.Height)

		err := s.processBlockWithPriorityRecovered(ctx, processBlockFound{hash: hash, baseURL: SourceTypeRetry, peerID: ""})
		require.Error(t, err)
		require.Contains(t, err.Error(), "see the PANIC log line", "round %d: the alternative must panic", i)
	}

	require.Equal(t, int32(2), chain.calls.Load())
	require.True(t, s.panicAttemptsExhausted(hash, "altPeer"), "the retry's alternative must be capped after the limit")
}

// TestGapBV054_BlockFoundChPanicIsCountedAndAnswersCaller panics in processBlockFoundChannel on a
// blockFoundCh worker, with a panic value that holds words the substring classifiers look for.
// The recover must count the panic on the block_found path, and a WaitToComplete caller, which
// waits on an unbuffered errCh, must get an ERR_PROCESSING error that is not a network, malicious
// or corrupt error and does not hold the panic value.
func TestGapBV054_BlockFoundChPanicIsCountedAndAnswersCaller(t *testing.T) {
	initPrometheusMetrics()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s, _, block := newProcessBlockFoundHarness(ctx, t)

	panicValue := "http connection timeout: corrupt malformed response"
	chain := &gap054PanicChain{ClientI: s.blockchainClient, onHeader: false, panicking: make(chan struct{}), panicValue: panicValue}
	s.blockValidation = NewBlockValidation(ctx, ulogger.TestLogger{}, s.settings, chain, blobmemory.New(), s.txStore, s.utxoStore, nil, nil)

	panicsBefore := testutil.ToFloat64(prometheusRecoveredPanics.WithLabelValues(recoveredPanicPathBlockFound))

	errCh := make(chan error)

	done := make(chan struct{})

	go func() {
		s.processBlockFoundChItem(ctx, 0, processBlockFound{hash: block.Hash(), baseURL: "http://test-peer", peerID: "peerA", errCh: errCh})
		close(done)
	}()

	var err error

	select {
	case err = <-errCh:
	case <-time.After(10 * time.Second):
		t.Fatal("the WaitToComplete caller got no answer after the panic")
	}

	gap054RequireAnsweredOnce(t, errCh, done)

	select {
	case <-chain.panicking:
	default:
		t.Fatal("setup: processing never reached the injected panic")
	}

	require.Equal(t, panicsBefore+1, testutil.ToFloat64(prometheusRecoveredPanics.WithLabelValues(recoveredPanicPathBlockFound)), "the recover must count the panic on the block_found path")

	require.Error(t, err)
	require.True(t, errors.Is(err, errors.ErrProcessing), "the caller must get ERR_PROCESSING, got %v", err)
	require.NotContains(t, err.Error(), panicValue, "the panic value must stay out of the error text")
	require.False(t, errors.IsBlockCorrupt(err))
	require.False(t, errors.IsNetworkError(err), "%v", err)
	require.False(t, errors.IsMaliciousResponseError(err), "%v", err)
}

// gap054RequireAnsweredOnce checks, after the caller took one answer from errCh, that the
// blockFoundCh worker returned and sent no second answer. A second send on the unbuffered errCh
// would block the worker, so done would never close.
func gap054RequireAnsweredOnce(t *testing.T, errCh chan error, done chan struct{}) {
	t.Helper()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("the blockFoundCh worker did not return after it answered the caller: a second send on the unbuffered errCh blocks it for ever")
	}

	select {
	case extra := <-errCh:
		t.Fatalf("the caller got a second answer: %v", extra)
	default:
	}
}

// gap054ExistsChain wraps a blockchain client and reports each block as already stored.
type gap054ExistsChain struct {
	blockchain.ClientI
}

func (c *gap054ExistsChain) GetBlockExists(context.Context, *chainhash.Hash) (bool, error) {
	return true, nil
}

// gap054PanicLogger wraps a logger and panics on a Debugf call whose format holds match.
type gap054PanicLogger struct {
	ulogger.Logger
	match string
}

func (l *gap054PanicLogger) Debugf(format string, args ...interface{}) {
	if strings.Contains(format, l.match) {
		panic("injected panic after the caller was answered")
	}

	l.Logger.Debugf(format, args...)
}

// TestGapBV054_BlockFoundChPanicAfterAnswerDoesNotBlockWorker answers a WaitToComplete caller and
// then panics on the same blockFoundCh worker. processBlockFoundChannel answers nil for a block
// that is already stored, and the panic comes after it returns. The caller must get that one nil
// answer and no panic error, and the worker must return, so the pool does not lose a worker.
func TestGapBV054_BlockFoundChPanicAfterAnswerDoesNotBlockWorker(t *testing.T) {
	initPrometheusMetrics()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s, _, block := newProcessBlockFoundHarness(ctx, t)

	s.blockValidation = NewBlockValidation(ctx, ulogger.TestLogger{}, s.settings, &gap054ExistsChain{ClientI: s.blockchainClient}, blobmemory.New(), s.txStore, s.utxoStore, nil, nil)
	s.logger = &gap054PanicLogger{Logger: ulogger.TestLogger{}, match: "processBlockFoundChannel completed successfully"}

	panicsBefore := testutil.ToFloat64(prometheusRecoveredPanics.WithLabelValues(recoveredPanicPathBlockFound))

	for i := 1; i <= 2; i++ {
		errCh := make(chan error)
		done := make(chan struct{})

		go func() {
			s.processBlockFoundChItem(ctx, 0, processBlockFound{hash: block.Hash(), baseURL: "http://test-peer", peerID: "peerA", errCh: errCh})
			close(done)
		}()

		select {
		case err := <-errCh:
			require.NoError(t, err, "item %d: the caller must get the answer the processing gave, not the panic error", i)
		case <-time.After(10 * time.Second):
			t.Fatalf("item %d: the WaitToComplete caller got no answer", i)
		}

		gap054RequireAnsweredOnce(t, errCh, done)
		require.Equal(t, panicsBefore+float64(i), testutil.ToFloat64(prometheusRecoveredPanics.WithLabelValues(recoveredPanicPathBlockFound)), "item %d: the panic after the answer must be recovered and counted", i)
	}
}

// gap054ParentPanicChain wraps a blockchain client and panics on each GetBlockExists call for one
// hash. With the block's parent hash, the panic comes in queueFetchedBlock, after the fetch.
type gap054ParentPanicChain struct {
	blockchain.ClientI
	parent chainhash.Hash
	calls  atomic.Int32
}

func (c *gap054ParentPanicChain) GetBlockExists(ctx context.Context, h *chainhash.Hash) (bool, error) {
	if *h == c.parent {
		c.calls.Add(1)
		panic("injected panic after the announced block was fetched")
	}

	return c.ClientI.GetBlockExists(ctx, h)
}

// TestGapBV054_BlockFoundChPanicCapStopsFetch panics on each blockFoundCh delivery of one block,
// after the announced block was fetched. After MaxCorruptAttemptsPerBlock panics, the same peer's
// next announcement must stop before the fetch, and its WaitToComplete caller must get one
// ERR_PROCESSING answer that does not requeue the block. Another peer keeps its own budget, an
// empty peerID is never capped, and no peer is charged or reported.
func TestGapBV054_BlockFoundChPanicCapStopsFetch(t *testing.T) {
	initPrometheusMetrics()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s, _, block := newProcessBlockFoundHarness(ctx, t)
	s.settings.BlockValidation.MaxCorruptAttemptsPerBlock = 2

	reputation := &subtreeAttributionP2PClient{}
	s.p2pClient = reputation

	reports := &gap054PeerReportCounter{ClientI: s.blockchainClient}
	s.blockchainClient = reports

	chain := &gap054ParentPanicChain{ClientI: reports, parent: *block.Header.HashPrevBlock}
	s.blockValidation = NewBlockValidation(ctx, ulogger.TestLogger{}, s.settings, chain, blobmemory.New(), s.txStore, s.utxoStore, nil, nil)

	blockBytes, err := block.Bytes()
	require.NoError(t, err)

	httpmock.ActivateNonDefault(util.HTTPClient())
	defer httpmock.DeactivateAndReset()

	fetchKey := "GET http://test-peer/block/" + block.Hash().String()
	httpmock.RegisterResponder("GET", "http://test-peer/block/"+block.Hash().String(), httpmock.NewBytesResponder(200, blockBytes))

	deliver := func(peerID string) error {
		errCh := make(chan error)
		done := make(chan struct{})

		go func() {
			s.processBlockFoundChItem(ctx, 0, processBlockFound{hash: block.Hash(), baseURL: "http://test-peer", peerID: peerID, errCh: errCh})
			close(done)
		}()

		var answer error

		select {
		case answer = <-errCh:
		case <-time.After(10 * time.Second):
			t.Fatalf("peer %q: the WaitToComplete caller got no answer", peerID)
		}

		gap054RequireAnsweredOnce(t, errCh, done)

		return answer
	}

	for i := 1; i <= 2; i++ {
		fetchesBefore := httpmock.GetCallCountInfo()[fetchKey]

		err := deliver("peerA")
		require.Error(t, err)
		require.Contains(t, err.Error(), "see the PANIC log line", "delivery %d must panic", i)
		require.Equal(t, fetchesBefore+1, httpmock.GetCallCountInfo()[fetchKey], "delivery %d must fetch the block", i)
		require.Equal(t, int32(i), chain.calls.Load())
	}

	require.True(t, s.panicAttemptsExhausted(block.Hash(), "peerA"), "the block_found recover must count toward the cap")

	fetchesBefore := httpmock.GetCallCountInfo()[fetchKey]

	err = deliver("peerA")
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.ErrProcessing), "a capped announcement must be ERR_PROCESSING, got %v", err)
	require.Contains(t, err.Error(), "recovered-panic cap reached")
	require.False(t, errors.IsBlockCorrupt(err))
	require.False(t, errors.IsNetworkError(err), "%v", err)
	require.False(t, errors.IsMaliciousResponseError(err), "%v", err)
	require.Equal(t, fetchesBefore, httpmock.GetCallCountInfo()[fetchKey], "a capped announcement must stop before the fetch")
	require.Equal(t, int32(2), chain.calls.Load(), "a capped announcement must stop before the stage that panics")

	err = deliver("peerB")
	require.Contains(t, err.Error(), "see the PANIC log line", "another peer must keep its own budget")
	require.Equal(t, fetchesBefore+1, httpmock.GetCallCountInfo()[fetchKey], "another peer's announcement must be fetched")
	require.Equal(t, int32(3), chain.calls.Load())

	for i := 1; i <= 3; i++ {
		err = deliver("")
		require.Contains(t, err.Error(), "see the PANIC log line", "an empty peerID must never be capped (delivery %d)", i)
	}

	require.Equal(t, fetchesBefore+4, httpmock.GetCallCountInfo()[fetchKey])

	require.Equal(t, 0, reputation.totalCharges(), "no peer may be charged for a panic in this node")
	require.Equal(t, 0, reputation.totalMaliciousReports())
	require.Equal(t, int32(0), reports.reports.Load(), "no peer failure may be reported for a panic in this node")
}

// TestGapBV054_ConcurrentPanicErrorIsDistinct checks that isLocalPanic matches the
// waiter error from runOncePerBlock, also when another error wraps it, and matches no other
// ERR_PROCESSING error. teranode's errors.Is matches on the code, so it cannot tell them apart.
// An error with the same text but no marker must not match either: the check is by type, not text.
func TestGapBV054_ConcurrentPanicErrorIsDistinct(t *testing.T) {
	hash := chainhash.Hash{1}

	waiterErr := newConcurrentValidationPanicError(&hash)

	require.True(t, errors.Is(waiterErr, errors.ErrProcessing), "the waiter error must still be ERR_PROCESSING")
	require.True(t, isLocalPanic(waiterErr))
	require.True(t, isLocalPanic(errors.NewServiceError("failed block validation", waiterErr)), "a wrapped waiter error must still match")
	require.True(t, isLocalCatchupFault(waiterErr), "the waiter error must be a local catchup fault")

	require.False(t, errors.IsNetworkError(waiterErr), "%v", waiterErr)
	require.False(t, errors.IsMaliciousResponseError(waiterErr), "%v", waiterErr)
	require.False(t, errors.IsBlockCorrupt(waiterErr), "%v", waiterErr)
	require.False(t, errors.IsContextError(waiterErr), "%v", waiterErr)

	ordinary := errors.NewProcessingError("terminal generic peer error")
	require.True(t, errors.Is(ordinary, waiterErr), "setup: teranode errors.Is matches any two ERR_PROCESSING errors")
	require.False(t, isLocalPanic(ordinary), "an ordinary ERR_PROCESSING error must not match")
	require.False(t, isLocalCatchupFault(ordinary), "an ordinary ERR_PROCESSING error must stay a peer error")

	sameText := errors.NewProcessingError("%s", waiterErr.Error())
	require.False(t, isLocalPanic(sameText), "an error with the same text but no marker must not match")

	require.False(t, isLocalPanic(nil))
}

// TestGapBV054_ConcurrentPanicWaiterIsLocalInReleaseCatchupLock ends a catchup with the waiter
// error from runOncePerBlock while the catchup itself did not panic. releaseCatchupLock must score
// it local_panic and charge no peer.
func TestGapBV054_ConcurrentPanicWaiterIsLocalInReleaseCatchupLock(t *testing.T) {
	server, _, _, cleanup := setupTestCatchupServer(t)
	defer cleanup()

	recorder := newPeerFailureRecordingP2PClient()
	server.p2pClient = recorder

	header := testhelpers.CreateTestHeaders(t, 1)[0]
	catchupCtx := &CatchupContext{
		blockUpTo: &model.Block{Header: header, Height: 1000},
		baseURL:   "http://honest-primary:8000",
		peerID:    "honest-primary",
		startTime: time.Now(),
	}
	require.NoError(t, server.acquireCatchupLock(catchupCtx))

	relErr := newConcurrentValidationPanicError(header.Hash())
	server.releaseCatchupLock(catchupCtx, &relErr)

	require.False(t, catchupCtx.panicked, "setup: the catchup itself did not panic")

	require.NotNil(t, server.previousCatchupAttempt)
	require.Equal(t, "local_panic", server.previousCatchupAttempt.ErrorType)

	recorder.mu.Lock()
	defer recorder.mu.Unlock()

	require.Equal(t, 0, recorder.failuresByPeer["honest-primary"], "the primary must not be charged for a panic in this node")
	require.Equal(t, 0, recorder.maliciousByPeer["honest-primary"])
	require.NotContains(t, recorder.errorMsgsByPeer, "honest-primary", "no catchup error may be stored against the primary")
}

// TestGapBV054_ConcurrentPanicWaiterIsNotReportedByCatchupConsumer makes catchup return the waiter
// error from runOncePerBlock. processCatchupChItem must treat it as local: no ReportPeerFailure,
// no walk of the alternatives, one counted attempt and cleared markers.
func TestGapBV054_ConcurrentPanicWaiterIsNotReportedByCatchupConsumer(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s, _, block := newProcessBlockFoundHarness(ctx, t)

	reports := &gap054PeerReportCounter{ClientI: s.blockchainClient}
	s.blockchainClient = reports

	var catchups atomic.Int32

	s.catchupFunc = func(context.Context, *model.Block, string, string) error {
		catchups.Add(1)
		return newConcurrentValidationPanicError(block.Hash())
	}

	s.processBlockNotify.Set(*block.Hash(), true, ttlcache.DefaultTTL)
	s.catchupAlternatives.Set(*block.Hash(), []processBlockCatchup{{block: block, baseURL: "http://alt-peer", peerID: "altPeer"}}, ttlcache.DefaultTTL)

	s.processCatchupChItem(ctx, processBlockCatchup{block: block, baseURL: "http://test-peer", peerID: "peerA"})

	require.Equal(t, int32(0), reports.reports.Load(), "no peer failure may be reported for a panic in this node")
	require.Equal(t, int32(1), catchups.Load(), "the alternatives must not be walked for a local fault")

	attempts := s.blockCatchupAttempts.Get(*block.Hash())
	require.NotNil(t, attempts)
	require.Equal(t, 1, attempts.Value())
	require.Nil(t, s.processBlockNotify.Get(*block.Hash()))
	require.Nil(t, s.catchupAlternatives.Get(*block.Hash()))
}

// TestGapBV054_ConcurrentPanicWaiterIsNotChargedToCachedAlternative fails the primary with an
// ordinary peer error, so processCatchupChItem walks the cached alternatives. The alternative's
// catchup returns the waiter error from runOncePerBlock. reportCatchupFailureForError must not
// charge the alternative for a panic in this node. The primary's charge proves the charging line
// runs, so a zero for the alternative is the exemption and not a dead path.
func TestGapBV054_ConcurrentPanicWaiterIsNotChargedToCachedAlternative(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s, _, block := newProcessBlockFoundHarness(ctx, t)

	reputation := &subtreeAttributionP2PClient{}
	s.p2pClient = reputation

	reports := &gap054PeerReportCounter{ClientI: s.blockchainClient}
	s.blockchainClient = reports

	var altCatchups atomic.Int32

	s.catchupFunc = func(_ context.Context, _ *model.Block, peerID, _ string) error {
		if peerID == "altPeer" {
			altCatchups.Add(1)
			return newConcurrentValidationPanicError(block.Hash())
		}

		return errors.NewProcessingError("terminal generic peer error")
	}

	s.processBlockNotify.Set(*block.Hash(), true, ttlcache.DefaultTTL)
	s.catchupAlternatives.Set(*block.Hash(), []processBlockCatchup{{block: block, baseURL: "http://alt-peer", peerID: "altPeer"}}, ttlcache.DefaultTTL)

	s.processCatchupChItem(ctx, processBlockCatchup{block: block, baseURL: "http://test-peer", peerID: "peerA"})

	require.Equal(t, int32(1), altCatchups.Load(), "setup: the cached alternative must be tried")
	require.Equal(t, 1, reputation.charged("peerA"), "setup: the primary's ordinary peer error must be charged")
	require.Equal(t, 0, reputation.charged("altPeer"), "the alternative must not be charged for a panic in this node")
	require.Equal(t, 0, reputation.totalMaliciousReports())
	require.Equal(t, int32(1), reports.reports.Load(), "only the primary's failure may be reported")
}

// gap054CatchupCalls counts catchupFunc calls per peer.
type gap054CatchupCalls struct {
	mu    sync.Mutex
	calls map[string]int
}

func (c *gap054CatchupCalls) add(peerID string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.calls == nil {
		c.calls = make(map[string]int)
	}

	c.calls[peerID]++
}

func (c *gap054CatchupCalls) get(peerID string) int {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.calls[peerID]
}

// gap054RequireConcurrentPanicEndState checks the state the primary path leaves after the waiter
// error: one counted attempt, both markers cleared, only the primary's earlier failure reported.
func gap054RequireConcurrentPanicEndState(t *testing.T, s *Server, block *model.Block, reports *gap054PeerReportCounter, wantReports int32) {
	t.Helper()

	attempts := s.blockCatchupAttempts.Get(*block.Hash())
	require.NotNil(t, attempts, "the cycle must count toward the catchup cap")
	require.Equal(t, 1, attempts.Value())
	require.Nil(t, s.processBlockNotify.Get(*block.Hash()), "the processing marker must be cleared")
	require.Nil(t, s.catchupAlternatives.Get(*block.Hash()), "the cached alternatives must be cleared")
	require.Equal(t, wantReports, reports.reports.Load(), "only the primary's ordinary failure may be reported")
}

// TestGapBV054_ConcurrentPanicStopsP2PAlternatives fails the primary with an ordinary peer error,
// so processCatchupChItem asks the P2P-selected peers. The first one's catchup returns the waiter
// error of a validation that panicked on another goroutine. That is a bug in this node, so no
// further P2P peer and no cached alternative may be tried, the peer is not charged, and the cycle
// ends as the primary path ends it for that error.
func TestGapBV054_ConcurrentPanicStopsP2PAlternatives(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s, _, block := newProcessBlockFoundHarness(ctx, t)

	alt1 := tipPeer("alt1", "http://alt1-peer", block.Hash())
	alt2 := tipPeer("alt2", "http://alt2-peer", block.Hash())
	alt1.Height = block.Height
	alt2.Height = block.Height

	reputation := &subtreeAttributionP2PClient{peers: []*p2p.PeerInfo{alt1, alt2}}
	s.p2pClient = reputation

	reports := &gap054PeerReportCounter{ClientI: s.blockchainClient}
	s.blockchainClient = reports

	alt1ID := peer.ID("alt1").String()
	calls := &gap054CatchupCalls{}

	s.catchupFunc = func(_ context.Context, _ *model.Block, peerID, _ string) error {
		calls.add(peerID)

		switch peerID {
		case "peerA":
			return errors.NewProcessingError("terminal generic peer error")
		case alt1ID:
			return newConcurrentValidationPanicError(block.Hash())
		default:
			return nil
		}
	}

	s.processBlockNotify.Set(*block.Hash(), true, ttlcache.DefaultTTL)
	s.catchupAlternatives.Set(*block.Hash(), []processBlockCatchup{{block: block, baseURL: "http://cached-alt", peerID: "cachedAlt"}}, ttlcache.DefaultTTL)

	s.processCatchupChItem(ctx, processBlockCatchup{block: block, baseURL: "http://test-peer", peerID: "peerA"})

	require.Equal(t, 1, calls.get("peerA"))
	require.Equal(t, 1, calls.get(alt1ID), "setup: the first P2P peer must be tried")
	require.Equal(t, 0, calls.get(peer.ID("alt2").String()), "no further P2P peer may be tried after the waiter error")
	require.Equal(t, 0, calls.get("cachedAlt"), "no cached alternative may be tried after the waiter error")

	require.Equal(t, 1, reputation.charged("peerA"), "setup: the primary's ordinary peer error is charged")
	require.Equal(t, 0, reputation.charged(alt1ID), "the P2P peer must not be charged for a panic in this node")
	require.Equal(t, 0, reputation.totalMaliciousReports())

	gap054RequireConcurrentPanicEndState(t, s, block, reports, 1)
}

// TestGapBV054_ConcurrentPanicStopsCachedAlternatives is the same check for the cached
// alternatives loop. No P2P peer is offered, the first cached alternative's catchup returns the
// waiter error, and the second must not be tried.
func TestGapBV054_ConcurrentPanicStopsCachedAlternatives(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s, _, block := newProcessBlockFoundHarness(ctx, t)

	reputation := &subtreeAttributionP2PClient{}
	s.p2pClient = reputation

	reports := &gap054PeerReportCounter{ClientI: s.blockchainClient}
	s.blockchainClient = reports

	calls := &gap054CatchupCalls{}

	s.catchupFunc = func(_ context.Context, _ *model.Block, peerID, _ string) error {
		calls.add(peerID)

		switch peerID {
		case "peerA":
			return errors.NewProcessingError("terminal generic peer error")
		case "cachedAlt1":
			return newConcurrentValidationPanicError(block.Hash())
		default:
			return nil
		}
	}

	s.processBlockNotify.Set(*block.Hash(), true, ttlcache.DefaultTTL)
	s.catchupAlternatives.Set(*block.Hash(), []processBlockCatchup{
		{block: block, baseURL: "http://cached-alt1", peerID: "cachedAlt1"},
		{block: block, baseURL: "http://cached-alt2", peerID: "cachedAlt2"},
	}, ttlcache.DefaultTTL)

	s.processCatchupChItem(ctx, processBlockCatchup{block: block, baseURL: "http://test-peer", peerID: "peerA"})

	require.Equal(t, 1, calls.get("peerA"))
	require.Equal(t, 1, calls.get("cachedAlt1"), "setup: the first cached alternative must be tried")
	require.Equal(t, 0, calls.get("cachedAlt2"), "no further cached alternative may be tried after the waiter error")

	require.Equal(t, 1, reputation.charged("peerA"), "setup: the primary's ordinary peer error is charged")
	require.Equal(t, 0, reputation.charged("cachedAlt1"), "the cached alternative must not be charged for a panic in this node")
	require.Equal(t, 0, reputation.totalMaliciousReports())

	gap054RequireConcurrentPanicEndState(t, s, block, reports, 1)
}

// TestGapBV054_ConcurrentPanicStopsP2PAlternativesOnDeclineSkip covers a caller of
// tryAlternativePeersForCatchup that skips the primary: the primary is at its local-policy decline
// cap for the hash. The first P2P peer returns the waiter error, so the second is not tried, no
// peer is charged or reported, and the cycle counts and clears as on the primary path.
func TestGapBV054_ConcurrentPanicStopsP2PAlternativesOnDeclineSkip(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s, _, block := newProcessBlockFoundHarness(ctx, t)
	s.settings.BlockValidation.MaxCorruptAttemptsPerBlock = 1
	s.recordPolicyDeclineAttempt(block.Hash(), "peerA")
	require.True(t, s.policyDeclineAttemptsExhausted(block.Hash(), "peerA"), "setup: the primary must be at its decline cap")

	alt1 := tipPeer("alt1", "http://alt1-peer", block.Hash())
	alt2 := tipPeer("alt2", "http://alt2-peer", block.Hash())
	alt1.Height = block.Height
	alt2.Height = block.Height

	reputation := &subtreeAttributionP2PClient{peers: []*p2p.PeerInfo{alt1, alt2}}
	s.p2pClient = reputation

	reports := &gap054PeerReportCounter{ClientI: s.blockchainClient}
	s.blockchainClient = reports

	alt1ID := peer.ID("alt1").String()
	calls := &gap054CatchupCalls{}

	s.catchupFunc = func(_ context.Context, _ *model.Block, peerID, _ string) error {
		calls.add(peerID)

		if peerID == alt1ID {
			return newConcurrentValidationPanicError(block.Hash())
		}

		return nil
	}

	s.processBlockNotify.Set(*block.Hash(), true, ttlcache.DefaultTTL)

	s.processCatchupChItem(ctx, processBlockCatchup{block: block, baseURL: "http://test-peer", peerID: "peerA"})

	require.Equal(t, 0, calls.get("peerA"), "setup: the primary is skipped")
	require.Equal(t, 1, calls.get(alt1ID), "setup: the first P2P peer must be tried")
	require.Equal(t, 0, calls.get(peer.ID("alt2").String()), "no further P2P peer may be tried after the waiter error")
	require.Equal(t, 0, reputation.totalCharges(), "no peer may be charged for a panic in this node")
	require.Equal(t, 0, reputation.totalMaliciousReports())

	gap054RequireConcurrentPanicEndState(t, s, block, reports, 0)
}

// TestGapBV054_BlockFoundChPanicCapAnswersStoredBlock: a peer at its recovered-panic cap for a hash
// re-announces it. If this node has stored the block, the caller must get nil, as for any other
// peer, and nothing is fetched. If the block is not stored, the caller gets the capped
// ERR_PROCESSING answer and nothing is fetched.
func TestGapBV054_BlockFoundChPanicCapAnswersStoredBlock(t *testing.T) {
	initPrometheusMetrics()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s, _, block := newProcessBlockFoundHarness(ctx, t)
	s.settings.BlockValidation.MaxCorruptAttemptsPerBlock = 2

	for i := 0; i < 2; i++ {
		s.recordPanicAttempt(block.Hash(), "peerA")
	}

	require.True(t, s.panicAttemptsExhausted(block.Hash(), "peerA"), "setup: peerA must be at its cap")

	httpmock.ActivateNonDefault(util.HTTPClient())
	defer httpmock.DeactivateAndReset()

	fetchKey := "GET http://test-peer/block/" + block.Hash().String()
	httpmock.RegisterResponder("GET", "http://test-peer/block/"+block.Hash().String(), httpmock.NewStringResponder(http.StatusInternalServerError, ""))

	stored := s.blockValidation
	s.blockValidation = NewBlockValidation(ctx, ulogger.TestLogger{}, s.settings, &gap054ExistsChain{ClientI: s.blockchainClient}, blobmemory.New(), s.txStore, s.utxoStore, nil, nil)

	require.NoError(t, gap054Deliver(ctx, t, s, block.Hash(), "http://test-peer", "peerA"), "a stored block must answer nil, also for a capped peer")
	require.Equal(t, 0, httpmock.GetCallCountInfo()[fetchKey], "a stored block must not be fetched")

	s.blockValidation = stored

	err := gap054Deliver(ctx, t, s, block.Hash(), "http://test-peer", "peerA")
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.ErrProcessing), "got %v", err)
	require.Contains(t, err.Error(), "recovered-panic cap reached")
	require.Equal(t, 0, httpmock.GetCallCountInfo()[fetchKey], "a capped announcement of a block that is not stored must stop before the fetch")
}

// gap054PeerListCounter counts the peer-registry reads of the 429 fallback, so a test can tell an
// alternative that was considered and skipped from one that was never considered.
type gap054PeerListCounter struct {
	*subtreeAttributionP2PClient
	lookups atomic.Int32
}

func (c *gap054PeerListCounter) GetPeersForCatchup(ctx context.Context) ([]*p2p.PeerInfo, error) {
	c.lookups.Add(1)
	return c.subtreeAttributionP2PClient.GetPeersForCatchup(ctx)
}

// gap054Deliver runs one blockFoundCh item for peerID on a worker, as a WaitToComplete caller
// would, and returns the one answer the caller gets.
func gap054Deliver(ctx context.Context, t *testing.T, s *Server, hash *chainhash.Hash, baseURL, peerID string) error {
	t.Helper()

	errCh := make(chan error)
	done := make(chan struct{})

	go func() {
		s.processBlockFoundChItem(ctx, 0, processBlockFound{hash: hash, baseURL: baseURL, peerID: peerID, errCh: errCh})
		close(done)
	}()

	var answer error

	select {
	case answer = <-errCh:
	case <-time.After(10 * time.Second):
		t.Fatalf("peer %q: the WaitToComplete caller got no answer", peerID)
	}

	gap054RequireAnsweredOnce(t, errCh, done)

	return answer
}

// TestGapBV054_BlockFoundChPanicAfter429FallbackCapsServingPeer: the announcer answers 429, so
// fetchAnnouncedBlock fetches the block from another peer that has it as its tip, and processing
// the block that peer served panics. The panic must count for the peer that served the
// bytes, not for the announcer. After MaxCorruptAttemptsPerBlock panics the serving peer must not be
// fetched from again on this path: the fallback considers it and skips it before the fetch. No
// peer is charged.
func TestGapBV054_BlockFoundChPanicAfter429FallbackCapsServingPeer(t *testing.T) {
	initPrometheusMetrics()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s, _, block := newProcessBlockFoundHarness(ctx, t)
	s.settings.BlockValidation.MaxCorruptAttemptsPerBlock = 2
	s.announcedBlockRateLimits = newAnnouncedBlockRateLimitCache()

	altID := peer.ID("alt").String()

	registry := &gap054PeerListCounter{subtreeAttributionP2PClient: &subtreeAttributionP2PClient{
		peers: []*p2p.PeerInfo{tipPeer("alt", "http://alt-peer", block.Hash())},
	}}
	s.p2pClient = registry

	httpmock.ActivateNonDefault(util.HTTPClient())
	defer httpmock.DeactivateAndReset()

	announcerKey := "GET http://announcer-peer/block/" + block.Hash().String()
	httpmock.RegisterResponder("GET", "http://announcer-peer/block/"+block.Hash().String(), httpmock.NewStringResponder(http.StatusTooManyRequests, ""))

	// The alternative serves the real block, and processing it panics after the fetch, in
	// queueFetchedBlock's parent check. A panic while the bytes are parsed cannot be used:
	// NewBlockFromReader recovers its own panics and returns an error.
	chain := &gap054ParentPanicChain{ClientI: s.blockchainClient, parent: *block.Header.HashPrevBlock}
	s.blockValidation = NewBlockValidation(ctx, ulogger.TestLogger{}, s.settings, chain, blobmemory.New(), s.txStore, s.utxoStore, nil, nil)

	blockBytes, err := block.Bytes()
	require.NoError(t, err)

	var altFetches atomic.Int32

	httpmock.RegisterResponder("GET", "http://alt-peer/block/"+block.Hash().String(), func(req *http.Request) (*http.Response, error) {
		altFetches.Add(1)

		return httpmock.NewBytesResponse(http.StatusOK, blockBytes), nil
	})

	// MaxCorruptAttemptsPerBlock is 2 and the announcer's 429 allowance is 3, so each of the three
	// 429s below is within the allowance and falls back.
	require.Greater(t, announcedBlockRateLimitAllowance, 2, "setup: the third 429 must still fall back")

	for i := 1; i <= 2; i++ {
		err := gap054Deliver(ctx, t, s, block.Hash(), "http://announcer-peer", "announcer")
		require.Error(t, err)
		require.Contains(t, err.Error(), "see the PANIC log line", "delivery %d must panic in the fallback fetch", i)
		require.Equal(t, int32(i), altFetches.Load(), "delivery %d must fetch from the alternative", i)
		require.Equal(t, i, httpmock.GetCallCountInfo()[announcerKey])
	}

	require.True(t, s.panicAttemptsExhausted(block.Hash(), altID), "the panics must fill the budget of the peer that served the bytes")
	require.False(t, s.panicAttemptsExhausted(block.Hash(), "announcer"), "the announcer served no bytes, so its budget must stay empty")
	require.Nil(t, s.blockPanicAttempts.Get(blockAttemptKey{hash: *block.Hash(), peerID: "announcer"}), "no panic may count for the announcer")

	lookupsBefore := registry.lookups.Load()

	err = gap054Deliver(ctx, t, s, block.Hash(), "http://announcer-peer", "announcer")
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.ErrServiceRateLimited), "with the alternative skipped the caller gets the announcer's 429, got %v", err)
	require.Equal(t, 3, httpmock.GetCallCountInfo()[announcerKey], "the announcer is not capped, so it is still asked")
	require.Equal(t, lookupsBefore+1, registry.lookups.Load(), "setup: the third delivery must reach the fallback")
	require.Equal(t, int32(2), altFetches.Load(), "a capped alternative must be skipped before the fetch")

	require.Equal(t, 0, registry.totalCharges(), "no peer may be charged for a panic in this node or for a 429 within the allowance")
	require.Equal(t, 0, registry.totalMaliciousReports())
}
