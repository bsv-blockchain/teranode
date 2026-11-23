package blockassembly

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockassembly/subtreeprocessor"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

// A reset clears the tx map that the store requests of the state it retires were built from. A meta
// build of such a request that finds no parent is the reset's own side effect: it is cancelled, not
// failed (issue 2269, PR 4a).

// supersedeHarness is a real subtree processor whose subtree requests the test takes off the channel
// and feeds to a store pipeline itself.
type supersedeHarness struct {
	stp      *subtreeprocessor.SubtreeProcessor
	requests chan subtreeprocessor.NewSubtreeRequest
	seq      int
}

func newSupersedeHarness(t *testing.T) *supersedeHarness {
	t.Helper()

	initPrometheusMetrics()

	items := setupBlockAssemblyTest(t)
	items.blockAssembler.subtreeProcessor.Stop(t.Context())

	stp, err := subtreeprocessor.NewSubtreeProcessor(t.Context(), ulogger.TestLogger{}, items.blockAssembler.settings, items.blobStore, items.blockchainClient, items.utxoStore, items.newSubtreeChan)
	require.NoError(t, err)

	stp.Start(t.Context())

	return &supersedeHarness{stp: stp, requests: items.newSubtreeChan}
}

// complete queues the transactions of one complete subtree, so the processor goroutine places them, and returns its request.
func (h *supersedeHarness) complete(t *testing.T) subtreeprocessor.NewSubtreeRequest {
	t.Helper()

	parent := chainhash.HashH([]byte("parent"))
	nodes := make([]subtreepkg.Node, 0, 4)
	inpoints := make([]*subtreepkg.TxInpoints, 0, 4)

	for range 4 {
		h.seq++

		nodes = append(nodes, subtreepkg.Node{Hash: chainhash.HashH([]byte(fmt.Sprintf("superseded-tx-%d", h.seq))), Fee: 1, SizeInBytes: 100})
		inpoints = append(inpoints, &subtreepkg.TxInpoints{ParentTxHashes: []chainhash.Hash{parent}})
	}

	h.stp.AddBatch(nodes, inpoints)

	select {
	case req := <-h.requests:
		return req
	case <-time.After(10 * time.Second):
		t.Fatal("the processor sent no subtree")

		return subtreeprocessor.NewSubtreeRequest{}
	}
}

func (h *supersedeHarness) reset(t *testing.T) {
	t.Helper()

	response := h.stp.Reset(&model.BlockHeader{
		Version:        1,
		HashPrevBlock:  &chainhash.Hash{},
		HashMerkleRoot: &chainhash.Hash{},
		Nonce:          2269,
	}, nil, nil, false, func() error { return nil })
	require.NoError(t, response.Err)
}

// pausedParents is a ParentTxMap whose first read signals that the build is under way and then waits.
type pausedParents struct {
	subtreeprocessor.TxInpointsMap

	once    sync.Once
	entered chan struct{}
	release chan struct{}
}

func (m *pausedParents) Get(hash chainhash.Hash) (*subtreepkg.TxInpoints, bool) {
	m.once.Do(func() { close(m.entered) })
	<-m.release

	return m.TxInpointsMap.Get(hash)
}

func TestSubtreeStoreSuperseded_ResetWhileTheMetaBuildIsPaused_ResetDoesNotWaitAndTheBuildIsCancelled(t *testing.T) {
	server, _, _, _ := setup(t)
	h := newSupersedeHarness(t)
	p := startStorePipeline(t, server)

	req := h.complete(t)
	paused := &pausedParents{TxInpointsMap: req.ParentTxMap, entered: make(chan struct{}), release: make(chan struct{})}
	result := make(chan subtreeprocessor.StoreResult, 1)
	req.ParentTxMap = paused
	req.Result = result
	p.in <- req

	select {
	case <-paused.entered:
	case <-time.After(10 * time.Second):
		t.Fatal("the meta build never started")
	}

	failedBefore := testutil.ToFloat64(prometheusBlockAssemblySubtreeStoreFailed)

	// the build is blocked inside the map: a reset that waited for it would not return
	resetDone := make(chan struct{})

	go func() {
		defer close(resetDone)
		h.reset(t)
	}()

	select {
	case <-resetDone:
	case <-time.After(10 * time.Second):
		t.Fatal("reset waited for the paused store request")
	}

	close(paused.release)

	got := awaitResult(t, result)

	require.True(t, got.Cancelled, "%+v", got)
	require.False(t, got.OK())
	require.Equal(t, failedBefore, testutil.ToFloat64(prometheusBlockAssemblySubtreeStoreFailed), "a superseded build is not a store failure")
}

func TestSubtreeStoreSuperseded_RequestMadeAfterTheReset_StillFailsWhenItsParentIsMissing(t *testing.T) {
	server, _, _, _ := setup(t)
	h := newSupersedeHarness(t)
	p := startStorePipeline(t, server)

	h.reset(t)

	req := h.complete(t)
	parents := &noParents{TxInpointsMap: req.ParentTxMap}
	result := make(chan subtreeprocessor.StoreResult, 1)
	req.ParentTxMap = parents
	req.Result = result

	failedBefore := testutil.ToFloat64(prometheusBlockAssemblySubtreeStoreFailed)

	p.in <- req

	got := awaitResult(t, result)

	require.False(t, got.Cancelled, "%+v", got)
	require.Error(t, got.Err)
	require.Equal(t, failedBefore+1, testutil.ToFloat64(prometheusBlockAssemblySubtreeStoreFailed))
}

func TestSubtreeStoreSuperseded_ReadErrorAfterTheReset_IsNotRetried(t *testing.T) {
	server, _, _, _ := setup(t)
	h := newSupersedeHarness(t)
	p := startStorePipeline(t, server)

	req := h.complete(t)
	h.reset(t)

	reads := &countingFlakyReads{flakyReads: flakyReads{TxInpointsMap: req.ParentTxMap, fails: 1 << 30}}
	result := make(chan subtreeprocessor.StoreResult, 1)
	req.ParentTxMap = reads
	req.Result = result
	p.in <- req

	got := awaitResult(t, result)

	require.True(t, got.Cancelled, "%+v", got)
	require.Equal(t, 1, reads.count(), "a superseded build does not wait and read again")
}

// countingFlakyReads is flakyReads that counts its reads.
type countingFlakyReads struct {
	flakyReads

	cmu   sync.Mutex
	reads int
}

func (m *countingFlakyReads) GetWithErr(hash chainhash.Hash) (*subtreepkg.TxInpoints, bool, error) {
	m.cmu.Lock()
	m.reads++
	m.cmu.Unlock()

	return m.flakyReads.GetWithErr(hash)
}

func (m *countingFlakyReads) count() int {
	m.cmu.Lock()
	defer m.cmu.Unlock()

	return m.reads
}
