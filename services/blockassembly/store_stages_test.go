package blockassembly

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/blockassembly/subtreeprocessor"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/util/testutil/blobfault"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// Issue 2269, plan PR W: with blockassembly_storedPrefixTemplate on, the meta builds of the store
// workers run up to a CPU-sized width and the blob writes stay within a writer bound of their own;
// with it off the workers do both, as before.

// gatedInpoints is a parent tx map that counts the meta builds in flight and holds each build, at its
// first read, until the gate is opened. A build is in flight from its first read to its last.
type gatedInpoints struct {
	subtreeprocessor.TxInpointsMap
	gate  <-chan struct{}
	inc   *atomic.Int32
	max   *atomic.Int32
	reads atomic.Int32
	size  int32
}

func (g *gatedInpoints) Get(hash chainhash.Hash) (*subtreepkg.TxInpoints, bool) {
	n := g.reads.Add(1)
	if n == 1 {
		cur := g.inc.Add(1)
		for {
			m := g.max.Load()
			if cur <= m || g.max.CompareAndSwap(m, cur) {
				break
			}
		}

		<-g.gate
	}

	if n == g.size {
		g.inc.Add(-1)
	}

	return g.TxInpointsMap.Get(hash)
}

func startStagedListener(t *testing.T, flag bool, metaWorkers, writers int) *BlockAssembly {
	t.Helper()

	server, _ := setupServerWithSettings(t, func(s *settings.Settings) {
		s.BlockAssembly.StoredPrefixTemplate = flag
		s.BlockAssembly.StoreQueueMaxMB = 64
		s.BlockAssembly.SubtreeStorageWorkers = 4
		s.BlockAssembly.SubtreeStorageMetaWorkers = metaWorkers
		s.BlockAssembly.SubtreeStorageWriters = writers
	})

	if flag {
		require.NotNil(t, server.storeQueue)
	}

	return server
}

// announceTo is a blockchain client that sends the hash of every subtree notification to ch.
func announceTo(ch chan<- chainhash.Hash) *blockchain.Mock {
	client := &blockchain.Mock{}
	client.On("IsFSMCurrentState", mock.Anything, mock.Anything).Return(true, nil)
	client.On("SendNotification", mock.Anything, mock.Anything).
		Run(func(args mock.Arguments) {
			n, _ := args.Get(1).(*blockchain.Notification)
			ch <- chainhash.Hash(n.Hash)
		}).
		Return(nil)

	return client
}

func closeOnce(ch chan struct{}) func() {
	var once sync.Once

	return func() { once.Do(func() { close(ch) }) }
}

func TestStoreStages_MetaBuildsRunUpToTheMetaWidthWhileWritesStayWithinTheWriterBound(t *testing.T) {
	const (
		metaWidth = 6 // more than the 4 workers of the old path
		writers   = 3
		requests  = 24
	)

	server := startStagedListener(t, true, metaWidth, writers)
	fault := blobfault.New(server.subtreeStore)
	writeGate := make(chan struct{})
	entered := make(chan struct{}, 4*requests)
	fault.OnSetAny(fileformat.FileTypeSubtree, blobfault.Rule{Block: writeGate, Entered: entered})
	fault.OnSetAny(fileformat.FileTypeSubtreeMeta, blobfault.Rule{Block: writeGate, Entered: entered})
	server.subtreeStore = fault

	buildGate := make(chan struct{})
	openBuilds, openWrites := closeOnce(buildGate), closeOnce(writeGate)
	t.Cleanup(openBuilds)
	t.Cleanup(openWrites)

	var inFlight, maxInFlight atomic.Int32

	results := make([]chan subtreeprocessor.StoreResult, 0, requests)

	for seed := 1; seed <= requests; seed++ {
		subtree, txMap := makeSubtree(t, seed)
		result := make(chan subtreeprocessor.StoreResult, 1)
		results = append(results, result)

		server.storeQueue.Push(subtreeprocessor.NewSubtreeRequest{
			Subtree:          subtree,
			ParentTxMap:      &gatedInpoints{TxInpointsMap: txMap, gate: buildGate, inc: &inFlight, max: &maxInFlight, size: 16},
			SkipNotification: true,
			ErrChan:          make(chan error, 1),
			Result:           result,
		})
	}

	require.Eventually(t, func() bool { return inFlight.Load() == metaWidth }, 5*time.Second, 5*time.Millisecond,
		"the builds fill the meta stage")
	time.Sleep(100 * time.Millisecond)
	require.EqualValues(t, metaWidth, maxInFlight.Load(), "no more builds than the meta width run at once")

	openBuilds()

	require.Eventually(t, func() bool { return len(entered) >= writers }, 5*time.Second, 5*time.Millisecond,
		"the writes fill the writer stage")
	time.Sleep(100 * time.Millisecond)
	require.Equal(t, writers, len(entered), "no more writes than the writer bound are in the store at once")
	require.Positive(t, server.storeQueue.Held(), "the bytes between the stages stay charged until written")

	openWrites()

	for _, r := range results {
		require.True(t, awaitResult(t, r).OK())
	}

	require.Eventually(t, func() bool { return server.storeQueue.Held() == 0 }, 5*time.Second, 5*time.Millisecond,
		"the queue's byte accounting balances to zero after the burst")
}

func TestStoreStages_ABurstIsAnnouncedInOrderAndLeavesNoBytesHeld(t *testing.T) {
	const requests = 40

	server := startStagedListener(t, true, 8, 2)

	announced := make(chan chainhash.Hash, requests)
	server.blockchainClient = announceTo(announced)

	var maxHeld atomic.Uint64

	stop := make(chan struct{})
	done := make(chan struct{})

	go func() {
		defer close(done)

		for {
			select {
			case <-stop:
				return
			default:
				if h := server.storeQueue.Held(); h > maxHeld.Load() {
					maxHeld.Store(h)
				}
			}
		}
	}()

	var order []chainhash.Hash

	results := make([]chan subtreeprocessor.StoreResult, 0, requests)

	for seed := 1; seed <= requests; seed++ {
		subtree, txMap := makeSubtree(t, seed)
		result := make(chan subtreeprocessor.StoreResult, 1)
		results = append(results, result)
		order = append(order, *subtree.RootHash())

		server.storeQueue.Push(subtreeprocessor.NewSubtreeRequest{Subtree: subtree, ParentTxMap: txMap, ErrChan: make(chan error, 1), Result: result})
	}

	for _, r := range results {
		require.True(t, awaitResult(t, r).OK())
	}

	for _, want := range order {
		select {
		case got := <-announced:
			require.Equal(t, want, got, "announcements keep the order of the requests")
		case <-time.After(5 * time.Second):
			t.Fatal("a stored subtree was not announced")
		}
	}

	close(stop)
	<-done

	require.Positive(t, maxHeld.Load())
	require.Eventually(t, func() bool { return server.storeQueue.Held() == 0 }, 5*time.Second, 5*time.Millisecond)
}

func TestStoreStages_WithTheFlagOffTheWorkersKeepTheirOwnWidth(t *testing.T) {
	server := startStagedListener(t, false, 1, 1)
	require.Nil(t, server.storeQueue)

	fault := blobfault.New(server.subtreeStore)
	writeGate := make(chan struct{})
	entered := make(chan struct{}, 64)
	fault.OnSetAny(fileformat.FileTypeSubtree, blobfault.Rule{Block: writeGate, Entered: entered})
	server.subtreeStore = fault

	buildGate := make(chan struct{})
	openBuilds, openWrites := closeOnce(buildGate), closeOnce(writeGate)

	p := startStorePipeline(t, server)

	// registered after the pipeline's, so it runs first: the pipeline's cleanup waits for its workers
	t.Cleanup(openBuilds)
	t.Cleanup(openWrites)

	var inFlight, maxInFlight atomic.Int32

	for seed := 1; seed <= 4; seed++ {
		subtree, txMap := makeSubtree(t, seed)
		p.in <- subtreeprocessor.NewSubtreeRequest{
			Subtree:          subtree,
			ParentTxMap:      &gatedInpoints{TxInpointsMap: txMap, gate: buildGate, inc: &inFlight, max: &maxInFlight, size: 16},
			SkipNotification: true,
			ErrChan:          make(chan error, 1),
		}
	}

	// the 4 workers each build at once and each has a write in flight, whatever the stage settings
	require.Eventually(t, func() bool { return inFlight.Load() == 4 }, 5*time.Second, 5*time.Millisecond)
	require.Eventually(t, func() bool { return len(entered) == 4 }, 5*time.Second, 5*time.Millisecond)
}
