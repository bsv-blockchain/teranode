package subtreevalidation

import (
	"context"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-chaincfg"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/txmetacache"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/kafka"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// The cache-probe worker limit is shared by every subtree being validated, not
// applied per subtree: on a collapsing node 192 subtrees each ran their own 32
// probe workers, ~6,000 goroutines contending with the Kafka txmeta handler for
// the cache's bucket locks and the CPU.
func TestProcessTxMetaUsingCache_ProbeWorkersBoundedAcrossSubtrees(t *testing.T) {
	const (
		limit    = 2
		subtrees = 16
	)

	cache := setupTestCache(t)
	server := setupCacheTestServer(t, cache, limit, 0)

	// Enough transactions per subtree to fill several probe tasks.
	hashes := generateTestHashes(8 * probeTaskMinKeys)

	start := make(chan struct{})

	var wg sync.WaitGroup

	for s := 0; s < subtrees; s++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			<-start

			for round := 0; round < 5; round++ {
				metaSlice := make([]metaSliceItem, len(hashes))
				_, _ = server.processTxMetaUsingCache(context.Background(), hashes, metaSlice, false)
			}
		}()
	}

	close(start)
	wg.Wait()

	peak := server.peerProbePool.peak.Load()
	require.Positive(t, peak)
	require.LessOrEqual(t, int(peak), limit, "per-subtree limits would allow %d workers", subtrees*limit)
}

// Block validation probes from its own pool: a burst of peer subtrees holding
// every peer slot must not hold up a block's cache pre-check.
func TestProcessBlockTxMetaUsingCache_NotBlockedByPeerBurst(t *testing.T) {
	cache := setupTestCache(t)
	server := setupCacheTestServer(t, cache, 2, 0)

	// Occupy every peer slot, as a burst of peer subtrees would.
	peer := server.peerProbePool.slotsFor(server)
	for i := 0; i < cap(peer); i++ {
		peer <- struct{}{}
	}

	hashes := generateTestHashes(3 * probeTaskMinKeys)
	done := make(chan error, 1)

	go func() {
		_, err := server.processBlockTxMetaUsingCache(context.Background(), hashes, make([]metaSliceItem, len(hashes)))
		done <- err
	}()

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("block cache probe waited on the peer subtree pool")
	}
}

// A block's subtrees that this node has not validated yet go through
// ValidateSubtreeInternal (CheckBlockSubtrees, CheckSubtreeFromBlock). Their cache
// probe must use the block pool too, not queue behind a burst of peer subtrees.
func TestValidateSubtreeInternal_FromBlockNotBlockedByPeerBurst(t *testing.T) {
	InitPrometheusMetrics()

	utxoStore, validatorClient, txStore, subtreeStore, blockchainClient, deferFunc := setup(t)
	defer deferFunc()

	for _, tx := range []*bt.Tx{tx1, tx2, tx3, tx4} {
		_, _, err := utxoStore.SpendAndCreate(context.Background(), tx, 0, utxo.WithCreateOnly())
		require.NoError(t, err)
	}

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.SubtreeValidation.ProcessTxMetaUsingCacheConcurrency = 2
	tSettings.SubtreeValidation.TxMetaCacheEnabled = true // New puts the txmeta cache in front of the store

	nilConsumer := &kafka.KafkaConsumerGroup{}

	server, err := New(context.Background(), ulogger.TestLogger{}, tSettings, subtreeStore, txStore, utxoStore, validatorClient, blockchainClient, nilConsumer, nilConsumer, nil, nil)
	require.NoError(t, err)
	require.IsType(t, &txmetacache.TxMetaCache{}, server.utxoStore)

	// Occupy every peer slot, as a burst of peer subtrees would.
	peer := server.peerProbePool.slotsFor(server)
	for i := 0; i < cap(peer); i++ {
		peer <- struct{}{}
	}

	st, err := subtreepkg.NewTreeByLeafCount(4)
	require.NoError(t, err)

	for _, h := range []*chainhash.Hash{hash1, hash2, hash3, hash4} {
		require.NoError(t, st.AddNode(*h, 1, 0))
	}

	validate := func(fromBlock bool, timeout time.Duration) error {
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()

		v := ValidateSubtree{
			SubtreeHash: *st.RootHash(),
			BaseURL:     "http://localhost:8000",
			TxHashes:    []chainhash.Hash{*hash1, *hash2, *hash3, *hash4},
			FromBlock:   fromBlock,
		}

		_, err := server.ValidateSubtreeInternal(ctx, v, chaincfg.GenesisActivationHeight, nil)

		return err
	}

	// The peer path waits on the full peer pool, so this test does reach the pool.
	require.ErrorIs(t, validate(false, 200*time.Millisecond), errors.ErrContextCanceled)

	require.NoError(t, validate(true, 5*time.Second))
}

// A non-positive setting falls back to NumCPU workers, as the per-call errgroup
// limit (util.SafeSetLimit) did, instead of silently meaning one worker.
func TestProcessTxMetaUsingCache_NonPositiveConcurrencyMeansNumCPU(t *testing.T) {
	for _, setting := range []int{0, -1} {
		server := setupCacheTestServer(t, setupTestCache(t), setting, 0)
		require.Equal(t, runtime.NumCPU(), cap(server.peerProbePool.slotsFor(server)))
	}
}

// Probe tasks are whole bucket ranges, but adjacent small ranges are merged so a
// small subtree is one task instead of one goroutine per range.
func TestProbeTasks_MergesSmallRanges(t *testing.T) {
	small := make([]int, 256)
	for r := range small {
		small[r] = 4 // a 1,024-tx subtree
	}

	require.Equal(t, [][2]int{{0, 256}}, probeTasks(small, probeTaskMinKeys))

	large := make([]int, 4)
	for r := range large {
		large[r] = probeTaskMinKeys // already big enough on their own
	}

	require.Equal(t, [][2]int{{0, 1}, {1, 2}, {2, 3}, {3, 4}}, probeTasks(large, probeTaskMinKeys))

	mixed := []int{0, 1000, 0, 1500, probeTaskMinKeys, 0, 10}
	require.Equal(t, [][2]int{{0, 4}, {4, 5}, {5, 7}}, probeTasks(mixed, probeTaskMinKeys))
}
