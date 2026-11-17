package blockassembly

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/blockassembly/subtreeprocessor"
	"github.com/bsv-blockchain/teranode/stores/blob"
	"github.com/bsv-blockchain/teranode/stores/blob/options"
	"github.com/bsv-blockchain/teranode/util/testutil"
	"github.com/stretchr/testify/require"
)

// dahSpyStore wraps a real blob.Store and records the Delete-At-Height (DAH)
// passed to Set for each file type. Interface embedding promotes every blob.Store
// method; only Set is overridden so the DAH the caller requested is observable.
type dahSpyStore struct {
	blob.Store

	mu      sync.Mutex
	dahByFT map[fileformat.FileType]uint32 // last DAH seen per file type
	setFTs  map[fileformat.FileType]bool   // which file types Set was called for
}

func newDAHSpyStore(inner blob.Store) *dahSpyStore {
	return &dahSpyStore{
		Store:   inner,
		dahByFT: make(map[fileformat.FileType]uint32),
		setFTs:  make(map[fileformat.FileType]bool),
	}
}

func (s *dahSpyStore) Set(ctx context.Context, key []byte, fileType fileformat.FileType, value []byte, opts ...options.FileOption) error {
	o := options.NewFileOptions(opts...)

	s.mu.Lock()
	s.dahByFT[fileType] = o.DAH
	s.setFTs[fileType] = true
	s.mu.Unlock()

	return s.Store.Set(ctx, key, fileType, value, opts...)
}

func (s *dahSpyStore) dahFor(fileType fileformat.FileType) (uint32, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.dahByFT[fileType], s.setFTs[fileType]
}

// TestStoredSubtreeCarriesFiniteDAH pins BA-SUBTREE-002: subtrees persisted by
// Block Assembly while in assembly state MUST carry a finite Delete-At-Height
// TTL, and Block Assembly MUST NOT promote them to permanent storage (DAH=0).
//
// The test wraps the subtree store in a spy that captures the DAH passed to Set,
// stores a subtree through the real storeSubtreeData path, and asserts the DAH is
// the finite value (currentHeight + GlobalBlockHeightRetention) and, crucially,
// never 0.
func TestStoredSubtreeCarriesFiniteDAH(t *testing.T) {
	initPrometheusMetrics()

	const blockHeight = uint32(123)

	common := testutil.NewCommonTestSetup(t)
	require.NotZero(t, common.Settings.GlobalBlockHeightRetention,
		"precondition: a non-zero retention is required for a finite, non-permanent DAH")

	spyStore := newDAHSpyStore(testutil.NewMemoryBlobStore())

	ctx, cancel := context.WithCancel(common.Ctx)

	blockchainClient := testutil.NewMemorySQLiteBlockchainClient(common.Logger, common.Settings, t)
	utxoStore := testutil.NewSQLiteMemoryUTXOStore(ctx, common.Logger, common.Settings, t)
	require.NoError(t, utxoStore.SetBlockHeight(blockHeight))

	server := New(common.Logger, common.Settings, nil, utxoStore, spyStore, blockchainClient)
	server.SetSkipWaitForPendingBlocks(true)
	require.NoError(t, server.Init(ctx))

	t.Cleanup(func() {
		cancel()
		_ = server.Stop(context.Background())
		if server.blockAssembler != nil {
			server.blockAssembler.Wait()
		}
	})

	// Build a complete subtree plus the parent-tx map its meta needs.
	subtree, err := subtreepkg.NewTreeByLeafCount(16)
	require.NoError(t, err)

	txMap := subtreeprocessor.NewSplitTxInpointsMap(256)
	previousHash := chainhash.HashH([]byte("dah-test-previous"))

	for i := range uint64(16) {
		txHash := chainhash.HashH(fmt.Appendf(nil, "dah-test-tx-%d", i))
		require.NoError(t, subtree.AddNode(txHash, i, i))

		in0 := &bt.Input{PreviousTxOutIndex: 0}
		require.NoError(t, in0.PreviousTxIDAdd(&previousHash))

		ti, err := subtreepkg.NewTxInpointsFromInputs([]*bt.Input{in0})
		require.NoError(t, err)

		txMap.Set(txHash, &ti)
		previousHash = txHash
	}

	subtreeRetryChan := make(chan *subtreeRetrySend, 1000)

	subtreeDone, allDone, err := server.storeSubtreeData(ctx, subtreeprocessor.NewSubtreeRequest{
		Subtree:     subtree,
		ParentTxMap: txMap,
	}, subtreeRetryChan)
	require.NoError(t, err)

	select {
	case ok := <-subtreeDone:
		require.True(t, ok, "subtree storage must succeed")
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for subtree storage")
	}

	select {
	case <-allDone:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for subtree + meta storage to complete")
	}

	expectedDAH := blockHeight + common.Settings.GlobalBlockHeightRetention

	subtreeDAH, subtreeSet := spyStore.dahFor(fileformat.FileTypeSubtree)
	require.True(t, subtreeSet, "the subtree data must have been stored")
	require.NotZero(t, subtreeDAH,
		"BA-SUBTREE-002: a subtree persisted during assembly must carry a finite, non-zero DAH "+
			"(DAH=0 means permanent storage, which is the Block Persister's responsibility, not Block Assembly's)")
	require.Equal(t, expectedDAH, subtreeDAH,
		"the subtree DAH must be currentHeight + GlobalBlockHeightRetention")

	metaDAH, metaSet := spyStore.dahFor(fileformat.FileTypeSubtreeMeta)
	require.True(t, metaSet, "the subtree meta must have been stored")
	require.NotZero(t, metaDAH, "BA-SUBTREE-002: the subtree meta must also carry a finite, non-zero DAH")
	require.Equal(t, expectedDAH, metaDAH, "the subtree meta DAH must match the subtree DAH")
}
