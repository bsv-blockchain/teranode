package subtreeprocessor

import (
	"context"
	"net/url"
	"sync"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	txmap "github.com/bsv-blockchain/go-tx-map"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	blob_memory "github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/sql"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// markRecordingStore records the hashes passed to MarkTransactionsOnLongestChain
// (without touching the wrapped store) so a test can see exactly which txs a
// reorg marked.
type markRecordingStore struct {
	utxo.Store

	mu       sync.Mutex
	markedOn []chainhash.Hash
}

func (s *markRecordingStore) MarkTransactionsOnLongestChain(_ context.Context, hashes []chainhash.Hash, onLongestChain bool) error {
	if onLongestChain {
		s.mu.Lock()
		s.markedOn = append(s.markedOn, hashes...)
		s.mu.Unlock()
	}

	return nil
}

// reorgBlocks collects the winning txs of every moveForward block by iterating
// the block's tx set. Every tx in the block must be marked on the longest chain:
// a callback that returns true stops the iteration, and only the first tx is
// marked.
func TestReorgBlocks_MarksEveryWinningTxOnLongestChain(t *testing.T) {
	const txCount = 8000

	ctx := context.Background()
	logger := ulogger.NewErrorTestLogger(t)

	utxoStoreURL, err := url.Parse("sqlitememory:///test")
	require.NoError(t, err)

	sqlStore, err := sql.New(ctx, logger, test.CreateBaseTestSettings(t), utxoStoreURL)
	require.NoError(t, err)

	utxoStore := &markRecordingStore{Store: sqlStore}
	blobStore := blob_memory.New()

	st, err := subtreepkg.NewTreeByLeafCount(16384)
	require.NoError(t, err)
	require.NoError(t, st.AddCoinbaseNode())

	want := make(map[chainhash.Hash]struct{}, txCount)

	for i := 0; i < txCount; i++ {
		h := chainhash.HashH([]byte{byte(i), byte(i >> 8), 0xaa, 0x55})
		want[h] = struct{}{}

		require.NoError(t, st.AddSubtreeNode(subtreepkg.Node{Hash: h, Fee: 1, SizeInBytes: 250}))
	}

	require.Len(t, want, txCount)

	key := st.RootHash()

	stBytes, err := st.Serialize()
	require.NoError(t, err)
	require.NoError(t, blobStore.Set(ctx, key[:], fileformat.FileTypeSubtree, stBytes))

	meta := subtreepkg.NewSubtreeMeta(st)
	for i := range st.Nodes {
		require.NoError(t, meta.SetTxInpoints(i, subtreepkg.NewTxInpoints()))
	}

	metaBytes, err := meta.Serialize()
	require.NoError(t, err)
	require.NoError(t, blobStore.Set(ctx, key[:], fileformat.FileTypeSubtreeMeta, metaBytes))

	// A small moved-back block with txs disjoint from the winning block, so
	// reorgBlocks takes the full path (not the catchup shortcut, which never
	// collects winning txs).
	backTx := chainhash.HashH([]byte("reorg-collect-back-tx"))
	backKey := storeReorgSubtree(t, ctx, blobStore, []subtreepkg.Node{
		{Hash: backTx, Fee: 100, SizeInBytes: 250},
	})

	startHeader := &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  &chainhash.Hash{},
		HashMerkleRoot: &chainhash.Hash{},
		Timestamp:      2000000001,
		Nonce:          9101,
	}
	backHeader := &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  startHeader.Hash(),
		HashMerkleRoot: &chainhash.Hash{},
		Timestamp:      2000000002,
		Nonce:          9102,
	}
	forwardHeader := &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  startHeader.Hash(),
		HashMerkleRoot: &chainhash.Hash{},
		Timestamp:      2000000003,
		Nonce:          9103,
	}

	blockchainClient := &blockchain.Mock{}
	blockchainClient.On("GetBlocksMinedNotSet", mock.Anything).Return([]*model.Block{}, nil)
	blockchainClient.On("SetBlockProcessedAt", mock.Anything, mock.Anything, mock.Anything).Return(nil)
	blockchainClient.On("GetBlockHeader", mock.Anything, mock.Anything).Return(startHeader, &model.BlockHeaderMeta{}, nil)
	blockchainClient.On("GetBlockIsMined", mock.Anything, mock.Anything).Return(true, nil)

	newSubtreeChan := make(chan NewSubtreeRequest, 16)
	go func() {
		for req := range newSubtreeChan {
			if req.ErrChan != nil {
				req.ErrChan <- nil
			}
		}
	}()
	t.Cleanup(func() { close(newSubtreeChan) })

	stp, err := NewSubtreeProcessor(ctx, logger, test.CreateBaseTestSettings(t), blobStore, blockchainClient, utxoStore, newSubtreeChan)
	require.NoError(t, err)

	stp.InitCurrentBlockHeader(backHeader)
	stp.Start(ctx)
	t.Cleanup(func() { stp.Stop(context.Background()) })

	_, err = sqlStore.Create(ctx, coinbaseTx2, 2)
	require.NoError(t, err)
	_, err = sqlStore.Create(ctx, coinbaseTx3, 2)
	require.NoError(t, err)

	blockBack := &model.Block{Height: 2, CoinbaseTx: coinbaseTx2, Subtrees: []*chainhash.Hash{backKey}, Header: backHeader}
	blockForward := &model.Block{
		Height:           2,
		CoinbaseTx:       coinbaseTx3,
		Subtrees:         []*chainhash.Hash{key},
		Header:           forwardHeader,
		TransactionCount: txCount + 1,
	}

	require.NoError(t, stp.Reorg([]*model.Block{blockBack}, []*model.Block{blockForward}))

	got := make(map[chainhash.Hash]struct{}, len(utxoStore.markedOn))
	for _, h := range utxoStore.markedOn {
		got[h] = struct{}{}
	}

	missing := 0

	for h := range want {
		if _, ok := got[h]; !ok {
			missing++
		}
	}

	require.Zero(t, missing, "%d of %d winning txs were not marked on the longest chain (marked %d)", missing, txCount, len(got))
}

// reorgBlocks collects the losing txs of every moveForward block through the
// swiss-backed losing map's Iter; each of them must come out, not one per bucket.
func TestAppendAllTxHashes_VisitsEveryEntry(t *testing.T) {
	const n = 20000

	m := txmap.NewSplitSwissMap(n)
	want := make(map[chainhash.Hash]struct{}, n)

	for i := 0; i < n; i++ {
		h := chainhash.HashH([]byte{byte(i), byte(i >> 8), 0x11})
		want[h] = struct{}{}

		require.NoError(t, m.Put(h, 1))
	}

	got := appendAllTxHashes(nil, m)

	require.Len(t, got, len(want))

	for _, h := range got {
		delete(want, h)
	}

	require.Empty(t, want)
}
