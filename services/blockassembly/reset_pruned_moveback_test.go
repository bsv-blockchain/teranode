package blockassembly

import (
	"context"
	"sync"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-subtree"
	tErrors "github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/blockassembly/subtreeprocessor"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// markRecorderStore records the hashes handed to MarkTransactionsOnLongestChain(false)
// and can fail BatchDecorate, the existence lookup reset runs before marking.
type markRecorderStore struct {
	utxo.Store
	mu          sync.Mutex
	markedFalse []chainhash.Hash
	decorateErr error
	// itemErrs sets a per-item error on the given hashes after a successful lookup
	itemErrs map[chainhash.Hash]error
}

func (s *markRecorderStore) MarkTransactionsOnLongestChain(ctx context.Context, txHashes []chainhash.Hash, onLongestChain bool) error {
	if !onLongestChain {
		s.mu.Lock()
		s.markedFalse = append(s.markedFalse, txHashes...)
		s.mu.Unlock()
	}

	return s.Store.MarkTransactionsOnLongestChain(ctx, txHashes, onLongestChain)
}

func (s *markRecorderStore) BatchDecorate(ctx context.Context, items []*utxo.UnresolvedMetaData, f ...fields.FieldName) error {
	if s.decorateErr != nil {
		return s.decorateErr
	}

	if err := s.Store.BatchDecorate(ctx, items, f...); err != nil {
		return err
	}

	for _, it := range items {
		if err, ok := s.itemErrs[it.Hash]; ok {
			it.Err = err
		}
	}

	return nil
}

func seedSubtreeWithTxs(t *testing.T, items *baTestItems, txHashes ...chainhash.Hash) *chainhash.Hash {
	t.Helper()

	st, err := subtree.NewTreeByLeafCount(64)
	require.NoError(t, err)
	require.NoError(t, st.AddCoinbaseNode())

	for _, h := range txHashes {
		require.NoError(t, st.AddSubtreeNode(subtree.Node{Hash: h, Fee: 100, SizeInBytes: 250}))
	}

	stBytes, err := st.Serialize()
	require.NoError(t, err)

	rootHash := st.RootHash()
	require.NoError(t, items.blobStore.Set(t.Context(), rootHash[:], fileformat.FileTypeSubtree, stBytes))
	require.NoError(t, items.blobStore.Set(t.Context(), rootHash[:], fileformat.FileTypeSubtreeMeta, []byte{}))

	return rootHash
}

// setupPrunedMoveBackReset builds a reset whose moveBack block h2B holds two
// transactions: one still in the UTXO store, one already pruned (no record).
// It returns the assembler, the recorder, and both hashes.
func setupPrunedMoveBackReset(t *testing.T) (*BlockAssembler, *markRecorderStore, *baTestItems, chainhash.Hash, chainhash.Hash) {
	t.Helper()

	initPrometheusMetrics()

	items := setupBlockAssemblyTest(t)
	ctx := t.Context()
	ba := items.blockAssembler

	ba.settings.BlockValidation.IsParentMinedRetryMaxRetry = 1

	mockStp := &subtreeprocessor.MockSubtreeProcessor{}
	mockStp.On("WaitForPendingBlocks", mock.Anything).Return(nil)
	mockStp.On("Reset", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(subtreeprocessor.ResetResponse{})
	mockStp.On("GetCurrentBlockHeader").Return(model.GenesisBlockHeader)
	injectMockStp(t, items, mockStp)

	rec := &markRecorderStore{Store: items.utxoStore}
	ba.utxoStore = rec

	// the surviving tx has a record and is mined (unmined_since cleared)
	existing := bt.NewTx()
	in := &bt.Input{PreviousTxOutIndex: 0, PreviousTxSatoshis: 5000000000, SequenceNumber: 0xffffffff, UnlockingScript: bscript.NewFromBytes([]byte{})}
	require.NoError(t, in.PreviousTxIDAdd(&chainhash.Hash{}))
	existing.Inputs = []*bt.Input{in}
	existing.Outputs = []*bt.Output{{Satoshis: 1000, LockingScript: bscript.NewFromBytes([]byte{0x76, 0xa9, 0x14, 0x00, 0x00, 0x00, 0x00, 0x00, 0x88, 0xac})}}

	_, _, err := items.utxoStore.SpendAndCreate(ctx, existing, 0, utxo.WithCreateOnly())
	require.NoError(t, err)

	existingHash := *existing.TxIDChainHash()
	require.NoError(t, items.utxoStore.MarkTransactionsOnLongestChain(ctx, []chainhash.Hash{existingHash}, true))

	prunedHash := chainhash.HashH([]byte("pruned moveBack tx"))

	subtreeB := seedSubtreeWithTxs(t, items, existingHash, prunedHash)
	subtreeA2 := seedSubtreeWithTxs(t, items, chainhash.HashH([]byte("main h2A")))

	require.NoError(t, items.addBlock(ctx, blockHeader1))
	h1 := blockHeader1

	h2A := &model.BlockHeader{Version: 1, HashPrevBlock: h1.Hash(), HashMerkleRoot: &chainhash.Hash{}, Nonce: 22, Bits: *bits}
	h3A := &model.BlockHeader{Version: 1, HashPrevBlock: h2A.Hash(), HashMerkleRoot: &chainhash.Hash{}, Nonce: 23, Bits: *bits}
	h2B := &model.BlockHeader{Version: 1, HashPrevBlock: h1.Hash(), HashMerkleRoot: &chainhash.Hash{}, Nonce: 32, Bits: *bits}

	coinbaseTx, _ := bt.NewTxFromString("02000000010000000000000000000000000000000000000000000000000000000000000000ffffffff03510101ffffffff0100f2052a01000000232103656065e6886ca1e947de3471c9e723673ab6ba34724476417fa9fcef8bafa604ac00000000")

	subtreeA3 := seedSubtreeWithTxs(t, items, chainhash.HashH([]byte("main h3A")))

	for _, blk := range []*model.Block{
		{Header: h2A, CoinbaseTx: coinbaseTx, TransactionCount: 1, Subtrees: []*chainhash.Hash{subtreeA2}},
		{Header: h3A, CoinbaseTx: coinbaseTx, TransactionCount: 1, Subtrees: []*chainhash.Hash{subtreeA3}},
		{Header: h2B, CoinbaseTx: coinbaseTx, TransactionCount: 2, Subtrees: []*chainhash.Hash{subtreeB}},
	} {
		require.NoError(t, items.blockchainClient.AddBlock(ctx, blk, ""))
	}

	// the assembler is parked on h2B; the best chain is h3A, so h2B moves back
	ba.setBestBlockHeader(h2B, 2)

	return ba, rec, items, existingHash, prunedHash
}

func unminedSinceOf(t *testing.T, items *baTestItems, h chainhash.Hash) uint32 {
	t.Helper()

	data, err := items.utxoStore.Get(t.Context(), &h, fields.UnminedSince)
	require.NoError(t, err)

	return data.UnminedSince
}

// A moveBack tx the UTXO store already pruned has no record. Marking it unmined
// makes the store call logger.Fatalf (process exit), so reset must leave it out
// of the mark and still mark the txs that survive.
func TestReset_PrunedMoveBackTxIsNotMarked(t *testing.T) {
	ba, rec, items, existingHash, prunedHash := setupPrunedMoveBackReset(t)

	require.Zero(t, unminedSinceOf(t, items, existingHash), "precondition: existing tx is mined")

	require.NoError(t, ba.reset(t.Context()))

	require.NotContains(t, rec.markedFalse, prunedHash, "a pruned tx must not reach MarkTransactionsOnLongestChain(false)")
	require.Contains(t, rec.markedFalse, existingHash, "a surviving moveBack tx must still be marked unmined")
	require.NotZero(t, unminedSinceOf(t, items, existingHash), "surviving moveBack tx must have unmined_since set")
}

// If the existence lookup fails outright, reset cannot tell pruned from live. It
// marks them all, as it did before the lookup existed: dropping them would leave
// live txs flagged mined while their block is off the chain, and nothing would
// repair that. A pruned tx among them still hits the store's fatal, as before.
func TestReset_MoveBackExistenceLookupFailureMarksAll(t *testing.T) {
	ba, rec, items, existingHash, prunedHash := setupPrunedMoveBackReset(t)
	rec.decorateErr = tErrors.NewStorageError("store unavailable")

	require.NoError(t, ba.reset(t.Context()))
	require.Contains(t, rec.markedFalse, existingHash)
	require.Contains(t, rec.markedFalse, prunedHash)
	require.NotZero(t, unminedSinceOf(t, items, existingHash))
}

// A per-tx lookup error other than not found (a transient Aerospike record error)
// is no evidence the record is gone: that tx is still marked.
func TestReset_MoveBackTransientLookupErrorStillMarks(t *testing.T) {
	ba, rec, items, existingHash, prunedHash := setupPrunedMoveBackReset(t)
	rec.itemErrs = map[chainhash.Hash]error{existingHash: tErrors.NewStorageError("record timeout")}

	require.NoError(t, ba.reset(t.Context()))
	require.Contains(t, rec.markedFalse, existingHash, "a tx whose lookup failed transiently is still marked")
	require.NotContains(t, rec.markedFalse, prunedHash)
	require.NotZero(t, unminedSinceOf(t, items, existingHash))
}
