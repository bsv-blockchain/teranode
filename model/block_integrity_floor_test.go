package model

import (
	"context"
	"io"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/stores/blob/options"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

// toCheckOnlyStore serves its subtrees only as FileTypeSubtreeToCheck, the file subtree validation
// leaves when it fetched a subtree it could not yet fully validate.
type toCheckOnlyStore struct {
	inner SubtreeStore
}

func (s *toCheckOnlyStore) GetIoReader(ctx context.Context, key []byte, fileType fileformat.FileType, opts ...options.FileOption) (io.ReadCloser, error) {
	if fileType != fileformat.FileTypeSubtreeToCheck {
		return nil, errors.ErrNotFound
	}

	return s.inner.GetIoReader(ctx, key, fileType, opts...)
}

// floorCoinbase is CoinbaseHex, the fixture coinbase, with the given nLockTime and input sequence
// number, so a test can make it non-final.
func floorCoinbase(t *testing.T, lockTime, sequence uint32) *bt.Tx {
	t.Helper()

	coinbase, err := bt.NewTxFromString(CoinbaseHex)
	require.NoError(t, err)

	coinbase.LockTime = lockTime
	coinbase.Inputs[0].SequenceNumber = sequence
	require.True(t, IsConsensusCoinbase(coinbase))

	return coinbase
}

// singleSubtreeBlock builds a height-0 block whose one subtree holds only the coinbase placeholder,
// with the header's merkle root committed to that subtree with coinbase in the placeholder's slot.
func singleSubtreeBlock(t *testing.T, coinbase *bt.Tx) (*Block, *rawSubtreeStore) {
	t.Helper()

	subtree, err := subtreepkg.NewTreeByLeafCount(2)
	require.NoError(t, err)
	require.NoError(t, subtree.AddCoinbaseNode())

	store := &rawSubtreeStore{data: make(map[chainhash.Hash][]byte)}
	store.put(t, subtree)

	root, err := subtree.RootHashWithReplaceRootNode(coinbase.TxIDChainHash(), 0, uint64(coinbase.Size())) //nolint:gosec
	require.NoError(t, err)

	header := minedHeaderOnParent(t, 1, root, regtestGenesisHeader(t).Hash())

	block, err := NewBlock(header, coinbase, []*chainhash.Hash{subtree.RootHash()}, 1, uint64(coinbase.Size()), 0, 0) //nolint:gosec
	require.NoError(t, err)

	return block, store
}

func checkFloor(t *testing.T, block *Block, store SubtreeStore) error {
	t.Helper()

	return block.CheckIntegrityFloor(context.Background(), store, nil, bindingTestSettings(t), ulogger.TestLogger{})
}

// TestCheckIntegrityFloor_ReadsOnlyTheBoundarySubtrees: an honest three-subtree body passes, and the
// floor reads the first and last subtrees only, leaving SubtreeSlices for Valid to load.
func TestCheckIntegrityFloor_ReadsOnlyTheBoundarySubtrees(t *testing.T) {
	newBlock, store := bodyShapeFixture(t, []int{4, 4, 4}, []int{4, 4, 2})
	block := newBlock()

	require.NoError(t, checkFloor(t, block, store))
	require.Equal(t, int32(2), store.reads.Load(), "only the first and last subtrees are read")
	require.Empty(t, block.SubtreeSlices, "the floor must not fill SubtreeSlices")
}

// TestCheckIntegrityFloor_SwappedCoinbaseIsUnbound: honest subtrees with a coinbase the header does
// not commit to cannot be bound, so the floor fails before any coinbase rule runs.
func TestCheckIntegrityFloor_SwappedCoinbaseIsUnbound(t *testing.T) {
	newBlock, store := bodyShapeFixture(t, []int{4, 4}, []int{4, 3})
	block := newBlock()
	block.CoinbaseTx = floorCoinbase(t, 0, 0xFFFFFFFE)

	err := checkFloor(t, block, store)
	require.Error(t, err)
	require.True(t, errors.IsBlockCorrupt(err), "got: %v", err)
	require.Contains(t, err.Error(), "merkle root does not match")
}

// TestCheckIntegrityFloor_NonFinalCoinbaseOnBoundBody: the body is bound, so the coinbase is the
// block's own, and its non-final nLockTime fails the floor with the same verdict Valid gives.
func TestCheckIntegrityFloor_NonFinalCoinbaseOnBoundBody(t *testing.T) {
	// A height-based nLockTime of 500 at height 0, and a sequence number that does not opt out.
	block, store := singleSubtreeBlock(t, floorCoinbase(t, 500, 0))

	err := checkFloor(t, block, store)
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.ErrBlockInvalid), "got: %v", err)
	require.Contains(t, err.Error(), "coinbase is not final")
}

// TestCheckIntegrityFloor_FinalCoinbaseOnBoundBodyPasses is the control for the test above.
func TestCheckIntegrityFloor_FinalCoinbaseOnBoundBodyPasses(t *testing.T) {
	block, store := singleSubtreeBlock(t, floorCoinbase(t, 500, 0xFFFFFFFF))

	require.NoError(t, checkFloor(t, block, store))
}

// TestCheckIntegrityFloor_ReadsTheToCheckFile: with no validated subtree file the floor reads the
// to-check file, as Valid's loader does.
func TestCheckIntegrityFloor_ReadsTheToCheckFile(t *testing.T) {
	block, store := singleSubtreeBlock(t, floorCoinbase(t, 0, 0xFFFFFFFF))

	require.NoError(t, checkFloor(t, block, &toCheckOnlyStore{inner: store}))
}

// TestCheckIntegrityFloor_SubtreeFileUnderTheWrongKey: a subtree file that is not the one its key
// names fails the floor, so a misfiled subtree cannot stand in for the committed one.
func TestCheckIntegrityFloor_SubtreeFileUnderTheWrongKey(t *testing.T) {
	block, store := singleSubtreeBlock(t, floorCoinbase(t, 0, 0xFFFFFFFF))

	other, err := subtreepkg.NewTreeByLeafCount(2)
	require.NoError(t, err)
	require.NoError(t, other.AddCoinbaseNode())
	require.NoError(t, other.AddNode(chainhash.Hash{0xEE}, 1, 1))

	otherBytes, err := other.Serialize()
	require.NoError(t, err)

	store.data[*block.Subtrees[0]] = otherBytes

	err = checkFloor(t, block, store)
	require.Error(t, err)
	require.Contains(t, err.Error(), "does not match its key")
}

// TestCheckIntegrityFloor_MissingSubtree: a subtree that is in neither file fails the floor.
func TestCheckIntegrityFloor_MissingSubtree(t *testing.T) {
	block, _ := singleSubtreeBlock(t, floorCoinbase(t, 0, 0xFFFFFFFF))

	err := checkFloor(t, block, &rawSubtreeStore{data: make(map[chainhash.Hash][]byte)})
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.ErrStorageError), "got: %v", err)
}

// TestCheckIntegrityFloor_UnreadableSubtree: bytes that do not deserialize as a subtree fail the
// floor.
func TestCheckIntegrityFloor_UnreadableSubtree(t *testing.T) {
	block, store := singleSubtreeBlock(t, floorCoinbase(t, 0, 0xFFFFFFFF))
	store.data[*block.Subtrees[0]] = []byte{0x01}

	err := checkFloor(t, block, store)
	require.Error(t, err)
	require.Contains(t, err.Error(), "failed to deserialize subtree")
}

// TestCheckIntegrityFloor_NoSubtreeStore: a body carrying subtrees cannot be bound without a store
// to read them from, so the floor fails rather than passing an unbound body.
func TestCheckIntegrityFloor_NoSubtreeStore(t *testing.T) {
	block, _ := singleSubtreeBlock(t, floorCoinbase(t, 0, 0xFFFFFFFF))

	require.Error(t, checkFloor(t, block, nil))
}

// TestCheckIntegrityFloor_NoCoinbase: a body without a coinbase fails the floor.
func TestCheckIntegrityFloor_NoCoinbase(t *testing.T) {
	block, store := singleSubtreeBlock(t, floorCoinbase(t, 0, 0xFFFFFFFF))
	block.CoinbaseTx = nil

	err := checkFloor(t, block, store)
	require.Error(t, err)
	require.True(t, errors.IsBlockCorrupt(err), "got: %v", err)
}

// TestCheckIntegrityFloor_CoinbaseOnlyBody: a body with no subtrees is bound by the coinbase-only
// rule, then held to the same coinbase rules.
func TestCheckIntegrityFloor_CoinbaseOnlyBody(t *testing.T) {
	coinbase := floorCoinbase(t, 0, 0xFFFFFFFF)
	header := minedHeaderOnParent(t, 1, coinbase.TxIDChainHash(), regtestGenesisHeader(t).Hash())

	block, err := NewBlock(header, coinbase, []*chainhash.Hash{}, 1, uint64(coinbase.Size()), 0, 0) //nolint:gosec
	require.NoError(t, err)
	require.NoError(t, checkFloor(t, block, nil), "a bound coinbase-only body needs no subtree store")

	header.HashMerkleRoot = &chainhash.Hash{0xCD}
	block, err = NewBlock(header, coinbase, []*chainhash.Hash{}, 1, uint64(coinbase.Size()), 0, 0) //nolint:gosec
	require.NoError(t, err)

	err = checkFloor(t, block, nil)
	require.Error(t, err)
	require.True(t, errors.IsBlockCorrupt(err), "got: %v", err)
}

// TestCheckIntegrityFloor_FinalityNeedsTheMedianTimePast: from CSV onwards a time-based nLockTime is
// judged against the parent's median time past. With no ancestor headers there is none, so the floor
// fails rather than pass a rule it did not evaluate; Valid skips the rule in that case.
func TestCheckIntegrityFloor_FinalityNeedsTheMedianTimePast(t *testing.T) {
	tSettings := bindingTestSettings(t)
	params := *tSettings.ChainCfgParams
	params.CSVHeight = 0
	tSettings.ChainCfgParams = &params

	// A time-based nLockTime and a sequence number that does not opt out.
	block, store := singleSubtreeBlock(t, floorCoinbase(t, 500_000_001, 0))

	err := block.CheckIntegrityFloor(context.Background(), store, nil, tSettings, ulogger.TestLogger{})
	require.Error(t, err)
	require.Contains(t, err.Error(), "coinbase finality rule cannot be evaluated")
}
