package model

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-chaincfg"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/stores/blob/options"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
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

// floorChain is the one-header run block validation hands the floor for a block on genesis: the
// parent's median time past is the regtest genesis timestamp.
func floorChain(t *testing.T) []*BlockHeader {
	t.Helper()

	return []*BlockHeader{regtestGenesisHeader(t)}
}

// boundSubtreeBlockAt builds a block at height on genesis, with header time headerTime, whose one
// subtree holds only the coinbase placeholder and whose merkle root commits to coinbase in that slot,
// so the floor binds the body and then judges the coinbase as the block's own.
func boundSubtreeBlockAt(t *testing.T, coinbase *bt.Tx, height, headerTime uint32) (*Block, *rawSubtreeStore) {
	t.Helper()

	subtree, err := subtreepkg.NewTreeByLeafCount(2)
	require.NoError(t, err)
	require.NoError(t, subtree.AddCoinbaseNode())

	store := &rawSubtreeStore{data: make(map[chainhash.Hash][]byte)}
	store.put(t, subtree)

	root, err := subtree.RootHashWithReplaceRootNode(coinbase.TxIDChainHash(), 0, uint64(coinbase.Size())) //nolint:gosec
	require.NoError(t, err)

	header := minedHeaderAt(t, headerTime, root, regtestGenesisHeader(t).Hash())

	block, err := NewBlock(header, coinbase, []*chainhash.Hash{subtree.RootHash()}, 1, uint64(coinbase.Size()), height, 0) //nolint:gosec
	require.NoError(t, err)

	return block, store
}

// requireBoundFloorFailure asserts the floor failed with the verdict a bound body gets, invalid and
// not corrupt, and with the rule's own message, so a failure from some other check cannot pass.
func requireBoundFloorFailure(t *testing.T, err error, message string) {
	t.Helper()

	require.Error(t, err)
	require.True(t, errors.Is(err, errors.ErrBlockInvalid), "a bound body's failure is invalid, got: %v", err)
	require.False(t, errors.IsBlockCorrupt(err), "a bound body's failure is not corrupt, got: %v", err)
	require.Contains(t, err.Error(), message)
}

// TestCheckIntegrityFloor_FinalityUsesTheParentMedianTimePast: from CSV onwards the floor judges a
// time-based coinbase nLockTime against the median time past of the header run it is given. An
// nLockTime between the parent's median time past and the header time is non-final there, and one
// below the median time past is final. A floor that judged against a median time past of 0 would pass
// the non-final coinbase, which is the coinbase the floor exists to keep from an early add.
func TestCheckIntegrityFloor_FinalityUsesTheParentMedianTimePast(t *testing.T) {
	tSettings := finalitySettings(t, finalityTestHeight)

	t.Run("nLockTime between the parent MTP and the header time is non-final", func(t *testing.T) {
		coinbase := finalityCoinbase(t, finalityTestHeight, regtestGenesisTimestamp+500, nonFinalSequence)
		block, store := boundSubtreeBlockAt(t, coinbase, finalityTestHeight, finalityTestHeaderTime)

		err := block.CheckIntegrityFloor(context.Background(), store, floorChain(t), tSettings, ulogger.TestLogger{})
		requireBoundFloorFailure(t, err, "coinbase is not final")
	})

	t.Run("control: nLockTime one below the parent MTP is final", func(t *testing.T) {
		coinbase := finalityCoinbase(t, finalityTestHeight, regtestGenesisTimestamp-1, nonFinalSequence)
		block, store := boundSubtreeBlockAt(t, coinbase, finalityTestHeight, finalityTestHeaderTime)

		require.NoError(t, block.CheckIntegrityFloor(context.Background(), store, floorChain(t), tSettings, ulogger.TestLogger{}))
	})
}

// TestCheckIntegrityFloor_HeaderContextFailureFailsTheFloor: a header the contextual checks reject
// fails the floor with their verdict, even when the body is bound and its coinbase passes every rule.
func TestCheckIntegrityFloor_HeaderContextFailureFailsTheFloor(t *testing.T) {
	tSettings := finalitySettings(t, finalityTestHeight)
	coinbase := finalityCoinbase(t, finalityTestHeight, 0, 0xFFFFFFFF)

	threeHoursAhead := uint32(time.Now().Add(3 * time.Hour).Unix()) //nolint:gosec
	block, store := boundSubtreeBlockAt(t, coinbase, finalityTestHeight, threeHoursAhead)

	err := block.CheckIntegrityFloor(context.Background(), store, floorChain(t), tSettings, ulogger.TestLogger{})
	requireBoundFloorFailure(t, err, "more than two hours in the future")

	// Control: the same body under a header time the checks accept passes.
	block, store = boundSubtreeBlockAt(t, coinbase, finalityTestHeight, finalityTestHeaderTime)
	require.NoError(t, block.CheckIntegrityFloor(context.Background(), store, floorChain(t), tSettings, ulogger.TestLogger{}))
}

// TestCheckIntegrityFloor_CoinbaseRulesOnBoundBody puts a bound body through the floor for each
// coinbase rule other than finality, at a height where BIP34 is active: each failure carries the
// rule's own message and the bound verdict, and the unchanged coinbase passes.
func TestCheckIntegrityFloor_CoinbaseRulesOnBoundBody(t *testing.T) {
	tSettings := finalitySettings(t, chaincfg.RegressionNetParams.CSVHeight)

	finalCoinbase := func(t *testing.T) *bt.Tx {
		return finalityCoinbase(t, finalityTestHeight, 0, 0xFFFFFFFF)
	}

	tests := []struct {
		name     string
		coinbase func(t *testing.T) *bt.Tx
		message  string
	}{
		{
			name:     "control: a coinbase that passes every rule",
			coinbase: finalCoinbase,
		},
		{
			name: "BIP34: the coinbase encodes another height",
			coinbase: func(t *testing.T) *bt.Tx {
				return finalityCoinbase(t, finalityTestHeight+1, 0, 0xFFFFFFFF)
			},
			message: "does not match block height",
		},
		{
			name: "the coinbase scriptSig is longer than 100 bytes",
			coinbase: func(t *testing.T) *bt.Tx {
				coinbase := finalCoinbase(t)
				scriptSig := append(util.EncodeCoinbaseHeightPush(finalityTestHeight), make([]byte, 100)...)
				coinbase.Inputs[0].UnlockingScript = bscript.NewFromBytes(scriptSig)

				return coinbase
			},
			message: "bad coinbase length",
		},
		{
			name: "a common transaction rule: an output above the money range",
			coinbase: func(t *testing.T) *bt.Tx {
				coinbase := finalCoinbase(t)
				coinbase.Outputs[0].Satoshis = 21_000_000*100_000_000 + 1

				return coinbase
			},
			message: "bad-txns-vout-toolarge",
		},
		{
			name: "the first transaction is not a coinbase",
			coinbase: func(t *testing.T) *bt.Tx {
				tx := bt.NewTx()
				require.NoError(t, tx.From("0100000000000000000000000000000000000000000000000000000000000000", 0, "", 0))
				require.NoError(t, tx.AddP2PKHOutputFromAddress("1DkmRkb5iQFkDu4NBysog5bugnsyx7kwtn", 1))
				require.False(t, IsConsensusCoinbase(tx), "fixture precondition: not a coinbase")

				return tx
			},
			message: "is not a valid coinbase tx",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			block, store := boundSubtreeBlockAt(t, tt.coinbase(t), finalityTestHeight, finalityTestHeaderTime)

			err := block.CheckIntegrityFloor(context.Background(), store, floorChain(t), tSettings, ulogger.TestLogger{})
			if tt.message == "" {
				require.NoError(t, err)
				return
			}

			requireBoundFloorFailure(t, err, tt.message)
		})
	}
}
