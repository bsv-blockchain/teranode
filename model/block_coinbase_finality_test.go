package model

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-chaincfg"
	txmap "github.com/bsv-blockchain/go-tx-map"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/settings"
	blobmemory "github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

const (
	// finalityTestHeight is below regtest's first halving (150), so the 50 BTC output is the full
	// subsidy, and below regtest's CSVHeight (576).
	finalityTestHeight = uint32(100)

	// regtestGenesisTimestamp is the regtest genesis header's timestamp. With currentChain holding
	// only the genesis header, it is also the parent's median time past.
	regtestGenesisTimestamp = uint32(1296688602)

	// finalityTestHeaderTime is the header timestamp of the blocks that need the header time and the
	// parent's median time past to differ.
	finalityTestHeaderTime = regtestGenesisTimestamp + 1000

	nonFinalSequence = uint32(0)
)

// finalityCoinbase builds a coinbase that satisfies every rule except possibly finality: one
// consensus-shaped input, a scriptSig that starts with the BIP34 height push, one output of 50 BTC,
// with the given nLockTime and input sequence.
func finalityCoinbase(t *testing.T, height, lockTime, sequence uint32) *bt.Tx {
	t.Helper()

	scriptSig := append(util.EncodeCoinbaseHeightPush(height), []byte("/coinbase-finality/")...)

	cb := bt.NewTx()
	require.NoError(t, cb.From("0000000000000000000000000000000000000000000000000000000000000000", 0xFFFFFFFF, "", 0))
	cb.Inputs[0].UnlockingScript = bscript.NewFromBytes(scriptSig)
	cb.Inputs[0].SequenceNumber = sequence
	require.NoError(t, cb.AddP2PKHOutputFromAddress("1DkmRkb5iQFkDu4NBysog5bugnsyx7kwtn", 50*100_000_000))
	cb.LockTime = lockTime

	require.True(t, IsConsensusCoinbase(cb), "fixture precondition: consensus-shaped coinbase")

	return cb
}

// minedHeaderAt is minedHeaderOnParent with a chosen timestamp.
func minedHeaderAt(t *testing.T, timestamp uint32, merkleRoot, prev *chainhash.Hash) *BlockHeader {
	t.Helper()

	nBits, err := NewNBitFromString("207fffff")
	require.NoError(t, err)

	hdr := &BlockHeader{
		Version:        4,
		HashPrevBlock:  prev,
		HashMerkleRoot: merkleRoot,
		Timestamp:      timestamp,
		Bits:           *nBits,
	}

	for {
		if ok, _, _ := hdr.HasMetTargetDifficulty(); ok {
			return hdr
		}

		hdr.Nonce++
	}
}

// finalityBlock wraps cb in a bound, coinbase-only block on the regtest genesis (header merkle root
// = coinbase txid), so every failure is a verdict on the block hash.
func finalityBlock(t *testing.T, cb *bt.Tx, headerTime uint32) *Block {
	t.Helper()

	hdr := minedHeaderAt(t, headerTime, cb.TxIDChainHash(), regtestGenesisHeader(t).Hash())

	block, err := NewBlock(hdr, cb, []*chainhash.Hash{}, 1, uint64(cb.Size()), finalityTestHeight, 0) //nolint:gosec
	require.NoError(t, err)

	return block
}

// finalitySettings activates BIP34 at height 1 and CSV at csvHeight.
func finalitySettings(t *testing.T, csvHeight uint32) *settings.Settings {
	t.Helper()

	params := chaincfg.RegressionNetParams
	params.BIP0034Height = 1
	params.CSVHeight = csvHeight

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.ChainCfgParams = &params

	return tSettings
}

func validateFinalityBlock(t *testing.T, block *Block, tSettings *settings.Settings, currentChain []*BlockHeader) (bool, error) {
	t.Helper()

	return block.Valid(context.Background(), ulogger.TestLogger{}, blobmemory.New(), createTestUTXOStore(t),
		txmap.NewSyncedMap[chainhash.Hash, []uint32](), currentChain, []uint32{0}, tSettings, nil)
}

func requireBoundNonFinal(t *testing.T, valid bool, err error) {
	t.Helper()

	require.Error(t, err, "a non-final coinbase must be rejected (bitcoin-sv bad-txns-nonfinal)")
	require.False(t, valid)
	require.True(t, errors.Is(err, errors.ErrBlockInvalid), "must be ERR_BLOCK_INVALID on a bound body, got: %v", err)
	require.False(t, errors.IsBlockCorrupt(err), "a bound body must not be classified corrupt")
	require.Contains(t, err.Error(), "bad-txns-nonfinal")
}

// TestBlock_Valid_NonFinalCoinbaseRejected is bitcoin-sv/teranode#4981 (GAP-BV-045, BV-BODY-019):
// bitcoin-sv applies IsFinalTx to the coinbase in ContextualCheckBlock, and Teranode's only finality
// check was in the validator, which never sees a coinbase. A bound block at height H whose coinbase
// has nLockTime = H and input sequence 0 is non-final and must be rejected; with sequence 0xFFFFFFFF
// the same coinbase is final.
func TestBlock_Valid_NonFinalCoinbaseRejected(t *testing.T) {
	tSettings := finalitySettings(t, chaincfg.RegressionNetParams.CSVHeight)
	chain := []*BlockHeader{regtestGenesisHeader(t)}

	t.Run("control: sequence 0xFFFFFFFF makes the coinbase final", func(t *testing.T) {
		block := finalityBlock(t, finalityCoinbase(t, finalityTestHeight, finalityTestHeight, 0xFFFFFFFF), regtestGenesisTimestamp)

		valid, err := validateFinalityBlock(t, block, tSettings, chain)
		require.NoError(t, err, "fixture must satisfy every other block rule, so the only variable is finality")
		require.True(t, valid)
	})

	t.Run("nLockTime == height with sequence 0 is non-final and INVALID", func(t *testing.T) {
		block := finalityBlock(t, finalityCoinbase(t, finalityTestHeight, finalityTestHeight, nonFinalSequence), regtestGenesisTimestamp)

		valid, err := validateFinalityBlock(t, block, tSettings, chain)
		requireBoundNonFinal(t, valid, err)
	})

	t.Run("nLockTime == height-1 with sequence 0 is final", func(t *testing.T) {
		block := finalityBlock(t, finalityCoinbase(t, finalityTestHeight, finalityTestHeight-1, nonFinalSequence), regtestGenesisTimestamp)

		valid, err := validateFinalityBlock(t, block, tSettings, chain)
		require.NoError(t, err)
		require.True(t, valid)
	})
}

// TestBlock_Valid_CoinbaseTimeLockCutoff pins which time a time-based coinbase nLockTime is compared
// against: the block's header time before CSV, the parent's median time past from CSV onwards
// (bitcoin-sv ContextualCheckBlock). The header time is 1000s after the parent's median time past, so
// an nLockTime between the two is final before CSV and non-final after it.
func TestBlock_Valid_CoinbaseTimeLockCutoff(t *testing.T) {
	chain := []*BlockHeader{regtestGenesisHeader(t)}
	between := regtestGenesisTimestamp + 500

	t.Run("pre-CSV: nLockTime below the header time is final", func(t *testing.T) {
		block := finalityBlock(t, finalityCoinbase(t, finalityTestHeight, between, nonFinalSequence), finalityTestHeaderTime)

		valid, err := validateFinalityBlock(t, block, finalitySettings(t, finalityTestHeight+1), chain)
		require.NoError(t, err)
		require.True(t, valid)
	})

	t.Run("pre-CSV: nLockTime equal to the header time is non-final", func(t *testing.T) {
		block := finalityBlock(t, finalityCoinbase(t, finalityTestHeight, finalityTestHeaderTime, nonFinalSequence), finalityTestHeaderTime)

		valid, err := validateFinalityBlock(t, block, finalitySettings(t, finalityTestHeight+1), chain)
		requireBoundNonFinal(t, valid, err)
	})

	t.Run("post-CSV: nLockTime between the parent MTP and the header time is non-final", func(t *testing.T) {
		block := finalityBlock(t, finalityCoinbase(t, finalityTestHeight, between, nonFinalSequence), finalityTestHeaderTime)

		valid, err := validateFinalityBlock(t, block, finalitySettings(t, finalityTestHeight), chain)
		requireBoundNonFinal(t, valid, err)
	})

	t.Run("post-CSV: nLockTime equal to the parent MTP is non-final", func(t *testing.T) {
		block := finalityBlock(t, finalityCoinbase(t, finalityTestHeight, regtestGenesisTimestamp, nonFinalSequence), finalityTestHeaderTime)

		valid, err := validateFinalityBlock(t, block, finalitySettings(t, finalityTestHeight), chain)
		requireBoundNonFinal(t, valid, err)
	})

	t.Run("post-CSV: nLockTime one below the parent MTP is final", func(t *testing.T) {
		block := finalityBlock(t, finalityCoinbase(t, finalityTestHeight, regtestGenesisTimestamp-1, nonFinalSequence), finalityTestHeaderTime)

		valid, err := validateFinalityBlock(t, block, finalitySettings(t, finalityTestHeight), chain)
		require.NoError(t, err)
		require.True(t, valid)
	})
}

// TestBlock_Valid_CoinbaseFinalityWithoutChain pins the no-ancestor-headers case (block assembly
// passes none): from CSV onwards the parent's median time past is unknown, so a time-based nLockTime
// is not evaluated, as the median-time-past header rule is skipped. A height-based nLockTime needs no
// chain and is still enforced.
func TestBlock_Valid_CoinbaseFinalityWithoutChain(t *testing.T) {
	tSettings := finalitySettings(t, finalityTestHeight)

	t.Run("post-CSV time-based nLockTime is skipped", func(t *testing.T) {
		block := finalityBlock(t, finalityCoinbase(t, finalityTestHeight, finalityTestHeaderTime, nonFinalSequence), finalityTestHeaderTime)

		valid, err := validateFinalityBlock(t, block, tSettings, nil)
		require.NoError(t, err)
		require.True(t, valid)
	})

	t.Run("height-based nLockTime is still enforced", func(t *testing.T) {
		block := finalityBlock(t, finalityCoinbase(t, finalityTestHeight, finalityTestHeight, nonFinalSequence), finalityTestHeaderTime)

		valid, err := validateFinalityBlock(t, block, tSettings, nil)
		requireBoundNonFinal(t, valid, err)
	})
}

// TestBlock_Valid_UnboundNonFinalCoinbaseIsCorrupt pins the binding-aware classification: with
// subtrees present but no subtree store, CheckMerkleRoot never runs, so a non-final coinbase on that
// unbound body is corrupt (re-download), never invalid (poisoned).
func TestBlock_Valid_UnboundNonFinalCoinbaseIsCorrupt(t *testing.T) {
	tSettings := finalitySettings(t, chaincfg.RegressionNetParams.CSVHeight)

	txHash, err := chainhash.NewHashFromStr("0f9188f13cb7b2c71f2a335e3a4fc328bf5beb436012afca590b1a11466e2206")
	require.NoError(t, err)

	coinbase := finalityCoinbase(t, finalityTestHeight, finalityTestHeight, nonFinalSequence)
	st, merkleRoot := buildSubtreeAndMerkleRoot(t, coinbase, *txHash)
	hdr := minedHeaderVersion(t, 4, merkleRoot)

	block, err := NewBlock(hdr, coinbase, []*chainhash.Hash{st.RootHash()}, 2, 123, finalityTestHeight, 0)
	require.NoError(t, err)

	valid, err := block.Valid(context.Background(), ulogger.TestLogger{}, nil,
		&panicTxMetaStore{}, txmap.NewSyncedMap[chainhash.Hash, []uint32](), []*BlockHeader{}, []uint32{}, tSettings, nil)
	require.False(t, valid)
	require.Error(t, err)
	require.True(t, errors.IsBlockCorrupt(err), "an unbound non-final coinbase must be corrupt, got: %v", err)
	require.False(t, errors.Is(err, errors.ErrBlockInvalid), "an unbound body must never be poisoned")
	require.Contains(t, err.Error(), "bad-txns-nonfinal")
}

func TestCoinbaseFinalityViolation(t *testing.T) {
	const (
		height     = uint32(100)
		headerTime = uint32(1_600_001_000)
		mtp        = uint32(1_600_000_000)
		preCSV     = height + 1
		postCSV    = height
	)

	tests := []struct {
		name      string
		lockTime  uint32
		sequence  uint32
		csvHeight uint32
		mtp       uint32
		want      string
	}{
		{"nLockTime 0 is final whatever the sequence", 0, nonFinalSequence, postCSV, mtp, ""},
		{"final sequence waives any nLockTime", 0xFFFFFFFF, 0xFFFFFFFF, postCSV, mtp, ""},
		{"height lock below the height is final", height - 1, nonFinalSequence, postCSV, mtp, ""},
		{"height lock equal to the height is non-final", height, nonFinalSequence, postCSV, mtp, "bad-txns-nonfinal"},
		{"height lock above the height is non-final", height + 1, nonFinalSequence, postCSV, mtp, "bad-txns-nonfinal"},
		{"highest height lock is non-final", lockTimeThreshold - 1, nonFinalSequence, postCSV, mtp, "bad-txns-nonfinal"},
		{"pre-CSV: time lock below the header time is final", headerTime - 1, nonFinalSequence, preCSV, mtp, ""},
		{"pre-CSV: time lock equal to the header time is non-final", headerTime, nonFinalSequence, preCSV, mtp, "bad-txns-nonfinal"},
		{"pre-CSV: header time applies even without a parent MTP", headerTime, nonFinalSequence, preCSV, 0, "bad-txns-nonfinal"},
		{"post-CSV: time lock below the parent MTP is final", mtp - 1, nonFinalSequence, postCSV, mtp, ""},
		{"post-CSV: time lock equal to the parent MTP is non-final", mtp, nonFinalSequence, postCSV, mtp, "bad-txns-nonfinal"},
		{"post-CSV: time lock between MTP and header time is non-final", mtp + 500, nonFinalSequence, postCSV, mtp, "bad-txns-nonfinal"},
		{"post-CSV: lowest time lock is final", lockTimeThreshold, nonFinalSequence, postCSV, mtp, ""},
		{"post-CSV: time lock without a parent MTP is not evaluated", headerTime, nonFinalSequence, postCSV, 0, ""},
		{"post-CSV: height lock without a parent MTP is still enforced", height, nonFinalSequence, postCSV, 0, "bad-txns-nonfinal"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			params := chaincfg.RegressionNetParams
			params.CSVHeight = tt.csvHeight

			cb := finalityCoinbase(t, height, tt.lockTime, tt.sequence)
			require.Equal(t, tt.want, CoinbaseFinalityViolation(cb, headerTime, height, &params, tt.mtp))

			// The promise CoinbaseFinalityNeedsMedianTimePast makes to a caller without headers:
			// when it says the median time past does not matter, leaving it unknown gives the same
			// verdict.
			if !CoinbaseFinalityNeedsMedianTimePast(cb, height, &params) {
				require.Equal(t, tt.want, CoinbaseFinalityViolation(cb, headerTime, height, &params, 0))
			}
		})
	}
}

func TestCoinbaseFinalityNeedsMedianTimePast(t *testing.T) {
	const height = uint32(100)

	tests := []struct {
		name      string
		lockTime  uint32
		sequence  uint32
		csvHeight uint32
		want      bool
	}{
		{"post-CSV time lock with a non-final sequence needs it", lockTimeThreshold, nonFinalSequence, height, true},
		{"pre-CSV time lock is measured against the header time", lockTimeThreshold, nonFinalSequence, height + 1, false},
		{"height lock never needs it", lockTimeThreshold - 1, nonFinalSequence, height, false},
		{"nLockTime 0 never needs it", 0, nonFinalSequence, height, false},
		{"final sequence waives the time lock", lockTimeThreshold, 0xFFFFFFFF, height, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			params := chaincfg.RegressionNetParams
			params.CSVHeight = tt.csvHeight

			cb := finalityCoinbase(t, height, tt.lockTime, tt.sequence)
			require.Equal(t, tt.want, CoinbaseFinalityNeedsMedianTimePast(cb, height, &params))
		})
	}
}
