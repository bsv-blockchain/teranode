package model

import (
	"bytes"
	"context"
	"math"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	txmap "github.com/bsv-blockchain/go-tx-map"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/settings"
	blobmemory "github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// preGenesisHeight is the last regtest height before Genesis activates (regtest activates at 100),
// so the pre-Genesis coinbase limits apply. unboundBodyHeight (100) is the first post-Genesis one.
const preGenesisHeight = uint32(99)

// TestBlock_Valid_CoinbaseCommonRules covers bitcoin-sv/teranode#4835. bitcoin-sv's CheckCoinbase
// runs CheckTransactionCommon on the coinbase: non-empty inputs and outputs, the consensus size
// limit, the output money range, and the pre-Genesis sigop limit. Regular transactions get those
// checks from BDK, but the coinbase never goes through BDK, so before the fix Block.Valid accepted
// a coinbase with no outputs: the reward check sums zero outputs and zero never exceeds the subsidy.
//
// Every case goes through the real Block.Valid with a non-nil subtree store. A bound body is the
// miner's committed body, so each violation is genuine invalidity (condemn once), classified the
// same way as the neighbouring bad-cb-length check.
func TestBlock_Valid_CoinbaseCommonRules(t *testing.T) {
	callValid := func(t *testing.T, block *Block, tSettings *settings.Settings, subtreeStore *blobmemory.Memory) (bool, error) {
		t.Helper()

		// A nil *blobmemory.Memory is still a non-nil interface value, so branch to pass a real nil.
		if subtreeStore == nil {
			return block.Valid(context.Background(), ulogger.TestLogger{}, nil, &panicTxMetaStore{},
				txmap.NewSyncedMap[chainhash.Hash, []uint32](), []*BlockHeader{}, []uint32{}, tSettings, nil)
		}

		return block.Valid(context.Background(), ulogger.TestLogger{}, subtreeStore, &panicTxMetaStore{},
			txmap.NewSyncedMap[chainhash.Hash, []uint32](), []*BlockHeader{}, []uint32{}, tSettings, nil)
	}

	// coinbaseOnlyBlock returns a coinbase-only block at height whose header commits to coinbase.
	coinbaseOnlyBlock := func(t *testing.T, coinbase *bt.Tx, height uint32) *Block {
		t.Helper()

		block, err := NewBlock(minedHeaderVersion(t, 1, coinbase.TxIDChainHash()), coinbase,
			[]*chainhash.Hash{}, 1, 123, height, 0)
		require.NoError(t, err)

		return block
	}

	requireInvalid := func(t *testing.T, ok bool, err error, reason string) {
		t.Helper()

		require.False(t, ok)
		require.Error(t, err)
		require.True(t, errors.Is(err, errors.ErrBlockInvalid), "a bound coinbase violation must condemn invalid, got: %v", err)
		require.False(t, errors.IsBlockCorrupt(err), "a bound coinbase violation must not be corrupt")
		require.Contains(t, err.Error(), reason)
	}

	t.Run("coinbase-only block whose coinbase has no outputs is invalid", func(t *testing.T) {
		tSettings, subsidy := unboundBodySettings(t)

		coinbase := unboundBodyCoinbase(t, subsidy)
		coinbase.Outputs = nil

		ok, err := callValid(t, coinbaseOnlyBlock(t, coinbase, unboundBodyHeight), tSettings, blobmemory.New())
		requireInvalid(t, ok, err, "bad-txns-vout-empty")
	})

	t.Run("block with subtrees whose coinbase has no outputs is invalid", func(t *testing.T) {
		tSettings, _ := unboundBodySettings(t)

		coinbase := unboundBodyCoinbase(t, 0)
		coinbase.Outputs = nil

		// The header merkle root is computed over this output-less coinbase, so the body is bound.
		block, store := honestSubtreeBody(t, coinbase)

		ok, err := callValid(t, block, tSettings, store)
		requireInvalid(t, ok, err, "bad-txns-vout-empty")
	})

	t.Run("unbound body whose coinbase has no outputs stays corrupt", func(t *testing.T) {
		tSettings, _ := unboundBodySettings(t)

		coinbase := unboundBodyCoinbase(t, 0)
		coinbase.Outputs = nil

		// Subtrees present but no subtree store: CheckMerkleRoot never runs, so the body is unbound.
		subtreeHash := chainhash.Hash{0x01}
		block, err := NewBlock(minedHeaderVersion(t, 1, coinbase.TxIDChainHash()), coinbase,
			[]*chainhash.Hash{&subtreeHash}, 2, 123, unboundBodyHeight, 0)
		require.NoError(t, err)

		ok, err := callValid(t, block, tSettings, nil)
		require.False(t, ok)
		require.Error(t, err)
		require.True(t, errors.IsBlockCorrupt(err), "an unbound body must be corrupt, got: %v", err)
		require.False(t, errors.Is(err, errors.ErrBlockInvalid), "an unbound body must never be poisoned")
		require.Contains(t, err.Error(), "bad-txns-vout-empty")
	})

	t.Run("coinbase output above the money limit is rejected as vout-toolarge", func(t *testing.T) {
		tSettings, _ := unboundBodySettings(t)

		ok, err := callValid(t, coinbaseOnlyBlock(t, unboundBodyCoinbase(t, maxMoneySatoshis+1), unboundBodyHeight), tSettings, blobmemory.New())
		requireInvalid(t, ok, err, "bad-txns-vout-toolarge")
	})

	t.Run("pre-Genesis coinbase over the sigop limit is invalid", func(t *testing.T) {
		tSettings := test.CreateBaseTestSettings(t)

		coinbase := unboundBodyCoinbase(t, util.GetBlockSubsidyForHeight(preGenesisHeight, tSettings.ChainCfgParams))
		coinbase.Outputs[0].LockingScript = checkSigScript(maxTxSigOpsCountBeforeGenesis + 1)

		ok, err := callValid(t, coinbaseOnlyBlock(t, coinbase, preGenesisHeight), tSettings, blobmemory.New())
		requireInvalid(t, ok, err, "bad-txn-sigops")
	})

	t.Run("the same sigop count is accepted after Genesis", func(t *testing.T) {
		tSettings, subsidy := unboundBodySettings(t)

		coinbase := unboundBodyCoinbase(t, subsidy)
		coinbase.Outputs[0].LockingScript = checkSigScript(maxTxSigOpsCountBeforeGenesis + 1)

		ok, err := callValid(t, coinbaseOnlyBlock(t, coinbase, unboundBodyHeight), tSettings, blobmemory.New())
		require.NoError(t, err)
		require.True(t, ok)
	})
}

// TestCoinbaseCommonRuleViolation pins each rule of bitcoin-sv's CheckTransactionCommon as applied
// to a coinbase, in bitcoin-sv's order, with bitcoin-sv's reason strings and boundaries.
func TestCoinbaseCommonRuleViolation(t *testing.T) {
	params := test.CreateBaseTestSettings(t).ChainCfgParams

	coinbase := func(t *testing.T) *bt.Tx {
		t.Helper()

		tx, err := bt.NewTxFromString(CoinbaseHex)
		require.NoError(t, err)

		// A push-only scriptSig and a one-byte output script carry no sigops, so the sigop cases
		// below control the whole count.
		tx.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{0x02, 0x01, 0x02})
		tx.Outputs = tx.Outputs[:1]
		tx.Outputs[0].Satoshis = 1
		tx.Outputs[0].LockingScript = bscript.NewFromBytes([]byte{bscript.OpTRUE})

		return tx
	}

	t.Run("a well-formed coinbase passes before and after Genesis", func(t *testing.T) {
		require.Empty(t, CoinbaseCommonRuleViolation(coinbase(t), preGenesisHeight, params))
		require.Empty(t, CoinbaseCommonRuleViolation(coinbase(t), unboundBodyHeight, params))
	})

	t.Run("no inputs", func(t *testing.T) {
		tx := coinbase(t)
		tx.Inputs = nil

		require.Equal(t, "bad-txns-vin-empty", CoinbaseCommonRuleViolation(tx, unboundBodyHeight, params))
	})

	t.Run("no outputs", func(t *testing.T) {
		tx := coinbase(t)
		tx.Outputs = nil

		require.Equal(t, "bad-txns-vout-empty", CoinbaseCommonRuleViolation(tx, unboundBodyHeight, params))
	})

	t.Run("empty inputs are reported before empty outputs", func(t *testing.T) {
		tx := coinbase(t)
		tx.Inputs = nil
		tx.Outputs = nil

		require.Equal(t, "bad-txns-vin-empty", CoinbaseCommonRuleViolation(tx, unboundBodyHeight, params))
	})

	t.Run("pre-Genesis size limit is exactly one megabyte", func(t *testing.T) {
		tx := coinbase(t)
		base := tx.Size() - len(*tx.Outputs[0].LockingScript)

		// Grow the output script until the serialised transaction is exactly the limit. The script
		// length varint grows from 1 to 5 bytes on the way, which the second pass accounts for.
		script := make([]byte, maxTxSizeConsensusBeforeGenesis-base)
		tx.Outputs[0].LockingScript = bscript.NewFromBytes(script)
		script = script[:len(script)-(tx.Size()-maxTxSizeConsensusBeforeGenesis)]
		tx.Outputs[0].LockingScript = bscript.NewFromBytes(script)
		require.Equal(t, maxTxSizeConsensusBeforeGenesis, tx.Size())

		require.Empty(t, CoinbaseCommonRuleViolation(tx, preGenesisHeight, params))

		tx.Outputs[0].LockingScript = bscript.NewFromBytes(append(script, 0x00))
		require.Equal(t, "bad-txns-oversize", CoinbaseCommonRuleViolation(tx, preGenesisHeight, params))

		// After Genesis the limit is one gigabyte, so the same transaction passes.
		require.Empty(t, CoinbaseCommonRuleViolation(tx, unboundBodyHeight, params))
	})

	t.Run("size is checked before output values", func(t *testing.T) {
		tx := coinbase(t)
		tx.Outputs[0].LockingScript = bscript.NewFromBytes(make([]byte, maxTxSizeConsensusBeforeGenesis))
		tx.Outputs[0].Satoshis = maxMoneySatoshis + 1

		require.Equal(t, "bad-txns-oversize", CoinbaseCommonRuleViolation(tx, preGenesisHeight, params))
	})

	t.Run("an output value with the sign bit set is negative", func(t *testing.T) {
		tx := coinbase(t)
		tx.Outputs[0].Satoshis = math.MaxInt64 + 1

		require.Equal(t, "bad-txns-vout-negative", CoinbaseCommonRuleViolation(tx, unboundBodyHeight, params))
	})

	t.Run("an output value at the money limit passes, one above does not", func(t *testing.T) {
		tx := coinbase(t)
		tx.Outputs[0].Satoshis = maxMoneySatoshis
		require.Empty(t, CoinbaseCommonRuleViolation(tx, unboundBodyHeight, params))

		tx.Outputs[0].Satoshis = maxMoneySatoshis + 1
		require.Equal(t, "bad-txns-vout-toolarge", CoinbaseCommonRuleViolation(tx, unboundBodyHeight, params))
	})

	t.Run("outputs each in range but summing above the money limit", func(t *testing.T) {
		tx := coinbase(t)
		tx.Outputs[0].Satoshis = maxMoneySatoshis
		tx.AddOutput(&bt.Output{Satoshis: 1, LockingScript: bscript.NewFromBytes([]byte{bscript.OpTRUE})})

		require.Equal(t, "bad-txns-txouttotal-toolarge", CoinbaseCommonRuleViolation(tx, unboundBodyHeight, params))
	})

	t.Run("pre-Genesis sigop limit counts CHECKSIG exactly", func(t *testing.T) {
		tx := coinbase(t)
		tx.Outputs[0].LockingScript = checkSigScript(maxTxSigOpsCountBeforeGenesis)
		require.Empty(t, CoinbaseCommonRuleViolation(tx, preGenesisHeight, params))

		tx.Outputs[0].LockingScript = checkSigScript(maxTxSigOpsCountBeforeGenesis + 1)
		require.Equal(t, "bad-txn-sigops", CoinbaseCommonRuleViolation(tx, preGenesisHeight, params))
		require.Empty(t, CoinbaseCommonRuleViolation(tx, unboundBodyHeight, params), "no sigop limit after Genesis")
	})

	t.Run("pre-Genesis sigops are summed across the scriptSig and every output", func(t *testing.T) {
		tx := coinbase(t)
		half := maxTxSigOpsCountBeforeGenesis / 2
		tx.Outputs[0].LockingScript = checkSigScript(half)
		tx.AddOutput(&bt.Output{Satoshis: 1, LockingScript: checkSigScript(half)})
		require.Empty(t, CoinbaseCommonRuleViolation(tx, preGenesisHeight, params))

		// One CHECKSIG in the coinbase scriptSig tips the total over the limit.
		tx.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{bscript.Op1, bscript.OpCHECKSIG})
		require.Equal(t, "bad-txn-sigops", CoinbaseCommonRuleViolation(tx, preGenesisHeight, params))
	})
}

// TestPreGenesisSigOpCount pins the port of bitcoin-sv's CScript::GetSigOpCount(fAccurate=false)
// for a pre-Genesis era, including where it stops counting.
func TestPreGenesisSigOpCount(t *testing.T) {
	tests := []struct {
		name   string
		script []byte
		want   uint64
	}{
		{name: "empty", script: nil, want: 0},
		{name: "CHECKSIG and CHECKSIGVERIFY count one each", script: []byte{bscript.OpCHECKSIG, bscript.OpCHECKSIGVERIFY}, want: 2},
		{name: "CHECKMULTISIG counts twenty regardless of the key count", script: []byte{bscript.Op1, bscript.OpCHECKMULTISIG}, want: 20},
		{name: "CHECKMULTISIGVERIFY counts twenty", script: []byte{bscript.OpCHECKMULTISIGVERIFY}, want: 20},
		{name: "opcodes inside a direct push are data", script: []byte{0x02, bscript.OpCHECKSIG, bscript.OpCHECKSIG, bscript.OpCHECKSIG}, want: 1},
		{name: "opcodes inside PUSHDATA1 are data", script: []byte{bscript.OpPUSHDATA1, 0x01, bscript.OpCHECKSIG, bscript.OpCHECKSIG}, want: 1},
		{name: "opcodes inside PUSHDATA2 are data", script: []byte{bscript.OpPUSHDATA2, 0x01, 0x00, bscript.OpCHECKSIG, bscript.OpCHECKSIG}, want: 1},
		{name: "opcodes inside PUSHDATA4 are data", script: []byte{bscript.OpPUSHDATA4, 0x01, 0x00, 0x00, 0x00, bscript.OpCHECKSIG, bscript.OpCHECKSIG}, want: 1},
		{name: "a truncated direct push stops the count", script: []byte{bscript.OpCHECKSIG, 0x05, bscript.OpCHECKSIG}, want: 1},
		{name: "a truncated PUSHDATA1 length stops the count", script: []byte{bscript.OpCHECKSIG, bscript.OpPUSHDATA1}, want: 1},
		{name: "a truncated PUSHDATA2 length stops the count", script: []byte{bscript.OpCHECKSIG, bscript.OpPUSHDATA2, 0x01}, want: 1},
		{name: "a truncated PUSHDATA4 operand stops the count", script: []byte{bscript.OpCHECKSIG, bscript.OpPUSHDATA4, 0x09, 0x00, 0x00, 0x00, bscript.OpCHECKSIG}, want: 1},
		{name: "a 0xff byte stops the count", script: []byte{bscript.OpCHECKSIG, bscript.OpINVALIDOPCODE, bscript.OpCHECKSIG}, want: 1},
		{name: "OP_RETURN does not stop the pre-Genesis count", script: []byte{bscript.OpRETURN, bscript.OpCHECKSIG}, want: 1},
		{name: "OP_0 is not a push of the following bytes", script: []byte{bscript.Op0, bscript.OpCHECKSIG}, want: 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, preGenesisSigOpCount(tt.script))
		})
	}
}

// checkSigScript returns a script of n OP_CHECKSIG opcodes: n pre-Genesis sigops.
func checkSigScript(n int) *bscript.Script {
	return bscript.NewFromBytes(bytes.Repeat([]byte{bscript.OpCHECKSIG}, n))
}
