package bsv

import (
	"encoding/binary"
	"strings"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/teranode/test/utils/transactions"
	"github.com/bsv-blockchain/teranode/test/utils/wirepeer"
	"github.com/stretchr/testify/require"
)

// Port of bitcoin-sv's bsv-sigopslimit-consensus-test.py.
//
// Upstream tests one consensus rule and its removal: before Genesis a block may
// carry at most MAX_BLOCK_SIGOPS_PER_MB signature operations per megabyte, and
// after Genesis the rule is gone. Its own docstring walks the two halves - six
// blocks below the fork, three of them rejected with bad-blk-sigops, then the
// same shapes above the fork, all accepted.
//
// The rule does not exist in Teranode, in either half. Established by reading
// the tree rather than inferred from the acceptances this test measures:
// nothing counts signature operations per block. The only sigops setting is
// maxtxsigopscountspolicy, which is per TRANSACTION, is handed to BDK as
// SetMaxSigOpsPostGenesisPolicy - so it is a POST-Genesis policy limit, not the
// pre-Genesis consensus one - and is 4294967295 in settings.conf. Teranode's own
// documentation for it says "BTC/BCH impose sigops limits per block and
// transaction; BSV removed this restriction". The two error strings a block-level
// limit would need, bad-txns-too-many-sigops and high-sigops, exist in
// services/rpc/Server.go only inside a commented-out block.
//
// So the post-Genesis half of this script agrees with Teranode, and the
// pre-Genesis half does not. The port asserts both, and the pre-Genesis
// over-limit case is a tripwire: it asserts that Teranode ACCEPTS a block
// bitcoin-sv rejects. See the sigops-limit-not-enforced-pre-genesis gap.
//
// bitcoin-sv's rule, for reference (src/validation.cpp:5795-5811,
// src/config.cpp:825-829):
//
//	nMbRoundedUp := 1 + (blockSize-1)/ONE_MEGABYTE
//	allowance    := nMbRoundedUp * MAX_BLOCK_SIGOPS_PER_MB_BEFORE_GENESIS
//
// counted with GetSigOpCountWithoutP2SH over every input's scriptSig and every
// output's scriptPubKey, and enforced only while !IsProtocolActive(era, Genesis)
// - the comment there reads "Sigops are not counted after Genesis anymore".
const (
	// maxBlockSigopsPerMB is upstream's MAX_BLOCK_SIGOPS_PER_MB
	// (test_framework/cdefs.py:137), which is
	// MAX_BLOCK_SIGOPS_PER_MB_BEFORE_GENESIS in src/consensus/consensus.h:37.
	maxBlockSigopsPerMB = 20000

	// sigopsLimitActivation is where Genesis activates for this port.
	//
	// Upstream uses 125 and mines to 120. The number is small here for the same
	// reason as genesisPushOnlyActivation: CoinbaseMaturity is 1 under
	// SETTINGS_CONTEXT=test, so the fixture needs three blocks, and the funding
	// block lands at height 3. Two pre-Genesis heights (4 and 5) plus the
	// activation height itself (6) is exactly what the three cases need.
	sigopsLimitActivation = 6

	// coinbaseSigops is the sigop count of a Teranode coinbase output.
	//
	// Teranode pays its coinbase to P2PKH, whose scriptPubKey ends in
	// OP_CHECKSIG, so every block starts one sigop into its own budget. Upstream
	// relies on the same fact and says so twice - "this goes over the limit
	// because the coinbase has one sigop".
	coinbaseSigops = 1
)

// checksigScript returns a locking script of n OP_CHECKSIG, upstream's
// CScript([OP_CHECKSIG] * n).
//
// Under bitcoin-sv's pre-Genesis counting each OP_CHECKSIG in a scriptPubKey is
// one sigop, so the script's sigop count is exactly n. The script is never
// executed - it is an output nobody spends - so it does not have to be
// satisfiable.
func checksigScript(n int) *bscript.Script {
	ops := make([]byte, n)
	for i := range ops {
		ops[i] = bscript.OpCHECKSIG
	}

	return bscript.NewFromBytes(ops)
}

// legacySigopCount is upstream's get_legacy_sigopcount_block
// (test_framework/blocktools.py:208), which is bitcoin-sv's
// GetSigOpCountWithoutP2SH (src/validation.cpp:438): every input's scriptSig plus
// every output's scriptPubKey, with fAccurate false, so OP_CHECKMULTISIG and
// OP_CHECKMULTISIGVERIFY count as 20 rather than reading the key count off the
// stack.
//
// It exists so the port asserts the sigop counts it builds, the way upstream does
// with assert_equal(get_legacy_sigopcount_block(b3), MAX_BLOCK_SIGOPS_PER_MB).
// Without it a mistake in checksigScript would leave every case below passing
// while measuring nothing.
func legacySigopCount(txs ...*bt.Tx) int {
	count := 0

	for _, tx := range txs {
		for _, in := range tx.Inputs {
			count += scriptSigopCount(in.UnlockingScript)
		}

		for _, out := range tx.Outputs {
			count += scriptSigopCount(out.LockingScript)
		}
	}

	return count
}

// scriptSigopCount counts sigops in one script under bitcoin-sv's inaccurate
// counting.
//
// It must skip over pushed data rather than scanning raw bytes, which is the
// whole reason this helper exists rather than a one-line range over the script.
// Measured while writing this port: a raw byte scan reads Teranode's coinbase as
// 21 sigops, not 1, because the coinbase input's scriptSig carries arbitrary data
// - height, extra nonce, the "test" tag - and that data happens to contain a 0xae
// byte, which is OP_CHECKMULTISIG's value and so scored 20. bitcoin-sv's
// GetSigOpCount walks the script with GetOp and never looks inside a push, so the
// same coinbase counts 1 there.
//
// Like GetOp, a push whose declared length runs past the end of the script ends
// the walk and the count so far stands.
func scriptSigopCount(s *bscript.Script) int {
	if s == nil {
		return 0
	}

	b := *s
	count := 0

	for i := 0; i < len(b); {
		op := b[i]
		i++

		switch {
		case op >= bscript.OpDATA1 && op <= bscript.OpDATA75:
			// A direct push: op is the byte count that follows.
			i += int(op)

		case op == bscript.OpPUSHDATA1:
			if i >= len(b) {
				return count
			}

			i += 1 + int(b[i])

		case op == bscript.OpPUSHDATA2:
			if i+1 >= len(b) {
				return count
			}

			i += 2 + int(binary.LittleEndian.Uint16(b[i:]))

		case op == bscript.OpPUSHDATA4:
			if i+3 >= len(b) {
				return count
			}

			i += 4 + int(binary.LittleEndian.Uint32(b[i:]))

		case op == bscript.OpCHECKSIG, op == bscript.OpCHECKSIGVERIFY:
			count++

		case op == bscript.OpCHECKMULTISIG, op == bscript.OpCHECKMULTISIGVERIFY:
			count += 20
		}
	}

	return count
}

// spendToScript builds a transaction spending fundingTx's vout into a single
// output carrying the given locking script.
//
// Like spendWithOpAdd it fills the input by hand rather than going through
// td.CreateTransactionWithOptions: the funding output is anyone-can-spend, so an
// empty unlocking script satisfies it, and an empty scriptSig contributes no
// sigops of its own. That keeps the block's sigop count equal to the coinbase
// plus the output script, which is what the arithmetic below relies on.
func spendToScript(t *testing.T, fundingTx *bt.Tx, vout uint32, amount uint64, lockingScript *bscript.Script) *bt.Tx {
	t.Helper()

	tx := bt.NewTx()

	require.NoError(t, tx.FromUTXOs(&bt.UTXO{
		TxIDHash:      fundingTx.TxIDChainHash(),
		Vout:          vout,
		LockingScript: fundingTx.Outputs[vout].LockingScript,
		Satoshis:      fundingTx.Outputs[vout].Satoshis,
	}), "add input spending funding output %d", vout)

	tx.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{})

	tx.AddOutput(&bt.Output{Satoshis: amount, LockingScript: lockingScript})

	return tx
}

func TestBSVSigopsLimitConsensus(t *testing.T) {
	td := wirepeer.NewLegacyDaemon(t, wirepeer.WithGenesisActivationHeight(sigopsLimitActivation))
	defer td.Stop(t)

	require.EqualValues(t, 1, td.Settings.ChainCfgParams.CoinbaseMaturity,
		"the fixture's block layout assumes the TestDaemon's CoinbaseMaturity of 1")
	require.EqualValues(t, sigopsLimitActivation, td.Settings.ChainCfgParams.GenesisActivationHeight,
		"the override should have moved the Genesis activation height")

	// Recorded as an assertion rather than a comment because the whole port turns
	// on it: if this ever becomes a real limit, the arithmetic below stops
	// describing anything and the tripwires should be revisited.
	require.EqualValues(t, 4294967295, td.Settings.Policy.MaxTxSigopsCountsPolicy,
		"maxtxsigopscountspolicy is effectively unlimited, so no per-transaction "+
			"sigops limit interferes with the block-level cases below")

	coinbaseTx := td.MineToMaturityAndGetSpendableCoinbaseTx(t, td.Ctx)

	// Three anyone-can-spend outputs, one per case, so no case is a double spend
	// of another. Upstream gets the same from prepare_init_chain's spendable
	// outputs; Teranode coinbases are P2PKH, so this funding transaction stands
	// in for upstream's OP_TRUE coinbases.
	fundingTx := td.CreateTransactionWithOptions(t,
		transactions.WithInput(coinbaseTx, 0),
		transactions.WithOutput(1e6, anyoneCanSpendScript()),
		transactions.WithOutput(1e6, anyoneCanSpendScript()),
		transactions.WithOutput(1e6, anyoneCanSpendScript()),
	)

	require.NoError(t, td.PropagationClient.ProcessTransaction(td.Ctx, fundingTx),
		"funding transaction should be accepted")
	td.WaitForBlockAssemblyToProcessTx(t, fundingTx.TxID())

	fundingBlock := td.MineAndWait(t, 1)

	require.EqualValues(t, sigopsLimitActivation-3, fundingBlock.Height,
		"funding tx %s should confirm three blocks below the activation height, "+
			"leaving two pre-Genesis heights and the activation height free",
		fundingTx.TxID())

	// Upstream Test 3: a block with exactly MAX_BLOCK_SIGOPS_PER_MB sigops is
	// accepted before Genesis. Both nodes accept it, for different reasons -
	// bitcoin-sv because it is exactly at the limit, Teranode because there is no
	// limit. Included for fidelity with upstream, and because it is the control
	// for the case below: it establishes that a block of this shape is acceptable
	// at all, so the next case differs only in the sigop count.
	atLimitTx := spendToScript(t, fundingTx, 0, 100000, checksigScript(maxBlockSigopsPerMB-coinbaseSigops))
	_, atLimitBlock := td.CreateTestBlock(t, fundingBlock, 0x5190, atLimitTx)

	require.EqualValues(t, sigopsLimitActivation-2, atLimitBlock.Height,
		"the at-limit block must sit below the activation height")

	// Upstream: assert_equal(get_legacy_sigopcount_block(b3), MAX_BLOCK_SIGOPS_PER_MB).
	// This also pins coinbaseSigops: if Teranode's coinbase stops being a
	// single-OP_CHECKSIG P2PKH, this fails rather than silently shifting every
	// count below by one.
	require.Equal(t, coinbaseSigops, legacySigopCount(atLimitBlock.CoinbaseTx),
		"a Teranode coinbase should carry exactly one sigop")
	require.Equal(t, maxBlockSigopsPerMB, legacySigopCount(atLimitBlock.CoinbaseTx, atLimitTx),
		"the at-limit block must carry exactly MAX_BLOCK_SIGOPS_PER_MB sigops")
	require.NoError(t,
		td.BlockValidationClient.ProcessBlock(td.Ctx, atLimitBlock, atLimitBlock.Height, "", "legacy", 0),
		"a pre-Genesis block with exactly MAX_BLOCK_SIGOPS_PER_MB sigops should be accepted")

	atLimitHash := atLimitBlock.Header.Hash().String()

	require.Eventually(t, func() bool {
		return tryBestBlockHash(td) == atLimitHash
	}, 30*time.Second, 250*time.Millisecond,
		"the at-limit block should become the tip; tip is %s at height %d",
		bestBlockHash(t, td), bestHeight(t, td))

	// Upstream Test 4: one sigop more, and the block is rejected with
	// RejectResult(16, b'bad-blk-sigops').
	//
	// TRIPWIRE. Teranode accepts it, because it counts nothing. If this ever
	// starts failing, Teranode has grown a pre-Genesis block sigops limit, which
	// is what the gap asks for - rewrite this subtest to assert the rejection and
	// retire the waiver in registry.yaml.
	overLimitTx := spendToScript(t, fundingTx, 1, 100000, checksigScript(maxBlockSigopsPerMB))
	_, overLimitBlock := td.CreateTestBlock(t, atLimitBlock, 0x5191, overLimitTx)

	require.EqualValues(t, sigopsLimitActivation-1, overLimitBlock.Height,
		"the over-limit block must still sit below the activation height")

	// Upstream: assert_equal(get_legacy_sigopcount_block(b4), MAX_BLOCK_SIGOPS_PER_MB + 1).
	// The block really is over bitcoin-sv's limit, so the acceptance below is a
	// divergence and not an accounting error on this side.
	require.Equal(t, maxBlockSigopsPerMB+1, legacySigopCount(overLimitBlock.CoinbaseTx, overLimitTx),
		"the over-limit block must carry exactly MAX_BLOCK_SIGOPS_PER_MB+1 sigops, "+
			"one more than bitcoin-sv allows a pre-Genesis block of this size")
	// TRIPWIRE, and deliberately discriminating rather than a bare require.NoError.
	//
	// The point of this assertion is that the block is not refused FOR ITS SIGOP
	// COUNT. A bare NoError would report "the gap has been fixed" for any error at
	// all, including a transient one under suite load - which is exactly what
	// happened once while this port was being written, and cost time to rule out.
	// So the error is inspected: a sigops refusal means the gap is fixed and this
	// subtest should be rewritten, and anything else is a different problem and
	// says so.
	err := td.BlockValidationClient.ProcessBlock(td.Ctx, overLimitBlock, overLimitBlock.Height, "", "legacy", 0)
	if err != nil {
		require.NotContains(t, strings.ToLower(err.Error()), "sigop",
			"TRIPWIRE FIRED: a pre-Genesis block with MAX_BLOCK_SIGOPS_PER_MB+1 sigops was "+
				"refused for its sigop count, where until now Teranode accepted it. The "+
				"sigops-limit-absent-pre-genesis gap has been fixed - rewrite this subtest to "+
				"assert upstream's bad-blk-sigops rejection and retire the waivers in "+
				"registry.yaml. Error: %v", err)

		require.NoError(t, err,
			"the over-limit block was refused, but not for its sigop count, so this is not the "+
				"tripwire firing - it is an unrelated failure and needs diagnosing on its own")
	}

	overLimitHash := overLimitBlock.Header.Hash().String()

	require.Eventually(t, func() bool {
		return tryBestBlockHash(td) == overLimitHash
	}, 30*time.Second, 250*time.Millisecond,
		"TRIPWIRE: the over-limit block should become the tip, since nothing rejected it; "+
			"tip is %s at height %d", bestBlockHash(t, td), bestHeight(t, td))
	require.EqualValues(t, sigopsLimitActivation-1, bestHeight(t, td),
		"the over-limit block's height should be the tip height")

	// Upstream Test 11: above Genesis the same over-limit block is accepted, and
	// this half the two nodes agree on. This is the assertion the port actually
	// contributes - that Teranode does not impose a sigops limit where BSV
	// removed one.
	postGenesisTx := spendToScript(t, fundingTx, 2, 100000, checksigScript(maxBlockSigopsPerMB))
	_, postGenesisBlock := td.CreateTestBlock(t, overLimitBlock, 0x5192, postGenesisTx)

	require.EqualValues(t, sigopsLimitActivation, postGenesisBlock.Height,
		"the post-Genesis block must sit at the activation height")
	require.Equal(t, maxBlockSigopsPerMB+1, legacySigopCount(postGenesisBlock.CoinbaseTx, postGenesisTx),
		"the post-Genesis block must carry the same over-limit sigop count, so the only "+
			"difference from the case above is which side of the fork it sits on")
	require.NoError(t,
		td.BlockValidationClient.ProcessBlock(td.Ctx, postGenesisBlock, postGenesisBlock.Height, "", "legacy", 0),
		"a block with MAX_BLOCK_SIGOPS_PER_MB+1 sigops should be accepted at and above the "+
			"Genesis activation height, where the limit no longer applies")

	postGenesisHash := postGenesisBlock.Header.Hash().String()

	require.Eventually(t, func() bool {
		return tryBestBlockHash(td) == postGenesisHash
	}, 30*time.Second, 250*time.Millisecond,
		"the post-Genesis block should become the tip; tip is %s at height %d",
		bestBlockHash(t, td), bestHeight(t, td))
	require.EqualValues(t, sigopsLimitActivation, bestHeight(t, td),
		"the tip should be at the activation height")
}
