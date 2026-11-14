//go:build bvgaps

package blockvalidation

import (
	"bytes"
	"context"
	"encoding/binary"
	"net/url"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/legacy/bsvutil"
	"github.com/bsv-blockchain/teranode/services/legacy/txscript"
	"github.com/bsv-blockchain/teranode/settings"
	blockchain_store "github.com/bsv-blockchain/teranode/stores/blockchain"
	utxostore "github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/jarcoal/httpmock"
	"github.com/stretchr/testify/require"
)

// Fixtures for the pre-Genesis block sigop limit: below GenesisActivationHeight the total sigops of
// a block, counted with P2SH, must be <= 20,000 x ceil(block size in bytes / 1,000,000), and a
// violation is ERR_BLOCK_INVALID.
//
// The block is built on regtest genesis, so it is at height 1. The test chain's
// GenesisActivationHeight is set on the private copy of the chain params CreateBaseTestSettings
// hands out: sigOpLimitPreGenesisChain puts the block below it, sigOpLimitPostGenesisChain at it.
//
// Every block goes through the full BlockValidation path (ValidateBlock, non-optimistic) with the
// real subtree validation server from setup(): the block's transactions are unknown to the node,
// so their bodies are fetched as subtree data and validated, then Block.Valid runs. That is every
// place a block-level sigop accumulator could live, so the proofs do not presume where the fix goes.
// The rule is not in model.Block.Valid alone because Valid reads subtree hashes and meta, never
// transaction bodies.
const (
	// sigOpLimitPreGenesisChain is a GenesisActivationHeight far above the block's height.
	sigOpLimitPreGenesisChain = uint32(10_000)
	// sigOpLimitPostGenesisChain is a GenesisActivationHeight equal to the block's height.
	sigOpLimitPostGenesisChain = uint32(1)
	sigOpLimitBlockHeight      = uint32(1)

	// sigOpLimitPerStartedMegabyte is the pre-Genesis allowance per started 1,000,000 bytes.
	sigOpLimitPerStartedMegabyte = 20_000
	// sigOpLimitPerTransaction is the pre-Genesis per-transaction sigop limit.
	sigOpLimitPerTransaction = 20_000

	// sigOpLimitNoPadding builds the block from the sigop transactions only, at whatever size they
	// come to.
	sigOpLimitNoPadding = 0

	// sigOpLimitMaxPaddingChunk keeps each padding transaction well under the pre-Genesis
	// 1,000,000-byte per-transaction consensus size limit.
	sigOpLimitMaxPaddingChunk = 500_000
	sigOpLimitOutputValue     = uint64(1_000)

	sigOpLimitSubtreeDataPattern   = `=~^/subtree_data/[a-z0-9]+\z`
	sigOpLimitSubtreeDataResponder = "GET " + sigOpLimitSubtreeDataPattern
)

// sigOpTxSpec describes one non-coinbase transaction of the block.
//   - checkSigs: OP_CHECKSIGs in its single output script (counted the same by the inaccurate and
//     the accurate counter: 1 each).
//   - p2shRedeemCheckSigs: when > 0, the transaction spends a P2SH output whose redeem script is
//     OP_0 OP_IF <that many OP_CHECKSIGs> OP_ENDIF OP_TRUE (see sigOpLimitRedeemScript). The
//     scriptSig only pushes the redeem script, so the non-P2SH count of the input is 0 and only
//     P2SH counting sees these sigops.
type sigOpTxSpec struct {
	checkSigs           int
	p2shRedeemCheckSigs int
}

// sigOpLimitValidation is the fixture's result: the validation outcome and what the fixture
// independently measured about the block it validated.
type sigOpLimitValidation struct {
	err          error
	sizeInBytes  int
	sigOpsNoP2SH int
	sigOpsP2SH   int
	maxTxSigOps  int

	// subtreeDataFetches is how many times the transaction bodies were fetched from the peer, and
	// blockStored whether the block is in the blockchain store afterwards. The controls use them
	// to prove the bodies went through subtree validation and the block was accepted end to end.
	subtreeDataFetches int
	blockStored        bool
}

// sigOpLimitTwentyThousandOneSplitAcrossTwoTxs is 20,001 sigops in two transactions, neither over
// the per-transaction limit.
var sigOpLimitTwentyThousandOneSplitAcrossTwoTxs = []sigOpTxSpec{{checkSigs: 10_000}, {checkSigs: 10_001}}

// TestKnownDefect_PreGenesisBlockSigOpLimit_RejectsBlockOver20000SigOps proves a pre-Genesis block
// over the block sigop limit is accepted.
//
// Spec: BV-BODY-021 and AC-BV-BODY-021.1 (tracked as GAP-BV-048).
//
// Scenario: Given a test chain whose GenesisActivationHeight (10,000) is above the block's height
// (1), When a 900,000-byte block whose two transactions carry 10,000 and 10,001 sigops (20,001 in
// total, limit 20,000) is validated, Then it must be ERR_BLOCK_INVALID. Each transaction is within
// the per-transaction limit, so only the block total breaks the rule.
//
// Today: it is accepted, because no block-level sigop accumulator exists. The only pre-Genesis
// sigop check is the per-transaction one, model.TxCommonRuleViolation
// (model/block_coinbase_common_rules.go), which Block.Valid and quick validation apply to the
// coinbase only and validator.CheckTransactionContextFree to single transactions; nothing in
// model.Block.Valid, BlockValidation.ValidateBlock or subtree validation sums across transactions.
//
// Controls: the same 900,000-byte pre-Genesis block shape with exactly 20,000 sigops is accepted,
// proving the harness takes a block of this shape through the full validation path to acceptance;
// and the same 20,001-sigop block at GenesisActivationHeight is accepted, because the limit applies
// only below it. Both pass today and must keep passing after the fix.
func TestKnownDefect_PreGenesisBlockSigOpLimit_RejectsBlockOver20000SigOps(t *testing.T) {
	const (
		blockSizeInBytes = 900_000
		atLimitSigOps    = 20_000
		overLimitSigOps  = 20_001
	)

	t.Run("control: exactly 20,000 sigops in the same pre-Genesis block is accepted", func(t *testing.T) {
		// Given / When
		block := validateBlockWithSigOps(t, sigOpLimitPreGenesisChain,
			[]sigOpTxSpec{{checkSigs: 10_000}, {checkSigs: 10_000}}, blockSizeInBytes)

		// Then
		require.Equal(t, blockSizeInBytes, block.sizeInBytes)
		require.Equal(t, atLimitSigOps, block.sigOpsP2SH)

		requireSigOpLimitBlockAccepted(t, block)
	})

	t.Run("control: the same 20,001-sigop block at the Genesis activation height is accepted", func(t *testing.T) {
		// Given / When
		block := validateBlockWithSigOps(t, sigOpLimitPostGenesisChain,
			sigOpLimitTwentyThousandOneSplitAcrossTwoTxs, blockSizeInBytes)

		// Then
		require.Equal(t, overLimitSigOps, block.sigOpsP2SH)

		requireSigOpLimitBlockAccepted(t, block)
	})

	// Given a pre-Genesis chain, When a 900,000-byte block with 20,001 sigops is validated
	block := validateBlockWithSigOps(t, sigOpLimitPreGenesisChain,
		sigOpLimitTwentyThousandOneSplitAcrossTwoTxs, blockSizeInBytes)

	require.Equal(t, blockSizeInBytes, block.sizeInBytes)
	require.Equal(t, overLimitSigOps, block.sigOpsP2SH)
	require.LessOrEqual(t, block.maxTxSigOps, sigOpLimitPerTransaction, "every transaction is within the per-transaction limit")

	// Then it is invalid
	requireSigOpLimitBlockInvalid(t, block)
}

// TestKnownDefect_PreGenesisBlockSigOpLimit_ScalesPerStartedMegabyte proves the pre-Genesis block
// sigop limit's ceil scaling is not enforced.
//
// Spec: BV-BODY-021 and AC-BV-BODY-021.1 (tracked as GAP-BV-048).
//
// Scenario: the limit is 20,000 x ceil(size / 1,000,000), as bitcoin-sv computes it with
// ((size - 1) / 1,000,000 + 1). Given a pre-Genesis chain, When a block of 1,050,000 bytes
// (ceil = 2) is validated, Then it may carry 40,000 sigops but not 40,001; one of exactly 1,000,000
// bytes (ceil = 1) may not carry 20,001, which rules out a floor(size / 1 MB) + 1 formula (limit
// 40,000 there); one of 1,000,001 bytes (ceil = 2) may carry 40,000.
//
// Today: the invalid cases are accepted, for the reason given on
// TestKnownDefect_PreGenesisBlockSigOpLimit_RejectsBlockOver20000SigOps.
//
// Controls: the accepted cases pass today; the 40,000-sigop ones would fail under a flat 20,000
// limit, so a fix must scale.
func TestKnownDefect_PreGenesisBlockSigOpLimit_ScalesPerStartedMegabyte(t *testing.T) {
	const (
		justOverOneMegabyte    = 1_050_000
		exactlyOneMegabyte     = 1_000_000
		oneByteOverOneMegabyte = 1_000_001

		twoMegabyteLimitSigOps     = 40_000
		overTwoMegabyteLimitSigOps = 40_001
		overOneMegabyteLimitSigOps = 20_001
	)

	t.Run("control: 40,000 sigops in a block just over 1 MB is accepted", func(t *testing.T) {
		// Given / When
		block := validateBlockWithSigOps(t, sigOpLimitPreGenesisChain,
			[]sigOpTxSpec{{checkSigs: 20_000}, {checkSigs: 20_000}}, justOverOneMegabyte)

		// Then
		require.Equal(t, twoMegabyteLimitSigOps, block.sigOpsP2SH)
		requireSigOpLimitBlockAccepted(t, block)
	})

	t.Run("40,001 sigops in a block just over 1 MB is invalid", func(t *testing.T) {
		// Given / When
		block := validateBlockWithSigOps(t, sigOpLimitPreGenesisChain,
			[]sigOpTxSpec{{checkSigs: 20_000}, {checkSigs: 20_000}, {checkSigs: 1}}, justOverOneMegabyte)

		// Then
		require.Equal(t, overTwoMegabyteLimitSigOps, block.sigOpsP2SH)
		require.LessOrEqual(t, block.maxTxSigOps, sigOpLimitPerTransaction)
		requireSigOpLimitBlockInvalid(t, block)
	})

	t.Run("20,001 sigops in a block of exactly 1 MB is invalid", func(t *testing.T) {
		// Given / When
		block := validateBlockWithSigOps(t, sigOpLimitPreGenesisChain,
			sigOpLimitTwentyThousandOneSplitAcrossTwoTxs, exactlyOneMegabyte)

		// Then
		require.Equal(t, exactlyOneMegabyte, block.sizeInBytes)
		require.Equal(t, overOneMegabyteLimitSigOps, block.sigOpsP2SH)
		require.LessOrEqual(t, block.maxTxSigOps, sigOpLimitPerTransaction)
		requireSigOpLimitBlockInvalid(t, block)
	})

	t.Run("control: 40,000 sigops in a block of 1,000,001 bytes is accepted", func(t *testing.T) {
		// Given / When
		block := validateBlockWithSigOps(t, sigOpLimitPreGenesisChain,
			[]sigOpTxSpec{{checkSigs: 20_000}, {checkSigs: 20_000}}, oneByteOverOneMegabyte)

		// Then
		require.Equal(t, oneByteOverOneMegabyte, block.sizeInBytes)
		require.Equal(t, twoMegabyteLimitSigOps, block.sigOpsP2SH)
		requireSigOpLimitBlockAccepted(t, block)
	})
}

// TestKnownDefect_PreGenesisBlockSigOpLimit_CountsP2SHRedeemScriptSigOps proves the pre-Genesis
// block sigop limit does not count P2SH redeem-script sigops.
//
// Spec: BV-BODY-021 and AC-BV-BODY-021.1 (tracked as GAP-BV-048).
//
// Scenario: Given a pre-Genesis chain, When a block is validated whose output scripts carry 19,999
// sigops and one of whose transactions spends a P2SH output with a 2-OP_CHECKSIG redeem script (so
// 19,999 sigops without P2SH and 20,001 with it), Then it must be ERR_BLOCK_INVALID.
//
// Today: it is accepted (see TestKnownDefect_PreGenesisBlockSigOpLimit_RejectsBlockOver20000SigOps).
//
// Controls: the same block spending a P2SH output with a 1-OP_CHECKSIG redeem script (P2SH-counted
// total exactly 20,000) is accepted today and after the fix; it proves a block with a P2SH spend
// reaches acceptance on the block-level path. It does not prove script validity: setup() uses
// validator.MockValidator, which runs no scripts. The redeem scripts (sigOpLimitRedeemScript) are
// nonetheless script-valid, so the sigop total stays the block's only violation under a real
// validator too.
func TestKnownDefect_PreGenesisBlockSigOpLimit_CountsP2SHRedeemScriptSigOps(t *testing.T) {
	const (
		outputScriptSigOps = 19_999
		atLimitSigOps      = 20_000
		overLimitSigOps    = 20_001
	)

	t.Run("control: P2SH-counted total of exactly 20,000 is accepted", func(t *testing.T) {
		// Given / When
		block := validateBlockWithSigOps(t, sigOpLimitPreGenesisChain,
			[]sigOpTxSpec{{checkSigs: 10_000}, {checkSigs: 9_999}, {p2shRedeemCheckSigs: 1}}, sigOpLimitNoPadding)

		// Then
		require.Equal(t, outputScriptSigOps, block.sigOpsNoP2SH)
		require.Equal(t, atLimitSigOps, block.sigOpsP2SH)
		requireSigOpLimitBlockAccepted(t, block)
	})

	t.Run("P2SH redeem-script sigops tip the total over 20,000", func(t *testing.T) {
		// Given / When
		block := validateBlockWithSigOps(t, sigOpLimitPreGenesisChain,
			[]sigOpTxSpec{{checkSigs: 10_000}, {checkSigs: 9_999}, {p2shRedeemCheckSigs: 2}}, sigOpLimitNoPadding)

		// Then
		require.Equal(t, outputScriptSigOps, block.sigOpsNoP2SH, "without P2SH counting the block is under the limit")
		require.Equal(t, overLimitSigOps, block.sigOpsP2SH)
		requireSigOpLimitBlockInvalid(t, block)
	})
}

// requireSigOpLimitBlockAccepted is the passing side of every control: the block is valid end to
// end.
func requireSigOpLimitBlockAccepted(t *testing.T, block sigOpLimitValidation) {
	t.Helper()
	require.NoError(t, block.err, "control: this block must be accepted")
	require.Positive(t, block.subtreeDataFetches, "control: the transaction bodies must have been fetched and validated")
	require.True(t, block.blockStored, "control: the accepted block must be in the blockchain store")
}

// requireSigOpLimitBlockInvalid is the spec assertion: the over-limit block is ERR_BLOCK_INVALID.
// Today ValidateBlock returns nil for it.
func requireSigOpLimitBlockInvalid(t *testing.T, block sigOpLimitValidation) {
	t.Helper()

	startedMegabytes := (block.sizeInBytes + 999_999) / 1_000_000

	require.Error(t, block.err,
		"a pre-Genesis block of %d bytes with %d P2SH-counted sigops (limit %d) must be rejected, but was accepted",
		block.sizeInBytes, block.sigOpsP2SH, sigOpLimitPerStartedMegabyte*startedMegabytes)
	require.True(t, errors.Is(block.err, errors.ErrBlockInvalid), "an over-limit block must be ERR_BLOCK_INVALID, got: %v", block.err)
}

// validateBlockWithSigOps builds a block at height 1 holding txSpecs (plus enough zero-sigop
// padding transactions to make its serialised size exactly targetSize, when targetSize is not
// sigOpLimitNoPadding) and runs it through BlockValidation.ValidateBlock on a test chain with the
// given GenesisActivationHeight.
func validateBlockWithSigOps(t *testing.T, genesisActivationHeight uint32, txSpecs []sigOpTxSpec, targetSize int) sigOpLimitValidation {
	t.Helper()
	initPrometheusMetrics()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	configure := sigOpLimitSettings(genesisActivationHeight)

	utxoStore, subtreeValidationClient, _, txStore, subtreeStore, deferFunc := setup(t, configure)
	defer deferFunc()

	tSettings := test.CreateBaseTestSettings(t)
	configure(tSettings)

	blockChainStore, err := blockchain_store.NewStore(ulogger.TestLogger{}, &url.URL{Scheme: "sqlitememory"}, tSettings)
	require.NoError(t, err)
	blockchainClient, err := blockchain.NewLocalClient(ulogger.TestLogger{}, tSettings, blockChainStore, nil, nil)
	require.NoError(t, err)

	// Coinbase: height push and an OP_TRUE output, so it carries no sigops of its own.
	coinbase := bt.NewTx()
	require.NoError(t, coinbase.From("0000000000000000000000000000000000000000000000000000000000000000", 0xffffffff, "", 0))
	coinbase.Inputs[0].UnlockingScript = bscript.NewFromBytes(append(util.EncodeCoinbaseHeightPush(sigOpLimitBlockHeight), []byte("/sigop limit/")...))
	coinbase.AddOutput(&bt.Output{
		Satoshis:      util.GetBlockSubsidyForHeight(sigOpLimitBlockHeight, tSettings.ChainCfgParams),
		LockingScript: bscript.NewFromBytes([]byte{bscript.OpTRUE}),
	})

	// Padding transactions needed to reach targetSize (estimated from the sigop transactions'
	// size; the last one is trimmed to hit the target exactly below).
	paddingCount := 0

	if targetSize > sigOpLimitNoPadding {
		estimate := model.BlockHeaderSize + 3 + coinbase.Size()
		for _, spec := range txSpecs {
			estimate += 100 + spec.checkSigs + spec.p2shRedeemCheckSigs
		}

		require.Greater(t, targetSize, estimate, "fixture: target size below the unpadded block")
		paddingCount = (targetSize - estimate + sigOpLimitMaxPaddingChunk - 1) / sigOpLimitMaxPaddingChunk
	}

	// One parent, already mined (block ID 0), whose outputs the block's transactions spend.
	parent := bt.NewTx()
	require.NoError(t, parent.From("1111111111111111111111111111111111111111111111111111111111111111", 0, "51", 100_000_000))
	parent.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{bscript.OpTRUE})

	redeemScripts := make([][]byte, len(txSpecs))

	for i, spec := range txSpecs {
		lock := bscript.NewFromBytes([]byte{bscript.OpTRUE})

		if spec.p2shRedeemCheckSigs > 0 {
			redeemScripts[i] = sigOpLimitRedeemScript(spec.p2shRedeemCheckSigs)
			lock = sigOpLimitP2SHLockingScript(redeemScripts[i])
		}

		parent.AddOutput(&bt.Output{Satoshis: sigOpLimitOutputValue, LockingScript: lock})
	}

	for i := 0; i < paddingCount; i++ {
		parent.AddOutput(&bt.Output{Satoshis: sigOpLimitOutputValue, LockingScript: bscript.NewFromBytes([]byte{bscript.OpTRUE})})
	}

	_, _, err = utxoStore.SpendAndCreate(ctx, parent, 0, utxostore.WithMinedBlockInfo(utxostore.MinedBlockInfo{BlockID: 0, BlockHeight: 0}), utxostore.WithCreateOnly())
	require.NoError(t, err)

	spendParentOutput := func(vout int) *bt.Tx {
		tx := bt.NewTx()
		require.NoError(t, tx.FromUTXOs(&bt.UTXO{
			TxIDHash:      parent.TxIDChainHash(),
			Vout:          uint32(vout), //nolint:gosec
			LockingScript: parent.Outputs[vout].LockingScript,
			Satoshis:      sigOpLimitOutputValue,
		}))

		return tx
	}

	txs := make([]*bt.Tx, 0, len(txSpecs)+paddingCount)

	for i, spec := range txSpecs {
		tx := spendParentOutput(i)

		if spec.p2shRedeemCheckSigs > 0 {
			tx.Inputs[0].UnlockingScript = sigOpLimitPushScript(redeemScripts[i])
		} else {
			tx.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{bscript.OpTRUE})
		}

		tx.AddOutput(&bt.Output{Satoshis: sigOpLimitOutputValue, LockingScript: sigOpLimitCheckSigScript(spec.checkSigs)})
		txs = append(txs, tx)
	}

	if paddingCount > 0 {
		paddingTxs := make([]*bt.Tx, paddingCount)

		for i := range paddingTxs {
			paddingTxs[i] = spendParentOutput(len(txSpecs) + i)
			paddingTxs[i].Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{bscript.OpTRUE})
			paddingTxs[i].AddOutput(&bt.Output{Satoshis: sigOpLimitOutputValue, LockingScript: sigOpLimitPaddingScript(sigOpLimitMaxPaddingChunk)})
		}

		// Trim the last padding transaction until the block is exactly targetSize bytes. The
		// output script length varint does not change size across the trim (all >= 65,536).
		for range 4 {
			size := sigOpLimitWireSize(append(append([]*bt.Tx{coinbase}, txs...), paddingTxs...))
			if size == targetSize {
				break
			}

			last := paddingTxs[paddingCount-1]
			n := len(*last.Outputs[0].LockingScript) - 7 + (targetSize - size)
			require.Positive(t, n, "fixture: padding chunk went negative")
			last.Outputs[0].LockingScript = sigOpLimitPaddingScript(n)
		}

		txs = append(txs, paddingTxs...)
	}

	allTxs := append([]*bt.Tx{coinbase}, txs...)
	result := sigOpLimitValidation{sizeInBytes: sigOpLimitWireSize(allTxs)}

	if targetSize > sigOpLimitNoPadding {
		require.Equal(t, targetSize, result.sizeInBytes, "fixture: block size")
	}

	result.sigOpsNoP2SH, result.sigOpsP2SH, result.maxTxSigOps = sigOpLimitCountSigOps(allTxs)

	// One subtree: coinbase placeholder, then every transaction.
	leaves := 1
	for leaves < len(allTxs) {
		leaves *= 2
	}

	subtree, err := subtreepkg.NewTreeByLeafCount(leaves)
	require.NoError(t, err)
	require.NoError(t, subtree.AddCoinbaseNode())

	subtreeData := subtreepkg.NewSubtreeData(subtree)

	for i, tx := range txs {
		require.NoError(t, subtree.AddNode(*tx.TxIDChainHash(), 0, uint64(tx.Size()))) //nolint:gosec
		require.NoError(t, subtreeData.AddTx(tx, i+1))
	}

	nodeBytes, err := subtree.SerializeNodes()
	require.NoError(t, err)
	httpmock.RegisterResponder("GET", `=~^/subtree/[a-z0-9]+\z`, httpmock.NewBytesResponder(200, nodeBytes))

	subtreeDataBytes, err := subtreeData.Serialize()
	require.NoError(t, err)
	httpmock.RegisterResponder("GET", sigOpLimitSubtreeDataPattern, httpmock.NewBytesResponder(200, subtreeDataBytes))

	replicated := subtree.Duplicate()
	replicated.ReplaceRootNode(coinbase.TxIDChainHash(), 0, uint64(coinbase.Size())) //nolint:gosec

	nBits, err := model.NewNBitFromString("207fffff")
	require.NoError(t, err)

	header := &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  tSettings.ChainCfgParams.GenesisHash,
		HashMerkleRoot: replicated.RootHash(),
		Timestamp:      uint32(time.Now().Unix()), //nolint:gosec
		Bits:           *nBits,
	}

	for ok, _, _ := header.HasMetTargetDifficulty(); !ok; ok, _, _ = header.HasMetTargetDifficulty() {
		header.Nonce++
	}

	block, err := model.NewBlock(header, coinbase, []*chainhash.Hash{subtree.RootHash()},
		uint64(len(allTxs)), uint64(result.sizeInBytes), sigOpLimitBlockHeight, 0) //nolint:gosec
	require.NoError(t, err)

	blockValidation := NewBlockValidation(ctx, ulogger.TestLogger{}, tSettings, blockchainClient, subtreeStore, txStore, utxoStore, nil, subtreeValidationClient)
	result.err = blockValidation.ValidateBlock(ctx, block, "http://localhost:8000")
	result.subtreeDataFetches = httpmock.GetCallCountInfo()[sigOpLimitSubtreeDataResponder]

	result.blockStored, err = blockchainClient.GetBlockExists(ctx, block.Hash())
	require.NoError(t, err)

	return result
}

func sigOpLimitSettings(genesisActivationHeight uint32) func(*settings.Settings) {
	return func(s *settings.Settings) {
		s.ChainCfgParams.GenesisActivationHeight = genesisActivationHeight
		s.ChainCfgParams.Checkpoints = nil
		s.BlockValidation.OptimisticMining = false
	}
}

func sigOpLimitCheckSigScript(checkSigs int) *bscript.Script {
	return bscript.NewFromBytes(bytes.Repeat([]byte{bscript.OpCHECKSIG}, checkSigs))
}

// sigOpLimitRedeemScript is OP_0 OP_IF <checkSigs x OP_CHECKSIG> OP_ENDIF OP_TRUE. Sigop counting
// is static and counts the OP_CHECKSIGs in the unexecuted branch, so the P2SH-counted sigops are
// checkSigs; at run time the branch is skipped and the script leaves OP_TRUE, so the spend is
// script-valid even with an empty scriptSig stack. The block's only violation is therefore its
// sigop total.
func sigOpLimitRedeemScript(checkSigs int) []byte {
	script := []byte{bscript.OpFALSE, bscript.OpIF}
	script = append(script, bytes.Repeat([]byte{bscript.OpCHECKSIG}, checkSigs)...)

	return append(script, bscript.OpENDIF, bscript.OpTRUE)
}

func sigOpLimitP2SHLockingScript(redeemScript []byte) *bscript.Script {
	script := []byte{bscript.OpHASH160, bscript.OpDATA20}
	script = append(script, bsvutil.Hash160(redeemScript)...)
	script = append(script, bscript.OpEQUAL)

	return bscript.NewFromBytes(script)
}

// sigOpLimitPushScript is a push-only script pushing data (PUSHDATA2 for anything over 75 bytes).
func sigOpLimitPushScript(data []byte) *bscript.Script {
	script := &bscript.Script{}
	if err := script.AppendPushData(data); err != nil {
		panic(err)
	}

	return script
}

// sigOpLimitPaddingScript is OP_FALSE OP_RETURN PUSHDATA4 <n zero bytes>: pushed data carries no
// sigops.
func sigOpLimitPaddingScript(n int) *bscript.Script {
	script := make([]byte, 0, n+7)
	script = append(script, bscript.OpFALSE, bscript.OpRETURN, bscript.OpPUSHDATA4)
	script = binary.LittleEndian.AppendUint32(script, uint32(n)) //nolint:gosec // n is a test-controlled size
	script = append(script, make([]byte, n)...)

	return bscript.NewFromBytes(script)
}

// sigOpLimitWireSize is the consensus serialised size of a block with these transactions.
func sigOpLimitWireSize(txs []*bt.Tx) int {
	size := model.BlockHeaderSize + len(bt.VarInt(uint64(len(txs))).Bytes())
	for _, tx := range txs {
		size += tx.Size()
	}

	return size
}

// sigOpLimitCountSigOps counts the block's sigops with services/legacy/txscript as an independent
// oracle: the inaccurate count of every scriptSig and output script, plus, for P2SH, the accurate
// count of the redeem script of every input spending a P2SH output. maxTx is the largest
// P2SH-counted per-transaction count.
func sigOpLimitCountSigOps(txs []*bt.Tx) (noP2SH, withP2SH, maxTx int) {
	for _, tx := range txs {
		txCount := 0

		for _, in := range tx.Inputs {
			txCount += txscript.GetSigOpCount(*in.UnlockingScript, 0)
		}

		for _, out := range tx.Outputs {
			txCount += txscript.GetSigOpCount(*out.LockingScript, 0)
		}

		noP2SH += txCount

		if !tx.IsCoinbase() {
			for _, in := range tx.Inputs {
				if in.PreviousTxScript != nil && in.PreviousTxScript.IsP2SH() {
					txCount += txscript.GetPreciseSigOpCount(*in.UnlockingScript, *in.PreviousTxScript, txscript.ScriptBip16)
				}
			}
		}

		withP2SH += txCount

		if txCount > maxTx {
			maxTx = txCount
		}
	}

	return noP2SH, withP2SH, maxTx
}
