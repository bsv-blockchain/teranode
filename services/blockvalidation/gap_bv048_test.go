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

// GAP-BV-048 fixtures. BV-BODY-021: below GenesisActivationHeight the total sigops of a block,
// counted with P2SH, must be <= 20,000 x ceil(block size in bytes / 1,000,000), and a violation is
// ERR_BLOCK_INVALID.
//
// The block is built on regtest genesis, so it is at height 1. The test chain's
// GenesisActivationHeight is set on the private copy of the chain params CreateBaseTestSettings
// hands out: gap048PreGenesis puts the block below it, gap048PostGenesis at it.
//
// Every block goes through the full BlockValidation path (ValidateBlock, non-optimistic) with the
// real subtree validation server from setup(): the block's transactions are unknown to the node,
// so their bodies are fetched as subtree data and validated, then Block.Valid runs. That is every
// place a block-level sigop accumulator could live, so the proofs do not presume where the fix goes.
// The rule is not in model.Block.Valid alone because Valid reads subtree hashes and meta, never
// transaction bodies.
const (
	gap048PreGenesis  = uint32(10_000)
	gap048PostGenesis = uint32(1)
	gap048BlockHeight = uint32(1)

	// gap048MaxPadChunk keeps each padding transaction well under the pre-Genesis 1,000,000-byte
	// per-transaction consensus size limit.
	gap048MaxPadChunk = 500_000
	gap048OutputValue = uint64(1_000)

	gap048SubtreeDataPattern   = `=~^/subtree_data/[a-z0-9]+\z`
	gap048SubtreeDataResponder = "GET " + gap048SubtreeDataPattern
)

// gap048Tx describes one non-coinbase transaction of the block.
//   - checkSigs: OP_CHECKSIGs in its single output script (counted the same by the inaccurate and
//     the accurate counter: 1 each).
//   - p2shRedeemCheckSigs: when > 0, the transaction spends a P2SH output whose redeem script is
//     OP_0 OP_IF <that many OP_CHECKSIGs> OP_ENDIF OP_TRUE (see gap048RedeemScript). The scriptSig
//     only pushes the redeem script, so the non-P2SH count of the input is 0 and only P2SH counting
//     sees these sigops.
type gap048Tx struct {
	checkSigs           int
	p2shRedeemCheckSigs int
}

// gap048Block is the fixture's result: the validation outcome and what the fixture independently
// measured about the block it validated.
type gap048Block struct {
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

func gap048Configure(genesisActivationHeight uint32) func(*settings.Settings) {
	return func(s *settings.Settings) {
		s.ChainCfgParams.GenesisActivationHeight = genesisActivationHeight
		s.ChainCfgParams.Checkpoints = nil
		s.BlockValidation.OptimisticMining = false
	}
}

func gap048CheckSigScript(n int) *bscript.Script {
	return bscript.NewFromBytes(bytes.Repeat([]byte{bscript.OpCHECKSIG}, n))
}

// gap048RedeemScript is OP_0 OP_IF <n x OP_CHECKSIG> OP_ENDIF OP_TRUE. Sigop counting is static
// and counts the OP_CHECKSIGs in the unexecuted branch, so the P2SH-counted sigops are n; at run
// time the branch is skipped and the script leaves OP_TRUE, so the spend is script-valid even with
// an empty scriptSig stack. The block's only violation is therefore its sigop total.
func gap048RedeemScript(n int) []byte {
	s := []byte{bscript.OpFALSE, bscript.OpIF}
	s = append(s, bytes.Repeat([]byte{bscript.OpCHECKSIG}, n)...)

	return append(s, bscript.OpENDIF, bscript.OpTRUE)
}

func gap048P2SHLockingScript(redeem []byte) *bscript.Script {
	s := []byte{bscript.OpHASH160, bscript.OpDATA20}
	s = append(s, bsvutil.Hash160(redeem)...)
	s = append(s, bscript.OpEQUAL)

	return bscript.NewFromBytes(s)
}

// gap048PushScript is a push-only script pushing data (PUSHDATA2 for anything over 75 bytes).
func gap048PushScript(data []byte) *bscript.Script {
	s := &bscript.Script{}
	if err := s.AppendPushData(data); err != nil {
		panic(err)
	}

	return s
}

// gap048PadScript is OP_FALSE OP_RETURN PUSHDATA4 <n zero bytes>: pushed data carries no sigops.
func gap048PadScript(n int) *bscript.Script {
	s := make([]byte, 0, n+7)
	s = append(s, bscript.OpFALSE, bscript.OpRETURN, bscript.OpPUSHDATA4)
	s = binary.LittleEndian.AppendUint32(s, uint32(n)) //nolint:gosec // n is a test-controlled size
	s = append(s, make([]byte, n)...)

	return bscript.NewFromBytes(s)
}

// gap048WireSize is the consensus serialised size of a block with these transactions.
func gap048WireSize(txs []*bt.Tx) int {
	size := model.BlockHeaderSize + len(bt.VarInt(uint64(len(txs))).Bytes())
	for _, tx := range txs {
		size += tx.Size()
	}

	return size
}

// gap048Count counts the block's sigops with services/legacy/txscript as an independent oracle:
// the inaccurate count of every scriptSig and output script, plus, for P2SH, the accurate count of
// the redeem script of every input spending a P2SH output. maxTx is the largest P2SH-counted
// per-transaction count.
func gap048Count(txs []*bt.Tx) (noP2SH, withP2SH, maxTx int) {
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

// validateGap048Block builds a block at height 1 holding specs (plus enough zero-sigop padding
// transactions to make its serialised size exactly targetSize, when targetSize > 0) and runs it
// through BlockValidation.ValidateBlock on a test chain with the given GenesisActivationHeight.
func validateGap048Block(t *testing.T, genesisActivationHeight uint32, specs []gap048Tx, targetSize int) gap048Block {
	t.Helper()
	initPrometheusMetrics()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	configure := gap048Configure(genesisActivationHeight)

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
	coinbase.Inputs[0].UnlockingScript = bscript.NewFromBytes(append(util.EncodeCoinbaseHeightPush(gap048BlockHeight), []byte("/gap048/")...))
	coinbase.AddOutput(&bt.Output{
		Satoshis:      util.GetBlockSubsidyForHeight(gap048BlockHeight, tSettings.ChainCfgParams),
		LockingScript: bscript.NewFromBytes([]byte{bscript.OpTRUE}),
	})

	// Padding transactions needed to reach targetSize (estimated from the sigop transactions'
	// size; the last one is trimmed to hit the target exactly below).
	padCount := 0

	if targetSize > 0 {
		estimate := model.BlockHeaderSize + 3 + coinbase.Size()
		for _, s := range specs {
			estimate += 100 + s.checkSigs + s.p2shRedeemCheckSigs
		}

		require.Greater(t, targetSize, estimate, "fixture: target size below the unpadded block")
		padCount = (targetSize - estimate + gap048MaxPadChunk - 1) / gap048MaxPadChunk
	}

	// One parent, already mined (block ID 0), whose outputs the block's transactions spend.
	parent := bt.NewTx()
	require.NoError(t, parent.From("1111111111111111111111111111111111111111111111111111111111111111", 0, "51", 100_000_000))
	parent.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{bscript.OpTRUE})

	redeems := make([][]byte, len(specs))

	for i, s := range specs {
		lock := bscript.NewFromBytes([]byte{bscript.OpTRUE})

		if s.p2shRedeemCheckSigs > 0 {
			redeems[i] = gap048RedeemScript(s.p2shRedeemCheckSigs)
			lock = gap048P2SHLockingScript(redeems[i])
		}

		parent.AddOutput(&bt.Output{Satoshis: gap048OutputValue, LockingScript: lock})
	}

	for i := 0; i < padCount; i++ {
		parent.AddOutput(&bt.Output{Satoshis: gap048OutputValue, LockingScript: bscript.NewFromBytes([]byte{bscript.OpTRUE})})
	}

	_, _, err = utxoStore.SpendAndCreate(ctx, parent, 0, utxostore.WithMinedBlockInfo(utxostore.MinedBlockInfo{BlockID: 0, BlockHeight: 0}), utxostore.WithCreateOnly())
	require.NoError(t, err)

	spend := func(vout int) *bt.Tx {
		tx := bt.NewTx()
		require.NoError(t, tx.FromUTXOs(&bt.UTXO{
			TxIDHash:      parent.TxIDChainHash(),
			Vout:          uint32(vout), //nolint:gosec
			LockingScript: parent.Outputs[vout].LockingScript,
			Satoshis:      gap048OutputValue,
		}))

		return tx
	}

	txs := make([]*bt.Tx, 0, len(specs)+padCount)

	for i, s := range specs {
		tx := spend(i)

		if s.p2shRedeemCheckSigs > 0 {
			tx.Inputs[0].UnlockingScript = gap048PushScript(redeems[i])
		} else {
			tx.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{bscript.OpTRUE})
		}

		tx.AddOutput(&bt.Output{Satoshis: gap048OutputValue, LockingScript: gap048CheckSigScript(s.checkSigs)})
		txs = append(txs, tx)
	}

	if padCount > 0 {
		pads := make([]*bt.Tx, padCount)

		for i := range pads {
			pads[i] = spend(len(specs) + i)
			pads[i].Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{bscript.OpTRUE})
			pads[i].AddOutput(&bt.Output{Satoshis: gap048OutputValue, LockingScript: gap048PadScript(gap048MaxPadChunk)})
		}

		// Trim the last padding transaction until the block is exactly targetSize bytes. The
		// output script length varint does not change size across the trim (all >= 65,536).
		for range 4 {
			size := gap048WireSize(append(append([]*bt.Tx{coinbase}, txs...), pads...))
			if size == targetSize {
				break
			}

			last := pads[padCount-1]
			n := len(*last.Outputs[0].LockingScript) - 7 + (targetSize - size)
			require.Positive(t, n, "fixture: padding chunk went negative")
			last.Outputs[0].LockingScript = gap048PadScript(n)
		}

		txs = append(txs, pads...)
	}

	all := append([]*bt.Tx{coinbase}, txs...)
	result := gap048Block{sizeInBytes: gap048WireSize(all)}

	if targetSize > 0 {
		require.Equal(t, targetSize, result.sizeInBytes, "fixture: block size")
	}

	result.sigOpsNoP2SH, result.sigOpsP2SH, result.maxTxSigOps = gap048Count(all)

	// One subtree: coinbase placeholder, then every transaction.
	leaves := 1
	for leaves < len(all) {
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
	httpmock.RegisterResponder("GET", gap048SubtreeDataPattern, httpmock.NewBytesResponder(200, subtreeDataBytes))

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
		uint64(len(all)), uint64(result.sizeInBytes), gap048BlockHeight, 0) //nolint:gosec
	require.NoError(t, err)

	bv := NewBlockValidation(ctx, ulogger.TestLogger{}, tSettings, blockchainClient, subtreeStore, txStore, utxoStore, nil, subtreeValidationClient)
	result.err = bv.ValidateBlock(ctx, block, "http://localhost:8000")
	result.subtreeDataFetches = httpmock.GetCallCountInfo()[gap048SubtreeDataResponder]

	result.blockStored, err = blockchainClient.GetBlockExists(ctx, block.Hash())
	require.NoError(t, err)

	return result
}

// requireGap048Accepted is the passing side of every control: the block is valid end to end.
func requireGap048Accepted(t *testing.T, b gap048Block) {
	t.Helper()
	require.NoError(t, b.err, "control: this block must be accepted")
	require.Positive(t, b.subtreeDataFetches, "control: the transaction bodies must have been fetched and validated")
	require.True(t, b.blockStored, "control: the accepted block must be in the blockchain store")
}

// requireGap048Invalid is the spec assertion of BV-BODY-021: the over-limit block is
// ERR_BLOCK_INVALID. Today ValidateBlock returns nil for it.
func requireGap048Invalid(t *testing.T, b gap048Block) {
	t.Helper()
	require.Error(t, b.err,
		"GAP-BV-048 (BV-BODY-021): pre-Genesis block of %d bytes with %d P2SH-counted sigops (limit %d) was accepted",
		b.sizeInBytes, b.sigOpsP2SH, 20_000*((b.sizeInBytes+999_999)/1_000_000))
	require.True(t, errors.Is(b.err, errors.ErrBlockInvalid), "BV-BODY-021: must be ERR_BLOCK_INVALID, got: %v", b.err)
}

// gap048Split20001 is 20,001 sigops in two transactions, neither over the per-transaction limit.
var gap048Split20001 = []gap048Tx{{checkSigs: 10_000}, {checkSigs: 10_001}}

// TestGapBV048_PreGenesisBlockOver20000SigOpsIsInvalid asserts AC-BV-BODY-021.1 (BV-BODY-021).
//
// Spec: below GenesisActivationHeight a block's sigops, counted with P2SH, must be at most
// 20,000 x ceil(size / 1,000,000); a 900,000-byte block whose transactions total 20,001 sigops
// must be ERR_BLOCK_INVALID. Today it is accepted: no block-level sigop accumulator exists. The
// only pre-Genesis sigop check is the per-transaction one, model.TxCommonRuleViolation
// (model/block_coinbase_common_rules.go), which Block.Valid and quick validation apply to the
// coinbase only and validator.CheckTransactionContextFree to single transactions; nothing in
// model.Block.Valid, BlockValidation.ValidateBlock or subtree validation sums across transactions.
// Each transaction here carries at most 10,001 sigops, so only the block total breaks the rule.
func TestGapBV048_PreGenesisBlockOver20000SigOpsIsInvalid(t *testing.T) {
	b := validateGap048Block(t, gap048PreGenesis, gap048Split20001, 900_000)

	require.Equal(t, 900_000, b.sizeInBytes)
	require.Equal(t, 20_001, b.sigOpsP2SH)
	require.LessOrEqual(t, b.maxTxSigOps, 20_000, "every transaction is within the per-transaction limit")

	requireGap048Invalid(t, b)
}

// TestGapBV048_Control_PreGenesisBlockAt20000SigOpsIsAccepted is the boundary control for
// AC-BV-BODY-021.1 (BV-BODY-021): the same 900,000-byte pre-Genesis block shape with exactly
// 20,000 sigops is valid. Passes today and must keep passing after the fix; it proves the harness
// takes a block of this shape through the full validation path to acceptance.
func TestGapBV048_Control_PreGenesisBlockAt20000SigOpsIsAccepted(t *testing.T) {
	b := validateGap048Block(t, gap048PreGenesis, []gap048Tx{{checkSigs: 10_000}, {checkSigs: 10_000}}, 900_000)

	require.Equal(t, 900_000, b.sizeInBytes)
	require.Equal(t, 20_000, b.sigOpsP2SH)

	requireGap048Accepted(t, b)
}

// TestGapBV048_Control_PostGenesisBlockOver20000SigOpsIsAccepted is the Genesis-boundary control
// for BV-BODY-021: the limit applies only below GenesisActivationHeight, so the same 20,001-sigop
// 900,000-byte block at GenesisActivationHeight is not rejected on this rule. Passes today and
// must keep passing after the fix.
func TestGapBV048_Control_PostGenesisBlockOver20000SigOpsIsAccepted(t *testing.T) {
	b := validateGap048Block(t, gap048PostGenesis, gap048Split20001, 900_000)

	require.Equal(t, 20_001, b.sigOpsP2SH)

	requireGap048Accepted(t, b)
}

// TestGapBV048_PreGenesisLimitScalesPerStartedMegabyte asserts BV-BODY-021's ceil scaling
// (AC-BV-BODY-021.1): the limit is 20,000 x ceil(size / 1,000,000), as bitcoin-sv computes it with
// ((size - 1) / 1,000,000 + 1). A pre-Genesis block of 1,050,000 bytes (ceil = 2) may carry 40,000
// sigops but not 40,001; one of exactly 1,000,000 bytes (ceil = 1) may not carry 20,001, which
// rules out a floor(size / 1 MB) + 1 formula (limit 40,000 there); one of 1,000,001 bytes
// (ceil = 2) may carry 40,000. The accepted cases are controls (pass today; the 40,000 ones would
// fail under a flat 20,000 limit, so a fix must scale). The invalid cases fail today for the
// reason given on TestGapBV048_PreGenesisBlockOver20000SigOpsIsInvalid.
func TestGapBV048_PreGenesisLimitScalesPerStartedMegabyte(t *testing.T) {
	const size = 1_050_000

	t.Run("control: 40,000 sigops in a block just over 1 MB is accepted", func(t *testing.T) {
		b := validateGap048Block(t, gap048PreGenesis,
			[]gap048Tx{{checkSigs: 20_000}, {checkSigs: 20_000}}, size)

		require.Equal(t, 40_000, b.sigOpsP2SH)
		requireGap048Accepted(t, b)
	})

	t.Run("40,001 sigops in a block just over 1 MB is invalid", func(t *testing.T) {
		b := validateGap048Block(t, gap048PreGenesis,
			[]gap048Tx{{checkSigs: 20_000}, {checkSigs: 20_000}, {checkSigs: 1}}, size)

		require.Equal(t, 40_001, b.sigOpsP2SH)
		require.LessOrEqual(t, b.maxTxSigOps, 20_000)
		requireGap048Invalid(t, b)
	})

	t.Run("20,001 sigops in a block of exactly 1 MB is invalid", func(t *testing.T) {
		b := validateGap048Block(t, gap048PreGenesis, gap048Split20001, 1_000_000)

		require.Equal(t, 1_000_000, b.sizeInBytes)
		require.Equal(t, 20_001, b.sigOpsP2SH)
		require.LessOrEqual(t, b.maxTxSigOps, 20_000)
		requireGap048Invalid(t, b)
	})

	t.Run("control: 40,000 sigops in a block of 1,000,001 bytes is accepted", func(t *testing.T) {
		b := validateGap048Block(t, gap048PreGenesis,
			[]gap048Tx{{checkSigs: 20_000}, {checkSigs: 20_000}}, 1_000_001)

		require.Equal(t, 1_000_001, b.sizeInBytes)
		require.Equal(t, 40_000, b.sigOpsP2SH)
		requireGap048Accepted(t, b)
	})
}

// TestGapBV048_PreGenesisP2SHSigOpsCount asserts BV-BODY-021's "counted with P2SH": the block's
// output scripts carry 19,999 sigops, and one transaction spends a P2SH output whose redeem
// script holds 2 OP_CHECKSIGs, so the count without P2SH is 19,999 and with P2SH 20,001. The
// block must be ERR_BLOCK_INVALID; it is accepted today (see
// TestGapBV048_PreGenesisBlockOver20000SigOpsIsInvalid). The control spends a P2SH output with
// a 1-CHECKSIG redeem script (P2SH-counted total exactly 20,000), which is valid today and after
// the fix; it proves a block with a P2SH spend reaches acceptance on the block-level path. It does
// not prove script validity: setup() uses validator.MockValidator, which runs no scripts. The
// redeem scripts (gap048RedeemScript) are nonetheless script-valid, so the sigop total stays the
// block's only violation under a real validator too.
func TestGapBV048_PreGenesisP2SHSigOpsCount(t *testing.T) {
	t.Run("control: P2SH-counted total of exactly 20,000 is accepted", func(t *testing.T) {
		b := validateGap048Block(t, gap048PreGenesis,
			[]gap048Tx{{checkSigs: 10_000}, {checkSigs: 9_999}, {p2shRedeemCheckSigs: 1}}, 0)

		require.Equal(t, 19_999, b.sigOpsNoP2SH)
		require.Equal(t, 20_000, b.sigOpsP2SH)
		requireGap048Accepted(t, b)
	})

	t.Run("P2SH redeem-script sigops tip the total over 20,000", func(t *testing.T) {
		b := validateGap048Block(t, gap048PreGenesis,
			[]gap048Tx{{checkSigs: 10_000}, {checkSigs: 9_999}, {p2shRedeemCheckSigs: 2}}, 0)

		require.Equal(t, 19_999, b.sigOpsNoP2SH, "without P2SH counting the block is under the limit")
		require.Equal(t, 20_001, b.sigOpsP2SH)
		requireGap048Invalid(t, b)
	})
}
