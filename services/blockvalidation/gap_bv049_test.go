//go:build bvgaps

package blockvalidation

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	bec "github.com/bsv-blockchain/go-sdk/primitives/ec"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockassembly/subtreeprocessor"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/settings"
	utxostore "github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// GAP-BV-049 (BV-BODY-022): BIP30 and coinbase-collision handling.
//
// BV-BODY-022: "Below BIP0034Height, except at blocks 91842 and 91880, a block MUST NOT contain a
// transaction whose ID equals that of a transaction in an ancestor with unspent outputs (BIP30). At
// any height, a coinbase whose ID collides with an earlier coinbase MUST NOT leave the earlier
// coinbase's unspent outputs in the UTXO set."
//
// These proofs cover only the cases on which bitcoin-sv and Teranode should agree. Re-creating a FULLY
// SPENT transaction is deliberately not tested: the utxoset store refuses it where bitcoin-sv allows
// it, and that divergence is an open question in the spec.
//
// The harness is the node's own pipeline for a block on the tip, on real stores: block validation
// (ValidateBlock on newNoPersistHarness: sqlitememory blockchain and UTXO stores), then block
// assembly moving the accepted block forward (SubtreeProcessor.MoveForwardBlock on the same stores),
// which is what creates a coinbase's UTXOs. The colliding transaction is a coinbase, the historical
// BIP30 case: two byte-identical coinbases have the same txid, and below BIP0034Height nothing forces
// the block height into the coinbase. Every block is coinbase-only, so its body is bound by the
// coinbase-txid merkle rule and subtree validation is never reached.

const (
	// gap049BIP34Height is above every height the BIP30 cases use, including the faked 91841/91843.
	gap049BIP34Height = 1_000_000

	// gap049CoinbaseMaturity is CreateBaseTestSettings' coinbase maturity, which the UTXO store of
	// setup() (behind newNoPersistHarness) is built with: block 1's coinbase matures at height 2,
	// block 3's at height 4, so a spend at height 2 or 3 tells the two apart.
	gap049CoinbaseMaturity = 1
)

// gap049Settings is the private settings copy every GAP-BV-049 test runs on: no checkpoints, BIP34
// activated at bip34Height and a full 50 BTC subsidy at the fixture heights. CreateBaseTestSettings
// hands out its own copy of the chain parameters, so the overrides do not leak into other tests.
func gap049Settings(t *testing.T, bip34Height uint32) *settings.Settings {
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.BlockValidation.OptimisticMining = false
	tSettings.ChainCfgParams.Checkpoints = nil
	tSettings.ChainCfgParams.BIP0034Height = int32(bip34Height) //nolint:gosec
	// Regtest halves every 150 blocks, which leaves no subsidy at the faked 9184x heights.
	tSettings.ChainCfgParams.SubsidyReductionInterval = 210_000
	require.Equal(t, uint16(gap049CoinbaseMaturity), tSettings.ChainCfgParams.CoinbaseMaturity, "fixture: the maturity the UTXO store is built with")

	return tSettings
}

// gap049Harness is block validation and block assembly on one pair of real stores.
type gap049Harness struct {
	ctx       context.Context
	t         *testing.T
	settings  *settings.Settings
	bv        *BlockValidation
	client    blockchain.ClientI
	utxoStore utxostore.Store
	stp       *subtreeprocessor.SubtreeProcessor
	baseTime  uint32
}

func newGap049Harness(ctx context.Context, t *testing.T, tSettings *settings.Settings) *gap049Harness {
	t.Helper()

	initPrometheusMetrics()

	bv, client := newNoPersistHarness(ctx, t, tSettings)

	newSubtreeChan := make(chan subtreeprocessor.NewSubtreeRequest, 10)

	stp, err := subtreeprocessor.NewSubtreeProcessor(ctx, ulogger.TestLogger{}, tSettings, bv.subtreeStore, client, bv.utxoStore, newSubtreeChan)
	require.NoError(t, err)

	stp.Start(ctx)

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case req := <-newSubtreeChan:
				if req.ErrChan != nil {
					req.ErrChan <- nil
				}
			}
		}
	}()

	genesis, _, err := client.GetBlockHeader(ctx, tSettings.ChainCfgParams.GenesisHash)
	require.NoError(t, err)

	stp.SetCurrentBlockHeader(genesis)

	return &gap049Harness{
		ctx:       ctx,
		t:         t,
		settings:  tSettings,
		bv:        bv,
		client:    client,
		utxoStore: bv.utxoStore,
		stp:       stp,
		baseTime:  uint32(time.Now().Add(-time.Hour).Unix()), //nolint:gosec
	}
}

// coinbase builds a coinbase paying 50 BTC to a fresh key, so no two calls collide by accident.
// scriptSigPrefix is put in front of the miner tag; pass a BIP34 height push to make the coinbase
// valid at that height once BIP34 is active.
func (h *gap049Harness) coinbase(scriptSigPrefix []byte) *bt.Tx {
	h.t.Helper()

	address := gap049Address(h.t)

	scriptSig := append(append([]byte{}, scriptSigPrefix...), []byte("/gap-bv-049/")...)

	coinbaseTx := bt.NewTx()
	require.NoError(h.t, coinbaseTx.From("0000000000000000000000000000000000000000000000000000000000000000", 0xffffffff, "", 0))
	coinbaseTx.Inputs[0].UnlockingScript = bscript.NewFromBytes(scriptSig)
	require.NoError(h.t, coinbaseTx.AddP2PKHOutputFromAddress(address, 50*100000000))
	require.True(h.t, coinbaseTx.IsCoinbase())

	return coinbaseTx
}

// block mines a coinbase-only block on prev carrying coinbaseTx, at the given height. ValidateBlock
// takes block.Height as already settled against the parent (Server.deriveBlockHeight on the peer
// route, see the comment ahead of the checkpoint check in BlockValidation.go ValidateBlock), so the
// faked exemption heights below are the heights the BIP30 rule sees.
func (h *gap049Harness) block(prev *chainhash.Hash, coinbaseTx *bt.Tx, height uint32) *model.Block {
	h.t.Helper()

	timestamp := h.baseTime + height%1000

	bits, err := h.client.GetNextWorkRequired(h.ctx, prev, int64(timestamp))
	require.NoError(h.t, err)

	hdr := minedHeaderWithBits(h.t, prev, coinbaseTx.TxIDChainHash(), *bits, timestamp)

	block, err := model.NewBlock(hdr, coinbaseTx, []*chainhash.Hash{}, 1, uint64(coinbaseTx.Size()), height, 0) //nolint:gosec
	require.NoError(h.t, err)

	return block
}

func (h *gap049Harness) validate(block *model.Block) error {
	return h.bv.ValidateBlockWithOptions(h.ctx, block, "http://localhost", &ValidateBlockOptions{PeerID: "peer-serving"})
}

// accept validates block, requires it to be accepted, and moves block assembly forward onto it, which
// creates its coinbase UTXOs the way a running node does.
func (h *gap049Harness) accept(block *model.Block) {
	h.t.Helper()

	require.NoError(h.t, h.validate(block), "fixture: block %s at height %d must be accepted", block.Hash(), block.Height)

	// The step a running node takes on the block-mined notification: mark the block's transactions
	// mined and the block mined_set, so a child does not wait on its parent (waitForPreviousBlocksToBeProcessed).
	require.NoError(h.t, h.bv.setTxMinedStatus(h.ctx, block.Hash()))

	h.moveForward(block)
}

func (h *gap049Harness) moveForward(block *model.Block) {
	h.t.Helper()

	_, meta, err := h.client.GetBlockHeader(h.ctx, block.Hash())
	require.NoError(h.t, err)

	block.ID = meta.ID

	require.NoError(h.t, h.stp.MoveForwardBlock(block))
}

// requireAccepted asserts block passed validation and is stored valid.
func (h *gap049Harness) requireAccepted(block *model.Block, err error, msgAndArgs ...interface{}) {
	h.t.Helper()

	require.NoError(h.t, err, msgAndArgs...)

	_, meta, err := h.client.GetBlockHeader(h.ctx, block.Hash())
	require.NoError(h.t, err)
	require.False(h.t, meta.Invalid, msgAndArgs...)
}

// requireUnspentCoinbase asserts the coinbase's first output is in the UTXO store and unspent: the
// "ancestor with unspent outputs" precondition of the BIP30 rule.
func (h *gap049Harness) requireUnspentCoinbase(coinbaseTx *bt.Tx) {
	h.t.Helper()

	utxoHash, err := util.UTXOHashFromOutput(coinbaseTx.TxIDChainHash(), coinbaseTx.Outputs[0], 0)
	require.NoError(h.t, err)

	res, err := h.utxoStore.GetSpend(h.ctx, &utxostore.Spend{TxID: coinbaseTx.TxIDChainHash(), Vout: 0, UTXOHash: utxoHash})
	require.NoError(h.t, err)
	// An unspent coinbase output reads OK or IMMATURE, depending on the store's current height.
	require.Contains(h.t, []int{int(utxostore.Status_OK), int(utxostore.Status_IMMATURE)}, res.Status,
		"precondition: the coinbase's output must be in the UTXO set and unspent, got status %d", res.Status)
	require.Nil(h.t, res.SpendingData, "precondition: the coinbase's output must be unspent")
}

// spendAt spends output 0 of coinbaseTx at blockHeight and returns the store's verdict.
func (h *gap049Harness) spendAt(coinbaseTx *bt.Tx, blockHeight uint32) error {
	h.t.Helper()

	spendTx := bt.NewTx()
	require.NoError(h.t, spendTx.From(coinbaseTx.TxIDChainHash().String(), 0, coinbaseTx.Outputs[0].LockingScript.String(), coinbaseTx.Outputs[0].Satoshis))
	require.NoError(h.t, spendTx.AddP2PKHOutputFromAddress(gap049Address(h.t), coinbaseTx.Outputs[0].Satoshis-1000))

	_, _, err := h.utxoStore.SpendAndCreate(h.ctx, spendTx, blockHeight, utxostore.WithSpendOnly())

	return err
}

func gap049Address(t *testing.T) string {
	t.Helper()

	privateKey, err := bec.NewPrivateKey()
	require.NoError(t, err)

	address, err := bscript.NewAddressFromPublicKey(privateKey.PubKey(), true)
	require.NoError(t, err)

	return address.AddressString
}

// TestGapBV049_DuplicateOfUnspentAncestorBelowBIP34IsRejected covers GAP-BV-049 (BV-BODY-022, BIP30).
//
// Spec-correct behaviour: below BIP0034Height, and outside the exempt blocks 91842 and 91880, a block
// containing a transaction whose ID equals that of a transaction in an ancestor that still has
// unspent outputs is invalid: ERR_BLOCK_INVALID, and the block is not accepted.
//
// Expected to fail today: no BIP30 check exists anywhere on the block path (model.Block.Valid and
// BlockValidation.ValidateBlock check duplicate txids only within one block, model/Block.go
// ~1706/1738), so the block is accepted. Where the colliding transaction is created, the collision is
// swallowed: block assembly's SubtreeProcessor.processCoinbaseUtxos logs "already exist. Skipping"
// on ErrTxExists, quick validation's applyOneByOne calls collectExisting, and subtree validation's
// check_block_subtrees.go treats TX_EXISTS as already validated.
//
// The duplicate is a byte-identical coinbase of block 1, whose output block assembly created and which
// is still unspent. Height 2 is the honest chain. 91841 and 91843 sit either side of the first
// exemption height (block.Height faked as settled; see gap049Harness.block), so the exemption must not
// reach past block 91842 itself.
//
// The exemption itself (blocks 91842 and 91880 accept the duplicate) is deliberately not tested here.
// bitcoin-sv exempts the two historic blocks by height AND hash (validation.cpp ConnectBlock,
// fEnforceBIP30: 00000000000a4d0a...0caec and 00000000000743f1...cd721), which a test with a faked
// height cannot match, so a height-only exemption test would fail a bitcoin-sv-faithful fix. Whether
// BV-BODY-022's "except at blocks 91842 and 91880" means the heights or the two named blocks is an
// open question for the spec; the subtests here pass under either reading once the gap is fixed.
func TestGapBV049_DuplicateOfUnspentAncestorBelowBIP34IsRejected(t *testing.T) {
	for _, height := range []uint32{2, 91841, 91843} {
		t.Run(fmt.Sprintf("height_%d", height), func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			h := newGap049Harness(ctx, t, gap049Settings(t, gap049BIP34Height))

			coinbase1 := h.coinbase([]byte{0x01, 0x01})
			block1 := h.block(h.settings.ChainCfgParams.GenesisHash, coinbase1, 1)
			h.accept(block1)
			h.requireUnspentCoinbase(coinbase1)

			duplicate := h.block(block1.Hash(), coinbase1, height)
			require.Equal(t, *coinbase1.TxIDChainHash(), *duplicate.CoinbaseTx.TxIDChainHash(), "fixture: the block must repeat block 1's txid")
			require.Less(t, height, uint32(gap049BIP34Height), "fixture: the height must be below BIP0034Height")

			err := h.validate(duplicate)

			require.Error(t, err, "GAP-BV-049: a block at height %d repeating the txid of an ancestor transaction with unspent outputs was accepted (BIP30)", height)
			require.True(t, errors.Is(err, errors.ErrBlockInvalid), "GAP-BV-049: expected ERR_BLOCK_INVALID for a BIP30 violation, got: %v", err)
		})
	}
}

// TestGapBV049_Control_SameShapeWithoutDuplicateIsAccepted is the control for the test above
// (BV-BODY-022): the same harness, chain and heights with a fresh coinbase instead of the duplicate.
// The block is accepted, so the failure above is the missing BIP30 check, not the fixture.
func TestGapBV049_Control_SameShapeWithoutDuplicateIsAccepted(t *testing.T) {
	for _, height := range []uint32{2, 91841, 91843} {
		t.Run(fmt.Sprintf("height_%d", height), func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			h := newGap049Harness(ctx, t, gap049Settings(t, gap049BIP34Height))

			coinbase1 := h.coinbase([]byte{0x01, 0x01})
			block1 := h.block(h.settings.ChainCfgParams.GenesisHash, coinbase1, 1)
			h.accept(block1)
			h.requireUnspentCoinbase(coinbase1)

			fresh := h.block(block1.Hash(), h.coinbase([]byte{0x01, 0x02}), height)

			h.requireAccepted(fresh, h.validate(fresh), "control: a block with no repeated txid must be accepted at height %d", height)
		})
	}
}

// gap049CollisionChain builds the BIP34 coinbase-collision shape on the honest chain, with BIP34
// active from height 3: block 1, below BIP34, has a coinbase whose scriptSig happens to start with a
// height push of 3; block 2 is ordinary; block 3 is at BIP34 and carries, as its coinbase, either a
// byte-identical copy of block 1's coinbase (valid for BIP34 at height 3, and outside BIP30, which
// applies only below BIP0034Height) or a fresh one. This is the shape of the future mainnet
// coinbase collisions the spec names (from ~1,983,702). Blocks 1 and 2 are accepted and moved
// forward; block 3 is returned unvalidated, with block 1's coinbase.
func gap049CollisionChain(t *testing.T, h *gap049Harness, collide bool) (coinbase1 *bt.Tx, block3 *model.Block) {
	t.Helper()

	coinbase1 = h.coinbase(util.EncodeCoinbaseHeightPush(3))
	block1 := h.block(h.settings.ChainCfgParams.GenesisHash, coinbase1, 1)
	h.accept(block1)
	h.requireUnspentCoinbase(coinbase1)

	block2 := h.block(block1.Hash(), h.coinbase(util.EncodeCoinbaseHeightPush(2)), 2)
	h.accept(block2)

	coinbase3 := h.coinbase(util.EncodeCoinbaseHeightPush(3))
	if collide {
		coinbase3 = coinbase1
	}

	block3 = h.block(block2.Hash(), coinbase3, 3)

	return coinbase1, block3
}

// TestGapBV049_CollidingCoinbaseLeavesNoEarlierOutputs covers GAP-BV-049 (BV-BODY-022, second
// sentence): at any height, a coinbase whose ID collides with an earlier coinbase MUST NOT leave the
// earlier coinbase's unspent outputs in the UTXO set. bitcoin-sv overwrites them, so the outputs that
// stand afterwards are the new coinbase's, maturing from the new block.
//
// Expected to fail today: block validation accepts block 3 (correctly), and block assembly's
// SubtreeProcessor.processCoinbaseUtxos (services/blockassembly/subtreeprocessor/SubtreeProcessor.go
// ~7206-7226) gets ErrTxExists from SpendAndCreate(WithCreateOnly) and skips, logging "coinbase
// utxos for ... already exist. Skipping". Block 1's outputs stay, maturing at 1 + 1 = 2, so they can
// be spent at height 2, where the new coinbase's outputs (mature at 3 + 1 = 4) cannot.
//
// The shape models mainnet, where the block at BIP34Height carries consensus BIP34Hash. The
// precondition that block 3 is accepted follows BV-BODY-022's "below BIP0034Height" scope, not
// bitcoin-sv's BIP34Hash gate (validation.cpp ConnectBlock), under which a regtest-like chain whose
// block at BIP34Height does not match BIP34Hash would keep enforcing BIP30 and reject block 3 with
// bad-txns-BIP30.
func TestGapBV049_CollidingCoinbaseLeavesNoEarlierOutputs(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	h := newGap049Harness(ctx, t, gap049Settings(t, 3))

	coinbase1, block3 := gap049CollisionChain(t, h, true)
	require.Equal(t, *coinbase1.TxIDChainHash(), *block3.CoinbaseTx.TxIDChainHash(), "fixture: block 3's coinbase must collide with block 1's")

	h.requireAccepted(block3, h.validate(block3), "precondition: at BIP0034Height the colliding coinbase is outside BIP30 and must be accepted")
	h.moveForward(block3)

	err := h.spendAt(coinbase1, 1+gap049CoinbaseMaturity)

	require.Error(t, err, "GAP-BV-049: the colliding coinbase left block 1's coinbase outputs in the UTXO set; they were spendable at height %d, where block 3's coinbase is still immature", 1+gap049CoinbaseMaturity)
	require.True(t, errors.Is(err, errors.ErrTxCoinbaseImmature), "GAP-BV-049: expected the overwritten coinbase output to mature from block 3, got: %v", err)
}

// TestGapBV049_Control_FreshCoinbaseMaturesFromItsBlock is the control for the test above
// (BV-BODY-022): the same harness and chain with a fresh coinbase in block 3. Block assembly creates
// its outputs maturing from block 3, so the spend at height 2 is refused as immature: the harness
// creates coinbase UTXOs through processCoinbaseUtxos and the maturity probe tells the two maturities
// apart.
func TestGapBV049_Control_FreshCoinbaseMaturesFromItsBlock(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	h := newGap049Harness(ctx, t, gap049Settings(t, 3))

	_, block3 := gap049CollisionChain(t, h, false)

	h.requireAccepted(block3, h.validate(block3), "control: block 3 with a fresh coinbase must be accepted")
	h.moveForward(block3)
	h.requireUnspentCoinbase(block3.CoinbaseTx)

	err := h.spendAt(block3.CoinbaseTx, 1+gap049CoinbaseMaturity)
	require.True(t, errors.Is(err, errors.ErrTxCoinbaseImmature), "control: block 3's coinbase must be immature at height %d, got: %v", 1+gap049CoinbaseMaturity, err)

	require.NoError(t, h.spendAt(block3.CoinbaseTx, 3+gap049CoinbaseMaturity), "control: block 3's coinbase must be spendable once mature")
}
