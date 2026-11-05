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

// BIP30 and coinbase-collision handling (BV-BODY-022).
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
	// bip30FarAwayBIP34Height is above every height the BIP30 cases use, including the faked
	// 91841/91843, so all of them are below BIP0034Height.
	bip30FarAwayBIP34Height = 1_000_000

	// bip30CoinbaseMaturity is CreateBaseTestSettings' coinbase maturity, which the UTXO store of
	// setup() (behind newNoPersistHarness) is built with: block 1's coinbase matures at height 2,
	// block 3's at height 4, so a spend at height 2 or 3 tells the two apart.
	bip30CoinbaseMaturity = 1

	// bip30CoinbaseSubsidy is the 50 BTC every fixture coinbase pays, in satoshis.
	bip30CoinbaseSubsidy = 50 * 100000000

	// bip30SpendFee is the fee the maturity probe's spending transaction leaves, in satoshis.
	bip30SpendFee = 1000

	// bip30NoHalvingInterval replaces regtest's 150-block halving interval, which leaves no subsidy at
	// the faked 9184x heights.
	bip30NoHalvingInterval = 210_000
)

// bip30Block3Coinbase says which coinbase the coinbase-collision chain puts in block 3.
type bip30Block3Coinbase int

const (
	// bip30Block3RepeatsBlock1Coinbase gives block 3 a byte-identical copy of block 1's coinbase.
	bip30Block3RepeatsBlock1Coinbase bip30Block3Coinbase = iota
	// bip30Block3HasFreshCoinbase gives block 3 a coinbase of its own.
	bip30Block3HasFreshCoinbase
)

// bip30Settings is the private settings copy every BIP30 test runs on: no checkpoints, BIP34
// activated at bip34Height and a full 50 BTC subsidy at the fixture heights. CreateBaseTestSettings
// hands out its own copy of the chain parameters, so the overrides do not leak into other tests.
func bip30Settings(t *testing.T, bip34Height uint32) *settings.Settings {
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.BlockValidation.OptimisticMining = false
	tSettings.ChainCfgParams.Checkpoints = nil
	tSettings.ChainCfgParams.BIP0034Height = int32(bip34Height) //nolint:gosec
	tSettings.ChainCfgParams.SubsidyReductionInterval = bip30NoHalvingInterval
	require.Equal(t, uint16(bip30CoinbaseMaturity), tSettings.ChainCfgParams.CoinbaseMaturity, "fixture: the maturity the UTXO store is built with")

	return tSettings
}

// bip30Harness is block validation and block assembly on one pair of real stores.
type bip30Harness struct {
	ctx               context.Context
	t                 *testing.T
	settings          *settings.Settings
	blockValidation   *BlockValidation
	blockchainClient  blockchain.ClientI
	utxoStore         utxostore.Store
	subtreeProcessor  *subtreeprocessor.SubtreeProcessor
	firstBlockSeconds uint32
}

func newBIP30Harness(ctx context.Context, t *testing.T, tSettings *settings.Settings) *bip30Harness {
	t.Helper()

	initPrometheusMetrics()

	blockValidation, blockchainClient := newNoPersistHarness(ctx, t, tSettings)

	newSubtreeChan := make(chan subtreeprocessor.NewSubtreeRequest, 10)

	subtreeProcessor, err := subtreeprocessor.NewSubtreeProcessor(ctx, ulogger.TestLogger{}, tSettings, blockValidation.subtreeStore, blockchainClient, blockValidation.utxoStore, newSubtreeChan)
	require.NoError(t, err)

	subtreeProcessor.Start(ctx)

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

	genesis, _, err := blockchainClient.GetBlockHeader(ctx, tSettings.ChainCfgParams.GenesisHash)
	require.NoError(t, err)

	subtreeProcessor.SetCurrentBlockHeader(genesis)

	return &bip30Harness{
		ctx:               ctx,
		t:                 t,
		settings:          tSettings,
		blockValidation:   blockValidation,
		blockchainClient:  blockchainClient,
		utxoStore:         blockValidation.utxoStore,
		subtreeProcessor:  subtreeProcessor,
		firstBlockSeconds: uint32(time.Now().Add(-time.Hour).Unix()), //nolint:gosec
	}
}

// newCoinbase builds a coinbase paying 50 BTC to a fresh key, so no two calls collide by accident.
// scriptSigPrefix is put in front of the miner tag; pass a BIP34 height push to make the coinbase
// valid at that height once BIP34 is active.
func (harness *bip30Harness) newCoinbase(scriptSigPrefix []byte) *bt.Tx {
	harness.t.Helper()

	address := bip30FreshAddress(harness.t)

	scriptSig := append(append([]byte{}, scriptSigPrefix...), []byte("/gap-bv-049/")...)

	coinbaseTx := bt.NewTx()
	require.NoError(harness.t, coinbaseTx.From("0000000000000000000000000000000000000000000000000000000000000000", 0xffffffff, "", 0))
	coinbaseTx.Inputs[0].UnlockingScript = bscript.NewFromBytes(scriptSig)
	require.NoError(harness.t, coinbaseTx.AddP2PKHOutputFromAddress(address, bip30CoinbaseSubsidy))
	require.True(harness.t, coinbaseTx.IsCoinbase())

	return coinbaseTx
}

// mineBlockOn mines a coinbase-only block on parent carrying coinbaseTx, at the given height.
// ValidateBlock takes block.Height as already settled against the parent (Server.deriveBlockHeight on
// the peer route, see the comment ahead of the checkpoint check in BlockValidation.go ValidateBlock),
// so the faked exemption heights below are the heights the BIP30 rule sees.
func (harness *bip30Harness) mineBlockOn(parent *chainhash.Hash, coinbaseTx *bt.Tx, height uint32) *model.Block {
	harness.t.Helper()

	timestamp := harness.firstBlockSeconds + height%1000

	bits, err := harness.blockchainClient.GetNextWorkRequired(harness.ctx, parent, int64(timestamp))
	require.NoError(harness.t, err)

	header := minedHeaderWithBits(harness.t, parent, coinbaseTx.TxIDChainHash(), *bits, timestamp)

	block, err := model.NewBlock(header, coinbaseTx, []*chainhash.Hash{}, 1, uint64(coinbaseTx.Size()), height, 0) //nolint:gosec
	require.NoError(harness.t, err)

	return block
}

// validateFromPeer runs block validation on block as if a peer had served it.
func (harness *bip30Harness) validateFromPeer(block *model.Block) error {
	return harness.blockValidation.ValidateBlockWithOptions(harness.ctx, block, "http://localhost", &ValidateBlockOptions{PeerID: "peer-serving"})
}

// acceptOntoTip validates block, requires it to be accepted, and moves block assembly forward onto
// it, which creates its coinbase UTXOs the way a running node does.
func (harness *bip30Harness) acceptOntoTip(block *model.Block) {
	harness.t.Helper()

	require.NoError(harness.t, harness.validateFromPeer(block), "fixture: block %s at height %d must be accepted", block.Hash(), block.Height)

	// The step a running node takes on the block-mined notification: mark the block's transactions
	// mined and the block mined_set, so a child does not wait on its parent (waitForPreviousBlocksToBeProcessed).
	require.NoError(harness.t, harness.blockValidation.setTxMinedStatus(harness.ctx, block.Hash()))

	harness.moveBlockAssemblyOnto(block)
}

// moveBlockAssemblyOnto moves block assembly forward onto the stored block, which creates the
// block's coinbase UTXOs.
func (harness *bip30Harness) moveBlockAssemblyOnto(block *model.Block) {
	harness.t.Helper()

	_, meta, err := harness.blockchainClient.GetBlockHeader(harness.ctx, block.Hash())
	require.NoError(harness.t, err)

	block.ID = meta.ID

	require.NoError(harness.t, harness.subtreeProcessor.MoveForwardBlock(block))
}

// requireBlockAccepted asserts block passed validation and is stored valid.
func (harness *bip30Harness) requireBlockAccepted(block *model.Block, err error, msgAndArgs ...interface{}) {
	harness.t.Helper()

	require.NoError(harness.t, err, msgAndArgs...)

	_, meta, err := harness.blockchainClient.GetBlockHeader(harness.ctx, block.Hash())
	require.NoError(harness.t, err)
	require.False(harness.t, meta.Invalid, msgAndArgs...)
}

// requireCoinbaseOutputUnspent asserts the coinbase's first output is in the UTXO store and unspent:
// the "ancestor with unspent outputs" precondition of the BIP30 rule.
func (harness *bip30Harness) requireCoinbaseOutputUnspent(coinbaseTx *bt.Tx) {
	harness.t.Helper()

	utxoHash, err := util.UTXOHashFromOutput(coinbaseTx.TxIDChainHash(), coinbaseTx.Outputs[0], 0)
	require.NoError(harness.t, err)

	res, err := harness.utxoStore.GetSpend(harness.ctx, &utxostore.Spend{TxID: coinbaseTx.TxIDChainHash(), Vout: 0, UTXOHash: utxoHash})
	require.NoError(harness.t, err)
	// An unspent coinbase output reads OK or IMMATURE, depending on the store's current height.
	require.Contains(harness.t, []int{int(utxostore.Status_OK), int(utxostore.Status_IMMATURE)}, res.Status,
		"precondition: the coinbase's output must be in the UTXO set and unspent, got status %d", res.Status)
	require.Nil(harness.t, res.SpendingData, "precondition: the coinbase's output must be unspent")
}

// spendCoinbaseOutputAt spends output 0 of coinbaseTx at blockHeight and returns the store's verdict.
func (harness *bip30Harness) spendCoinbaseOutputAt(coinbaseTx *bt.Tx, blockHeight uint32) error {
	harness.t.Helper()

	spendTx := bt.NewTx()
	require.NoError(harness.t, spendTx.From(coinbaseTx.TxIDChainHash().String(), 0, coinbaseTx.Outputs[0].LockingScript.String(), coinbaseTx.Outputs[0].Satoshis))
	require.NoError(harness.t, spendTx.AddP2PKHOutputFromAddress(bip30FreshAddress(harness.t), coinbaseTx.Outputs[0].Satoshis-bip30SpendFee))

	_, _, err := harness.utxoStore.SpendAndCreate(harness.ctx, spendTx, blockHeight, utxostore.WithSpendOnly())

	return err
}

// bip30FreshAddress returns a P2PKH address for a newly generated key.
func bip30FreshAddress(t *testing.T) string {
	t.Helper()

	privateKey, err := bec.NewPrivateKey()
	require.NoError(t, err)

	address, err := bscript.NewAddressFromPublicKey(privateKey.PubKey(), true)
	require.NoError(t, err)

	return address.AddressString
}

// requireRejectedAsBIP30Violation asserts the block repeating an unspent ancestor's txid was
// rejected as an invalid block.
func requireRejectedAsBIP30Violation(t *testing.T, err error, height uint32) {
	t.Helper()

	require.Error(t, err, "a block at height %d repeating the txid of an ancestor transaction with unspent outputs was accepted (BIP30)", height)
	require.True(t, errors.Is(err, errors.ErrBlockInvalid), "expected ERR_BLOCK_INVALID for a BIP30 violation, got: %v", err)
}

// TestKnownDefect_BlockRepeatingUnspentAncestorTxIDBelowBIP34_IsRejected proves that Teranode
// accepts a block that breaks BIP30.
//
// Spec: BV-BODY-022 (tracked as GAP-BV-049).
//
// Scenario: Given BIP0034Height at 1,000,000 and block 1 whose coinbase output is in the UTXO set
// and unspent, When a block on block 1 at height 2, 91841 or 91843 carries a byte-identical copy of
// block 1's coinbase, Then validation rejects it with ERR_BLOCK_INVALID. Height 2 is the honest
// chain; 91841 and 91843 sit either side of the first exempt height (block.Height faked as settled;
// see bip30Harness.mineBlockOn), so the exemption must not reach past block 91842 itself.
//
// Today: no BIP30 check exists anywhere on the block path (model.Block.Valid and
// BlockValidation.ValidateBlock check duplicate txids only within one block, model/Block.go
// ~1706/1738), so the block is accepted. Where the colliding transaction is created, the collision is
// swallowed: block assembly's SubtreeProcessor.processCoinbaseUtxos logs "already exist. Skipping"
// on ErrTxExists, quick validation's applyOneByOne calls collectExisting, and subtree validation's
// check_block_subtrees.go treats TX_EXISTS as already validated.
//
// Controls: the same harness, chain and heights with a fresh coinbase instead of the duplicate. The
// block is accepted, so the failure is the missing BIP30 check, not the fixture.
//
// The exemption itself (blocks 91842 and 91880 accept the duplicate) is deliberately not tested here.
// bitcoin-sv exempts the two historic blocks by height AND hash (validation.cpp ConnectBlock,
// fEnforceBIP30: 00000000000a4d0a...0caec and 00000000000743f1...cd721), which a test with a faked
// height cannot match, so a height-only exemption test would fail a bitcoin-sv-faithful fix. Whether
// BV-BODY-022's "except at blocks 91842 and 91880" means the heights or the two named blocks is an
// open question for the spec; the subtests here pass under either reading once the gap is fixed.
func TestKnownDefect_BlockRepeatingUnspentAncestorTxIDBelowBIP34_IsRejected(t *testing.T) {
	const (
		ancestorHeight             = 1
		honestNextHeight           = 2
		justBelowFirstExemptHeight = 91841
		justAboveFirstExemptHeight = 91843
	)

	// BIP34 height pushes (0x01 = push one byte) leading the two coinbases' scriptSigs.
	ancestorCoinbasePrefix := []byte{0x01, 0x01}
	freshCoinbasePrefix := []byte{0x01, 0x02}

	duplicateHeights := []uint32{honestNextHeight, justBelowFirstExemptHeight, justAboveFirstExemptHeight}

	// Given: block 1 on genesis, accepted and moved forward, with its coinbase output unspent.
	setUpChainWithUnspentAncestorCoinbase := func(ctx context.Context, t *testing.T) (*bip30Harness, *bt.Tx, *model.Block) {
		harness := newBIP30Harness(ctx, t, bip30Settings(t, bip30FarAwayBIP34Height))

		ancestorCoinbase := harness.newCoinbase(ancestorCoinbasePrefix)
		ancestorBlock := harness.mineBlockOn(harness.settings.ChainCfgParams.GenesisHash, ancestorCoinbase, ancestorHeight)
		harness.acceptOntoTip(ancestorBlock)
		harness.requireCoinbaseOutputUnspent(ancestorCoinbase)

		return harness, ancestorCoinbase, ancestorBlock
	}

	for _, height := range duplicateHeights {
		t.Run(fmt.Sprintf("a block at height %d repeating an unspent ancestor coinbase's txid is rejected", height), func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			harness, ancestorCoinbase, ancestorBlock := setUpChainWithUnspentAncestorCoinbase(ctx, t)

			duplicateBlock := harness.mineBlockOn(ancestorBlock.Hash(), ancestorCoinbase, height)
			require.Equal(t, *ancestorCoinbase.TxIDChainHash(), *duplicateBlock.CoinbaseTx.TxIDChainHash(), "fixture: the block must repeat block 1's txid")
			require.Less(t, height, uint32(bip30FarAwayBIP34Height), "fixture: the height must be below BIP0034Height")

			// When
			err := harness.validateFromPeer(duplicateBlock)

			// Then
			requireRejectedAsBIP30Violation(t, err, height)
		})
	}

	t.Run("control: the same chain with a fresh coinbase instead of the duplicate is accepted", func(t *testing.T) {
		for _, height := range duplicateHeights {
			t.Run(fmt.Sprintf("at height %d", height), func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()

				harness, _, ancestorBlock := setUpChainWithUnspentAncestorCoinbase(ctx, t)

				freshBlock := harness.mineBlockOn(ancestorBlock.Hash(), harness.newCoinbase(freshCoinbasePrefix), height)

				harness.requireBlockAccepted(freshBlock, harness.validateFromPeer(freshBlock), "control: a block with no repeated txid must be accepted at height %d", height)
			})
		}
	})
}

// bip30CollisionChainBIP34Height is where the coinbase-collision chain activates BIP34: block 3.
const bip30CollisionChainBIP34Height = 3

// buildBIP34CoinbaseCollisionChain builds the BIP34 coinbase-collision shape on the honest chain,
// with BIP34 active from height 3: block 1, below BIP34, has a coinbase whose scriptSig happens to
// start with a height push of 3; block 2 is ordinary; block 3 is at BIP34 and carries, as its
// coinbase, either a byte-identical copy of block 1's coinbase (valid for BIP34 at height 3, and
// outside BIP30, which applies only below BIP0034Height) or a fresh one. This is the shape of the
// future mainnet coinbase collisions the spec names (from ~1,983,702). Blocks 1 and 2 are accepted
// and moved forward; block 3 is returned unvalidated, with block 1's coinbase.
func buildBIP34CoinbaseCollisionChain(t *testing.T, harness *bip30Harness, block3Coinbase bip30Block3Coinbase) (coinbase1 *bt.Tx, block3 *model.Block) {
	t.Helper()

	const (
		block1Height = 1
		block2Height = 2
		block3Height = bip30CollisionChainBIP34Height
	)

	coinbase1 = harness.newCoinbase(util.EncodeCoinbaseHeightPush(block3Height))
	block1 := harness.mineBlockOn(harness.settings.ChainCfgParams.GenesisHash, coinbase1, block1Height)
	harness.acceptOntoTip(block1)
	harness.requireCoinbaseOutputUnspent(coinbase1)

	block2 := harness.mineBlockOn(block1.Hash(), harness.newCoinbase(util.EncodeCoinbaseHeightPush(block2Height)), block2Height)
	harness.acceptOntoTip(block2)

	coinbase3 := harness.newCoinbase(util.EncodeCoinbaseHeightPush(block3Height))
	if block3Coinbase == bip30Block3RepeatsBlock1Coinbase {
		coinbase3 = coinbase1
	}

	block3 = harness.mineBlockOn(block2.Hash(), coinbase3, block3Height)

	return coinbase1, block3
}

// TestKnownDefect_CollidingCoinbase_LeavesNoEarlierCoinbaseOutputs proves that a coinbase whose
// txid collides with an earlier coinbase leaves the earlier coinbase's outputs in the UTXO set.
//
// Spec: BV-BODY-022, second sentence (tracked as GAP-BV-049).
//
// Scenario: Given BIP34 active from height 3, block 1 (below BIP34) whose coinbase starts with a
// height push of 3, and an ordinary block 2, When block 3 carries a byte-identical copy of block 1's
// coinbase and is accepted and moved forward, Then the outputs that stand are block 3's, maturing at
// 3 + 1 = 4, so spending them at height 1 + 1 = 2 is refused as immature. bitcoin-sv overwrites the
// earlier outputs this way.
//
// Today: block validation accepts block 3 (correctly), and block assembly's
// SubtreeProcessor.processCoinbaseUtxos (services/blockassembly/subtreeprocessor/SubtreeProcessor.go
// ~7206-7226) gets ErrTxExists from SpendAndCreate(WithCreateOnly) and skips, logging "coinbase
// utxos for ... already exist. Skipping". Block 1's outputs stay, maturing at 1 + 1 = 2, so they can
// be spent at height 2, where the new coinbase's outputs (mature at 3 + 1 = 4) cannot.
//
// Controls: the same harness and chain with a fresh coinbase in block 3. Block assembly creates its
// outputs maturing from block 3, so the spend at height 2 is refused as immature and the spend at
// height 4 succeeds: the harness creates coinbase UTXOs through processCoinbaseUtxos and the maturity
// probe tells the two maturities apart.
//
// The shape models mainnet, where the block at BIP34Height carries consensus BIP34Hash. The
// precondition that block 3 is accepted follows BV-BODY-022's "below BIP0034Height" scope, not
// bitcoin-sv's BIP34Hash gate (validation.cpp ConnectBlock), under which a regtest-like chain whose
// block at BIP34Height does not match BIP34Hash would keep enforcing BIP30 and reject block 3 with
// bad-txns-BIP30.
func TestKnownDefect_CollidingCoinbase_LeavesNoEarlierCoinbaseOutputs(t *testing.T) {
	const (
		block1Height                = 1
		block3Height                = bip30CollisionChainBIP34Height
		block1CoinbaseMatureHeight  = block1Height + bip30CoinbaseMaturity
		block3CoinbaseMaturedHeight = block3Height + bip30CoinbaseMaturity
	)

	t.Run("block 3's colliding coinbase replaces block 1's outputs, which are then immature at height 2", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// Given
		harness := newBIP30Harness(ctx, t, bip30Settings(t, bip30CollisionChainBIP34Height))

		coinbase1, block3 := buildBIP34CoinbaseCollisionChain(t, harness, bip30Block3RepeatsBlock1Coinbase)
		require.Equal(t, *coinbase1.TxIDChainHash(), *block3.CoinbaseTx.TxIDChainHash(), "fixture: block 3's coinbase must collide with block 1's")

		// When
		harness.requireBlockAccepted(block3, harness.validateFromPeer(block3), "precondition: at BIP0034Height the colliding coinbase is outside BIP30 and must be accepted")
		harness.moveBlockAssemblyOnto(block3)

		// Then
		err := harness.spendCoinbaseOutputAt(coinbase1, block1CoinbaseMatureHeight)

		require.Error(t, err, "the colliding coinbase left block 1's coinbase outputs in the UTXO set; they were spendable at height %d, where block 3's coinbase is still immature", block1CoinbaseMatureHeight)
		require.True(t, errors.Is(err, errors.ErrTxCoinbaseImmature), "expected the overwritten coinbase output to mature from block 3, got: %v", err)
	})

	t.Run("control: a fresh coinbase in block 3 matures from block 3", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// Given
		harness := newBIP30Harness(ctx, t, bip30Settings(t, bip30CollisionChainBIP34Height))

		_, block3 := buildBIP34CoinbaseCollisionChain(t, harness, bip30Block3HasFreshCoinbase)

		// When
		harness.requireBlockAccepted(block3, harness.validateFromPeer(block3), "control: block 3 with a fresh coinbase must be accepted")
		harness.moveBlockAssemblyOnto(block3)
		harness.requireCoinbaseOutputUnspent(block3.CoinbaseTx)

		// Then
		err := harness.spendCoinbaseOutputAt(block3.CoinbaseTx, block1CoinbaseMatureHeight)
		require.True(t, errors.Is(err, errors.ErrTxCoinbaseImmature), "control: block 3's coinbase must be immature at height %d, got: %v", block1CoinbaseMatureHeight, err)

		require.NoError(t, harness.spendCoinbaseOutputAt(block3.CoinbaseTx, block3CoinbaseMaturedHeight), "control: block 3's coinbase must be spendable once mature")
	})
}
