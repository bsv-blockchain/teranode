package netsync

import (
	"bytes"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-chaincfg"
	bec "github.com/bsv-blockchain/go-sdk/primitives/ec"
	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/legacy/bsvutil"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/test/utils/transactions"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// seamSubsidy is regtest's block subsidy at height 1.
const seamSubsidy = 5_000_000_000

// seamFixture is one regtest block at height 1 over genesis: a coinbase and ten
// fee-paying transactions whose parents sit in the UTXO store, written so the
// body has both shapes quick validation partitions on. Seven transactions spend
// a stored parent and nothing else; c1 spends the eighth parent output and c2
// and c3 chain off it, so the in-block-parent wave runs too. At four merkle
// items per subtree the eleven transactions make three subtrees, so every
// subtree boundary the sink and the reader agree on is crossed at least twice.
type seamFixture struct {
	block *bsvutil.Block
	hash  chainhash.Hash
	// parents are the four stored transactions the block spends from, two
	// 10,000 satoshi P2PKH outputs each.
	parents []*bt.Tx
	// spends is every non-coinbase transaction in block order.
	spends []*bt.Tx
	// spentBy maps each spent outpoint to the transaction that spent it.
	spentBy map[seamOutpoint]*bt.Tx
	fees    uint64
}

type seamOutpoint struct {
	txid chainhash.Hash
	vout uint32
}

// newSeamFixture builds the block and its parents from keys alone, before any
// store exists, so the block's hash is known before the checkpoint that proves
// it has to be pinned. s is read for the subtree size and the chain parameters.
func newSeamFixture(t *testing.T, s *settingsForSeam) *seamFixture {
	t.Helper()

	priv, pub := bec.PrivateKeyFromBytes([]byte("SEAM_PARENT_KEY"))

	payTo, err := bscript.NewP2PKHFromPubKeyBytes(pub.Compressed())
	require.NoError(t, err)

	f := &seamFixture{spentBy: make(map[seamOutpoint]*bt.Tx)}

	// Four non-coinbase parents with a fabricated input each. Not coinbases on
	// purpose: the full route enforces coinbase maturity, and a parent stored
	// at height 0 would be a hundred blocks too young to spend.
	for seed := byte(1); seed <= 4; seed++ {
		parent := bt.NewTx()
		in := &bt.Input{PreviousTxOutIndex: 0, SequenceNumber: 0xffffffff, UnlockingScript: bscript.NewFromBytes([]byte{0x00})}
		require.NoError(t, in.PreviousTxIDAdd(&chainhash.Hash{seed}))
		parent.Inputs = append(parent.Inputs, in)
		parent.Outputs = append(parent.Outputs,
			&bt.Output{Satoshis: 10_000, LockingScript: payTo},
			&bt.Output{Satoshis: 10_000, LockingScript: payTo},
		)
		f.parents = append(f.parents, parent)
	}

	spend := func(parent *bt.Tx, vout uint32, sats uint64) *bt.Tx {
		tx := transactions.Create(t,
			transactions.WithPrivateKey(priv),
			transactions.WithInput(parent, vout),
			transactions.WithP2PKHOutputs(1, sats, pub),
		)
		f.spentBy[seamOutpoint{txid: *parent.TxIDChainHash(), vout: vout}] = tx
		f.fees += parent.Outputs[vout].Satoshis - sats

		return tx
	}

	// Seven independent spends: parents 0..2 both outputs, parent 3 output 0.
	for i := 0; i < 7; i++ {
		f.spends = append(f.spends, spend(f.parents[i/2], uint32(i%2), 9_000)) //nolint:gosec // small loop index
	}

	c1 := spend(f.parents[3], 1, 9_000)
	c2 := spend(c1, 0, 8_000)
	c3 := spend(c2, 0, 7_000)
	f.spends = append(f.spends, c1, c2, c3)

	// The miner claims the subsidy and every fee, the honest shape. On the full
	// route the reward check reads the fees from the promoted .subtree, which
	// subtree validation rebuilt from transaction meta; netsync wrote none.
	coinbase := transactions.Create(t,
		transactions.WithCoinbaseData(1, "/seam/"),
		transactions.WithP2PKHOutputs(1, seamSubsidy+f.fees, pub),
	)

	msgBlock := &wire.MsgBlock{
		Header: wire.BlockHeader{
			Version:   1,
			PrevBlock: *s.params.GenesisHash,
			Timestamp: time.Now().Truncate(time.Second),
			Bits:      s.params.PowLimitBits,
		},
	}

	for _, tx := range append([]*bt.Tx{coinbase}, f.spends...) {
		msgTx := wire.NewMsgTx(1)
		require.NoError(t, msgTx.Deserialize(bytes.NewReader(tx.Bytes())))
		msgBlock.Transactions = append(msgBlock.Transactions, msgTx)
	}

	f.block = bsvutil.NewBlock(msgBlock)
	f.block.SetHeight(1)

	// The merkle root is computed by the same builder the sink drives, over a
	// throwaway manager that carries only what pipelineHeaderFixture reads.
	pipelineHeaderFixture(t, &SyncManager{logger: ulogger.TestLogger{}, settings: s.settings, chainParams: s.params}, f.block)
	mineRegtestPoW(t, f.block)

	f.hash = *f.block.Hash()

	return f
}

// settingsForSeam is the settings and chain-params pair every seam subtest
// starts from. params is a copy of regtest, so pinning a checkpoint on it
// leaks nowhere, and s.ChainCfgParams points at that same copy.
type settingsForSeam struct {
	settings *settings.Settings
	params   *chaincfg.Params
}

func newSettingsForSeam(t *testing.T, unified bool) *settingsForSeam {
	t.Helper()

	s := test.CreateBaseTestSettings(t)
	params := chaincfg.RegressionNetParams
	s.ChainCfgParams = &params
	s.BlockValidation.OptimisticMining = false
	s.BlockValidation.IsParentMinedRetryMaxRetry = 1
	s.BlockAssembly.Disabled = true
	s.BlockAssembly.MaximumMerkleItemsPerSubtree = 4
	s.GlobalBlockHeightRetention = 1000
	s.BlockValidation.OutpointOnlyBelowCheckpoint = unified
	s.BlockValidation.LegacyUnifiedBelowCheckpoint = unified

	return &settingsForSeam{settings: s, params: &params}
}

// TestSeam_AConvertedMultiSubtreeBlockCommitsThroughRealBlockValidation is the
// PR's production seam end to end: a block streams through pipelineBlockSink into
// a file blob store as .subtreeToCheck structure files, subtree data and a
// converted record, and commitParkedBlock then commits it through the real
// blockvalidation server over gRPC against the same store and a real SQL UTXO
// store. Nothing on the path is a mock or a spy.
//
// Two subtests, one per route, and the shipped default first:
//
//   - "default route flags": both route flags false, which is what an unmodified
//     node runs. legacyUnifiedRoute is false and the block takes
//     ValidateBlockWithOptions, so CheckBlockSubtrees must find the subtrees
//     missing (the files are .subtreeToCheck, never .subtree), validate every
//     transaction through the validator, and write the promoted .subtree with
//     fees rebuilt from transaction meta before the block reward is checked.
//   - "unified route flags": both true, the configuration the Hetzner nodes soak.
//     The block takes quickValidateBlock, which reads the .subtreeToCheck files,
//     spends by outpoint, promotes the .subtree itself and commits with
//     mined_set.
//
// Both subtests assert which route the server took, by block validation's
// outpoint-only blocks counter: one entry to the quick route on the unified
// subtest, none on the default one. Both also assert the same end state, because
// "the chain has the block" is not the claim. On the default route a .subtree written by netsync makes
// CheckBlockSubtrees skip every subtree, so no transaction is created and the
// reward check reads the zero fees netsync wrote: with this fixture's coinbase,
// which claims its fees, that rejects the honest block (observed with the writer
// reverted to .subtree); with a subsidy-only coinbase it would commit an empty
// block. The transactions in the store, stamped with the block, and the parents'
// spends are what say the block was validated.
func TestSeam_AConvertedMultiSubtreeBlockCommitsThroughRealBlockValidation(t *testing.T) {
	t.Run("default route flags", func(t *testing.T) { runSeam(t, false) })
	t.Run("unified route flags", func(t *testing.T) { runSeam(t, true) })
}

func runSeam(t *testing.T, unified bool) {
	t.Helper()

	cfg := newSettingsForSeam(t, unified)
	f := newSeamFixture(t, cfg)

	// Pin the block as the checkpoint, before the stack and the manager read the
	// params: the header cache copies the checkpoint list when it is built, and
	// the blockvalidation server reads it on every route decision.
	cfg.params.Checkpoints = []chaincfg.Checkpoint{{Height: 1, Hash: &f.hash}}

	stack := newLegacyValidationStack(t, cfg.settings)
	ctx := stack.ctx

	// The parents are stamped as mined in genesis, block id 0. The full route's
	// parent-on-chain check (model.Block.checkParentExistsOnChain) takes the
	// genesis shortcut for that id; a parent with no block id at all fails it.
	for _, parent := range f.parents {
		_, err := stack.utxos.Create(ctx, parent, 0,
			utxo.WithMinedBlockInfo(utxo.MinedBlockInfo{BlockID: model.GenesisBlockID, BlockHeight: 0}),
			utxo.WithSkipExtendedInputs(true))
		require.NoError(t, err)
	}

	sm := newLegacySyncManager(t, stack, cfg.settings, cfg.params)

	// A genuine header-cache proof: the one-header run from genesis ends at the
	// pinned hash, so the block is header-proven the way production proves it.
	require.True(t, sm.headerCache.Fill(*cfg.params.GenesisHash, 1, []*wire.BlockHeader{&f.block.MsgBlock().Header}))
	require.True(t, sm.blockOrigin(f.hash).headerProven, "the fixture must be proven, or neither route is the one under test")
	require.Equal(t, unified, sm.unifiedRoute(1), "the route flags must select the route this subtest names")

	body := blockBodyBytes(t, f.block)

	converted, err := sm.pipelineBlockSink(f.hash, &f.block.MsgBlock().Header, bytes.NewReader(body), sinkPayloadLen(body))
	require.NoError(t, err)
	require.True(t, converted)

	record, err := sm.blockPark.ReadConverted(ctx, f.hash)
	require.NoError(t, err)
	require.Len(t, record.Subtrees, 3, "eleven transactions at four per subtree make three subtrees")

	for _, root := range record.Subtrees {
		requireSeamFile(t, stack, *root, fileformat.FileTypeSubtreeToCheck, true, "netsync writes the unvalidated name")
		requireSeamFile(t, stack, *root, fileformat.FileTypeSubtreeData, true, "the transactions travel beside the structure")
		requireSeamFile(t, stack, *root, fileformat.FileTypeSubtree, false, "only block validation promotes a subtree")
	}

	quickBefore := outpointOnlyBlocks(t)

	require.True(t, sm.commitParkedBlock(parkedBlock{hash: f.hash, prevBlock: *cfg.params.GenesisHash}),
		"the converted record must commit through the real server")

	// The route the server actually took. Both routes end with the block in the
	// chain, so that alone cannot say which one ran; only the quick route counts
	// the block on entry.
	wantQuick := 0.0
	if unified {
		wantQuick = 1
	}

	require.Equal(t, quickBefore+wantQuick, outpointOnlyBlocks(t),
		"the unified route must take quickValidateBlock exactly once, and the default route never")

	// End state on the chain.
	exists, err := stack.chain.GetBlockExists(ctx, &f.hash)
	require.NoError(t, err)
	require.True(t, exists, "the block must be in the chain")

	_, meta, err := stack.chain.GetBlockHeader(ctx, &f.hash)
	require.NoError(t, err)
	require.Equal(t, uint32(1), meta.Height)
	require.False(t, meta.Invalid, "a valid block must carry no invalid mark")

	best, bestMeta, err := stack.chain.GetBestBlockHeader(ctx)
	require.NoError(t, err)
	require.Equal(t, f.hash.String(), best.Hash().String(), "the block must be the tip")
	require.Equal(t, uint32(1), bestMeta.Height)

	// Quick validation commits with mined_set and stamps the block id at create;
	// the full route leaves both to block validation's setMined worker, which the
	// helper drives. Either way the block ends mined and its transactions carry
	// its id, which is what lets the next block spend them.
	requireBlockMinedByValidation(t, stack, f.hash)

	// End state in the UTXO store: every transaction created and stamped with
	// this block, every input spent by the transaction that carries it.
	for _, tx := range f.spends {
		md, err := stack.utxos.Get(ctx, tx.TxIDChainHash(), fields.BlockIDs)
		require.NoError(t, err, "%s must have been created by block validation", tx.TxIDChainHash())
		require.Contains(t, md.BlockIDs, meta.ID, "%s must be stamped with the block that mined it", tx.TxIDChainHash())
	}

	for outpoint, spender := range f.spentBy {
		resp, err := stack.utxos.GetSpend(ctx, &utxo.Spend{TxID: &outpoint.txid, Vout: outpoint.vout})
		require.NoError(t, err)
		require.NotNil(t, resp.SpendingData, "output %d of %s must be spent", outpoint.vout, outpoint.txid)
		require.Equal(t, spender.TxIDChainHash().String(), resp.SpendingData.TxID.String(),
			"output %d of %s must be spent by %s", outpoint.vout, outpoint.txid, spender.TxIDChainHash())
	}

	// End state on disk: the record is gone with the entry, the structure files
	// were promoted by block validation, and netsync's own files remain until
	// pipelineBlockDelete or their delete-at-height.
	requireSeamFile(t, stack, f.hash, fileformat.FileTypeBlock, false, "the committed disposition deletes the record")

	for _, root := range record.Subtrees {
		requireSeamFile(t, stack, *root, fileformat.FileTypeSubtree, true, "block validation promotes the subtree after validating it")
		requireSeamFile(t, stack, *root, fileformat.FileTypeSubtreeToCheck, true, "the unvalidated file stays beside the promoted one")
	}
}

func requireSeamFile(t *testing.T, stack *legacyValidationStack, key chainhash.Hash, fileType fileformat.FileType, want bool, why string) {
	t.Helper()

	got, err := stack.subtrees.Exists(stack.ctx, key[:], fileType)
	require.NoError(t, err)
	require.Equal(t, want, got, "%s under %s: %s", fileType, key, why)
}
