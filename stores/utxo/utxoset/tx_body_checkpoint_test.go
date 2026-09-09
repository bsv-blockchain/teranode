package utxoset

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/stretchr/testify/require"
)

// checkpointFloor is the pinned checkpoint height these tests build their store around.
//
// A literal rather than mainnet's real highest checkpoint, so the tests say what they mean and
// do not move the day a checkpoint is added to go-chaincfg. It sits above mainnet's genesis
// activation height so the output-storability rule is the ordinary one.
const checkpointFloor = 700_000

// newCheckpointStore opens a store whose chain params carry ONE checkpoint, at
// checkpointFloor, with the body-skip setting as given.
//
// The params are COPIED before the checkpoint list is replaced. ChainCfgParams points at a
// package-level struct shared by every test in the binary, and editing it in place would move
// the checkpoint list for the whole run.
func newCheckpointStore(t *testing.T, skip bool) (*Store, context.Context) {
	t.Helper()

	return newTestStoreWith(t, func(tSettings *settings.Settings) {
		params := *tSettings.ChainCfgParams
		params.Checkpoints = []chaincfg.Checkpoint{{Height: checkpointFloor}}
		tSettings.ChainCfgParams = &params

		tSettings.UtxoStore.SkipTxBodyBelowCheckpoint = skip
	})
}

// bodyRows counts the tx_body rows a transaction holds, across every live window.
func bodyRows(t *testing.T, s *Store, ctx context.Context, tx *bt.Tx) int {
	t.Helper()

	var n int
	require.NoError(t, s.pool.QueryRow(ctx,
		`SELECT count(*) FROM tx_body WHERE txid = $1`, hashBytes(tx)).Scan(&n))

	return n
}

// coinAt reads the satoshis and locking script off one named output.
func coinAt(t *testing.T, s *Store, ctx context.Context, tx *bt.Tx, vout uint32) (int64, []byte) {
	t.Helper()

	var (
		sats   int64
		script []byte
	)

	require.NoError(t, s.pool.QueryRow(ctx, `
		SELECT satoshis, script FROM utxo
		 WHERE leaf = $1 AND ukey = $2 AND txid = $3`,
		LeafFor(hashBytes(tx)), Pack(hashBytes(tx), vout), hashBytes(tx)).Scan(&sats, &script))

	return sats, script
}

// minedAt is the block information a below-checkpoint block application supplies.
func minedAt(height uint32, blockID uint32) utxo.CreateOption {
	return utxo.WithMinedBlockInfo(utxo.MinedBlockInfo{
		BlockID: blockID, BlockHeight: height, OnLongestChain: true,
	})
}

// createBatchDirect runs one batch through exactly the steps sendCreateBatch runs, in a
// transaction of its own.
//
// Tests that need a batch spanning several heights cannot get one from s.Create: the batcher
// decides what travels together, so composing a batch by hand is the only way to say which
// transactions share a statement. The steps are the callback's own, so nothing here is a
// second copy of what production does.
func createBatchDirect(t *testing.T, s *Store, ctx context.Context, items []*createItem) *createPlan {
	t.Helper()

	for _, it := range items {
		require.NoError(t, s.ensureTxBodyPartition(ctx, it.blockHeight))

		if mi, mined := minedBlock(it.options.MinedBlockInfos); mined {
			require.NoError(t, s.ensureTxMinedPartition(ctx, mi.BlockHeight))
		}
	}

	plan := s.planCreates(items)

	dbTx, err := s.pool.Begin(ctx)
	require.NoError(t, err)

	require.NoError(t, s.lockTxids(ctx, dbTx, plan.txids))
	require.NoError(t, s.runCreatePlan(ctx, dbTx, plan))
	require.NoError(t, dbTx.Commit(ctx))

	return plan
}

// minedItem is one block-path create, ready for createBatchDirect.
func minedItem(tx *bt.Tx, height uint32, blockID uint32) *createItem {
	options := &utxo.CreateOptions{}
	minedAt(height, blockID)(options)

	return &createItem{tx: tx, blockHeight: height, options: options}
}

// TestSkipTxBodyBelowCheckpointWritesNoBodyButKeepsTheCoins is the change in one test.
//
// Below the checkpoint the body is dead weight: the subtree data file holds the same bytes,
// the spend path reads coin rows, and no spend on the outpoint-only route reads a parent body.
// So the row is not written -- and everything that does not depend on it must be unaffected.
func TestSkipTxBodyBelowCheckpointWritesNoBodyButKeepsTheCoins(t *testing.T) {
	s, ctx := newCheckpointStore(t, true)

	tx := mkTx(t, 2, 5_000)
	_, err := s.Create(ctx, tx, checkpointFloor, minedAt(checkpointFloor, 42))
	require.NoError(t, err)

	require.Equal(t, 0, bodyRows(t, s, ctx, tx), "at the floor is below the checkpoint, so no body")
	require.Equal(t, 2, coinCount(t, s, ctx, tx), "both outputs still have coins")

	sats, script := coinAt(t, s, ctx, tx, 0)
	require.Equal(t, int64(5_000), sats)
	require.Equal(t, []byte(*tx.Outputs[0].LockingScript), script)

	// The transaction EXISTS. A body-less read is a record with no bytes, never a miss: its
	// coins are live and a caller told "not found" would reject its children.
	got, err := s.Get(ctx, tx.TxIDChainHash(), fields.Tx)
	require.NoError(t, err)
	require.Nil(t, got.Tx, "the bytes were never written, so there is nothing to decode")

	got, err = s.Get(ctx, tx.TxIDChainHash(), fields.BlockIDs, fields.SizeInBytes)
	require.NoError(t, err)
	require.Equal(t, uint64(tx.Size()), got.SizeInBytes)
	require.Equal(t, []uint32{42}, got.BlockIDs)

	// The outpoint-only spend: the route this store takes below the checkpoint.
	outpointOnly := spendOutput(t, tx, 0, 1)
	_, err = spendOnly(ctx, s, outpointOnly, checkpointFloor+1,
		utxo.WithSkipUTXOHashCheck(true), utxo.WithSkipExtendedInputs(true))
	require.NoError(t, err)

	// And the ordinary spend, hash check and all, on an input decorated from the coin row
	// rather than from the parent's body.
	normal := spendOutput(t, tx, 1, 1)
	normal.Inputs[0].PreviousTxSatoshis = 0
	normal.Inputs[0].PreviousTxScript = nil

	require.NoError(t, s.PreviousOutputsDecorate(ctx, normal))
	require.Equal(t, uint64(5_000), normal.Inputs[0].PreviousTxSatoshis)
	require.NotNil(t, normal.Inputs[0].PreviousTxScript)

	_, err = spendOnly(ctx, s, normal, checkpointFloor+1)
	require.NoError(t, err)
}

// TestSkipTxBodyBelowCheckpointBatchDecoratesFromTheCoinRow is the batched half of the
// decorate check, because BatchPreviousOutputsDecorate is the call the validator actually makes.
func TestSkipTxBodyBelowCheckpointBatchDecoratesFromTheCoinRow(t *testing.T) {
	s, ctx := newCheckpointStore(t, true)

	tx := mkTx(t, 2, 5_000)
	_, err := s.Create(ctx, tx, checkpointFloor-1, minedAt(checkpointFloor-1, 7))
	require.NoError(t, err)
	require.Equal(t, 0, bodyRows(t, s, ctx, tx))

	a := spendOutput(t, tx, 0, 1)
	b := spendOutput(t, tx, 1, 2)

	for _, child := range []*bt.Tx{a, b} {
		child.Inputs[0].PreviousTxSatoshis = 0
		child.Inputs[0].PreviousTxScript = nil
	}

	require.NoError(t, s.BatchPreviousOutputsDecorate(ctx, []*bt.Tx{a, b}))

	for _, child := range []*bt.Tx{a, b} {
		require.Equal(t, uint64(5_000), child.Inputs[0].PreviousTxSatoshis)
		require.NotNil(t, child.Inputs[0].PreviousTxScript)
	}
}

// TestSkipTxBodyAboveCheckpointStillWritesTheBody: one block past the floor the node is on the
// ordinary route, where a body is still the only copy of the unlocking scripts this store has.
func TestSkipTxBodyAboveCheckpointStillWritesTheBody(t *testing.T) {
	s, ctx := newCheckpointStore(t, true)

	tx := mkTx(t, 1, 5_000)
	_, err := s.Create(ctx, tx, checkpointFloor+1, minedAt(checkpointFloor+1, 43))
	require.NoError(t, err)

	require.Equal(t, 1, bodyRows(t, s, ctx, tx))

	got, err := s.Get(ctx, tx.TxIDChainHash(), fields.Tx)
	require.NoError(t, err)
	require.NotNil(t, got.Tx)
	require.Equal(t, tx.TxIDChainHash().String(), got.Tx.TxIDChainHash().String())
}

// TestMempoolCreateKeepsItsBodyBelowTheCheckpoint pins the exemption the mempool path has.
//
// A mempool transaction carries no mined height, so there is no block below the checkpoint to
// place it in, and no subtree data file holding its bytes either. It keeps its body whatever
// the setting says.
func TestMempoolCreateKeepsItsBodyBelowTheCheckpoint(t *testing.T) {
	s, ctx := newCheckpointStore(t, true)

	tx := mkTx(t, 1, 5_000)
	_, err := s.Create(ctx, tx, checkpointFloor-10)
	require.NoError(t, err)

	require.Equal(t, 1, bodyRows(t, s, ctx, tx), "a mempool create has no mined height to skip on")
}

// TestTxBodyIsWrittenBelowTheCheckpointWhenTheSettingIsOff is today's behaviour, and the
// default: the setting is opt-in, so an operator who has not asked for this loses nothing.
func TestTxBodyIsWrittenBelowTheCheckpointWhenTheSettingIsOff(t *testing.T) {
	s, ctx := newCheckpointStore(t, false)

	tx := mkTx(t, 1, 5_000)
	_, err := s.Create(ctx, tx, checkpointFloor-1, minedAt(checkpointFloor-1, 41))
	require.NoError(t, err)

	require.Equal(t, 1, bodyRows(t, s, ctx, tx))
}

// TestSkipTxBodyBatchMixesHeightsAroundTheFloor is the case one statement has to get right per
// ROW rather than per batch: block application below the checkpoint and the tip's own writes
// reach the same batcher, so a single statement can carry both.
func TestSkipTxBodyBatchMixesHeightsAroundTheFloor(t *testing.T) {
	s, ctx := newCheckpointStore(t, true)

	below := mkTx(t, 1, 5_000)
	atFloor := mkTx(t, 1, 6_000)
	above := mkTx(t, 1, 7_000)

	createBatchDirect(t, s, ctx, []*createItem{
		minedItem(below, checkpointFloor-1, 1),
		minedItem(atFloor, checkpointFloor, 2),
		minedItem(above, checkpointFloor+1, 3),
	})

	require.Equal(t, 0, bodyRows(t, s, ctx, below))
	require.Equal(t, 0, bodyRows(t, s, ctx, atFloor), "the floor itself is below the checkpoint")
	require.Equal(t, 1, bodyRows(t, s, ctx, above))

	// Every one of them still has its coin, whichever side of the floor it fell.
	for _, tx := range []*bt.Tx{below, atFloor, above} {
		require.Equal(t, 1, coinCount(t, s, ctx, tx))
	}
}

// TestSkipTxBodyReplayStillReportsTxExistsAndWritesNoBody: a re-applied block after a crash
// must answer exactly as it does today, and must not take the second offer's body as an
// excuse to write a row the first offer deliberately skipped.
func TestSkipTxBodyReplayStillReportsTxExistsAndWritesNoBody(t *testing.T) {
	s, ctx := newCheckpointStore(t, true)

	tx := mkTx(t, 1, 5_000)

	_, err := s.Create(ctx, tx, checkpointFloor-1, minedAt(checkpointFloor-1, 42))
	require.NoError(t, err)

	_, err = s.Create(ctx, tx, checkpointFloor-1, minedAt(checkpointFloor-1, 42))
	require.True(t, errors.Is(err, errors.ErrTxExists))

	require.Equal(t, 0, bodyRows(t, s, ctx, tx))
	require.Equal(t, 1, coinCount(t, s, ctx, tx))
}

// TestGenesisKeepsItsBodyBelowTheCheckpoint pins the one height the shared boundary excludes.
//
// model.BelowCheckpoint requires height > 0, so genesis is NOT below the checkpoint however
// many checkpoints sit above it. A hand-written `height <= highest` would take genesis's body
// away, and this store would then disagree with the outpoint-only spend gate about which
// blocks are on the fast path. The boundary has exactly one definition; this is the test that
// says which one.
func TestGenesisKeepsItsBodyBelowTheCheckpoint(t *testing.T) {
	s, ctx := newCheckpointStore(t, true)

	tx := mkTx(t, 1, 5_000)
	_, err := s.Create(ctx, tx, 0, minedAt(0, 1))
	require.NoError(t, err)

	require.Equal(t, 1, bodyRows(t, s, ctx, tx), "genesis is not below the checkpoint")
}

// TestSkipTxBodyBelowCheckpointThroughSpendAndCreate is the entry point mainnet actually uses.
//
// Every production write is a SpendAndCreate, so a gate proven only through Create is proven on
// a path block application does not take. The shape here is block application's own: a parent
// already applied, then a child that spends it and is created in the same call, both carrying
// the block that contains them.
func TestSkipTxBodyBelowCheckpointThroughSpendAndCreate(t *testing.T) {
	s, ctx := newCheckpointStore(t, true)

	parent := mkTx(t, 2, 5_000)
	_, err := s.Create(ctx, parent, checkpointFloor-2, minedAt(checkpointFloor-2, 5))
	require.NoError(t, err)
	require.Equal(t, 0, bodyRows(t, s, ctx, parent))

	child := spendOutput(t, parent, 0, 2)

	_, spends, err := s.SpendAndCreate(ctx, child, checkpointFloor-1, minedAt(checkpointFloor-1, 6))
	require.NoError(t, err)
	require.Len(t, spends, 1)
	require.NoError(t, spends[0].Err)

	require.Equal(t, 0, bodyRows(t, s, ctx, child), "the create half of the call skips the body too")
	require.Equal(t, 2, coinCount(t, s, ctx, child), "and both its outputs still have coins")

	// The parent's spent coin is gone, which is what proves the spend half ran.
	require.Equal(t, 1, coinCount(t, s, ctx, parent))

	// A body-less transaction's own outputs stay spendable.
	grandchild := spendOutput(t, child, 0, 1)

	_, err = spendOnly(ctx, s, grandchild, checkpointFloor)
	require.NoError(t, err)
	require.Equal(t, 1, coinCount(t, s, ctx, child))
}
