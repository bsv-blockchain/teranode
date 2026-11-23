package blockvalidation

import (
	"context"
	"net/url"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	bec "github.com/bsv-blockchain/go-sdk/primitives/ec"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo/utxoset"
	tpostgres "github.com/bsv-blockchain/teranode/test/utils/postgres"
	"github.com/bsv-blockchain/teranode/ulogger"
	testutil "github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// Below the checkpoint, with the outpoint-only fast path on and blockvalidation_quick_validate_skip_utxo_lock
// at its default false, quick validation creates a block's coins locked and unlocks them once the
// block has committed. The utxoset store refused that list outright, so a node on utxoset with
// those two settings could not apply a single below-checkpoint block. The test drives the real
// apply and the real unlock pass against a real utxoset store and checks the end state: the block
// applies, its coins are locked until the unlock, and spendable after it.
func TestQuickValidate_LockedBlockBelowCheckpointAppliesOnUtxoset(t *testing.T) {
	ctx := context.Background()

	dsn, cleanup, err := tpostgres.SetupTestPostgresContainer()
	if err != nil {
		testutil.SkipIfContainerUnavailable(t, err)
		t.Fatalf("postgres container unavailable: %v", err)
	}

	t.Cleanup(func() { _ = cleanup() })

	bv, _, stop := newListHarness(t, "locked_utxoset_unused", true)
	defer stop()

	require.False(t, bv.settings.BlockValidation.QuickValidateSkipUtxoLock, "the setting is at its default")
	require.True(t, bv.settings.BlockValidation.OutpointOnlyBelowCheckpoint)

	storeURL, err := url.Parse(dsn)
	require.NoError(t, err)

	store, err := utxoset.New(ctx, ulogger.TestLogger{}, bv.settings, storeURL)
	require.NoError(t, err)

	t.Cleanup(func() { _ = store.Close(ctx) })

	require.NoError(t, store.SetBlockHeight(100))

	bv.utxoStore = store

	root, key := seedRoot(t, store, 6, "LOCKED_UTXOSET_KEY")

	txs := make([]*bt.Tx, 6)
	for i := range txs {
		txs[i] = spendOf(t, key, root, uint32(i), 90_000) //nolint:gosec // test index
	}

	layout := [][]*bt.Tx{txs}
	block := netBlockFor(layout)

	require.NoError(t, bv.createAndSpendUTXOsForBatch(ctx, block, netBatchFor(t, bv, block, layout)),
		"a locked below-checkpoint block applies")

	outKey, _ := bec.PrivateKeyFromBytes([]byte("netted-out"))
	child := spendOf(t, outKey, txs[0], 0, 80_000)

	_, _, err = store.SpendAndCreate(ctx, child, block.Height+1)
	require.ErrorIs(t, err, errors.ErrTxLocked, "the block's coins are locked until it commits")

	tree, err := subtreepkg.NewTreeByLeafCount(8)
	require.NoError(t, err)
	require.NoError(t, tree.AddCoinbaseNode())

	for _, tx := range txs {
		require.NoError(t, tree.AddNode(*tx.TxIDChainHash(), 0, 0))
	}

	block.SubtreeSlices = []*subtreepkg.Subtree{tree}

	require.NoError(t, bv.unlockSubtreeTransactionsIfNeeded(ctx, block, "test"))

	_, _, err = store.SpendAndCreate(ctx, child, block.Height+1)
	require.NoError(t, err, "the coins are spendable after the unlock")
}
