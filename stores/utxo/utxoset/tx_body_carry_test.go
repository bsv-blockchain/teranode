package utxoset

import (
	"context"
	"fmt"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/stretchr/testify/require"
)

// carryTestHeight is above every checkpoint the test store carries, so nothing here depends on
// the below-checkpoint body skip.
const carryTestHeight = 1_000_000

// bodyWindowExists reports whether the tx_body partition covering height is still a table.
func bodyWindowExists(t *testing.T, s *Store, ctx context.Context, height uint32) bool {
	t.Helper()

	var n int
	require.NoError(t, s.pool.QueryRow(ctx,
		`SELECT count(*) FROM pg_class WHERE relname = $1 AND relkind = 'r'`,
		fmt.Sprintf("tx_body_w%d", height/TxBodyPartitionBlocks)).Scan(&n))

	return n > 0
}

// pruneAt is one pruner session at height, with the store's tip at tip: the pruner's clock is
// the block persister's archived height, which trails the tip.
func pruneAt(t *testing.T, s *Store, ctx context.Context, height, tip uint32) {
	t.Helper()

	require.NoError(t, s.SetBlockHeight(tip))

	svc, err := s.GetPrunerService()
	require.NoError(t, err)

	_, err = svc.Prune(ctx, height, "deadbeef")
	require.NoError(t, err)
}

// archivable asks for tx exactly as the block persister does when it has to build a subtree
// data file from the store (services/blockpersister/streaming_process_subtree.go): one
// BatchDecorate for fields.Tx, then the same three checks it applies before it writes the
// bytes. It returns nil when the persister would succeed.
func archivable(t *testing.T, s *Store, ctx context.Context, tx *bt.Tx) error {
	t.Helper()

	item := &utxo.UnresolvedMetaData{Hash: *tx.TxIDChainHash()}
	require.NoError(t, s.BatchDecorate(ctx, []*utxo.UnresolvedMetaData{item}, fields.Tx))

	switch {
	case item.Err != nil:
		return item.Err
	case item.Data == nil:
		return errors.NewProcessingError("no record for %s", item.Hash)
	case item.Data.Tx == nil:
		return errors.NewProcessingError("transaction is nil for hash %s", item.Hash)
	case !item.Data.TxIsSerializable() || !item.Data.Tx.TxIDChainHash().IsEqual(&item.Hash):
		return errors.NewProcessingError("transaction is not retained in full for hash %s", item.Hash)
	}

	return nil
}

// TestUnminedBodyOutlivesItsWindow pins review finding on PR 1663: the body of a transaction
// still waiting to be mined was dropped with its creation partition, 288 blocks after it arrived.
//
// Block assembly never writes a subtree data file, so for a block this node mines the block
// persister builds that file from the store, asking BatchDecorate for fields.Tx. A transaction
// that waited more than 288 blocks and was then mined came back with a nil Tx and the
// persister failed the block, every retry, with "transaction is nil". SQL and Aerospike keep an
// unmined transaction's body for as long as it waits; this store has to as well.
//
// The end state, all through the real pruner: the body survives the drop of its partition while
// the transaction waits, survives its mining, and goes once it is mined and 288 blocks deep.
func TestUnminedBodyOutlivesItsWindow(t *testing.T) {
	s, ctx := newTestStore(t)

	tx := mkTx(t, 1, 5_000)
	_, err := s.Create(ctx, tx, carryTestHeight)
	require.NoError(t, err)
	require.NoError(t, archivable(t, s, ctx, tx), "inside its partition the body is there")

	// Its partition, 999,984 to 1,000,031, drops once the pruner height is 288 past its top.
	dropAt := uint32(carryTestHeight/TxBodyPartitionBlocks+1)*TxBodyPartitionBlocks + s.bodyRetention
	pruneAt(t, s, ctx, dropAt, dropAt+1)
	require.False(t, bodyWindowExists(t, s, ctx, carryTestHeight), "the creation partition is gone")

	got, err := s.Get(ctx, tx.TxIDChainHash(), fields.Tx)
	require.NoError(t, err)
	require.NotNil(t, got.Tx, "a waiting transaction keeps its body past its partition")
	require.Equal(t, tx.TxID(), got.Tx.TxID())

	// Mined well after its partition went, and the persister has not archived that block yet.
	minedAt := dropAt + 100
	_, err = s.SetMinedMulti(ctx, hashes(tx),
		utxo.MinedBlockInfo{BlockID: 7, BlockHeight: minedAt, OnLongestChain: true})
	require.NoError(t, err)

	pruneAt(t, s, ctx, minedAt-1, minedAt)
	require.NoError(t, archivable(t, s, ctx, tx), "the persister can build the subtree data file for its block")

	// Once it is mined and 288 deep the ordinary horizon applies to it too.
	pruneAt(t, s, ctx, minedAt+s.bodyRetention-1, minedAt+s.bodyRetention)
	require.NoError(t, archivable(t, s, ctx, tx), "still inside 288 blocks of its block")

	pruneAt(t, s, ctx, minedAt+s.bodyRetention, minedAt+s.bodyRetention+1)

	got, err = s.Get(ctx, tx.TxIDChainHash(), fields.Tx)
	require.NoError(t, err)
	require.Nil(t, got.Tx, "mined and 288 deep, the body goes")
	require.Equal(t, 0, carriedBodies(t, s, ctx))
}

// TestBodyOfTransactionMinedAheadOfThePersisterSurvivesTheDrop covers the lag between the tip
// and the pruner's clock. The pruner height is the persister's archived height; a waiting
// transaction mined in a block the persister has not reached yet has already lost its unmined
// marker when the drop runs, so a carry that looked only at the marker at drop time would let
// the body go and wedge the persister on that block.
//
// The carry runs a partition ahead of the drop, measured from the tip, so the transaction was
// carried while it was still waiting.
func TestBodyOfTransactionMinedAheadOfThePersisterSurvivesTheDrop(t *testing.T) {
	s, ctx := newTestStore(t)

	tx := mkTx(t, 1, 5_000)
	_, err := s.Create(ctx, tx, carryTestHeight)
	require.NoError(t, err)

	dropAt := uint32(carryTestHeight/TxBodyPartitionBlocks+1)*TxBodyPartitionBlocks + s.bodyRetention

	// Every block from well before the drop, persister one block behind the tip, with the
	// transaction mined in the block at the tip two blocks before the partition drops.
	minedAt := dropAt - 1

	for p := dropAt - TxBodyPartitionBlocks - 2; p <= dropAt; p++ {
		if p+1 == minedAt {
			_, err = s.SetMinedMulti(ctx, hashes(tx),
				utxo.MinedBlockInfo{BlockID: 9, BlockHeight: minedAt, OnLongestChain: true})
			require.NoError(t, err)
		}

		pruneAt(t, s, ctx, p, p+1)
	}

	require.False(t, bodyWindowExists(t, s, ctx, carryTestHeight), "the creation partition is gone")
	require.NoError(t, archivable(t, s, ctx, tx))
}

// TestBodyOfTransactionMinedBeforeTheFirstCarrySurvivesTheDrop is the same lag with the
// pruner's earlier sessions skipped: nothing ran while the transaction waited, so the first
// carry that sees it runs after it was mined. It was mined in a block the persister has not
// archived, so its body is still needed, and the carry must copy it because it is not yet
// archived, whether or not it is still waiting.
func TestBodyOfTransactionMinedBeforeTheFirstCarrySurvivesTheDrop(t *testing.T) {
	s, ctx := newTestStore(t)

	tx := mkTx(t, 1, 5_000)
	_, err := s.Create(ctx, tx, carryTestHeight)
	require.NoError(t, err)

	dropAt := uint32(carryTestHeight/TxBodyPartitionBlocks+1)*TxBodyPartitionBlocks + s.bodyRetention

	// Mined above the persister's archived height, which is the height the one session runs at.
	minedAt := dropAt + 5
	_, err = s.SetMinedMulti(ctx, hashes(tx),
		utxo.MinedBlockInfo{BlockID: 9, BlockHeight: minedAt, OnLongestChain: true})
	require.NoError(t, err)

	pruneAt(t, s, ctx, dropAt, minedAt+5)

	require.False(t, bodyWindowExists(t, s, ctx, carryTestHeight), "the creation partition is gone")
	require.NoError(t, archivable(t, s, ctx, tx), "the persister cannot build the subtree data file for the block")

	// Once the persister has archived the block and it is 288 deep, the body goes.
	pruneAt(t, s, ctx, minedAt+s.bodyRetention, minedAt+s.bodyRetention+1)
	require.Equal(t, 0, carriedBodies(t, s, ctx))
}

// TestCarriedBodyIsFoundWhileThePersisterLags: a carried body stays until its block is 288
// blocks below the pruner's height, which is the persister's archived height, so it is still
// stored however far the tip runs ahead of a stalled persister. The lookup has to look for it
// on the same terms, not by the block's distance from the tip, or the persister asks for a
// body the store holds and gets nil once the tip is 336 blocks past the block.
func TestCarriedBodyIsFoundWhileThePersisterLags(t *testing.T) {
	s, ctx := newTestStore(t)

	tx := mkTx(t, 1, 5_000)
	_, err := s.Create(ctx, tx, carryTestHeight)
	require.NoError(t, err)

	dropAt := uint32(carryTestHeight/TxBodyPartitionBlocks+1)*TxBodyPartitionBlocks + s.bodyRetention
	pruneAt(t, s, ctx, dropAt, dropAt+1)
	require.Equal(t, 1, carriedBodies(t, s, ctx), "carried while it waited")

	minedAt := dropAt + 2
	_, err = s.SetMinedMulti(ctx, hashes(tx),
		utxo.MinedBlockInfo{BlockID: 11, BlockHeight: minedAt, OnLongestChain: true})
	require.NoError(t, err)

	// The persister stalls below the block and the tip runs on; no pruner session reclaims.
	require.NoError(t, s.SetBlockHeight(minedAt+TxBodyPartitionBlocks+s.bodyRetention+64))
	require.Equal(t, 1, carriedBodies(t, s, ctx), "the body is still stored")

	got, err := s.Get(ctx, tx.TxIDChainHash(), fields.Tx)
	require.NoError(t, err)
	require.NotNil(t, got.Tx, "the lookup did not look for a carried body the store still holds")
	require.NoError(t, archivable(t, s, ctx, tx))
}

// TestBodyCarryIsIdempotent pins the crash window between the carry and the drop. The carry
// commits on its own and the drop comes after it, so a restart there carries the same partition
// again, and a read must still see exactly one body.
func TestBodyCarryIsIdempotent(t *testing.T) {
	s, ctx := newTestStore(t)

	tx := mkTx(t, 1, 5_000)
	_, err := s.Create(ctx, tx, carryTestHeight)
	require.NoError(t, err)

	bound := uint32(carryTestHeight/TxBodyPartitionBlocks+1) * TxBodyPartitionBlocks

	for i := 0; i < 2; i++ {
		n, err := s.carryUnminedBodies(ctx, bound)
		require.NoError(t, err)

		if i == 0 {
			require.Equal(t, int64(1), n)
		} else {
			require.Zero(t, n, "a second pass copies nothing")
		}
	}

	require.Equal(t, 1, carriedBodies(t, s, ctx))

	// Both copies are live until the partition drops, and the read returns the one transaction.
	require.NoError(t, archivable(t, s, ctx, tx))
}

// TestMinedBodyIsNotCarried: a transaction mined in time keeps the ordinary horizon. Carrying
// it would make the side table a second body store.
func TestMinedBodyIsNotCarried(t *testing.T) {
	s, ctx := newTestStore(t)

	tx := mkTx(t, 1, 5_000)
	_, err := s.Create(ctx, tx, carryTestHeight)
	require.NoError(t, err)

	_, err = s.SetMinedMulti(ctx, hashes(tx),
		utxo.MinedBlockInfo{BlockID: 3, BlockHeight: carryTestHeight + 1, OnLongestChain: true})
	require.NoError(t, err)

	dropAt := uint32(carryTestHeight/TxBodyPartitionBlocks+1)*TxBodyPartitionBlocks + s.bodyRetention
	pruneAt(t, s, ctx, dropAt, dropAt+1)

	require.Equal(t, 0, carriedBodies(t, s, ctx))

	got, err := s.Get(ctx, tx.TxIDChainHash(), fields.Tx)
	require.NoError(t, err)
	require.Nil(t, got.Tx)
}

// forgetCarriedBodies empties the carry table, for a test of the readers that answer about a
// waiting transaction with no body. The pruner no longer produces that state, since it carries
// a waiting transaction's body past its partition; a body that was never written, which a create
// that is not faithful to its hash skips, still does.
func forgetCarriedBodies(t *testing.T, s *Store, ctx context.Context) {
	t.Helper()

	_, err := s.pool.Exec(ctx, `DELETE FROM tx_body_carry`)
	require.NoError(t, err)
}

// carriedBodies counts the rows of the carry table.
func carriedBodies(t *testing.T, s *Store, ctx context.Context) int {
	t.Helper()

	var n int
	require.NoError(t, s.pool.QueryRow(ctx, `SELECT count(*) FROM tx_body_carry`).Scan(&n))

	return n
}

// TestBodyOfRecentlyMinedTransactionSurvivesADelayedDropForARemine is the reorg the 288-block
// horizon exists for. A transaction waited past its partition with no pruner session to carry it,
// was mined, and the persister archived its block before the delayed session dropped the partition.
// A shallow reorg then un-mines that block and the transaction is mined again in a block the
// persister has not archived, so the persister needs the body a second time. The block was 288
// blocks deep nowhere near the drop, and a carried body is kept for exactly that long
// (reclaimCarriedBodySQL), so the drop must carry it too.
func TestBodyOfRecentlyMinedTransactionSurvivesADelayedDropForARemine(t *testing.T) {
	s, ctx := newTestStore(t)

	tx := mkTx(t, 1, 5_000)
	_, err := s.Create(ctx, tx, carryTestHeight)
	require.NoError(t, err)

	dropAt := uint32(carryTestHeight/TxBodyPartitionBlocks+1)*TxBodyPartitionBlocks + s.bodyRetention

	// Mined one block below the persister's archived height, which is the height the one
	// delayed session runs at.
	minedAt := dropAt - 1
	_, err = s.SetMinedMulti(ctx, hashes(tx),
		utxo.MinedBlockInfo{BlockID: 9, BlockHeight: minedAt, OnLongestChain: true})
	require.NoError(t, err)

	pruneAt(t, s, ctx, dropAt, dropAt+1)
	require.False(t, bodyWindowExists(t, s, ctx, carryTestHeight), "the creation partition is gone")

	// A two-block reorg un-mines the block, and the transaction is mined again above the
	// archived height.
	require.NoError(t, s.MarkTransactionsOnLongestChain(ctx, []chainhash.Hash{*tx.TxIDChainHash()}, false))
	_, err = s.SetMinedMulti(ctx, hashes(tx),
		utxo.MinedBlockInfo{BlockID: 9, BlockHeight: minedAt, UnsetMined: true})
	require.NoError(t, err)

	got, err := s.Get(ctx, tx.TxIDChainHash(), fields.Tx)
	require.NoError(t, err)
	require.NotNil(t, got.Tx, "the waiting transaction lost its body to the delayed drop")

	remineAt := dropAt + 2
	_, err = s.SetMinedMulti(ctx, hashes(tx),
		utxo.MinedBlockInfo{BlockID: 12, BlockHeight: remineAt, OnLongestChain: true})
	require.NoError(t, err)
	require.NoError(t, s.SetBlockHeight(remineAt))

	require.NoError(t, archivable(t, s, ctx, tx), "the persister cannot build the subtree data file for the remined block")

	// Once the new block is archived and 288 deep, the body goes.
	pruneAt(t, s, ctx, remineAt+s.bodyRetention, remineAt+s.bodyRetention+1)
	require.Equal(t, 0, carriedBodies(t, s, ctx))
}

// TestBodyMinedPastItsWindowIsCarriedUntilItsBlockIs288Deep pins what the drop pass carries
// with the reclaim's floor: a transaction created near the top of its partition and mined a few
// blocks later is mined in a block that is still inside the 288-block horizon when its partition
// drops, so a reorg can still un-mine it and its body is carried. This is not only the
// long-waiting transaction; it is every transaction whose block is above its creation partition's
// top, a fraction of ordinary tip traffic. The copy lives until the block is 288 deep.
func TestBodyMinedPastItsWindowIsCarriedUntilItsBlockIs288Deep(t *testing.T) {
	s, ctx := newTestStore(t)

	windowTop := uint32(carryTestHeight/TxBodyPartitionBlocks+1)*TxBodyPartitionBlocks - 1

	tx := mkTx(t, 1, 5_000)
	_, err := s.Create(ctx, tx, windowTop)
	require.NoError(t, err)

	// Mined two blocks after it arrived, in the next partition.
	minedAt := windowTop + 2
	_, err = s.SetMinedMulti(ctx, hashes(tx),
		utxo.MinedBlockInfo{BlockID: 5, BlockHeight: minedAt, OnLongestChain: true})
	require.NoError(t, err)

	dropAt := windowTop + 1 + s.bodyRetention
	pruneAt(t, s, ctx, dropAt, dropAt+1)
	require.False(t, bodyWindowExists(t, s, ctx, windowTop), "the creation partition is gone")
	require.Equal(t, 1, carriedBodies(t, s, ctx), "its block is 287 deep, inside the horizon")
	require.NoError(t, archivable(t, s, ctx, tx))

	pruneAt(t, s, ctx, minedAt+s.bodyRetention, minedAt+s.bodyRetention+1)
	require.Equal(t, 0, carriedBodies(t, s, ctx), "288 deep, the carried copy goes")
}
