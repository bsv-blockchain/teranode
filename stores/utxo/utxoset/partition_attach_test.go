package utxoset

import (
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/stretchr/testify/require"
)

// withMined is the block-path create option for a block on the longest chain.
func withMined(blockID, height uint32) utxo.CreateOption {
	return utxo.WithMinedBlockInfo(utxo.MinedBlockInfo{BlockID: blockID, BlockHeight: height, OnLongestChain: true})
}

// partitionEnsurers names the three block-path partition creators, each with the parent it
// adds to and a height that opens a window none of the tests have touched.
var partitionEnsurers = []struct {
	name   string
	parent string
	ensure func(s *Store, ctx context.Context, height uint32) error
	height uint32
}{
	{"tx_mined", "tx_mined", (*Store).ensureTxMinedPartition, 700_000},
	{"tx_body", "tx_body", (*Store).ensureTxBodyPartition, 700_000},
	{"spend_journal", "spend_journal", (*Store).ensureSpendJournalPartition, 700_000},
}

// TestPartitionCreationDoesNotWaitForReaders is the reason a new partition is built as a
// standalone table and then attached, rather than created as a partition in one statement.
//
// CREATE TABLE ... PARTITION OF takes the strongest lock on the parent, so it queues behind
// every reader and every later reader queues behind it. MEASURED on mainnet: 939 to 1,004 ms
// per window boundary, on the block path, waiting for the pruner's stamp query to finish.
// ATTACH PARTITION takes a lock that ordinary readers and writers do not conflict with.
//
// The reader here holds its lock in an open transaction for the whole test, the way the
// stamp's multi-second query does. The ensure must finish anyway.
func TestPartitionCreationDoesNotWaitForReaders(t *testing.T) {
	for _, tc := range partitionEnsurers {
		t.Run(tc.name, func(t *testing.T) {
			s, ctx := newTestStore(t)

			reader, err := s.pool.Begin(ctx)
			require.NoError(t, err)
			defer func() { _ = reader.Rollback(ctx) }()

			var n int
			require.NoError(t, reader.QueryRow(ctx, `SELECT count(*) FROM `+tc.parent).Scan(&n),
				"the reader's lock on the parent is held until its transaction ends")

			bounded, cancel := context.WithTimeout(ctx, 3*time.Second)
			defer cancel()

			start := time.Now()
			require.NoError(t, tc.ensure(s, bounded, tc.height),
				"partition creation must not queue behind a reader")
			require.Less(t, time.Since(start), 3*time.Second)
		})
	}
}

// TestPartitionCreationRefusesAStandaloneTableOfThatName: only the pruner leaves a window
// as a standalone table, between detaching it and dropping it. The block path must never
// re-attach it, because its rows belong to a window the pruner has already stamped and is
// discarding. Failing loudly here is what stops that.
func TestPartitionCreationRefusesAStandaloneTableOfThatName(t *testing.T) {
	s, ctx := newTestStore(t)

	// A standalone table shaped like a window, as a crash between detach and drop leaves it.
	_, err := s.pool.Exec(ctx, `CREATE TABLE tx_mined_w2430 (LIKE tx_mined INCLUDING ALL)`)
	require.NoError(t, err)

	err = s.ensureTxMinedPartition(ctx, 2430*TxMinedPartitionBlocks)
	require.Error(t, err, "a standalone table of the window's name must not be adopted")

	var isPartition bool
	require.NoError(t, s.pool.QueryRow(ctx,
		`SELECT relispartition FROM pg_class WHERE oid = 'tx_mined_w2430'::regclass`).Scan(&isPartition))
	require.False(t, isPartition, "the table must be left exactly as it was found")
}

// TestPartitionCreationIsIdempotentAcrossACacheLoss: after a restart the in-memory window
// cache is empty, so the ensure runs again for a window that already exists. It must find
// the attached partition and do nothing.
func TestPartitionCreationIsIdempotentAcrossACacheLoss(t *testing.T) {
	for _, tc := range partitionEnsurers {
		t.Run(tc.name, func(t *testing.T) {
			s, ctx := newTestStore(t)

			require.NoError(t, tc.ensure(s, ctx, tc.height))

			s.minedWindow.Store(0)
			s.bodyWindow.Store(0)
			s.journalLeaf.Store(0)

			require.NoError(t, tc.ensure(s, ctx, tc.height), "a second ensure for an existing window is a no-op")
		})
	}
}
