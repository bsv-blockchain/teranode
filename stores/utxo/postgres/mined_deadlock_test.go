// Copyright (c) 2024 The bsv-blockchain developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package postgres

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/stretchr/testify/require"
)

// TestSetMinedMulti_ReRunSameBlockIsIdempotent demonstrates the safety property the
// SetMinedMulti deadlock retry (mined.go, wired through retryOnPgDeadlock in
// deadlock.go) depends on: because a deadlock rolls back the WHOLE UPDATE-chunk
// batch (verified empirically — see the comment above the retry call in
// SetMinedMulti), the correct retry unit is the whole batch, not one chunk. That is
// only safe if re-running the whole batch for a block already recorded is a no-op.
// It is: the UPDATE's stride-aligned mined_info containment guard skips the append
// when the block is already present. This test simulates "attempt 1 actually
// committed, but the caller only observed an error and retried" by calling
// SetMinedMulti twice with an IDENTICAL MinedBlockInfo and asserting the second call
// does not duplicate the 12-byte mined_info record.
func TestSetMinedMulti_ReRunSameBlockIsIdempotent(t *testing.T) {
	st := newTestStoreWithFlag(t, true)
	ctx := context.Background()
	require.NoError(t, st.SetBlockHeight(100))

	tx := testExtendedTx(t)
	_, err := st.Create(ctx, tx, 100)
	require.NoError(t, err)
	h := tx.TxIDChainHash()

	info := utxo.MinedBlockInfo{BlockID: 7, BlockHeight: 100, SubtreeIdx: 3, OnLongestChain: true}

	res1, err := st.SetMinedMulti(ctx, []*chainhash.Hash{h}, info)
	require.NoError(t, err)
	require.Equal(t, []uint32{7}, res1[*h])

	// Re-run with the SAME block info -- this is what a deadlock retry does. It must
	// be a no-op append, not a duplicate.
	res2, err := st.SetMinedMulti(ctx, []*chainhash.Hash{h}, info)
	require.NoError(t, err)
	require.Equal(t, []uint32{7}, res2[*h], "re-running the same block append must not duplicate the entry")

	var minedInfo []byte
	require.NoError(t, st.pool.QueryRow(ctx,
		`SELECT mined_info FROM txs WHERE hash=$1`, h[:]).Scan(&minedInfo))
	require.Len(t, minedInfo, minedRecordSize, "exactly one 12-byte record after idempotent re-mine")
	bids, heights, sidxs := decodeMinedInfo(minedInfo)
	require.Equal(t, []uint32{7}, bids, "block_id must not grow on re-run")
	require.Equal(t, []uint32{100}, heights, "height must stay aligned with block_id")
	require.Equal(t, []int{3}, sidxs, "subtree_idx must stay aligned with block_id")
}

// TestAttemptSetMinedUpdateBatch_NonDeadlockErrorSurfacesImmediately exercises the
// SetMinedMulti retry site's error classification with a REAL (non-deadlock)
// Postgres error, not just the synthetic pgconn.PgError values used in
// deadlock_test.go. attemptSetMinedUpdateBatch is called directly with a
// deliberately malformed SQL string in place of the real updateSQL, producing a
// genuine driver syntax error. isPgDeadlock must classify it as NOT a deadlock, so
// the shared retryOnPgDeadlock loop this function feeds (see the call site in
// SetMinedMulti) never retries it -- matching TestRetryOnPgDeadlock's generic
// "a non-deadlock error returns immediately" case for this site's actual error type.
func TestAttemptSetMinedUpdateBatch_NonDeadlockErrorSurfacesImmediately(t *testing.T) {
	st := newTestStoreWithFlag(t, true)
	ctx := context.Background()

	conn, err := st.pool.Acquire(ctx)
	require.NoError(t, err)
	defer conn.Release()

	chunkArgs := [][]interface{}{
		{[][]byte{{1, 2, 3}}, []int32{1}, []int32{1}, []int32{1}},
	}

	res, err := st.attemptSetMinedUpdateBatch(ctx, conn, `SELECT this is not valid SQL`, chunkArgs)
	require.Error(t, err)
	require.False(t, isPgDeadlock(err), "a syntax error must not classify as a deadlock")
	require.Equal(t, "UPDATE chunk %d", res.stage)
	require.Equal(t, 0, res.failedChunk)
	require.Nil(t, res.resultMap)
}
