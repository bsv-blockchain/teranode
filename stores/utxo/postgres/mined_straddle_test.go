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

// TestPackMinedInfoRoundTrip is a pure (DB-free) round-trip of the mined_info
// codec: pack the parallel slices, decode them back, and confirm equality plus
// the fixed 12-byte stride. Empty input packs to nil (→ SQL NULL) and decodes to
// nil slices.
func TestPackMinedInfoRoundTrip(t *testing.T) {
	require.Nil(t, packMinedInfo(nil, nil, nil), "empty set must pack to nil (SQL NULL)")

	bids, bhs, sis := decodeMinedInfo(nil)
	require.Nil(t, bids)
	require.Nil(t, bhs)
	require.Nil(t, sis)

	inBids := []uint32{7, 999, 196608, 0xFFFFFFFF}
	inHeights := []uint32{100, 888, 0, 12345}
	inSubtrees := []int{3, 5, 0, 42}

	packed := packMinedInfo(inBids, inHeights, inSubtrees)
	require.Len(t, packed, len(inBids)*minedRecordSize)

	gotBids, gotHeights, gotSubtrees := decodeMinedInfo(packed)
	require.Equal(t, inBids, gotBids)
	require.Equal(t, inHeights, gotHeights)
	require.Equal(t, inSubtrees, gotSubtrees)
	require.Equal(t, inBids, decodeMinedBlockIDs(packed))
}

// TestSetMinedMulti_StrideAlignedGuardRejectsStraddle is the REQUIRED adversarial
// test for the packed mined_info duplicate guard. mined_info is a flat bytea of
// fixed 12-byte records (block_id||height||subtree_idx, each int4 big-endian).
// The idempotent-append guard must only skip when the block_id matches at a
// RECORD-ALIGNED offset (0,12,24,...). A NAIVE guard using bare position()/strpos
// would false-match a block_id whose byte pattern appears MID-RECORD or STRADDLING
// two adjacent records, and would then wrongly skip a legitimate distinct block.
//
// This test crafts exactly those two false-match patterns and asserts the block
// IS appended (guard did not false-match):
//
//	Records after the first two mines (big-endian):
//	  R0 = BE(7)  BE(999) BE(3)   -> bytes 00000007 000003E7 00000003
//	  R1 = BE(999) BE(888) BE(5)  -> bytes 000003E7 00000378 00000005
//
//	Case A (mid-record): block_id 999 equals R0's HEIGHT field, whose bytes
//	  (00 00 03 E7) sit at byte offset 4 — a field boundary, NOT a record
//	  boundary. A bare strpos would match there and skip the append.
//	Case B (cross-record straddle): block_id 196608 = 0x00030000, whose bytes
//	  (00 03 00 00) span byte offset 10..13 — the last 2 bytes of R0's subtree
//	  field and the first 2 bytes of R1's block_id field. A bare strpos would
//	  match there and skip the append.
func TestSetMinedMulti_StrideAlignedGuardRejectsStraddle(t *testing.T) {
	st := newTestStoreWithFlag(t, true)
	ctx := context.Background()
	require.NoError(t, st.SetBlockHeight(100))

	tx := testExtendedTx(t)
	_, err := st.Create(ctx, tx, 100)
	require.NoError(t, err)
	h := tx.TxIDChainHash()

	mine := func(blockID, height uint32, subtreeIdx int) {
		res, mErr := st.SetMinedMulti(ctx, []*chainhash.Hash{h}, utxo.MinedBlockInfo{
			BlockID: blockID, BlockHeight: height, SubtreeIdx: subtreeIdx,
		})
		require.NoError(t, mErr)
		require.Contains(t, res[*h], blockID, "block %d must be recorded", blockID)
	}

	// Build R0 = (7, 999, 3) then R1 = (999, 888, 5).
	mine(7, 999, 3)
	mine(999, 888, 5) // Case A: 999 equals R0's height field bytes (mid-record).

	// Case B: 196608 (0x00030000) straddles the R0/R1 boundary at byte offset 10.
	mine(196608, 0, 0)

	var minedInfo []byte
	require.NoError(t, st.pool.QueryRow(ctx,
		`SELECT mined_info FROM txs WHERE hash=$1`, h[:]).Scan(&minedInfo))
	require.Len(t, minedInfo, 3*minedRecordSize, "all three distinct blocks must be appended (no false-match)")

	bids, heights, sidxs := decodeMinedInfo(minedInfo)
	require.Equal(t, []uint32{7, 999, 196608}, bids, "block ids must be the three distinct blocks in append order")
	require.Equal(t, []uint32{999, 888, 0}, heights, "heights must stay aligned with their block ids")
	require.Equal(t, []int{3, 5, 0}, sidxs, "subtree idxs must stay aligned with their block ids")

	// And a genuine re-mine of an already-recorded block IS a no-op (still 3 records).
	mine(7, 999, 3)
	require.NoError(t, st.pool.QueryRow(ctx,
		`SELECT mined_info FROM txs WHERE hash=$1`, h[:]).Scan(&minedInfo))
	require.Len(t, minedInfo, 3*minedRecordSize, "re-mining an existing aligned block must not duplicate")
}
