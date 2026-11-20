package chainancestry

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/stretchr/testify/require"
)

// chainOf builds a straight run of rows from a tip at height top down to 0, with the id of
// each block equal to its height plus 100, and returns them newest first.
func chainOf(top uint32) []Row {
	rows := make([]Row, 0, top+1)

	hashAt := func(h uint32) chainhash.Hash {
		var x chainhash.Hash
		x[0] = byte(h)
		x[1] = byte(h >> 8)
		x[31] = 0x77

		return x
	}

	for h := int(top); h >= 0; h-- {
		r := Row{Hash: hashAt(uint32(h)), Height: uint32(h), ID: uint32(h) + 100, MinedSet: true} //nolint:gosec // small test values
		if h > 0 {
			r.PrevHash = hashAt(uint32(h - 1)) //nolint:gosec // small test values
		}

		rows = append(rows, r)
	}

	return rows
}

// stubFetcher serves a fixed chain, with an optional edit applied to every answer so the
// fault cases a healthy store cannot produce can be tested. It is the one named exception to
// the rule against stubbing the blockchain store, and it is limited to those faults.
type stubFetcher struct {
	rows  []Row
	edit  func([]Row) []Row
	calls int
}

func (s *stubFetcher) Headers(_ context.Context, from chainhash.Hash, n uint32) ([]Row, error) {
	s.calls++

	start := -1

	for i := range s.rows {
		if s.rows[i].Hash == from {
			start = i

			break
		}
	}

	if start < 0 {
		return nil, nil
	}

	end := start + int(n)
	if end > len(s.rows) {
		end = len(s.rows)
	}

	out := make([]Row, end-start)
	copy(out, s.rows[start:end])

	if s.edit != nil {
		out = s.edit(out)
	}

	return out, nil
}

func TestBuildProvesAStraightWalkInChunks(t *testing.T) {
	rows := chainOf(25)
	f := &stubFetcher{rows: rows}

	anc, err := Build(context.Background(), f, rows[0].Hash, 25, 3, 7)
	require.NoError(t, err)

	require.Equal(t, uint32(3), anc.Lo())
	require.Equal(t, uint32(25), anc.Hi())
	require.Equal(t, rows[0].Hash, anc.Anchor())
	require.Equal(t, 4, f.calls, "23 heights in chunks of 7 is four fetches")

	for h := uint32(3); h <= 25; h++ {
		id, ok := anc.BlockID(h)
		require.True(t, ok)
		require.Equal(t, h+100, id, "the id is taken from each row's own height")
		require.True(t, anc.Contains(h+100))
	}

	_, ok := anc.BlockID(2)
	require.False(t, ok, "below the range it says nothing")
	require.False(t, anc.Covers(26))
	require.False(t, anc.Contains(102), "height 2's block is outside the range")

	_, notMined := anc.NotMined(3, 25)
	require.False(t, notMined)

	hs, ids := anc.Pairs(10, 12)
	require.Equal(t, []int32{10, 11, 12}, hs)
	require.Equal(t, []int32{110, 111, 112}, ids)
}

func TestBuildReachesGenesisWhenTheChainIsShort(t *testing.T) {
	rows := chainOf(4)
	f := &stubFetcher{rows: rows}

	anc, err := Build(context.Background(), f, rows[0].Hash, 4, 0, 100)
	require.NoError(t, err)
	require.Equal(t, uint32(0), anc.Lo())
	require.Equal(t, uint32(4), anc.Hi())
}

func TestBuildRecordsHeightsWhoseBlockIsNotMined(t *testing.T) {
	rows := chainOf(10)
	rows[3].MinedSet = false // height 7
	rows[9].MinedSet = false // height 1
	f := &stubFetcher{rows: rows}

	anc, err := Build(context.Background(), f, rows[0].Hash, 10, 0, 0)
	require.NoError(t, err)

	h, ok := anc.NotMined(0, 10)
	require.True(t, ok)
	require.Equal(t, uint32(1), h, "the lowest such height in the range")

	h, ok = anc.NotMined(2, 10)
	require.True(t, ok)
	require.Equal(t, uint32(7), h)

	_, ok = anc.NotMined(8, 10)
	require.False(t, ok)
}

// Every fork-shaped failure is an abort with zero output, and each names the check it failed.
func TestBuildRejectsEveryUnprovenWalk(t *testing.T) {
	rows := chainOf(12)
	tip := rows[0].Hash

	cases := []struct {
		name  string
		edit  func([]Row) []Row
		check string
	}{
		{
			name:  "short answer that does not reach genesis",
			edit:  func(r []Row) []Row { return r[:len(r)-1] },
			check: CheckLength,
		},
		{
			// Two rows at one height with the right total length is how a flag-selected fetch
			// answers a height with two flagged blocks. The row before the duplicate names a
			// parent at the height below, so the duplicate fails linkage before it can fail
			// descent; either way it is rejected, which is what matters.
			name: "duplicate height, the right length",
			edit: func(r []Row) []Row {
				out := append([]Row{}, r[:2]...)
				out = append(out, r[1]) // height 11 twice
				out = append(out, r[2:len(r)-1]...)

				return out
			},
			check: CheckLinkage,
		},
		{
			name: "a losing branch that never rejoins",
			edit: func(r []Row) []Row {
				r[4].Hash[5] ^= 0xff // row 4's hash no longer matches row 3's parent link

				return r
			},
			check: CheckLinkage,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := &stubFetcher{rows: chainOf(12), edit: tc.edit}

			anc, err := Build(context.Background(), f, tip, 12, 0, 100)
			require.Nil(t, anc)

			var rej *RejectedError
			require.ErrorAs(t, err, &rej)
			require.Equal(t, tc.check, rej.Check)
		})
	}

	t.Run("wrong anchor", func(t *testing.T) {
		f := &stubFetcher{rows: chainOf(12)}

		_, err := Build(context.Background(), f, tip, 11, 0, 100)

		var rej *RejectedError
		require.ErrorAs(t, err, &rej)
		require.Equal(t, CheckAnchor, rej.Check, "the anchor's height is proven against the row")
	})

	t.Run("chunk join broken", func(t *testing.T) {
		calls := 0
		f := &stubFetcher{rows: chainOf(12), edit: func(r []Row) []Row {
			calls++
			if calls == 2 {
				// The second chunk's top row is replaced by an unrelated block at the right height.
				r[0].Hash[7] ^= 0xff
			}

			return r
		}}

		_, err := Build(context.Background(), f, tip, 12, 0, 5)

		var rej *RejectedError
		require.ErrorAs(t, err, &rej)
		require.Equal(t, CheckChunkJoin, rej.Check)
	})
}
