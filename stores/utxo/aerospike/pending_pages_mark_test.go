package aerospike

import (
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/stretchr/testify/require"
)

// A newer page write A fails and marks the master. An older write B then completes. B must not
// drop the mark, because the pages still lack A's state and A's retry fires no signal.
func TestPendingPagesMarkSurvivesAnOlderSuccessfulWrite(t *testing.T) {
	txID := chainhash.HashH([]byte("pending-pages"))
	known := func(v int64) dahVersion { return dahVersion{value: v, known: true} }

	cases := []struct {
		name        string
		mark        dahVersion
		success     dahVersion
		wantPending bool
	}{
		{"an older success keeps a newer mark", known(2), known(1), true},
		{"an equal success clears the mark", known(2), known(2), false},
		{"a newer success clears the mark", known(2), known(3), false},
		{"an unversioned success keeps a versioned mark", known(2), dahVersion{}, true},
		{"any success clears an unversioned mark", dahVersion{}, known(1), false},
		{"an unversioned success clears an unversioned mark", dahVersion{}, dahVersion{}, false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := &Store{}

			s.markPagesPending(&txID, tc.mark)
			require.True(t, s.pagesPending(&txID))

			s.clearPagesPending(&txID, tc.success)
			require.Equal(t, tc.wantPending, s.pagesPending(&txID))
		})
	}

	t.Run("a failed older write does not lower a newer mark", func(t *testing.T) {
		s := &Store{}

		s.markPagesPending(&txID, known(2))
		s.markPagesPending(&txID, known(1))
		s.markPagesPending(&txID, dahVersion{})

		s.clearPagesPending(&txID, known(1))
		require.True(t, s.pagesPending(&txID), "the mark must still name version 2")

		s.clearPagesPending(&txID, known(2))
		require.False(t, s.pagesPending(&txID))
	})

	t.Run("clearing a master without a mark is a no-op", func(t *testing.T) {
		s := &Store{}

		s.clearPagesPending(&txID, known(1))
		require.False(t, s.pagesPending(&txID))
	})
}
