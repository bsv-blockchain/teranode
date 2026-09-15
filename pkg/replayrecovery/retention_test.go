package replayrecovery

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRecoveryRetentionDiscover(t *testing.T) {
	for _, tc := range []struct {
		name                             string
		confirmed, spent, tip, retention uint32
		repair                           bool
	}{
		{"recent confirmation", 200, 0, 200, 10, false},
		{"recent spend", 100, 199, 200, 10, false},
		{"boundary", 100, 190, 200, 10, true},
		{"inside boundary", 100, 191, 200, 10, false},
		{"overflow", math.MaxUint32 - 1, 0, math.MaxUint32, 10, false},
		{"future spend", 100, 201, 200, 0, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b, s, tx, manifest, journal := recoveryFixture(t)
			s.retention = tc.retention
			s.tip.Height = tc.tip
			e := s.evidence[tx.TxID()]
			e.BlockHeight = tc.confirmed
			e.LastSpendHeight = tc.spent
			s.evidence[tx.TxID()] = e
			audit, err := discoverForTest(t.Context(), b, s, manifest, nil)
			if tc.repair {
				require.NoError(t, err)
				require.EqualValues(t, 1, audit.FullySpent)
			} else {
				require.ErrorIs(t, err, ErrIncomplete)
				require.EqualValues(t, 1, audit.Unknown)
				require.Zero(t, audit.FullySpent)
			}
			result, err := Apply(t.Context(), b, s, manifest, journal, ApplyOptions{Guard: allowRecovery, Maintenance: true, Tip: s.tip})
			if tc.repair {
				require.NoError(t, err)
				require.EqualValues(t, 1, result.Repaired)
			} else {
				require.ErrorIs(t, err, ErrIncomplete)
				require.Zero(t, b.writes)
				require.Contains(t, b.records, tx.TxID())
			}
		})
	}
}

func TestRecoveryRetentionApplyRechecksEvidenceAndPolicy(t *testing.T) {
	for _, change := range []string{"confirmation", "spend", "policy"} {
		t.Run(change, func(t *testing.T) {
			b, s, tx, manifest, journal := recoveryFixture(t)
			s.retention = 10
			_, err := discoverForTest(t.Context(), b, s, manifest, nil)
			require.NoError(t, err)
			e := s.evidence[tx.TxID()]
			switch change {
			case "confirmation":
				e.BlockHeight = s.tip.Height
			case "spend":
				e.LastSpendHeight = s.tip.Height
			case "policy":
				s.retention = 101
			}
			s.evidence[tx.TxID()] = e
			_, err = Apply(t.Context(), b, s, manifest, journal, ApplyOptions{Guard: allowRecovery, Maintenance: true, Tip: s.tip})
			require.Error(t, err)
			require.Zero(t, b.writes)
			require.Contains(t, b.records, tx.TxID())
		})
	}
}

func TestRecoveryRetentionAbsentChildNeverMarksRecentSpend(t *testing.T) {
	b, s, tx, manifest, journal := recoveryFixture(t)
	s.retention = 10
	delete(b.records, tx.TxID())
	e := s.evidence[tx.TxID()]
	e.LastSpendHeight = s.tip.Height
	s.evidence[tx.TxID()] = e
	audit, err := discoverForTest(t.Context(), b, s, manifest, nil)
	require.ErrorIs(t, err, ErrIncomplete)
	require.EqualValues(t, 1, audit.Unknown)
	_, err = Apply(t.Context(), b, s, manifest, journal, ApplyOptions{Guard: allowRecovery, Maintenance: true, Tip: s.tip})
	require.ErrorIs(t, err, ErrIncomplete)
	require.Zero(t, b.writes)
	require.False(t, b.HasMarker(b.records["parent"], tx.TxID()))
}
