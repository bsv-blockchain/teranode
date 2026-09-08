package pruner

import (
	"testing"

	"github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/uaerospike"
	"github.com/stretchr/testify/require"
)

// TestNewServiceLeavesPrunedSetNil pins the fix for #1701: the cuckoo-filter
// parent-marker skip must stay off. PrunedTxSet only ever answers "maybe
// present", and a false positive suppresses the deletedChildren write on a
// live parent, which is what stops a replay of the pruned child being taken
// for an idempotent retry. NewService must never construct the set, under any
// setting.
//
// This asserts the construction site rather than a pruning outcome on purpose:
// the behavioural test needs a genuine cuckoo collision to fire, so it cannot
// tell the fix from its absence.
func TestNewServiceLeavesPrunedSetNil(t *testing.T) {
	for _, defensive := range []bool{false, true} {
		s := createTestSettings()
		s.Pruner.UTXODefensiveEnabled = defensive
		s.Pruner.UTXOPrunedSetMaxEntries = 1 << 20

		svc, err := NewService(s, Options{
			Logger:        ulogger.NewErrorTestLogger(t),
			Client:        &uaerospike.Client{},
			ExternalStore: memory.New(),
			Namespace:     "test",
			Set:           "test",
			IndexWaiter:   &MockIndexWaiter{},
		})
		require.NoError(t, err)
		require.Nil(t, svc.prunedSet, "prunedSet must stay nil (defensiveEnabled=%v)", defensive)
	}
}
