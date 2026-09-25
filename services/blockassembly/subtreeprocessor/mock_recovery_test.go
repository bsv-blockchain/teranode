package subtreeprocessor

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMockRecoveryPendingRepeatedReads(t *testing.T) {
	processor := &MockSubtreeProcessor{}
	require.False(t, processor.RecoveryPending())
	processor.RecoveryPendingState.Store(true)
	for range 3 {
		require.True(t, processor.RecoveryPending())
	}
	processor.RecoveryPendingState.Store(false)
	require.False(t, processor.RecoveryPending())
}
