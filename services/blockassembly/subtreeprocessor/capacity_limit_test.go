package subtreeprocessor

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCapacityLimit_SetAndGet(t *testing.T) {
	stp := setupTestSubtreeProcessor(t)

	require.Equal(t, int64(0), stp.GetMaxUnminedTransactions(), "default should be 0 (unlimited)")
	require.True(t, stp.CanAcceptTransactions(1000000), "should accept when unlimited")
	require.Equal(t, int64(math.MaxInt64), stp.RemainingCapacity(), "remaining should be MaxInt64 when unlimited")

	stp.SetMaxUnminedTransactions(1000)
	require.Equal(t, int64(1000), stp.GetMaxUnminedTransactions())
}

func TestCapacityLimit_CanAcceptTransactions(t *testing.T) {
	stp := setupTestSubtreeProcessor(t)
	stp.SetMaxUnminedTransactions(100)

	require.True(t, stp.CanAcceptTransactions(50), "should accept when below limit")
	require.True(t, stp.CanAcceptTransactions(100), "should accept when exactly at limit")
	require.False(t, stp.CanAcceptTransactions(101), "should reject when exceeding limit")
}

func TestCapacityLimit_RemainingCapacity(t *testing.T) {
	stp := setupTestSubtreeProcessor(t)
	stp.SetMaxUnminedTransactions(100)

	remaining := stp.RemainingCapacity()
	require.True(t, remaining > 0, "remaining should be positive when under limit")
}

func TestCapacityLimit_IsCapacityLimitReached(t *testing.T) {
	stp := setupTestSubtreeProcessor(t)
	stp.SetMaxUnminedTransactions(10)

	require.False(t, stp.IsCapacityLimitReached(), "should not be reached initially")

	stp.capacityLimitReached.Store(true)
	require.True(t, stp.IsCapacityLimitReached(), "should be reached after being set")
}

func TestCapacityLimit_Unlimited(t *testing.T) {
	stp := setupTestSubtreeProcessor(t)
	stp.SetMaxUnminedTransactions(0)

	require.True(t, stp.CanAcceptTransactions(1000000000), "should accept any amount when unlimited")
	require.Equal(t, int64(math.MaxInt64), stp.RemainingCapacity(), "remaining should be MaxInt64 when unlimited")
}
