package blockvalidation

import (
	"context"
	"fmt"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

func TestForkManagerMaxParallelForksVariations(t *testing.T) {
	initPrometheusMetrics()

	tests := []struct {
		name             string
		maxParallelForks int
		blocksToMark     int
		expectedCanProc  bool
	}{
		{"limit 1 blocks second processing", 1, 1, false},
		{"limit 1 allows first processing", 1, 0, true},
		{"limit 4 allows up to 4", 4, 3, true},
		{"limit 4 blocks at 4", 4, 4, false},
		{"limit 8 allows many concurrent", 8, 7, true},
		{"limit 8 blocks at 8", 8, 8, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger := ulogger.TestLogger{}
			tSettings := test.CreateBaseTestSettings(t)
			tSettings.BlockValidation.MaxParallelForks = tt.maxParallelForks
			fm := NewForkManager(logger, tSettings)

			require.Equal(t, tt.maxParallelForks, fm.maxParallelForks)

			guards := make([]*BlockProcessingGuard, 0, tt.blocksToMark)
			for i := 0; i < tt.blocksToMark; i++ {
				hash := makeSettingsTestHash(t, i+1)
				guard, err := fm.MarkBlockProcessing(hash)
				require.NoError(t, err)
				guards = append(guards, guard)
			}

			nextHash := makeSettingsTestHash(t, tt.blocksToMark+1)
			canProcess, err := fm.CanProcessBlock(context.Background(), nextHash)
			require.NoError(t, err)
			require.Equal(t, tt.expectedCanProc, canProcess)

			for _, g := range guards {
				g.Release()
			}
		})
	}
}

func TestForkManagerMaxParallelForksRelease(t *testing.T) {
	initPrometheusMetrics()

	logger := ulogger.TestLogger{}
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.BlockValidation.MaxParallelForks = 2
	fm := NewForkManager(logger, tSettings)

	hash1 := makeSettingsTestHash(t, 1)
	hash2 := makeSettingsTestHash(t, 2)
	hash3 := makeSettingsTestHash(t, 3)

	guard1, err := fm.MarkBlockProcessing(hash1)
	require.NoError(t, err)

	guard2, err := fm.MarkBlockProcessing(hash2)
	require.NoError(t, err)

	canProcess, err := fm.CanProcessBlock(context.Background(), hash3)
	require.NoError(t, err)
	require.False(t, canProcess)

	guard1.Release()

	canProcess, err = fm.CanProcessBlock(context.Background(), hash3)
	require.NoError(t, err)
	require.True(t, canProcess)

	guard2.Release()
}

func TestForkManagerMaxTrackedForksVariations(t *testing.T) {
	initPrometheusMetrics()

	tests := []struct {
		name            string
		maxTrackedForks int
		forksToRegister int
	}{
		{"limit 1 allows single fork", 1, 1},
		{"limit 5 allows 5 forks", 5, 5},
		{"limit 10 allows 10 forks", 10, 10},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger := ulogger.TestLogger{}
			tSettings := test.CreateBaseTestSettings(t)
			tSettings.BlockValidation.MaxTrackedForks = tt.maxTrackedForks
			fm := NewForkManager(logger, tSettings)

			require.Equal(t, tt.maxTrackedForks, fm.maxTrackedForks)

			for i := 0; i < tt.forksToRegister; i++ {
				hash := makeSettingsTestHash(t, i+1)
				forkID := fmt.Sprintf("fork-%d", i)
				fm.RegisterFork(forkID, hash, uint32(1000+i))
			}

			require.Equal(t, tt.forksToRegister, fm.GetForkCount())
		})
	}
}

func TestForkManagerMaxParallelForksDefaultFallback(t *testing.T) {
	initPrometheusMetrics()

	logger := ulogger.TestLogger{}
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.BlockValidation.MaxParallelForks = 0
	fm := NewForkManager(logger, tSettings)

	require.Equal(t, 4, fm.maxParallelForks, "MaxParallelForks=0 should fallback to default 4")
}

func TestForkManagerMaxTrackedForksDefaultFallback(t *testing.T) {
	initPrometheusMetrics()

	logger := ulogger.TestLogger{}
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.BlockValidation.MaxTrackedForks = 0
	fm := NewForkManager(logger, tSettings)

	require.Equal(t, 1000, fm.maxTrackedForks, "MaxTrackedForks=0 should fallback to default 1000")
}

func TestForkManagerMaxParallelForksNegative(t *testing.T) {
	initPrometheusMetrics()

	logger := ulogger.TestLogger{}
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.BlockValidation.MaxParallelForks = -1
	fm := NewForkManager(logger, tSettings)

	require.Equal(t, 4, fm.maxParallelForks, "MaxParallelForks=-1 should fallback to default 4")
}

func makeSettingsTestHash(t *testing.T, n int) *chainhash.Hash {
	t.Helper()
	suffix := []byte("0000000000000000000000000000000000000000000000000000000000000000")
	hex := []byte("0123456789abcdef")
	suffix[63] = hex[n%16]
	suffix[62] = hex[(n/16)%16]
	suffix[61] = hex[(n/256)%16]
	hash, err := chainhash.NewHashFromStr(string(suffix))
	require.NoError(t, err)
	return hash
}
