package utxoset

import (
	"testing"

	storelogger "github.com/bsv-blockchain/teranode/stores/utxo/logger"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

// With logging=true every caller holds the utxoset store through stores/utxo/logger. The stamp
// worker and the rewind tool's preflight both reach the stamp through Stamper, which the wrapper
// forwards like any other method, so what they get is this store's own stamp and its floors.
//
// The pruner provider is still found by type assertion in services/pruner and the wrapper does
// not forward GetPrunerService here; the separate PR stu/logger-forward-pruner-service covers it.
func TestLoggingWrapperOffersTheStamp(t *testing.T) {
	s, ctx := newTestStore(t)

	wrapped := storelogger.New(ctx, ulogger.TestLogger{}, s)

	st := wrapped.Stamper()
	require.Same(t, s, st, "the logging wrapper must hand back the utxoset store's own stamp")

	want, err := s.Floors(ctx)
	require.NoError(t, err)

	got, err := st.Floors(ctx)
	require.NoError(t, err)
	require.Equal(t, want, got)
	require.Equal(t, s.StampDepth(), st.StampDepth())
	require.Equal(t, s.WindowBlocks(), st.WindowBlocks())
	require.NotZero(t, st.WindowBlocks(), "a zero width would tell the pruner service there is no stamp")
}
