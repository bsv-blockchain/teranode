package blockassembly

import (
	"context"
	"net/http"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockassembly/subtreeprocessor"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestDeepReorgNeedsHalt(t *testing.T) {
	tests := []struct {
		name      string
		moveBack  int
		retention uint32
		want      bool
	}{
		{"pruning off never halts", 1000, 0, false},
		{"one short of the retention resets", 7, 8, false},
		{"the retention halts", 8, 8, true},
		{"past the retention halts", 9, 8, true},
		{"no move back never halts", 0, 8, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, deepReorgNeedsHalt(tt.moveBack, tt.retention))
		})
	}
}

func TestDeepReorgHaltRecord(t *testing.T) {
	h := &deepReorgHalt{target: chainhash.HashH([]byte("target")), height: 1234, moveBack: 300}

	got, err := parseDeepReorgHalt(h.bytes())
	require.NoError(t, err)
	require.Equal(t, h, got)

	got, err = parseDeepReorgHalt([]byte{0})
	require.NoError(t, err)
	require.Nil(t, got, "the byte 0 is no halt")

	for _, bad := range [][]byte{{}, {1}, {2}, append([]byte{0}, h.bytes()[1:]...)} {
		_, err = parseDeepReorgHalt(bad)
		require.Error(t, err, "%d bytes", len(bad))
	}
}

// deepReorgFixture puts block assembly on a1 with b1, a sibling of a1, the best block and a
// retention of 1, so following the best block moves back the retention.
func deepReorgFixture(t *testing.T) (*baTestItems, *model.BlockHeader, *subtreeprocessor.MockSubtreeProcessor) {
	t.Helper()

	items := setupBlockAssemblyTest(t)
	genesis := genesisHeader(t, items)

	a1 := buildChain(genesis, 1, 610)[0]
	b1 := buildChain(genesis, 1, 710)[0]
	require.NoError(t, items.addBlock(t.Context(), a1))
	require.NoError(t, items.addBlock(t.Context(), b1))

	mockStp := &subtreeprocessor.MockSubtreeProcessor{}
	mockStp.On("WaitForPendingBlocks", mock.Anything).Return(nil)
	injectMockStp(t, items, mockStp)

	items.blockAssembler.settings.GlobalBlockHeightRetention = 1
	items.blockAssembler.settings.UtxoStore.BlockHeightRetentionAdjustment = 0
	items.blockAssembler.setBestBlockHeader(a1, 1)

	return items, b1, mockStp
}

// A reorg that moves back the retention halts block assembly loudly: no candidates, no
// solutions, readiness failing, no further block moves, and a record that survives a restart.
func TestDeepReorgHaltStopsBlockAssembly(t *testing.T) {
	initPrometheusMetrics()

	items, b1, mockStp := deepReorgFixture(t)
	ba := items.blockAssembler

	err := ba.handleReorg(t.Context(), b1, 1)
	require.Error(t, err)
	require.True(t, ba.DeepReorgHalted())
	mockStp.AssertNotCalled(t, "Reset", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything)

	_, _, err = ba.GetMiningCandidate(t.Context())
	require.True(t, errors.Is(err, errors.ErrServiceUnavailable), "candidates are refused while halted, got %v", err)

	status, msg, err := ba.deepReorgHaltHealth(t.Context(), false)
	require.NoError(t, err)
	require.Equal(t, http.StatusServiceUnavailable, status)
	require.Contains(t, msg, "resetblockassembly")

	// A new block announcement leaves block assembly on its tip.
	ba.processNewBlockAnnouncement(t.Context())
	tip, _ := ba.CurrentBlock()
	require.NotEqual(t, b1.Hash().String(), tip.Hash().String())

	// A reset nobody but block assembly asked for is refused.
	errCh := make(chan error, 1)
	ba.handleResetRequests(t.Context(), resetRequest{ErrCh: errCh})
	require.True(t, errors.Is(<-errCh, errors.ErrServiceUnavailable))
	require.True(t, ba.DeepReorgHalted())

	// The record outlives a restart.
	data, err := items.blockchainClient.GetState(t.Context(), DeepReorgHaltStateKey)
	require.NoError(t, err)
	recorded, err := parseDeepReorgHalt(data)
	require.NoError(t, err)
	require.Equal(t, *b1.Hash(), recorded.target)

	restarted := &BlockAssembler{settings: ba.settings, logger: ba.logger, blockchainClient: items.blockchainClient}
	require.NoError(t, restarted.loadDeepReorgHalt(t.Context()))
	require.True(t, restarted.DeepReorgHalted())
}

// Only an operator's reset runs while halted, and it lifts the halt and its record.
func TestDeepReorgHaltLiftedByOperatorReset(t *testing.T) {
	initPrometheusMetrics()

	items, b1, mockStp := deepReorgFixture(t)
	ba := items.blockAssembler

	require.Error(t, ba.handleReorg(t.Context(), b1, 1))
	require.True(t, ba.DeepReorgHalted())

	mockStp.On("Reset", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(subtreeprocessor.ResetResponse{})

	errCh := make(chan error, 1)
	ba.handleResetRequests(context.Background(), resetRequest{Operator: true, ErrCh: errCh})
	require.NoError(t, <-errCh)
	require.False(t, ba.DeepReorgHalted())

	data, err := items.blockchainClient.GetState(t.Context(), DeepReorgHaltStateKey)
	require.NoError(t, err)
	recorded, err := parseDeepReorgHalt(data)
	require.NoError(t, err)
	require.Nil(t, recorded, "the record is cleared with the halt")

	mockStp.AssertCalled(t, "Reset", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything)
}

// A halted block assembly still starts, so the halt never turns into a restart loop, and it
// starts halted. The pass that halts records a pending mark of skipped fork transactions first,
// as processNewBlockAnnouncement does, and the start must finish that mark too.
func TestDeepReorgHaltSurvivesRestart(t *testing.T) {
	initPrometheusMetrics()

	items, b1, _ := deepReorgFixture(t)
	ba := items.blockAssembler

	require.NoError(t, ba.SetState(t.Context()), "a running node has its tip, a1, persisted")
	require.NoError(t, ba.saveSkippedForkMark(t.Context(), true))
	require.Error(t, ba.handleReorg(t.Context(), b1, 1))
	require.True(t, ba.DeepReorgHalted())

	restarted, err := startBlockAssembler(t, items, ba.settings, nil)
	require.NoError(t, err, "a halted block assembly must start, or the halt becomes a restart loop")
	t.Cleanup(func() { restarted.subtreeProcessor.Stop(context.Background()) })

	require.True(t, restarted.DeepReorgHalted(), "the halt outlives the restart")

	_, _, err = restarted.GetMiningCandidate(t.Context())
	require.True(t, errors.Is(err, errors.ErrServiceUnavailable), "candidates are refused after the restart, got %v", err)
}
