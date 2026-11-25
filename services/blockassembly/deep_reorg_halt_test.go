package blockassembly

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockassembly/blockassembly_api"
	"github.com/bsv-blockchain/teranode/services/blockassembly/subtreeprocessor"
	"github.com/bsv-blockchain/teranode/services/blockchain"
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

// deepReorgFixture puts block assembly on a1 with b2, the tip of a two-block branch beside a1,
// the best block and a retention of 1, so following the best block moves back the retention.
func deepReorgFixture(t *testing.T) (*baTestItems, *model.BlockHeader, *subtreeprocessor.MockSubtreeProcessor) {
	t.Helper()

	items := setupBlockAssemblyTest(t)
	genesis := genesisHeader(t, items)

	a1 := buildChain(genesis, 1, 610)[0]
	b := buildChain(genesis, 2, 710)
	require.NoError(t, items.addBlock(t.Context(), a1))
	addChain(t, items, b)

	mockStp := &subtreeprocessor.MockSubtreeProcessor{}
	mockStp.On("WaitForPendingBlocks", mock.Anything).Return(nil)
	injectMockStp(t, items, mockStp)

	items.blockAssembler.settings.GlobalBlockHeightRetention = 1
	items.blockAssembler.settings.UtxoStore.BlockHeightRetentionAdjustment = 0
	items.blockAssembler.setBestBlockHeader(a1, 1)

	return items, b[1], mockStp
}

// A reorg that moves back the retention halts block assembly loudly: no candidates, no block
// moves, a record that survives a restart, and readiness left alone so clients still reach it.
func TestDeepReorgHaltStopsBlockAssembly(t *testing.T) {
	initPrometheusMetrics()

	items, b2, mockStp := deepReorgFixture(t)
	ba := items.blockAssembler

	err := ba.handleReorg(t.Context(), b2, 2)
	require.Error(t, err)
	require.True(t, ba.DeepReorgHalted())
	mockStp.AssertNotCalled(t, "Reset", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything)

	_, _, err = ba.GetMiningCandidate(t.Context())
	require.True(t, errors.Is(err, errors.ErrServiceUnavailable), "candidates are refused while halted, got %v", err)

	// Readiness stays as it was: a Service that drops unready pods would cut the operator off.
	server := &BlockAssembly{logger: ba.logger, settings: ba.settings, blockAssembler: ba}
	_, msg, err := server.Health(t.Context(), false)
	require.NoError(t, err)
	require.NotContains(t, msg, "halted", "the halt is not a readiness check")

	// A new block announcement does nothing at all: a blockchain client with no expectations
	// panics on any call the pass would make.
	realClient := ba.blockchainClient
	ba.blockchainClient = &blockchain.Mock{}
	ba.processNewBlockAnnouncement(t.Context())
	ba.blockchainClient = realClient

	tip, _ := ba.CurrentBlock()
	require.NotEqual(t, b2.Hash().String(), tip.Hash().String())

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
	require.Equal(t, *b2.Hash(), recorded.target)

	restarted := &BlockAssembler{settings: ba.settings, logger: ba.logger, blockchainClient: items.blockchainClient}
	require.NoError(t, restarted.loadDeepReorgHalt(t.Context()))
	require.True(t, restarted.DeepReorgHalted())
}

// Only an operator's reset runs while halted, and it lifts the halt and its record.
func TestDeepReorgHaltLiftedByOperatorReset(t *testing.T) {
	initPrometheusMetrics()

	items, b2, mockStp := deepReorgFixture(t)
	ba := items.blockAssembler

	require.Error(t, ba.handleReorg(t.Context(), b2, 2))
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

// A halted block assembly still starts, finishes the pending skipped-fork mark the halting pass
// recorded, and ends up halted: from its record, or by meeting the same reorg again when the
// record was lost.
func TestDeepReorgHaltSurvivesRestart(t *testing.T) {
	for _, recordLost := range []bool{false, true} {
		t.Run(fmt.Sprintf("recordLost=%t", recordLost), func(t *testing.T) {
			initPrometheusMetrics()

			items, b2, _ := deepReorgFixture(t)
			ba := items.blockAssembler

			require.NoError(t, ba.SetState(t.Context()), "a running node has its tip, a1, persisted")
			require.NoError(t, ba.saveSkippedForkMark(t.Context(), true))
			require.Error(t, ba.handleReorg(t.Context(), b2, 2))
			require.True(t, ba.DeepReorgHalted())

			if recordLost {
				require.NoError(t, items.blockchainClient.SetState(t.Context(), DeepReorgHaltStateKey, []byte{0}))
			}

			restarted, err := startBlockAssembler(t, items, ba.settings, nil)
			require.NoError(t, err, "a halted block assembly must start, or the halt becomes a restart loop")
			t.Cleanup(func() { restarted.subtreeProcessor.Stop(context.Background()) })

			require.Eventually(t, restarted.DeepReorgHalted, 10*time.Second, 20*time.Millisecond, "the halt outlives the restart")

			_, _, err = restarted.GetMiningCandidate(t.Context())
			require.True(t, errors.Is(err, errors.ErrServiceUnavailable), "candidates are refused after the restart, got %v", err)
		})
	}
}

// A halted server refuses a mining solution before anything else; the same server, not halted,
// goes on to its next check.
func TestDeepReorgHaltRefusesMiningSolution(t *testing.T) {
	initPrometheusMetrics()

	items, b2, _ := deepReorgFixture(t)
	ba := items.blockAssembler
	server := &BlockAssembly{logger: ba.logger, settings: ba.settings, blockAssembler: ba}

	// Still loading: the check after the halt guard answers without touching the submission queue.
	ba.unminedTransactionsLoading.Store(true)

	req := &blockassembly_api.SubmitMiningSolutionRequest{}

	_, err := server.SubmitMiningSolution(t.Context(), req)
	require.True(t, errors.Is(err, errors.ErrServiceNotReady), "not halted: the next check answers, got %v", err)

	require.Error(t, ba.handleReorg(t.Context(), b2, 2))

	_, err = server.SubmitMiningSolution(t.Context(), req)
	require.True(t, errors.Is(err, errors.ErrServiceUnavailable), "halted: the solution is refused, got %v", err)
}

// Each resetblockassembly command reaches block assembly as an operator reset, the one reset that
// runs while halted, and lifts the halt and its record.
func TestDeepReorgHaltLiftedByResetCommands(t *testing.T) {
	commands := map[string]func(*BlockAssembly, context.Context) error{
		"ResetBlockAssembly": func(s *BlockAssembly, ctx context.Context) error {
			_, err := s.ResetBlockAssembly(ctx, &blockassembly_api.EmptyMessage{})
			return err
		},
		"ResetBlockAssemblyFully": func(s *BlockAssembly, ctx context.Context) error {
			_, err := s.ResetBlockAssemblyFully(ctx, &blockassembly_api.EmptyMessage{})
			return err
		},
		"ResetBlockAssemblyValidateInputs": func(s *BlockAssembly, ctx context.Context) error {
			_, err := s.ResetBlockAssemblyValidateInputs(ctx, &blockassembly_api.EmptyMessage{})
			return err
		},
	}

	for name, command := range commands {
		t.Run(name, func(t *testing.T) {
			initPrometheusMetrics()

			items, b2, mockStp := deepReorgFixture(t)
			ba := items.blockAssembler
			server := &BlockAssembly{logger: ba.logger, settings: ba.settings, blockAssembler: ba}

			require.Error(t, ba.handleReorg(t.Context(), b2, 2))
			require.True(t, ba.DeepReorgHalted())

			mockStp.On("Reset", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
				Return(subtreeprocessor.ResetResponse{})

			require.NoError(t, command(server, t.Context()))

			// The command queues the request for block assembly's loop, which this fixture does
			// not run, so the test plays the loop's part.
			var req resetRequest
			select {
			case req = <-ba.resetCh:
			case <-time.After(10 * time.Second):
				t.Fatal("the command queued no reset")
			}

			require.True(t, req.Operator, "a resetblockassembly command is an operator reset")
			ba.handleResetRequests(t.Context(), req)

			require.False(t, ba.DeepReorgHalted())

			data, err := items.blockchainClient.GetState(t.Context(), DeepReorgHaltStateKey)
			require.NoError(t, err)
			recorded, err := parseDeepReorgHalt(data)
			require.NoError(t, err)
			require.Nil(t, recorded)
		})
	}
}
