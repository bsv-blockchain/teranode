package blockassembly

import (
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockassembly/blockassembly_api"
	"github.com/bsv-blockchain/teranode/services/blockassembly/subtreeprocessor"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"
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

// stateFailClient is the real blockchain client with its state reads or writes made to fail.
type stateFailClient struct {
	blockchain.ClientI
	failGet, failSet bool
	failSetKey       string // fails writes of this key only
}

func (c *stateFailClient) GetState(ctx context.Context, key string) ([]byte, error) {
	if c.failGet {
		return nil, errors.NewStorageError("state read failed")
	}

	return c.ClientI.GetState(ctx, key)
}

func (c *stateFailClient) SetState(ctx context.Context, key string, data []byte) error {
	if c.failSet || key == c.failSetKey {
		return errors.NewStorageError("state write failed")
	}

	return c.ClientI.SetState(ctx, key, data)
}

// Loading a record halts only on a halt record, and fails on one it cannot read; clearing writes
// the record before it lifts the halt, so a failed write leaves it halted.
func TestDeepReorgHaltLoadAndClear(t *testing.T) {
	initPrometheusMetrics()

	halt := &deepReorgHalt{target: chainhash.HashH([]byte("target")), height: 1234, moveBack: 300}

	newAssembler := func(t *testing.T) (*BlockAssembler, *stateFailClient) {
		items := setupBlockAssemblyTest(t)
		client := &stateFailClient{ClientI: items.blockchainClient}

		return &BlockAssembler{logger: ulogger.TestLogger{}, settings: items.blockAssembler.settings, blockchainClient: client}, client
	}

	loads := []struct {
		name    string
		record  []byte // nil writes no record
		failGet bool
		wantErr bool
		halted  bool
	}{
		{name: "no record"},
		{name: "the byte 0", record: []byte{0}},
		{name: "a halt record", record: halt.bytes(), halted: true},
		{name: "a record it cannot parse", record: []byte{2}, wantErr: true},
		{name: "a store it cannot read", failGet: true, wantErr: true},
	}

	for _, tt := range loads {
		t.Run("load/"+tt.name, func(t *testing.T) {
			b, client := newAssembler(t)
			prometheusBlockAssemblyDeepReorgHalted.Set(0)

			if tt.record != nil {
				require.NoError(t, client.SetState(t.Context(), DeepReorgHaltStateKey, tt.record))
			}

			client.failGet = tt.failGet

			err := b.loadDeepReorgHalt(t.Context())
			require.Equal(t, tt.wantErr, err != nil, "got %v", err)
			require.Equal(t, tt.halted, b.DeepReorgHalted())
			require.Equal(t, map[bool]float64{true: 1, false: 0}[tt.halted], testutil.ToFloat64(prometheusBlockAssemblyDeepReorgHalted))
		})
	}

	clears := []struct {
		name     string
		halted   bool
		failSet  bool
		wantErr  bool
		stillSet bool
	}{
		{name: "not halted"},
		{name: "halted", halted: true},
		{name: "halted and the write fails", halted: true, failSet: true, wantErr: true, stillSet: true},
	}

	for _, tt := range clears {
		t.Run("clear/"+tt.name, func(t *testing.T) {
			b, client := newAssembler(t)

			// A halt record on disk: clearing either replaces it with the byte 0 or leaves it.
			require.NoError(t, client.SetState(t.Context(), DeepReorgHaltStateKey, halt.bytes()))
			prometheusBlockAssemblyDeepReorgHalted.Set(0)

			if tt.halted {
				b.deepReorgHalt.Store(halt)
				prometheusBlockAssemblyDeepReorgHalted.Set(1)
			}

			client.failSet = tt.failSet

			err := b.clearDeepReorgHalt(t.Context())
			require.Equal(t, tt.wantErr, err != nil, "got %v", err)
			require.Equal(t, tt.stillSet, b.DeepReorgHalted())
			require.Equal(t, map[bool]float64{true: 1, false: 0}[tt.stillSet], testutil.ToFloat64(prometheusBlockAssemblyDeepReorgHalted))

			data, err := client.ClientI.GetState(t.Context(), DeepReorgHaltStateKey)
			require.NoError(t, err)

			if tt.halted && !tt.failSet {
				require.Equal(t, []byte{0}, data, "the record is cleared")
			} else {
				require.Equal(t, halt.bytes(), data, "nothing is written when not halted or when the write fails")
			}
		})
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

	a1, _ := ba.CurrentBlock()

	err := ba.handleReorg(t.Context(), b2, 2)
	require.Error(t, err)
	require.True(t, ba.DeepReorgHalted())
	require.Equal(t, 1.0, testutil.ToFloat64(prometheusBlockAssemblyDeepReorgHalted))
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

	// A reset nobody but block assembly asked for is refused before it touches anything, and a
	// reset owed from earlier stays owed.
	owed := &resetRequest{FullReset: true}
	ba.resetOwed = owed
	errCh := make(chan error, 1)
	ba.handleResetRequests(t.Context(), resetRequest{ErrCh: errCh})
	require.True(t, errors.Is(<-errCh, errors.ErrServiceUnavailable))
	require.True(t, ba.DeepReorgHalted())
	require.Same(t, owed, ba.resetOwed)

	// The heartbeat's retry of an owed reset waits too, so the operator's reset takes it in.
	ba.resetOwedRetryAt = time.Now().Add(-time.Second)
	ba.requeueRefusedReset()
	require.Same(t, owed, ba.resetOwed)

	// So is one that would not move back at all.
	_, err = ba.resetToBlockWithResponse(t.Context(), a1, 1)
	require.True(t, errors.Is(err, errors.ErrServiceUnavailable), "got %v", err)
	mockStp.AssertNotCalled(t, "Reset", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything)

	// The record outlives a restart.
	data, err := items.blockchainClient.GetState(t.Context(), DeepReorgHaltStateKey)
	require.NoError(t, err)
	recorded, err := parseDeepReorgHalt(data)
	require.NoError(t, err)
	require.Equal(t, deepReorgHalt{target: *b2.Hash(), height: 2, moveBack: 1}, *recorded)

	restarted := &BlockAssembler{settings: ba.settings, logger: ba.logger, blockchainClient: items.blockchainClient}
	require.NoError(t, restarted.loadDeepReorgHalt(t.Context()))
	require.True(t, restarted.DeepReorgHalted())

	// An operator's reset refused because the skipped-fork mark cannot be recorded stays owed as
	// the operator's, so the heartbeat retries it while halted, and the retry lifts the halt.
	ba.resetOwed = nil
	ba.bestTipPosKnown.Store(false)
	ba.blockchainClient = &stateFailClient{ClientI: items.blockchainClient, failSetKey: SkippedForkMarkStateKey}
	errCh = make(chan error, 1)
	ba.handleResetRequests(t.Context(), resetRequest{Operator: true, ErrCh: errCh})
	require.Error(t, <-errCh)
	require.True(t, ba.DeepReorgHalted())
	require.NotNil(t, ba.resetOwed)
	require.True(t, ba.resetOwed.Operator)
	require.False(t, ba.operatorResetRunning.Load(), "the exemption ends with the refused reset")

	ba.blockchainClient = items.blockchainClient
	mockStp.On("Reset", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(subtreeprocessor.ResetResponse{})
	ba.resetOwedRetryAt = time.Now().Add(-time.Second)
	ba.requeueRefusedReset()
	require.Nil(t, ba.resetOwed)

	select {
	case req := <-ba.resetCh:
		ba.handleResetRequests(t.Context(), req)
	case <-time.After(10 * time.Second):
		t.Fatal("the operator's owed reset was not retried")
	}

	require.False(t, ba.DeepReorgHalted())
	data, err = items.blockchainClient.GetState(t.Context(), DeepReorgHaltStateKey)
	require.NoError(t, err)
	recorded, err = parseDeepReorgHalt(data)
	require.NoError(t, err)
	require.Nil(t, recorded, "the record is cleared with the halt")
}

// A halted block assembly still starts, finishes the pending skipped-fork mark the halting pass
// recorded, and ends up halted: from its record, or by meeting the same reorg again when the
// record was lost. A record it cannot read fails the start rather than lift the halt.
func TestDeepReorgHaltSurvivesRestart(t *testing.T) {
	for _, record := range []string{"kept", "lost", "unreadable"} {
		t.Run("record="+record, func(t *testing.T) {
			initPrometheusMetrics()

			items, b2, _ := deepReorgFixture(t)
			ba := items.blockAssembler

			require.NoError(t, ba.SetState(t.Context()), "a running node has its tip, a1, persisted")
			require.NoError(t, ba.saveSkippedForkMark(t.Context(), true))
			require.Error(t, ba.handleReorg(t.Context(), b2, 2))
			require.True(t, ba.DeepReorgHalted())

			recordLost := record == "lost"

			switch record {
			case "lost":
				require.NoError(t, items.blockchainClient.SetState(t.Context(), DeepReorgHaltStateKey, []byte{0}))
			case "unreadable":
				require.NoError(t, items.blockchainClient.SetState(t.Context(), DeepReorgHaltStateKey, []byte{2}))
			default:
				// Pruning off: the reorg no longer halts, so only the record can.
				ba.settings.GlobalBlockHeightRetention = 0
			}

			restarted, err := startBlockAssembler(t, items, ba.settings, nil)
			if record == "unreadable" {
				require.Error(t, err)
				return
			}

			require.NoError(t, err, "a halted block assembly must start, or the halt becomes a restart loop")
			t.Cleanup(func() { restarted.subtreeProcessor.Stop(context.Background()) })

			if recordLost {
				require.Eventually(t, restarted.DeepReorgHalted, 10*time.Second, 20*time.Millisecond, "the same reorg halts it again")
			} else {
				require.True(t, restarted.DeepReorgHalted(), "the record halts it as it starts")
			}

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

// A reset RPC is an operator reset only when it carries the resetblockassembly command's
// metadata: that one runs while halted and lifts the halt and its record. Any other caller, such
// as the validator's shed recovery, gets an ordinary reset, which the halt refuses.
func TestDeepReorgHaltResetCommands(t *testing.T) {
	type resetCommand struct {
		run            func(*BlockAssembly, context.Context) error
		full, validate bool
	}

	commands := map[string]resetCommand{
		"ResetBlockAssembly": {run: func(s *BlockAssembly, ctx context.Context) error {
			_, err := s.ResetBlockAssembly(ctx, &blockassembly_api.EmptyMessage{})
			return err
		}},
		"ResetBlockAssemblyFully": {full: true, run: func(s *BlockAssembly, ctx context.Context) error {
			_, err := s.ResetBlockAssemblyFully(ctx, &blockassembly_api.EmptyMessage{})
			return err
		}},
		"ResetBlockAssemblyValidateInputs": {validate: true, run: func(s *BlockAssembly, ctx context.Context) error {
			_, err := s.ResetBlockAssemblyValidateInputs(ctx, &blockassembly_api.EmptyMessage{})
			return err
		}},
	}

	// The header as the command sets it, a header with any other value, and none at all.
	headers := map[string]func(context.Context) context.Context{
		"operator": func(ctx context.Context) context.Context {
			md, _ := metadata.FromOutgoingContext(WithOperatorReset(ctx))
			return metadata.NewIncomingContext(ctx, md)
		},
		"header=false": func(ctx context.Context) context.Context {
			return metadata.NewIncomingContext(ctx, metadata.Pairs(OperatorResetMetadataKey, "false"))
		},
		"no header": func(ctx context.Context) context.Context { return ctx },
	}

	for name, command := range commands {
		for header, withHeader := range headers {
			operator := header == "operator"

			t.Run(name+"/"+header, func(t *testing.T) {
				initPrometheusMetrics()

				items, b2, mockStp := deepReorgFixture(t)
				ba := items.blockAssembler
				server := &BlockAssembly{logger: ba.logger, settings: ba.settings, blockAssembler: ba}

				require.Error(t, ba.handleReorg(t.Context(), b2, 2))
				require.True(t, ba.DeepReorgHalted())

				mockStp.On("Reset", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
					Return(subtreeprocessor.ResetResponse{})

				require.NoError(t, command.run(server, withHeader(t.Context())))

				// The command queues the request for block assembly's loop, which this fixture does
				// not run, so the test plays the loop's part.
				var req resetRequest
				select {
				case req = <-ba.resetCh:
				case <-time.After(10 * time.Second):
					t.Fatal("the command queued no reset")
				}

				require.Equal(t, resetRequest{FullReset: command.full, ValidateInputs: command.validate, Operator: operator},
					resetRequest{FullReset: req.FullReset, ValidateInputs: req.ValidateInputs, Operator: req.Operator})
				ba.handleResetRequests(t.Context(), req)

				require.Equal(t, !operator, ba.DeepReorgHalted())
				require.Equal(t, map[bool]float64{true: 0, false: 1}[operator], testutil.ToFloat64(prometheusBlockAssemblyDeepReorgHalted))

				data, err := items.blockchainClient.GetState(t.Context(), DeepReorgHaltStateKey)
				require.NoError(t, err)
				recorded, err := parseDeepReorgHalt(data)
				require.NoError(t, err)

				if operator {
					require.Nil(t, recorded, "the record is cleared with the halt")
					mockStp.AssertCalled(t, "Reset", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything)
				} else {
					require.NotNil(t, recorded, "the record stays with the halt")
					mockStp.AssertNotCalled(t, "Reset", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything)
				}
			})
		}
	}
}

// Block locators are sparse, so the reorg lists start below the real fork point and share the
// blocks between. A fork 14 deep at retention 15 moves back 14 blocks and must not halt.
func TestDeepReorgHaltCountsOnlyBlocksMovedBack(t *testing.T) {
	initPrometheusMetrics()

	items := setupBlockAssemblyTest(t)
	genesis := genesisHeader(t, items)

	prefix := buildChain(genesis, 30, 400)
	addChain(t, items, prefix)
	a := buildChain(prefix[29], 14, 500)
	addChain(t, items, a)
	b := buildChain(prefix[29], 14, 600)
	addChain(t, items, b)

	mockStp := &subtreeprocessor.MockSubtreeProcessor{}
	mockStp.On("WaitForPendingBlocks", mock.Anything).Return(nil)
	mockStp.On("Reset", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(subtreeprocessor.ResetResponse{})
	injectMockStp(t, items, mockStp)

	ba := items.blockAssembler
	ba.settings.GlobalBlockHeightRetention = 15
	ba.settings.UtxoStore.BlockHeightRetentionAdjustment = 0
	ba.setBestBlockHeader(a[13], 44)

	back, forward, err := ba.getReorgBlocks(t.Context(), b[13], 44)
	require.NoError(t, err)
	require.Greater(t, len(back), 14, "the locators must start below the fork for this test to mean anything")
	require.Equal(t, 14, movedBackCount(back, forward))

	_, err = ba.resetToBlockWithResponse(t.Context(), b[13], 44)
	require.NoError(t, err)
	require.False(t, ba.DeepReorgHalted())
}

// An operator reset that fails leaves block assembly on its old tip and pool, so the halt and its
// record stay.
func TestDeepReorgHaltKeptByFailedOperatorReset(t *testing.T) {
	initPrometheusMetrics()

	items, b2, mockStp := deepReorgFixture(t)
	ba := items.blockAssembler

	require.Error(t, ba.handleReorg(t.Context(), b2, 2))

	mockStp.On("Reset", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(subtreeprocessor.ResetResponse{Err: errors.NewProcessingError("reset failed")})
	mockStp.On("GetCurrentBlockHeader").Return((*model.BlockHeader)(nil))

	errCh := make(chan error, 1)
	ba.handleResetRequests(t.Context(), resetRequest{Operator: true, ErrCh: errCh})
	require.Error(t, <-errCh)
	require.True(t, ba.DeepReorgHalted())

	data, err := items.blockchainClient.GetState(t.Context(), DeepReorgHaltStateKey)
	require.NoError(t, err)
	recorded, err := parseDeepReorgHalt(data)
	require.NoError(t, err)
	require.NotNil(t, recorded, "the record stays with the halt")
}
