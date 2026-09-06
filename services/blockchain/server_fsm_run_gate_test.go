package blockchain

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockchain/blockchain_api"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/ordishs/gocore"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// TestHighestCheckpointHeight covers the small helper used to derive the
// guard threshold from a network's hard-coded checkpoint list.
func TestHighestCheckpointHeight(t *testing.T) {
	tests := []struct {
		name string
		in   []chaincfg.Checkpoint
		want uint32
	}{
		{name: "nil list", in: nil, want: 0},
		{name: "empty list", in: []chaincfg.Checkpoint{}, want: 0},
		{
			name: "single entry",
			in:   []chaincfg.Checkpoint{{Height: 600000}},
			want: 600000,
		},
		{
			name: "unordered list picks max",
			in: []chaincfg.Checkpoint{
				{Height: 200000},
				{Height: 938000},
				{Height: 500000},
			},
			want: 938000,
		},
		{name: "mainnet params", in: chaincfg.MainNetParams.Checkpoints, want: 945000},
		{name: "regtest has no checkpoints", in: chaincfg.RegressionNetParams.Checkpoints, want: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, HighestCheckpointHeight(tt.in))
		})
	}
}

// fsmGateStore is a minimal store stub that implements just the methods
// guardRunBelowHighestCheckpoint touches (GetBestBlockHeader). It is
// embedded in errorStore so the rest of the Store interface is satisfied
// by the parent MockStore.
type fsmGateStore struct {
	errorStore
}

func (s *fsmGateStore) GetBestBlockHeader(ctx context.Context) (*model.BlockHeader, *model.BlockHeaderMeta, error) {
	args := s.Called(ctx)
	hdr, _ := args.Get(0).(*model.BlockHeader)
	meta, _ := args.Get(1).(*model.BlockHeaderMeta)
	return hdr, meta, args.Error(2)
}

// newTestBlockchainForGate returns a *Blockchain with just enough state for
// guardRunBelowHighestCheckpoint and SendFSMEvent to run: store, settings,
// logger, stats (for the tracing decorator), and a buffered notifications
// channel that gets drained so the FSM enter_state callback can publish
// without blocking.
func newTestBlockchainForGate(t *testing.T, params *chaincfg.Params, store *fsmGateStore) *Blockchain {
	t.Helper()
	initPrometheusMetrics()
	b := &Blockchain{
		logger:        ulogger.TestLogger{},
		store:         store,
		settings:      &settings.Settings{ChainCfgParams: params},
		notifications: make(chan *blockchain_api.Notification, 10),
		stats:         gocore.NewStat("blockchain-test"),
	}
	done := make(chan struct{})
	t.Cleanup(func() { close(done) })
	go func() {
		for {
			select {
			case <-done:
				return
			case <-b.notifications:
			}
		}
	}()
	return b
}

// TestGuardRunBelowHighestCheckpoint is the regression test for the FSM
// RUN gate. While the local chain tip sits below the network's highest
// hard-coded checkpoint the blockchain server must refuse RUN, regardless
// of which caller (catchup, legacy startSync, etc.) initiated it.
//
// Triggering RUN early during a deep IBD lets the mempool/validator run
// under pre-Genesis output rules (chain height < 620538 on mainnet) and
// the legacy service relay tx invs that current peers ban on sight
// (`bad-txns-vout-p2sh BAN THRESHOLD EXCEEDED`).
func TestGuardRunBelowHighestCheckpoint(t *testing.T) {
	ctx := context.Background()
	highest := HighestCheckpointHeight(chaincfg.MainNetParams.Checkpoints)
	require.Greater(t, highest, uint32(0), "mainnet must have at least one checkpoint")

	tests := []struct {
		name       string
		params     *chaincfg.Params
		height     uint32
		storeErr   error
		nilMeta    bool
		wantErr    bool
		wantSubstr string
	}{
		{
			name:       "below highest checkpoint rejects",
			params:     &chaincfg.MainNetParams,
			height:     highest - 1,
			wantErr:    true,
			wantSubstr: "below highest checkpoint",
		},
		{
			name:    "exactly at highest checkpoint permits",
			params:  &chaincfg.MainNetParams,
			height:  highest,
			wantErr: false,
		},
		{
			name:    "above highest checkpoint permits",
			params:  &chaincfg.MainNetParams,
			height:  highest + 100,
			wantErr: false,
		},
		{
			name:    "network with no checkpoints permits",
			params:  &chaincfg.RegressionNetParams,
			height:  0,
			wantErr: false,
		},
		{
			name:       "store error fails closed",
			params:     &chaincfg.MainNetParams,
			storeErr:   errors.NewError("forced read failure"),
			wantErr:    true,
			wantSubstr: "cannot read best block header",
		},
		{
			name:       "nil meta fails closed",
			params:     &chaincfg.MainNetParams,
			nilMeta:    true,
			wantErr:    true,
			wantSubstr: "best block header meta unavailable",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := &fsmGateStore{}
			hdr := &model.BlockHeader{HashPrevBlock: &chainhash.Hash{}, HashMerkleRoot: &chainhash.Hash{}}
			var meta *model.BlockHeaderMeta
			if !tt.nilMeta {
				meta = &model.BlockHeaderMeta{Height: tt.height}
			}
			store.On("GetBestBlockHeader", mock.Anything).Return(hdr, meta, tt.storeErr)

			b := newTestBlockchainForGate(t, tt.params, store)

			err := b.guardRunBelowHighestCheckpoint(ctx)
			if tt.wantErr {
				require.Error(t, err)
				if tt.wantSubstr != "" {
					require.Contains(t, err.Error(), tt.wantSubstr)
				}
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// TestGuardRunBelowHighestCheckpoint_NilSettings verifies the defensive
// path: missing settings or ChainCfgParams short-circuits to allow RUN.
// Production code always populates these, but the guard must not panic.
func TestGuardRunBelowHighestCheckpoint_NilSettings(t *testing.T) {
	t.Run("nil settings", func(t *testing.T) {
		b := &Blockchain{logger: ulogger.TestLogger{}}
		guardErr := b.guardRunBelowHighestCheckpoint(context.Background())
		require.NoError(t, guardErr)
	})

	t.Run("nil ChainCfgParams", func(t *testing.T) {
		b := &Blockchain{logger: ulogger.TestLogger{}, settings: &settings.Settings{}}
		guardErr := b.guardRunBelowHighestCheckpoint(context.Background())
		require.NoError(t, guardErr)
	})
}

// TestInit_MigratesPersistedLegacySyncingToCatchingBlocks verifies that Init
// maps a persisted "LEGACYSYNCING" state to CATCHINGBLOCKS and re-persists it,
// so a node that crashed mid-legacy-sync is not bricked on restart.
func TestInit_MigratesPersistedLegacySyncingToCatchingBlocks(t *testing.T) {
	ctx := context.Background()

	store := &fsmGateStore{}
	// Seed the persisted FSM state to the orphan value.
	require.NoError(t, store.SetFSMState(ctx, "LEGACYSYNCING"))

	b := newTestBlockchainForGate(t, &chaincfg.RegressionNetParams, store)

	require.NoError(t, b.Init(ctx))

	// The in-memory FSM must have been set to CATCHINGBLOCKS.
	require.Equal(t, blockchain_api.FSMStateType_CATCHINGBLOCKS.String(), b.finiteStateMachine.Current())

	// The store must have been re-written with the migrated state.
	persisted, err := store.GetFSMState(ctx)
	require.NoError(t, err)
	require.Equal(t, blockchain_api.FSMStateType_CATCHINGBLOCKS.String(), persisted)
}

// TestSendFSMEvent_RunGate_SourceState pins the source-state semantics of
// the RUN gate: the checkpoint rule applies to every valid RUN regardless of
// source state. Below the checkpoint, RUN is rejected and the FSM remains in
// its source state. At or above the checkpoint, RUN reaches RUNNING from IDLE
// or CATCHINGBLOCKS.
//
// The IDLE cases used to expect RUNNING, on the reasoning that a fresh node
// boots into CATCHINGBLOCKS so the exemption could never be reached. That made a
// safety property depend on a boot default, and it was never exhaustive: a store
// persisted in IDLE before that default changed, or a node stopped from RUNNING
// and then overtaken by a checkpoint bump, both sit in IDLE below the checkpoint.
func TestSendFSMEvent_RunGate_SourceState(t *testing.T) {
	ctx := context.Background()
	highest := HighestCheckpointHeight(chaincfg.MainNetParams.Checkpoints)
	require.Greater(t, highest, uint32(0))

	tests := []struct {
		name       string
		startState blockchain_api.FSMStateType
		tipHeight  uint32
		wantErr    bool
		wantState  blockchain_api.FSMStateType
		wantSubstr string
	}{
		{
			name:       "fresh boot IDLE with tip 0 below checkpoint rejects",
			startState: blockchain_api.FSMStateType_IDLE,
			tipHeight:  0,
			wantErr:    true,
			wantState:  blockchain_api.FSMStateType_IDLE,
			wantSubstr: "below highest checkpoint",
		},
		{
			name:       "IDLE with tip 100 still below checkpoint rejects",
			startState: blockchain_api.FSMStateType_IDLE,
			tipHeight:  100,
			wantErr:    true,
			wantState:  blockchain_api.FSMStateType_IDLE,
			wantSubstr: "below highest checkpoint",
		},
		{
			// A node that is genuinely caught up must still reach RUNNING
			// directly from IDLE, otherwise removing the exemption would cost
			// every stopped node a pointless trip through catch-up.
			name:       "IDLE with tip above checkpoint reaches RUNNING",
			startState: blockchain_api.FSMStateType_IDLE,
			tipHeight:  highest + 50,
			wantErr:    false,
			wantState:  blockchain_api.FSMStateType_RUNNING,
		},
		{
			name:       "CATCHINGBLOCKS below checkpoint rejects",
			startState: blockchain_api.FSMStateType_CATCHINGBLOCKS,
			tipHeight:  highest - 1,
			wantErr:    true,
			wantState:  blockchain_api.FSMStateType_CATCHINGBLOCKS,
			wantSubstr: "below highest checkpoint",
		},
		{
			name:       "CATCHINGBLOCKS above checkpoint succeeds",
			startState: blockchain_api.FSMStateType_CATCHINGBLOCKS,
			tipHeight:  highest + 50,
			wantErr:    false,
			wantState:  blockchain_api.FSMStateType_RUNNING,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := &fsmGateStore{}
			hdr := &model.BlockHeader{HashPrevBlock: &chainhash.Hash{}, HashMerkleRoot: &chainhash.Hash{}}
			meta := &model.BlockHeaderMeta{Height: tt.tipHeight}
			// The gate now runs for every RUN, so GetBestBlockHeader is consulted
			// for IDLE sources too. Maybe() is kept because networks with no
			// checkpoints short-circuit before the store read.
			store.On("GetBestBlockHeader", mock.Anything).Return(hdr, meta, nil).Maybe()

			b := newTestBlockchainForGate(t, &chaincfg.MainNetParams, store)
			b.settings.BlockChain.FSMStateChangeDelay = 0
			b.finiteStateMachine = b.NewFiniteStateMachine()
			b.finiteStateMachine.SetState(tt.startState.String())

			req := &blockchain_api.SendFSMEventRequest{Event: blockchain_api.FSMEventType_RUN}
			resp, err := b.SendFSMEvent(ctx, req)

			if tt.wantErr {
				require.Error(t, err)
				if tt.wantSubstr != "" {
					require.Contains(t, err.Error(), tt.wantSubstr)
				}
				require.Equal(t, tt.wantState.String(), b.finiteStateMachine.Current(),
					"FSM must remain in source state when gate rejects")
			} else {
				require.NoError(t, err)
				require.NotNil(t, resp)
				require.Equal(t, tt.wantState, resp.State)
				require.Equal(t, tt.wantState.String(), b.finiteStateMachine.Current())

				// Every accepted transition must also reach the store, so a restart
				// resumes where the node actually got to. resp.State and the FSM are
				// the same in-memory object, so neither can show this.
				persisted, perr := store.GetFSMState(ctx)
				require.NoError(t, perr)
				require.Equal(t, tt.wantState.String(), persisted,
					"reached state must be persisted, not just set in memory")
			}
		})
	}
}

// TestSendFSMEvent_RunFromIdle_NoCheckpoints pins that networks with no
// hard-coded checkpoints are untouched by the gate change. regtest is the case
// that matters: the guard short-circuits on highest == 0 before it reads the
// chain tip, so RUN from IDLE still goes straight to RUNNING and local dev keeps
// working with a single setfsmstate --fsmstate running command (block assembly
// refuses to hand out a mining candidate outside RUNNING, so dev needs to get
// there).
func TestSendFSMEvent_RunFromIdle_NoCheckpoints(t *testing.T) {
	ctx := context.Background()
	require.Zero(t, HighestCheckpointHeight(chaincfg.RegressionNetParams.Checkpoints))

	store := &fsmGateStore{}
	// Deliberately no GetBestBlockHeader expectation: the guard must not reach
	// the store when the network defines no checkpoints.

	b := newTestBlockchainForGate(t, &chaincfg.RegressionNetParams, store)
	b.settings.BlockChain.FSMStateChangeDelay = 0
	b.finiteStateMachine = b.NewFiniteStateMachine()
	b.finiteStateMachine.SetState(blockchain_api.FSMStateType_IDLE.String())

	resp, err := b.SendFSMEvent(ctx, &blockchain_api.SendFSMEventRequest{
		Event: blockchain_api.FSMEventType_RUN,
	})
	require.NoError(t, err)
	require.Equal(t, blockchain_api.FSMStateType_RUNNING, resp.State)
	require.Equal(t, blockchain_api.FSMStateType_RUNNING.String(), b.finiteStateMachine.Current())
	store.AssertNotCalled(t, "GetBestBlockHeader", mock.Anything)
}

// TestRunFromIdle_BelowCheckpointRejectsAndPreservesIdle protects the Run RPC
// contract: a nil error means the node reached RUNNING. A below-checkpoint node
// must therefore reject RUN and remain durably parked in IDLE rather than
// silently entering the irreversible CATCHINGBLOCKS state.
func TestRunFromIdle_BelowCheckpointRejectsAndPreservesIdle(t *testing.T) {
	ctx := context.Background()
	highest := HighestCheckpointHeight(chaincfg.MainNetParams.Checkpoints)
	require.Greater(t, highest, uint32(0))

	store := &fsmGateStore{}
	hdr := &model.BlockHeader{HashPrevBlock: &chainhash.Hash{}, HashMerkleRoot: &chainhash.Hash{}}
	store.On("GetBestBlockHeader", mock.Anything).Return(hdr, &model.BlockHeaderMeta{Height: 0}, nil)

	b := newTestBlockchainForGate(t, &chaincfg.MainNetParams, store)
	b.settings.BlockChain.FSMStateChangeDelay = 0
	b.finiteStateMachine = b.NewFiniteStateMachine()
	b.finiteStateMachine.SetState(blockchain_api.FSMStateType_IDLE.String())
	require.NoError(t, store.SetFSMState(ctx, blockchain_api.FSMStateType_IDLE.String()))

	_, err := b.Run(ctx, nil)
	require.Error(t, err)
	require.ErrorContains(t, err, "below highest checkpoint")
	require.ErrorContains(t, err, "setfsmstate --fsmstate catchingblocks")
	require.Equal(t, blockchain_api.FSMStateType_IDLE.String(), b.finiteStateMachine.Current())

	persisted, perr := store.GetFSMState(ctx)
	require.NoError(t, perr)
	require.Equal(t, blockchain_api.FSMStateType_IDLE.String(), persisted,
		"rejected RUN must not change the persisted state")

	store.AssertExpectations(t)
}

// TestSendFSMEvent_InvalidRunSkipsCheckpointRead protects error ordering for
// direct SendFSMEvent callers. RUN is not a valid event from RUNNING, so the FSM
// error must be returned without consulting the chain tip while holding fsmMu.
func TestSendFSMEvent_InvalidRunSkipsCheckpointRead(t *testing.T) {
	ctx := context.Background()
	store := &fsmGateStore{}
	hdr := &model.BlockHeader{HashPrevBlock: &chainhash.Hash{}, HashMerkleRoot: &chainhash.Hash{}}
	store.On("GetBestBlockHeader", mock.Anything).Return(hdr, &model.BlockHeaderMeta{Height: 0}, nil).Maybe()

	b := newTestBlockchainForGate(t, &chaincfg.MainNetParams, store)
	b.settings.BlockChain.FSMStateChangeDelay = 0
	b.finiteStateMachine = b.NewFiniteStateMachine()
	b.finiteStateMachine.SetState(blockchain_api.FSMStateType_RUNNING.String())

	_, err := b.SendFSMEvent(ctx, &blockchain_api.SendFSMEventRequest{
		Event: blockchain_api.FSMEventType_RUN,
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "FSM event RUN rejected in state RUNNING")
	require.Equal(t, blockchain_api.FSMStateType_RUNNING.String(), b.finiteStateMachine.Current())
	store.AssertNotCalled(t, "GetBestBlockHeader", mock.Anything)
}

// TestSendFSMEvent_RunFromIdle_StoreErrorRejects pins fail-closed behavior when
// the chain tip cannot be read. The operator must see the store error and the
// node must remain in IDLE.
func TestSendFSMEvent_RunFromIdle_StoreErrorRejects(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name       string
		hdr        *model.BlockHeader
		meta       *model.BlockHeaderMeta
		storeErr   error
		wantSubstr string
	}{
		{
			name:       "store read fails",
			storeErr:   errors.NewStorageError("boom"),
			wantSubstr: "cannot read best block header",
		},
		{
			name:       "meta unavailable",
			hdr:        &model.BlockHeader{HashPrevBlock: &chainhash.Hash{}, HashMerkleRoot: &chainhash.Hash{}},
			meta:       nil,
			wantSubstr: "best block header meta unavailable",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := &fsmGateStore{}
			store.On("GetBestBlockHeader", mock.Anything).Return(tt.hdr, tt.meta, tt.storeErr)

			b := newTestBlockchainForGate(t, &chaincfg.MainNetParams, store)
			b.settings.BlockChain.FSMStateChangeDelay = 0
			b.finiteStateMachine = b.NewFiniteStateMachine()
			b.finiteStateMachine.SetState(blockchain_api.FSMStateType_IDLE.String())

			_, err := b.SendFSMEvent(ctx, &blockchain_api.SendFSMEventRequest{
				Event: blockchain_api.FSMEventType_RUN,
			})
			require.Error(t, err)
			require.Contains(t, err.Error(), tt.wantSubstr)
			require.Equal(t, blockchain_api.FSMStateType_IDLE.String(), b.finiteStateMachine.Current(),
				"FSM must stay in IDLE when the gate cannot reach a verdict")
		})
	}
}
