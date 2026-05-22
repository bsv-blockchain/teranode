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
			wantSubstr: "refusing RUN",
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
		require.NoError(t, b.guardRunBelowHighestCheckpoint(context.Background()))
	})

	t.Run("nil ChainCfgParams", func(t *testing.T) {
		b := &Blockchain{logger: ulogger.TestLogger{}, settings: &settings.Settings{}}
		require.NoError(t, b.guardRunBelowHighestCheckpoint(context.Background()))
	})
}

// TestSendFSMEvent_RunGate_SourceState pins the source-state semantics of
// the RUN gate added in PR #844 and fixed here: IDLE → RUNNING is the boot
// path and must succeed even when the local tip sits below the highest
// checkpoint (a fresh node has tip 0 and nothing else can move the FSM
// forward), while LEGACYSYNCING → RUNNING and CATCHINGBLOCKS → RUNNING
// claim "I'm caught up" and must still be rejected when the tip is below
// the checkpoint.
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
			name:       "fresh boot IDLE with tip 0 below checkpoint succeeds",
			startState: blockchain_api.FSMStateType_IDLE,
			tipHeight:  0,
			wantErr:    false,
			wantState:  blockchain_api.FSMStateType_RUNNING,
		},
		{
			name:       "IDLE with tip 100 still below checkpoint succeeds",
			startState: blockchain_api.FSMStateType_IDLE,
			tipHeight:  100,
			wantErr:    false,
			wantState:  blockchain_api.FSMStateType_RUNNING,
		},
		{
			name:       "LEGACYSYNCING below checkpoint rejects",
			startState: blockchain_api.FSMStateType_LEGACYSYNCING,
			tipHeight:  highest - 1,
			wantErr:    true,
			wantState:  blockchain_api.FSMStateType_LEGACYSYNCING,
			wantSubstr: "refusing RUN",
		},
		{
			name:       "LEGACYSYNCING at checkpoint succeeds",
			startState: blockchain_api.FSMStateType_LEGACYSYNCING,
			tipHeight:  highest,
			wantErr:    false,
			wantState:  blockchain_api.FSMStateType_RUNNING,
		},
		{
			name:       "CATCHINGBLOCKS below checkpoint rejects",
			startState: blockchain_api.FSMStateType_CATCHINGBLOCKS,
			tipHeight:  highest - 1,
			wantErr:    true,
			wantState:  blockchain_api.FSMStateType_CATCHINGBLOCKS,
			wantSubstr: "refusing RUN",
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
			// GetBestBlockHeader is only consulted when the gate runs (non-IDLE
			// sources). Always-on Maybe lets IDLE tests skip the call without
			// failing strict expectations.
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
			}
		})
	}
}
