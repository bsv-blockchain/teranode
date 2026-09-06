package blockchain

import (
	"context"
	"net/url"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockchain/blockchain_api"
	"github.com/bsv-blockchain/teranode/settings"
	blockchainstore "github.com/bsv-blockchain/teranode/stores/blockchain"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

const blockAssemblerStateKey = "BlockAssembler"

const bootStateTestCoinbase = "01000000010000000000000000000000000000000000000000000000000000000000000000ffffffff17030100002f6d312d65752f29c267ffea1adb87f33b398fffffffff03ac505763000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88acaa505763000000001976a9143c22b6d9ba7b50b6d6e615c69d11ecb2ba3db14588acaa505763000000001976a914b7177c7deb43f3869eabc25cfd9f618215f34d5588ac00000000"

type bootStateFaultStore struct {
	blockchainstore.Store
	getFSMStateErr error
	setFSMStateErr error
}

func (s *bootStateFaultStore) GetFSMState(ctx context.Context) (string, error) {
	if s.getFSMStateErr != nil {
		return "", s.getFSMStateErr
	}

	return s.Store.GetFSMState(ctx)
}

func (s *bootStateFaultStore) SetFSMState(ctx context.Context, state string) error {
	if s.setFSMStateErr != nil {
		return s.setFSMStateErr
	}

	return s.Store.SetFSMState(ctx, state)
}

func newBootStateBlockchain(t *testing.T, configured string, params *chaincfg.Params) (*Blockchain, blockchainstore.Store) {
	t.Helper()

	storeURL, err := url.Parse("sqlitememory://")
	require.NoError(t, err)

	tSettings := &settings.Settings{ChainCfgParams: params}
	tSettings.BlockChain.InitializeNodeInState = configured

	store, err := blockchainstore.NewStore(ulogger.TestLogger{}, storeURL, tSettings)
	require.NoError(t, err)

	b, err := New(context.Background(), ulogger.TestLogger{}, tSettings, store, nil)
	require.NoError(t, err)

	return b, store
}

func bootStateParamsWithCheckpoint(height int32) *chaincfg.Params {
	params := chaincfg.RegressionNetParams
	params.Name = "boot-state-test"
	params.Checkpoints = []chaincfg.Checkpoint{{
		Height: height,
		Hash:   &chainhash.Hash{1},
	}}

	return &params
}

func storeBootStateTestBlock(t *testing.T, store blockchainstore.Store, params *chaincfg.Params) {
	t.Helper()

	coinbase, err := bt.NewTxFromString(bootStateTestCoinbase)
	require.NoError(t, err)

	block := &model.Block{
		Header: &model.BlockHeader{
			Version:        1,
			HashPrevBlock:  params.GenesisHash,
			HashMerkleRoot: &chainhash.Hash{},
			Timestamp:      1231006506,
			Bits:           model.NBit{0xff, 0xff, 0x00, 0x1d},
			Nonce:          1,
		},
		CoinbaseTx:       coinbase,
		TransactionCount: 1,
		Subtrees:         []*chainhash.Hash{},
	}

	_, height, err := store.StoreBlock(context.Background(), block, "")
	require.NoError(t, err)
	require.Equal(t, uint32(1), height)
}

func TestInit_FreshNodeUsesConfiguredBootState(t *testing.T) {
	tests := []struct {
		name       string
		configured string
		want       blockchain_api.FSMStateType
	}{
		{name: "unset keeps automatic catchup", want: blockchain_api.FSMStateType_CATCHINGBLOCKS},
		{name: "IDLE parks production node", configured: "IDLE", want: blockchain_api.FSMStateType_IDLE},
		{name: "CATCHINGBLOCKS remains explicit option", configured: "CATCHINGBLOCKS", want: blockchain_api.FSMStateType_CATCHINGBLOCKS},
		{name: "RUNNING remains available without checkpoints", configured: "RUNNING", want: blockchain_api.FSMStateType_RUNNING},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			b, store := newBootStateBlockchain(t, tt.configured, &chaincfg.RegressionNetParams)

			persisted, err := store.GetFSMState(ctx)
			require.NoError(t, err)
			require.Empty(t, persisted)

			require.NoError(t, b.Init(ctx))
			require.Equal(t, tt.want.String(), b.finiteStateMachine.Current())

			persisted, err = store.GetFSMState(ctx)
			require.NoError(t, err)
			require.Equal(t, tt.want.String(), persisted)
		})
	}
}

func TestInit_InvalidConfiguredBootStateFails(t *testing.T) {
	for _, configured := range []string{"idle", "Running", "FOO", "LEGACYSYNCING", "1"} {
		t.Run(configured, func(t *testing.T) {
			b, _ := newBootStateBlockchain(t, configured, &chaincfg.RegressionNetParams)

			err := b.Init(context.Background())
			require.Error(t, err)
			require.ErrorContains(t, err, "blockchain_initializeNodeInState")
			require.ErrorContains(t, err, configured)
			require.ErrorContains(t, err, "IDLE")
			require.ErrorContains(t, err, "CATCHINGBLOCKS")
			require.ErrorContains(t, err, "RUNNING")
		})
	}
}

func TestInit_InvalidConfiguredBootStateFailsWithPersistedState(t *testing.T) {
	ctx := context.Background()
	b, store := newBootStateBlockchain(t, "INVALID", &chaincfg.RegressionNetParams)
	require.NoError(t, store.SetFSMState(ctx, "IDLE"))

	err := b.Init(ctx)
	require.Error(t, err)
	require.ErrorContains(t, err, "blockchain_initializeNodeInState")
}

func TestFSMBootState_NilSettingsUsesAutomaticCatchup(t *testing.T) {
	b := &Blockchain{}

	state, err := b.fsmBootState()
	require.NoError(t, err)
	require.Equal(t, "CATCHINGBLOCKS", state)
}

func TestInit_LocalTestStartStateOverridesConfiguredBootState(t *testing.T) {
	ctx := context.Background()
	b, store := newBootStateBlockchain(t, "INVALID", &chaincfg.RegressionNetParams)
	b.localTestStartState = "IDLE"

	require.NoError(t, b.Init(ctx))
	require.Equal(t, "IDLE", b.finiteStateMachine.Current())

	persisted, err := store.GetFSMState(ctx)
	require.NoError(t, err)
	require.Equal(t, "IDLE", persisted)
}

func TestInit_LocalTestStartStateWriteFailureAbortsStartup(t *testing.T) {
	b, store := newBootStateBlockchain(t, "INVALID", &chaincfg.RegressionNetParams)
	b.localTestStartState = "IDLE"
	b.store = &bootStateFaultStore{
		Store:          store,
		setFSMStateErr: errors.NewError("forced local test FSM state write failure"),
	}

	err := b.Init(context.Background())
	require.Error(t, err)
	require.ErrorContains(t, err, "failed to persist local test FSM state")
	require.ErrorContains(t, err, "forced local test FSM state write failure")
	require.NotContains(t, err.Error(), "%v")
	require.NotContains(t, err.Error(), "%!")
}

func TestInit_PersistedStateOverridesConfiguredBootState(t *testing.T) {
	ctx := context.Background()
	b, store := newBootStateBlockchain(t, "CATCHINGBLOCKS", &chaincfg.RegressionNetParams)
	require.NoError(t, store.SetFSMState(ctx, "IDLE"))

	require.NoError(t, b.Init(ctx))
	require.Equal(t, "IDLE", b.finiteStateMachine.Current())

	persisted, err := store.GetFSMState(ctx)
	require.NoError(t, err)
	require.Equal(t, "IDLE", persisted)
}

func TestInit_SeededStoreUsesConfiguredBootState(t *testing.T) {
	ctx := context.Background()
	b, store := newBootStateBlockchain(t, "IDLE", &chaincfg.RegressionNetParams)
	require.NoError(t, store.SetState(ctx, blockAssemblerStateKey, []byte("seeded-checkpoint")))

	persisted, err := store.GetFSMState(ctx)
	require.NoError(t, err)
	require.Empty(t, persisted)

	require.NoError(t, b.Init(ctx))
	require.Equal(t, "IDLE", b.finiteStateMachine.Current())

	blockAssemblerState, err := store.GetState(ctx, blockAssemblerStateKey)
	require.NoError(t, err)
	require.Equal(t, []byte("seeded-checkpoint"), blockAssemblerState)
}

func TestInit_ConfiguredRunningHonorsCheckpointGate(t *testing.T) {
	tests := []struct {
		name             string
		checkpointHeight int32
		wantErr          bool
	}{
		{name: "below checkpoint rejects", checkpointHeight: 2, wantErr: true},
		{name: "at checkpoint starts running", checkpointHeight: 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			params := bootStateParamsWithCheckpoint(tt.checkpointHeight)
			b, store := newBootStateBlockchain(t, "RUNNING", params)
			storeBootStateTestBlock(t, store, params)

			err := b.Init(ctx)
			if tt.wantErr {
				require.Error(t, err)
				require.ErrorContains(t, err, "below highest checkpoint")

				persisted, getErr := store.GetFSMState(ctx)
				require.NoError(t, getErr)
				require.Empty(t, persisted, "rejected startup must not persist RUNNING")
				return
			}

			require.NoError(t, err)
			require.Equal(t, "RUNNING", b.finiteStateMachine.Current())

			persisted, getErr := store.GetFSMState(ctx)
			require.NoError(t, getErr)
			require.Equal(t, "RUNNING", persisted)
		})
	}
}

func TestInit_PersistedRunningHonorsCheckpointGate(t *testing.T) {
	tests := []struct {
		name             string
		checkpointHeight int32
		storeTip         bool
		wantState        string
	}{
		{name: "missing tip resumes catchup", checkpointHeight: 1, wantState: "CATCHINGBLOCKS"},
		{name: "below checkpoint resumes catchup", checkpointHeight: 2, storeTip: true, wantState: "CATCHINGBLOCKS"},
		{name: "at checkpoint remains running", checkpointHeight: 1, storeTip: true, wantState: "RUNNING"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			params := bootStateParamsWithCheckpoint(tt.checkpointHeight)
			b, store := newBootStateBlockchain(t, "IDLE", params)
			if tt.storeTip {
				storeBootStateTestBlock(t, store, params)
			}
			require.NoError(t, store.SetFSMState(ctx, "RUNNING"))

			require.NoError(t, b.Init(ctx))
			require.Equal(t, tt.wantState, b.finiteStateMachine.Current())

			persisted, err := store.GetFSMState(ctx)
			require.NoError(t, err)
			require.Equal(t, tt.wantState, persisted)
		})
	}
}

func TestInit_FSMStateReadFailureAbortsStartup(t *testing.T) {
	b, store := newBootStateBlockchain(t, "IDLE", &chaincfg.RegressionNetParams)
	b.store = &bootStateFaultStore{
		Store:          store,
		getFSMStateErr: errors.NewError("forced FSM state read failure"),
	}

	err := b.Init(context.Background())
	require.Error(t, err)
	require.ErrorContains(t, err, "failed to get persisted FSM state")
	require.ErrorContains(t, err, "forced FSM state read failure")
	require.NotContains(t, err.Error(), "%v")
	require.NotContains(t, err.Error(), "%!")
}

func TestInit_InitialFSMStateWriteFailureAbortsStartup(t *testing.T) {
	b, store := newBootStateBlockchain(t, "IDLE", &chaincfg.RegressionNetParams)
	b.store = &bootStateFaultStore{
		Store:          store,
		setFSMStateErr: errors.NewError("forced FSM state write failure"),
	}

	err := b.Init(context.Background())
	require.Error(t, err)
	require.ErrorContains(t, err, "failed to persist initial IDLE state")
	require.ErrorContains(t, err, "forced FSM state write failure")
	require.NotContains(t, err.Error(), "%v")
	require.NotContains(t, err.Error(), "%!")
}

func TestInit_UnsafePersistedRunningMigrationWriteFailureAbortsStartup(t *testing.T) {
	ctx := context.Background()
	params := bootStateParamsWithCheckpoint(2)
	b, store := newBootStateBlockchain(t, "IDLE", params)
	storeBootStateTestBlock(t, store, params)
	require.NoError(t, store.SetFSMState(ctx, "RUNNING"))
	b.store = &bootStateFaultStore{
		Store:          store,
		setFSMStateErr: errors.NewError("forced FSM migration write failure"),
	}

	err := b.Init(ctx)
	require.Error(t, err)
	require.ErrorContains(t, err, "failed to persist safe FSM state after RUNNING gate rejection")
	require.ErrorContains(t, err, "forced FSM migration write failure")
	require.NotContains(t, err.Error(), "%v")
	require.NotContains(t, err.Error(), "%!")

	persisted, getErr := store.GetFSMState(ctx)
	require.NoError(t, getErr)
	require.Equal(t, "RUNNING", persisted)
}

func TestInit_LegacySyncingMigrationWriteFailureAbortsStartup(t *testing.T) {
	ctx := context.Background()
	b, store := newBootStateBlockchain(t, "IDLE", &chaincfg.RegressionNetParams)
	require.NoError(t, store.SetFSMState(ctx, "LEGACYSYNCING"))
	b.store = &bootStateFaultStore{
		Store:          store,
		setFSMStateErr: errors.NewError("forced legacy FSM migration write failure"),
	}

	err := b.Init(ctx)
	require.Error(t, err)
	require.ErrorContains(t, err, "failed to persist migrated FSM state")
	require.ErrorContains(t, err, "forced legacy FSM migration write failure")
	require.NotContains(t, err.Error(), "%v")
	require.NotContains(t, err.Error(), "%!")

	persisted, getErr := store.GetFSMState(ctx)
	require.NoError(t, getErr)
	require.Equal(t, "LEGACYSYNCING", persisted)
}
