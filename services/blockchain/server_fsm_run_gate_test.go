package blockchain

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/ulogger"
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
		{name: "mainnet params", in: chaincfg.MainNetParams.Checkpoints, want: 938000},
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
// guardRunBelowHighestCheckpoint to run: store + settings + logger.
func newTestBlockchainForGate(t *testing.T, params *chaincfg.Params, store *fsmGateStore) *Blockchain {
	t.Helper()
	return &Blockchain{
		logger:   ulogger.TestLogger{},
		store:    store,
		settings: &settings.Settings{ChainCfgParams: params},
	}
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
