package validator

import (
	"context"
	"testing"

	bt "github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

// createOptionsSpyStore records the options a SpendAndCreate was issued with.
type createOptionsSpyStore struct {
	utxo.Store
	got utxo.CreateOptions
}

func (s *createOptionsSpyStore) SpendAndCreate(_ context.Context, _ *bt.Tx, _ uint32, opts ...utxo.CreateOption) (*meta.Data, []*utxo.Spend, error) {
	s.got = utxo.CreateOptions{}

	for _, apply := range opts {
		apply(&s.got)
	}

	return &meta.Data{}, nil, nil
}

// TestSpendAndCreateLocksRecordsKeptOutOfTheTemplate pins when a record is created Locked.
//
// A transaction handed to block assembly is Locked until the handoff succeeds. A block-context
// transaction kept out of the template because block assembly is at its in-memory limit is
// unmined and outside the template until its block is processed; created unlocked, a child
// arriving through any ingress point could enter the template without its parent if that block
// lost a fork race. LockUnmined keeps it unspendable by an ordinary child until then.
func TestSpendAndCreateLocksRecordsKeptOutOfTheTemplate(t *testing.T) {
	tests := []struct {
		name               string
		addToBlockAssembly bool
		options            []Option
		wantLocked         bool
	}{
		{"handed to block assembly is locked until the handoff succeeds", true, nil, true},
		{"kept out of the template with no reason to lock is unlocked, as before", false, nil, false},
		{"kept out of the template while block assembly is full is locked", false, []Option{WithLockUnmined(true)}, true},
		{"spend only never creates a record, so there is nothing to lock", false, []Option{WithLockUnmined(true), WithSkipUtxoCreation(true)}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			spy := &createOptionsSpyStore{}
			v := &Validator{logger: ulogger.TestLogger{}, utxoStore: spy}

			_, _, err := v.spendAndCreateInUtxoStore(context.Background(), bt.NewTx(), 100, tt.addToBlockAssembly, ProcessOptions(tt.options...))
			require.NoError(t, err)
			require.Equal(t, tt.wantLocked, spy.got.Locked)
		})
	}
}

// The option has to survive the gRPC round trip, or a remote validator silently creates unlocked
// records and the protection exists only in single-process deployments.
func TestLockUnminedSurvivesTheWire(t *testing.T) {
	req := buildValidateTxRequest([]byte{1}, 1, ProcessOptions(WithLockUnmined(true)))
	require.True(t, req.GetLockUnmined())

	opts, err := optionsFromValidateRequest(req)
	require.NoError(t, err)
	require.True(t, opts.LockUnmined, "the server must read the field back into the options")

	req = buildValidateTxRequest([]byte{1}, 1, ProcessOptions())
	require.False(t, req.GetLockUnmined())

	opts, err = optionsFromValidateRequest(req)
	require.NoError(t, err)
	require.False(t, opts.LockUnmined)
}
