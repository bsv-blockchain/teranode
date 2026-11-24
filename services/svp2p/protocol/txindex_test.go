package protocol

import (
	"context"
	"io"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	terrors "github.com/bsv-blockchain/teranode/errors"
	"github.com/stretchr/testify/require"
)

// fakeTxIndex is a minimal TxIndex used only to prove the manager seam wires
// through to whatever bridge supplies, without exercising bridge itself.
type fakeTxIndex struct {
	matchHashes    []*chainhash.Hash
	matchCollision bool
}

func (f *fakeTxIndex) Match(_, _ uint64, _ []uint64) ([]*chainhash.Hash, bool) {
	return f.matchHashes, f.matchCollision
}

func (f *fakeTxIndex) Open(_ context.Context, _ chainhash.Hash) (io.ReadCloser, uint64, error) {
	return nil, 0, ErrTxUnknown
}

// TestErrTxUnknown_IsNotFound guards the sentinel's error code, since Task
// 6/8's compact-block reconstruction dispatches on it directly.
func TestErrTxUnknown_IsNotFound(t *testing.T) {
	require.Error(t, ErrTxUnknown)
	require.True(t, terrors.Is(ErrTxUnknown, terrors.ErrNotFound))
}

// TestPeerManager_TxIndex_DefaultsNil guards the compact-blocks off-by-default
// path: a manager that never had SetTxIndex called must report a nil index,
// since Server only calls it when legacy_compactBlocks is on.
func TestPeerManager_TxIndex_DefaultsNil(t *testing.T) {
	m := newTestManager(t, nil)

	require.Nil(t, m.txIndex())
}

// TestPeerManager_SetTxIndex_RoundTrips guards the seam itself: whatever
// bridge hands the manager through SetTxIndex must be exactly what txIndex()
// returns.
func TestPeerManager_SetTxIndex_RoundTrips(t *testing.T) {
	m := newTestManager(t, nil)

	idx := &fakeTxIndex{}
	m.SetTxIndex(idx)

	require.Same(t, idx, m.txIndex())
}
