package aerospike

import (
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/stretchr/testify/require"
)

func TestParentOutputAnswer(t *testing.T) {
	script := bscript.NewFromBytes([]byte{0x76, 0xa9})
	parent := &bt.Tx{Outputs: []*bt.Output{{Satoshis: 10, LockingScript: script}, nil}}

	t.Run("missing master record is TxNotFound", func(t *testing.T) {
		a := parentOutputAnswer(0, nil, errors.NewTxNotFoundError("gone"), false, nil, nil)
		require.Equal(t, utxo.ParentOutputTxNotFound, a.Status)
		require.NoError(t, a.Err)
	})

	t.Run("missing external blob is Err, never TxNotFound", func(t *testing.T) {
		a := parentOutputAnswer(0, nil, errors.NewTxNotFoundError("blob gone"), true, nil, nil)
		require.Equal(t, utxo.ParentOutputUnknown, a.Status)
		require.Error(t, a.Err)
	})

	t.Run("a read fault is Err, never TxNotFound", func(t *testing.T) {
		a := parentOutputAnswer(0, nil, errors.NewProcessingError("timeout"), false, nil, nil)
		require.Equal(t, utxo.ParentOutputUnknown, a.Status)
		require.Error(t, a.Err)
	})

	t.Run("an unparseable blockHeights bin is Err", func(t *testing.T) {
		a := parentOutputAnswer(0, parent, nil, false, nil, errors.NewStorageError("bad heights"))
		require.Equal(t, utxo.ParentOutputUnknown, a.Status)
		require.Error(t, a.Err)
	})

	t.Run("an inline record with no outputs bin is Err", func(t *testing.T) {
		a := parentOutputAnswer(0, &bt.Tx{}, nil, false, nil, nil)
		require.Equal(t, utxo.ParentOutputUnknown, a.Status)
		require.Error(t, a.Err)
	})

	t.Run("index past the end is NoSuchIndex", func(t *testing.T) {
		a := parentOutputAnswer(2, parent, nil, false, nil, nil)
		require.Equal(t, utxo.ParentOutputNoSuchIndex, a.Status)
		require.NoError(t, a.Err)
	})

	t.Run("a nil entry in an inline record is NoSuchIndex, as the seeder leaves spent outputs", func(t *testing.T) {
		a := parentOutputAnswer(1, parent, nil, false, nil, nil)
		require.Equal(t, utxo.ParentOutputNoSuchIndex, a.Status)
		require.NoError(t, a.Err)
	})

	t.Run("an output an external reconstruction dropped is NoSuchIndex", func(t *testing.T) {
		a := parentOutputAnswer(1, parent, nil, true, nil, nil)
		require.Equal(t, utxo.ParentOutputNoSuchIndex, a.Status)
		require.NoError(t, a.Err)
	})

	t.Run("not mined", func(t *testing.T) {
		a := parentOutputAnswer(0, parent, nil, false, nil, nil)
		require.Equal(t, utxo.ParentOutputNotMined, a.Status)
		require.Equal(t, uint64(10), a.Satoshis)
		require.Same(t, script, a.LockingScript)
	})

	t.Run("mined reports the lowest recorded height", func(t *testing.T) {
		a := parentOutputAnswer(0, parent, nil, false, []uint32{105, 103, 104}, nil)
		require.Equal(t, utxo.ParentOutputMined, a.Status)
		require.Equal(t, uint32(103), a.Height)
	})
}
