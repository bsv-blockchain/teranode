package subtreeprocessor

import (
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/stretchr/testify/require"
)

// The counting map lets a reorg test assert how much map work a path does, by count rather than by
// wall time, so a complexity regression fails deterministically in CI.
func TestCountingTxMap_CountsEachOperation(t *testing.T) {
	m := newCountingTxMap(NewSplitTxInpointsMap(4))
	h1, h2 := chainhash.Hash{1}, chainhash.Hash{2}
	p := voutZeroInpoints(chainhash.Hash{9})

	m.Set(h1, &p)
	_, inserted := m.SetIfNotExists(h1, &p)
	require.False(t, inserted)
	_, inserted = m.SetIfNotExists(h2, &p)
	require.True(t, inserted)
	require.True(t, m.Exists(h1))
	_, ok := m.Get(h2)
	require.True(t, ok)
	require.True(t, m.Delete(h1))
	require.Equal(t, 1, m.Length())

	require.Equal(t, txMapOpCounts{set: 1, setIfNotExists: 2, inserted: 1, exists: 1, get: 1, delete: 1}, m.counts())
}

func TestCountingTxMap_ResetCounts(t *testing.T) {
	m := newCountingTxMap(NewSplitTxInpointsMap(4))
	m.Exists(chainhash.Hash{1})
	m.resetCounts()

	require.Equal(t, txMapOpCounts{}, m.counts())
}
