package subtreeprocessor

import (
	"sync/atomic"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
)

// txMapOpCounts is how many of each operation a countingTxMap served. inserted counts the
// SetIfNotExists calls that inserted.
type txMapOpCounts struct {
	set, setIfNotExists, inserted, exists, get, delete int64
}

// countingTxMap wraps a TxInpointsMap and counts its operations.
//
// The processor type-asserts its map to *SplitTxInpointsMap or *DiskTxMap for the bulk paths, and a
// wrapper fails those assertions, so installing one changes which path runs. Use it only where the
// code under test goes through the TxInpointsMap interface.
type countingTxMap struct {
	TxInpointsMap

	set, setIfNotExists, inserted, exists, get, delete atomic.Int64
}

func newCountingTxMap(inner TxInpointsMap) *countingTxMap {
	return &countingTxMap{TxInpointsMap: inner}
}

func (m *countingTxMap) Set(hash chainhash.Hash, inpoints *subtreepkg.TxInpoints) {
	m.set.Add(1)
	m.TxInpointsMap.Set(hash, inpoints)
}

func (m *countingTxMap) SetIfNotExists(hash chainhash.Hash, inpoints *subtreepkg.TxInpoints) (*subtreepkg.TxInpoints, bool) {
	m.setIfNotExists.Add(1)

	p, inserted := m.TxInpointsMap.SetIfNotExists(hash, inpoints)
	if inserted {
		m.inserted.Add(1)
	}

	return p, inserted
}

func (m *countingTxMap) Exists(hash chainhash.Hash) bool {
	m.exists.Add(1)
	return m.TxInpointsMap.Exists(hash)
}

func (m *countingTxMap) Get(hash chainhash.Hash) (*subtreepkg.TxInpoints, bool) {
	m.get.Add(1)
	return m.TxInpointsMap.Get(hash)
}

func (m *countingTxMap) Delete(hash chainhash.Hash) bool {
	m.delete.Add(1)
	return m.TxInpointsMap.Delete(hash)
}

func (m *countingTxMap) counts() txMapOpCounts {
	return txMapOpCounts{
		set:            m.set.Load(),
		setIfNotExists: m.setIfNotExists.Load(),
		inserted:       m.inserted.Load(),
		exists:         m.exists.Load(),
		get:            m.get.Load(),
		delete:         m.delete.Load(),
	}
}

func (m *countingTxMap) resetCounts() {
	for _, c := range []*atomic.Int64{&m.set, &m.setIfNotExists, &m.inserted, &m.exists, &m.get, &m.delete} {
		c.Store(0)
	}
}
