package meta

import (
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	subtree "github.com/bsv-blockchain/go-subtree"
)

// BenchmarkMetaBytes measures the allocations of serializing tx metadata to its
// cache-wire form. The txmetacache cache-repopulate path (TxMetaCache.
// BatchDecorate) calls MetaBytes once per transaction to produce a buffer it
// then copies into the cache backend and discards — so the buffer is poolable
// (Tier C). This benchmark is the per-tx baseline for that allocation.
func BenchmarkMetaBytes(b *testing.B) {
	// Common case the MetaBytes buffer sizing is tuned for: a single-parent tx.
	tx := bt.NewTx()
	if err := tx.From("0000000000000000000000000000000000000000000000000000000000000001", 0,
		"76a914000000000000000000000000000000000000000088ac", 1000); err != nil {
		b.Fatalf("build tx: %v", err)
	}

	inpoints, err := subtree.NewTxInpointsFromTx(tx)
	if err != nil {
		b.Fatalf("inpoints: %v", err)
	}

	d := &Data{
		Fee:          1000,
		SizeInBytes:  250,
		TxInpoints:   inpoints,
		BlockIDs:     []uint32{100, 101},
		BlockHeights: []uint32{100, 101},
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if _, err := d.MetaBytes(); err != nil {
			b.Fatalf("MetaBytes: %v", err)
		}
	}
}
