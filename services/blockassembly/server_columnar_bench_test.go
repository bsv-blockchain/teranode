package blockassembly

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/services/blockassembly/blockassembly_api"
)

// BenchmarkAddTxBatchColumnar_Validation measures the overhead of the offset
// validation block before the per-tx loop. Single-parent / single-vout txs,
// batch of 1000 — representative of the steady-state workload.
//
// To isolate the validation cost, the bench builds a well-formed batch once
// and re-submits it. With BlockAssembly.Disabled=true the handler returns
// immediately after validation, so the per-iteration cost is dominated by
// proto unmarshal (constant across all variants) plus our validation.
func BenchmarkAddTxBatchColumnar_Validation(b *testing.B) {
	ba, _ := setupServer(&testing.T{})
	ba.settings.BlockAssembly.Disabled = true
	ba.settings.BlockAssembly.StoreTxInpointsForSubtreeMeta = true

	const batchSize = 1000

	txidsPacked := make([]byte, batchSize*32)
	parentTxHashesPacked := make([]byte, batchSize*32) // 1 parent per tx
	fees := make([]uint64, batchSize)
	sizes := make([]uint64, batchSize)
	parentTxOffsets := make([]uint32, batchSize+1)
	voutIdxsPacked := make([]uint32, batchSize*2) // [count=1, vout=0] per tx
	voutIdxsTxOffsets := make([]uint32, batchSize+1)

	for i := 0; i < batchSize; i++ {
		txid := chainhash.Hash{byte(i), byte(i >> 8)}
		copy(txidsPacked[i*32:(i+1)*32], txid[:])

		parent := chainhash.Hash{byte(i + 1), byte(i + 1>>8)}
		copy(parentTxHashesPacked[i*32:(i+1)*32], parent[:])

		fees[i] = 1000
		sizes[i] = 250
		parentTxOffsets[i+1] = uint32(i + 1)
		voutIdxsPacked[i*2] = 1   // count
		voutIdxsPacked[i*2+1] = 0 // vout value
		voutIdxsTxOffsets[i+1] = uint32((i + 1) * 2)
	}

	req := &blockassembly_api.AddTxBatchColumnarRequest{
		TxidsPacked:          txidsPacked,
		Fees:                 fees,
		Sizes:                sizes,
		ParentTxHashesPacked: parentTxHashesPacked,
		ParentTxOffsets:      parentTxOffsets,
		VoutIdxsPacked:       voutIdxsPacked,
		VoutIdxsTxOffsets:    voutIdxsTxOffsets,
	}

	ctx := context.Background()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		if _, err := ba.AddTxBatchColumnar(ctx, req); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkOffsetValidationLoop isolates just the monotonicity + endpoint
// check cost over a 1000-tx batch's offset arrays. This is the work added
// by the security hardening — nothing else.
func BenchmarkOffsetValidationLoop(b *testing.B) {
	const txCount = 1000

	parentTxOffsets := make([]uint32, txCount+1)
	voutIdxsTxOffsets := make([]uint32, txCount+1)
	for i := 0; i <= txCount; i++ {
		parentTxOffsets[i] = uint32(i)
		voutIdxsTxOffsets[i] = uint32(i * 2)
	}

	totalParents := txCount
	voutIdxsLen := txCount * 2

	b.ResetTimer()
	b.ReportAllocs()

	for n := 0; n < b.N; n++ {
		// Mirrors the validation block in AddTxBatchColumnar verbatim, so
		// the cost reported here is exactly what the production code pays
		// per batch.
		_ = parentTxOffsets[0]
		_ = voutIdxsTxOffsets[0]
		_ = int(parentTxOffsets[txCount]) != totalParents
		_ = int(voutIdxsTxOffsets[txCount]) != voutIdxsLen

		var bad bool
		for i := 1; i <= txCount; i++ {
			if parentTxOffsets[i] < parentTxOffsets[i-1] {
				bad = true
				break
			}
			if voutIdxsTxOffsets[i] < voutIdxsTxOffsets[i-1] {
				bad = true
				break
			}
		}
		_ = bad
	}
}
