package netsync

import (
	"bytes"
	"os"
	"runtime"
	"strconv"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/legacy/blockchain"
	"github.com/bsv-blockchain/teranode/services/legacy/bsvutil"
)

func buildProbeBlock(tb testing.TB) *wire.MsgBlock {
	tb.Helper()

	txs := buildBenchTxs(tb)
	blk := wire.NewMsgBlock(&wire.BlockHeader{Version: 1, Bits: 0x1d00ffff})

	for _, t := range txs {
		if err := blk.AddTransaction(t.MsgTx()); err != nil {
			tb.Fatal(err)
		}
	}

	return blk
}

// Leaves: what BuildMerkleTreeStore's first loop pays through a FRESH
// bsvutil.Block wrapper, which is what validateParkCandidate builds.
func BenchmarkParkLeavesOnly(b *testing.B) {
	blk := buildProbeBlock(b)
	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		wrapped := bsvutil.NewBlock(blk).Transactions()
		var sink byte
		for _, t := range wrapped {
			h := t.Hash()
			sink ^= h[0]
		}
		if sink == 0xff && len(wrapped) == 0 {
			b.Fatal("no")
		}
	}
}

// Tree above the leaves: leaves pre-memoized, so only the 64-byte internal
// hashing and the array allocation are timed.
func BenchmarkParkTreeAboveLeaves(b *testing.B) {
	blk := buildProbeBlock(b)
	wrapped := bsvutil.NewBlock(blk).Transactions()
	for _, t := range wrapped {
		_ = t.Hash()
	}
	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		m := blockchain.BuildMerkleTreeStore(wrapped)
		if m[len(m)-1] == nil {
			b.Fatal("nil root")
		}
	}
}

// Full merkle rebuild exactly as validateParkCandidate does it.
func BenchmarkParkMerkleFull(b *testing.B) {
	blk := buildProbeBlock(b)
	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		m := blockchain.BuildMerkleTreeStore(bsvutil.NewBlock(blk).Transactions())
		if m[len(m)-1] == nil {
			b.Fatal("nil root")
		}
	}
}

// Header hash + PoW target check only, i.e. checks two and three.
func BenchmarkParkHeaderAndPoW(b *testing.B) {
	blk := buildProbeBlock(b)
	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		got := blk.BlockHash()

		var hb bytes.Buffer
		if err := blk.Header.Serialize(&hb); err != nil {
			b.Fatal(err)
		}
		hdr, err := model.NewBlockHeaderFromBytes(hb.Bytes())
		if err != nil {
			b.Fatal(err)
		}
		_, _, _ = hdr.HasMetTargetDifficulty()
		if got.IsEqual(&chainhash.Hash{}) {
			b.Fatal("zero")
		}
	}
}

// The same merkle rebuild with the node's heap condition reproduced: a
// pointer-dense live heap near GOMEMLIMIT. Set GOMEMLIMIT and
// TERANODE_BENCH_BALLAST_SETS.
func BenchmarkParkMerkleFullUnderMemLimit(b *testing.B) {
	sets := 0
	if v := os.Getenv("TERANODE_BENCH_BALLAST_SETS"); v != "" {
		var err error
		sets, err = strconv.Atoi(v)
		if err != nil {
			b.Fatal(err)
		}
	}

	if sets <= 0 {
		b.Skip("set TERANODE_BENCH_BALLAST_SETS (and GOMEMLIMIT) to run this")
	}

	blk := buildProbeBlock(b)

	blockBallast := make([]*wire.MsgBlock, sets)
	for s := 0; s < sets; s++ {
		blockBallast[s] = buildProbeBlock(b)
	}

	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	b.Logf("live heap %.2f GiB, GOMEMLIMIT=%s", float64(ms.HeapAlloc)/(1<<30), os.Getenv("GOMEMLIMIT"))

	gcBefore := ms.NumGC
	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		m := blockchain.BuildMerkleTreeStore(bsvutil.NewBlock(blk).Transactions())
		if m[len(m)-1] == nil {
			b.Fatal("nil root")
		}
	}

	b.StopTimer()
	runtime.ReadMemStats(&ms)
	b.Logf("GC cycles during loop: %d, ballast blocks %d", ms.NumGC-gcBefore, len(blockBallast))
}
