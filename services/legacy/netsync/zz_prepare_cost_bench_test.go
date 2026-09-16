package netsync

import (
	"os"
	"runtime"
	"strconv"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	txmap "github.com/bsv-blockchain/go-tx-map"
	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/legacy/bsvutil"
)

const (
	benchTxCount   = 100001
	benchInputs    = 2
	benchOutputs   = 2
	benchSigScript = 107
	benchPkScript  = 25
)

// buildBenchTxs builds benchTxCount wire transactions with hashes already
// memoized, exactly the state createTxMap sees (HandleBlockDirect walks
// block.Transactions() and calls tx.Hash() on every one before prepareSubtrees).
func buildBenchTxs(tb testing.TB) []*bsvutil.Tx {
	tb.Helper()

	txs := make([]*bsvutil.Tx, benchTxCount)

	for i := 0; i < benchTxCount; i++ {
		msg := wire.NewMsgTx(1)

		nIn := benchInputs
		if i == 0 {
			nIn = 1
		}

		for j := 0; j < nIn; j++ {
			var h chainhash.Hash
			h[0] = byte(i)
			h[1] = byte(i >> 8)
			h[2] = byte(i >> 16)
			h[3] = byte(j)

			sig := make([]byte, benchSigScript)
			sig[0] = byte(j)

			msg.AddTxIn(&wire.TxIn{
				PreviousOutPoint: wire.OutPoint{Hash: h, Index: uint32(j)},
				SignatureScript:  sig,
				Sequence:         0xffffffff,
			})
		}

		for j := 0; j < benchOutputs; j++ {
			pk := make([]byte, benchPkScript)
			pk[0] = byte(j)
			msg.AddTxOut(&wire.TxOut{Value: 1000, PkScript: pk})
		}

		t := bsvutil.NewTx(msg)
		_ = t.Hash() // memoize, as HandleBlockDirect does
		txs[i] = t
	}

	return txs
}

// BenchmarkPrepareCreateTxMapLoop is the body of createTxMap: one bt.Tx per wire
// tx with every script cloned, plus the txMap insert. Serial, single goroutine,
// exactly as in production.
func BenchmarkPrepareCreateTxMapLoop(b *testing.B) {
	txs := buildBenchTxs(b)
	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		txMap := txmap.NewSyncedMap[chainhash.Hash, *TxMapWrapper](len(txs))
		txOrder := make([]chainhash.Hash, 0, len(txs))

		for _, wireTx := range txs {
			hashCopy := *wireTx.Hash()
			txOrder = append(txOrder, hashCopy)

			tx := &bt.Tx{}
			if err := WireTxToGoBtTx(wireTx, tx); err != nil {
				b.Fatal(err)
			}

			if !tx.IsCoinbase() {
				tx.SetTxHash(&hashCopy)
				txMap.Set(hashCopy, &TxMapWrapper{Tx: tx})
			}
		}

		if txMap.Length() == 0 {
			b.Fatal("empty")
		}
	}
}

// BenchmarkPrepareWireToBtOnly is the same loop with the map insert removed, to
// split conversion cost from map cost.
func BenchmarkPrepareWireToBtOnly(b *testing.B) {
	txs := buildBenchTxs(b)
	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		out := make([]*bt.Tx, 0, len(txs))

		for _, wireTx := range txs {
			tx := &bt.Tx{}
			if err := WireTxToGoBtTx(wireTx, tx); err != nil {
				b.Fatal(err)
			}

			out = append(out, tx)
		}

		if len(out) != len(txs) {
			b.Fatal("short")
		}
	}
}

// BenchmarkPrepareMapUnsized / Presized quantify the missing capacity hint:
// NewSyncedMap's argument is a LIMIT, not a capacity, so the backing map starts
// at zero and rehashes its way to 100k entries.
func BenchmarkPrepareMapUnsized(b *testing.B) {
	keys := make([]chainhash.Hash, benchTxCount)
	for i := range keys {
		keys[i][0] = byte(i)
		keys[i][1] = byte(i >> 8)
		keys[i][2] = byte(i >> 16)
	}

	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		m := make(map[chainhash.Hash]*TxMapWrapper)
		for i := range keys {
			m[keys[i]] = nil
		}
	}
}

func BenchmarkPrepareMapPresized(b *testing.B) {
	keys := make([]chainhash.Hash, benchTxCount)
	for i := range keys {
		keys[i][0] = byte(i)
		keys[i][1] = byte(i >> 8)
		keys[i][2] = byte(i >> 16)
	}

	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		m := make(map[chainhash.Hash]*TxMapWrapper, benchTxCount)
		for i := range keys {
			m[keys[i]] = nil
		}
	}
}

// buildBenchSlices builds the 25 filled subtrees the dedup scan and the
// RootHash loop see for a 100,001-leaf block at MaximumMerkleItemsPerSubtree=4096.
func buildBenchSlices(tb testing.TB) []*subtreepkg.Subtree {
	tb.Helper()

	subtreeSize, k, finalLeafCount, err := partitionLegacyBlock(benchTxCount, 4096)
	if err != nil {
		tb.Fatal(err)
	}

	tb.Logf("partition: subtreeSize=%d K=%d final=%d", subtreeSize, k, finalLeafCount)

	slices := make([]*subtreepkg.Subtree, k)

	next := 0

	for i := 0; i < k; i++ {
		capacity := subtreeSize
		if i == k-1 && k > 1 && finalLeafCount < subtreeSize {
			capacity = finalLeafCount
		}

		st, err := subtreepkg.NewIncompleteTreeByLeafCount(capacity)
		if err != nil {
			tb.Fatal(err)
		}

		if i == 0 {
			if err := st.AddCoinbaseNode(); err != nil {
				tb.Fatal(err)
			}
		}

		for !st.IsComplete() && next < benchTxCount-1 {
			var h chainhash.Hash
			h[0] = byte(next)
			h[1] = byte(next >> 8)
			h[2] = byte(next >> 16)
			h[3] = 0xab

			if err := st.AddNode(h, 0, 300); err != nil {
				break
			}

			next++
		}

		slices[i] = st
	}

	return slices
}

// BenchmarkPrepareDedupScan is the CVE-2012-2459 floor at line 567.
func BenchmarkPrepareDedupScan(b *testing.B) {
	slices := buildBenchSlices(b)
	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		if err := model.CheckSubtreeSlicesForDuplicateTxs(slices); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkPrepareSubtreeAlloc is the 25-iteration construction loop at lines 490-510.
func BenchmarkPrepareSubtreeAlloc(b *testing.B) {
	subtreeSize, k, finalLeafCount, err := partitionLegacyBlock(benchTxCount, 4096)
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		slices := make([]*subtreepkg.Subtree, k)
		datas := make([]*subtreepkg.Data, k)
		metas := make([]*subtreepkg.Meta, k)

		for i := 0; i < k; i++ {
			capacity := subtreeSize
			if i == k-1 && k > 1 && finalLeafCount < subtreeSize {
				capacity = finalLeafCount
			}

			st, err := subtreepkg.NewIncompleteTreeByLeafCount(capacity)
			if err != nil {
				b.Fatal(err)
			}

			if i == 0 {
				if err := st.AddCoinbaseNode(); err != nil {
					b.Fatal(err)
				}
			}

			slices[i] = st
			datas[i] = subtreepkg.NewSubtreeData(st)
			metas[i] = subtreepkg.NewSubtreeMeta(st)
		}
	}
}

// BenchmarkPrepareRootHashLoop is the 25 RootHash() calls at line 634 on a cold
// (uncached) rootHash, i.e. the worst case if writeSubtree had not already
// computed them.
func BenchmarkPrepareRootHashLoop(b *testing.B) {
	b.StopTimer()

	for n := 0; n < b.N; n++ {
		slices := buildBenchSlices(b)

		b.StartTimer()

		for i := range slices {
			if slices[i].RootHash() == nil {
				b.Fatal("nil root")
			}
		}

		b.StopTimer()
	}
}

// BenchmarkPrepareCreateSubtreesLoop is the createSubtrees loop body
// (AddNode + Data.AddTx + Meta.SetTxInpointsFromTx) on the outpointOnly path,
// as a cross-check against the 0.8s the node logs.
func BenchmarkPrepareCreateSubtreesLoop(b *testing.B) {
	wireTxs := buildBenchTxs(b)

	txMap := txmap.NewSyncedMap[chainhash.Hash, *TxMapWrapper](len(wireTxs))
	txOrder := make([]chainhash.Hash, 0, len(wireTxs))

	for _, wireTx := range wireTxs {
		hashCopy := *wireTx.Hash()
		txOrder = append(txOrder, hashCopy)

		tx := &bt.Tx{}
		if err := WireTxToGoBtTx(wireTx, tx); err != nil {
			b.Fatal(err)
		}

		if !tx.IsCoinbase() {
			tx.SetTxHash(&hashCopy)
			txMap.Set(hashCopy, &TxMapWrapper{Tx: tx})
		}
	}

	txMap.Freeze()

	subtreeSize, k, finalLeafCount, err := partitionLegacyBlock(len(txOrder), 4096)
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		b.StopTimer()

		slices := make([]*subtreepkg.Subtree, k)
		datas := make([]*subtreepkg.Data, k)
		metas := make([]*subtreepkg.Meta, k)

		for i := 0; i < k; i++ {
			capacity := subtreeSize
			if i == k-1 && k > 1 && finalLeafCount < subtreeSize {
				capacity = finalLeafCount
			}

			st, err := subtreepkg.NewIncompleteTreeByLeafCount(capacity)
			if err != nil {
				b.Fatal(err)
			}

			if i == 0 {
				if err := st.AddCoinbaseNode(); err != nil {
					b.Fatal(err)
				}
			}

			slices[i] = st
			datas[i] = subtreepkg.NewSubtreeData(st)
			metas[i] = subtreepkg.NewSubtreeMeta(st)
		}

		b.StartTimer()

		idx := 0

		for _, txHash := range txOrder {
			w, found := txMap.Get(txHash)
			if !found {
				continue
			}

			tx := w.Tx

			for idx < len(slices) && slices[idx].IsComplete() {
				idx++
			}

			if idx >= len(slices) {
				b.Fatalf("no slot")
			}

			if err := slices[idx].AddNode(txHash, 0, uint64(tx.Size())); err != nil {
				b.Fatal(err)
			}

			nodeIdx := slices[idx].Length() - 1

			if err := datas[idx].AddTx(tx, nodeIdx); err != nil {
				b.Fatal(err)
			}

			if err := metas[idx].SetTxInpointsFromTx(tx); err != nil {
				b.Fatal(err)
			}
		}
	}
}

// ballast keeps a large non-scannable live heap so the benchmark below runs
// under the same condition the Hetzner mainnet node is in: ~5.5 GB resident
// against GOMEMLIMIT=6GiB, i.e. the GC pinned at its soft limit.
var ballast [][]byte

func makeBallast(gib float64) {
	chunks := int(gib * 1024)
	ballast = make([][]byte, chunks)

	for i := range ballast {
		b := make([]byte, 1<<20)
		b[0] = byte(i)
		b[len(b)-1] = byte(i)
		ballast[i] = b
	}
}

// BenchmarkPrepareCreateTxMapLoopUnderMemLimit is BenchmarkPrepareCreateTxMapLoop
// with the node's heap condition reproduced: set GOMEMLIMIT=6GiB and
// TERANODE_BENCH_BALLAST_GIB=5.2 in the environment.
func BenchmarkPrepareCreateTxMapLoopUnderMemLimit(b *testing.B) {
	gib := 0.0
	if v := os.Getenv("TERANODE_BENCH_BALLAST_GIB"); v != "" {
		var err error
		gib, err = strconv.ParseFloat(v, 64)
		if err != nil {
			b.Fatal(err)
		}
	}

	if gib <= 0 {
		b.Skip("set TERANODE_BENCH_BALLAST_GIB (and GOMEMLIMIT) to run this")
	}

	txs := buildBenchTxs(b)
	makeBallast(gib)

	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	b.Logf("live heap before loop: %.2f GiB, GOMEMLIMIT=%s", float64(ms.HeapAlloc)/(1<<30), os.Getenv("GOMEMLIMIT"))

	gcBefore := ms.NumGC
	pauseBefore := ms.PauseTotalNs

	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		txMap := txmap.NewSyncedMap[chainhash.Hash, *TxMapWrapper](len(txs))
		txOrder := make([]chainhash.Hash, 0, len(txs))

		for _, wireTx := range txs {
			hashCopy := *wireTx.Hash()
			txOrder = append(txOrder, hashCopy)

			tx := &bt.Tx{}
			if err := WireTxToGoBtTx(wireTx, tx); err != nil {
				b.Fatal(err)
			}

			if !tx.IsCoinbase() {
				tx.SetTxHash(&hashCopy)
				txMap.Set(hashCopy, &TxMapWrapper{Tx: tx})
			}
		}

		if txMap.Length() == 0 {
			b.Fatal("empty")
		}
	}

	b.StopTimer()

	runtime.ReadMemStats(&ms)
	b.Logf("GC cycles during loop: %d, total STW pause: %s, ballast chunks %d",
		ms.NumGC-gcBefore, time.Duration(ms.PauseTotalNs-pauseBefore), len(ballast))
}

// ptrBallast is a POINTER-DENSE live heap, unlike the byte-slice ballast above.
// The node's live heap is bt.Tx graphs (one bt.Tx, one Inputs slice, one Input
// + one Script + one cloned script per input, likewise per output), so its mark
// cost scales with object count, not bytes. Rebuilding the ballast out of the
// same shape is what makes the GC condition comparable.
var ptrBallast [][]*bt.Tx

// BenchmarkPrepareCreateTxMapLoopPointerDense runs the createTxMap loop with a
// pointer-dense live heap near GOMEMLIMIT. Set GOMEMLIMIT and
// TERANODE_BENCH_BALLAST_SETS (each set is one 100,001-tx block's worth of
// bt.Tx: ~83 MB and ~1.7M objects).
func BenchmarkPrepareCreateTxMapLoopPointerDense(b *testing.B) {
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

	txs := buildBenchTxs(b)

	ptrBallast = make([][]*bt.Tx, sets)

	for s := 0; s < sets; s++ {
		set := make([]*bt.Tx, len(txs))

		for i, wireTx := range txs {
			tx := &bt.Tx{}
			if err := WireTxToGoBtTx(wireTx, tx); err != nil {
				b.Fatal(err)
			}

			set[i] = tx
		}

		ptrBallast[s] = set
	}

	var ms runtime.MemStats

	runtime.ReadMemStats(&ms)
	b.Logf("live heap %.2f GiB, live objects %d, GOMEMLIMIT=%s",
		float64(ms.HeapAlloc)/(1<<30), ms.Mallocs-ms.Frees, os.Getenv("GOMEMLIMIT"))

	gcBefore := ms.NumGC

	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		txMap := txmap.NewSyncedMap[chainhash.Hash, *TxMapWrapper](len(txs))
		txOrder := make([]chainhash.Hash, 0, len(txs))

		for _, wireTx := range txs {
			hashCopy := *wireTx.Hash()
			txOrder = append(txOrder, hashCopy)

			tx := &bt.Tx{}
			if err := WireTxToGoBtTx(wireTx, tx); err != nil {
				b.Fatal(err)
			}

			if !tx.IsCoinbase() {
				tx.SetTxHash(&hashCopy)
				txMap.Set(hashCopy, &TxMapWrapper{Tx: tx})
			}
		}

		if txMap.Length() == 0 {
			b.Fatal("empty")
		}
	}

	b.StopTimer()

	runtime.ReadMemStats(&ms)
	b.Logf("GC cycles during loop: %d, total STW pause: %s",
		ms.NumGC-gcBefore, time.Duration(ms.PauseTotalNs))

	if len(ptrBallast) == 0 {
		b.Fatal("ballast collected")
	}
}

// buildBenchTxsShaped is buildBenchTxs with the tx shape parameterised, so the
// createTxMap cost can be plotted against average transaction size. The wire
// block for block 759245 is not available here; this brackets it.
func buildBenchTxsShaped(tb testing.TB, count, nIn, nOut, sigLen, pkLen int) ([]*bsvutil.Tx, int) {
	tb.Helper()

	txs := make([]*bsvutil.Tx, count)
	total := 0

	for i := 0; i < count; i++ {
		msg := wire.NewMsgTx(1)

		in := nIn
		if i == 0 {
			in = 1
		}

		for j := 0; j < in; j++ {
			var h chainhash.Hash
			h[0] = byte(i)
			h[1] = byte(i >> 8)
			h[2] = byte(i >> 16)
			h[3] = byte(j)

			msg.AddTxIn(&wire.TxIn{
				PreviousOutPoint: wire.OutPoint{Hash: h, Index: uint32(j)},
				SignatureScript:  make([]byte, sigLen),
				Sequence:         0xffffffff,
			})
		}

		for j := 0; j < nOut; j++ {
			msg.AddTxOut(&wire.TxOut{Value: 1000, PkScript: make([]byte, pkLen)})
		}

		total += msg.SerializeSize()

		t := bsvutil.NewTx(msg)
		_ = t.Hash()
		txs[i] = t
	}

	return txs, total
}

// BenchmarkPrepareCreateTxMapBySize runs the createTxMap loop over 100,001
// transactions at a range of average sizes.
func BenchmarkPrepareCreateTxMapBySize(b *testing.B) {
	shapes := []struct {
		name                     string
		nIn, nOut, sigLen, pkLen int
	}{
		{"p2pkh_2in_2out_370B", 2, 2, 107, 25},
		{"1KB", 2, 2, 107, 400},
		{"4KB", 2, 2, 107, 1900},
		{"16KB", 2, 2, 107, 7900},
		{"40KB", 2, 2, 107, 19900},
	}

	for _, s := range shapes {
		b.Run(s.name, func(b *testing.B) {
			txs, total := buildBenchTxsShaped(b, benchTxCount, s.nIn, s.nOut, s.sigLen, s.pkLen)
			b.Logf("block bytes %.2f MB (avg tx %d B)", float64(total)/(1<<20), total/len(txs))
			b.ResetTimer()

			for n := 0; n < b.N; n++ {
				txMap := txmap.NewSyncedMap[chainhash.Hash, *TxMapWrapper](len(txs))
				txOrder := make([]chainhash.Hash, 0, len(txs))

				for _, wireTx := range txs {
					hashCopy := *wireTx.Hash()
					txOrder = append(txOrder, hashCopy)

					tx := &bt.Tx{}
					if err := WireTxToGoBtTx(wireTx, tx); err != nil {
						b.Fatal(err)
					}

					if !tx.IsCoinbase() {
						tx.SetTxHash(&hashCopy)
						txMap.Set(hashCopy, &TxMapWrapper{Tx: tx})
					}
				}

				if txMap.Length() == 0 {
					b.Fatal("empty")
				}
			}
		})
	}
}

// BenchmarkHandleBlockDirectPreamble measures the two serial full-block passes
// that sit in HandleBlockDirect BEFORE prepareSubtrees and are not covered by
// any DONE-in line: the SerializeSize walk (line 242) and the 100,001
// tx.Hash() calls (lines 254-258), which are the first time each transaction's
// txid is computed.
func BenchmarkHandleBlockDirectPreamble(b *testing.B) {
	shapes := []struct {
		name                     string
		nIn, nOut, sigLen, pkLen int
	}{
		{"p2pkh_370B", 2, 2, 107, 25},
		{"16KB", 2, 2, 107, 7900},
		{"40KB", 2, 2, 107, 19900},
	}

	for _, s := range shapes {
		b.Run(s.name, func(b *testing.B) {
			shaped, total := buildBenchTxsShaped(b, benchTxCount, s.nIn, s.nOut, s.sigLen, s.pkLen)

			msgs := make([]*wire.MsgTx, len(shaped))
			for i := range shaped {
				msgs[i] = shaped[i].MsgTx()
			}

			b.Logf("block bytes %.2f MB", float64(total)/(1<<20))
			b.ResetTimer()

			for n := 0; n < b.N; n++ {
				// fresh wrappers: memoized hashes discarded, as on a newly decoded block
				wrapped := make([]*bsvutil.Tx, len(msgs))
				for i := range msgs {
					wrapped[i] = bsvutil.NewTx(msgs[i])
				}

				sz := 0
				for i := range msgs {
					sz += msgs[i].SerializeSize()
				}

				hashes := make([]chainhash.Hash, len(wrapped))
				for i, tx := range wrapped {
					hashes[i] = *tx.Hash()
				}

				if sz == 0 || hashes[0].IsEqual(nil) {
					b.Fatal("bad")
				}
			}
		})
	}
}
