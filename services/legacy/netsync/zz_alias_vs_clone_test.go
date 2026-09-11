package netsync

import (
	"bytes"
	"fmt"
	"os"
	"runtime"
	"strconv"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	txmap "github.com/bsv-blockchain/go-tx-map"
	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/services/legacy/bsvutil"
)

// wireTxToGoBtTxCloning is the converter as it stood before aliasing: every
// script copied with bytes.Clone, and the previous-output hash copied so that
// nothing pointed into the wire transaction.
//
// It is kept here, in a test file, purely so the comparison that chose aliasing
// can be re-run. Point the benchmark's "clone" arm at the production converter
// instead and it silently compares aliasing against itself.
func wireTxToGoBtTxCloning(wireTx *bsvutil.Tx, tx *bt.Tx) {
	wTx := wireTx.MsgTx()

	tx.Version = uint32(wTx.Version) //nolint:gosec
	tx.LockTime = wTx.LockTime

	tx.Inputs = make([]*bt.Input, len(wTx.TxIn))

	for i, in := range wTx.TxIn {
		tx.Inputs[i] = &bt.Input{
			UnlockingScript:    &bscript.Script{},
			PreviousTxOutIndex: in.PreviousOutPoint.Index,
			SequenceNumber:     in.Sequence,
		}

		prevHash := in.PreviousOutPoint.Hash
		_ = tx.Inputs[i].PreviousTxIDAdd(&prevHash)
		*tx.Inputs[i].UnlockingScript = bytes.Clone(in.SignatureScript)
	}

	tx.Outputs = make([]*bt.Output, len(wTx.TxOut))

	for i, out := range wTx.TxOut {
		tx.Outputs[i] = &bt.Output{
			Satoshis:      uint64(out.Value), //nolint:gosec
			LockingScript: &bscript.Script{},
		}
		*tx.Outputs[i].LockingScript = bytes.Clone(out.PkScript)
	}
}

// wireTxToGoBtTxAliasing is the production converter, so the benchmark tracks
// what the node actually runs rather than a copy of it that can drift.
func wireTxToGoBtTxAliasing(wireTx *bsvutil.Tx, tx *bt.Tx) {
	if err := WireTxToGoBtTx(wireTx, tx); err != nil {
		panic(err)
	}
}

// wireTxToGoBtTxAliasingOwnHeader aliases the script BYTES but gives each one a
// fresh slice header, instead of pointing at the header inside the wire
// transaction.
//
// The difference is what stays reachable. Pointing at &in.SignatureScript keeps
// the whole wire.TxIn alive, and through it the MsgTx and its input and output
// slices. A fresh header keeps only the arena chunk the bytes live in, which
// has to stay anyway. It costs one small allocation per script to find out
// whether dropping the wire structs is worth more than that.
func wireTxToGoBtTxAliasingOwnHeader(wireTx *bsvutil.Tx, tx *bt.Tx) {
	wTx := wireTx.MsgTx()

	tx.Version = uint32(wTx.Version) //nolint:gosec
	tx.LockTime = wTx.LockTime

	tx.Inputs = make([]*bt.Input, len(wTx.TxIn))

	for i, in := range wTx.TxIn {
		unlocking := bscript.Script(in.SignatureScript)

		tx.Inputs[i] = &bt.Input{
			UnlockingScript:    &unlocking,
			PreviousTxOutIndex: in.PreviousOutPoint.Index,
			SequenceNumber:     in.Sequence,
		}

		prevHash := in.PreviousOutPoint.Hash
		_ = tx.Inputs[i].PreviousTxIDAdd(&prevHash)
	}

	tx.Outputs = make([]*bt.Output, len(wTx.TxOut))

	for i, out := range wTx.TxOut {
		locking := bscript.Script(out.PkScript)

		tx.Outputs[i] = &bt.Output{
			Satoshis:      uint64(out.Value), //nolint:gosec
			LockingScript: &locking,
		}
	}
}

// buildDecodedBenchTxs returns transactions that came out of go-wire's decoder,
// which is the only way to observe what aliasing costs.
//
// The decoder puts every script into a bump-allocated arena in 4 MiB chunks
// shared across transactions. A block assembled object by object with NewMsgTx
// and AddTxIn has no arena: each script is its own allocation, so aliasing one
// pins one script rather than a whole chunk, and a measurement taken that way
// would say aliasing is free when it is not. The existing size-sweep benchmark
// builds blocks that way, which is fine for timing the clone but useless here.
func buildDecodedBenchTxs(tb testing.TB, count, nIn, nOut, sigLen, pkLen int) ([]*bsvutil.Tx, int) {
	tb.Helper()

	txs, raw := buildDecodedBenchTxsWithBytes(tb, count, nIn, nOut, sigLen, pkLen)

	return txs, len(raw)
}

// buildDecodedBenchTxsWithBytes is buildDecodedBenchTxs, also returning the
// serialised block the transactions were decoded from, so a test can check that
// aliasing has not written through into it.
func buildDecodedBenchTxsWithBytes(tb testing.TB, count, nIn, nOut, sigLen, pkLen int) ([]*bsvutil.Tx, []byte) {
	tb.Helper()

	block := wire.NewMsgBlock(wire.NewBlockHeader(1, &chainhash.Hash{}, &chainhash.Hash{}, 0, 0))

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
				PreviousOutPoint: wire.OutPoint{Hash: h, Index: uint32(j)}, //nolint:gosec
				SignatureScript:  make([]byte, sigLen),
				Sequence:         0xffffffff,
			})
		}

		for j := 0; j < nOut; j++ {
			msg.AddTxOut(&wire.TxOut{Value: 1000, PkScript: make([]byte, pkLen)})
		}

		if err := block.AddTransaction(msg); err != nil {
			tb.Fatalf("add transaction: %v", err)
		}
	}

	var serialised bytes.Buffer
	if err := block.Serialize(&serialised); err != nil {
		tb.Fatalf("serialise: %v", err)
	}

	raw := serialised.Bytes()

	decoded := wire.NewMsgBlock(wire.NewBlockHeader(1, &chainhash.Hash{}, &chainhash.Hash{}, 0, 0))
	if err := decoded.Bsvdecode(bytes.NewReader(raw), 0, wire.BaseEncoding); err != nil {
		tb.Fatalf("decode: %v", err)
	}

	txs := make([]*bsvutil.Tx, 0, len(decoded.Transactions))

	for _, msgTx := range decoded.Transactions {
		wt := bsvutil.NewTx(msgTx)
		_ = wt.Hash() // memoize, as HandleBlockDirect does

		txs = append(txs, wt)
	}

	return txs, raw
}

type heapReading struct {
	wire  int64 // the decoded wire block on its own
	both  int64 // wire block and converted transactions live together
	after int64 // the wire block dropped, only converted transactions left
}

// measureConversionHeap runs one converter over a freshly decoded block and
// reports the three quantities that decide the question.
//
// Every reading is relative to a baseline taken inside this function, because
// the test binary is shared and an assertion against a share of total heap
// passes alone and fails in the suite.
func measureConversionHeap(tb testing.TB, count, nIn, nOut, sigLen, pkLen int,
	convert func(*bsvutil.Tx, *bt.Tx)) heapReading {
	tb.Helper()

	var baseline, wireOnly, withBoth, afterDrop runtime.MemStats

	runtime.GC()
	runtime.ReadMemStats(&baseline)

	wireTxs, _ := buildDecodedBenchTxs(tb, count, nIn, nOut, sigLen, pkLen)

	runtime.GC()
	runtime.ReadMemStats(&wireOnly)

	converted := make([]*bt.Tx, 0, len(wireTxs))
	txMap := txmap.NewSyncedMap[chainhash.Hash, *TxMapWrapper](len(wireTxs))

	for _, wireTx := range wireTxs {
		tx := &bt.Tx{}
		convert(wireTx, tx)

		hashCopy := *wireTx.Hash()
		tx.SetTxHash(&hashCopy)
		txMap.Set(hashCopy, &TxMapWrapper{Tx: tx})
		converted = append(converted, tx)
	}

	runtime.GC()
	runtime.ReadMemStats(&withBoth)

	// MANDATORY. The only later use of wireTxs is the assignment to nil below,
	// which is never read, so without this the compiler treats it as dead the
	// moment the loop ends and the collector reclaims it BEFORE the reading
	// above. Both readings then come out identical whether or not the wire side
	// is retained, which is how three earlier attempts at this measured nothing.
	runtime.KeepAlive(wireTxs)

	wireTxs = nil

	runtime.GC()
	runtime.GC()
	runtime.ReadMemStats(&afterDrop)

	runtime.KeepAlive(converted)
	runtime.KeepAlive(txMap)

	return heapReading{
		wire:  int64(wireOnly.HeapAlloc) - int64(baseline.HeapAlloc),
		both:  int64(withBoth.HeapAlloc) - int64(baseline.HeapAlloc),
		after: int64(afterDrop.HeapAlloc) - int64(baseline.HeapAlloc),
	}
}

func mb(v int64) string { return fmt.Sprintf("%.1f MB", float64(v)/(1<<20)) }

// TestAliasVersusCloneHeap is the measurement that decides whether the bt.Tx
// should own its scripts or point at the decoder's.
//
// Cloning gives the converted transactions their own copies, so the wire block
// and its arena become collectable the moment the conversion returns. The price
// is that both representations are live while one is built from the other.
//
// Aliasing has no such peak, but the arena can never be released: it is chunked
// and shared across transactions, so a single aliased script keeps its whole
// 4 MiB chunk, and every chunk carries at least one script.
//
// Which is better is a question about numbers, so here are the numbers. Run
// with -v to read them; the assertions only pin the two properties that must
// hold for the comparison to mean anything at all.
func TestAliasVersusCloneHeap(t *testing.T) {
	if raceDetectorEnabled {
		t.Skip("this measures heap proportions, which the race detector perturbs")
	}

	shapes := []struct {
		name                     string
		count                    int
		nIn, nOut, sigLen, pkLen int
	}{
		{"p2pkh_2in_2out_370B", 20000, 2, 2, 107, 25},
		{"4KB", 5000, 2, 2, 107, 1900},
		{"27KB_mainnet_mean", 2000, 2, 2, 107, 13500},
		{"40KB", 2000, 2, 2, 107, 19900},
	}

	for _, s := range shapes {
		t.Run(s.name, func(t *testing.T) {
			clone := measureConversionHeap(t, s.count, s.nIn, s.nOut, s.sigLen, s.pkLen,
				wireTxToGoBtTxCloning)

			alias := measureConversionHeap(t, s.count, s.nIn, s.nOut, s.sigLen, s.pkLen,
				wireTxToGoBtTxAliasing)

			own := measureConversionHeap(t, s.count, s.nIn, s.nOut, s.sigLen, s.pkLen,
				wireTxToGoBtTxAliasingOwnHeader)

			t.Logf("wire block alone      clone %s   alias %s   alias-own-header %s",
				mb(clone.wire), mb(alias.wire), mb(own.wire))
			t.Logf("peak, both live       clone %s   alias %s   alias-own-header %s",
				mb(clone.both), mb(alias.both), mb(own.both))
			t.Logf("steady, wire dropped  clone %s   alias %s   alias-own-header %s",
				mb(clone.after), mb(alias.after), mb(own.after))
			t.Logf("peak   as a multiple of the wire block  clone %.2fx  alias %.2fx  alias-own-header %.2fx",
				float64(clone.both)/float64(clone.wire), float64(alias.both)/float64(alias.wire),
				float64(own.both)/float64(own.wire))
			t.Logf("steady as a multiple of the wire block  clone %.2fx  alias %.2fx  alias-own-header %.2fx",
				float64(clone.after)/float64(clone.wire), float64(alias.after)/float64(alias.wire),
				float64(own.after)/float64(own.wire))

			if clone.wire <= 0 || alias.wire <= 0 {
				t.Fatalf("the wire block measured %d and %d bytes, so nothing here means anything",
					clone.wire, alias.wire)
			}

			// Cloning must release most of the wire block, or the clone is not
			// doing the one thing it exists to do and the comparison is void.
			released := clone.both - clone.after
			if float64(released) < float64(clone.wire)*0.5 {
				t.Fatalf("cloning gave back only %s of the %s the wire block cost", mb(released), mb(clone.wire))
			}

			// Aliasing must NOT release it, for the same reason: if it does,
			// this shape has no arena and the measurement is not about aliasing.
			aliasReleased := alias.both - alias.after
			if float64(aliasReleased) > float64(alias.wire)*0.5 {
				t.Fatalf("aliasing gave back %s of the %s wire block, so these scripts are not arena-backed and the test is measuring the wrong thing",
					mb(aliasReleased), mb(alias.wire))
			}
		})
	}
}

// aliasBenchBallast is a POINTER-DENSE live heap: bt.Tx graphs, the shape the
// node's own heap has. It is a package-level variable so nothing collects it
// mid-benchmark.
var aliasBenchBallast [][]*bt.Tx

// buildPointerBallast fills the ballast with `sets` blocks' worth of converted
// transactions, so the collector runs against a live heap of the object count
// the node has.
//
// Pointer density is the whole point and it is easy to get wrong. Measured: a
// 5.29 GiB ballast made of byte slices, under a 6 GiB limit, produced one
// garbage-collection cycle across the entire benchmark and no slowdown at all,
// because five thousand large objects cost nothing to mark. One set here is
// 100,000 transactions and roughly 1.7 million objects; eighty sets reach 137
// million, at which point the penalty appears immediately.
func buildPointerBallast(tb testing.TB, sets int) {
	tb.Helper()

	base, _ := buildDecodedBenchTxsWithBytes(tb, 100000, 2, 2, 107, 25)

	aliasBenchBallast = make([][]*bt.Tx, sets)

	for s := 0; s < sets; s++ {
		set := make([]*bt.Tx, len(base))

		for i, wireTx := range base {
			tx := &bt.Tx{}
			wireTxToGoBtTxCloning(wireTx, tx)

			set[i] = tx
		}

		aliasBenchBallast[s] = set
	}
}

// BenchmarkAliasVersusCloneConversion times the two converters over a decoded
// block. Set GOMEMLIMIT and TERANODE_BENCH_BALLAST_GIB to reproduce the node's
// condition, where the collector is pinned at its soft limit and an allocation
// pays off sweep debt before it may proceed.
func BenchmarkAliasVersusCloneConversion(b *testing.B) {
	// The ballast has to be POINTER-DENSE, not byte slices. Marking cost scales
	// with the number of live objects, not with bytes, so 5.2 GiB of byte
	// slices is about five thousand objects and the collector barely notices
	// it: measured, a 5.29 GiB byte-slice ballast under a 6 GiB limit produced
	// one GC cycle and no slowdown at all. The node's live heap is bt.Tx
	// graphs, millions of small objects, which is what ballastSets builds.
	if v := os.Getenv("TERANODE_BENCH_BALLAST_SETS"); v != "" {
		sets, err := strconv.Atoi(v)
		if err != nil {
			b.Fatal(err)
		}

		if sets > 0 {
			buildPointerBallast(b, sets)
		}
	}

	var pre runtime.MemStats

	runtime.ReadMemStats(&pre)
	b.Logf("live heap before the loop %.2f GiB, live objects %d, GOMEMLIMIT=%s",
		float64(pre.HeapAlloc)/(1<<30), pre.Mallocs-pre.Frees, os.Getenv("GOMEMLIMIT"))

	shapes := []struct {
		name                     string
		count                    int
		nIn, nOut, sigLen, pkLen int
	}{
		{"p2pkh_2in_2out_370B", 20000, 2, 2, 107, 25},
		{"4KB", 5000, 2, 2, 107, 1900},
		{"27KB_mainnet_mean", 2000, 2, 2, 107, 13500},
		{"40KB", 2000, 2, 2, 107, 19900},
	}

	variants := []struct {
		name    string
		convert func(*bsvutil.Tx, *bt.Tx)
	}{
		{"clone", wireTxToGoBtTxCloning},
		{"alias", wireTxToGoBtTxAliasing},
		{"alias_own_header", wireTxToGoBtTxAliasingOwnHeader},
	}

	defer func() { runtime.KeepAlive(aliasBenchBallast) }()

	for _, s := range shapes {
		txs, size := buildDecodedBenchTxs(b, s.count, s.nIn, s.nOut, s.sigLen, s.pkLen)

		for _, v := range variants {
			b.Run(s.name+"/"+v.name, func(b *testing.B) {
				b.SetBytes(int64(size))
				b.ReportAllocs()
				b.ResetTimer()

				for n := 0; n < b.N; n++ {
					txMap := txmap.NewSyncedMap[chainhash.Hash, *TxMapWrapper](len(txs))

					for _, wireTx := range txs {
						tx := &bt.Tx{}
						v.convert(wireTx, tx)

						hashCopy := *wireTx.Hash()
						tx.SetTxHash(&hashCopy)
						txMap.Set(hashCopy, &TxMapWrapper{Tx: tx})
					}

					if txMap.Length() == 0 {
						b.Fatal("empty")
					}
				}
			})
		}
	}
}
