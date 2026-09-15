package netsync

import (
	"bytes"
	"runtime"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	txmap "github.com/bsv-blockchain/go-tx-map"
	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/services/legacy/bsvutil"
)

// The invariant these tests protect is that converting a block from its wire
// form to bt.Tx leaves the process holding ONE copy of the block's bytes, not
// two. On a 3.44 GB mainnet block against a 6 GiB soft memory limit, two copies
// do not fit.
//
// How that invariant is met changed, and the history matters because the
// obvious reading of this file is now the opposite of what it used to say.
//
// It used to be met by cloning every script, so the wire block and its decode
// arena could be dropped the moment the conversion returned. The test here
// asserted exactly that: dropping the wire block must give back most of what it
// cost. That worked, after a fashion, but it paid for the guarantee with a full
// copy of every script and a transient peak of both representations at once,
// measured at 1.98 times the wire block.
//
// It is now met by aliasing: the bt.Tx points at the decoder's script bytes and
// the arena is deliberately retained. At the transaction size mainnet is
// actually carrying, a measured mean of 27 KB, a transaction is almost entirely
// script, so holding the arena and holding your own copies come to the same
// number of bytes. The clone bought nothing and cost a full pass over the block.
// Measured with the heap pinned at a 6 GiB limit against 137 million live
// objects, the conversion loop went from 22.89 ms to 0.43 ms per block at that
// shape, and the peak from 1.98 times the wire block to 1.01.
//
// Aliasing is safe because of what go-wire's arena promises in its own
// documentation: returned slices are stable forever with nothing ever moving
// them, capacity equals length so an append cannot reach into the next script,
// and the arena is never explicitly freed, so the collector reclaims a chunk
// only once nothing points into it. Nothing in teranode or go-bt writes through
// a script pointer.

// convertedHeapMultiple returns the live heap once both the wire block and its
// converted transactions exist, as a multiple of what the wire block cost on
// its own.
func convertedHeapMultiple(tb testing.TB, convert func(*bsvutil.Tx, *bt.Tx)) float64 {
	tb.Helper()

	var baseline, wireOnly, withBoth runtime.MemStats

	runtime.GC()
	runtime.ReadMemStats(&baseline)

	// Decoded from serialised bytes, not built object by object. go-wire's
	// decoder puts every script into a bump-allocated arena in 4 MiB chunks; a
	// block assembled with NewMsgTx and AddTxIn has no arena at all, so a test
	// built that way cannot observe anything this file is about. The first
	// version of the old test made exactly that mistake.
	//
	// The shape is the one mainnet carries. At 370-byte transactions the wire
	// structs outweigh the script bytes and the two strategies come much closer
	// together, which would make this a weak test of a strong property.
	wireTxs, _ := buildDecodedBenchTxs(tb, 2000, 2, 2, 107, 13500)

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

	// MANDATORY, and the reason three earlier attempts at this measured
	// nothing. Without it the compiler treats these as dead the moment the loop
	// ends and Go's precise collector reclaims them BEFORE the reading above,
	// so both readings come out identical whether or not the property holds.
	runtime.KeepAlive(wireTxs)
	runtime.KeepAlive(converted)
	runtime.KeepAlive(txMap)

	wireSize := int64(wireOnly.HeapAlloc) - int64(baseline.HeapAlloc)
	both := int64(withBoth.HeapAlloc) - int64(baseline.HeapAlloc)

	if wireSize <= 0 {
		tb.Fatalf("the wire block measured %d bytes, so nothing here means anything", wireSize)
	}

	// Every figure is relative to a baseline taken inside this function. An
	// assertion against a share of TOTAL heap passes alone and fails in the
	// suite, because the test binary is shared; that is how the third version
	// of the old test went wrong.
	return float64(both) / float64(wireSize)
}

// TestConversionHoldsOneCopyOfTheBlock is the invariant. Converting must not
// double the block's footprint, even for the moment it takes to convert.
func TestConversionHoldsOneCopyOfTheBlock(t *testing.T) {
	if raceDetectorEnabled {
		// The detector allocates shadow memory for every access and changes
		// allocation behaviour throughout, so the multiple measured here is not
		// the multiple a production build reaches. The property under test is a
		// memory one, so there is nothing to salvage by loosening the
		// threshold: it would either stop catching the regression or start
		// failing at random.
		t.Skip("this measures heap proportions, which the race detector perturbs")
	}

	got := convertedHeapMultiple(t, func(w *bsvutil.Tx, tx *bt.Tx) {
		if err := WireTxToGoBtTx(w, tx); err != nil {
			t.Fatal(err)
		}
	})

	t.Logf("peak heap during conversion: %.2f times the wire block", got)

	// 1.35 rather than a tight bound on the measured 1.01, because the slack is
	// the bt.Tx, bt.Input and bt.Output structs, whose share of the total moves
	// with transaction size and with the Go version. A conversion that copied
	// the scripts would land near 2.0 and is what this must catch.
	if got > 1.35 {
		t.Fatalf("conversion peaked at %.2f times the wire block; the process is carrying two copies of it, which does not fit a 3.44 GB block under a 6 GiB limit", got)
	}
}

// TestConversionDoesNotModifyTheWireBlock is the safety half of aliasing. The
// bt.Tx now points at the decoder's bytes, so anything writing through a script
// would corrupt the block the node received, and the corruption would only
// surface later as a hash mismatch.
func TestConversionDoesNotModifyTheWireBlock(t *testing.T) {
	wireTxs, raw := buildDecodedBenchTxsWithBytes(t, 200, 3, 3, 107, 512)

	before := append([]byte(nil), raw...)

	converted := make([]*bt.Tx, 0, len(wireTxs))

	for _, wireTx := range wireTxs {
		tx := &bt.Tx{}
		if err := WireTxToGoBtTx(wireTx, tx); err != nil {
			t.Fatal(err)
		}

		converted = append(converted, tx)
	}

	// Re-serialise the wire block from the objects the conversion aliased, and
	// compare against the bytes it was decoded from.
	rebuilt := wire.NewMsgBlock(wire.NewBlockHeader(1, &chainhash.Hash{}, &chainhash.Hash{}, 0, 0))
	for _, wireTx := range wireTxs {
		if err := rebuilt.AddTransaction(wireTx.MsgTx()); err != nil {
			t.Fatal(err)
		}
	}

	var out bytes.Buffer
	if err := rebuilt.Serialize(&out); err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(before, out.Bytes()) {
		t.Fatal("the wire block changed during conversion; an aliased script was written through")
	}

	runtime.KeepAlive(converted)
}

// TestConvertedTransactionsMatchTheWireBytes checks the content, not just that
// nothing was overwritten: each converted transaction must serialise to exactly
// the bytes the wire transaction does. Aliasing the wrong slice, or off by an
// input, would pass the mutation test above and fail here.
func TestConvertedTransactionsMatchTheWireBytes(t *testing.T) {
	wireTxs, _ := buildDecodedBenchTxsWithBytes(t, 200, 3, 3, 107, 512)

	for i, wireTx := range wireTxs {
		tx := &bt.Tx{}
		if err := WireTxToGoBtTx(wireTx, tx); err != nil {
			t.Fatal(err)
		}

		var want bytes.Buffer
		if err := wireTx.MsgTx().Serialize(&want); err != nil {
			t.Fatal(err)
		}

		if !bytes.Equal(want.Bytes(), tx.Bytes()) {
			t.Fatalf("transaction %d does not serialise to the bytes it came from", i)
		}

		if *tx.TxIDChainHash() != *wireTx.Hash() {
			t.Fatalf("transaction %d has a different id after conversion", i)
		}
	}
}
