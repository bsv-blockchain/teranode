package blockvalidation

import (
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
)

// The transaction shape that matters. Mainnet block 759245 carried 100,001
// transactions averaging 34,370 bytes, which is where the cost of an uncached
// transaction-ID computation stops being negligible: the ID is a double SHA256
// over a fresh full serialisation, so it is linear in transaction size and
// allocates a buffer that size every time.
func benchExtendedTx(tb testing.TB, scriptBytes int) *bt.Tx {
	tb.Helper()

	tx := bt.NewTx()

	unlocking := make([]byte, scriptBytes/2)
	locking := make([]byte, scriptBytes/2)

	in := &bt.Input{
		UnlockingScript:    bscript.NewFromBytes(unlocking),
		PreviousTxOutIndex: 0,
		SequenceNumber:     0xffffffff,
	}

	var prev chainhash.Hash
	prev[0] = 0x01

	if err := in.PreviousTxIDAdd(&prev); err != nil {
		tb.Fatal(err)
	}

	in.PreviousTxSatoshis = 1000
	in.PreviousTxScript = bscript.NewFromBytes(locking)

	tx.Inputs = append(tx.Inputs, in)
	tx.AddOutput(&bt.Output{Satoshis: 900, LockingScript: bscript.NewFromBytes(locking)})

	return tx
}

// BenchmarkTxIDTwiceUncached is what the extend stage used to do: call
// TxIDChainHash for the map key, then call it again to cache it. TxIDChainHash
// does not populate its own cache, so the first call is paid in full and thrown
// away.
func BenchmarkTxIDTwiceUncached(b *testing.B) {
	for _, size := range []int{400, 34370} {
		b.Run(sizeName(size), func(b *testing.B) {
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				tx := benchExtendedTx(b, size)

				_ = *tx.TxIDChainHash()
				tx.SetTxHash(tx.TxIDChainHash())
			}
		})
	}
}

// BenchmarkTxIDOnceCached is what it does now: compute, cache, use.
func BenchmarkTxIDOnceCached(b *testing.B) {
	for _, size := range []int{400, 34370} {
		b.Run(sizeName(size), func(b *testing.B) {
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				tx := benchExtendedTx(b, size)

				txID := tx.TxIDChainHash()
				tx.SetTxHash(txID)

				_ = *txID
			}
		})
	}
}

func sizeName(n int) string {
	if n >= 1024 {
		return "34KB_mainnet_shape"
	}

	return "400B_small"
}
