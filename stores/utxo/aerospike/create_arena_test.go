package aerospike

import (
	"os"
	"sync"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// TestCreateArena_ConcurrentReuseNoCorruption verifies that the pool's
// get/put cycle is race-free and that arena-backed slices produced within
// a single lease hold the correct bytes before the arena is returned.
func TestCreateArena_ConcurrentReuseNoCorruption(t *testing.T) {
	script, err := bscript.NewFromHexString("76a914000000000000000000000000000000000000000088ac")
	require.NoError(t, err)

	var wg sync.WaitGroup

	for g := 0; g < 8; g++ {
		wg.Add(1)

		go func(g int) {
			defer wg.Done()

			for i := 0; i < 50; i++ {
				a := getCreateArena()
				out := &bt.Output{Satoshis: uint64(g*100 + i), LockingScript: script}
				got := appendOutputInto(a, out)
				require.Equal(t, out.Bytes(), got) // bytes correct despite reuse
				putCreateArena(a)
			}
		}(g)
	}

	wg.Wait()
}

// assertArenaEqualNoArena is a helper that calls GetBinsToStore twice on the
// same tx — once with nil arena, once with the supplied arena — and asserts
// byte-identical results.  A fresh arena must be passed for each call so that
// prior test cases' allocations do not interfere.
func assertArenaEqualNoArena(t *testing.T, s *Store, tx *bt.Tx, isCoinbase bool, name string) {
	t.Helper()

	txHash := tx.TxIDChainHash()

	want, err := s.GetBinsToStore(tx, 0, nil, nil, nil, false, txHash, isCoinbase, false, false, nil)
	require.NoError(t, err, "%s: GetBinsToStore(nil arena) failed", name)

	arena := bt.NewArena(0)

	got, err := s.GetBinsToStore(tx, 0, nil, nil, nil, false, txHash, isCoinbase, false, false, arena)
	require.NoError(t, err, "%s: GetBinsToStore(arena) failed", name)

	require.Equal(t, len(want), len(got), "%s: number of bin groups differs", name)

	for ri := range want {
		require.Equal(t, len(want[ri]), len(got[ri]), "%s: number of bins in group %d differs", name, ri)

		for bi, wb := range want[ri] {
			gb := got[ri][bi]
			require.Equal(t, wb.Name, gb.Name, "%s: bin name mismatch at [%d][%d]", name, ri, bi)

			// createdAt is time.Now().UnixMilli() — by definition different between
			// two separate GetBinsToStore calls. Skip it; it is not affected by arena
			// vs nil and is covered by functional tests elsewhere.
			if wb.Name == fields.CreatedAt.String() {
				continue
			}

			wBytes, wOk := wb.Value.GetObject().([]byte)
			gBytes, gOk := gb.Value.GetObject().([]byte)

			if wOk && gOk {
				require.Equal(t, wBytes, gBytes, "%s: byte content mismatch for bin %q at [%d][%d]", name, wb.Name, ri, bi)
			} else {
				// Integers, booleans, and list values (inputs/outputs) — deep equality
				// recurses into nested []byte slices inside list entries.
				require.Equal(t, wb.Value, gb.Value, "%s: value mismatch for bin %q at [%d][%d]", name, wb.Name, ri, bi)
			}
		}
	}
}

// TestGetBinsToStore_ArenaMatchesNoArena asserts that GetBinsToStore produces
// byte-identical bin values whether or not an arena is supplied, across a
// corpus of tx shapes.
func TestGetBinsToStore_ArenaMatchesNoArena(t *testing.T) {
	InitPrometheusMetrics()

	s := Store{}
	s.SetUtxoBatchSize(100)
	s.SetSettings(test.CreateBaseTestSettings(t))

	p2pkhScript, err := bscript.NewFromHexString("76a914000000000000000000000000000000000000000088ac")
	require.NoError(t, err)

	opReturnScript, err := bscript.NewFromHexString("6a0b68656c6c6f20776f726c64")
	require.NoError(t, err)

	// shape: real/extended tx from testdata (the original single-case corpus)
	t.Run("real_extended_tx", func(t *testing.T) {
		txHex, err := os.ReadFile("testdata/fbebcc148e40cb6c05e57c6ad63abd49d5e18b013c82f704601bc4ba567dfb90.hex")
		require.NoError(t, err)

		tx, err := bt.NewTxFromString(string(txHex))
		require.NoError(t, err)

		assertArenaEqualNoArena(t, &s, tx, false, "real_extended_tx")
	})

	// shape: coinbase-shaped tx (single input with 32 zero-byte prev txid, one output)
	t.Run("coinbase_shaped", func(t *testing.T) {
		tx := bt.NewTx()
		cbIn := &bt.Input{PreviousTxOutIndex: 0xffffffff, SequenceNumber: 0xffffffff}
		cbIn.UnlockingScript = &bscript.Script{0x03, 0x4e, 0x01, 0x00}
		require.NoError(t, cbIn.PreviousTxIDAddStr("0000000000000000000000000000000000000000000000000000000000000000"))
		tx.Inputs = append(tx.Inputs, cbIn)
		tx.Outputs = append(tx.Outputs, &bt.Output{Satoshis: 5000000000, LockingScript: p2pkhScript})

		assertArenaEqualNoArena(t, &s, tx, true, "coinbase_shaped")
	})

	// shape: multi-output tx (5 outputs, mixed satoshi values)
	// Input satoshis (20000) exceeds total output satoshis (1000+2000+3000+4000+5000=15000)
	// so fee computation does not underflow.
	t.Run("multi_output", func(t *testing.T) {
		tx := bt.NewTx()
		in := &bt.Input{PreviousTxOutIndex: 0, PreviousTxSatoshis: 20000, SequenceNumber: 0xffffffff}
		in.UnlockingScript = p2pkhScript
		in.PreviousTxScript = p2pkhScript
		require.NoError(t, in.PreviousTxIDAddStr("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"))
		tx.Inputs = append(tx.Inputs, in)
		for i := range 5 {
			tx.Outputs = append(tx.Outputs, &bt.Output{Satoshis: uint64(1000 * (i + 1)), LockingScript: p2pkhScript})
		}

		assertArenaEqualNoArena(t, &s, tx, false, "multi_output")
	})

	// shape: OP_RETURN / zero-satoshi output tx
	t.Run("op_return_zero_satoshi", func(t *testing.T) {
		tx := bt.NewTx()
		in := &bt.Input{PreviousTxOutIndex: 1, PreviousTxSatoshis: 2000, SequenceNumber: 0xffffffff}
		in.UnlockingScript = p2pkhScript
		in.PreviousTxScript = p2pkhScript
		require.NoError(t, in.PreviousTxIDAddStr("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"))
		tx.Inputs = append(tx.Inputs, in)
		// regular output
		tx.Outputs = append(tx.Outputs, &bt.Output{Satoshis: 1999, LockingScript: p2pkhScript})
		// OP_RETURN with zero satoshis
		tx.Outputs = append(tx.Outputs, &bt.Output{Satoshis: 0, LockingScript: opReturnScript})

		assertArenaEqualNoArena(t, &s, tx, false, "op_return_zero_satoshi")
	})

	// shape: no-input (partial) tx — len(Inputs)==0, external=false.
	// GetBinsToStore handles this via the GetUtxoHashes path (no fee calculation).
	t.Run("no_input_partial_tx", func(t *testing.T) {
		tx := bt.NewTx()
		// no inputs
		tx.Outputs = append(tx.Outputs, &bt.Output{Satoshis: 5000, LockingScript: p2pkhScript})
		tx.Outputs = append(tx.Outputs, &bt.Output{Satoshis: 3000, LockingScript: p2pkhScript})

		assertArenaEqualNoArena(t, &s, tx, false, "no_input_partial_tx")
	})
}
