package postgres

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
)

// TestPackedOutputEncodingAgreesWithSQL proves the Go-side packing primitives
// agree byte-for-byte and bit-for-bit with the PostgreSQL functions the
// hot-path SQL relies on:
//
//   - bitmap encoding: Go's buf[i/8] |= 1 << (i%8) must satisfy
//     get_bit(buf, i) = 1 for every set bit (PostgreSQL numbers bit n in byte
//     n/8 from the LEAST significant position);
//   - flat hash packing: output i written at byte offset i*32 must satisfy
//     substr(buf, i*32+1, 32) (substr is 1-based — the off-by-one this test
//     exists to catch);
//   - the on-demand zero-bitmap initialiser used by FreezeUTXOs
//     (decode(repeat('00',(n+7)/8),'hex') + set_bit) must agree with Go's
//     setPackedBit on a zeroed buffer.
//
// Output counts cover the byte boundaries {1, 2, 7, 8, 9, 64, 65} and include
// a non-spendable (zero-value OP_RETURN) output mid-array for every count >= 2.
func TestPackedOutputEncodingAgreesWithSQL(t *testing.T) {
	ctx := context.Background()

	pool, err := pgxpool.New(ctx, testDSN)
	if err != nil {
		t.Skipf("Skipping: cannot connect to postgres: %v", err)
	}
	defer pool.Close()
	if err := pool.Ping(ctx); err != nil {
		t.Skipf("Skipping: cannot connect to postgres: %v", err)
	}

	base := testExtendedTx(t)
	spendableOut := base.Outputs[0] // P2PKH, non-zero satoshis
	opReturnOut := &bt.Output{
		Satoshis:      0,
		LockingScript: bscript.NewFromBytes([]byte{0x00, 0x6a, 0x04, 0xba, 0xdc, 0x0f, 0xfe}),
	}

	for _, count := range []int{1, 2, 7, 8, 9, 64, 65} {
		// Non-spendable OP_RETURN mid-array (count 1 has no "mid": all spendable).
		nonSpendableIdx := -1
		if count >= 2 {
			nonSpendableIdx = count / 2
		}

		tx := bt.NewTx()
		tx.Version = base.Version
		tx.LockTime = base.LockTime
		tx.Inputs = base.Inputs
		tx.Outputs = make([]*bt.Output, count)
		for i := range tx.Outputs {
			if i == nonSpendableIdx {
				tx.Outputs[i] = opReturnOut
			} else {
				tx.Outputs[i] = spendableOut
			}
		}
		txHash := tx.TxIDChainHash()

		p, err := buildOutputArrays(txHash, tx, false, 100, 100, 0)
		require.NoError(t, err, "count=%d", count)

		wantSpendable := count
		if nonSpendableIdx >= 0 {
			wantSpendable--
		}
		require.Equal(t, int32(count), p.outCount, "count=%d", count)
		require.Equal(t, int32(wantSpendable), p.spendableCount, "count=%d", count)
		require.Len(t, p.utxoHashes, count*16, "count=%d: 16-byte stride", count)
		require.Len(t, p.spendableBits, (count+7)/8, "count=%d: bytes rounded up", count)

		for i := 0; i < count; i++ {
			// 16-byte stride: SQL substr (1-based) must return exactly the
			// 16-byte utxo-hash prefix Go packed at offset i*16 — and both must
			// equal the first 16 bytes of the independently computed per-output
			// UTXO hash (utxo_hashes stores a 128-bit prefix, not the full hash).
			expected, err := util.UTXOHashFromOutput(txHash, tx.Outputs[i], uint32(i))
			require.NoError(t, err)

			var sqlHash []byte
			require.NoError(t, pool.QueryRow(ctx,
				`SELECT substr($1::bytea, $2::int * 16 + 1, 16)`,
				p.utxoHashes, i).Scan(&sqlHash))
			require.Equal(t, expected[:16], sqlHash, "count=%d vout=%d: substr stride mismatch", count, i)
			require.Equal(t, expected[:16], p.utxoHashes[i*16:(i+1)*16], "count=%d vout=%d: Go slice stride mismatch", count, i)

			// Bitmap: SQL get_bit must agree with the intended flag and with
			// Go's getPackedBit reader.
			wantBit := i != nonSpendableIdx
			var sqlBit int
			require.NoError(t, pool.QueryRow(ctx,
				`SELECT get_bit($1::bytea, $2::int)`,
				p.spendableBits, i).Scan(&sqlBit))
			require.Equal(t, wantBit, sqlBit == 1, "count=%d vout=%d: get_bit disagrees with Go encoding", count, i)
			require.Equal(t, wantBit, getPackedBit(p.spendableBits, i), "count=%d vout=%d", count, i)
		}

		// unpackBitmap (the read-time []bool reconstruction) round-trips.
		flags := unpackBitmap(p.spendableBits, count)
		require.Len(t, flags, count)
		for i := 0; i < count; i++ {
			require.Equal(t, i != nonSpendableIdx, flags[i], "count=%d vout=%d: unpackBitmap mismatch", count, i)
		}

		// FreezeUTXOs' on-demand zero-bitmap initialiser + set_bit must agree
		// with Go's setPackedBit on a zeroed buffer of the same size.
		for i := 0; i < count; i++ {
			var sqlBitmap []byte
			require.NoError(t, pool.QueryRow(ctx,
				`SELECT set_bit(decode(repeat('00', ($1::int + 7) / 8), 'hex'), $2::int, 1)`,
				count, i).Scan(&sqlBitmap))

			goBitmap := make([]byte, (count+7)/8)
			setPackedBit(goBitmap, i)
			require.Equal(t, goBitmap, sqlBitmap, "count=%d bit=%d: set_bit init disagrees with setPackedBit", count, i)
		}
	}
}
