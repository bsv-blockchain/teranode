package aerospike

import (
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/stretchr/testify/require"
)

func TestOutputSizeEqualsBytesLen(t *testing.T) {
	scripts := []string{
		"",
		"76a914000000000000000000000000000000000000000088ac",
		"6a4c64" + "00",
	}
	for _, hexScript := range scripts {
		s, err := bscript.NewFromHexString(hexScript)
		require.NoError(t, err)
		out := &bt.Output{Satoshis: 12345, LockingScript: s}
		require.Equal(t, len(out.Bytes()), out.Size())
	}
}

func makeTxForSize(t *testing.T, nIn, nOut int, withPrevScript bool) *bt.Tx {
	t.Helper()
	tx := bt.NewTx()
	script, err := bscript.NewFromHexString("76a914000000000000000000000000000000000000000088ac")
	require.NoError(t, err)
	for i := 0; i < nIn; i++ {
		in := &bt.Input{PreviousTxOutIndex: uint32(i), PreviousTxSatoshis: 1000, SequenceNumber: 0xffffffff}
		in.UnlockingScript = script
		if withPrevScript {
			in.PreviousTxScript = script
		}
		// PreviousTxIDAddStr sets the 32-byte previousTxIDHash, which is required for
		// Input.Size() to match the actual serialized length (Bytes() skips it when nil).
		require.NoError(t, in.PreviousTxIDAddStr("0000000000000000000000000000000000000000000000000000000000000000"))
		tx.Inputs = append(tx.Inputs, in)
	}
	for i := 0; i < nOut; i++ {
		tx.Outputs = append(tx.Outputs, &bt.Output{Satoshis: 1, LockingScript: script})
	}
	return tx
}

func TestExtendedTxSizeMatchesExtendedBytes(t *testing.T) {
	cases := []struct {
		nIn, nOut  int
		prevScript bool
	}{
		{1, 1, true}, {1, 1, false}, {3, 5, true}, {2, 0, true}, {0, 2, true}, {5, 1, false},
	}
	for _, c := range cases {
		tx := makeTxForSize(t, c.nIn, c.nOut, c.prevScript)
		require.Equal(t, len(tx.ExtendedBytes()), extendedTxSize(tx),
			"nIn=%d nOut=%d prevScript=%v", c.nIn, c.nOut, c.prevScript)
	}
}

func TestExtendedTxSize_LargePrevScript(t *testing.T) {
	tx := bt.NewTx()
	big := make([]byte, 300) // > 252 => 3-byte VarInt length prefix
	for i := range big {
		big[i] = 0x51
	}
	bigScript := bscript.Script(big)
	in := &bt.Input{PreviousTxOutIndex: 0, PreviousTxSatoshis: 1000, SequenceNumber: 0xffffffff}
	in.UnlockingScript = &bscript.Script{}
	in.PreviousTxScript = &bigScript
	require.NoError(t, in.PreviousTxIDAddStr("0000000000000000000000000000000000000000000000000000000000000000"))
	tx.Inputs = append(tx.Inputs, in)
	script, err := bscript.NewFromHexString("76a914000000000000000000000000000000000000000088ac")
	require.NoError(t, err)
	tx.Outputs = append(tx.Outputs, &bt.Output{Satoshis: 1, LockingScript: script})

	require.Equal(t, len(tx.ExtendedBytes()), extendedTxSize(tx))
}

func TestExtendedTxSize_DecodedTxShape(t *testing.T) {
	// Round-trip a constructed extended tx through bytes so inputs carry a real
	// (non-nil) previousTxIDHash, matching the production decode path.
	src := makeTxForSize(t, 3, 4, true)
	decoded, err := bt.NewTxFromBytes(src.ExtendedBytes())
	require.NoError(t, err)
	require.Equal(t, len(decoded.ExtendedBytes()), extendedTxSize(decoded))

	// Coinbase-shaped tx: single input with 32 zero-byte prev txid (non-nil).
	cb := bt.NewTx()
	cbIn := &bt.Input{PreviousTxOutIndex: 0xffffffff, SequenceNumber: 0xffffffff}
	cbIn.UnlockingScript = &bscript.Script{0x03, 0x01, 0x02, 0x03}
	require.NoError(t, cbIn.PreviousTxIDAddStr("0000000000000000000000000000000000000000000000000000000000000000"))
	cb.Inputs = append(cb.Inputs, cbIn)
	script, err := bscript.NewFromHexString("76a914000000000000000000000000000000000000000088ac")
	require.NoError(t, err)
	cb.Outputs = append(cb.Outputs, &bt.Output{Satoshis: 5000000000, LockingScript: script})
	require.Equal(t, len(cb.ExtendedBytes()), extendedTxSize(cb))
}

func TestAppendOutputInto_MatchesOutputBytes(t *testing.T) {
	arena := bt.NewArena(0)
	for _, hexScript := range []string{"", "76a914000000000000000000000000000000000000000088ac"} {
		s, err := bscript.NewFromHexString(hexScript)
		require.NoError(t, err)
		out := &bt.Output{Satoshis: 999, LockingScript: s}

		withArena := appendOutputInto(arena, out)
		require.Equal(t, out.Bytes(), withArena)

		withoutArena := appendOutputInto(nil, out)
		require.Equal(t, out.Bytes(), withoutArena)
	}
}

func TestAppendInputExtendedInto_MatchesManualLayout(t *testing.T) {
	arena := bt.NewArena(0)
	script, err := bscript.NewFromHexString("76a914000000000000000000000000000000000000000088ac")
	require.NoError(t, err)

	in := &bt.Input{PreviousTxOutIndex: 2, PreviousTxSatoshis: 4242, SequenceNumber: 0xffffffff}
	in.UnlockingScript = script
	in.PreviousTxScript = script
	require.NoError(t, in.PreviousTxIDAddStr("4a5e1e4baab89f3a32518a88c31bc87f618f76673e2cc77ab2127b7afdeda33b"))

	// Reference = the current manual layout: input.Bytes(false) + satoshis(8 LE)
	// + VarInt(prevScriptLen) + prevScript.
	want := in.Bytes(false)
	want = append(want,
		byte(in.PreviousTxSatoshis), byte(in.PreviousTxSatoshis>>8), byte(in.PreviousTxSatoshis>>16), byte(in.PreviousTxSatoshis>>24),
		byte(in.PreviousTxSatoshis>>32), byte(in.PreviousTxSatoshis>>40), byte(in.PreviousTxSatoshis>>48), byte(in.PreviousTxSatoshis>>56))
	want = append(want, bt.VarInt(uint64(len(*in.PreviousTxScript))).Bytes()...)
	want = append(want, *in.PreviousTxScript...)

	require.Equal(t, want, appendInputExtendedInto(arena, in))
	require.Equal(t, want, appendInputExtendedInto(nil, in))
}

func TestAppendInputExtendedInto_NilPrevScript(t *testing.T) {
	arena := bt.NewArena(0)
	script, err := bscript.NewFromHexString("ac")
	require.NoError(t, err)
	in := &bt.Input{PreviousTxOutIndex: 0, PreviousTxSatoshis: 1, SequenceNumber: 1}
	in.UnlockingScript = script
	in.PreviousTxScript = nil
	require.NoError(t, in.PreviousTxIDAddStr("4a5e1e4baab89f3a32518a88c31bc87f618f76673e2cc77ab2127b7afdeda33b"))

	want := in.Bytes(false)
	// satoshis = 1 little-endian (8 bytes)
	want = append(want, 1, 0, 0, 0, 0, 0, 0, 0)
	want = append(want, bt.VarInt(0).Bytes()...) // single 0x00 for nil prev script

	require.Equal(t, want, appendInputExtendedInto(arena, in))
}

func TestExtendedTxSize_NilPrevTxIDHash(t *testing.T) {
	// An input with no previousTxIDHash set: go-bt's ExtendedBytes() omits the
	// 32 txid bytes, but Input.Size() counts them. extendedTxSize must still
	// match len(ExtendedBytes()) — i.e. not rely on the hash being set.
	tx := bt.NewTx()
	in := &bt.Input{PreviousTxOutIndex: 0, PreviousTxSatoshis: 1000, SequenceNumber: 0xffffffff}
	in.UnlockingScript = &bscript.Script{}
	script, err := bscript.NewFromHexString("76a914000000000000000000000000000000000000000088ac")
	require.NoError(t, err)
	in.PreviousTxScript = script
	// deliberately do NOT set the previousTxIDHash
	require.Nil(t, in.PreviousTxIDChainHash(), "precondition for this test: hash is nil")
	tx.Inputs = append(tx.Inputs, in)
	tx.Outputs = append(tx.Outputs, &bt.Output{Satoshis: 1, LockingScript: script})

	require.Equal(t, len(tx.ExtendedBytes()), extendedTxSize(tx))
}

func BenchmarkAppendOutputInto_Arena(b *testing.B) {
	s, _ := bscript.NewFromHexString("76a914000000000000000000000000000000000000000088ac")
	out := &bt.Output{Satoshis: 1, LockingScript: s}
	arena := bt.NewArena(1 << 16)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if arena.Used() > 1<<15 {
			arena.Reset()
		}
		_ = appendOutputInto(arena, out)
	}
}

func TestAppendHelpers_LargeScripts(t *testing.T) {
	big := make([]byte, 300) // > 252 => 3-byte VarInt length prefix
	for i := range big {
		big[i] = 0x51
	}
	bigScript := bscript.Script(big)

	// Output with a >252-byte locking script
	out := &bt.Output{Satoshis: 7, LockingScript: &bigScript}
	require.Equal(t, out.Bytes(), appendOutputInto(bt.NewArena(0), out))
	require.Equal(t, out.Bytes(), appendOutputInto(nil, out))

	// Input with >252-byte unlocking AND prev script
	in := &bt.Input{PreviousTxOutIndex: 1, PreviousTxSatoshis: 9, SequenceNumber: 0xffffffff}
	in.UnlockingScript = &bigScript
	in.PreviousTxScript = &bigScript
	require.NoError(t, in.PreviousTxIDAddStr("4a5e1e4baab89f3a32518a88c31bc87f618f76673e2cc77ab2127b7afdeda33b"))

	want := in.Bytes(false)
	want = append(want, 9, 0, 0, 0, 0, 0, 0, 0) // satoshis = 9 LE
	want = append(want, bt.VarInt(uint64(len(bigScript))).Bytes()...)
	want = append(want, bigScript...)

	require.Equal(t, want, appendInputExtendedInto(bt.NewArena(0), in))
	require.Equal(t, want, appendInputExtendedInto(nil, in))
}
