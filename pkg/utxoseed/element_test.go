package utxoseed

import (
	"encoding/hex"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/stretchr/testify/require"
)

func TestElementLayout(t *testing.T) {
	var txid chainhash.Hash      // all-zero txid
	script := []byte{0x76, 0xa9} // OP_DUP OP_HASH160 (truncated, fine for a layout test)

	got := Element(txid, 1, 5, true, 50, script)

	// 32 (txid) + 4 (vout) + 4 (height/coinbase) + 8 (value) + 4 (scriptLen) + 2 (script)
	require.Len(t, got, 32+4+4+8+4+len(script))

	// txid: 32 zero bytes
	require.Equal(t, make([]byte, 32), got[0:32])
	// vout = 1, little-endian
	require.Equal(t, []byte{0x01, 0x00, 0x00, 0x00}, got[32:36])
	// height<<1 | coinbase = (5<<1)|1 = 11 = 0x0b, little-endian
	require.Equal(t, []byte{0x0b, 0x00, 0x00, 0x00}, got[36:40])
	// value = 50, little-endian uint64
	require.Equal(t, []byte{0x32, 0, 0, 0, 0, 0, 0, 0}, got[40:48])
	// scriptLen = 2, little-endian uint32
	require.Equal(t, []byte{0x02, 0x00, 0x00, 0x00}, got[48:52])
	// script bytes
	require.Equal(t, script, got[52:54])
}

func TestElementCoinbaseFlagOff(t *testing.T) {
	var txid chainhash.Hash
	got := Element(txid, 0, 5, false, 0, nil)
	// height<<1 | 0 = 10 = 0x0a
	require.Equal(t, []byte{0x0a, 0x00, 0x00, 0x00}, got[36:40])
}

func TestElementDistinctByVout(t *testing.T) {
	var txid chainhash.Hash
	a := Element(txid, 0, 1, false, 100, nil)
	b := Element(txid, 1, 1, false, 100, nil)
	require.NotEqual(t, hex.EncodeToString(a), hex.EncodeToString(b))
}

func TestElementAcceptsMaxValidHeight(t *testing.T) {
	var txid chainhash.Hash
	// 2^31 - 1 is the largest height that fits the (height<<1 | coinbase) word.
	require.NotPanics(t, func() { Element(txid, 0, (1<<31)-1, true, 1, nil) })
}

func TestElementPanicsOnHeightOverflow(t *testing.T) {
	var txid chainhash.Hash
	// height >= 2^31 would overflow height<<1 and alias distinct UTXOs; the
	// commitment invariant is enforced with a panic (unreachable from seed data,
	// where height is always encodedHeight>>1 of a uint32).
	require.Panics(t, func() { Element(txid, 0, 1<<31, false, 1, nil) })
}
