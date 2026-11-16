package txmetacache

import (
	"encoding/binary"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/stretchr/testify/require"
)

// checkDecodeMatchesReference decodes b with both decoders and requires the
// same verdict and, on success, the same fields the subtree validator reads.
func checkDecodeMatchesReference(t *testing.T, b []byte) {
	t.Helper()

	var ref meta.Data
	refErr := meta.NewMetaDataFromBytes(b, &ref)

	parents, vouts := metaArenaNeed(b)
	ps := make([]chainhash.Hash, parents)
	vs := make([]uint32, vouts)

	got, np, nv, err := decodeCachedMeta(b, ps, vs)

	if refErr != nil {
		require.Error(t, err, "reference rejected %x", b)
		return
	}

	require.NoError(t, err, "reference accepted %x", b)
	require.Equal(t, ref.Fee, got.Fee)
	require.Equal(t, ref.SizeInBytes, got.SizeInBytes)
	require.Equal(t, ref.IsCoinbase, got.IsCoinbase)
	require.Equal(t, ref.Frozen, got.Frozen)
	require.Equal(t, ref.Conflicting, got.Conflicting)
	require.Equal(t, ref.Locked, got.Locked)
	require.Equal(t, ref.InBlock, got.InBlock)
	require.Equal(t, ref.TxInpoints.ParentTxHashes, got.TxInpoints.ParentTxHashes)
	require.LessOrEqual(t, np, parents)
	require.LessOrEqual(t, nv, vouts)

	refBytes, refSerErr := ref.TxInpoints.Serialize()
	gotBytes, gotSerErr := got.TxInpoints.Serialize()
	require.Equal(t, refSerErr == nil, gotSerErr == nil)
	require.Equal(t, refBytes, gotBytes, "packed vouts differ")
}

func TestDecodeCachedMeta_MatchesReference(t *testing.T) {
	for i := uint64(0); i < 2000; i++ {
		b := appendMixedValue(nil, i)
		checkDecodeMatchesReference(t, b)

		// The legacy write path appends a 4-byte height; it must be ignored.
		checkDecodeMatchesReference(t, binary.BigEndian.AppendUint32(append([]byte{}, b...), 123))
	}

	// Real MetaBytes output, flags set, several vouts per parent, coinbase
	// without parents.
	d := &meta.Data{
		Fee:         42,
		SizeInBytes: 999,
		Conflicting: true,
		Locked:      true,
		TxInpoints: subtree.NewTxInpointsFromPacked(
			[]chainhash.Hash{mixedKey(1), mixedKey(2)},
			[]uint32{3, 0, 5, 9, 1, 7},
		),
	}
	b, err := d.MetaBytes()
	require.NoError(t, err)
	checkDecodeMatchesReference(t, b)

	cb := &meta.Data{Fee: 1, SizeInBytes: 2, IsCoinbase: true}
	b, err = cb.MetaBytes()
	require.NoError(t, err)
	checkDecodeMatchesReference(t, b)

	got, _, _, err := decodeCachedMeta(b, nil, nil)
	require.NoError(t, err)
	require.Nil(t, got.TxInpoints.ParentTxHashes, "no parents must decode to nil, like the reference")
}

func TestDecodeCachedMeta_RejectsTruncated(t *testing.T) {
	b := appendMixedValue(nil, 2) // 3 parents

	for n := 0; n < len(b); n++ {
		checkDecodeMatchesReference(t, b[:n])
	}
}

func FuzzDecodeCachedMeta(f *testing.F) {
	f.Add(appendMixedValue(nil, 0))
	f.Add(appendMixedValue(nil, 2))
	f.Add(make([]byte, 21))

	f.Fuzz(func(t *testing.T, b []byte) {
		checkDecodeMatchesReference(t, b)
	})
}
