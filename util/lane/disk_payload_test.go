package lane

import (
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/stretchr/testify/require"
)

func TestDiskPayloadRoundTrip(t *testing.T) {
	inpoints := subtreepkg.NewTxInpointsFromPacked([]chainhash.Hash{{0xA}, {0xB}}, []uint32{1, 3, 2, 0, 1})
	p := TxPositions{Own: Pack(200, 77), Parents: []uint64{Pack(255, 2), Pack(0, 1)}}

	b, err := EncodeDiskPayload(&inpoints, p)
	require.NoError(t, err)

	legacy, err := inpoints.Serialize()
	require.NoError(t, err)

	// version, own, the unchanged go-subtree inpoints, then the parent positions.
	require.Equal(t, DiskPayloadVersion, b[0])
	require.Equal(t, AppendPosition(nil, p.Own), b[1:9])
	require.Equal(t, legacy, b[9:9+len(legacy)])
	require.Equal(t, AppendPositions(nil, p.Parents), b[9+len(legacy):])

	gotInpoints, gotP, err := DecodeDiskPayload(b)
	require.NoError(t, err)
	require.Equal(t, p, gotP)
	require.Equal(t, inpoints.ParentTxHashes, gotInpoints.ParentTxHashes)
	require.Equal(t, inpoints.GetTxInpoints(), gotInpoints.GetTxInpoints())
}

func TestDiskPayloadNoParents(t *testing.T) {
	inpoints := subtreepkg.NewTxInpoints()

	b, err := EncodeDiskPayload(&inpoints, TxPositions{Own: Pack(0, 1)})
	require.NoError(t, err)

	_, p, err := DecodeDiskPayload(b)
	require.NoError(t, err)
	require.Equal(t, Pack(0, 1), p.Own)
	require.Empty(t, p.Parents)
}

func TestDiskPayloadRejects(t *testing.T) {
	inpoints := subtreepkg.NewTxInpointsFromPacked([]chainhash.Hash{{0xA}}, []uint32{1, 0})

	_, err := EncodeDiskPayload(&inpoints, TxPositions{Own: Pack(1, 1)})
	require.Error(t, err, "a parent without a position")

	_, err = EncodeDiskPayload(nil, TxPositions{Own: Pack(1, 1)})
	require.Error(t, err)

	_, err = EncodeDiskPayload(&inpoints, TxPositions{Parents: []uint64{Pack(1, 1)}})
	require.Error(t, err, "no own position")

	good, err := EncodeDiskPayload(&inpoints, TxPositions{Own: Pack(1, 1), Parents: []uint64{Pack(2, 2)}})
	require.NoError(t, err)

	legacy, err := inpoints.Serialize()
	require.NoError(t, err)

	for name, b := range map[string][]byte{
		"legacy payload": legacy,
		"version 2":      append([]byte{2}, good[1:]...),
		"truncated":      good[:len(good)-1],
		"trailing":       append(append([]byte{}, good...), 0),
		"zero own":       append(append([]byte{1}, make([]byte, 8)...), good[9:]...),
		"short":          good[:5],
	} {
		_, _, err := DecodeDiskPayload(b)
		require.Error(t, err, name)
	}

	// Written by hand, past the encoder's checks: a parent listed twice, which go-subtree's reader
	// accepts, so only the decoder's own position check refuses it.
	dup := subtreepkg.NewTxInpointsFromPacked([]chainhash.Hash{{0xA}, {0xA}}, []uint32{1, 0, 1, 1})

	b := AppendPosition([]byte{DiskPayloadVersion}, Pack(1, 1))
	b, err = dup.AppendSerialized(b)
	require.NoError(t, err)
	b = AppendPositions(b, []uint64{Pack(1, 2), Pack(1, 3)})

	_, _, err = DecodeDiskPayload(b)
	require.Error(t, err, "a parent listed twice")
}

func FuzzDecodeDiskPayload(f *testing.F) {
	inpoints := subtreepkg.NewTxInpointsFromPacked([]chainhash.Hash{{0xA}}, []uint32{1, 0})

	b, err := EncodeDiskPayload(&inpoints, TxPositions{Own: Pack(1, 1), Parents: []uint64{Pack(2, 2)}})
	if err != nil {
		f.Fatal(err)
	}

	f.Add(b)

	f.Fuzz(func(t *testing.T, data []byte) {
		in, p, err := DecodeDiskPayload(data)
		if err != nil {
			return
		}

		again, err := EncodeDiskPayload(&in, p)
		require.NoError(t, err)
		require.Equal(t, data, again)
	})
}
