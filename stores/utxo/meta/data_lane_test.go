package meta

import (
	"encoding/hex"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/util/lane"
	"github.com/stretchr/testify/require"
)

func laneSample() (*Data, []uint64) {
	return &Data{
		Fee:          300,
		SizeInBytes:  226,
		Locked:       true,
		InBlock:      true,
		LanePosition: lane.Pack(200, 41_000),
		TxInpoints:   testInpointsHash3Hash4,
	}, []uint64{lane.Pack(255, 2), lane.Pack(0, 1)}
}

func TestLaneMetaBytesRoundTrip(t *testing.T) {
	d, parents := laneSample()

	b, err := d.LaneMetaBytesInto(nil, parents)
	require.NoError(t, err)

	legacy, err := d.MetaBytes()
	require.NoError(t, err)

	// The lane encoding is magic, version and own position in front of the unchanged MetaBytes
	// body, then the parent positions.
	require.Equal(t, []byte{LaneMetaMagic, LaneMetaVersion}, b[:2])
	require.Equal(t, lane.AppendPosition(nil, d.LanePosition), b[2:10])
	require.Equal(t, legacy, b[10:10+len(legacy)])
	require.Equal(t, lane.AppendPositions(nil, parents), b[10+len(legacy):])

	var got Data

	gotParents, err := NewDataFromLaneMetaBytes(b, &got)
	require.NoError(t, err)
	require.Equal(t, parents, gotParents)
	require.Equal(t, d.LanePosition, got.LanePosition)
	require.Equal(t, d.Fee, got.Fee)
	require.Equal(t, d.SizeInBytes, got.SizeInBytes)
	require.True(t, got.Locked)
	require.True(t, got.InBlock)
	require.False(t, got.IsCoinbase)
	require.Equal(t, d.TxInpoints.ParentTxHashes, got.TxInpoints.ParentTxHashes)
	require.Equal(t, d.TxInpoints.GetTxInpoints(), got.TxInpoints.GetTxInpoints())

	// A recycled buffer is reused and the result still decodes.
	reused, err := d.LaneMetaBytesInto(make([]byte, 7, 4096), parents)
	require.NoError(t, err)
	require.Equal(t, b, reused)
}

func TestLaneMetaBytesGolden(t *testing.T) {
	d := &Data{Fee: 1, SizeInBytes: 2, IsCoinbase: true, LanePosition: lane.Pack(254, 9), TxInpoints: subtree.NewTxInpoints()}

	b, err := d.LaneMetaBytesInto(nil, nil)
	require.NoError(t, err)
	require.Equal(t, "a701"+"09000000000000fe"+"0100000000000000"+"0200000000000000"+"01"+"00000000", hex.EncodeToString(b))
}

func TestLaneMetaBytesRefusesMissingPositions(t *testing.T) {
	d, parents := laneSample()

	_, err := d.LaneMetaBytesInto(nil, parents[:1])
	require.Error(t, err, "one parent without a position")

	_, err = d.LaneMetaBytesInto(nil, []uint64{parents[0], lane.Pack(3, 0)})
	require.Error(t, err, "zero parent position")

	d.LanePosition = lane.None
	_, err = d.LaneMetaBytesInto(nil, parents)
	require.Error(t, err, "no own position")
}

func TestNewDataFromLaneMetaBytesRejects(t *testing.T) {
	d, parents := laneSample()

	good, err := d.LaneMetaBytesInto(nil, parents)
	require.NoError(t, err)

	legacy, err := d.MetaBytes()
	require.NoError(t, err)

	zeroOwn := append([]byte{}, good...)
	copy(zeroOwn[2:10], make([]byte, 8))

	zeroParent := append([]byte{}, good...)
	copy(zeroParent[len(zeroParent)-8:], make([]byte, 8))

	badFlags := append([]byte{}, good...)
	badFlags[26] |= 0x80

	for name, b := range map[string][]byte{
		"legacy MetaBytes": legacy,
		"empty":            {},
		"wrong magic":      append([]byte{0xA6}, good[1:]...),
		"version 2":        append([]byte{LaneMetaMagic, 2}, good[2:]...),
		"truncated":        good[:len(good)-1],
		"trailing":         append(append([]byte{}, good...), 0),
		"zero own":         zeroOwn,
		"zero parent":      zeroParent,
		"unknown flag":     badFlags,
	} {
		before := Data{Fee: 77}
		_, err := NewDataFromLaneMetaBytes(b, &before)
		require.Error(t, err, name)
		require.Equal(t, Data{Fee: 77}, before, "%s: d is unchanged on error", name)
	}

	// A parent listed twice cannot be matched to its positions.
	dup := &Data{LanePosition: lane.Pack(1, 1), TxInpoints: subtree.NewTxInpointsFromPacked([]chainhash.Hash{*hash3, *hash3}, []uint32{1, 0, 1, 1})}
	_, err = dup.LaneMetaBytesInto(nil, []uint64{lane.Pack(1, 2), lane.Pack(1, 3)})
	require.Error(t, err)
}

// The legacy encodings are unchanged by the lane encoding: these are their exact bytes.
func TestLegacyMetaEncodingsAreUnchanged(t *testing.T) {
	d, _ := laneSample()

	meta, err := d.MetaBytes()
	require.NoError(t, err)
	require.Equal(t, "2c01000000000000"+"e200000000000000"+"18"+"02000000"+
		hex.EncodeToString(hash3[:])+hex.EncodeToString(hash4[:])+
		"02000000"+"01000000"+"02000000"+"02000000"+"03000000"+"04000000", hex.EncodeToString(meta))

	full, err := d.Bytes()
	require.NoError(t, err)
	require.Equal(t, meta, full, "no tx and no block IDs: Bytes is MetaBytes")

	var back Data
	require.NoError(t, NewMetaDataFromBytes(meta, &back))
	require.Equal(t, lane.None, back.LanePosition, "the legacy encoding never carries a position")
}

func FuzzNewDataFromLaneMetaBytes(f *testing.F) {
	d, parents := laneSample()

	b, err := d.LaneMetaBytesInto(nil, parents)
	if err != nil {
		f.Fatal(err)
	}

	f.Add(b)

	f.Fuzz(func(t *testing.T, data []byte) {
		var got Data

		ps, err := NewDataFromLaneMetaBytes(data, &got)
		if err != nil {
			return
		}

		again, err := got.LaneMetaBytesInto(nil, ps)
		require.NoError(t, err)
		require.Equal(t, data, again)
	})
}
