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

// laneMetaByHand writes the lane metadata encoding without the encoder's checks, so a decoder test
// can hand it what the encoder refuses to write.
func laneMetaByHand(t *testing.T, own uint64, flags byte, inpoints subtree.TxInpoints, parents []uint64) []byte {
	t.Helper()

	b := []byte{LaneMetaMagic, LaneMetaVersion}
	b = lane.AppendPosition(b, own)
	b = append(b, make([]byte, 16)...) // fee and size
	b = append(b, flags)

	b, err := inpoints.AppendSerialized(b)
	require.NoError(t, err)

	return lane.AppendPositions(b, parents)
}

// The decoder refuses on its own what the encoder would never write: a parent listed twice, an
// inpoints region cut short, and any buffer shorter than the fixed header.
func TestNewDataFromLaneMetaBytesRefusesWhatTheEncoderWouldNotWrite(t *testing.T) {
	dupInpoints := subtree.NewTxInpointsFromPacked([]chainhash.Hash{*hash3, *hash3}, []uint32{1, 0, 1, 1})
	dup := laneMetaByHand(t, lane.Pack(1, 1), 0, dupInpoints, []uint64{lane.Pack(1, 2), lane.Pack(1, 3)})

	var got Data

	_, err := NewDataFromLaneMetaBytes(dup, &got)
	require.Error(t, err, "a parent listed twice")

	// The parent count promises one parent and the buffer ends there.
	cut := laneMetaByHand(t, lane.Pack(1, 1), 0, subtree.NewTxInpoints(), nil)
	cut = append(cut[:laneMetaHeader], 1, 0, 0, 0)
	_, err = NewDataFromLaneMetaBytes(cut, &got)
	require.Error(t, err, "inpoints cut short")

	// The same cut, with the bytes of a whole parent hash but no vouts.
	cut = append(append([]byte{}, cut...), hash3[:]...)
	_, err = NewDataFromLaneMetaBytes(cut, &got)
	require.Error(t, err, "inpoints cut inside a parent")

	d, parents := laneSample()
	good, err := d.LaneMetaBytesInto(nil, parents)
	require.NoError(t, err)

	for n := 0; n < laneMetaHeader+4; n++ {
		require.NotPanics(t, func() { _, err = NewDataFromLaneMetaBytes(good[:n], &got) }, "%d bytes", n)
		require.Error(t, err, "%d bytes", n)
	}
}

// Every flag round-trips on its own and together, and each lands on the bit MetaBytes uses.
func TestLaneMetaBytesKeepsEveryFlag(t *testing.T) {
	set := map[string]func(*Data){
		"coinbase":    func(d *Data) { d.IsCoinbase = true },
		"frozen":      func(d *Data) { d.Frozen = true },
		"conflicting": func(d *Data) { d.Conflicting = true },
		"locked":      func(d *Data) { d.Locked = true },
		"in block":    func(d *Data) { d.InBlock = true },
		"none":        func(*Data) {},
		"all": func(d *Data) {
			d.IsCoinbase, d.Frozen, d.Conflicting, d.Locked, d.InBlock = true, true, true, true, true
		},
	}

	for name, apply := range set {
		d, parents := laneSample()
		d.Locked, d.InBlock = false, false
		apply(d)

		b, err := d.LaneMetaBytesInto(nil, parents)
		require.NoError(t, err, name)

		legacy, err := d.MetaBytes()
		require.NoError(t, err, name)
		require.Equal(t, legacy[16], b[26], "%s: the flags byte is MetaBytes' own", name)

		var got Data

		_, err = NewDataFromLaneMetaBytes(b, &got)
		require.NoError(t, err, name)
		require.Equal(t,
			[5]bool{d.IsCoinbase, d.Frozen, d.Conflicting, d.Locked, d.InBlock},
			[5]bool{got.IsCoinbase, got.Frozen, got.Conflicting, got.Locked, got.InBlock}, name)
	}
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

func TestCacheBytesCarryTheOwnPositionAfterTheMetaBytes(t *testing.T) {
	d, parents := laneSample()

	legacy, err := d.MetaBytes()
	require.NoError(t, err)

	cached, err := d.CacheBytes()
	require.NoError(t, err)
	require.Equal(t, append(append([]byte(nil), legacy...), lane.AppendPosition(nil, d.LanePosition)...), cached)
	require.Equal(t, d.LanePosition, CachedLanePosition(cached[len(legacy):]))

	// A reader that ignores what follows the inpoints reads the entry as before.
	var back Data
	require.NoError(t, NewMetaDataFromBytes(cached, &back))
	require.Equal(t, d.Fee, back.Fee)
	require.Equal(t, d.TxInpoints.ParentTxHashes, back.TxInpoints.ParentTxHashes)

	// The lane encoding gives the same cache form.
	content, err := d.LaneMetaBytesInto(nil, parents)
	require.NoError(t, err)

	fromLane, err := AppendCacheFromLaneMetaBytes(nil, content)
	require.NoError(t, err)
	require.Equal(t, cached, fromLane)
}

func TestCacheBytesWithoutAPositionAreTheMetaBytes(t *testing.T) {
	d, _ := laneSample()
	d.LanePosition = lane.Pack(9, 0) // a lane but no position

	legacy, err := d.MetaBytes()
	require.NoError(t, err)

	cached, err := d.CacheBytes()
	require.NoError(t, err)
	require.Equal(t, legacy, cached)

	require.Zero(t, CachedLanePosition(nil))
	require.Zero(t, CachedLanePosition([]byte{1, 2, 3}), "only exactly eight bytes are a position")
}

func TestAppendCacheFromLaneMetaBytesRefusesWithoutChangingDst(t *testing.T) {
	d, parents := laneSample()

	content, err := d.LaneMetaBytesInto(nil, parents)
	require.NoError(t, err)

	// An own position with a lane and no position is refused, as is a truncated entry.
	bad := append([]byte(nil), content...)
	copy(bad[2:10], lane.AppendPosition(nil, lane.Pack(4, 0)))

	dst := []byte{0xAA}

	got, err := AppendCacheFromLaneMetaBytes(dst, bad)
	require.Error(t, err)
	require.Equal(t, []byte{0xAA}, got)

	got, err = AppendCacheFromLaneMetaBytes(dst, content[:len(content)-1])
	require.Error(t, err)
	require.Equal(t, []byte{0xAA}, got)
}

func TestCacheBytesReportAMetaBytesError(t *testing.T) {
	// Two parents and a single vout word, fewer than one count word per parent: the inpoints
	// cannot serialize.
	d := &Data{LanePosition: lane.Pack(3, 9), TxInpoints: subtree.NewTxInpointsFromPacked([]chainhash.Hash{*hash3, *hash4}, []uint32{0})}

	_, wantErr := d.MetaBytes()
	require.Error(t, wantErr, "the fixture really fails to serialize")

	_, err := d.CacheBytes()
	require.Error(t, err)
}

func TestCacheBytesIntoReuseTheCallersBuffer(t *testing.T) {
	d, _ := laneSample()

	dst := make([]byte, 0, 4096)

	got, err := d.CacheBytesInto(dst)
	require.NoError(t, err)
	require.Same(t, &dst[:1][0], &got[0], "written into dst's backing array")
}

func TestCachedLanePositionReadsExactlyEightBytes(t *testing.T) {
	nine := lane.AppendPosition(nil, lane.Pack(2, 5))
	require.Equal(t, lane.Pack(2, 5), CachedLanePosition(nine))
	require.Zero(t, CachedLanePosition(append(nine, 0)), "nine bytes are not a position")
}

func TestAppendCacheFromLaneMetaBytesRefusesShortInput(t *testing.T) {
	for n := 0; n < 12; n++ {
		_, err := AppendCacheFromLaneMetaBytes(nil, make([]byte, n))
		require.Error(t, err, "%d bytes", n)
	}
}

// A WireActionADDOwn entry's content is the cache form, and the view gives back its MetaBytes,
// without a copy, and its own position.
func TestOwnCacheBytesViewReadsTheCacheForm(t *testing.T) {
	d, _ := laneSample()

	content, err := d.CacheBytes()
	require.NoError(t, err)

	legacy, err := d.MetaBytes()
	require.NoError(t, err)

	got, own, err := OwnCacheBytesView(content)
	require.NoError(t, err)
	require.Equal(t, legacy, got)
	require.Equal(t, d.LanePosition, own)
	require.Equal(t, len(got), cap(got), "an append to the MetaBytes cannot write into the position")
	require.Same(t, &content[0], &got[0], "nothing is copied")

	// A record with no parents, a coinbase's shape, has a position straight after the header.
	coinbase := &Data{Fee: 1, SizeInBytes: 2, IsCoinbase: true, LanePosition: lane.Pack(0, 1)}

	content, err = coinbase.CacheBytes()
	require.NoError(t, err)

	_, own, err = OwnCacheBytesView(content)
	require.NoError(t, err)
	require.Equal(t, lane.Pack(0, 1), own)
}

func TestOwnCacheBytesViewRefusesWhatCacheBytesWouldNotWrite(t *testing.T) {
	d, parents := laneSample()

	content, err := d.CacheBytes()
	require.NoError(t, err)

	legacy, err := d.MetaBytes()
	require.NoError(t, err)

	laneContent, err := d.LaneMetaBytesInto(nil, parents)
	require.NoError(t, err)

	withPosition := func(v uint64) []byte {
		return lane.AppendPosition(append([]byte(nil), legacy...), v)
	}

	for name, bad := range map[string][]byte{
		"MetaBytes alone":        legacy,
		"one byte short":         content[:len(content)-1],
		"one byte extra":         append(append([]byte(nil), content...), 0),
		"zero position":          withPosition(lane.None),
		"a lane and no position": withPosition(lane.Pack(4, 0)),
		"the lane encoding":      laneContent,
		"empty":                  nil,
	} {
		_, _, err := OwnCacheBytesView(bad)
		require.Error(t, err, name)
	}

	for n := 0; n < 29; n++ {
		_, _, err := OwnCacheBytesView(make([]byte, n))
		require.Error(t, err, "%d zero bytes", n)
	}
}

// Whatever the view accepts is MetaBytes and one valid position, and it never panics.
func FuzzOwnCacheBytesView(f *testing.F) {
	d, _ := laneSample()

	content, err := d.CacheBytes()
	require.NoError(f, err)

	f.Add(content)
	f.Add(content[:len(content)-1])
	f.Add([]byte{})

	f.Fuzz(func(t *testing.T, b []byte) {
		legacy, own, err := OwnCacheBytesView(b)
		if err != nil {
			return
		}

		require.Len(t, legacy, len(b)-lane.PositionSize)
		require.True(t, lane.Valid(own))

		var back Data
		require.NoError(t, NewMetaDataFromBytes(legacy, &back))
	})
}
