package model

import (
	"bytes"
	"encoding/binary"
	"math"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/stretchr/testify/require"
)

func TestBestTipKey_RoundTrip(t *testing.T) {
	key := BestTipKey{
		ChainWork: bytes.Repeat([]byte{0x01}, 32),
		ID:        123456,
		Hash:      chainhash.Hash{1, 2, 3},
	}

	got, err := NewBestTipKeyFromBytes(key.Bytes())
	require.NoError(t, err)
	require.Equal(t, key, got)

	mark := BestTipLowWater{Key: key, Generation: 7}

	gotMark, err := NewBestTipLowWaterFromBytes(mark.Bytes())
	require.NoError(t, err)
	require.Equal(t, mark, gotMark)

	// An empty key, as a probe uses, encodes too.
	empty, err := NewBestTipKeyFromBytes(BestTipKey{}.Bytes())
	require.NoError(t, err)
	require.Empty(t, empty.ChainWork)
}

// A key an older binary saved carries a peer id. It still parses, and keeps its place by chain
// work and id, so a position block assembly saved before the upgrade is read, not refused.
// legacyBestTipKey encodes a key the way a binary that ordered by peer id wrote it: version 1,
// with the peer id between the chain work and the id.
func legacyBestTipKey(work, peer []byte, id uint32, hash chainhash.Hash) []byte {
	old := []byte{bestTipKeyLegacyVersion}
	old = binary.LittleEndian.AppendUint32(old, uint32(len(work)))
	old = append(old, work...)
	old = binary.LittleEndian.AppendUint32(old, uint32(len(peer)))
	old = append(old, peer...)
	old = binary.LittleEndian.AppendUint32(old, id)

	return append(old, hash[:]...)
}

// A version 1 key with a peer id held a place among equal-work blocks that its id does not give
// it in the order by id, so it is read last among blocks of its work, and a replay from it visits
// every equal-work sibling again. One with an empty peer id sorted first in the old order and
// keeps its id. The low-water mark carries the key and reads it the same way.
func TestBestTipKey_ReadsLegacyKey(t *testing.T) {
	work := bytes.Repeat([]byte{0x01}, 32)
	hash := chainhash.Hash{4, 5, 6}

	withPeer := legacyBestTipKey(work, []byte("12D3KooWpeer"), 77, hash)

	got, err := NewBestTipKeyFromBytes(withPeer)
	require.NoError(t, err)
	require.Equal(t, BestTipKey{ChainWork: work, ID: math.MaxUint32, Hash: hash}, got)

	mark, err := NewBestTipLowWaterFromBytes(binary.LittleEndian.AppendUint64(withPeer, 3))
	require.NoError(t, err)
	require.Equal(t, got, mark.Key)
	require.Equal(t, uint64(3), mark.Generation)

	got, err = NewBestTipKeyFromBytes(legacyBestTipKey(work, nil, 77, hash))
	require.NoError(t, err)
	require.Equal(t, BestTipKey{ChainWork: work, ID: 77, Hash: hash}, got)
}

// A request key must be in the current version: a version 1 key comes from a block assembly
// that orders by peer id, which falls back to following the best header when it is refused.
func TestBestTipKey_RequestKeyMustBeCurrent(t *testing.T) {
	work := bytes.Repeat([]byte{0x01}, 32)
	key := BestTipKey{ChainWork: work, ID: 9, Hash: chainhash.Hash{1}}

	got, err := NewCurrentBestTipKeyFromBytes(key.Bytes())
	require.NoError(t, err)
	require.Equal(t, key, got)

	for name, b := range map[string][]byte{
		"legacy with peer":  legacyBestTipKey(work, []byte("peer"), 9, chainhash.Hash{1}),
		"legacy empty peer": legacyBestTipKey(work, nil, 9, chainhash.Hash{1}),
		"empty":             nil,
	} {
		_, err := NewCurrentBestTipKeyFromBytes(b)
		require.Error(t, err, name)
	}
}

func TestBestTipKey_RejectsOtherBytes(t *testing.T) {
	key := BestTipKey{ChainWork: bytes.Repeat([]byte{0x01}, 32), ID: 1}
	encoded := key.Bytes()

	// The 8-byte sequence number an unreleased build persisted.
	seq := binary.LittleEndian.AppendUint64(nil, 42)

	for name, b := range map[string][]byte{
		"empty":           nil,
		"sequence number": seq,
		"truncated":       encoded[:len(encoded)-1],
		"trailing":        append(append([]byte(nil), encoded...), 0),
		"field too long":  {bestTipKeyVersion, 0xff, 0xff, 0xff, 0xff},
	} {
		_, err := NewBestTipKeyFromBytes(b)
		require.Error(t, err, name)
	}

	_, err := NewBestTipLowWaterFromBytes(encoded)
	require.Error(t, err, "a mark carries its generation")
}

func TestBestTipLowWater_DroppedTips(t *testing.T) {
	key := BestTipKey{ChainWork: bytes.Repeat([]byte{0x01}, 32), ID: 9}

	mark := BestTipLowWater{Key: key, Generation: 3}
	mark.AddDroppedTip(chainhash.Hash{1})
	mark.AddDroppedTip(chainhash.Hash{2})
	mark.AddDroppedTip(chainhash.Hash{1})
	require.Equal(t, []chainhash.Hash{{1}, {2}}, mark.DroppedTips, "a tip that dropped twice is listed once")
	require.False(t, mark.DroppedTipsOverflow)

	got, err := NewBestTipLowWaterFromBytes(mark.Bytes())
	require.NoError(t, err)
	require.Equal(t, mark, got)

	// The first encoding ended at the generation and carried no dropped tips.
	firstEncoding := binary.LittleEndian.AppendUint64(key.Bytes(), 5)
	got, err = NewBestTipLowWaterFromBytes(firstEncoding)
	require.NoError(t, err)
	require.Equal(t, BestTipLowWater{Key: key, Generation: 5}, got)

	// Past the bound a drop is not listed but still recorded, as the overflow flag.
	full := BestTipLowWater{Key: key}
	for i := 0; i < MaxBestTipLowWaterDroppedTips; i++ {
		full.AddDroppedTip(chainhash.Hash{byte(i), 1})
	}

	require.False(t, full.DroppedTipsOverflow)
	full.AddDroppedTip(chainhash.Hash{0xff})
	require.True(t, full.DroppedTipsOverflow)
	require.Len(t, full.DroppedTips, MaxBestTipLowWaterDroppedTips)

	got, err = NewBestTipLowWaterFromBytes(full.Bytes())
	require.NoError(t, err)
	require.Equal(t, full, got)

	encoded := mark.Bytes()
	for name, b := range map[string][]byte{
		"truncated hash":  encoded[:len(encoded)-1],
		"trailing":        append(append([]byte(nil), encoded...), 0),
		"unknown version": append(append([]byte(nil), firstEncoding...), 9, 0, 0, 0, 0, 0),
		"unknown flags":   append(append([]byte(nil), firstEncoding...), bestTipLowWaterTipsVersion, 2, 0, 0, 0, 0),
		"short header":    append(append([]byte(nil), firstEncoding...), bestTipLowWaterTipsVersion),
	} {
		_, err = NewBestTipLowWaterFromBytes(b)
		require.Error(t, err, name)
	}
}
