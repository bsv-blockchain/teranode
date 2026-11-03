package model

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/stretchr/testify/require"
)

func TestBestTipKey_RoundTrip(t *testing.T) {
	key := BestTipKey{
		ChainWork: bytes.Repeat([]byte{0x01}, 32),
		PeerID:    "12D3KooWpeer",
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

func TestBestTipKey_RejectsOtherBytes(t *testing.T) {
	key := BestTipKey{ChainWork: bytes.Repeat([]byte{0x01}, 32), PeerID: "p", ID: 1}
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
	key := BestTipKey{ChainWork: bytes.Repeat([]byte{0x01}, 32), PeerID: "p", ID: 9}

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
