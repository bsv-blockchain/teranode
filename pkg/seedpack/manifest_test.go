package seedpack

import (
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/stretchr/testify/require"
)

func sampleManifest() Manifest {
	var blockHash, setHash, c0, c1 [32]byte
	for i := range blockHash {
		blockHash[i] = byte(i)
		setHash[i] = byte(i + 1)
		c0[i] = byte(i + 2)
		c1[i] = byte(i + 3)
	}

	return Manifest{
		FormatVersion: FormatVersion,
		Height:        800000,
		BlockHash:     chainhash.Hash(blockHash),
		SetHash:       setHash,
		ChunkMin:      256 << 10,
		ChunkMax:      4 << 20,
		ChunkMask:     (1 << 20) - 1,
		Chunks: []ChunkRef{
			{Hash: c0, Size: 1024},
			{Hash: c1, Size: 2048},
		},
	}
}

func TestManifestRoundTrip(t *testing.T) {
	m := sampleManifest()

	got, err := ParseManifest(m.Serialize())
	require.NoError(t, err)
	require.Equal(t, m, got)
}

func TestManifestSerializeLength(t *testing.T) {
	m := sampleManifest()
	require.Len(t, m.Serialize(), 92+2*36)
}

func TestParseManifestRejectsTruncated(t *testing.T) {
	b := sampleManifest().Serialize()

	_, err := ParseManifest(b[:len(b)-1])
	require.Error(t, err)

	_, err = ParseManifest(b[:10])
	require.Error(t, err)
}

func TestParseManifestRejectsBadVersion(t *testing.T) {
	b := sampleManifest().Serialize()
	b[0] = 0xff

	_, err := ParseManifest(b)
	require.Error(t, err)
}
