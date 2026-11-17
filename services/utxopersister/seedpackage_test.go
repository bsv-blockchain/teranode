package utxopersister

import (
	"bytes"
	"context"
	"crypto/sha256"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/pkg/seedpack"
	"github.com/bsv-blockchain/teranode/stores/blob"
	"github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/bsv-blockchain/teranode/stores/blob/options"
	"github.com/stretchr/testify/require"
)

// overwriteChunk replaces a stored chunk's bytes in place. It is a test-only
// helper for corrupting a seed package to exercise the chunk-integrity checks.
func overwriteChunk(ctx context.Context, store blob.Store, hash [32]byte, data []byte) error {
	return store.Set(ctx, hash[:], fileformat.FileTypeSeedChunk, data, options.WithAllowOverwrite(true))
}

func pseudoBytes(n int, seed uint64) []byte {
	out := make([]byte, n)
	x := seed
	for i := range out {
		x += 0x9e3779b97f4a7c15
		z := x
		z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9
		z = (z ^ (z >> 27)) * 0x94d049bb133111eb
		z = z ^ (z >> 31)
		out[i] = byte(z)
	}
	return out
}

func testChunkCfg() seedpack.Config {
	return seedpack.Config{Min: 16, Max: 256, Mask: (1 << 6) - 1}
}

func TestSeedPackageRoundTrip(t *testing.T) {
	ctx := context.Background()
	store := memory.New()

	body := pseudoBytes(8000, 11)
	blockHash := chainhash.HashH([]byte("seed-block"))

	var setHash [32]byte
	for i := range setHash {
		setHash[i] = byte(i)
	}

	require.NoError(t, BuildSeedPackage(ctx, store, bytes.NewReader(body), 700000, blockHash, setHash, testChunkCfg()))

	var got bytes.Buffer
	require.NoError(t, StreamSeedPackage(ctx, store, blockHash, &got))
	require.Equal(t, body, got.Bytes())
}

func TestSeedPackageDetectsChunkCorruption(t *testing.T) {
	ctx := context.Background()
	store := memory.New()

	body := pseudoBytes(8000, 12)
	blockHash := chainhash.HashH([]byte("seed-block-2"))

	require.NoError(t, BuildSeedPackage(ctx, store, bytes.NewReader(body), 1, blockHash, [32]byte{}, testChunkCfg()))

	m, err := readManifest(ctx, store, blockHash)
	require.NoError(t, err)

	bad := make([]byte, int(m.Chunks[0].Size))
	require.NoError(t, overwriteChunk(ctx, store, m.Chunks[0].Hash, bad))

	var got bytes.Buffer
	err = StreamSeedPackage(ctx, store, blockHash, &got)
	require.Error(t, err, "reassembly must reject a chunk whose content hash no longer matches")
}

func TestSeedPackageDedup(t *testing.T) {
	ctx := context.Background()
	store := memory.New()

	a := pseudoBytes(20000, 13)

	const at = 10000
	b := make([]byte, 0, len(a)+5)
	b = append(b, a[:at]...)
	b = append(b, []byte("YYYYY")...)
	b = append(b, a[at:]...)

	ha := chainhash.HashH([]byte("a"))
	hb := chainhash.HashH([]byte("b"))

	require.NoError(t, BuildSeedPackage(ctx, store, bytes.NewReader(a), 1, ha, [32]byte{}, testChunkCfg()))
	require.NoError(t, BuildSeedPackage(ctx, store, bytes.NewReader(b), 2, hb, [32]byte{}, testChunkCfg()))

	ma, err := readManifest(ctx, store, ha)
	require.NoError(t, err)
	mb, err := readManifest(ctx, store, hb)
	require.NoError(t, err)

	setB := make(map[[32]byte]struct{}, len(mb.Chunks))
	for _, c := range mb.Chunks {
		setB[c.Hash] = struct{}{}
	}

	shared := 0
	for _, c := range ma.Chunks {
		if _, ok := setB[c.Hash]; ok {
			shared++
		}
	}

	require.Greater(t, shared*2, len(ma.Chunks), "the second seed should reuse >50%% of the first seed's chunks")

	for _, c := range ma.Chunks {
		blob, err := getChunk(ctx, store, c.Hash)
		require.NoError(t, err)
		require.Equal(t, c.Hash, sha256.Sum256(blob))
	}
}

func TestStreamSeedPackageMatchesReassembly(t *testing.T) {
	ctx := context.Background()
	store := memory.New()

	body := pseudoBytes(8000, 21)
	blockHash := chainhash.HashH([]byte("stream-block"))

	require.NoError(t, BuildSeedPackage(ctx, store, bytes.NewReader(body), 1, blockHash, [32]byte{}, testChunkCfg()))

	var buf bytes.Buffer
	require.NoError(t, StreamSeedPackage(ctx, store, blockHash, &buf))
	require.Equal(t, body, buf.Bytes())
}

func TestStreamSeedPackageDetectsCorruption(t *testing.T) {
	ctx := context.Background()
	store := memory.New()

	body := pseudoBytes(8000, 22)
	blockHash := chainhash.HashH([]byte("stream-block-2"))

	require.NoError(t, BuildSeedPackage(ctx, store, bytes.NewReader(body), 1, blockHash, [32]byte{}, testChunkCfg()))

	m, err := readManifest(ctx, store, blockHash)
	require.NoError(t, err)
	require.NoError(t, overwriteChunk(ctx, store, m.Chunks[0].Hash, make([]byte, int(m.Chunks[0].Size))))

	var buf bytes.Buffer
	require.Error(t, StreamSeedPackage(ctx, store, blockHash, &buf))
}
