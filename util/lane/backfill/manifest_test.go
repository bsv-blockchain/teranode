package backfill

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/util/lane"
	"github.com/stretchr/testify/require"
)

func txids(seed byte, n int) []chainhash.Hash {
	out := make([]chainhash.Hash, n)
	for i := range out {
		out[i] = chainhash.Hash{seed, byte(i), byte(i >> 8)}
	}

	return out
}

func testHeader() Header {
	return Header{Segment: 1, First: 1_000_001, SourceTipBlockID: 900_000, SourceTipHash: chainhash.Hash{0xEE}}
}

func writeSample(t *testing.T, path string) lane.ConversionSegment {
	t.Helper()

	m, err := CreateManifest(path, testHeader())
	require.NoError(t, err)

	defer func() { require.NoError(t, m.Close()) }()

	b0, err := m.AppendBlock(700_000, 700_000, txids(1, 3))
	require.NoError(t, err)
	require.Equal(t, lane.ConversionBlock{Seq: 0, BlockID: 700_000, Height: 700_000, First: 1_000_001, Count: 3}, b0)

	b1, err := m.AppendBlock(700_001, 700_001, txids(2, 2))
	require.NoError(t, err)
	require.Equal(t, uint64(1_000_004), b1.First)

	first, err := m.AppendSuffix(txids(3, 1))
	require.NoError(t, err)
	require.Equal(t, uint64(1_000_006), first)

	seg, err := m.Finish()
	require.NoError(t, err)

	return seg
}

func TestManifestWriteAndReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "segment-1.manifest")
	seg := writeSample(t, path)

	require.Equal(t, uint32(1), seg.ID)
	require.Equal(t, uint64(1_000_001), seg.First)
	require.Equal(t, uint64(1_000_005), seg.PrefixLast)
	require.Equal(t, uint64(1_000_006), seg.Last)
	require.NotEqual(t, chainhash.Hash{}, seg.ManifestDigest)

	m, err := OpenManifest(path)
	require.NoError(t, err)

	defer func() { require.NoError(t, m.Close()) }()

	again, err := m.Segment()
	require.NoError(t, err)
	require.Equal(t, seg, again, "the digest is the same read back")
	require.Equal(t, testHeader(), m.Header())
	require.Len(t, m.Blocks(), 2)
	require.NoError(t, lane.CheckConversionBlocks(seg, m.Blocks()))

	got, err := m.BlockTxIDs(1)
	require.NoError(t, err)
	require.Equal(t, txids(2, 2), got)

	suffix, first, err := m.SuffixTxIDs()
	require.NoError(t, err)
	require.Equal(t, txids(3, 1), suffix)
	require.Equal(t, uint64(1_000_006), first)

	_, err = m.AppendBlock(1, 800_000, nil)
	require.Error(t, err, "a finished manifest is immutable")

	_, err = m.BlockTxIDs(2)
	require.Error(t, err)
}

func TestManifestCutsOnlyAPartialLastRecord(t *testing.T) {
	path := filepath.Join(t.TempDir(), "m")

	m, err := CreateManifest(path, testHeader())
	require.NoError(t, err)
	_, err = m.AppendBlock(10, 10, txids(1, 4))
	require.NoError(t, err)
	_, err = m.AppendBlock(11, 11, txids(2, 4))
	require.NoError(t, err)
	require.NoError(t, m.Close())

	full, err := os.ReadFile(path)
	require.NoError(t, err)

	// A crash part-way through the second block record.
	require.NoError(t, os.WriteFile(path, full[:len(full)-20], 0o600))

	m, err = OpenManifest(path)
	require.NoError(t, err)
	require.Len(t, m.Blocks(), 1, "the torn record is dropped")

	// Resuming writes it again in the same place.
	b, err := m.AppendBlock(11, 11, txids(2, 4))
	require.NoError(t, err)
	require.Equal(t, uint64(1_000_005), b.First)
	require.NoError(t, m.Close())

	again, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, full, again)
}

func TestManifestRefusesDamage(t *testing.T) {
	path := filepath.Join(t.TempDir(), "m")
	writeSample(t, path)

	full, err := os.ReadFile(path)
	require.NoError(t, err)

	// Flip a txid byte inside the first block record: its CRC no longer matches and more records
	// follow it, so this is damage, not a torn tail.
	damaged := append([]byte{}, full...)
	damaged[5+9+headerPayload+4+9+blockFixed+3] ^= 0xFF
	require.NoError(t, os.WriteFile(path, damaged, 0o600))

	_, err = OpenManifest(path)
	require.Error(t, err)

	require.NoError(t, os.WriteFile(path, append([]byte("XXXX"), full[4:]...), 0o600))
	_, err = OpenManifest(path)
	require.Error(t, err, "wrong magic")

	_, err = CreateManifest(filepath.Join(t.TempDir(), "x"), Header{Segment: 1, First: lane.BackfillSlotSentinel})
	require.Error(t, err, "position 1 is reserved")

	existing := filepath.Join(t.TempDir(), "exists")
	require.NoError(t, os.WriteFile(existing, nil, 0o600))
	_, err = CreateManifest(existing, testHeader())
	require.Error(t, err, "never overwrites a manifest")
}

func TestManifestRefusesFallingHeight(t *testing.T) {
	m, err := CreateManifest(filepath.Join(t.TempDir(), "m"), testHeader())
	require.NoError(t, err)

	defer func() { require.NoError(t, m.Close()) }()

	_, err = m.AppendBlock(5, 100, txids(1, 1))
	require.NoError(t, err)
	_, err = m.AppendBlock(6, 100, txids(2, 1))
	require.Error(t, err)

	_, err = m.Segment()
	require.Error(t, err, "not finished")
}
