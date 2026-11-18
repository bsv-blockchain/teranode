package backfill

import (
	"encoding/binary"
	"hash/crc32"
	"os"
	"path/filepath"
	"sync"
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

	_, err = OpenManifest(path)
	require.Error(t, err, "reading refuses a partial record and cuts nothing")

	torn, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Len(t, torn, len(full)-20)

	m, err = ResumeManifest(path)
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
	damaged[5+frameHead+headerPayload+frameTail+frameHead+blockFixed+3] ^= 0xFF
	require.NoError(t, os.WriteFile(path, damaged, 0o600))

	_, err = OpenManifest(path)
	require.Error(t, err)
	_, err = ResumeManifest(path)
	require.Error(t, err)

	// A flipped bit in the first block record's length field is damage, not a torn tail: nothing
	// after it is cut.
	lengthHit := append([]byte{}, full...)
	lengthHit[5+frameHead+headerPayload+frameTail+8] ^= 0x01
	require.NoError(t, os.WriteFile(path, lengthHit, 0o600))

	_, err = ResumeManifest(path)
	require.Error(t, err)

	after, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, lengthHit, after, "no committed record is cut")

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

// craft builds a manifest whose records all carry good CRCs, so only the loader's own checks can
// refuse it.
func craft(records ...[]byte) []byte {
	out := append([]byte(manifestMagic), manifestVersion)

	for _, r := range records {
		head := append([]byte{r[0]}, binary.LittleEndian.AppendUint64(nil, uint64(len(r)-1))...)
		head = binary.LittleEndian.AppendUint32(head, crc32.Checksum(head, crcTable))

		body := crc32.New(crcTable)
		body.Write(head[:9])
		body.Write(r[1:])

		out = append(out, head...)
		out = append(out, r[1:]...)
		out = binary.LittleEndian.AppendUint32(out, body.Sum32())
	}

	return out
}

func headerRecord() []byte {
	r := []byte{recordHeader}
	r = binary.LittleEndian.AppendUint32(r, 1)
	r = binary.LittleEndian.AppendUint64(r, 1_000_001)
	r = binary.LittleEndian.AppendUint32(r, 900_000)

	return append(r, make([]byte, 32)...)
}

func blockRecord(count uint64, txids int) []byte {
	r := []byte{recordBlock}
	r = binary.LittleEndian.AppendUint32(r, 0)
	r = binary.LittleEndian.AppendUint32(r, 7)
	r = binary.LittleEndian.AppendUint32(r, 700_000)
	r = binary.LittleEndian.AppendUint64(r, 1_000_001)
	r = binary.LittleEndian.AppendUint64(r, count)

	return append(r, make([]byte, txids*chainhash.HashSize)...)
}

func suffixRecord(first, count uint64, txids int) []byte {
	r := []byte{recordSuffix}
	r = binary.LittleEndian.AppendUint64(r, first)
	r = binary.LittleEndian.AppendUint64(r, count)

	return append(r, make([]byte, txids*chainhash.HashSize)...)
}

func TestManifestRefusesRecordsThatDisagreeWithThemselves(t *testing.T) {
	dir := t.TempDir()

	good := craft(headerRecord(), blockRecord(2, 2), suffixRecord(1_000_003, 1, 1))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "good"), good, 0o600))

	m, err := OpenManifest(filepath.Join(dir, "good"))
	require.NoError(t, err)
	require.Len(t, m.Blocks(), 1)
	require.NoError(t, m.Close())

	for name, b := range map[string][]byte{
		"count above its txids":     craft(headerRecord(), blockRecord(1<<40, 0)),
		"count below its txids":     craft(headerRecord(), blockRecord(1, 2)),
		"suffix at the wrong start": craft(headerRecord(), blockRecord(2, 2), suffixRecord(1_000_004, 1, 1)),
		"suffix count wrong":        craft(headerRecord(), blockRecord(2, 2), suffixRecord(1_000_003, 2, 1)),
		"block before header":       craft(blockRecord(2, 2)),
		"block after the end":       craft(headerRecord(), blockRecord(2, 2), endRecord(1_000_002, 1_000_002), blockRecord(0, 0)),
		"suffix after the end":      craft(headerRecord(), blockRecord(2, 2), endRecord(1_000_002, 1_000_002), suffixRecord(1_000_003, 0, 0)),
	} {
		path := filepath.Join(dir, "bad")
		require.NoError(t, os.WriteFile(path, b, 0o600))

		_, err := ResumeManifest(path)
		require.Error(t, err, name)
	}

	ended := craft(headerRecord(), blockRecord(2, 2), endRecord(1_000_002, 1_000_002))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "ended"), ended, 0o600))

	m, err = OpenManifest(filepath.Join(dir, "ended"))
	require.NoError(t, err)
	require.True(t, m.Finished(), "the same file without the record after the end loads")
	require.NoError(t, m.Close())
}

func endRecord(prefixLast, last uint64) []byte {
	r := []byte{recordEnd}
	r = binary.LittleEndian.AppendUint64(r, prefixLast)

	return binary.LittleEndian.AppendUint64(r, last)
}

// A last record whose length reaches exactly to the end of the file but whose body fails its CRC
// is what a crash leaves when the payload was only partly written to disk: ResumeManifest cuts it
// and keeps every record before it, and OpenManifest refuses it.
func TestManifestCutsALastRecordWhoseBodyFailsItsCRC(t *testing.T) {
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

	damaged := append([]byte{}, full...)
	damaged[len(damaged)-frameTail-5] ^= 0xFF // a txid byte of the last record
	require.NoError(t, os.WriteFile(path, damaged, 0o600))

	_, err = OpenManifest(path)
	require.Error(t, err)

	m, err = ResumeManifest(path)
	require.NoError(t, err)
	require.Len(t, m.Blocks(), 1)
	require.NoError(t, m.Close())

	cut, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Less(t, len(cut), len(full))
	require.Equal(t, full[:len(cut)], cut)
}

// A manifest that assigns no position is refused before its end record is written, so the file
// stays open to blocks and a resume does not find it finished.
func TestManifestFinishRefusesAnEmptySegment(t *testing.T) {
	path := filepath.Join(t.TempDir(), "m")

	m, err := CreateManifest(path, testHeader())
	require.NoError(t, err)

	_, err = m.Finish()
	require.Error(t, err)
	require.False(t, m.Finished())
	require.NoError(t, m.Close())

	m, err = ResumeManifest(path)
	require.NoError(t, err)
	require.False(t, m.Finished(), "no end record reached the disk")

	_, err = m.AppendBlock(10, 10, txids(1, 2))
	require.NoError(t, err)

	seg, err := m.Finish()
	require.NoError(t, err)
	require.Equal(t, uint64(1_000_002), seg.Last)
	require.NoError(t, m.Close())
}

// A write that fails leaves the manifest refusing every later append, even once the disk would
// take it, so a retry cannot write a second copy of a record that may already be there. Resuming
// reloads the file and appends again.
func TestManifestRefusesAppendsAfterAFailedWrite(t *testing.T) {
	path := filepath.Join(t.TempDir(), "m")

	m, err := CreateManifest(path, testHeader())
	require.NoError(t, err)
	_, err = m.AppendBlock(10, 10, txids(1, 2))
	require.NoError(t, err)

	require.NoError(t, m.f.Close())

	_, err = m.AppendBlock(11, 11, txids(2, 2))
	require.Error(t, err, "the file is closed")

	// The disk is writable again, but this Manifest no longer knows what is on it.
	m.f, err = os.OpenFile(path, os.O_RDWR, 0)
	require.NoError(t, err)

	_, err = m.AppendBlock(11, 11, txids(2, 2))
	require.Error(t, err)
	_, err = m.AppendSuffix(nil)
	require.Error(t, err)
	_, err = m.Finish()
	require.Error(t, err)
	require.NoError(t, m.Close())

	m, err = ResumeManifest(path)
	require.NoError(t, err)
	require.Len(t, m.Blocks(), 1)

	b, err := m.AppendBlock(11, 11, txids(2, 2))
	require.NoError(t, err)
	require.Equal(t, uint64(1_000_003), b.First)
	require.NoError(t, m.Close())
}

// tempFiles lists the temporary files CreateManifest could leave beside path.
func tempFiles(t *testing.T, path string) []string {
	t.Helper()

	names, err := filepath.Glob(path + ".*.tmp")
	require.NoError(t, err)

	return names
}

// Creating a manifest writes it to a temporary file of its own and links it into place: a file an
// earlier crash left at a temporary-looking name is not touched, this call leaves no temporary
// file behind, and an existing manifest is never replaced.
func TestManifestCreateIsAllOrNothing(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "m")

	// Files an earlier crash could have left at temporary names.
	stale := make([]byte, 4096)
	require.NoError(t, os.WriteFile(path+".tmp", stale, 0o600))
	require.NoError(t, os.WriteFile(path+".123.tmp", stale, 0o600))

	m, err := CreateManifest(path, testHeader())
	require.NoError(t, err)
	require.NoError(t, m.Close())

	for _, name := range []string{path + ".tmp", path + ".123.tmp"} {
		kept, err := os.ReadFile(name)
		require.NoError(t, err)
		require.Equal(t, stale, kept, "%s is left as it was", name)
	}

	require.Equal(t, []string{path + ".123.tmp"}, tempFiles(t, path), "this call left no temporary file")

	m, err = OpenManifest(path)
	require.NoError(t, err, "the manifest holds a whole header and nothing of the stale file")
	require.Equal(t, testHeader(), m.Header())
	require.NoError(t, m.Close())

	before, err := os.ReadFile(path)
	require.NoError(t, err)

	_, err = CreateManifest(path, Header{Segment: 2, First: 5_000_000})
	require.Error(t, err)

	after, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, before, after, "the existing manifest is untouched")
}

// A crash between linking a new manifest into place and removing its temporary name leaves two
// names for one file. Once that manifest is finished, creating it again must fail without
// emptying it through either name.
func TestManifestCreateLeavesAFinishedManifestAlone(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "segment-1.manifest")

	seg := writeSample(t, path)

	require.NoError(t, os.Link(path, path+".tmp"))
	require.NoError(t, os.Link(path, path+".4242.tmp"))

	before, err := os.ReadFile(path)
	require.NoError(t, err)

	_, err = CreateManifest(path, testHeader())
	require.Error(t, err)

	after, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, before, after, "the finished manifest is unchanged")

	m, err := OpenManifest(path)
	require.NoError(t, err)

	got, err := m.Segment()
	require.NoError(t, err)
	require.Equal(t, seg, got)
	require.NoError(t, m.Close())
}

// Creators racing for one path leave exactly one manifest, whole and holding the winner's header,
// and no temporary file.
func TestManifestConcurrentCreatorsLeaveOneManifest(t *testing.T) {
	path := filepath.Join(t.TempDir(), "m")

	const creators = 8

	type result struct {
		header Header
		err    error
	}

	results := make(chan result, creators)

	var wg sync.WaitGroup

	for i := 0; i < creators; i++ {
		wg.Add(1)

		go func(i int) {
			defer wg.Done()

			h := testHeader()
			h.SourceTipBlockID = uint32(1_000 + i)

			m, err := CreateManifest(path, h)
			if err == nil {
				err = m.Close()
			}

			results <- result{header: h, err: err}
		}(i)
	}

	wg.Wait()
	close(results)

	var winners []Header

	for r := range results {
		if r.err == nil {
			winners = append(winners, r.header)
		}
	}

	require.Len(t, winners, 1)

	m, err := OpenManifest(path)
	require.NoError(t, err)
	require.Equal(t, winners[0], m.Header())
	require.NoError(t, m.Close())

	require.Empty(t, tempFiles(t, path))
}

// FuzzOpenManifest loads arbitrary bytes as a manifest. Loading never panics, and a manifest that
// loads reads back every txid its records name, and loads again the same after a resume.
func FuzzOpenManifest(f *testing.F) {
	f.Add(craft(headerRecord(), blockRecord(2, 2), suffixRecord(1_000_003, 1, 1)))
	f.Add(craft(headerRecord(), blockRecord(2, 2), endRecord(1_000_002, 1_000_002)))
	f.Add(append([]byte(manifestMagic), manifestVersion))

	f.Fuzz(func(t *testing.T, data []byte) {
		path := filepath.Join(t.TempDir(), "m")
		require.NoError(t, os.WriteFile(path, data, 0o600))

		m, err := OpenManifest(path)
		if err != nil {
			return
		}

		blocks := m.Blocks()

		for _, b := range blocks {
			ids, err := m.BlockTxIDs(b.Seq)
			require.NoError(t, err)
			require.Len(t, ids, int(b.Count))
		}

		if m.HasSuffix() {
			_, _, err = m.SuffixTxIDs()
			require.NoError(t, err)
		}

		require.NoError(t, m.Close())

		resumed, err := ResumeManifest(path)
		require.NoError(t, err, "a manifest that opens also resumes")
		require.Equal(t, blocks, resumed.Blocks())
		require.NoError(t, resumed.Close())
	})
}
