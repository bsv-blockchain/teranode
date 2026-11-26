package seeder

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	bloboptions "github.com/bsv-blockchain/teranode/stores/blob/options"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/require"
)

const compressedTestHash = "abc123"

// compressedSnapshot writes the bytes of the plain snapshot as
// <hash>.utxo-set.zst in a new directory: one zstd file when pieceSize is 0,
// otherwise a directory of byte-range pieces compressed one by one, the way
// seed-fetch.sh writes them. A sidecar over the uncompressed bytes sits beside
// it. It returns the compressed path and the uncompressed set's SHA-256.
func compressedSnapshot(t *testing.T, plain string, pieceSize int) (string, string) {
	t.Helper()

	data, err := os.ReadFile(plain)
	require.NoError(t, err)

	enc, err := zstd.NewWriter(nil)
	require.NoError(t, err)

	defer func() { _ = enc.Close() }()

	dir := t.TempDir()
	path := filepath.Join(dir, compressedTestHash+".utxo-set"+compressedUTXOSetExtension)

	if pieceSize == 0 {
		require.NoError(t, os.WriteFile(path, enc.EncodeAll(data, nil), 0o600))
	} else {
		require.NoError(t, os.Mkdir(path, 0o700))

		for i, off := 0, 0; off < len(data); i, off = i+1, off+pieceSize {
			piece := enc.EncodeAll(data[off:min(off+pieceSize, len(data))], nil)
			require.NoError(t, os.WriteFile(filepath.Join(path, fmt.Sprintf("piece_%04d.zst", i)), piece, 0o600))
		}
	}

	writeSidecar(t, uncompressedUTXOSetPath(path), data)

	sum := sha256.Sum256(data)

	return path, hex.EncodeToString(sum[:])
}

func piecePath(dir string, i int) string {
	return filepath.Join(dir, fmt.Sprintf("piece_%04d.zst", i))
}

// Every input form, read in one pass or two and with the zstd decoder
// synchronous or asynchronous, must create every record exactly once and
// pass its checksum. Pieces are cut by bytes, so records straddle pieces.
func TestImportUTXOSet_CompressedInputs(t *testing.T) {
	const n = 3000

	wrappers := benchWrappers("zst", n, 50)
	plain := writeCompleteSnapshotFile(t, wrappers)
	plainData, err := os.ReadFile(plain)
	require.NoError(t, err)

	plainSum := sha256.Sum256(plainData)

	zstFile, zstSum := compressedSnapshot(t, plain, 0)
	pieceDir, pieceSum := compressedSnapshot(t, plain, 4093)

	inputs := []struct {
		name, path, sum string
	}{
		{"plain file", plain, hex.EncodeToString(plainSum[:])},
		{"zst file", zstFile, zstSum},
		{"piece directory", pieceDir, pieceSum},
	}

	defer func(c int) { zstdDecoderConcurrency = c }(zstdDecoderConcurrency)

	for _, in := range inputs {
		for _, batch := range []int{0, 128} {
			for _, concurrency := range []int{1, 4} {
				t.Run(fmt.Sprintf("%s/batch=%d/decoders=%d", in.name, batch, concurrency), func(t *testing.T) {
					zstdDecoderConcurrency = concurrency

					opts := testImportOptions()
					opts.utxoBatchSize = batch
					opts.expectedSHA256 = in.sum

					store := &txidRecordingStore{seen: make(map[chainhash.Hash]int, n)}
					require.NoError(t, importUTXOSet(context.Background(), ulogger.TestLogger{}, store, in.path, opts))

					require.Len(t, store.seen, n)

					for _, w := range wrappers {
						require.Equal(t, 1, store.seen[w.TxID], "tx %s", w.TxID)
					}
				})
			}
		}
	}
}

// A set whose bytes do not hash to its sidecar fails once every record has
// been read, and says the UTXO store must be wiped, because the records it
// already holds came from that set and a re-run keeps them.
func TestImportUTXOSet_CompressedChecksumMismatch(t *testing.T) {
	const n = 500

	path, _ := compressedSnapshot(t, writeCompleteSnapshotFile(t, benchWrappers("mismatch", n, 0)), 1000)

	for _, batch := range []int{0, 128} {
		opts := testImportOptions()
		opts.utxoBatchSize = batch
		opts.expectedSHA256 = hex.EncodeToString(make([]byte, sha256.Size))

		err := importUTXOSet(context.Background(), ulogger.TestLogger{}, noopCreateStore{}, path, opts)
		require.ErrorContains(t, err, "checksum mismatch")
		require.ErrorContains(t, err, "wipe the UTXO store")
	}
}

// A damaged or incomplete piece directory must fail the import even with no
// sidecar to check against: the zstd decoder or the footer counts catch it.
func TestImportUTXOSet_DamagedPieces(t *testing.T) {
	plain := writeCompleteSnapshotFile(t, benchWrappers("damaged", 2000, 0))

	cases := []struct {
		name    string
		damage  func(t *testing.T, dir string)
		wantErr string
	}{
		{"piece cut mid-frame", func(t *testing.T, dir string) {
			b, err := os.ReadFile(piecePath(dir, 3))
			require.NoError(t, err)
			require.NoError(t, os.WriteFile(piecePath(dir, 3), b[:len(b)/2], 0o600))
		}, ""},
		{"last piece missing", func(t *testing.T, dir string) {
			entries, err := os.ReadDir(dir)
			require.NoError(t, err)
			require.NoError(t, os.Remove(piecePath(dir, len(entries)-1)))
		}, "truncated"},
		{"middle piece missing", func(t *testing.T, dir string) {
			require.NoError(t, os.Remove(piecePath(dir, 3)))
		}, ""},
		{"piece corrupted", func(t *testing.T, dir string) {
			b, err := os.ReadFile(piecePath(dir, 3))
			require.NoError(t, err)
			b[len(b)/2] ^= 0xff
			require.NoError(t, os.WriteFile(piecePath(dir, 3), b, 0o600))
		}, ""},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir, _ := compressedSnapshot(t, plain, 8192)
			tc.damage(t, dir)

			opts := testImportOptions()
			opts.utxoBatchSize = 0

			err := importUTXOSet(context.Background(), ulogger.TestLogger{}, noopCreateStore{}, dir, opts)
			require.Error(t, err)

			if tc.wantErr != "" {
				require.ErrorContains(t, err, tc.wantErr)
			}
		})
	}
}

// Reading the tip must decompress only the start of the set: pieces after
// the first are garbage here and must never be reached.
func TestReadUTXOSetTip_CompressedReadsOnlyTheStart(t *testing.T) {
	plain := writeCompleteSnapshotFile(t, benchWrappers("tip", 3000, 0))

	for _, pieceSize := range []int{0, 64 << 10} {
		path, _ := compressedSnapshot(t, plain, pieceSize)

		if pieceSize > 0 {
			pieces, err := utxoSetPieces(path)
			require.NoError(t, err)
			require.Greater(t, len(pieces), 1)

			for _, p := range pieces[1:] {
				require.NoError(t, os.WriteFile(p, []byte("not zstd"), 0o600))
			}
		}

		tip, err := readUTXOSetTip(path)
		require.NoError(t, err)
		require.Equal(t, uint32(200), tip.height)
		require.Equal(t, byte(0xcc), tip.hash[0])
	}
}

func TestFindUTXOSet(t *testing.T) {
	plainName := compressedTestHash + ".utxo-set"
	zstName := plainName + compressedUTXOSetExtension

	cases := []struct {
		name    string
		files   []string
		dirs    []string
		want    string
		wantErr string
	}{
		{"plain only", []string{plainName}, nil, plainName, ""},
		{"zst file only", []string{zstName}, nil, zstName, ""},
		{"piece directory only", nil, []string{zstName}, zstName, ""},
		{"neither names the plain file as missing", nil, nil, plainName, ""},
		{"both are refused", []string{plainName, zstName}, nil, "", "remove one"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()

			for _, f := range tc.files {
				require.NoError(t, os.WriteFile(filepath.Join(dir, f), nil, 0o600))
			}

			for _, d := range tc.dirs {
				require.NoError(t, os.Mkdir(filepath.Join(dir, d), 0o700))
			}

			got, err := findUTXOSet(dir, compressedTestHash)
			if tc.wantErr != "" {
				require.ErrorContains(t, err, tc.wantErr)
				return
			}

			require.NoError(t, err)
			require.Equal(t, filepath.Join(dir, tc.want), got)
		})
	}
}

// Only *.zst files are pieces, taken in name order; a fetch's unfinished
// .part files and anything else in the directory are not.
func TestUTXOSetPieces(t *testing.T) {
	dir := filepath.Join(t.TempDir(), compressedTestHash+".utxo-set.zst")
	require.NoError(t, os.Mkdir(dir, 0o700))

	_, err := utxoSetPieces(dir)
	require.ErrorContains(t, err, "holds no")

	for _, f := range []string{"piece_0002.zst", "piece_0000.zst", "piece_0003.zst.part", "fetch.log", "piece_0001.zst"} {
		require.NoError(t, os.WriteFile(filepath.Join(dir, f), nil, 0o600))
	}

	require.NoError(t, os.Mkdir(filepath.Join(dir, "sub.zst"), 0o700))

	pieces, err := utxoSetPieces(dir)
	require.NoError(t, err)
	require.Equal(t, []string{piecePath(dir, 0), piecePath(dir, 1), piecePath(dir, 2)}, pieces)

	_, err = utxoSetPieces(filepath.Join(dir, "missing.zst"))
	require.Error(t, err)
}

// A seed that stops part-way through a compressed set (here the download was
// incomplete) leaves no completion marker, and a plain re-run once the set is
// whole imports it. -recover-checkpoint then reads the tip from the
// compressed set too.
func TestProcessUTXOs_CompressedInput_InterruptedSeedReRuns(t *testing.T) {
	ctx := context.Background()
	tSettings := forceSeederSettings(t)
	pointUtxoStoreAt(t, tSettings, "compressed_rerun")
	blockStore := openTestBlockStore(t, tSettings)
	blockchainStore := newTestBlockchainStore(t)

	wrappers := benchWrappers("rerun", 300, 0)
	dir, sum := compressedSnapshot(t, writeCompleteSnapshotFile(t, wrappers), 2048)

	pieces, err := utxoSetPieces(dir)
	require.NoError(t, err)

	last := pieces[len(pieces)-1]
	lastBytes, err := os.ReadFile(last)
	require.NoError(t, err)
	require.NoError(t, os.Remove(last))

	_, err = processUTXOs(ctx, ulogger.TestLogger{}, tSettings, blockchainStore, dir, "", false, false, sum)
	require.ErrorContains(t, err, "truncated")

	exists, err := blockStore.Exists(ctx, nil, fileformat.FileTypeDat, bloboptions.WithFilename("lastProcessed"), bloboptions.WithNoHashPrefix())
	require.NoError(t, err)
	require.False(t, exists, "an interrupted seed must not be marked complete")

	require.NoError(t, os.WriteFile(last, lastBytes, 0o600))

	tip, err := processUTXOs(ctx, ulogger.TestLogger{}, tSettings, blockchainStore, dir, "", false, false, sum)
	require.NoError(t, err)
	require.Equal(t, uint32(200), tip.height)
	require.Equal(t, "200", readMarker(t, ctx, blockStore))

	for _, w := range []int{0, len(wrappers) - 1} {
		require.True(t, utxoStoreHasTx(ctx, t, tSettings, "compressed_rerun", &wrappers[w].TxID))
	}

	tip, err = processUTXOs(ctx, ulogger.TestLogger{}, tSettings, blockchainStore, dir, "", false, true, "")
	require.NoError(t, err)
	require.Equal(t, uint32(200), tip.height)
}

// Through Seeder: a compressed set's sidecar is checked in the import pass, so
// a stale one fails the seed without marking it complete, and a malformed one
// fails before the import starts.
func TestSeeder_CompressedInput_Checksum(t *testing.T) {
	cases := []struct {
		name    string
		sidecar func(plainPath string) string
		wantErr []string
	}{
		{"stale sidecar", func(p string) string {
			sum := sha256.Sum256([]byte("another snapshot"))
			return hex.EncodeToString(sum[:]) + "  " + filepath.Base(p) + "\n"
		}, []string{"checksum mismatch", "wipe the UTXO store"}},
		{"malformed sidecar", func(string) string { return "not-a-digest\n" }, []string{"malformed"}},
		{"sidecar for another file", func(string) string {
			return hex.EncodeToString(make([]byte, sha256.Size)) + "  other.utxo-set\n"
		}, []string{"belongs to"}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tSettings := forceSeederSettings(t)
			blockStore := openTestBlockStore(t, tSettings)

			dir, _ := compressedSnapshot(t, writeCompleteSnapshotFile(t, benchWrappers("seeder", 50, 0)), 1024)
			plainPath := uncompressedUTXOSetPath(dir)
			require.NoError(t, os.WriteFile(plainPath+checksumSidecarExtension, []byte(tc.sidecar(plainPath)), 0o600))

			err := Seeder(ulogger.TestLogger{}, tSettings, filepath.Dir(dir), compressedTestHash, true /* skipHeaders */, false /* skipUTXOs */, false /* force */, false /* recoverCheckpoint */, false /* skipChecksum */)
			for _, want := range tc.wantErr {
				require.ErrorContains(t, err, want)
			}

			exists, err := blockStore.Exists(context.Background(), nil, fileformat.FileTypeDat, bloboptions.WithFilename("lastProcessed"), bloboptions.WithNoHashPrefix())
			require.NoError(t, err)
			require.False(t, exists)
		})
	}
}
