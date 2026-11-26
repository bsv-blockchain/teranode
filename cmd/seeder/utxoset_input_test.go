package seeder

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/blockassembly"
	"github.com/bsv-blockchain/teranode/services/utxopersister"
	bloboptions "github.com/bsv-blockchain/teranode/stores/blob/options"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	utxosql "github.com/bsv-blockchain/teranode/stores/utxo/sql"
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

	writeSidecar(t, filepath.Join(dir, compressedTestHash+".utxo-set"), data)

	sum := sha256.Sum256(data)

	return path, hex.EncodeToString(sum[:])
}

// withDistinctOutputs gives every output its own value and script, so a misread
// byte changes what the store receives.
func withDistinctOutputs(wrappers []*utxopersister.UTXOWrapper) []*utxopersister.UTXOWrapper {
	for i, w := range wrappers {
		for j, u := range w.UTXOs {
			u.Value = uint64(1000 + i*1000 + j) //nolint:gosec // small test values
			u.Script = append(bytes.Clone(u.Script), byte(i), byte(i>>8), byte(j))
		}
	}

	return wrappers
}

// outputRecordingStore records the outputs of every created tx.
type outputRecordingStore struct {
	utxo.Store
	mu      sync.Mutex
	outputs map[chainhash.Hash][]*bt.Output
	creates int
}

func (s *outputRecordingStore) SpendAndCreate(_ context.Context, tx *bt.Tx, _ uint32, opts ...utxo.CreateOption) (*meta.Data, []*utxo.Spend, error) {
	o, err := utxo.ParseCreateOptions(opts...)
	if err != nil {
		return nil, nil, err
	}

	s.mu.Lock()
	s.outputs[*o.TxID] = tx.Outputs
	s.creates++
	s.mu.Unlock()

	return nil, nil, nil
}

// requireOutputs checks that the store got every wrapper's outputs exactly once.
func requireOutputs(t *testing.T, store *outputRecordingStore, wrappers []*utxopersister.UTXOWrapper) {
	t.Helper()

	require.Equal(t, len(wrappers), store.creates)

	for _, w := range wrappers {
		outs, ok := store.outputs[w.TxID]
		require.True(t, ok, "tx %s", w.TxID)

		for _, want := range w.UTXOs {
			out := outs[want.Index]
			require.NotNil(t, out, "tx %s output %d", w.TxID, want.Index)
			require.Equal(t, want.Value, out.Satoshis, "tx %s output %d", w.TxID, want.Index)
			require.Equal(t, want.Script, []byte(*out.LockingScript), "tx %s output %d", w.TxID, want.Index)
		}
	}
}

func piecePath(dir string, i int) string {
	return filepath.Join(dir, fmt.Sprintf("piece_%04d.zst", i))
}

// Every input form, in one pass or two and with either decoder mode, must hand the store
// every record once with its outputs intact, multi-record txs included, and pass its
// checksum. Pieces are cut by bytes, so records straddle pieces.
func TestImportUTXOSet_CompressedInputs(t *testing.T) {
	const n = 3000

	wrappers := withDistinctOutputs(benchWrappers("zst", n, 50))
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

					store := &outputRecordingStore{outputs: make(map[chainhash.Hash][]*bt.Output, n)}
					require.NoError(t, importUTXOSet(context.Background(), ulogger.TestLogger{}, store, in.path, opts))
					requireOutputs(t, store, wrappers)
				})
			}
		}
	}
}

// Any failure of a compressed import says to wipe the UTXO store: records already stored
// may come from a damaged piece, and a re-run keeps a record that already exists.
func TestImportUTXOSet_CompressedFailures_SayToWipe(t *testing.T) {
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()

	cases := []struct {
		name       string
		ctx        context.Context
		store      utxo.Store
		largeEvery int // failingMultiRecordStore fails only on a multi-record tx
		staleSum   bool
		want       string
	}{
		{"checksum mismatch", context.Background(), noopCreateStore{}, 0, true, "checksum mismatch"},
		{"cancelled", cancelled, noopCreateStore{}, 0, false, "cancel"},
		{"store error", context.Background(), failingMultiRecordStore{}, 50, false, "error in worker"},
	}

	for _, tc := range cases {
		path, sum := compressedSnapshot(t, writeCompleteSnapshotFile(t, benchWrappers("failures", 500, tc.largeEvery)), 1000)
		if tc.staleSum {
			sum = hex.EncodeToString(make([]byte, sha256.Size))
		}

		for _, batch := range []int{0, 128} {
			t.Run(fmt.Sprintf("%s/batch=%d", tc.name, batch), func(t *testing.T) {
				opts := testImportOptions()
				opts.utxoBatchSize = batch
				opts.expectedSHA256 = sum

				err := importUTXOSet(tc.ctx, ulogger.TestLogger{}, tc.store, path, opts)
				require.ErrorContains(t, err, tc.want)
				require.ErrorContains(t, err, "wipe the UTXO store")
			})
		}
	}
}

// With no sidecar, these damaged or incomplete piece directories still fail the import:
// the zstd decoder or the footer counts catch each of them.
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
			require.ErrorContains(t, err, "wipe the UTXO store")

			if tc.wantErr != "" {
				require.ErrorContains(t, err, tc.wantErr)
			}

			// A compressed set has no file offset to seek a footer at.
			require.NotContains(t, err.Error(), "seeking")
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

// Close must stop the stream: a decoder left open would go on opening and
// decompressing the remaining pieces for whoever still holds the reader.
func TestUTXOSetStream_CloseStopsReading(t *testing.T) {
	plain := writeCompleteSnapshotFile(t, benchWrappers("close", 3000, 0))
	zstFile, _ := compressedSnapshot(t, plain, 0)
	pieceDir, _ := compressedSnapshot(t, plain, 16<<10)

	for _, path := range []string{plain, zstFile, pieceDir} {
		s, err := openUTXOSetStream(path, false)
		require.NoError(t, err)

		_, err = s.reader.Peek(1)
		require.NoError(t, err)
		require.NoError(t, s.Close())

		_, err = io.Copy(io.Discard, s.reader)
		require.Error(t, err, path)
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

// A seed stopped part-way (here by a missing last piece) is not marked complete; a re-run on
// the whole set stores every output intact. Runs that do not import still check the checksum.
func TestProcessUTXOs_CompressedInput_InterruptedSeedReRuns(t *testing.T) {
	ctx := context.Background()
	tSettings := forceSeederSettings(t)
	pointUtxoStoreAt(t, tSettings, "compressed_rerun")
	blockStore := openTestBlockStore(t, tSettings)
	blockchainStore := newTestBlockchainStore(t)

	wrappers := withDistinctOutputs(benchWrappers("rerun", 300, 0))

	dir, sum := compressedSnapshot(t, writeCompleteSnapshotFile(t, wrappers), 2048)

	pieces, err := utxoSetPieces(dir)
	require.NoError(t, err)

	last := pieces[len(pieces)-1]
	lastBytes, err := os.ReadFile(last)
	require.NoError(t, err)
	require.NoError(t, os.Remove(last))

	_, err = processUTXOs(ctx, ulogger.TestLogger{}, tSettings, blockchainStore, dir, "", false, false, sum)
	require.ErrorContains(t, err, "truncated")
	require.ErrorContains(t, err, "wipe the UTXO store")

	exists, err := blockStore.Exists(ctx, nil, fileformat.FileTypeDat, bloboptions.WithFilename("lastProcessed"), bloboptions.WithNoHashPrefix())
	require.NoError(t, err)
	require.False(t, exists, "an interrupted seed must not be marked complete")

	require.NoError(t, os.WriteFile(last, lastBytes, 0o600))

	tip, err := processUTXOs(ctx, ulogger.TestLogger{}, tSettings, blockchainStore, dir, "", false, false, sum)
	require.NoError(t, err)
	require.Equal(t, uint32(200), tip.height)
	require.Equal(t, "200", readMarker(t, ctx, blockStore))

	u, err := url.Parse("sqlite:///compressed_rerun")
	require.NoError(t, err)

	store, err := utxosql.New(ctx, ulogger.TestLogger{}, tSettings, u)
	require.NoError(t, err)

	for _, w := range wrappers {
		got, err := store.Get(ctx, &w.TxID)
		require.NoError(t, err)
		require.NotNil(t, got.Tx)

		for _, want := range w.UTXOs {
			out := got.Tx.Outputs[want.Index]
			require.NotNil(t, out, "tx %s output %d", w.TxID, want.Index)
			require.Equal(t, want.Value, out.Satoshis, "tx %s output %d", w.TxID, want.Index)
			require.Equal(t, want.Script, []byte(*out.LockingScript), "tx %s output %d", w.TxID, want.Index)
		}
	}

	stale := hex.EncodeToString(make([]byte, sha256.Size))

	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	require.ErrorIs(t, verifyUTXOSetFile(cancelled, dir, sum), context.Canceled)

	// -recover-checkpoint takes the tip from the set, so it checks the set first.
	_, err = processUTXOs(ctx, ulogger.TestLogger{}, tSettings, blockchainStore, dir, "", false, true, stale)
	require.ErrorContains(t, err, "checksum mismatch")

	tip, err = processUTXOs(ctx, ulogger.TestLogger{}, tSettings, blockchainStore, dir, "", false, true, sum)
	require.NoError(t, err)
	require.Equal(t, uint32(200), tip.height)

	// With the checkpoint written, a plain re-run skips the import but still checks the set.
	require.NoError(t, blockchainStore.SetState(ctx, blockassembly.StateKey, []byte("checkpoint")))

	_, err = processUTXOs(ctx, ulogger.TestLogger{}, tSettings, blockchainStore, dir, "", false, false, stale)
	require.ErrorContains(t, err, "checksum mismatch")

	tip, err = processUTXOs(ctx, ulogger.TestLogger{}, tSettings, blockchainStore, dir, "", false, false, sum)
	require.NoError(t, err)
	require.Nil(t, tip, "a skipped import writes no checkpoint")
}

// A headers-only run imports the headers and never reads the UTXO set, so having
// both forms of the set in the directory is no reason to refuse.
func TestSeeder_SkipUTXOs_DoesNotLookForTheUTXOSet(t *testing.T) {
	dir := t.TempDir()

	genesis, err := newTestBlockchainStore(t).GetBlockByID(context.Background(), 0)
	require.NoError(t, err)

	genesisRecord, err := blockToIndexBytes(t, genesis, genesis.Header.Hash())
	require.NoError(t, err)

	headers, err := os.ReadFile(writeUtxoHeadersFile(t, 0, genesisRecord))
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(dir, compressedTestHash+".utxo-headers"), headers, 0o600))

	for _, name := range []string{compressedTestHash + ".utxo-set", compressedTestHash + ".utxo-set.zst"} {
		require.NoError(t, os.WriteFile(filepath.Join(dir, name), nil, 0o600))
	}

	require.NoError(t, Seeder(ulogger.TestLogger{}, forceSeederSettings(t), dir, compressedTestHash, false /* skipHeaders */, true /* skipUTXOs */, false /* force */, false /* recoverCheckpoint */, false /* skipChecksum */))
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
			plainPath := filepath.Join(filepath.Dir(dir), compressedTestHash+".utxo-set")
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
