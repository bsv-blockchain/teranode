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
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/blockassembly"
	"github.com/bsv-blockchain/teranode/services/utxopersister"
	"github.com/bsv-blockchain/teranode/settings"
	bloboptions "github.com/bsv-blockchain/teranode/stores/blob/options"
	"github.com/bsv-blockchain/teranode/stores/blockchain"
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

func piecePath(dir string, i int) string {
	return filepath.Join(dir, fmt.Sprintf("piece_%04d.zst", i))
}

// Every input form, in one pass or two and with either decoder mode, must land every
// output intact and unspent in a real UTXO store, multi-record txs included, and pass
// its checksum. Pieces are cut by bytes, so records straddle pieces.
func TestImportUTXOSet_CompressedInputs(t *testing.T) {
	const n = 600

	wrappers := withDistinctOutputs(benchWrappers("zst", n, 50))
	plain := writeCompleteSnapshotFile(t, wrappers)
	plainData, err := os.ReadFile(plain)
	require.NoError(t, err)

	plainSum := sha256.Sum256(plainData)

	zstFile, zstSum := compressedSnapshot(t, plain, 0)
	pieceDir, pieceSum := compressedSnapshot(t, plain, 1021)

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

					ctx := context.Background()
					store := newTestUtxoStore(ctx, t)
					require.NoError(t, importUTXOSet(ctx, ulogger.TestLogger{}, store, in.path, opts))
					requireStoredOutputs(ctx, t, store, wrappers)
				})
			}
		}
	}
}

// Any failure of a compressed import says to wipe the UTXO store: records already stored
// may come from a damaged piece, and a re-run keeps a record that already exists. A plain
// set, verified up front, says so only when reading the set itself failed.
func TestImportUTXOSet_FailuresSayWhetherToWipe(t *testing.T) {
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()

	cases := []struct {
		name       string
		plain      bool
		truncate   bool // cut the plain set short of its footer
		ctx        context.Context
		store      utxo.Store
		largeEvery int // failingMultiRecordStore fails only on a multi-record tx
		staleSum   bool
		want       string
		wipe       bool
	}{
		{"compressed/checksum mismatch", false, false, context.Background(), noopCreateStore{}, 0, true, "checksum mismatch", true},
		{"compressed/cancelled", false, false, cancelled, noopCreateStore{}, 0, false, "cancel", true},
		{"compressed/store error", false, false, context.Background(), failingMultiRecordStore{}, 50, false, "error in worker", true},
		{"plain/read failed part-way", true, true, context.Background(), noopCreateStore{}, 0, false, "failed part-way", true},
		{"plain/store error", true, false, context.Background(), failingMultiRecordStore{}, 50, false, "error in worker", false},
	}

	for _, tc := range cases {
		plain := writeCompleteSnapshotFile(t, benchWrappers("failures", 500, tc.largeEvery))
		path, sum := plain, ""

		if tc.truncate {
			data, err := os.ReadFile(plain)
			require.NoError(t, err)
			require.NoError(t, os.WriteFile(plain, data[:len(data)-100], 0o600))
		}

		if !tc.plain {
			path, sum = compressedSnapshot(t, plain, 1000)
		}

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

				if tc.wipe {
					require.ErrorContains(t, err, "wipe the UTXO store")
				} else {
					require.NotContains(t, err.Error(), "wipe the UTXO store")
				}
			})
		}
	}
}

// passGateStore holds every create of one kind (multi-record or not) until release
// is closed, and closes otherDone once every create of the other kind has arrived.
type passGateStore struct {
	utxo.Store
	gateMulti bool
	release   chan struct{}
	otherLeft atomic.Int64
	otherDone chan struct{}
}

func (s *passGateStore) SpendAndCreate(ctx context.Context, tx *bt.Tx, _ uint32, _ ...utxo.CreateOption) (*meta.Data, []*utxo.Spend, error) {
	if (len(tx.Outputs) > 128) == s.gateMulti {
		select {
		case <-s.release:
		case <-ctx.Done():
			return nil, nil, ctx.Err()
		}

		return nil, nil, nil
	}

	if s.otherLeft.Add(-1) == 0 {
		close(s.otherDone)
	}

	return nil, nil, nil
}

// Each pass opens the pieces itself, so each must check its own bytes. One pass is held
// while the other reads the set to its end; then the last piece changes in a way that
// keeps every count, and the held pass must fail its checksum. Each pass is held once.
func TestImportUTXOSet_EveryPassChecksItsOwnBytes(t *testing.T) {
	for _, gateMulti := range []bool{false, true} {
		t.Run(fmt.Sprintf("held multi-record pass=%t", gateMulti), func(t *testing.T) {
			// The pass that runs free must read the last piece, so the last record is its kind.
			n := 301 // index 300 is a 200-output tx
			if gateMulti {
				n = 300
			}

			wrappers := withDistinctOutputs(benchWrappers("per-pass", n, 50))
			data, err := os.ReadFile(writeCompleteSnapshotFile(t, wrappers))
			require.NoError(t, err)

			sum := sha256.Sum256(data)

			enc, err := zstd.NewWriter(nil)
			require.NoError(t, err)

			defer func() { _ = enc.Close() }()

			// 1000-byte pieces, then the last 500 bytes as the last piece.
			dir := filepath.Join(t.TempDir(), compressedTestHash+".utxo-set.zst")
			require.NoError(t, os.Mkdir(dir, 0o700))

			tailAt := len(data) - 500
			i := 0

			for off := 0; off < tailAt; off, i = off+1000, i+1 {
				require.NoError(t, os.WriteFile(piecePath(dir, i), enc.EncodeAll(data[off:min(off+1000, tailAt)], nil), 0o600))
			}

			last := piecePath(dir, i)
			require.NoError(t, os.WriteFile(last, enc.EncodeAll(data[tailAt:], nil), 0o600))

			// Change one byte inside the last output script: every length and count stays.
			changed := bytes.Clone(data[tailAt:])
			at := bytes.LastIndex(changed, []byte{0x76, 0xa9, 0x14})
			require.Positive(t, at)
			changed[at+3] ^= 0xff

			store := &passGateStore{gateMulti: gateMulti, release: make(chan struct{}), otherDone: make(chan struct{})}

			for _, w := range wrappers {
				if (len(w.UTXOs) > 128) != gateMulti {
					store.otherLeft.Add(1)
				}
			}

			opts := testImportOptions()
			opts.workerCount, opts.multiRecordWorkerCount, opts.channelSize = 1, 1, 1
			opts.expectedSHA256 = hex.EncodeToString(sum[:])

			done := make(chan error, 1)

			go func() {
				done <- importUTXOSet(context.Background(), ulogger.TestLogger{}, store, dir, opts)
			}()

			select {
			case <-store.otherDone:
			case err := <-done:
				t.Fatalf("import ended before the free pass finished: %v", err)
			case <-time.After(30 * time.Second):
				t.Fatal("the free pass did not finish")
			}

			// The free pass has the last piece open already; the held pass opens it later.
			tmp := last + ".tmp"
			require.NoError(t, os.WriteFile(tmp, enc.EncodeAll(changed, nil), 0o600))
			require.NoError(t, os.Rename(tmp, last))
			close(store.release)

			select {
			case err := <-done:
				require.ErrorContains(t, err, "checksum mismatch")
			case <-time.After(30 * time.Second):
				t.Fatal("the held pass did not finish")
			}
		})
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

// A set whose existence cannot be checked is an error, not an absence: otherwise an
// unreadable plain set would silently give way to the compressed one.
func TestFindUTXOSet_UncheckablePathIsAnError(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root ignores the directory permissions this test relies on")
	}

	dir := t.TempDir()
	locked := filepath.Join(dir, "locked")
	require.NoError(t, os.Mkdir(locked, 0o700))
	require.NoError(t, os.Symlink(filepath.Join(locked, "set"), filepath.Join(dir, compressedTestHash+".utxo-set")))
	require.NoError(t, os.WriteFile(filepath.Join(dir, compressedTestHash+".utxo-set.zst"), nil, 0o600))
	require.NoError(t, os.Chmod(locked, 0))
	t.Cleanup(func() { _ = os.Chmod(locked, 0o700) })

	_, err := findUTXOSet(dir, compressedTestHash)
	require.ErrorContains(t, err, "cannot check")
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

	requireStoredOutputs(ctx, t, openNamedUtxoStore(ctx, t, tSettings, "compressed_rerun"), wrappers)

	stale := hex.EncodeToString(make([]byte, sha256.Size))

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

// openNamedUtxoStore opens the named sqlite UTXO store (see pointUtxoStoreAt).
func openNamedUtxoStore(ctx context.Context, t *testing.T, tSettings *settings.Settings, name string) utxo.Store {
	t.Helper()

	u, err := url.Parse("sqlite:///" + name)
	require.NoError(t, err)

	store, err := utxosql.New(ctx, ulogger.TestLogger{}, tSettings, u)
	require.NoError(t, err)

	return store
}

// requireStoredOutputs checks every wrapper's outputs in store, each with its value
// and script, and unspent.
func requireStoredOutputs(ctx context.Context, t *testing.T, store utxo.Store, wrappers []*utxopersister.UTXOWrapper) {
	t.Helper()

	for _, w := range wrappers {
		got, err := store.Get(ctx, &w.TxID)
		require.NoError(t, err)
		require.NotNil(t, got.Tx)

		for _, want := range w.UTXOs {
			out := got.Tx.Outputs[want.Index]
			require.NotNil(t, out, "tx %s output %d", w.TxID, want.Index)
			require.Equal(t, want.Value, out.Satoshis, "tx %s output %d", w.TxID, want.Index)
			require.Equal(t, want.Script, []byte(*out.LockingScript), "tx %s output %d", w.TxID, want.Index)

			spend, err := store.GetSpend(ctx, &utxo.Spend{TxID: &w.TxID, Vout: want.Index})
			require.NoError(t, err)
			require.Equal(t, int(utxo.Status_OK), spend.Status, "tx %s output %d must be unspent", w.TxID, want.Index)
			require.Nil(t, spend.SpendingData, "tx %s output %d", w.TxID, want.Index)
		}
	}
}

// Each pass checks that the set still has the tip the seed will record, so a set
// replaced after the tip was read fails rather than seeding one tip under another.
func TestImportUTXOSet_TipChangedUnderTheSeed(t *testing.T) {
	path, sum := compressedSnapshot(t, writeCompleteSnapshotFile(t, benchWrappers("tip-changed", 50, 0)), 1000)

	for _, batch := range []int{0, 128} {
		opts := testImportOptions()
		opts.utxoBatchSize = batch
		opts.expectedSHA256 = sum
		opts.tip = &utxoSetTip{height: 200} // the set's real tip hash starts with 0xcc

		err := importUTXOSet(context.Background(), ulogger.TestLogger{}, noopCreateStore{}, path, opts)
		require.ErrorContains(t, err, "changed while the seed was reading it")

		opts.tip.hash[0] = 0xcc
		require.NoError(t, importUTXOSet(context.Background(), ulogger.TestLogger{}, noopCreateStore{}, path, opts))
	}
}

// cancelAfterCtx reports cancellation from its n-th Err call on, so a test can
// cancel a loop part-way through without timing.
type cancelAfterCtx struct {
	context.Context
	calls, n int
}

func (c *cancelAfterCtx) Err() error {
	c.calls++
	if c.calls >= c.n {
		return context.Canceled
	}

	return nil
}

// Verifying a set without importing it stops when the seed is cancelled part-way
// through the read, not only before it starts.
func TestVerifyUTXOSetFile_StopsOnCancel(t *testing.T) {
	path, sum := compressedSnapshot(t, writeCompleteSnapshotFile(t, benchWrappers("verify-cancel", 40_000, 0)), 1<<20)

	tip, err := verifyUTXOSetFile(context.Background(), path, sum)
	require.NoError(t, err)
	require.Equal(t, uint32(200), tip.height)

	ctx := &cancelAfterCtx{Context: context.Background(), n: 3}
	_, err = verifyUTXOSetFile(ctx, path, sum)
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, 3, ctx.calls, "the read must stop at the first check that sees the cancel")
}

// A headers-only run imports the headers and never reads the UTXO set, so having
// both forms of the set in the directory is no reason to refuse.
func TestSeeder_SkipUTXOs_DoesNotLookForTheUTXOSet(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	tSettings := forceSeederSettings(t)
	chainURL, err := url.Parse("sqlite:///headers_only")
	require.NoError(t, err)

	tSettings.BlockChain.StoreURL = chainURL

	genesis, err := newTestBlockchainStore(t).GetBlockByID(ctx, 0)
	require.NoError(t, err)

	genesisRecord, err := blockToIndexBytes(t, genesis, genesis.Header.Hash())
	require.NoError(t, err)

	coinbase := makeCoinbaseTx(t, 1, 1)
	block1 := &model.Block{
		Header: &model.BlockHeader{
			Version:        1,
			Timestamp:      1700000001,
			Nonce:          1,
			HashPrevBlock:  genesis.Header.Hash(),
			HashMerkleRoot: coinbase.TxIDChainHash(),
			Bits:           genesis.Header.Bits,
		},
		CoinbaseTx:       coinbase,
		TransactionCount: 1,
		Height:           1,
	}

	block1Record, err := blockToIndexBytes(t, block1, block1.Header.Hash())
	require.NoError(t, err)

	headers, err := os.ReadFile(writeUtxoHeadersFile(t, 1, genesisRecord, block1Record))
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(dir, compressedTestHash+".utxo-headers"), headers, 0o600))

	for _, name := range []string{compressedTestHash + ".utxo-set", compressedTestHash + ".utxo-set.zst"} {
		require.NoError(t, os.WriteFile(filepath.Join(dir, name), nil, 0o600))
	}

	require.NoError(t, Seeder(ulogger.TestLogger{}, tSettings, dir, compressedTestHash, false /* skipHeaders */, true /* skipUTXOs */, false /* force */, false /* recoverCheckpoint */, false /* skipChecksum */))

	chain, err := blockchain.NewStore(ulogger.TestLogger{}, chainURL, tSettings)
	require.NoError(t, err)

	stored, err := chain.GetBlockByID(ctx, 1)
	require.NoError(t, err)
	require.Equal(t, block1.Header.Hash().String(), stored.Header.Hash().String())
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
