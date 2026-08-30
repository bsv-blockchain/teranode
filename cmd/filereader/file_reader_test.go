package filereader

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

// buildSubtreeMetaFixture returns a 4-leaf subtree and its correctly serialized
// meta bytes, built with go-subtree's own serializer rather than a hand-rolled
// layout, so a change to the wire format breaks the fixture instead of leaving
// the test asserting against bytes the node never writes. Same construction as
// model's buildMetaFixture.
func buildSubtreeMetaFixture(t *testing.T) (*subtree.Subtree, []byte) {
	t.Helper()

	st, err := subtree.NewTreeByLeafCount(4)
	require.NoError(t, err)

	for i := byte(0); i < 4; i++ {
		require.NoError(t, st.AddNode(chainhash.HashH([]byte{i, 0xaa}), 1, 0))
	}

	meta := subtree.NewSubtreeMeta(st)

	for i := 0; i < 4; i++ {
		parent := chainhash.HashH([]byte{byte(i), 0xbb})
		require.NoError(t, meta.SetTxInpoints(i, subtree.NewTxInpointsFromPacked([]chainhash.Hash{parent}, []uint32{1, 0})))
	}

	metaBytes, err := meta.Serialize()
	require.NoError(t, err)

	return st, metaBytes
}

// writeSubtreePair lays the two files handleSubtreeMeta reads into dir and
// returns the path of the .subtreeMeta.
//
// The .subtreeMeta carries a fileformat header because readFile consumes one
// before dispatching, and parseSubtreeMetaBestEffort reads one again when it
// re-opens the file. The sibling .subtree does not, because handleSubtreeMeta
// hands that reader straight to DeserializeFromReader without reading a header
// first. That asymmetry predates this change (the sibling read dates to 2025)
// and is pinned here as behaviour, not endorsed.
func writeSubtreePair(t *testing.T, dir string, st *subtree.Subtree, metaBytes []byte) string {
	t.Helper()

	base := st.RootHash().String()

	stBytes, err := st.Serialize()
	require.NoError(t, err)

	require.NoError(t, os.WriteFile(filepath.Join(dir, base+".subtree"), stBytes, 0o600))

	metaPath := filepath.Join(dir, base+".subtreeMeta")
	withHeader := append(fileformat.NewHeader(fileformat.FileTypeSubtreeMeta).Bytes(), metaBytes...)
	require.NoError(t, os.WriteFile(metaPath, withHeader, 0o600))

	return metaPath
}

// runProcessFile runs ProcessFile with stdout captured, because the per-index
// dump this tool exists to produce goes to stdout rather than the logger, and
// "the file was rejected but the dump still ran" is the property three review
// rounds kept breaking.
func runProcessFile(t *testing.T, path string) (string, error) {
	t.Helper()

	origVerbose, origUseStore := verbose, useStore
	verbose, useStore = true, false

	t.Cleanup(func() { verbose, useStore = origVerbose, origUseStore })

	origStdout := os.Stdout

	r, w, err := os.Pipe()
	require.NoError(t, err)

	os.Stdout = w

	done := make(chan []byte, 1)

	go func() {
		var buf bytes.Buffer

		_, _ = io.Copy(&buf, r)

		done <- buf.Bytes()
	}()

	processErr := ProcessFile(context.Background(), path, ulogger.TestLogger{}, &settings.Settings{})

	require.NoError(t, w.Close())

	os.Stdout = origStdout

	out := <-done

	require.NoError(t, r.Close())

	return string(out), processErr
}

// TestHandleSubtreeMeta covers the reader shapes issue 1425 is about, through
// the CLI's real entry point. Each rejected shape has to do two things at once:
// return the error, so `filereader x.subtreeMeta || alert` exits non-zero, and
// still print whatever the body yields, because which entries are missing is the
// diagnosis an operator opens the file for. An earlier fix returned on rejection
// and lost the dump, which is why both halves are asserted rather than one.
//
// The dumped-anyway output is only reachable when the body itself parses. On the
// over-long-count shape go-subtree's deserializer indexes past its destination
// slice and panics, so the assertion there is that the recover() in
// parseSubtreeMetaBestEffort catches it and the CLI reports instead of dying.
func TestHandleSubtreeMeta(t *testing.T) {
	tests := []struct {
		name string
		// mutate rewrites the correctly serialized meta bytes. nil leaves them alone.
		mutate func(t *testing.T, metaBytes []byte) []byte
		// wantErr is a substring of the returned error, or "" for the accepted case.
		wantErr string
		// wantOut are substrings that must appear on stdout.
		wantOut []string
		// notWantOut are substrings that must not.
		notWantOut []string
	}{
		{
			name:    "a well-formed meta is accepted and dumped",
			mutate:  nil,
			wantErr: "",
			wantOut: []string{"Number of transactions: 4", "         3: "},
			notWantOut: []string{
				"Subtree meta REJECTED",
				"Dumping the body anyway",
			},
		},
		{
			// A count of 2 against 4 real leaves. The body is intact, so the
			// best-effort re-parse succeeds and the operator still gets every index.
			name: "a short claimed count is rejected and the body is still dumped",
			mutate: func(_ *testing.T, metaBytes []byte) []byte {
				binary.LittleEndian.PutUint32(metaBytes[32:36], 2)

				return metaBytes
			},
			wantErr: "error reading subtree meta",
			wantOut: []string{
				"entry count mismatch",
				"Dumping the body anyway",
				"         3: ",
			},
		},
		{
			// The shape that genuinely panicked: the deserializer sizes its slice
			// from the real subtree (4) and writes the file-claimed count (5), so a
			// well-formed fifth entry hits index 4 of a length-4 slice. The CLI has
			// to name the defect instead of dying on it.
			name: "an over-long count with a well-formed extra entry is recovered, not a panic",
			mutate: func(t *testing.T, metaBytes []byte) []byte {
				t.Helper()

				extraInpoints := subtree.NewTxInpointsFromPacked([]chainhash.Hash{chainhash.HashH([]byte{0xcc})}, []uint32{1, 0})

				extra, err := extraInpoints.Serialize()
				require.NoError(t, err)

				binary.LittleEndian.PutUint32(metaBytes[32:36], 5)

				return append(metaBytes, extra...)
			},
			wantErr: "error reading subtree meta",
			wantOut: []string{
				"entry count mismatch",
				"Body could not be parsed",
			},
			notWantOut: []string{"Dumping the body anyway"},
		},
		{
			// A meta built for a different subtree, sitting under this subtree's
			// name. The count still matches, so the body parses and gets dumped.
			name: "a foreign root hash is rejected and the body is still dumped",
			mutate: func(_ *testing.T, metaBytes []byte) []byte {
				other := chainhash.HashH([]byte{0xfe, 0xed})
				copy(metaBytes[:32], other[:])

				return metaBytes
			},
			wantErr: "error reading subtree meta",
			wantOut: []string{
				"root hash mismatch",
				"Dumping the body anyway",
				"         3: ",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()

			st, metaBytes := buildSubtreeMetaFixture(t)

			if tt.mutate != nil {
				metaBytes = tt.mutate(t, metaBytes)
			}

			metaPath := writeSubtreePair(t, dir, st, metaBytes)

			var (
				out string
				err error
			)

			require.NotPanics(t, func() {
				out, err = runProcessFile(t, metaPath)
			})

			if tt.wantErr == "" {
				require.NoError(t, err)
			} else {
				// The error has to come back, not just be printed: ProcessFile's
				// caller turns it into a non-zero exit code.
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.wantErr)
			}

			for _, want := range tt.wantOut {
				require.Contains(t, out, want)
			}

			for _, notWant := range tt.notWantOut {
				require.NotContains(t, out, notWant)
			}
		})
	}
}

// TestHandleSubtreeMetaMissingSubtreeFile pins the other half of the pairing:
// the meta is checked against the sibling .subtree file, so without it there is
// nothing to check against and the CLI has to say which file it wanted.
func TestHandleSubtreeMetaMissingSubtreeFile(t *testing.T) {
	dir := t.TempDir()

	st, metaBytes := buildSubtreeMetaFixture(t)

	metaPath := writeSubtreePair(t, dir, st, metaBytes)

	require.NoError(t, os.Remove(filepath.Join(dir, st.RootHash().String()+".subtree")))

	_, err := runProcessFile(t, metaPath)
	require.Error(t, err)
	require.Contains(t, err.Error(), "depends on subtree file")
}

// TestParseSubtreeMetaBestEffortMissingFile covers the re-open path directly:
// the best-effort dump re-reads the file rather than holding it across both
// parses, so a file that has gone away between the two reads has to return nil
// instead of dereferencing a nil reader.
func TestParseSubtreeMetaBestEffortMissingFile(t *testing.T) {
	dir := t.TempDir()

	st, _ := buildSubtreeMetaFixture(t)

	require.NotPanics(t, func() {
		got := parseSubtreeMetaBestEffort(st, ulogger.TestLogger{}, &settings.Settings{}, dir, st.RootHash().String())
		require.Nil(t, got)
	})
}

// TestParseSubtreeMetaBestEffortHeaderlessFile covers the second early return in
// the re-open path: a file too short to hold a fileformat header.
func TestParseSubtreeMetaBestEffortHeaderlessFile(t *testing.T) {
	dir := t.TempDir()

	st, _ := buildSubtreeMetaFixture(t)

	base := st.RootHash().String()
	require.NoError(t, os.WriteFile(filepath.Join(dir, base+".subtreeMeta"), []byte("short"), 0o600))

	require.NotPanics(t, func() {
		got := parseSubtreeMetaBestEffort(st, ulogger.TestLogger{}, &settings.Settings{}, dir, base)
		require.Nil(t, got)
	})
}
