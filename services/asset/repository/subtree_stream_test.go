package repository

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReadSubtreeNodesPageFromReaderCapsLimit(t *testing.T) {
	nodes := make([]subtreepkg.Node, 150)
	for i := range nodes {
		nodes[i] = subtreepkg.Node{
			Hash: hashForSubtreeStreamTest(byte(i)),
			Fee:  uint64(i),
		}
	}

	page, totalNodes, err := readSubtreeNodesPageFromReader(context.Background(), bytes.NewReader(serializeSubtreeStreamTestData(t, nodes, nil)), 0, 1000)
	require.NoError(t, err)

	assert.Len(t, page, 100)
	assert.Equal(t, 150, totalNodes)
	assert.Equal(t, nodes[99].Hash, page[99].Hash)
}

func TestReadSubtreePageFromReaderStopsAfterPartialPage(t *testing.T) {
	nodes := make([]subtreepkg.Node, 5)
	for i := range nodes {
		nodes[i] = subtreepkg.Node{
			Hash: hashForSubtreeStreamTest(byte(i)),
			Fee:  uint64(i),
		}
	}

	conflictingNodes := []chainhash.Hash{nodes[4].Hash}
	stream := serializeSubtreeStreamTestData(t, nodes, conflictingNodes)
	partialPageEnd := subtreeStreamHeaderSize + 2*subtreeNodeRecordSize

	page, offset, totalNodes, err := readSubtreePageFromReader(context.Background(), bytes.NewReader(stream[:partialPageEnd]), 1, 1)
	require.NoError(t, err)

	assert.Equal(t, 1, offset)
	assert.Equal(t, len(nodes), totalNodes)
	require.Len(t, page.Nodes, 1)
	assert.Equal(t, nodes[1].Hash, page.Nodes[0].Hash)
	assert.Empty(t, page.ConflictingNodes)
}

func TestReadSubtreeNodesPageFromReader_RejectsNegativeOffset(t *testing.T) {
	stream := serializeSubtreeStreamTestData(t, nil, nil)
	_, _, err := readSubtreeNodesPageFromReader(context.Background(), bytes.NewReader(stream), -1, 1)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "offset cannot be negative")
}

func TestReadSubtreeNodesPageFromReader_RejectsNegativeLimit(t *testing.T) {
	stream := serializeSubtreeStreamTestData(t, nil, nil)
	_, _, err := readSubtreeNodesPageFromReader(context.Background(), bytes.NewReader(stream), 0, -1)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "limit cannot be negative")
}

func TestReadSubtreeNodesPageFromReader_OffsetPastEndReturnsEmpty(t *testing.T) {
	nodes := []subtreepkg.Node{{
		Hash: hashForSubtreeStreamTest(0),
		Fee:  1,
	}}
	stream := serializeSubtreeStreamTestData(t, nodes, nil)

	page, totalNodes, err := readSubtreeNodesPageFromReader(context.Background(), bytes.NewReader(stream), 5, 10)
	require.NoError(t, err)
	assert.Empty(t, page)
	assert.Equal(t, 1, totalNodes)
}

func TestReadSubtreeNodesPageFromReader_ZeroLimitReturnsEmpty(t *testing.T) {
	nodes := []subtreepkg.Node{{
		Hash: hashForSubtreeStreamTest(0),
		Fee:  1,
	}}
	stream := serializeSubtreeStreamTestData(t, nodes, nil)

	page, totalNodes, err := readSubtreeNodesPageFromReader(context.Background(), bytes.NewReader(stream), 0, 0)
	require.NoError(t, err)
	assert.Empty(t, page)
	assert.Equal(t, 1, totalNodes)
}

func TestReadSubtreeNodesPageFromReader_PageTruncatedByTotalNodes(t *testing.T) {
	nodes := make([]subtreepkg.Node, 3)
	for i := range nodes {
		nodes[i] = subtreepkg.Node{Hash: hashForSubtreeStreamTest(byte(i)), Fee: uint64(i)}
	}
	stream := serializeSubtreeStreamTestData(t, nodes, nil)

	page, totalNodes, err := readSubtreeNodesPageFromReader(context.Background(), bytes.NewReader(stream), 2, 50)
	require.NoError(t, err)
	require.Len(t, page, 1)
	assert.Equal(t, 3, totalNodes)
	assert.Equal(t, nodes[2].Hash, page[0].Hash)
}

func TestReadSubtreePageFromReader_RejectsNegativeOffset(t *testing.T) {
	stream := serializeSubtreeStreamTestData(t, nil, nil)
	_, _, _, err := readSubtreePageFromReader(context.Background(), bytes.NewReader(stream), -1, 1)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "offset cannot be negative")
}

func TestReadSubtreePageFromReader_RejectsNegativeLimit(t *testing.T) {
	stream := serializeSubtreeStreamTestData(t, nil, nil)
	_, _, _, err := readSubtreePageFromReader(context.Background(), bytes.NewReader(stream), 0, -1)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "limit cannot be negative")
}

func TestReadSubtreePageFromReader_EmptySubtree(t *testing.T) {
	stream := serializeSubtreeStreamTestData(t, nil, nil)

	page, offset, totalNodes, err := readSubtreePageFromReader(context.Background(), bytes.NewReader(stream), 0, 10)
	require.NoError(t, err)
	assert.Equal(t, 0, offset)
	assert.Equal(t, 0, totalNodes)
	assert.Equal(t, 0, page.Height)
	assert.Empty(t, page.Nodes)
	assert.Empty(t, page.ConflictingNodes)
}

func TestReadSubtreePageFromReader_FullPageIncludesConflictingNodes(t *testing.T) {
	nodes := []subtreepkg.Node{
		{Hash: hashForSubtreeStreamTest(0), Fee: 1},
		{Hash: hashForSubtreeStreamTest(1), Fee: 2},
	}
	conflicting := []chainhash.Hash{hashForSubtreeStreamTest(0)}
	stream := serializeSubtreeStreamTestData(t, nodes, conflicting)

	page, offset, totalNodes, err := readSubtreePageFromReader(context.Background(), bytes.NewReader(stream), 0, len(nodes))
	require.NoError(t, err)
	assert.Equal(t, 0, offset)
	assert.Equal(t, len(nodes), totalNodes)
	require.Len(t, page.ConflictingNodes, 1)
	assert.Equal(t, conflicting[0], page.ConflictingNodes[0])
}

func TestReadSubtreePageFromReader_TruncatedHeaderErrors(t *testing.T) {
	stream := serializeSubtreeStreamTestData(t, nil, nil)
	_, _, _, err := readSubtreePageFromReader(context.Background(), bytes.NewReader(stream[:subtreeStreamHeaderSize-1]), 0, 10)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unable to read subtree root information")
}

func TestSubtreeNodeHashesReadCloser_StreamsOnlyHashes(t *testing.T) {
	nodes := make([]subtreepkg.Node, 4)
	for i := range nodes {
		nodes[i] = subtreepkg.Node{Hash: hashForSubtreeStreamTest(byte(i)), Fee: uint64(i)}
	}
	stream := serializeSubtreeStreamTestData(t, nodes, nil)

	rc, err := newSubtreeNodeHashesReadCloser(context.Background(), io.NopCloser(bytes.NewReader(stream)))
	require.NoError(t, err)
	defer rc.Close()

	out, err := io.ReadAll(rc)
	require.NoError(t, err)

	require.Len(t, out, len(nodes)*chainhash.HashSize)
	for i, node := range nodes {
		assert.Equal(t, node.Hash[:], out[i*chainhash.HashSize:(i+1)*chainhash.HashSize])
	}
}

func TestSubtreeNodeHashesReadCloser_ZeroLengthReadReturnsZero(t *testing.T) {
	nodes := []subtreepkg.Node{{Hash: hashForSubtreeStreamTest(0), Fee: 1}}
	stream := serializeSubtreeStreamTestData(t, nodes, nil)

	rc, err := newSubtreeNodeHashesReadCloser(context.Background(), io.NopCloser(bytes.NewReader(stream)))
	require.NoError(t, err)
	defer rc.Close()

	n, err := rc.Read(nil)
	require.NoError(t, err)
	assert.Equal(t, 0, n)
}

func TestSubtreeNodeHashesReadCloser_EmptySubtreeYieldsEOF(t *testing.T) {
	stream := serializeSubtreeStreamTestData(t, nil, nil)

	rc, err := newSubtreeNodeHashesReadCloser(context.Background(), io.NopCloser(bytes.NewReader(stream)))
	require.NoError(t, err)
	defer rc.Close()

	out, err := io.ReadAll(rc)
	require.NoError(t, err)
	assert.Empty(t, out)
}

func TestSubtreeNodeHashesReadCloser_TruncatedHeaderClosesSource(t *testing.T) {
	stream := serializeSubtreeStreamTestData(t, nil, nil)
	source := &countingCloser{Reader: bytes.NewReader(stream[:subtreeStreamHeaderSize-1])}

	_, err := newSubtreeNodeHashesReadCloser(context.Background(), source)
	require.Error(t, err)
	assert.Equal(t, 1, source.closes, "source must be closed on header read failure")
}

func TestSubtreeNodeHashesReadCloser_ContextCancellationReturnsError(t *testing.T) {
	nodes := []subtreepkg.Node{
		{Hash: hashForSubtreeStreamTest(0), Fee: 1},
		{Hash: hashForSubtreeStreamTest(1), Fee: 2},
	}
	stream := serializeSubtreeStreamTestData(t, nodes, nil)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	rc, err := newSubtreeNodeHashesReadCloser(ctx, io.NopCloser(bytes.NewReader(stream)))
	require.NoError(t, err)
	defer rc.Close()

	buf := make([]byte, chainhash.HashSize)
	_, err = rc.Read(buf)
	require.Error(t, err)
	assert.ErrorIs(t, err, context.Canceled)
}

func TestReadSubtreeConflictingNodes_TruncatedCountErrors(t *testing.T) {
	// Build a stream that ends right after the node records, omitting the
	// conflicting-node count entirely.
	nodes := []subtreepkg.Node{{Hash: hashForSubtreeStreamTest(0), Fee: 1}}
	stream := serializeSubtreeStreamTestData(t, nodes, nil)
	truncated := stream[:subtreeStreamHeaderSize+subtreeNodeRecordSize]

	_, _, _, err := readSubtreePageFromReader(context.Background(), bytes.NewReader(truncated), 0, len(nodes))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unable to read number of conflicting nodes")
}

type countingCloser struct {
	io.Reader
	closes int
}

func (c *countingCloser) Close() error {
	c.closes++
	return nil
}

func TestReadSubtreePageFromReaderRejectsImpossibleConflictCount(t *testing.T) {
	nodes := []subtreepkg.Node{{
		Hash: hashForSubtreeStreamTest(0),
		Fee:  1,
	}}
	stream := serializeSubtreeStreamTestData(t, nodes, nil)
	binary.LittleEndian.PutUint64(stream[subtreeStreamHeaderSize+subtreeNodeRecordSize:], 2)

	_, _, _, err := readSubtreePageFromReader(context.Background(), bytes.NewReader(stream), 0, 1)
	require.Error(t, err)

	assert.Contains(t, err.Error(), "conflicting node count exceeds node count")
}

func serializeSubtreeStreamTestData(t *testing.T, nodes []subtreepkg.Node, conflictingNodes []chainhash.Hash) []byte {
	t.Helper()

	var buf bytes.Buffer
	rootHash := hashForSubtreeStreamTest(255)
	_, err := buf.Write(rootHash[:])
	require.NoError(t, err)

	var bytes8 [8]byte
	binary.LittleEndian.PutUint64(bytes8[:], sumSubtreeStreamTestFees(nodes))
	_, err = buf.Write(bytes8[:])
	require.NoError(t, err)

	binary.LittleEndian.PutUint64(bytes8[:], 0)
	_, err = buf.Write(bytes8[:])
	require.NoError(t, err)

	binary.LittleEndian.PutUint64(bytes8[:], uint64(len(nodes)))
	_, err = buf.Write(bytes8[:])
	require.NoError(t, err)

	for _, node := range nodes {
		_, err = buf.Write(node.Hash[:])
		require.NoError(t, err)

		binary.LittleEndian.PutUint64(bytes8[:], node.Fee)
		_, err = buf.Write(bytes8[:])
		require.NoError(t, err)

		binary.LittleEndian.PutUint64(bytes8[:], node.SizeInBytes)
		_, err = buf.Write(bytes8[:])
		require.NoError(t, err)
	}

	binary.LittleEndian.PutUint64(bytes8[:], uint64(len(conflictingNodes)))
	_, err = buf.Write(bytes8[:])
	require.NoError(t, err)

	for _, hash := range conflictingNodes {
		_, err = buf.Write(hash[:])
		require.NoError(t, err)
	}

	return buf.Bytes()
}

func hashForSubtreeStreamTest(seed byte) chainhash.Hash {
	var hash chainhash.Hash
	for i := range hash {
		hash[i] = seed
	}
	return hash
}

func sumSubtreeStreamTestFees(nodes []subtreepkg.Node) uint64 {
	var fees uint64
	for _, node := range nodes {
		fees += node.Fee
	}
	return fees
}

func BenchmarkSubtreeNodeHashesReader(b *testing.B) {
	const benchNodes = 1024

	nodes := make([]subtreepkg.Node, benchNodes)
	for i := range nodes {
		nodes[i] = subtreepkg.Node{
			Hash: hashForSubtreeStreamTest(byte(i)),
			Fee:  uint64(i),
		}
	}
	stream := serializeSubtreeStreamTestDataB(b, nodes, nil)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		rc, err := newSubtreeNodeHashesReadCloser(context.Background(), io.NopCloser(bytes.NewReader(stream)))
		if err != nil {
			b.Fatalf("newSubtreeNodeHashesReadCloser: %v", err)
		}
		if _, err := io.Copy(io.Discard, rc); err != nil {
			b.Fatalf("drain: %v", err)
		}
		_ = rc.Close()
	}
}

// TestSubtreeNodeHashesReaderAllocsBound locks in the streaming-path memory claim:
// allocations must NOT scale with subtree size. The streaming reader allocates a
// bufio buffer + reader struct + io.NopCloser wrapper; we cap at 16 per Read pass
// to leave headroom but still catch regressions that re-introduce O(nodes) buffering.
func TestSubtreeNodeHashesReaderAllocsBound(t *testing.T) {
	const benchNodes = 1024
	const maxAllocsPerOp = 16

	result := testing.Benchmark(BenchmarkSubtreeNodeHashesReader)
	if result.N == 0 {
		t.Fatal("benchmark did not run")
	}
	if got := result.AllocsPerOp(); got > maxAllocsPerOp {
		t.Fatalf("AllocsPerOp=%d exceeded ceiling %d for streaming path (subtree=%d nodes); the implementation likely regressed to non-streaming buffering", got, maxAllocsPerOp, benchNodes)
	}
}

// serializeSubtreeStreamTestDataB is the benchmark equivalent of
// serializeSubtreeStreamTestData (which only accepts *testing.T).
func serializeSubtreeStreamTestDataB(b *testing.B, nodes []subtreepkg.Node, conflictingNodes []chainhash.Hash) []byte {
	b.Helper()

	var buf bytes.Buffer
	rootHash := hashForSubtreeStreamTest(255)
	if _, err := buf.Write(rootHash[:]); err != nil {
		b.Fatalf("write root hash: %v", err)
	}

	var bytes8 [8]byte
	binary.LittleEndian.PutUint64(bytes8[:], sumSubtreeStreamTestFees(nodes))
	if _, err := buf.Write(bytes8[:]); err != nil {
		b.Fatalf("write fees: %v", err)
	}

	binary.LittleEndian.PutUint64(bytes8[:], 0)
	if _, err := buf.Write(bytes8[:]); err != nil {
		b.Fatalf("write size: %v", err)
	}

	binary.LittleEndian.PutUint64(bytes8[:], uint64(len(nodes)))
	if _, err := buf.Write(bytes8[:]); err != nil {
		b.Fatalf("write numLeaves: %v", err)
	}

	for _, node := range nodes {
		if _, err := buf.Write(node.Hash[:]); err != nil {
			b.Fatalf("write node hash: %v", err)
		}

		binary.LittleEndian.PutUint64(bytes8[:], node.Fee)
		if _, err := buf.Write(bytes8[:]); err != nil {
			b.Fatalf("write node fee: %v", err)
		}

		binary.LittleEndian.PutUint64(bytes8[:], node.SizeInBytes)
		if _, err := buf.Write(bytes8[:]); err != nil {
			b.Fatalf("write node size: %v", err)
		}
	}

	binary.LittleEndian.PutUint64(bytes8[:], uint64(len(conflictingNodes)))
	if _, err := buf.Write(bytes8[:]); err != nil {
		b.Fatalf("write conflict count: %v", err)
	}

	for _, hash := range conflictingNodes {
		if _, err := buf.Write(hash[:]); err != nil {
			b.Fatalf("write conflict hash: %v", err)
		}
	}

	return buf.Bytes()
}
