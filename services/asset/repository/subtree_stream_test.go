package repository

import (
	"bytes"
	"context"
	"encoding/binary"
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
