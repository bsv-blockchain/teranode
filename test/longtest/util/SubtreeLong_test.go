package util

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"runtime"
	"runtime/pprof"
	"testing"
	"time"

	subtreepkg "github.com/bitcoin-sv/teranode/pkg/go-subtree"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// go test -v -tags test_subtree ./test/...

func Test_Deserialize(t *testing.T) {
	runtime.SetCPUProfileRate(1000)

	size := 1024 * 1024
	subtreeBytes := generateLargeSubtreeBytes(t, size)

	f, _ := os.Create("cpu.prof")
	defer f.Close()

	_ = pprof.StartCPUProfile(f)
	defer pprof.StopCPUProfile()

	subtree, err := subtreepkg.NewTreeByLeafCount(size)
	require.NoError(t, err)

	err = subtree.Deserialize(subtreeBytes)
	require.NoError(t, err)

	f, _ = os.Create("mem.prof")
	defer f.Close()
	_ = pprof.WriteHeapProfile(f)
}

func Test_DeserializeFromReader(t *testing.T) {
	runtime.SetCPUProfileRate(1000)

	size := 1024 * 1024
	subtreeBytes := generateLargeSubtreeBytes(t, size)
	r := bytes.NewReader(subtreeBytes)

	f, _ := os.Create("cpu.prof")
	defer f.Close()

	_ = pprof.StartCPUProfile(f)
	defer pprof.StopCPUProfile()

	subtree, err := subtreepkg.NewTreeByLeafCount(size)
	require.NoError(t, err)

	err = subtree.DeserializeFromReader(r)
	require.NoError(t, err)

	f, _ = os.Create("mem.prof")
	defer f.Close()
	_ = pprof.WriteHeapProfile(f)
}

func Test_BuildMerkleTreeStoreFromBytesBig(t *testing.T) {

	numberOfItems := 1_024 * 1_024
	subtree, err := subtreepkg.NewTreeByLeafCount(numberOfItems)
	require.NoError(t, err)

	for i := 0; i < numberOfItems; i++ {
		tx := bt.NewTx()
		_ = tx.AddOpReturnOutput([]byte(fmt.Sprintf("tx%d", i)))
		require.NoError(t, subtree.AddNode(*tx.TxIDChainHash(), 1, 0))
	}

	f, _ := os.Create("cpu.prof")
	defer f.Close()

	_ = pprof.StartCPUProfile(f)
	defer pprof.StopCPUProfile()

	start := time.Now()

	hashes, err := subtreepkg.BuildMerkleTreeStoreFromBytes(subtree.Nodes)
	require.NoError(t, err)

	rootHash, err := chainhash.NewHash((*hashes)[len(*hashes)-1][:])
	require.NoError(t, err)

	assert.Equal(t, "199037f7b64e6dd88701dab414e88e24c328e7f5907640d00631974894dcc698", rootHash.String())

	t.Logf("Time taken: %s\n", time.Since(start))

	f, _ = os.Create("mem.prof")
	defer f.Close()
	_ = pprof.WriteHeapProfile(f)
}

func generateLargeSubtreeBytes(t *testing.T, size int) []byte {
	st, err := subtreepkg.NewIncompleteTreeByLeafCount(size)
	require.NoError(t, err)

	var bb [32]byte
	for i := 0; i < size; i++ {
		// int to bytes
		binary.LittleEndian.PutUint32(bb[:], uint32(i))
		_ = st.AddNode(bb, 111, 234)
	}

	ser, err := st.Serialize()
	require.NoError(t, err)

	return ser
}
