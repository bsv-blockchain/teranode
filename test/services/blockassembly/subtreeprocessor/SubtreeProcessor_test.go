//go:build test_all || test_services || test_subtreeprocessor || test_subtree || test_long

package subtreeprocessor

import (
	"bytes"
	"encoding/binary"
	"os"
	"runtime/pprof"
	"testing"

	"github.com/bitcoin-sv/teranode/services/blockassembly/subtreeprocessor"
	"github.com/bitcoin-sv/teranode/util"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// go test -v -tags test_subtreeprocessor ./test/...

func Test_DeserializeHashesFromReaderIntoBuckets(t *testing.T) {
	size := 1024 * 1024
	subtreeBytes := generateLargeSubtreeBytes(t, size)
	r := bytes.NewReader(subtreeBytes)

	f, _ := os.Create("cpu.prof")
	defer f.Close()

	_ = pprof.StartCPUProfile(f)
	defer pprof.StopCPUProfile()

	buckets, _, err := subtreeprocessor.DeserializeHashesFromReaderIntoBuckets(r, 16)
	require.NoError(t, err)

	f, _ = os.Create("mem.prof")
	defer f.Close()
	_ = pprof.WriteHeapProfile(f)

	assert.Equal(t, 16, len(buckets))
}

func generateLargeSubtreeBytes(t *testing.T, size int) []byte {
	st, err := util.NewIncompleteTreeByLeafCount(size)
	require.NoError(t, err)

	var bb [32]byte
	for i := 0; i < size; i++ {
		// int to bytes
		//nolint:gosec
		binary.LittleEndian.PutUint32(bb[:], uint32(i))
		_ = st.AddNode(bb, uint64(i), uint64(i))
	}

	ser, err := st.Serialize()
	require.NoError(t, err)

	return ser
}
