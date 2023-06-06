package merkle

import (
	"testing"

	"github.com/libsv/go-p2p/chaincfg/chainhash"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var coinbase = "8c14f0db3df150123e6f3dbbf30f8b955a8249b62ac1d1ff16284aefa3d06d87"

var txIds []string = []string{
	"fff2525b8931402dd09222c50775608f75787bd2b87e56995a7bdd30f79702c4",
	"6359f0868171b1d194cbee1af2f16ea598ae8fad666d9b012c8ed2b79a236ec4",
	"e9a66845e05d5abc0ad04ec80f774a7e585c6e8db975962d069a522137b80c1d",
}

var expectedMerkleRoot = "f3e94742aca4b5ef85488dc37c06c3282295ffec960994b2c0d5ac2a25a95766"

func TestOpen(t *testing.T) {
	// b := make([]byte, 32)
	// _, err := rand.Read(b)
	// require.NoError(t, err)

	chaintip, err := chainhash.NewHashFromStr("5bc1ec4dca8e07b2f816f538a7caf1b9e3765a1977082398914d54b215dfb362")
	require.NoError(t, err)

	height := uint32(1)

	container, err := OpenForWriting(chaintip, height, 4)
	require.NoError(t, err)

	defer func() {
		err = container.deleteAll()
		require.NoError(t, err)
	}()

	count := container.Count()
	assert.Equal(t, count, uint32(1))

	for _, txid := range txIds {
		hash, err := chainhash.NewHashFromStr(txid)
		require.NoError(t, err)

		err = container.AddTxID(hash, 1)
		require.NoError(t, err)
	}

	count = container.Count()
	assert.Equal(t, uint32(4), count)

	assert.Equal(t, uint64(3), container.fees)

	err = container.AddTxID(chaintip, 1)
	require.NoError(t, err)

	count = container.Count()
	assert.Equal(t, uint32(1), count)

	err = container.Close()
	require.NoError(t, err)

	container, err = OpenForReading(chaintip, height, 0)
	require.NoError(t, err)

	count = container.Count()
	assert.Equal(t, uint32(4), count)

	coinbaseHash, err := chainhash.NewHashFromStr(coinbase)
	require.NoError(t, err)

	merkleRoot, err := container.MerkleRoot(coinbaseHash)
	require.NoError(t, err)

	assert.Equal(t, expectedMerkleRoot, merkleRoot.String())

	container.Close()

	// Make sure opening this height again opens the 2nd file, not the first...
	container, err = OpenForWriting(chaintip, height, 4)
	require.NoError(t, err)

	count = container.Count()
	assert.Equal(t, uint32(1), count)
}
