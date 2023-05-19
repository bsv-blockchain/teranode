package merkle

import (
	"crypto/rand"
	"testing"

	"github.com/libsv/go-p2p/chaincfg/chainhash"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOpen(t *testing.T) {
	b := make([]byte, 32)
	_, err := rand.Read(b)
	require.NoError(t, err)

	chaintip, err := chainhash.NewHashFromStr("5bc1ec4dca8e07b2f816f538a7caf1b9e3765a1977082398914d54b215dfb362")
	require.NoError(t, err)

	height := uint32(1)

	txidFile, err := OpenForWriting(chaintip, height, 2)
	require.NoError(t, err)

	defer func() {
		_ = txidFile.deleteAll()
	}()

	count := txidFile.Count()
	assert.Equal(t, count, uint32(1))

	err = txidFile.AddTxID(chaintip)
	require.NoError(t, err)

	count = txidFile.Count()
	assert.Equal(t, count, uint32(2))

	err = txidFile.AddTxID(chaintip)
	require.NoError(t, err)

	count = txidFile.Count()
	assert.Equal(t, count, uint32(1))

}
