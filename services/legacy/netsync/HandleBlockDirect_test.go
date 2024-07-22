package netsync

import (
	"os"
	"testing"

	"github.com/bitcoin-sv/ubsv/services/legacy/bsvutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandleBlockDirect(t *testing.T) {
	// Load the block
	f, err := os.Open("../testdata/00000000000000000ad4cd15bbeaf6cb4583c93e13e311f9774194aadea87386.bin")
	require.NoError(t, err)
	defer f.Close()

	block, err := bsvutil.NewBlockFromReader(f)
	require.NoError(t, err)

	assert.Equal(t, block.Hash().String(), "00000000000000000ad4cd15bbeaf6cb4583c93e13e311f9774194aadea87386")

	// var sm SyncManager
	// err = sm.HandleBlockDirect(context.Background(), nil, block)

	// require.NoError(t, err)
}
