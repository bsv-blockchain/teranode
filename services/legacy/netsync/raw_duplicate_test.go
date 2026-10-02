package netsync

import (
	"bytes"
	"context"
	"io"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	peerpkg "github.com/bsv-blockchain/teranode/services/legacy/peer"
	"github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/stretchr/testify/require"
)

// When two peers send the same block at once, one copy is converted as it arrives and the other,
// refused a second conversion, is written raw. On 2026-09-24 mainnet block 707,178 arrived from
// two peers within a second, the raw copy finished first, and the park took it. Processing the raw
// body writes the block's subtree files, and the converting copy wrote the same subtree files in
// its own format a second later, so validation read a mix, failed, and the block was given up with
// both files deleted. The chain stopped at 707,177. A raw copy is now discarded whenever another
// copy of the block is converting or has already been converted, and only the raw file goes.

// A copy refused a second conversion, because another copy of the block is converting, is not
// written at all. Writing it raw is what let the park take it whenever the converting copy then
// failed, and processing a raw copy after a conversion attempt failed subtree validation twice on
// 2026-09-24: 707,178, and 708,115 after the converting copy's peer was disconnected. Its bytes
// are drained so the connection stays on a message boundary, and its on-disk message is ignored.
// If the converting copy fails, nobody owes the block any more and the next pass asks for it.
func TestADuplicateCopyIsDrainedAndNeverParked(t *testing.T) {
	store := memory.New()
	sm := newPipelineParkManager(t, store, 8)
	enablePrefetchBudgetForTest(t, sm, 4)

	hash := chainhash.Hash{0x7a}

	// The first copy holds the block's admission.
	err := sm.AcquireBlockPrefetch(context.Background(), hash)
	require.NoError(t, err)

	inner := func(chainhash.Hash, *wire.BlockHeader, io.Reader, int64) (bool, error) {
		t.Fatal("a duplicate must not reach the converter")

		return false, nil
	}

	body := bytes.Repeat([]byte{7}, 4096)
	r := bytes.NewReader(body)

	converted, err := sm.admitPipelineSink(inner)(hash, &wire.BlockHeader{}, r, int64(len(body)))
	require.NoError(t, err, "the peer did nothing wrong")
	require.False(t, converted)
	require.Zero(t, r.Len(), "every byte is read off the wire")

	exists, err := store.Exists(context.Background(), hash[:], fileformat.FileTypeMsgBlock, parkOpts...)
	require.NoError(t, err)
	require.False(t, exists, "nothing is written")

	sm.handleBlockOnDiskMsg(&blockOnDiskMsg{body: peerpkg.BlockBody{Hash: hash, Size: int64(len(body))}})

	require.False(t, sm.blockPark.Has(hash), "and its on-disk message parks nothing")
}
