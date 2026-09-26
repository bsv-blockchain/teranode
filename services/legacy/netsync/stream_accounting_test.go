package netsync

import (
	"bytes"
	"io"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-wire"
	"github.com/stretchr/testify/require"
)

// Every block now streams to disk, so the path that decodes a whole block, the only one that fed
// the block-size average, never runs. On mainnet at height 707,700 the average read 0 MB, which
// left the size ladder at 20 blocks per peer for blocks up to 1.3 GB and the time cap with nothing
// to estimate from. And a stream only counted as complete when it had read the payload's declared
// length, but the 80-byte header is read before the stream is counted, so none ever was: no
// peer's rate was recorded, and neither was the time of its last block.

func TestAStreamedBlockRecordsItsSizeAndItsPeersRate(t *testing.T) {
	sm := newRaceManager(t)
	sm.blockSizeTracker = newBlockSizeTracker(10)
	sm.streams = newStreamRegistry()
	mockCommittedTip(t, sm, 10, 0)

	owner := newTestPeer(t, "10.0.0.1:8333")
	hash := chainhash.Hash{0x5a}
	require.True(t, sm.blockDownloads.Add(owner, hash))

	const payload = int64(1 << 20)

	body := bytes.Repeat([]byte{1}, int(payload)-wire.MaxBlockHeaderPayload)

	sink := sm.trackBlockStreams(func(_ chainhash.Hash, _ *wire.BlockHeader, r io.Reader, _ int64) (bool, error) {
		_, err := io.Copy(io.Discard, r)

		return true, err
	})

	converted, err := sink(hash, &wire.BlockHeader{}, bytes.NewReader(body), payload)
	require.NoError(t, err)
	require.True(t, converted)

	require.Equal(t, payload, sm.blockSizeTracker.getAverageSize(), "the block's size feeds the average")
	require.Positive(t, sm.streams.peerRate(owner), "its peer's rate is recorded")
	require.False(t, sm.streams.lastBlockBytes(owner).IsZero(), "and when that peer last delivered a block")
}
