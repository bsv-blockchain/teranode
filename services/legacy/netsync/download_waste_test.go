package netsync

import (
	"bytes"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-wire"
	peerpkg "github.com/bsv-blockchain/teranode/services/legacy/peer"
	"github.com/bsv-blockchain/teranode/util/expiringmap"
	"github.com/stretchr/testify/require"
)

// Every way download bandwidth is lost is counted, so the node's reports and the recorder can say
// how much was wasted and how. Until 2026-09-24 none of this was recorded: duplicate copies, a
// peer dropped with blocks owed, a stream cut part way, and the total received were all read off
// the log by hand, and a night of syncing on four peers went unnoticed.

func streamManager(t *testing.T) *SyncManager {
	t.Helper()

	sm := newRaceManager(t)
	sm.blockSizeTracker = newBlockSizeTracker(10)
	sm.streams = newStreamRegistry()
	mockCommittedTip(t, sm, 10, 0)

	return sm
}

func TestBytesReceivedAreCounted(t *testing.T) {
	sm := streamManager(t)

	sink := sm.trackBlockStreams(func(_ chainhash.Hash, _ *wire.BlockHeader, r io.Reader, _ int64) (bool, error) {
		_, err := io.Copy(io.Discard, r)

		return true, err
	})

	_, err := sink(chainhash.Hash{0x61}, &wire.BlockHeader{}, bytes.NewReader(make([]byte, 5000)), 5080)
	require.NoError(t, err)

	require.Equal(t, int64(5000), sm.waste.received.Load())
}

func TestAStreamCutPartWayIsCountedWithItsBytes(t *testing.T) {
	sm := streamManager(t)

	sink := sm.trackBlockStreams(func(_ chainhash.Hash, _ *wire.BlockHeader, r io.Reader, _ int64) (bool, error) {
		buf := make([]byte, 3000)
		_, _ = io.ReadFull(r, buf)

		return false, errors.New("connection reset")
	})

	_, err := sink(chainhash.Hash{0x62}, &wire.BlockHeader{}, bytes.NewReader(make([]byte, 9000)), 9080)
	require.Error(t, err)

	require.Equal(t, int64(1), sm.waste.streamsFailed.Load())
	require.Equal(t, int64(3000), sm.waste.bytesWasted.Load(), "the bytes that arrived before it was cut")
}

func TestAPeerDroppedWithBlocksOwedIsCounted(t *testing.T) {
	sm := streamManager(t)

	p := newTestPeer(t, "10.0.0.7:8333")
	sm.peerStates.Set(p, &peerSyncState{requestedTxns: expiringmap.New[chainhash.Hash, struct{}](time.Hour)})

	require.True(t, sm.blockDownloads.Add(p, chainhash.Hash{0x63}))
	require.True(t, sm.blockDownloads.Add(p, chainhash.Hash{0x64}))

	sm.handleDonePeerMsg(p)

	require.Equal(t, int64(1), sm.waste.droppedOwing.Load())
	require.Equal(t, int64(2), sm.waste.blocksOwedAtDrop.Load())
}

func TestADuplicateConvertedCopyIsCounted(t *testing.T) {
	h := newParkWiringHarness(t, true)
	h.sm.drainAsync.Store(true)
	h.sm.parkCommits = make(chan parkCommit, 4)

	header := wire.BlockHeader{Version: 1, PrevBlock: h.blocks[1].MsgBlock().BlockHash()}
	body := peerpkg.BlockBody{Header: header, TxCount: 3, Size: 4096, Hash: header.BlockHash(), Converted: true}

	h.sm.handleBlockOnDiskMsg(&blockOnDiskMsg{body: body, peer: h.peer})
	h.sm.handleBlockOnDiskMsg(&blockOnDiskMsg{body: body, peer: h.peer})

	require.Equal(t, int64(1), h.sm.waste.dupConverted.Load(), "the second converted copy of a parked block")
}

func TestADrainedDuplicateIsCounted(t *testing.T) {
	sm := streamManager(t)

	sm.noteDrainedDuplicate(chainhash.Hash{0x65})

	require.Equal(t, int64(1), sm.waste.dupDrained.Load())
}

// A block asked of another peer because its owner went quiet is counted and logged with the
// peer let off, so every duplicate copy can be traced to a reason. On 2026-09-24 three
// duplicates arrived within twelve seconds and the log could not say why they were asked for.
func TestAReRequestAfterAQuietOwnerIsCounted(t *testing.T) {
	sm, _ := orderManager(t)
	sm.streams = newStreamRegistry()
	hash := chainhash.Hash{0x66}

	past := time.Now().Add(-2 * blockRequestRetryInterval)
	sm.blockDownloads.now = func() time.Time { return past }
	require.True(t, sm.blockDownloads.Add(newTestPeer(t, "10.0.0.8:8333"), hash))
	sm.blockDownloads.now = time.Now

	require.Len(t, sm.unownedBlocks([]wantedBlock{{height: 100, hash: hash}}), 1, "the quiet owner is let off")
	require.Equal(t, int64(1), sm.waste.reAskedQuiet.Load())
}
