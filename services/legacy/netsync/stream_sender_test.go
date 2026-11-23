package netsync

import (
	"bytes"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-wire"
	peerpkg "github.com/bsv-blockchain/teranode/services/legacy/peer"
	"github.com/stretchr/testify/require"
)

// A block stream is credited to the peer sending it, and only when that peer owes the block. It
// used to be credited to whichever peer the download ledger said owed the block, whoever sent
// the bytes. Any connected peer could then send a copy of a block another peer owed and set that
// peer's rate, keep it looking busy, set the largest recent block size, and, by sending slowly,
// have the frontier race drop the honest owner.

// drainingSink stands in for admitPipelineSink's drain of a copy from a peer that does not owe
// the block: it reads the bytes and converts nothing. during runs while the stream is live.
func drainingSink(during func()) func(chainhash.Hash, *wire.BlockHeader, io.Reader, int64) (bool, error) {
	return func(_ chainhash.Hash, _ *wire.BlockHeader, r io.Reader, _ int64) (bool, error) {
		if during != nil {
			during()
		}

		_, err := io.Copy(io.Discard, r)

		return false, err
	}
}

func TestACopyFromAPeerThatDoesNotOweTheBlockChangesNoPeersAccounting(t *testing.T) {
	sm, owner, fast, _ := reaskSetup(t)
	stranger, _ := schedulerPeer(t, sm, 3, 2000)
	now := time.Now()

	block := heightHash(t, sm, 11)
	askAt(t, sm, owner, block, now.Add(-3*time.Minute))

	sink := sm.trackBlockStreams(drainingSink(func() {
		require.False(t, sm.streams.arriving(block), "a copy nobody owes is not the block arriving")
		_, _, _, ok := sm.streams.arrivingFrom(block, stranger)
		require.False(t, ok)
		require.True(t, sm.streams.lastBlockBytes(owner).IsZero(), "the owner sends nothing")
		require.False(t, sm.ownerStillSending(block), "so it does not look busy")

		held, n := sm.streams.arrivingBytes()
		require.Zero(t, held, "a drained copy holds no disk")
		require.Zero(t, n)
	}))

	_, err := sink(block, &wire.BlockHeader{}, peerpkg.NewDeliveryReader(bytes.NewReader(make([]byte, 1_000_000)), stranger), 2_000_000_000)
	require.NoError(t, err)

	require.InDelta(t, 3_500_000, sm.streams.peerRate(owner), 0, "the owner's rate is its own")
	require.InDelta(t, 80_000_000, sm.streams.peerRate(fast), 0)
	require.Zero(t, sm.streams.peerRate(stranger), "the stranger is not measured on a block it was not asked for")
	require.True(t, sm.streams.lastBlockBytes(owner).IsZero())
	require.True(t, sm.streams.lastBlockBytes(stranger).IsZero())
	require.Equal(t, int64(reaskTypicalBlock), sm.blockSizeTracker.largestRecentSize(), "a declared 2 GB nobody asked for is not a block size")
	require.Equal(t, int64(reaskTypicalBlock), sm.blockSizeTracker.getAverageSize())
}

// The race drops the peer sending a block slowly. A slow copy from a peer that does not owe the
// block is not the owner's, and must not cost the owner its connection.
func TestTheRaceNeverDropsAnOwnerForACopyItDidNotSend(t *testing.T) {
	sm, owner, fast, _ := reaskSetup(t)
	stranger, _ := schedulerPeer(t, sm, 3, 2000)
	now := time.Now()

	block := heightHash(t, sm, 11)
	askAt(t, sm, owner, block, now.Add(-3*time.Minute))

	sink := sm.trackBlockStreams(func(_ chainhash.Hash, _ *wire.BlockHeader, r io.Reader, _ int64) (bool, error) {
		// 1,000 bytes in 40 s: 25 bytes a second, far under the race's 100 KB/s.
		_, err := io.CopyN(io.Discard, r, 1000)
		require.NoError(t, err)

		sm.maybeRaceSlowBlock(time.Now().Add(40 * time.Second))

		_, err = io.Copy(io.Discard, r)

		return false, err
	})

	_, err := sink(block, &wire.BlockHeader{}, peerpkg.NewDeliveryReader(bytes.NewReader(make([]byte, 5000)), stranger), 300_000_000)
	require.NoError(t, err)

	require.True(t, owner.Connected(), "the owner sent nothing slowly")
	require.False(t, sm.blockDownloads.HasOwner(fast, block), "and nobody else was asked")
	require.ElementsMatch(t, []*peerpkg.Peer{owner}, sm.blockDownloads.OwnersOf(block))
}

// The sync manager reads the stream registry from its own goroutines while each peer's read loop
// starts and finishes streams. Every field a reader sees must be set before the stream is
// published. The test runs a stream from an owner many times on one goroutine while another
// reads the registry; -race reports a field written after publication.
func TestTheStreamRegistryIsSafeToReadWhileStreamsStartAndFinish(t *testing.T) {
	sm, owner, _, _ := reaskSetup(t)
	now := time.Now()

	block := heightHash(t, sm, 11)
	askAt(t, sm, owner, block, now.Add(-time.Minute))

	sink := sm.trackBlockStreams(drainingSink(nil))

	stop := make(chan struct{})

	var wg sync.WaitGroup

	wg.Add(1)

	go func() {
		defer wg.Done()

		for {
			select {
			case <-stop:
				return
			default:
			}

			sm.streams.lastBlockBytes(owner)
			sm.streams.pending(owner)
			sm.streams.arriving(block)
			sm.streams.arrivingFrom(block, owner)
			sm.streams.arrivingBytes()
			sm.streams.pickRace(time.Now().Add(time.Minute), 10, 0)
		}
	}()

	for range 500 {
		_, err := sink(block, &wire.BlockHeader{}, peerpkg.NewDeliveryReader(bytes.NewReader(make([]byte, 64)), owner), 144)
		require.NoError(t, err)
	}

	close(stop)
	wg.Wait()

	require.Positive(t, sm.streams.peerRate(owner), "the owner's streams were measured")
}

// Only a converted copy from a peer that owes the block is a block size. An owed copy that is
// drained, because another copy converted the block, would count the block twice.
func TestOnlyAConvertedOwedCopySetsTheBlockSize(t *testing.T) {
	sm, owner, _, _ := reaskSetup(t)
	now := time.Now()

	block := heightHash(t, sm, 11)
	askAt(t, sm, owner, block, now.Add(-time.Minute))

	drained := sm.trackBlockStreams(drainingSink(nil))
	_, err := drained(block, &wire.BlockHeader{}, peerpkg.NewDeliveryReader(bytes.NewReader(make([]byte, 64)), owner), 2_000_000_000)
	require.NoError(t, err)
	require.Equal(t, int64(reaskTypicalBlock), sm.blockSizeTracker.largestRecentSize(), "a drained copy sets no size")

	converted := sm.trackBlockStreams(func(_ chainhash.Hash, _ *wire.BlockHeader, r io.Reader, _ int64) (bool, error) {
		_, err := io.Copy(io.Discard, r)

		return true, err
	})
	_, err = converted(block, &wire.BlockHeader{}, peerpkg.NewDeliveryReader(bytes.NewReader(make([]byte, 64)), owner), 2_000_000_000)
	require.NoError(t, err)
	require.Equal(t, int64(2_000_000_000), sm.blockSizeTracker.largestRecentSize(), "the converted copy does")
}
