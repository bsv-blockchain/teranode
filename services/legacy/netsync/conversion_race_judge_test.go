package netsync

import (
	"bytes"
	"context"
	"io"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/errors"
	peerpkg "github.com/bsv-blockchain/teranode/services/legacy/peer"
	"github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/stretchr/testify/require"
)

// A converting copy that sends no byte arrives on a DATA1 sub-peer whose primary does not owe the
// block, so the copy's owner cannot be resolved and the sub-peer is the sender. At the bound the
// whole association goes, as every other peer drop does through its primary: only the primary is
// known to netsync's bookkeeping, and nothing re-opens a lost DATA1. Disconnecting the sub-peer
// alone left the primary connected.
func TestATakeoverDropsTheAssociationOfACopyWhoseOwnerIsNotResolved(t *testing.T) {
	store := memory.New()
	sm := newPipelineParkManager(t, store, 8)
	sm.blockPark.dir = t.TempDir()

	timer := make(chan time.Time, 1)
	sm.takeoverAfter = func(time.Duration) <-chan time.Time { return timer }

	blk := wireBlockWithTxs(t, 40, false)
	pipelineHeaderFixture(t, sm, blk)
	proveBlockOrigin(t, sm, blk)

	body := blockBodyBytes(t, blk)
	hash := *blk.Hash()
	header := &blk.MsgBlock().Header
	n := sinkPayloadLen(body)
	owner := owingPeer(t, sm, hash, 248)

	primary, _, _ := connectRacePeer(t, 249, 1000)
	data1, _, _ := connectRacePeer(t, 250, 1000)
	assoc := peerpkg.NewAssociation([]byte{0x07, 0x08, 0x09}, primary)
	primary.SetAssociation(assoc)
	require.True(t, assoc.AddStream(wire.StreamTypeData1, data1))
	data1.SetAssociation(assoc)
	data1.SetStreamType(wire.StreamTypeData1)

	slowR, slowW := io.Pipe()
	firstDone := make(chan sinkResult, 1)

	go func() {
		converted, err := sm.pipelineBlockSink(hash, header, peerpkg.NewDeliveryReader(slowR, data1), n)
		firstDone <- sinkResult{converted, err}
	}()

	_, err := slowW.Write(body[:midTxOffset(t, blk, 20)])
	require.NoError(t, err)
	require.Eventually(t, func() bool { return sm.conversionOf(hash) != nil }, 5*time.Second, 10*time.Millisecond)

	first := sm.conversionOf(hash)
	require.Equal(t, data1, first.sender, "the primary does not owe the block, so the sender is the sub-peer")
	require.Eventually(t, first.reading, 5*time.Second, 10*time.Millisecond)

	secondDone := make(chan sinkResult, 1)

	go func() {
		converted, err := sm.raceDuplicateCopy(hash, header, peerpkg.NewDeliveryReader(bytes.NewReader(body), owner), n, sm.pipelineBlockSink)
		secondDone <- sinkResult{converted, err}
	}()

	require.Eventually(t, first.yielding, 5*time.Second, 10*time.Millisecond, "the second copy takes over")

	timer <- time.Now()

	require.True(t, disconnectsWithin(primary, 5*time.Second), "the association's primary is disconnected")
	require.True(t, disconnectsWithin(data1, 5*time.Second), "and the sub-peer with it")
	require.True(t, owner.Connected(), "the peer of the complete copy is kept")

	_ = slowW.CloseWithError(io.ErrClosedPipe)

	got := <-secondDone
	require.NoError(t, got.err)
	require.True(t, got.converted)

	got = <-firstDone
	require.False(t, got.converted)
}

// A converting copy whose bytes are badly encoded is judged even when a takeover happened during
// the read that delivered them. The bad encoding is the peer's, and the complete copy proved the
// block's body, so the verdict is on the peer alone: the read loop drops its association and bans
// nobody (an encoding fault carries no ErrBlockBodyMismatch), and a delivery that converted
// nothing deletes nothing (pipelineBlockDelete). It used to be drained as a duplicate and the
// peer was not judged. The complete copy still converts.
func TestAnInvalidEncodingDuringATakeoverIsJudged(t *testing.T) {
	ctx := context.Background()
	store := memory.New()
	sm := newPipelineParkManager(t, store, 8)
	sm.blockPark.dir = t.TempDir()
	sm.takeoverAfter = func(time.Duration) <-chan time.Time { return nil }

	blk := wireBlockWithTxs(t, 40, false)
	pipelineHeaderFixture(t, sm, blk)
	proveBlockOrigin(t, sm, blk)

	body := blockBodyBytes(t, blk)
	hash := *blk.Hash()
	header := &blk.MsgBlock().Header
	n := sinkPayloadLen(body)
	slowPeer := owingPeer(t, sm, hash, 251)
	owner := owingPeer(t, sm, hash, 252)

	slowR, slowW := io.Pipe()
	firstDone := make(chan sinkResult, 1)

	go func() {
		converted, err := sm.pipelineBlockSink(hash, header, peerpkg.NewDeliveryReader(slowR, slowPeer), n)
		firstDone <- sinkResult{converted, err}
	}()

	// Every byte up to transaction 20, which the converting copy then waits for.
	start := midTxOffset(t, blk, 20) - blk.Transactions()[20].MsgTx().SerializeSize()/2
	_, err := slowW.Write(body[:start])
	require.NoError(t, err)
	require.Eventually(t, func() bool { return sm.conversionOf(hash) != nil }, 5*time.Second, 10*time.Millisecond)

	first := sm.conversionOf(hash)
	require.Eventually(t, first.reading, 5*time.Second, 10*time.Millisecond, "the slow copy waits in a read")

	secondDone := make(chan sinkResult, 1)

	go func() {
		converted, err := sm.raceDuplicateCopy(hash, header, peerpkg.NewDeliveryReader(bytes.NewReader(body), owner), n, sm.pipelineBlockSink)
		secondDone <- sinkResult{converted, err}
	}()

	require.Eventually(t, first.yielding, 5*time.Second, 10*time.Millisecond, "the second copy takes over")

	// The read already waiting gets transaction 20 as the peer encoded it: version 1, one input,
	// its outpoint, and a script length no transaction can carry.
	bad := bytes.Join([][]byte{
		{0x01, 0x00, 0x00, 0x00, 0x01},
		make([]byte, 36),
		{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff},
	}, nil)

	go func() {
		_, _ = slowW.Write(bad)
		_ = slowW.Close()
	}()

	got := <-firstDone
	require.Error(t, got.err, "the bad encoding is judged, not drained")
	require.True(t, errors.Is(got.err, errors.ErrBlockInvalid), "an invalid block verdict, which the read loop reads as the peer's fault: %v", got.err)
	require.False(t, errors.Is(got.err, errors.ErrBlockBodyMismatch), "an encoding fault is not grounds for a ban")
	require.False(t, got.converted)

	got = <-secondDone
	require.NoError(t, got.err)
	require.True(t, got.converted, "the complete copy converted")

	_, err = sm.blockPark.ReadConverted(ctx, hash)
	require.NoError(t, err)
	requireNoSideFiles(t, sm.blockPark.dir)
}
