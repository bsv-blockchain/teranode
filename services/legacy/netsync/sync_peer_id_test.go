package netsync

import (
	"testing"
	"time"

	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/stretchr/testify/require"
)

// TestSyncPeerID_DoesNotDependOnBlockHandler is a regression test. SyncPeerID
// used to post getSyncPeerMsg to msgChan and wait on an unbuffered reply, and
// blockHandler was the only responder. A caller that raced Stop — or ran before
// Start — therefore blocked for ever, parking the calling goroutine for the life
// of the process.
//
// The manager here is deliberately never started, so no blockHandler exists.
// That is the condition the old implementation could not survive.
func TestSyncPeerID_DoesNotDependOnBlockHandler(t *testing.T) {
	ctx := &testContext{}
	require.NoError(t, ctx.Setup(t, &testConfig{
		dbName:      "TestSyncPeerIDNoBlockHandler",
		chainParams: &chaincfg.MainNetParams,
	}))

	defer ctx.Teardown()

	done := make(chan int32, 1)
	go func() { done <- ctx.syncManager.SyncPeerID() }()

	select {
	case id := <-done:
		require.Zero(t, id, "no sync peer has been selected")
	case <-time.After(5 * time.Second):
		t.Fatal("SyncPeerID blocked: it must not depend on the block handler running")
	}
}

// TestSyncPeerID_SurvivesStop covers the shutdown race directly: the block
// handler has exited, so nothing is left to answer a message-channel round trip.
func TestSyncPeerID_SurvivesStop(t *testing.T) {
	ctx := &testContext{}
	require.NoError(t, ctx.Setup(t, &testConfig{
		dbName:      "TestSyncPeerIDAfterStop",
		chainParams: &chaincfg.MainNetParams,
	}))

	defer ctx.Teardown()

	ctx.syncManager.Start()
	ctx.syncManager.Stop()

	done := make(chan int32, 1)
	go func() { done <- ctx.syncManager.SyncPeerID() }()

	select {
	case id := <-done:
		require.Zero(t, id)
	case <-time.After(5 * time.Second):
		t.Fatal("SyncPeerID blocked after Stop")
	}
}
