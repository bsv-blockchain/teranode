package svp2p

import (
	"context"
	"fmt"
	"net/url"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/svp2p/protocol"
	"github.com/bsv-blockchain/teranode/services/svp2p/svp2ptest"
	"github.com/bsv-blockchain/teranode/services/validator"
	"github.com/bsv-blockchain/teranode/settings"
	blobmemory "github.com/bsv-blockchain/teranode/stores/blob/memory"
	blockchain_store "github.com/bsv-blockchain/teranode/stores/blockchain"
	utxosql "github.com/bsv-blockchain/teranode/stores/utxo/sql"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// Loggers
// ---------------------------------------------------------------------------

type syncHarness struct {
	logger          *svp2ptest.RecordingLogger
	blockchainStore blockchain_store.Store
	server          *Server
}

// syncHarnessCounter keeps each harness's settings context unique.
// util.GetListener caches gRPC listeners by (context, service name), so two
// harnesses sharing a context would share a listener.
var syncHarnessCounter atomic.Uint64

// newSyncHarness builds a node. tweaks run against the settings before anything
// is constructed from them, which is how a leg shrinks a production window — the
// download timeout, the slow-fetch fuse — to something a test can wait out. They
// are the same dials an operator has.
func newSyncHarness(t *testing.T, name string, connectPeers []string, maxLastBlockTime time.Duration,
	tweaks ...func(*settings.Settings),
) *syncHarness {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())

	logger := &svp2ptest.RecordingLogger{}

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.Context = fmt.Sprintf("%s-svp2psync-%s-%d", tSettings.Context, name, syncHarnessCounter.Add(1))
	tSettings.Legacy.ListenAddresses = []string{"127.0.0.1:0"}
	tSettings.Legacy.GRPCListenAddress = svp2ptest.FreePort(t)
	tSettings.Legacy.WorkingDir = t.TempDir()
	tSettings.Legacy.ConnectPeers = connectPeers
	tSettings.GRPCAdminAPIKey = "test-admin-key"
	tSettings.BlockAssembly.GRPCListenAddress = svp2ptest.FreePort(t)
	tSettings.BlockAssembly.GRPCAddress = tSettings.BlockAssembly.GRPCListenAddress
	tSettings.SubtreeValidation.GRPCListenAddress = svp2ptest.FreePort(t)
	tSettings.SubtreeValidation.GRPCAddress = tSettings.SubtreeValidation.GRPCListenAddress
	tSettings.BlockValidation.GRPCListenAddress = svp2ptest.FreePort(t)
	tSettings.BlockValidation.GRPCAddress = tSettings.BlockValidation.GRPCListenAddress

	// blockchain.LocalClient answers SetBlockSubtreesSet straight from the
	// store and publishes no BlockSubtreesSet notification, which is what
	// normally drives block validation's setMined worker. Its periodic
	// mined-not-set sweep is the other, equally real, trigger, so the sweep
	// interval is shortened rather than the mined flag being written by hand:
	// the bridge's waitForPreviousBlockMined is a genuine gate on this path and
	// it must be a real setMined that opens it.
	tSettings.BlockValidation.PeriodicProcessingInterval = 200 * time.Millisecond

	for _, tweak := range tweaks {
		tweak(tSettings)
	}

	blockchainStore, err := blockchain_store.NewStore(logger, &url.URL{Scheme: "sqlitememory"}, tSettings)
	require.NoError(t, err)

	blockchainClient, err := blockchain.NewLocalClient(logger, tSettings, blockchainStore, nil, nil)
	require.NoError(t, err)

	utxoStoreURL, err := url.Parse("sqlitememory:///svp2p")
	require.NoError(t, err)

	tSettings.UtxoStore.UtxoStore = utxoStoreURL

	utxoStore, err := utxosql.New(ctx, logger, tSettings, utxoStoreURL)
	require.NoError(t, err)

	subtreeStore := blobmemory.New()
	tempStore := blobmemory.New()
	txStore := blobmemory.New()

	// The three Teranode services the ingestion path reaches over gRPC run
	// here for real, in process, on their own loopback ports — the same
	// constructors and clients daemon_services.go uses. Nothing about the
	// pipeline is stubbed: what is removed is the network between processes.
	blockAssemblyClient := svp2ptest.StartBlockAssembly(ctx, t, logger, tSettings, txStore, subtreeStore, utxoStore, blockchainClient)

	validatorClient, err := validator.New(ctx, logger, tSettings, utxoStore, nil, nil, nil, blockAssemblyClient, blockchainClient)
	require.NoError(t, err)

	subtreeValidationClient := svp2ptest.StartSubtreeValidation(ctx, t, tSettings.Context, logger, tSettings, subtreeStore,
		txStore, utxoStore, validatorClient, blockchainClient)

	blockValidationClient := svp2ptest.StartBlockValidation(ctx, t, tSettings.Context, logger, tSettings, subtreeStore, txStore,
		utxoStore, validatorClient, blockchainClient, blockAssemblyClient)

	server := NewWithDeps(logger, tSettings, blockchainClient, Deps{
		ValidationClient:  validatorClient,
		SubtreeStore:      subtreeStore,
		TempStore:         tempStore,
		UtxoStore:         utxoStore,
		SubtreeValidation: subtreeValidationClient,
		BlockValidation:   blockValidationClient,
		BlockAssembly:     blockAssemblyClient,
	})

	server.syncTick = 100 * time.Millisecond
	server.maxLastBlockTime = maxLastBlockTime

	// Registered last so it runs FIRST on teardown: every service above was
	// started against this context and its Stop only returns once the context
	// is cancelled.
	t.Cleanup(cancel)

	return &syncHarness{
		logger:          logger,
		blockchainStore: blockchainStore,
		server:          server,
	}
}

// start brings the svp2p service up and registers its teardown.
func (h *syncHarness) start(t *testing.T) {
	t.Helper()

	cancel := startServer(t, h.server)

	t.Cleanup(func() {
		cancel()
		_ = h.server.Stop(context.Background())
	})
}

// waitForHeight polls the node's own blockchain store until it reaches height,
// and dumps the node's log before failing so the reason is visible.
func (h *syncHarness) waitForHeight(t *testing.T, height uint32, timeout time.Duration, what string) {
	t.Helper()

	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		if _, got := h.bestBlock(t); got == height {
			return
		}

		time.Sleep(100 * time.Millisecond)
	}

	_, got := h.bestBlock(t)

	h.logger.Dump(t)
	t.Fatalf("%s: the blockchain store reached height %d, wanted %d", what, got, height)
}

// waitFor polls cond and dumps the node's log before failing.
func (h *syncHarness) waitFor(t *testing.T, cond func() bool, timeout time.Duration, what string) {
	t.Helper()

	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		if cond() {
			return
		}

		time.Sleep(100 * time.Millisecond)
	}

	h.logger.Dump(t)
	t.Fatal(what)
}

// bestBlock reads the node's own blockchain store, which is the exit-criterion
// witness: nothing but a completed ingest through ProcessBlock puts a block there.
//
// A store fault is reported rather than swallowed, so it cannot present later
// as a plain height or hash mismatch.
func (h *syncHarness) bestBlock(t *testing.T) (chainhash.Hash, uint32) {
	t.Helper()

	header, meta, err := h.blockchainStore.GetBestBlockHeader(context.Background())
	if err != nil {
		t.Logf("blockchain store GetBestBlockHeader failed: %v", err)

		return chainhash.Hash{}, 0
	}

	return *header.Hash(), meta.Height
}

// startBlockAssembly runs the real block assembly service in-process on its own
// gRPC port. Deps.BlockAssembly is a *blockassembly.Client, so the ingestion
// path's WaitForBlockAssemblyReady gate can only be satisfied honestly by a
// service that actually answers GetBlockAssemblyState.
// ---------------------------------------------------------------------------

const syncTestChainLength = 20

// TestIntegrationHeadersFirstSyncFromScriptedPeer is the in-repo proxy for the
// phase's "syncs testnet end to end" exit: a node that starts at genesis pulls
// a whole chain from one peer through headers-first sync, block download,
// streaming ingest and ProcessBlock, and ends with that chain in its own
// blockchain store.
func TestIntegrationHeadersFirstSyncFromScriptedPeer(t *testing.T) {
	require.Greater(t, syncTestChainLength, protocol.MaxBlocksInTransitPerPeer,
		"the chain must be longer than one getdata batch, or the second scheduling round is never exercised")

	tSettings := test.CreateBaseTestSettings(t)
	chain := svp2ptest.BuildFixtureChain(t, tSettings, syncTestChainLength)

	peer := svp2ptest.NewScriptedPeer(t, chain, tSettings.ChainCfgParams.Net, svp2ptest.Script{}, true)

	h := newSyncHarness(t, "happy", []string{peer.Addr}, 0)
	h.start(t)

	h.waitForHeight(t, uint32(syncTestChainLength), 60*time.Second, "headers-first sync")

	hash, height := h.bestBlock(t)
	require.Equal(t, uint32(syncTestChainLength), height)
	require.Equal(t, chain.Tip(), hash, "the node's best block must be the scripted chain's tip")
}

// TestIntegrationSyncPeerRotationRecoversFromAStalledPeer covers the
// adversarial leg: the serving peer answers headers and half the blocks, then
// stops answering getdata while staying connected.
//
// The sync-peer rotation (blockdownload.go CheckStall ->
// StallActionRotateSyncPeer) is what catches that peer here, and the leg is
// arranged so that it is the only thing that can. DetectStalling's
// nStallingSince clock never starts, because nothing is ever in flight at the
// download window's edge. The parallel fetch has nobody to race to while the
// replacement peer is still down. The per-block download timeout would take ten
// minutes, far outside this leg's budget. Each of those three is asserted below
// rather than left to inference.
func TestIntegrationSyncPeerRotationRecoversFromAStalledPeer(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)
	chain := svp2ptest.BuildFixtureChain(t, tSettings, syncTestChainLength)

	stalled := svp2ptest.NewScriptedPeer(t, chain, tSettings.ChainCfgParams.Net, svp2ptest.ServeLimit(syncTestChainLength/2), true)
	replacement := svp2ptest.NewScriptedPeer(t, chain, tSettings.ChainCfgParams.Net, svp2ptest.Script{}, false)

	h := newSyncHarness(t, "stall", []string{stalled.Addr, replacement.Addr}, 3*time.Second)
	h.start(t)

	// The stalling peer delivers its half first, and then nothing.
	h.waitFor(t, func() bool { return stalled.ServedBlocks() == syncTestChainLength/2 },
		60*time.Second, "the stalling peer never delivered its half of the chain")

	h.waitFor(t, func() bool {
		_, height := h.bestBlock(t)
		return height == uint32(syncTestChainLength/2)
	}, 60*time.Second, "the half the stalling peer did deliver never reached the blockchain store")

	// The rotation is the mechanism under test. manager.go syncTickOnce formats
	// exactly this line, naming the rotated peer, and only when
	// BlockDownloader.CheckStall returns StallActionRotateSyncPeer. Matching the
	// whole formatted line rather than two loose substrings is what keeps the
	// other rotation log (the pre-admission timeout in BlockDone) and any
	// unrelated peer-teardown line from satisfying it between them.
	wantRotation := fmt.Sprintf("rotating the sync peer %s: no sync progress", stalled.Addr)

	h.waitFor(t, func() bool { return h.logger.Contains(wantRotation) },
		60*time.Second, "the sync peer was never rotated for making no progress")

	// The rotation releases the sync slot and the peer's downloads without
	// disconnecting it. DetectStalling's disconnect never fires here: with the
	// whole remaining chain inside the download window, no peer is ever held at
	// the window's edge, so nStallingSince never starts.
	require.Equal(t, int32(1), h.server.manager.ConnectedCount(),
		"a rotation must leave the rotated peer connected")
	noStallDisconnect(t, h, "the stall must be caught by the rotation, not by a DetectStalling disconnect")

	// Nor is it the parallel fetch, which after Task 6b is the mechanism that
	// normally reaches a peer sitting on a block first — its fuse is 30 seconds
	// against the rotation's window, shrunk to 3 here. It cannot fire in this
	// leg for a structural reason rather than a lucky one: the replacement peer
	// is not listening yet, so the stalling peer is the ONLY holder available
	// and there is nobody to race to. That is exactly the case the rotation and
	// the download timeout are the fallbacks for, and this leg is where the
	// rotation half of it is covered.
	require.Zero(t, replacement.RequestedCount(),
		"the replacement is not up yet, so no block can have been raced to it")

	// The stalling peer then goes away, which is what releases the blocks it
	// re-claimed while it was still the only candidate on offer.
	stalled.Close()

	replacement.Listen()

	// The budget below has to cover the dial loop reaching a peer whose port was
	// closed until now. That backoff starts at dialRetryBase (5 s) and DOUBLES
	// per failed attempt with no reset until a connection completes, so the wait
	// the replacement needs grows with how long the earlier waits above took.
	// Two minutes covers roughly five failed attempts; if the earlier waits ever
	// start running long, raise this budget rather than trimming them.
	h.waitForHeight(t, uint32(syncTestChainLength), 120*time.Second, "sync after the replacement peer connected")

	hash, _ := h.bestBlock(t)
	require.Equal(t, chain.Tip(), hash)
	require.Positive(t, replacement.ServedBlocks(), "the replacement peer must have served the rest of the chain")
}

// twoPeerChainLength is longer than one getdata batch on purpose: a peer may
// hold at most MaxBlocksInTransitPerPeer blocks at a time, so a chain of more
// than that CANNOT be taken by one peer in a single pass. That is what makes the
// distribution below a property of the scheduler rather than a race between two
// goroutines.
const twoPeerChainLength = 2*protocol.MaxBlocksInTransitPerPeer + 4

// TestIntegrationBlockDownloadSpreadsAcrossTwoPeers is the multi-peer leg:
// headers from one sync peer, blocks from several, which is the model Phase 3
// exists to deliver. Both peers serve everything, so nothing here is adversarial
// — the claim is only that the download window is offered to every useful peer
// and that the chain completes.
//
// The slow-fetch fuse is pushed out of reach so that parallel FETCH cannot be
// what puts work on the second peer. Whatever both peers are asked for here,
// they are asked for because the walk distributed the window, not because a
// stalled block was raced.
func TestIntegrationBlockDownloadSpreadsAcrossTwoPeers(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)
	chain := svp2ptest.BuildFixtureChain(t, tSettings, twoPeerChainLength)

	first := svp2ptest.NewScriptedPeer(t, chain, tSettings.ChainCfgParams.Net, svp2ptest.Script{}, true)
	second := svp2ptest.NewScriptedPeer(t, chain, tSettings.ChainCfgParams.Net, svp2ptest.Script{}, true)

	h := newSyncHarness(t, "spread", []string{first.Addr, second.Addr}, 0, func(s *settings.Settings) {
		s.Legacy.BlockDownloadSlowFetchTimeout = time.Hour
	})
	h.start(t)

	h.waitForHeight(t, uint32(twoPeerChainLength), 120*time.Second, "two-peer block download")

	hash, height := h.bestBlock(t)
	require.Equal(t, uint32(twoPeerChainLength), height)
	require.Equal(t, chain.Tip(), hash)

	require.Positive(t, first.RequestedCount(), "the first peer must have been asked for blocks")
	require.Positive(t, second.RequestedCount(), "the second peer must have been asked for blocks")
	require.Positive(t, first.ServedBlocks())
	require.Positive(t, second.ServedBlocks())

	// Every block of the chain was fetched from one peer or the other, and both
	// contributed. That union is the distribution claim; which peer got which
	// slice is the scheduler's business and not fixed.
	fetched := make(map[chainhash.Hash]struct{}, twoPeerChainLength)

	for _, header := range chain.Headers {
		hash := header.BlockHash()

		if first.Requested(hash) || second.Requested(hash) {
			fetched[hash] = struct{}{}
		}
	}

	require.Len(t, fetched, twoPeerChainLength, "every block must have been requested from some peer")

	// The serve total is bounded, which it was not before Task 21. Multi-peer
	// download means blocks arrive out of order, and a block whose parent is not
	// in our chain yet is refused before admission (bridge PreAdmit) and
	// released. Nothing held it back, and re-requests are tick-driven, so it was
	// fetched again on the next tick and every tick after until the parent
	// landed: 526 serves for this 36-block chain, about fourteen times over,
	// with 476 pre-admit refusals against 36 real ingests.
	//
	// The scheduler now defers such a block until its parent is held, so each
	// one is fetched at most twice: once too early, and once when the chain is
	// ready for it. Measured here after the fix: 55 to 59, so the bound below
	// has room without being vacuous — it would have failed nine times over on
	// the old behavior.
	//
	// Closing the remaining gap means keeping the early block's bytes instead of
	// discarding them, which is orphan-block retention and a different piece of
	// work. On mainnet these are gigabyte blocks, so it is worth having: the
	// residual is one wasted transfer per out-of-order arrival.
	served := first.ServedBlocks() + second.ServedBlocks()

	t.Logf("block serves for a %d-block chain: first=%d second=%d total=%d",
		twoPeerChainLength, first.ServedBlocks(), second.ServedBlocks(), served)

	require.LessOrEqual(t, served, 2*twoPeerChainLength,
		"a parent-missing block must be re-fetched once at most, not once per tick")
	require.GreaterOrEqual(t, served, twoPeerChainLength,
		"every block has to cross the wire at least once")
}

// raceChainLength is short on purpose. The walk considers only the FIRST
// already-in-flight block it meets, so it races at most one block per pass, and
// a shorter chain keeps the leg quick without weakening it.
const raceChainLength = 6

// TestIntegrationRacesABlockAwayFromASilentPeer is the adversarial leg the
// parallel fetch exists for: a peer accepts a getdata and then never sends the
// block.
//
// Before Task 6b the only answers to that were the staller disconnect, which
// needs the whole download window drained first, and the per-block timeout,
// which needs ten minutes and throws the connection away with it. The race needs
// neither: after the slow-fetch fuse the block is simply asked of somebody else,
// and the silent peer keeps its connection and its other work.
//
// The silent peer is brought up ALONE so that it is certainly the peer holding
// the chain when the second one arrives. Which peer wins the sync slot is not
// deterministic when both are connected from the start, and this leg needs the
// blocks parked on the peer that will not serve them.
func TestIntegrationRacesABlockAwayFromASilentPeer(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)
	chain := svp2ptest.BuildFixtureChain(t, tSettings, raceChainLength)

	// serveLimit 0: it answers version, ping and getheaders, and never a block.
	silent := svp2ptest.NewScriptedPeer(t, chain, tSettings.ChainCfgParams.Net, svp2ptest.ServeLimit(0), true)
	rescuer := svp2ptest.NewScriptedPeer(t, chain, tSettings.ChainCfgParams.Net, svp2ptest.Script{}, false)

	h := newSyncHarness(t, "race", []string{silent.Addr, rescuer.Addr}, 0, func(s *settings.Settings) {
		// The fuse, shrunk from 30 seconds so the leg does not have to wait one
		// out. Everything else keeps its production window, which is the point:
		// the rotation (180 s) and the download timeout (10 minutes) are both
		// out of reach inside this test, so the race is the only mechanism that
		// can finish the sync.
		s.Legacy.BlockDownloadSlowFetchTimeout = 500 * time.Millisecond
	})
	h.start(t)

	// The silent peer takes the chain and sits on it.
	h.waitFor(t, func() bool { return silent.RequestedCount() > 0 },
		60*time.Second, "the silent peer was never asked for a block")

	require.Zero(t, silent.ServedBlocks(), "this peer answers headers and no blocks")

	firstBlock := chain.Headers[0].BlockHash()

	h.waitFor(t, func() bool { return silent.Requested(firstBlock) },
		60*time.Second, "the head of the chain was never requested from the silent peer")

	// Only now does anyone else exist to race to.
	rescuer.Listen()

	h.waitFor(t, func() bool { return h.server.manager.ConnectedCount() == 2 },
		120*time.Second, "the second peer never connected")

	h.waitForHeight(t, uint32(raceChainLength), 120*time.Second, "sync through raced block downloads")

	hash, _ := h.bestBlock(t)
	require.Equal(t, chain.Tip(), hash)

	// THE RACE ITSELF: a block the silent peer was asked for, and never served,
	// was asked of the other peer too.
	require.True(t, rescuer.Requested(firstBlock),
		"the block the silent peer sat on must be raced to the peer that can serve it")
	require.Positive(t, rescuer.ServedBlocks())
	require.Zero(t, silent.ServedBlocks(), "the silent peer served nothing at any point")

	// What the race did NOT need. The silent peer is still connected: no
	// disconnect, no rotation, no partial download thrown away but its own.
	require.Equal(t, int32(2), h.server.manager.ConnectedCount(),
		"a race must not cost the slow peer its connection")
	noStallDisconnect(t, h, "the recovery here is the race, not the stall or timeout disconnect")
}

// TestIntegrationBlocksTravelOnData1 is the phase-4 exit for block delivery: a
// node that negotiates BlockPriority with its peer and opens DATA1
// (protocol/streams.go setupStreams, Task 9) must actually pull its blocks
// down that stream, not merely open it. The scripted peer here acks the
// node's createstream and, from then on, answers every getdata's block
// replies on the connection that ack went out on
// (svp2ptest.ScriptedPeer.writeGetDataReply) — the same routing SVNode's own
// BlockPriority policy performs (stream_policy.cpp:187-195).
func TestIntegrationBlocksTravelOnData1(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)
	chain := svp2ptest.BuildFixtureChain(t, tSettings, syncTestChainLength)

	require.True(t, tSettings.Legacy.AllowBlockPriority, "legacy_allowBlockPriority must default on for DATA1 to be requested")

	peer := svp2ptest.NewScriptedPeer(t, chain, tSettings.ChainCfgParams.Net, svp2ptest.Script{}, true)

	h := newSyncHarness(t, "data1", []string{peer.Addr}, 0)
	h.start(t)

	h.waitForHeight(t, uint32(syncTestChainLength), 60*time.Second, "sync over DATA1")

	hash, height := h.bestBlock(t)
	require.Equal(t, uint32(syncTestChainLength), height)
	require.Equal(t, chain.Tip(), hash)

	ack, ok := peer.Transcript.FirstOn(svp2ptest.Out, "streamack")
	require.True(t, ok, "the peer must have acked the node's createstream")

	conns := peer.Conns()
	require.Len(t, conns, 2, "the node must open exactly two connections to this peer: GENERAL then DATA1")

	generalConn, data1Conn := conns[0], conns[1]

	// Blocks may legitimately have been served on GENERAL before DATA1
	// attached (the association's own handshake is not instantaneous), so
	// only entries after the streamack count against GENERAL. Ordering is
	// compared by Seq, the entry's position in transcript order, rather than
	// by wall-clock time: a coarse clock can tie two entries and turn a real
	// ordering violation into a false negative under At.After.
	blocksOnGeneralAfterAck := 0
	blocksOnData1 := 0

	for _, e := range peer.Transcript.Snapshot() {
		if e.Dir != svp2ptest.Out || e.Cmd != "block" {
			continue
		}

		switch e.Conn {
		case data1Conn:
			blocksOnData1++
		case generalConn:
			// Judged by the REQUEST's position: a getdata read before the ack
			// may still be answered on GENERAL after it (the serve goroutine
			// decided the target before DATA1 existed). Only a block whose
			// getdata arrived after the ack is a routing violation.
			if e.Seq > ack.Seq && e.ReplyTo > ack.Seq {
				blocksOnGeneralAfterAck++
			}
		}
	}

	require.Positive(t, blocksOnData1, "at least one block must have travelled on the DATA1 connection")
	require.Zero(t, blocksOnGeneralAfterAck, "no block may travel on GENERAL once DATA1 has attached")
}

// noStallDisconnect asserts that NEITHER DetectStalling clause disconnected
// anyone. Both reasons have to be named: Task 25 split the single shared log
// line "stalling block download" into one text per rule, so a check for either
// text alone would quietly stop covering the other. The strings come from the
// production accessor rather than being copied here, so they cannot drift.
func noStallDisconnect(t *testing.T, h *syncHarness, msg string) {
	t.Helper()

	require.False(t, h.logger.Contains(protocol.StallActionDisconnect.DisconnectReason()), msg)
	require.False(t, h.logger.Contains(protocol.StallActionDisconnectTimeout.DisconnectReason()), msg)
}

// TestIntegrationDownloadTimeoutDisconnectsASilentSolePeer covers what the race
// CANNOT do, and is therefore the leg that keeps Task 6's timeout honest: with
// one useful peer there is nobody to race to, and the front block's own clock is
// the only thing left.
//
// The disconnect here is unambiguously the timeout rather than the staller rule.
// nStallingSince is only ever started by ANOTHER peer's empty batch naming this
// one — SendGetDataBlocks excludes the walking peer from being its own staller —
// so with a single peer that clock can never start.
func TestIntegrationDownloadTimeoutDisconnectsASilentSolePeer(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)
	chain := svp2ptest.BuildFixtureChain(t, tSettings, raceChainLength)

	silent := svp2ptest.NewScriptedPeer(t, chain, tSettings.ChainCfgParams.Net, svp2ptest.ServeLimit(0), true)

	h := newSyncHarness(t, "timeout", []string{silent.Addr}, 0, func(s *settings.Settings) {
		// One percent of the ten minute block interval is six seconds. This is
		// the operator's dial, used here to make a ten minute rule testable
		// rather than to change what is being tested.
		s.Legacy.BlockDownloadTimeoutBasePercent = 1
		s.Legacy.BlockDownloadTimeoutBaseIBDPercent = 1
	})
	h.start(t)

	h.waitFor(t, func() bool { return silent.RequestedCount() > 0 },
		60*time.Second, "the silent peer was never asked for a block")

	// The rule is named in the log now, so this pins the TIMEOUT clause
	// specifically rather than the shared text both clauses used to share —
	// which is what the test's own preamble above always claimed to be
	// asserting (Task 25, phase-3 ledger residual "Distinguishing the two
	// disconnect logs").
	want := fmt.Sprintf("disconnecting %s: %s", silent.Addr, protocol.StallActionDisconnectTimeout.DisconnectReason())

	h.waitFor(t, func() bool { return h.logger.Contains(want) },
		60*time.Second, "the silent peer was never disconnected by the download timeout")

	require.Zero(t, silent.ServedBlocks())
	require.False(t, h.logger.Contains("rotating the sync peer"),
		"the rotation window is 180 seconds and must not be what fired here")
}
