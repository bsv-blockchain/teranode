package smoke

import (
	"context"
	"encoding/json"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/daemon"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/test"
	helper "github.com/bsv-blockchain/teranode/test/utils"
	"github.com/bsv-blockchain/teranode/test/utils/svnode"
	"github.com/stretchr/testify/require"
)

const (
	// teranodeLegacyListenAddr is the address teranode's legacy P2P listener binds to
	teranodeLegacyListenAddr = "0.0.0.0:18444"
	// teranodeLegacyConnectAddr is the address svnode uses to connect to teranode's legacy listener
	teranodeLegacyConnectAddr = "127.0.0.1:18444"

	errAddTeranodePeer = "Failed to add teranode as peer on svnode"
	errSVNodeConnect   = "SVNode failed to connect to teranode"
	errStartSVNode     = "Failed to start svnode"
)

var legacySyncTestLock sync.Mutex

// newSVNode creates an SVNode using Docker via testcontainers
func newSVNode() svnode.SVNodeI {
	options := svnode.DefaultOptions()
	return svnode.New(options)
}

// waitForOutboundPeer waits for svnode to have at least one outbound peer.
// Bitcoin SV only downloads blocks from outbound connections, so this is
// necessary to confirm the AddNode connection is established (not just an
// inbound connection from ConnectPeers).
func waitForOutboundPeer(ctx context.Context, sv svnode.SVNodeI, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		if time.Now().After(deadline) {
			peers, _ := sv.GetPeerInfo()
			return errors.NewProcessingError("timeout waiting for outbound peer on svnode, peers: %d", len(peers))
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		if hasOutboundPeer(sv) {
			return nil
		}
		time.Sleep(500 * time.Millisecond)
	}
}

// waitForLegacyListener probes the legacy P2P port until it accepts TCP connections.
// This replaces a fixed sleep and avoids the race where svnode connects before the
// legacy listener's Accept() loop has started.
func waitForLegacyListener(t *testing.T, addr string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", addr, 500*time.Millisecond)
		if err == nil {
			_ = conn.Close()
			return
		}
		time.Sleep(250 * time.Millisecond)
	}
	t.Fatalf("legacy listener at %s not ready within %s", addr, timeout)
}

func hasOutboundPeer(sv svnode.SVNodeI) bool {
	peers, err := sv.GetPeerInfo()
	if err != nil {
		return false
	}
	for _, peer := range peers {
		if inbound, ok := peer["inbound"].(bool); ok && !inbound {
			return true
		}
	}
	return false
}

// TestLegacySync tests that teranode can sync blocks from svnode
//
// This test:
// 1. Starts svnode in Docker
// 2. Generates blocks on svnode
// 3. Starts teranode with legacy enabled, connecting to svnode
// 4. Verifies teranode catches up to svnode's block height
func TestLegacySync(t *testing.T) {
	legacySyncTestLock.Lock()
	defer legacySyncTestLock.Unlock()

	ctx := t.Context()

	// Start svnode in Docker
	sv := newSVNode()
	err := sv.Start(ctx)
	require.NoError(t, err, errStartSVNode)

	defer func() {
		_ = sv.Stop(context.Background())
	}()

	// Generate 101 blocks on svnode (enough for coinbase maturity)
	const targetHeight = 101
	_, err = sv.Generate(targetHeight)
	require.NoError(t, err, "Failed to generate blocks on svnode")

	// Verify svnode has blocks
	blockCount, err := sv.GetBlockCount()
	require.NoError(t, err)
	require.Equal(t, targetHeight, blockCount, "SVNode should have %d blocks", targetHeight)

	t.Logf("SVNode started with %d blocks", blockCount)

	// Start teranode with legacy enabled, connecting to svnode
	td := daemon.NewTestDaemon(t, daemon.TestOptions{
		EnableRPC:       true,
		EnableP2P:       true,
		EnableLegacy:    true,
		EnableValidator: true,
		SettingsOverrideFunc: test.ComposeSettings(
			test.SystemTestSettings(),
			func(settings *settings.Settings) {
				settings.Legacy.ConnectPeers = []string{sv.P2PHost()}
				settings.P2P.StaticPeers = []string{}
			},
		),
	})

	defer td.Stop(t)

	// Wait for teranode to sync to svnode's height
	err = helper.WaitForNodeBlockHeight(ctx, td.BlockchainClient, uint32(targetHeight), 60*time.Second)
	require.NoError(t, err, "Teranode failed to sync to svnode's block height")

	t.Logf("Teranode synced to height %d from svnode", targetHeight)

	// Verify peer connection via RPC
	resp, err := td.CallRPC(ctx, "getpeerinfo", []any{})
	require.NoError(t, err)

	var p2pResp helper.P2PRPCResponse
	err = json.Unmarshal([]byte(resp), &p2pResp)
	require.NoError(t, err)

	// Find peer connected to svnode's P2P port
	var legacyPeers []string
	for _, peer := range p2pResp.Result {
		if strings.Contains(peer.Addr, ":18333") {
			legacyPeers = append(legacyPeers, peer.Addr)
		}
	}
	require.GreaterOrEqual(t, len(legacyPeers), 1, "Teranode should be connected to svnode")

	t.Logf("Teranode connected to %d legacy peer(s)", len(legacyPeers))
}

// TestSVNodeSyncFromTeranode tests that svnode can sync blocks from teranode.
// This validates that blocks generated by teranode are valid according to legacy node consensus rules.
//
// This test:
// 1. Starts teranode without legacy, generates and persists blocks
// 2. Stops teranode, restarts with legacy listening on port 18444
// 3. Starts svnode with -connect flag pointing to teranode's legacy listener
// 4. Verifies svnode syncs teranode's blocks via IBD over the outbound connection
//
// Note: Bitcoin SV only downloads blocks from outbound connections (eclipse attack
// prevention). The -connect flag creates a real outbound connection (unlike addnode)
// that Bitcoin SV reliably uses for initial block download.
func TestSVNodeSyncFromTeranode(t *testing.T) {
	legacySyncTestLock.Lock()
	defer legacySyncTestLock.Unlock()

	ctx := t.Context()

	// Start teranode without legacy to generate and persist blocks
	td := daemon.NewTestDaemon(t, daemon.TestOptions{
		EnableRPC:            true,
		EnableValidator:      true,
		EnableBlockPersister: true,
		// PreserveDataDir:      true,
		SettingsOverrideFunc: test.ComposeSettings(
			test.SystemTestSettings(),
		),
	})

	err := td.BlockchainClient.Run(td.Ctx, "test")
	require.NoError(t, err, "failed to initialize blockchain")
	defer td.Stop(t)

	// Generate blocks on teranode
	const teranodeBlocks = 5
	const targetHeight = teranodeBlocks

	// Generate blocks with slight delay - svnode complains about timestamps otherwise
	var minedBlocks []*model.Block
	for i := 0; i < teranodeBlocks; i++ {
		time.Sleep(500 * time.Millisecond)
		block := td.MineAndWait(t, 1)
		minedBlocks = append(minedBlocks, block)
	}

	t.Logf("Generated %d blocks on teranode", teranodeBlocks)

	// Wait for all blocks to be persisted before restarting with legacy
	for i, block := range minedBlocks {
		err = td.WaitForBlockPersisted(block.Hash(), 30*time.Second)
		require.NoError(t, err, "Block %d was not persisted within timeout", i+1)
	}
	t.Log("All blocks persisted")

	td.Stop(t)
	td.ResetServiceManagerContext(t)

	// Restart teranode with legacy to serve blocks to svnode
	td = daemon.NewTestDaemon(t, daemon.TestOptions{
		EnableRPC:            true,
		EnableP2P:            true,
		EnableValidator:      true,
		EnableBlockPersister: true,
		EnableLegacy:         true,
		SkipRemoveDataDir:    true,
		SettingsOverrideFunc: test.ComposeSettings(
			test.SystemTestSettings(),
			func(s *settings.Settings) {
				s.Legacy.AllowSyncCandidateFromLocalPeers = true
				// s.Legacy.AdvertiseFullNode = true
				s.Legacy.ListenAddresses = []string{teranodeLegacyListenAddr}
				s.P2P.StaticPeers = []string{}
			},
		),
	})

	defer func() { td.Stop(t) }()

	td.WaitForBlockHeight(t, minedBlocks[len(minedBlocks)-1], 30*time.Second)
	waitForLegacyListener(t, teranodeLegacyConnectAddr, 10*time.Second)

	// Start svnode with -connect flag pointing to teranode. This uses Bitcoin SV's
	// standard IBD path where the connection is established during node startup.
	// Bitcoin SV 1.2.0 has an intermittent bug where it receives valid headers but
	// fails to request blocks. If the first attempt fails, restart both teranode
	// and svnode to get a completely fresh state.
	const maxAttempts = 3
	var sv svnode.SVNodeI

	for attempt := 1; attempt <= maxAttempts; attempt++ {
		opts := svnode.DefaultOptions()
		opts.ConnectTo = []string{teranodeLegacyConnectAddr}
		sv = svnode.New(opts)
		err = sv.Start(ctx)
		require.NoError(t, err, errStartSVNode)

		err = waitForOutboundPeer(ctx, sv, 15*time.Second)
		require.NoError(t, err, errSVNodeConnect)

		err = sv.WaitForBlockHeight(ctx, targetHeight, 30*time.Second)
		if err == nil {
			break
		}

		t.Logf("Sync attempt %d/%d failed: %v", attempt, maxAttempts, err)
		if logs, logErr := sv.GetLogs(ctx); logErr == nil {
			t.Logf("SVNode logs (attempt %d):\n%s", attempt, logs)
		}
		_ = sv.Stop(context.Background())

		if attempt < maxAttempts {
			// Restart teranode legacy service to get a completely fresh state
			td.Stop(t)
			td.ResetServiceManagerContext(t)
			td = daemon.NewTestDaemon(t, daemon.TestOptions{
				EnableRPC:            true,
				EnableP2P:            true,
				EnableValidator:      true,
				EnableBlockPersister: true,
				EnableLegacy:         true,
				SkipRemoveDataDir:    true,
				SettingsOverrideFunc: test.ComposeSettings(
					test.SystemTestSettings(),
					func(s *settings.Settings) {
						s.Legacy.AllowSyncCandidateFromLocalPeers = true
						// s.Legacy.AdvertiseFullNode = true
						s.Legacy.ListenAddresses = []string{teranodeLegacyListenAddr}
						s.P2P.StaticPeers = []string{}
					},
				),
			})
			td.WaitForBlockHeight(t, minedBlocks[len(minedBlocks)-1], 30*time.Second)
			waitForLegacyListener(t, teranodeLegacyConnectAddr, 10*time.Second)
		} else {
			require.NoError(t, err, "SVNode failed to sync from teranode after %d attempts", maxAttempts)
		}
	}

	defer func() {
		_ = sv.Stop(context.Background())
	}()

	t.Logf("SVNode synced to height %d from teranode - blocks validated by legacy consensus", targetHeight)
}

// TestBidirectionalSync tests bidirectional sync between teranode and svnode
// This validates that both nodes can generate blocks and the other will sync
//
// This test uses the persist pattern:
// 1. SVNode generates initial blocks
// 2. Teranode (with legacy) syncs from SVNode
// 3. Teranode restarts without legacy, generates blocks with persister
// 4. Teranode restarts with legacy, SVNode syncs teranode's blocks
// 5. SVNode generates more blocks, teranode syncs
func TestBidirectionalSync(t *testing.T) {
	legacySyncTestLock.Lock()
	defer legacySyncTestLock.Unlock()

	ctx := t.Context()

	// Start svnode in Docker
	sv := newSVNode()
	err := sv.Start(ctx)
	require.NoError(t, err, errStartSVNode)

	defer func() {
		_ = sv.Stop(context.Background())
	}()

	// Generate initial blocks on svnode
	const initialBlocks = 10
	_, err = sv.Generate(initialBlocks)
	require.NoError(t, err, "Failed to generate initial blocks on svnode")

	t.Logf("SVNode generated %d initial blocks", initialBlocks)

	// Phase 1: Start teranode WITH legacy to sync from SVNode
	td := daemon.NewTestDaemon(t, daemon.TestOptions{
		EnableRPC:            true,
		EnableP2P:            true,
		EnableLegacy:         true,
		EnableValidator:      true,
		EnableBlockPersister: true,
		SettingsOverrideFunc: test.ComposeSettings(
			test.SystemTestSettings(),
			func(s *settings.Settings) {
				s.Legacy.AllowSyncCandidateFromLocalPeers = true
				s.Legacy.ConnectPeers = []string{sv.P2PHost()}
				s.P2P.StaticPeers = []string{}
			},
		),
	})

	// Wait for teranode to sync initial blocks from svnode
	err = helper.WaitForNodeBlockHeight(ctx, td.BlockchainClient, uint32(initialBlocks), 30*time.Second)
	require.NoError(t, err, "Teranode failed to sync initial blocks")

	t.Log("Teranode synced initial blocks from SVNode")

	// wait for teranode to persist blocks
	// get block at each height and check for persistence
	for i := 1; i <= initialBlocks; i++ {
		block, err := td.BlockchainClient.GetBlockByHeight(ctx, uint32(i))
		require.NoError(t, err, "Failed to get block at height %d", i)
		require.NotNil(t, block, "Block at height %d should not be nil", i)
		err = td.WaitForBlockPersisted(block.Hash(), 30*time.Second)
		require.NoError(t, err, "Block %d was not persisted within timeout", i)
	}

	// // Stop teranode to restart without legacy for block generation
	td.Stop(t)
	td.ResetServiceManagerContext(t)

	// Phase 2: Restart teranode WITHOUT legacy to generate blocks with persister
	td = daemon.NewTestDaemon(t, daemon.TestOptions{
		EnableRPC:            true,
		EnableP2P:            true,
		EnableValidator:      true,
		EnableBlockPersister: true,
		SkipRemoveDataDir:    true,
		SettingsOverrideFunc: test.ComposeSettings(
			test.SystemTestSettings(),
		),
	})

	err = td.BlockchainClient.Run(td.Ctx, "test")
	require.NoError(t, err, "failed to initialize blockchain")

	// Generate blocks on teranode (without legacy connection)
	const teranodeBlocks = 5
	var teranodeMinedBlocks []*model.Block
	for i := 0; i < teranodeBlocks; i++ {
		time.Sleep(500 * time.Millisecond)
		block := td.MineAndWait(t, 1)
		teranodeMinedBlocks = append(teranodeMinedBlocks, block)
	}

	currentHeight := initialBlocks + teranodeBlocks
	t.Logf("Teranode generated %d more blocks, current height: %d", teranodeBlocks, currentHeight)

	// Wait for all blocks to be persisted before restarting with legacy
	for i, block := range teranodeMinedBlocks {
		err = td.WaitForBlockPersisted(block.Hash(), 30*time.Second)
		require.NoError(t, err, "Block %d was not persisted within timeout", i+1)
	}
	t.Log("All teranode blocks persisted")

	// // Stop teranode to restart with legacy
	td.Stop(t)
	td.ResetServiceManagerContext(t)

	// Stop svnode from earlier phases - we'll start fresh ones in Phase 3
	_ = sv.Stop(context.Background())

	// Phase 3: Restart teranode WITH legacy to serve blocks to svnode.
	// Use fresh svnode with -connect flag for standard IBD path.
	// Bitcoin SV 1.2.0 has an intermittent bug where it receives valid headers
	// but fails to request blocks. Restart both on failure.
	td = daemon.NewTestDaemon(t, daemon.TestOptions{
		EnableRPC:            true,
		EnableP2P:            true,
		EnableValidator:      true,
		EnableBlockPersister: true,
		EnableLegacy:         true,
		SkipRemoveDataDir:    true,
		SettingsOverrideFunc: test.ComposeSettings(
			test.SystemTestSettings(),
			func(s *settings.Settings) {
				s.Legacy.AllowSyncCandidateFromLocalPeers = true
				// s.Legacy.AdvertiseFullNode = true
				s.Legacy.ListenAddresses = []string{teranodeLegacyListenAddr}
				s.P2P.StaticPeers = []string{}
			},
		),
	})

	defer func() { td.Stop(t) }()

	td.WaitForBlockHeight(t, teranodeMinedBlocks[len(teranodeMinedBlocks)-1], 30*time.Second)
	waitForLegacyListener(t, teranodeLegacyConnectAddr, 10*time.Second)

	const phase3MaxAttempts = 3
	for attempt := 1; attempt <= phase3MaxAttempts; attempt++ {
		opts := svnode.DefaultOptions()
		opts.ConnectTo = []string{teranodeLegacyConnectAddr}
		sv = svnode.New(opts)
		err = sv.Start(ctx)
		require.NoError(t, err, errStartSVNode)

		err = waitForOutboundPeer(ctx, sv, 15*time.Second)
		require.NoError(t, err, errSVNodeConnect)

		err = sv.WaitForBlockHeight(ctx, currentHeight, 30*time.Second)
		if err == nil {
			break
		}

		t.Logf("Phase 3 sync attempt %d/%d failed: %v", attempt, phase3MaxAttempts, err)
		if logs, logErr := sv.GetLogs(ctx); logErr == nil {
			t.Logf("SVNode logs (attempt %d):\n%s", attempt, logs)
		}
		_ = sv.Stop(context.Background())

		if attempt < phase3MaxAttempts {
			td.Stop(t)
			td.ResetServiceManagerContext(t)
			td = daemon.NewTestDaemon(t, daemon.TestOptions{
				EnableRPC:            true,
				EnableP2P:            true,
				EnableValidator:      true,
				EnableBlockPersister: true,
				EnableLegacy:         true,
				SkipRemoveDataDir:    true,
				SettingsOverrideFunc: test.ComposeSettings(
					test.SystemTestSettings(),
					func(s *settings.Settings) {
						s.Legacy.AllowSyncCandidateFromLocalPeers = true
						// s.Legacy.AdvertiseFullNode = true
						s.Legacy.ListenAddresses = []string{teranodeLegacyListenAddr}
						s.P2P.StaticPeers = []string{}
					},
				),
			})
			td.WaitForBlockHeight(t, teranodeMinedBlocks[len(teranodeMinedBlocks)-1], 30*time.Second)
			waitForLegacyListener(t, teranodeLegacyConnectAddr, 10*time.Second)
		} else {
			require.NoError(t, err, "SVNode failed to sync teranode blocks after %d attempts", phase3MaxAttempts)
		}
	}

	t.Log("SVNode synced teranode's blocks")

	// Restart teranode with ConnectPeers to svnode for Phase 4.
	// Teranode needs an outbound connection to svnode to sync blocks
	// (Bitcoin protocol only syncs from outbound peers for security).
	svP2PHost := sv.P2PHost()
	td.Stop(t)
	td.ResetServiceManagerContext(t)
	td = daemon.NewTestDaemon(t, daemon.TestOptions{
		EnableRPC:            true,
		EnableP2P:            true,
		EnableValidator:      true,
		EnableBlockPersister: true,
		EnableLegacy:         true,
		SkipRemoveDataDir:    true,
		SettingsOverrideFunc: test.ComposeSettings(
			test.SystemTestSettings(),
			func(s *settings.Settings) {
				s.Legacy.AllowSyncCandidateFromLocalPeers = true
				// s.Legacy.AdvertiseFullNode = true
				s.Legacy.ListenAddresses = []string{teranodeLegacyListenAddr}
				s.Legacy.ConnectPeers = []string{svP2PHost}
				s.P2P.StaticPeers = []string{}
			},
		),
	})

	td.WaitForBlockHeight(t, teranodeMinedBlocks[len(teranodeMinedBlocks)-1], 30*time.Second)

	// Phase 4: SVNode generates more blocks, teranode syncs
	const moreBlocks = 5
	_, err = sv.Generate(moreBlocks)
	require.NoError(t, err, "Failed to generate more blocks on svnode")

	finalHeight := currentHeight + moreBlocks
	t.Logf("SVNode generated %d more blocks, target height: %d", moreBlocks, finalHeight)

	// Wait for teranode to sync svnode's new blocks
	err = helper.WaitForNodeBlockHeight(ctx, td.BlockchainClient, uint32(finalHeight), 30*time.Second)
	require.NoError(t, err, "Teranode failed to sync svnode's new blocks")

	// Verify both nodes are at the same height
	// svBlockCount, err := sv.GetBlockCount()
	// require.NoError(t, err)

	header, _, err := td.BlockchainClient.GetBestBlockHeader(ctx)
	require.NoError(t, err)

	// require.Equal(t, finalHeight, svBlockCount, "SVNode height mismatch")
	// require.Equal(t, uint32(finalHeight), meta.Height, "Teranode height mismatch")

	// Verify both nodes have the same best block hash
	svBestHash, err := sv.GetBestBlockHash()
	require.NoError(t, err)

	teranodeBestHash := header.Hash().String()
	require.Equal(t, svBestHash, teranodeBestHash, "Best block hash should match between svnode and teranode")

	t.Logf("Bidirectional sync complete - both nodes at height %d with hash %s", finalHeight, svBestHash)
}

// TestSVNodeValidatesTeranodeBlocks specifically tests that blocks generated by teranode
// pass validation by svnode's consensus rules
//
// This test uses the persist pattern:
// 1. SVNode generates 1 block (required for accepting blocks from pruned node)
// 2. Teranode (without legacy, with persister) generates blocks
// 3. Teranode restarts with legacy
// 4. SVNode syncs and validates each block
// func TestSVNodeValidatesTeranodeBlocks(t *testing.T) {
// 	t.Skip()
// 	legacySyncTestLock.Lock()
// 	defer legacySyncTestLock.Unlock()

// 	ctx := t.Context()

// 	// Phase 1: Start teranode WITHOUT legacy to generate blocks with persister
// 	td := daemon.NewTestDaemon(t, daemon.TestOptions{
// 		EnableRPC:            true,
// 		EnableP2P:            true,
// 		EnableValidator:      true,
// 		EnableBlockPersister: true,
// 		PreserveDataDir:      true,
// 		SettingsOverrideFunc: test.ComposeSettings(
// 			test.SystemTestSettings(),
// 		),
// 	})

// 	err := td.BlockchainClient.Run(td.Ctx, "test")
// 	require.NoError(t, err, "failed to initialize blockchain")

// 	// Generate multiple blocks on teranode
// 	const blocksToGenerate = 10
// 	var generatedBlocks []*model.Block
// 	for i := 0; i < blocksToGenerate; i++ {
// 		time.Sleep(500 * time.Millisecond)
// 		block := td.MineAndWait(t, 1)
// 		generatedBlocks = append(generatedBlocks, block)
// 	}

// 	t.Logf("Teranode generated %d blocks", blocksToGenerate)

// 	// Wait for all blocks to be persisted before restarting with legacy
// 	for i, block := range generatedBlocks {
// 		err = td.WaitForBlockPersisted(block.Hash(), 30*time.Second)
// 		require.NoError(t, err, "Block %d was not persisted within timeout", i+1)
// 	}
// 	t.Log("All blocks persisted")

// 	// Stop teranode to restart with legacy
// 	td.Stop(t)
// 	td.ResetServiceManagerContext(t)

// 	// Start svnode in Docker before teranode so it's ready
// 	sv := newSVNode()
// 	err = sv.Start(ctx)
// 	require.NoError(t, err, errStartSVNode)

// 	defer func() {
// 		_ = sv.Stop(context.Background())
// 	}()

// 	// Phase 2: Restart teranode WITH legacy listening + ConnectPeers to svnode.
// 	// ConnectPeers creates a teranode→svnode connection (inbound on svnode).
// 	// sv.AddNode below creates an svnode→teranode outbound connection (required
// 	// for block download in Bitcoin SV).
// 	td = daemon.NewTestDaemon(t, daemon.TestOptions{
// 		EnableRPC:            true,
// 		EnableP2P:            true,
// 		EnableValidator:      true,
// 		EnableBlockPersister: true,
// 		EnableLegacy:         true,
// 		SkipRemoveDataDir:    true,
// 		PreserveDataDir:      true,
// 		SettingsOverrideFunc: test.ComposeSettings(
// 			test.SystemTestSettings(),
// 			func(s *settings.Settings) {
// 				s.Legacy.AllowSyncCandidateFromLocalPeers = true
// 				// s.Legacy.AdvertiseFullNode = true
// 				s.Legacy.ListenAddresses = []string{teranodeLegacyListenAddr}
// 				s.Legacy.ConnectPeers = []string{sv.P2PHost()}
// 				s.P2P.StaticPeers = []string{}
// 			},
// 		),
// 	})

// 	defer td.Stop(t)

// 	// Wait for teranode to load its blockchain to the target height
// 	td.WaitForBlockHeight(t, generatedBlocks[len(generatedBlocks)-1], 30*time.Second)
// 	t.Log("Teranode loaded blockchain, waiting for legacy listener...")

// 	// Wait for the legacy P2P listener to start accepting connections
// 	waitForLegacyListener(t, teranodeLegacyConnectAddr, 10*time.Second)

// 	// Have svnode create an outbound connection to teranode
// 	err = sv.AddNode(teranodeLegacyConnectAddr, "add")
// 	require.NoError(t, err, errAddTeranodePeer)

// 	err = waitForOutboundPeer(ctx, sv, 30*time.Second)
// 	require.NoError(t, err, errSVNodeConnect)

// 	// Generate a block on svnode to trigger sync via inv exchange
// 	_, err = sv.Generate(1)
// 	require.NoError(t, err, "Failed to generate trigger block on svnode")

// 	// Wait for svnode to sync all teranode blocks (teranode's chain has more work)
// 	finalHeight := blocksToGenerate
// 	syncErr := sv.WaitForBlockHeight(ctx, finalHeight, 60*time.Second)
// 	if syncErr != nil {
// 		t.Logf("Sync attempt failed, retrying: %v", syncErr)
// 		_ = sv.AddNode(teranodeLegacyConnectAddr, "remove")
// 		_ = sv.DisconnectNode(teranodeLegacyConnectAddr)
// 		time.Sleep(2 * time.Second)

// 		err = sv.AddNode(teranodeLegacyConnectAddr, "add")
// 		require.NoError(t, err, errAddTeranodePeer)

// 		err = waitForOutboundPeer(ctx, sv, 30*time.Second)
// 		require.NoError(t, err, errSVNodeConnect)

// 		syncErr = sv.WaitForBlockHeight(ctx, finalHeight, 60*time.Second)
// 	}
// 	require.NoError(t, syncErr, "SVNode failed to sync blocks from teranode")

// 	t.Logf("SVNode synced to height %d", finalHeight)

// 	// Verify each block was validated by svnode
// 	for i := 2; i <= finalHeight; i++ {
// 		// Verify the chain is valid on svnode up to this height
// 		valid, err := sv.VerifyChain(1, i) // Quick verify of last block
// 		require.NoError(t, err, "Failed to verify chain on svnode")
// 		require.True(t, valid, "SVNode chain verification failed")
// 	}

// 	// Final verification - verify entire chain
// 	valid, err := sv.VerifyChain(4, finalHeight) // Full verification
// 	require.NoError(t, err, "Failed to verify full chain on svnode")
// 	require.True(t, valid, "SVNode full chain verification failed")

// 	// Verify block hashes match at the target height.
// 	// We compare at finalHeight (not best block) because sv.Generate(1) may have
// 	// added an extra block beyond teranode's chain.
// 	svBlockHash, err := sv.GetBlockHash(finalHeight)
// 	require.NoError(t, err)

// 	teranodeBlock, err := td.BlockchainClient.GetBlockByHeight(ctx, uint32(finalHeight))
// 	require.NoError(t, err)

// 	require.Equal(t, svBlockHash, teranodeBlock.Hash().String(), "Block hash at height %d should match", finalHeight)

// 	t.Logf("All %d teranode blocks validated by svnode - chain integrity confirmed", blocksToGenerate)
// }
