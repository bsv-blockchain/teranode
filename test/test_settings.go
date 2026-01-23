package test

import (
	"fmt"
	"net/url"
	"path/filepath"

	"github.com/bsv-blockchain/teranode/settings"
)

// Pre-defined P2P identities for multi-node test scenarios.
// These match the docker-compose configurations for teranode1, teranode2, teranode3.
var (
	// Node 1 P2P identity
	Node1PeerID     = "12D3KooWAFXWuxgdJoRsaA4J4RRRr8yu6WCrAPf8FaS7UfZg3ceG"
	Node1PrivateKey = "c8a1b91ae120878d91a04c904e0d565aa44b2575c1bb30a729bd3e36e2a1d5e6067216fa92b1a1a7e30d0aaabe288e25f1efc0830f309152638b61d84be6b71d"

	// Node 2 P2P identity
	Node2PeerID     = "12D3KooWG6aCkDmi5tqx4G4AvVDTQdSVvTSzzQvk1vh9CtSR8KEW"
	Node2PrivateKey = "89a2d8acf5b2e60fd969914c326c63cde50675a47897c0eaacc02eb6ff8665585d4d059f977910472bcb75040617632019cc0749443fdc66d331b61c8cfb4b0f"

	// Node 3 P2P identity
	Node3PeerID     = "12D3KooWHHeTM3aK4s9DKS6DQ7SbBb7czNyJsPZtQiUKa4fduMB9"
	Node3PrivateKey = "d77a7cac7833f2c0263ed7b9aaeb8dda1effaf8af948d570ed8f7a93bd3c418d6efee7bdd82ddb80484be84ba0c78ea07251a3ba2b45b2b3367fd5e2f0284e7c"
)

// SystemTestSettings returns a settings override function that configures
// settings equivalent to what "dev.system.test" context provided.
// Tests can compose this with additional overrides as needed.
// Note: Service toggles (Start*) are controlled via TestOptions in daemon.NewTestDaemon,
// not through the Settings struct. Use EnableRPC, EnableP2P, etc. in TestOptions instead.
func SystemTestSettings() func(*settings.Settings) {
	return func(s *settings.Settings) {
		s.BlockChain.StoreURL = mustParseURL("sqlite:///blockchain")
		s.Coinbase.Store = mustParseURL("sqlitememory:///coinbase")
		s.Coinbase.P2PStaticPeers = []string{}
		s.Coinbase.WaitForPeers = false

		// Tracing - disabled for faster test execution
		s.TracingEnabled = false
	}
}

func mustParseURL(rawURL string) *url.URL {
	u, err := url.Parse(rawURL)
	if err != nil {
		panic("invalid URL in test settings: " + rawURL + ": " + err.Error())
	}
	return u
}

// SystemTestSettingsWithBlockAssemblyDisabled returns system test settings
// with block assembly disabled. Useful for tests that don't need block assembly.
//
// Note: Use TestOptions.EnableBlockAssembly = false instead of this function
// to control whether block assembly service starts.
func SystemTestSettingsWithBlockAssemblyDisabled() func(*settings.Settings) {
	return ComposeSettings(
		SystemTestSettings(),
		func(s *settings.Settings) {
			s.BlockAssembly.Disabled = true
		},
	)
}

// SystemTestSettingsWithCoinbaseDisabled returns system test settings
// with coinbase disabled. Useful for tests that don't need coinbase tracking.
//
// Note: Use TestOptions to control which services start. This function exists
// for consistency but has no effect on the Settings struct.
func SystemTestSettingsWithCoinbaseDisabled() func(*settings.Settings) {
	return ComposeSettings(
		SystemTestSettings(),
		func(s *settings.Settings) {
			// Coinbase service start is controlled via TestOptions, not Settings.
			// This function is kept for API compatibility but doesn't modify settings.
		},
	)
}

// SystemTestSettingsWithPolicyOverrides returns system test settings
// with specific policy overrides for testing edge cases.
func SystemTestSettingsWithPolicyOverrides(maxTxSize, maxScriptSize, maxScriptNumLength int64) func(*settings.Settings) {
	return ComposeSettings(
		SystemTestSettings(),
		func(s *settings.Settings) {
			if maxTxSize > 0 {
				s.Policy.MaxTxSizePolicy = int(maxTxSize)
			}
			if maxScriptSize > 0 {
				s.Policy.MaxScriptSizePolicy = int(maxScriptSize)
			}
			if maxScriptNumLength > 0 {
				s.Policy.MaxScriptNumLengthPolicy = int(maxScriptNumLength)
			}
		},
	)
}

// ComposeSettings combines multiple settings override functions into one.
// This allows tests to compose base settings with test-specific overrides.
//
// Example:
//
//	daemon.NewTestDaemon(t, daemon.TestOptions{
//	    EnableRPC: true,
//	    SettingsOverrideFunc: test.ComposeSettings(
//	        test.SystemTestSettings(),
//	        func(s *settings.Settings) {
//	            s.TracingEnabled = true
//	            s.TracingSampleRate = 1.0
//	        },
//	    ),
//	})
func ComposeSettings(overrides ...func(*settings.Settings)) func(*settings.Settings) {
	return func(s *settings.Settings) {
		for _, override := range overrides {
			if override != nil {
				override(s)
			}
		}
	}
}

// MultiNodeSettings returns settings for a specific node in a multi-node test scenario.
// nodeNumber must be 1, 2, or 3.
//
// This configures node-specific settings for multi-node testing:
//   - Unique client name (teranode1, teranode2, teranode3)
//   - Unique P2P identity (peer ID and private key)
//   - Separate stores per node (blockchain, utxo)
//
// NOTE: This function does NOT override service listen addresses or ports.
// TestDaemon dynamically allocates ports to avoid conflicts. Overriding them
// would cause a mismatch between the pre-allocated listeners and the settings.
//
// Example usage for 3-node test:
//
//	node1 := daemon.NewTestDaemon(t, daemon.TestOptions{
//	    EnableP2P: true,
//	    SettingsOverrideFunc: test.MultiNodeSettings(1),
//	})
//	node2 := daemon.NewTestDaemon(t, daemon.TestOptions{
//	    EnableP2P: true,
//	    SettingsOverrideFunc: test.MultiNodeSettings(2),
//	})
//	// Connect nodes
//	node2.InjectPeer(t, node1)
func MultiNodeSettings(nodeNumber int) func(*settings.Settings) {
	if nodeNumber < 1 || nodeNumber > 3 {
		panic(fmt.Sprintf("nodeNumber must be 1, 2, or 3, got %d", nodeNumber))
	}

	peerIDs := []string{Node1PeerID, Node2PeerID, Node3PeerID}
	privateKeys := []string{Node1PrivateKey, Node2PrivateKey, Node3PrivateKey}

	return func(s *settings.Settings) {
		// Unique client name
		s.ClientName = fmt.Sprintf("teranode%d", nodeNumber)

		// Unique P2P identity
		s.P2P.PeerID = peerIDs[nodeNumber-1]
		s.P2P.PrivateKey = privateKeys[nodeNumber-1]

		// No static peers by default - use ConnectToPeer/InjectPeer instead
		s.P2P.StaticPeers = []string{}

		// === Stores (separate per node) ===
		// Uses nested paths like teranode1/blockchain1.db to isolate node data
		// SQLite URLs are resolved relative to DataFolder by util.InitSQLiteDB
		s.BlockChain.StoreURL = mustParseURL(fmt.Sprintf("sqlite:///teranode%d/blockchain%d", nodeNumber, nodeNumber))
		s.UtxoStore.UtxoStore = mustParseURL(fmt.Sprintf("sqlite:///teranode%d/utxo%d", nodeNumber, nodeNumber))

		// Block stores - file-based, separate per node
		// File URLs use file://./relative/path format for relative paths (Host="." triggers relative path handling)
		blockStorePath := filepath.Join(s.DataFolder, fmt.Sprintf("teranode%d", nodeNumber), "blockstore")
		blockStoreURL := mustParseURL(fmt.Sprintf("file://./%s", blockStorePath))
		s.Block.BlockStore = blockStoreURL
		s.BlockPersister.Store = blockStoreURL // Both stores should use the same per-node path

		subtreeStorePath := filepath.Join(s.DataFolder, fmt.Sprintf("teranode%d", nodeNumber), "subtreestore")
		subtreeStoreURL := mustParseURL(fmt.Sprintf("file://./%s", subtreeStorePath))
		s.SubtreeValidation.SubtreeStore = subtreeStoreURL

		s.SubtreeValidation.QuorumPath = filepath.Join(s.DataFolder, fmt.Sprintf("teranode%d", nodeNumber), "subtree_quorum")

		// NOTE: Do NOT override listen addresses here.
		// TestDaemon pre-allocates listeners on dynamic ports before calling SettingsOverrideFunc.
		// Overriding the addresses would cause the health check and other services to fail
		// because they'd try to connect to ports that don't have listeners.
	}
}
