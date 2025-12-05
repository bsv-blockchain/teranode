package smoke

import (
	"fmt"
	"net/url"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/daemon"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/test/utils/aerospike"
	"github.com/stretchr/testify/require"
)

func getAerospikeInstance(t *testing.T) *url.URL {
	urlStr, teardownFn, err := aerospike.InitAerospikeContainer()
	require.NoError(t, err, "Failed to setup Aerospike container")

	url, err := url.Parse(urlStr)
	require.NoError(t, err, "Failed to parse UTXO store URL")

	t.Cleanup(func() {
		_ = teardownFn()
	})

	return url
}

func getTestDaemon(t *testing.T, settingsContext string, aerospikeURL *url.URL) *daemon.TestDaemon {
	d := daemon.NewTestDaemon(t, daemon.TestOptions{
		EnableRPC:       true,
		EnableP2P:       true,
		EnableValidator: true,
		SettingsOverrideFunc: func(s *settings.Settings) {
			s.P2P.PeerCacheDir = t.TempDir()
			s.UtxoStore.UtxoStore = aerospikeURL
			s.ChainCfgParams.CoinbaseMaturity = 2
			s.P2P.SyncCoordinatorPeriodicEvaluationInterval = 1 * time.Second
		},
		FSMState: blockchain.FSMStateRUNNING,
		// EnableFullLogging: true,
	})

	t.Cleanup(func() {
		d.Stop(t)
	})

	return d
}

func printPeerRegistry(t *testing.T, td *daemon.TestDaemon) {
	registry, err := td.P2PClient.GetPeerRegistry(t.Context())
	require.NoError(t, err)

	fmt.Printf("\nPeer %s (%s) registry:\n", td.Settings.ClientName, td.Settings.P2P.PeerID)

	for _, peerInfo := range registry {
		fmt.Printf("\tName: %s (%s): Height=%d, BlockHash=%s, DataHubURL=%s\n", peerInfo.ClientName, peerInfo.ID, peerInfo.Height, peerInfo.BlockHash, peerInfo.DataHubURL)
	}

	fmt.Println()
	fmt.Println()
}

// This test creates 2 nodes, and nodeA mines 3 blocks.  Then we inject nodeA into nodeB, and nodeB should sync up to nodeA's height.
func Test_NodeB_Inject_After_NodeA_Mined(t *testing.T) {
	t.Skip("Skipping until the settings are sorted out")
	SharedTestLock.Lock()
	defer SharedTestLock.Unlock()

	sharedAerospike := getAerospikeInstance(t)
	nodeA := getTestDaemon(t, "docker.host.teranode1.daemon", sharedAerospike)
	nodeB := getTestDaemon(t, "docker.host.teranode2.daemon", sharedAerospike)

	t.Log("         Creating initial blockchain: [Genesis] -> [Block1] -> [Block2] -> [Block3]")
	coinbaseTx := nodeA.MineToMaturityAndGetSpendableCoinbaseTx(t, nodeA.Ctx)
	t.Logf("         Coinbase transaction available for spending: %s", coinbaseTx.TxIDChainHash().String())

	// nodeA.InjectPeer(t, nodeB)
	nodeB.InjectPeer(t, nodeA)

	printPeerRegistry(t, nodeB)

	nodeABestBlockHeader, _, err := nodeA.BlockchainClient.GetBestBlockHeader(nodeA.Ctx)
	require.NoError(t, err)

	nodeB.WaitForBlockhash(t, nodeABestBlockHeader.Hash(), 10*time.Second)

}

// This test creates 2 nodes, and nodeB injects nodeA before nodeA mines any blocks.  Then we mine 3 blocks on nodeA, and nodeB should sync up to nodeA's height.
func Test_NodeB_Inject_Before_NodeA_Mined(t *testing.T) {
	t.SkipNow()

	SharedTestLock.Lock()
	defer SharedTestLock.Unlock()

	sharedAerospike := getAerospikeInstance(t)
	nodeA := getTestDaemon(t, "docker.host.teranode1.daemon", sharedAerospike)
	nodeB := getTestDaemon(t, "docker.host.teranode2.daemon", sharedAerospike)

	// nodeA.InjectPeer(t, nodeB)
	nodeB.InjectPeer(t, nodeA)

	printPeerRegistry(t, nodeB)

	// go func() {
	// 	for {
	// 		time.Sleep(5 * time.Second)
	// 		printPeerRegistry(t, nodeB)
	// 	}
	// }()

	t.Log("         Creating initial blockchain: [Genesis] -> [Block1] -> [Block2] -> [Block3]")
	coinbaseTx := nodeA.MineToMaturityAndGetSpendableCoinbaseTx(t, nodeA.Ctx)
	t.Logf("         Coinbase transaction available for spending: %s", coinbaseTx.TxIDChainHash().String())

	printPeerRegistry(t, nodeB)

	s, err := nodeB.CallRPC(t.Context(), "getpeerinfo", []interface{}{})
	require.NoError(t, err)
	t.Logf("         NodeB peer info: %s", s)

	nodeABestBlockHeader, _, err := nodeA.BlockchainClient.GetBestBlockHeader(nodeA.Ctx)
	require.NoError(t, err)

	nodeB.WaitForBlockhash(t, nodeABestBlockHeader.Hash(), 26*time.Second)

}
