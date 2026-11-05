//go:build network_chaos

package harness

import (
	"context"
	"fmt"
	"net/url"
	"path/filepath"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/blob"
	"github.com/bsv-blockchain/teranode/stores/blob/options"
	blockchainstore "github.com/bsv-blockchain/teranode/stores/blockchain"
	utxofactory "github.com/bsv-blockchain/teranode/stores/utxo/factory"
	"github.com/bsv-blockchain/teranode/test/utils/chainoracle"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

// Host-side addresses of a node's stores. These mirror what compose/cmd/gennodes
// generates: Postgres published on 15432 with database teranodeN owned by minerN,
// Aerospike for node N published on 3000+10*N, and the blob stores bind-mounted
// under data/multinode/teranodeN.
const (
	postgresHostPort  = 15432
	aerospikeBasePort = 3000
	aerospikePerNode  = 10

	// the daemon's defaults when the store URL sets no hashPrefix
	subtreeStoreHashPrefix = 2
	blockStoreHashPrefix   = -2
)

// NodeStores opens read handles on node n's blockchain, UTXO, subtree and
// block stores from the host, for the chain oracle. The handles are closed when
// the test ends. The settings come from the node's own settings context, so the
// chain parameters and UTXO batching match what the node runs with.
func (s *Stack) NodeStores(t *testing.T, n int) chainoracle.Stores {
	t.Helper()

	ctx := context.Background()
	logger := ulogger.New(fmt.Sprintf("oracle-teranode%d", n), ulogger.WithLevel("WARN"))
	tSettings := settings.NewSettings(fmt.Sprintf("docker.teranode%d.test", n))
	dataDir := filepath.Join(s.RepoRoot, "data", "multinode", fmt.Sprintf("teranode%d", n))

	chainURL := mustParseURL(t, fmt.Sprintf("postgres://miner%d:miner%d@localhost:%d/teranode%d", n, n, postgresHostPort, n))

	chainStore, err := blockchainstore.NewStore(logger, chainURL, tSettings)
	require.NoError(t, err, "open teranode%d blockchain store", n)

	tSettings.UtxoStore.UtxoStore = mustParseURL(t, fmt.Sprintf("aerospike://localhost:%d/test?set=utxo&externalStore=file://%s",
		aerospikeBasePort+aerospikePerNode*n, filepath.Join(dataDir, "external")))

	utxoStore, err := utxofactory.NewStore(ctx, logger, tSettings, "chainoracle", false)
	require.NoError(t, err, "open teranode%d UTXO store", n)

	t.Cleanup(func() { _ = utxoStore.Close(context.Background()) })

	subtreeStore, err := blob.NewStore(logger, mustParseURL(t, "file://"+filepath.Join(dataDir, "subtreestore")+"?checksum=false"),
		options.WithHashPrefix(subtreeStoreHashPrefix))
	require.NoError(t, err, "open teranode%d subtree store", n)

	blockStore, err := blob.NewStore(logger, mustParseURL(t, "file://"+filepath.Join(dataDir, "blockstore")),
		options.WithHashPrefix(blockStoreHashPrefix))
	require.NoError(t, err, "open teranode%d block store", n)

	return chainoracle.Stores{
		Chain:        chainoracle.StoreChain(chainStore),
		UTXO:         utxoStore,
		SubtreeStore: subtreeStore,
		BlockStore:   blockStore,
		Settings:     tSettings,
	}
}

// RequireStoresMatchChain runs the chain oracle against node n and fails the
// test, printing the full diff, if its UTXO store or blob store disagrees with
// its best chain. wantTip is the tip the node is expected to be on; the check
// first waits for the node's block persister to write that block's file, so a
// persister that is merely behind is not reported as drift.
func (s *Stack) RequireStoresMatchChain(t *testing.T, n int, wantTip string) *chainoracle.Report {
	t.Helper()

	tip, err := chainhash.NewHashFromStr(wantTip)
	require.NoError(t, err)

	stores := s.NodeStores(t, n)

	WaitForCondition(t, fmt.Sprintf("teranode%d to persist block %s", n, short(wantTip)), 2*time.Minute, 2*time.Second,
		func(ctx context.Context) (bool, error) {
			return stores.BlockStore.Exists(ctx, tip[:], fileformat.FileTypeBlock)
		})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	report, err := chainoracle.Check(ctx, stores, chainoracle.Options{})
	require.NoError(t, err, "chain oracle could not run against teranode%d", n)
	require.Equal(t, *tip, report.Tip, "teranode%d's best chain moved while the oracle ran", n)
	require.True(t, report.OK(), "teranode%d's stores disagree with its best chain:\n%s", n, report.String())

	t.Logf("teranode%d stores match the best chain: %d blocks, %d txs, %d outputs checked", n, report.Blocks, report.Txs, report.Outputs)

	return report
}

func mustParseURL(t *testing.T, raw string) *url.URL {
	t.Helper()

	u, err := url.Parse(raw)
	require.NoError(t, err)

	return u
}
