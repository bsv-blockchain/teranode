package aerospike_test

import (
	"cmp"
	"context"
	"fmt"
	"net/url"
	"os"
	"sync"
	"testing"

	"github.com/bsv-blockchain/aerospike-client-go/v8"
	"github.com/bsv-blockchain/go-batcher/v2/completion"
	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	teranode_aerospike "github.com/bsv-blockchain/teranode/stores/utxo/aerospike"
	spendpkg "github.com/bsv-blockchain/teranode/stores/utxo/spend"
	utxo2 "github.com/bsv-blockchain/teranode/test/longtest/stores/utxo"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/lane"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/bsv-blockchain/teranode/util/uaerospike"
	aeroTest "github.com/bsv-blockchain/testcontainers-aerospike-go"
	"github.com/stretchr/testify/require"
)

const (
	aerospikeHost           = "localhost" // "localhost"
	aerospikePort           = 3000        // 3800
	aerospikeNamespace      = teranode_aerospike.SharedTestNamespace
	aerospikeSet            = teranode_aerospike.SharedTestSet
	aerospikeBlockRetention = 1
)

var (
	// customize the container image if needed, for example a locally built mod-teranode fork:
	// TERANODE_TEST_AEROSPIKE_IMAGE=aerospike-tn:mark. Unset means test.AerospikeImage.
	aerospikeContainerImage = cmp.Or(os.Getenv("TERANODE_TEST_AEROSPIKE_IMAGE"), test.AerospikeImage)

	spendingTxID1, _ = chainhash.NewHashFromStr("5e3bc5947f48cec766090aa17f309fd16259de029dcef5d306b514848c9687c7")
	spendingTxID2, _ = chainhash.NewHashFromStr("663bc5947f48cec766090aa17f309fd16259de029dcef5d306b514848c9687c8")
)

const (
	fixtureTxHex             = "010000000000000000ef0152a9231baa4e4b05dc30c8fbb7787bab5f460d4d33b039c39dd8cc006f3363e4020000006b483045022100ce3605307dd1633d3c14de4a0cf0df1439f392994e561b648897c4e540baa9ad02207af74878a7575a95c9599e9cdc7e6d73308608ee59abcd90af3ea1a5c0cca41541210275f8390df62d1e951920b623b8ef9c2a67c4d2574d408e422fb334dd1f3ee5b6ffffffff706b9600000000001976a914a32f7eaae3afd5f73a2d6009b93f91aa11d16eef88ac05404b4c00000000001976a914aabb8c2f08567e2d29e3a64f1f833eee85aaf74d88ac80841e00000000001976a914a4aff400bef2fa074169453e703c611c6b9df51588ac204e0000000000001976a9144669d92d46393c38594b2f07587f01b3e5289f6088ac204e0000000000001976a914a461497034343a91683e86b568c8945fb73aca0288ac99fe2a00000000001976a914de7850e419719258077abd37d4fcccdb0a659b9388ac00000000"
	fixtureCoinbaseTxHex     = "01000000010000000000000000000000000000000000000000000000000000000000000000ffffffff17032dff0c2f71646c6e6b2f5e931c7f7b6199adf35e1300ffffffff01d15fa012000000001976a91417db35d440a673a218e70a5b9d07f895facf50d288ac00000000"
	fixtureTxWithOPReturnHex = "010000000000000000ef01977da9cf1e56bc7447e6561aa7d404e06343c3fd6034d5934eedddb222a928cc010000006b483045022100f7cd34af663f7ff3ab447476c1078610b0a258e88241bc98f93bec1275c65ace02205945dc2be5e855846e428c58e3758413b3f531f59a53528a3e4a75dfa09e894b4121033188d07302a394cdefba66bf83adf52b0922f16251a8dfb448cca061617f8953fffffffff5262400000000001976a9147f07da316209da8f3250d5ef06aa4fdf5179ffe288ac0200000000000000008a6a22314c74794d45366235416e4d6f70517242504c6b3446474e3855427568784b71726e0101357b2274223a32312e36362c2268223a38332c2270223a313031332c2263223a31372c227773223a312e35372c227764223a3232357d22314361674478397973596b4b79667952524a524d78793737454256776a64344c52780a31353638343830323731a2252400000000001976a9147f07da316209da8f3250d5ef06aa4fdf5179ffe288ac00000000"
)

// txFixtures is the set of transactions a test works with. Each set is its own: newTxFixtures
// gives its transactions a lock time no other set has, which changes every txid, so tests that
// share an Aerospike server never write the same records. The lock time is the only difference
// from the original fixtures; the store does not interpret it.
type txFixtures struct {
	tx               *bt.Tx
	coinbaseTx       *bt.Tx
	spendCoinbaseTx  *bt.Tx
	txWithOPReturn   *bt.Tx
	spendTx          *bt.Tx
	spendTx2         *bt.Tx
	spendTx3         *bt.Tx
	spendTxAll       *bt.Tx
	spendTxRemaining *bt.Tx

	utxoHash0 *chainhash.Hash
	utxoHash1 *chainhash.Hash
	utxoHash2 *chainhash.Hash
	utxoHash3 *chainhash.Hash
	utxoHash4 *chainhash.Hash

	spend     *utxo.Spend
	spends    []*utxo.Spend
	spendsAll []*utxo.Spend
}

// newTxFixtures returns a fresh set of the package's fixture transactions, with txids no other
// set has.
func newTxFixtures(t testing.TB) *txFixtures {
	t.Helper()

	fx := &txFixtures{
		tx:             uniqueFixtureTx(t, fixtureTxHex),
		coinbaseTx:     uniqueFixtureTx(t, fixtureCoinbaseTxHex),
		txWithOPReturn: uniqueFixtureTx(t, fixtureTxWithOPReturnHex),
	}

	fx.spendCoinbaseTx = utxo2.GetSpendingTx(fx.coinbaseTx, 0)

	hashes := make([]*chainhash.Hash, 5)
	for vout := range hashes {
		hash, err := util.UTXOHashFromOutput(fx.tx.TxIDChainHash(), fx.tx.Outputs[vout], uint32(vout)) //nolint:gosec // vout < 5
		require.NoError(t, err)

		hashes[vout] = hash
	}

	fx.utxoHash0, fx.utxoHash1, fx.utxoHash2, fx.utxoHash3, fx.utxoHash4 = hashes[0], hashes[1], hashes[2], hashes[3], hashes[4]

	fx.spendTx = utxo2.GetSpendingTx(fx.tx, 0)
	fx.spend = &utxo.Spend{
		TxID:         fx.tx.TxIDChainHash(),
		Vout:         0,
		UTXOHash:     fx.utxoHash0,
		SpendingData: spendpkg.NewSpendingData(spendingTxID1, 0),
	}
	fx.spends = []*utxo.Spend{fx.spend}
	fx.spendTx2 = utxo2.GetSpendingTx(fx.tx, 0)
	fx.spendTx3 = utxo2.GetSpendingTx(fx.tx, 0)
	fx.spendTxAll = utxo2.GetSpendingTx(fx.tx, 0, 1, 2, 3, 4)

	// The vout-to-spending-data pairing (3 and 4 spend at index 0) is as the fixtures always had it.
	spendingIdx := []int{0, 1, 2, 0, 0}
	for vout, hash := range hashes {
		fx.spendsAll = append(fx.spendsAll, &utxo.Spend{
			TxID:         fx.tx.TxIDChainHash(),
			Vout:         uint32(vout), //nolint:gosec // vout < 5
			UTXOHash:     hash,
			SpendingData: spendpkg.NewSpendingData(spendingTxID2, spendingIdx[vout]),
		})
	}

	fx.spendTxRemaining = utxo2.GetSpendingTx(fx.tx, 1, 2, 3, 4)

	return fx
}

// uniqueFixtureTx parses a fixture transaction and makes it unique with uniqueTx.
func uniqueFixtureTx(t testing.TB, txHex string) *bt.Tx {
	t.Helper()

	tx, err := bt.NewTxFromString(txHex)
	require.NoError(t, err)

	return uniqueTx(tx)
}

// uniqueTx gives tx a lock time no other transaction made unique here has (NextUniqueLockTime),
// which changes its txid, and returns it. Tests on a shared server must not create the same transaction as another
// test; the store does not interpret the lock time.
func uniqueTx(tx *bt.Tx) *bt.Tx {
	tx.LockTime = teranode_aerospike.NextUniqueLockTime()

	return tx
}

// initAerospike opens a store on the Aerospike server this test binary shares (see
// AcquireSharedTestServer for what a test on it must not do).
func initAerospike(t *testing.T, settings *settings.Settings, logger ulogger.Logger) (*uaerospike.Client, *teranode_aerospike.Store, context.Context, func()) {
	client, db, ctx, cleanup, _ := initAerospikeWithURL(t, settings, logger, nil)

	return client, db, ctx, cleanup
}

// initAerospikeOwnServer is initAerospike on a server of the test's own, for a test that needs
// the whole server: one that clears, scans or counts a set, or changes the server.
func initAerospikeOwnServer(t *testing.T, settings *settings.Settings, logger ulogger.Logger) (*uaerospike.Client, *teranode_aerospike.Store, context.Context, func()) {
	client, db, ctx, cleanup, _ := initAerospikeOwnServerWithURL(t, settings, logger)

	return client, db, ctx, cleanup
}

// initAerospikeOwnServerWithURL is initAerospikeOwnServer, also returning the URL the store was
// built from.
func initAerospikeOwnServerWithURL(t *testing.T, settings *settings.Settings, logger ulogger.Logger) (*uaerospike.Client, *teranode_aerospike.Store, context.Context, func(), *url.URL) {
	host, port := startOwnAerospikeServer(t, aerospikeContainerImage)

	return openStore(t, host, port, false, settings, logger, nil)
}

// initAerospikeWith is initAerospike on a server of the test's own, with beforeStore run on the
// raw client once the namespace accepts writes and before the store is constructed.
func initAerospikeWith(t *testing.T, settings *settings.Settings, logger ulogger.Logger, beforeStore func(*uaerospike.Client)) (*uaerospike.Client, *teranode_aerospike.Store, context.Context, func()) {
	client, db, ctx, cleanup, _ := initAerospikeWithURL(t, settings, logger, beforeStore)

	return client, db, ctx, cleanup
}

// initAerospikeWithURL is initAerospikeWith, also returning the URL the store was built from. With
// no beforeStore it uses the shared server, since nothing configures the server first.
func initAerospikeWithURL(t *testing.T, settings *settings.Settings, logger ulogger.Logger, beforeStore func(*uaerospike.Client)) (*uaerospike.Client, *teranode_aerospike.Store, context.Context, func(), *url.URL) {
	if beforeStore != nil {
		host, port := startOwnAerospikeServer(t, aerospikeContainerImage)

		return openStore(t, host, port, false, settings, logger, beforeStore)
	}

	server := teranode_aerospike.AcquireSharedTestServer(t, aerospikeContainerImage, "", settings.UtxoStore.UtxoBatchSize)

	return openStore(t, server.Host, server.Port, true, settings, logger, nil)
}

// conflictWALScansMu is held by every test or subtest that reads the process-wide count of
// conflict WAL fallback scans or can add to it, so no such test sees another's scans.
var conflictWALScansMu sync.Mutex

// lockConflictWALScans holds conflictWALScansMu until the test ends.
func lockConflictWALScans(t *testing.T) {
	t.Helper()

	conflictWALScansMu.Lock()
	t.Cleanup(conflictWALScansMu.Unlock)
}

// startOwnAerospikeServer starts a container for this test alone and terminates it at cleanup.
func startOwnAerospikeServer(t *testing.T, image string) (string, int) {
	t.Helper()

	teranode_aerospike.AcquireOwnServerSlot(t)

	ctx := context.Background()

	var (
		container *aeroTest.Container
		err       error
	)
	func() {
		defer func() {
			if r := recover(); r != nil {
				// Convert panic to error so we can skip cleanly
				err = errors.NewError("container startup panic: %v", r)
			}
		}()

		container, err = aeroTest.RunContainer(ctx, aeroTest.WithTTLSupport(aerospikeNamespace), aeroTest.WithImage(image))
	}()
	// Skips only when the container runtime is unreachable; any other failure fails the test.
	test.SkipIfContainerUnavailable(t, err)

	teranode_aerospike.CountContainerStart("own", t.Name())

	t.Cleanup(func() {
		require.NoError(t, container.Terminate(ctx))
	})

	host, err := container.Host(ctx)
	require.NoError(t, err)

	port, err := container.ServicePort(ctx)
	require.NoError(t, err)

	return host, port
}

// openStore connects a raw client and a store to the server at host:port. shared says other tests
// use the server too.
func openStore(t *testing.T, host string, port int, shared bool, settings *settings.Settings, logger ulogger.Logger, beforeStore func(*uaerospike.Client)) (*uaerospike.Client, *teranode_aerospike.Store, context.Context, func(), *url.URL) {
	t.Helper()

	teranode_aerospike.InitPrometheusMetrics()

	ctx := context.Background()

	// raw client to be able to do gets and cleanup
	client, aeroErr := uaerospike.NewClient(host, port)
	require.NoError(t, aeroErr)

	// testcontainers returns as soon as the service port responds, but Aerospike briefly rejects
	// writes with FAIL_FORBIDDEN while the namespace finishes coming up.
	require.NoError(t, teranode_aerospike.WaitForWritesReady(client, aerospikeNamespace, aerospikeSet))

	if beforeStore != nil {
		beforeStore(client)
	}

	aerospikeContainerURL := fmt.Sprintf("aerospike://%s:%d/%s?set=%s&block_retention=%d&externalStore=file://./data/externalStore", host, port, aerospikeNamespace, aerospikeSet, aerospikeBlockRetention)
	aeroURL, err := url.Parse(aerospikeContainerURL)
	require.NoError(t, err)

	// TERANODE_TEST_AEROSPIKE_NATIVE_OPS=true runs the store on the native mod-teranode operate path,
	// which only a fork image serves; on a stock image its probe falls back to the UDF path.
	if os.Getenv("TERANODE_TEST_AEROSPIKE_NATIVE_OPS") == "true" {
		settings.Aerospike.UseNativeTeranodeOps = true
	}

	// teranode db client
	var db *teranode_aerospike.Store
	db, err = teranode_aerospike.New(ctx, logger, settings, aeroURL)
	require.NoError(t, err)

	db.SetExternalStore(memory.New())

	if shared {
		db.LeaveSharedClientOpenOnClose()
	} else {
		// Close the store's cached client before the container stops (cleanups run last first), so
		// it does not keep reconnecting to a server that is gone for the rest of the run.
		t.Cleanup(func() { util.CloseAerospikeClient(aeroURL.Host) })
	}

	return client, db, ctx, func() {
		db.StopConflictingIndexSelfTest()
		client.Close()
	}, aeroURL
}

func cleanDB(t *testing.T, client *uaerospike.Client) {
	cleanSet(t, client, aerospikeSet)
}

func cleanSet(t *testing.T, client *uaerospike.Client, set string) {
	tSettings := test.CreateBaseTestSettings(t)

	policy := util.GetAerospikeWritePolicy(tSettings, 0)

	// Scan and delete all records in the set
	scanPolicy := aerospike.NewScanPolicy()
	recordSet, err := client.ScanAll(scanPolicy, aerospikeNamespace, set)
	require.NoError(t, err)

	for result := range recordSet.Results() {
		if result.Err != nil {
			t.Logf("Error getting record: %v", result.Err)
			continue
		}

		require.NotNil(t, result.Record)
		require.NotNil(t, result.Record.Key)

		_, err = client.Delete(policy, result.Record.Key)
		require.NoError(t, err)
	}
}

func setupStore(t *testing.T, client *uaerospike.Client) *teranode_aerospike.Store {
	s := &teranode_aerospike.Store{}

	s.SetSettings(test.CreateBaseTestSettings(t))
	s.SetLogger(ulogger.TestLogger{})
	s.SetUtxoBatchSize(100)
	s.SetClient(client)
	s.SetExternalStore(memory.New())
	s.SetNamespace(aerospikeNamespace)
	s.SetName(aerospikeSet)
	s.SetBlockHeightRetention(10)

	return s
}

func readTransaction(t *testing.T, filePath string) *bt.Tx {
	txHex, err := os.ReadFile(filePath)
	require.NoError(t, err)

	tx, err := bt.NewTxFromString(string(txHex))
	require.NoError(t, err)

	return tx
}

func prepareBatchStoreItem(t *testing.T, s *teranode_aerospike.Store, tx *bt.Tx, blockHeight uint32, blockIDs []uint32, blockHeights []uint32, subtreeIdxs []int) (*teranode_aerospike.BatchStoreItem, [][]*aerospike.Bin) {
	txHash := tx.TxIDChainHash()
	isCoinbase := tx.IsCoinbase()

	binsToStore, err := s.GetBinsToStore(tx, blockHeight, blockIDs, blockHeights, subtreeIdxs, true, txHash, isCoinbase, false, false, lane.None, nil)
	require.NoError(t, err)
	require.NotNil(t, binsToStore)

	bItem := teranode_aerospike.NewBatchStoreItem(
		txHash,
		isCoinbase,
		tx,
		blockHeight,
		blockIDs,
		0,
		completion.NewGroup(1),
	)

	return bItem, binsToStore
}
