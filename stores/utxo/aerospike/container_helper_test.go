package aerospike_test

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"testing"

	"github.com/aerospike/aerospike-client-go/v8"
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
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/bsv-blockchain/teranode/util/uaerospike"
	aeroTest "github.com/bsv-blockchain/testcontainers-aerospike-go"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
)

const (
	aerospikeHost           = "localhost" // "localhost"
	aerospikePort           = 3000        // 3800
	aerospikeNamespace      = "test"      // test
	aerospikeSet            = "test"      // utxo-test
	aerospikeBlockRetention = 1
)

var (
	coinbaseTXKey    *aerospike.Key
	tx, _            = bt.NewTxFromString("010000000000000000ef0152a9231baa4e4b05dc30c8fbb7787bab5f460d4d33b039c39dd8cc006f3363e4020000006b483045022100ce3605307dd1633d3c14de4a0cf0df1439f392994e561b648897c4e540baa9ad02207af74878a7575a95c9599e9cdc7e6d73308608ee59abcd90af3ea1a5c0cca41541210275f8390df62d1e951920b623b8ef9c2a67c4d2574d408e422fb334dd1f3ee5b6ffffffff706b9600000000001976a914a32f7eaae3afd5f73a2d6009b93f91aa11d16eef88ac05404b4c00000000001976a914aabb8c2f08567e2d29e3a64f1f833eee85aaf74d88ac80841e00000000001976a914a4aff400bef2fa074169453e703c611c6b9df51588ac204e0000000000001976a9144669d92d46393c38594b2f07587f01b3e5289f6088ac204e0000000000001976a914a461497034343a91683e86b568c8945fb73aca0288ac99fe2a00000000001976a914de7850e419719258077abd37d4fcccdb0a659b9388ac00000000")
	spendingTxID1, _ = chainhash.NewHashFromStr("5e3bc5947f48cec766090aa17f309fd16259de029dcef5d306b514848c9687c7")
	spendingTxID2, _ = chainhash.NewHashFromStr("663bc5947f48cec766090aa17f309fd16259de029dcef5d306b514848c9687c8")

	coinbaseTx, _ = bt.NewTxFromString("01000000010000000000000000000000000000000000000000000000000000000000000000ffffffff17032dff0c2f71646c6e6b2f5e931c7f7b6199adf35e1300ffffffff01d15fa012000000001976a91417db35d440a673a218e70a5b9d07f895facf50d288ac00000000")

	spendCoinbaseTx = utxo2.GetSpendingTx(coinbaseTx, 0)

	utxoHash0, _ = util.UTXOHashFromOutput(tx.TxIDChainHash(), tx.Outputs[0], 0)
	utxoHash1, _ = util.UTXOHashFromOutput(tx.TxIDChainHash(), tx.Outputs[1], 1)
	utxoHash2, _ = util.UTXOHashFromOutput(tx.TxIDChainHash(), tx.Outputs[2], 2)
	utxoHash3, _ = util.UTXOHashFromOutput(tx.TxIDChainHash(), tx.Outputs[3], 3)
	utxoHash4, _ = util.UTXOHashFromOutput(tx.TxIDChainHash(), tx.Outputs[4], 4)

	txWithOPReturn, _ = bt.NewTxFromString("010000000000000000ef01977da9cf1e56bc7447e6561aa7d404e06343c3fd6034d5934eedddb222a928cc010000006b483045022100f7cd34af663f7ff3ab447476c1078610b0a258e88241bc98f93bec1275c65ace02205945dc2be5e855846e428c58e3758413b3f531f59a53528a3e4a75dfa09e894b4121033188d07302a394cdefba66bf83adf52b0922f16251a8dfb448cca061617f8953fffffffff5262400000000001976a9147f07da316209da8f3250d5ef06aa4fdf5179ffe288ac0200000000000000008a6a22314c74794d45366235416e4d6f70517242504c6b3446474e3855427568784b71726e0101357b2274223a32312e36362c2268223a38332c2270223a313031332c2263223a31372c227773223a312e35372c227764223a3232357d22314361674478397973596b4b79667952524a524d78793737454256776a64344c52780a31353638343830323731a2252400000000001976a9147f07da316209da8f3250d5ef06aa4fdf5179ffe288ac00000000")

	spendTx = utxo2.GetSpendingTx(tx, 0)

	spend = &utxo.Spend{
		TxID:         tx.TxIDChainHash(),
		Vout:         0,
		UTXOHash:     utxoHash0,
		SpendingData: spendpkg.NewSpendingData(spendingTxID1, 0),
	}
	spends = []*utxo.Spend{spend}

	spendTx2 = utxo2.GetSpendingTx(tx, 0)

	spendTx3 = utxo2.GetSpendingTx(tx, 0)

	spendTxAll = utxo2.GetSpendingTx(tx, 0, 1, 2, 3, 4)

	spendsAll = []*utxo.Spend{{
		TxID:         tx.TxIDChainHash(),
		Vout:         0,
		UTXOHash:     utxoHash0,
		SpendingData: spendpkg.NewSpendingData(spendingTxID2, 0),
	}, {
		TxID:         tx.TxIDChainHash(),
		Vout:         1,
		UTXOHash:     utxoHash1,
		SpendingData: spendpkg.NewSpendingData(spendingTxID2, 1),
	}, {
		TxID:         tx.TxIDChainHash(),
		Vout:         2,
		UTXOHash:     utxoHash2,
		SpendingData: spendpkg.NewSpendingData(spendingTxID2, 2),
	}, {
		TxID:         tx.TxIDChainHash(),
		Vout:         3,
		UTXOHash:     utxoHash3,
		SpendingData: spendpkg.NewSpendingData(spendingTxID2, 0),
	}, {
		TxID:         tx.TxIDChainHash(),
		Vout:         4,
		UTXOHash:     utxoHash4,
		SpendingData: spendpkg.NewSpendingData(spendingTxID2, 0),
	}}

	spendTxRemaining = utxo2.GetSpendingTx(tx, 1, 2, 3, 4)
)

func initAerospike(t *testing.T, settings *settings.Settings, logger ulogger.Logger) (*uaerospike.Client, *teranode_aerospike.Store, context.Context, func()) {
	teranode_aerospike.InitPrometheusMetrics()

	ctx := context.Background()

	var (
		containerAny any
		err          error
	)
	func() {
		defer func() {
			if r := recover(); r != nil {
				// Convert panic to error so we can skip cleanly
				err = errors.NewError("container startup panic: %v", r)
			}
		}()
		c, e := aeroTest.RunContainer(ctx, aeroTest.WithTTLSupport(aerospikeNamespace))
		if e != nil {
			err = e
			return
		}
		containerAny = c
	}()
	if err != nil {
		t.Skipf("Skipping Aerospike integration tests: container not available (%v)", err)
	}

	// Assert required container interface
	type containerAPI interface {
		Terminate(ctx context.Context, opts ...testcontainers.TerminateOption) error
		Host(ctx context.Context) (string, error)
		ServicePort(ctx context.Context) (int, error)
	}

	container, ok := containerAny.(containerAPI)
	if !ok {
		t.Error("Aerospike integration tests: unexpected container type")
		t.FailNow()
	}

	// go func() {
	// 	reader, err := container.Logs(ctx)
	// 	if err != nil {
	// 		log.Fatalf("Failed to fetch logs: %v", err)
	// 	}
	// 	defer reader.Close()

	// 	scanner := bufio.NewScanner(reader)
	// 	for scanner.Scan() {
	// 		fmt.Println(scanner.Text()) // Print each log line as it appears
	// 	}

	// 	if err := scanner.Err(); err != nil {
	// 		log.Printf("Error reading container logs: %v", err)
	// 	}
	// }()

	t.Cleanup(func() {
		err = container.Terminate(ctx)
		require.NoError(t, err)
	})

	host, err := container.Host(ctx)
	require.NoError(t, err)

	port, err := container.ServicePort(ctx)
	require.NoError(t, err)

	// raw client to be able to do gets and cleanup
	client, aeroErr := uaerospike.NewClient(host, port)
	require.NoError(t, aeroErr)

	aerospikeContainerURL := fmt.Sprintf("aerospike://%s:%d/%s?set=%s&block_retention=%d&externalStore=file://./data/externalStore", host, port, aerospikeNamespace, aerospikeSet, aerospikeBlockRetention)
	aeroURL, err := url.Parse(aerospikeContainerURL)
	require.NoError(t, err)

	// teranode db client
	var db *teranode_aerospike.Store
	db, err = teranode_aerospike.New(ctx, logger, settings, aeroURL)
	require.NoError(t, err)

	db.SetExternalStore(memory.New())

	return client, db, ctx, func() {
		client.Close()
	}
}

func cleanDB(t *testing.T, client *uaerospike.Client) {
	tSettings := test.CreateBaseTestSettings(t)

	policy := util.GetAerospikeWritePolicy(tSettings, 0)

	// Scan and delete all records in the set
	scanPolicy := aerospike.NewScanPolicy()
	recordSet, err := client.ScanAll(scanPolicy, aerospikeNamespace, aerospikeSet)
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

	binsToStore, err := s.GetBinsToStore(tx, blockHeight, blockIDs, blockHeights, subtreeIdxs, true, txHash, isCoinbase, false, false)
	require.NoError(t, err)
	require.NotNil(t, binsToStore)

	bItem := teranode_aerospike.NewBatchStoreItem(
		txHash,
		isCoinbase,
		tx,
		blockHeight,
		blockIDs,
		0,
		make(chan error, 1),
	)

	return bItem, binsToStore
}
