//go:build test_all || test_stores || test_utxo || test_aerospike

package aerospike

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"testing"

	"github.com/aerospike/aerospike-client-go/v7"
	aeroTest "github.com/bitcoin-sv/testcontainers-aerospike-go"
	"github.com/bitcoin-sv/ubsv/stores/blob/memory"
	"github.com/bitcoin-sv/ubsv/stores/utxo"
	ubsv_aerospike "github.com/bitcoin-sv/ubsv/stores/utxo/aerospike"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/bitcoin-sv/ubsv/util/test"
	"github.com/bitcoin-sv/ubsv/util/uaerospike"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/stretchr/testify/require"
)

const (
	aerospikeHost       = "localhost" // "localhost"
	aerospikePort       = 3000        // 3800
	aerospikeNamespace  = "test"      // test
	aerospikeSet        = "test"      // utxo-test
	aerospikeExpiration = uint32(1)
)

var (
	coinbaseKey      *aerospike.Key
	tx, _            = bt.NewTxFromString("010000000000000000ef0152a9231baa4e4b05dc30c8fbb7787bab5f460d4d33b039c39dd8cc006f3363e4020000006b483045022100ce3605307dd1633d3c14de4a0cf0df1439f392994e561b648897c4e540baa9ad02207af74878a7575a95c9599e9cdc7e6d73308608ee59abcd90af3ea1a5c0cca41541210275f8390df62d1e951920b623b8ef9c2a67c4d2574d408e422fb334dd1f3ee5b6ffffffff706b9600000000001976a914a32f7eaae3afd5f73a2d6009b93f91aa11d16eef88ac05404b4c00000000001976a914aabb8c2f08567e2d29e3a64f1f833eee85aaf74d88ac80841e00000000001976a914a4aff400bef2fa074169453e703c611c6b9df51588ac204e0000000000001976a9144669d92d46393c38594b2f07587f01b3e5289f6088ac204e0000000000001976a914a461497034343a91683e86b568c8945fb73aca0288ac99fe2a00000000001976a914de7850e419719258077abd37d4fcccdb0a659b9388ac00000000")
	spendingTxID1, _ = chainhash.NewHashFromStr("5e3bc5947f48cec766090aa17f309fd16259de029dcef5d306b514848c9687c7")
	spendingTxID2, _ = chainhash.NewHashFromStr("663bc5947f48cec766090aa17f309fd16259de029dcef5d306b514848c9687c8")

	coinbaseTx, _ = bt.NewTxFromString("01000000010000000000000000000000000000000000000000000000000000000000000000ffffffff17032dff0c2f71646c6e6b2f5e931c7f7b6199adf35e1300ffffffff01d15fa012000000001976a91417db35d440a673a218e70a5b9d07f895facf50d288ac00000000")

	utxoHash0, _ = util.UTXOHashFromOutput(tx.TxIDChainHash(), tx.Outputs[0], 0)
	utxoHash1, _ = util.UTXOHashFromOutput(tx.TxIDChainHash(), tx.Outputs[1], 1)
	utxoHash2, _ = util.UTXOHashFromOutput(tx.TxIDChainHash(), tx.Outputs[2], 2)
	utxoHash3, _ = util.UTXOHashFromOutput(tx.TxIDChainHash(), tx.Outputs[3], 3)
	utxoHash4, _ = util.UTXOHashFromOutput(tx.TxIDChainHash(), tx.Outputs[4], 4)

	txWithOPReturn, _ = bt.NewTxFromString("010000000000000000ef01977da9cf1e56bc7447e6561aa7d404e06343c3fd6034d5934eedddb222a928cc010000006b483045022100f7cd34af663f7ff3ab447476c1078610b0a258e88241bc98f93bec1275c65ace02205945dc2be5e855846e428c58e3758413b3f531f59a53528a3e4a75dfa09e894b4121033188d07302a394cdefba66bf83adf52b0922f16251a8dfb448cca061617f8953fffffffff5262400000000001976a9147f07da316209da8f3250d5ef06aa4fdf5179ffe288ac0200000000000000008a6a22314c74794d45366235416e4d6f70517242504c6b3446474e3855427568784b71726e0101357b2274223a32312e36362c2268223a38332c2270223a313031332c2263223a31372c227773223a312e35372c227764223a3232357d22314361674478397973596b4b79667952524a524d78793737454256776a64344c52780a31353638343830323731a2252400000000001976a9147f07da316209da8f3250d5ef06aa4fdf5179ffe288ac00000000")

	spend = &utxo.Spend{
		TxID:         tx.TxIDChainHash(),
		Vout:         0,
		UTXOHash:     utxoHash0,
		SpendingTxID: spendingTxID1,
	}
	spends = []*utxo.Spend{spend}

	spends2 = []*utxo.Spend{{
		TxID:         tx.TxIDChainHash(),
		Vout:         0,
		UTXOHash:     utxoHash0,
		SpendingTxID: spendingTxID2,
	}}
	spends3 = []*utxo.Spend{{
		TxID:         tx.TxIDChainHash(),
		Vout:         0,
		UTXOHash:     utxoHash0,
		SpendingTxID: utxoHash3,
	}}
	spendsAll = []*utxo.Spend{{
		TxID:         tx.TxIDChainHash(),
		Vout:         0,
		UTXOHash:     utxoHash0,
		SpendingTxID: spendingTxID2,
	}, {
		TxID:         tx.TxIDChainHash(),
		Vout:         1,
		UTXOHash:     utxoHash1,
		SpendingTxID: spendingTxID2,
	}, {
		TxID:         tx.TxIDChainHash(),
		Vout:         2,
		UTXOHash:     utxoHash2,
		SpendingTxID: spendingTxID2,
	}, {
		TxID:         tx.TxIDChainHash(),
		Vout:         3,
		UTXOHash:     utxoHash3,
		SpendingTxID: spendingTxID2,
	}, {
		TxID:         tx.TxIDChainHash(),
		Vout:         4,
		UTXOHash:     utxoHash4,
		SpendingTxID: spendingTxID2,
	}}
)

func initAerospike(t *testing.T) (*aerospike.Client, *ubsv_aerospike.Store, context.Context, func()) {
	ubsv_aerospike.InitPrometheusMetrics()

	ctx := context.Background()

	container, err := aeroTest.RunContainer(ctx, aeroTest.WithImage("aerospike:ce-6.4.0.7_2"))
	require.NoError(t, err)

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

	aerospikeContainerURL := fmt.Sprintf("aerospike://%s:%d/%s?set=%s&expiration=%d&externalStore=file://./data/externalStore", host, port, aerospikeNamespace, aerospikeSet, aerospikeExpiration)
	aeroURL, err := url.Parse(aerospikeContainerURL)
	require.NoError(t, err)

	tSettings := test.CreateBaseTestSettings()

	// ubsv db client
	var db *ubsv_aerospike.Store
	db, err = ubsv_aerospike.New(ctx, ulogger.TestLogger{}, tSettings, aeroURL)
	require.NoError(t, err)

	db.SetClient(&uaerospike.Client{Client: client})
	db.SetNamespace("test")
	db.SetName("test")

	db.SetExternalStore(memory.New())

	return client, db, ctx, func() {
		client.Close()
	}
}

func cleanDB(t *testing.T, client *aerospike.Client, key *aerospike.Key, txs ...*bt.Tx) {
	policy := util.GetAerospikeWritePolicy(0, aerospike.TTLDontExpire)

	_, err := client.Delete(policy, key)
	require.NoError(t, err)

	if coinbaseKey != nil {
		_, err = client.Delete(policy, coinbaseKey)
		require.NoError(t, err)
	}

	if len(txs) > 0 {
		for _, tx := range txs {
			key, _ = aerospike.NewKey(aerospikeNamespace, aerospikeSet, tx.TxIDChainHash()[:])
			_, err = client.Delete(policy, key)
			require.NoError(t, err)
		}
	}
}

func printResponse(response *aerospike.Record) {
	fmt.Printf("Digest      : %x\n", response.Key.Digest())
	fmt.Printf("Namespace   : %s\n", response.Key.Namespace())
	fmt.Printf("SetName     : %s\n", response.Key.SetName())
	fmt.Printf("Node        : %s\n", response.Node.GetName())
	fmt.Printf("Bins        :")

	var indent = false

	for binName := range response.Bins {
		if indent {
			fmt.Printf("            : %s\n", binName)
		} else {
			fmt.Printf(" %s\n", binName)
		}

		indent = true
	}

	fmt.Printf("Generation  : %d\n", response.Generation)
	fmt.Printf("Expiration  : %d\n", response.Expiration)

	fmt.Println()

	for k, v := range response.Bins {
		switch k {
		case "Generation":
			fallthrough
		case "Expiration":
			fallthrough
		case "inputs":
			fallthrough
		case "outputs":
			fallthrough
		case "utxos":

		default:
			if arr, ok := v.([]interface{}); ok {
				printArray(k, arr)
			} else if b, ok := v.([]byte); ok {
				fmt.Printf("%-12s: %x\n", k, b)
			} else {
				fmt.Printf("%-12s: %v\n", k, v)
			}
		}
	}

	printArray("inputs", response.Bins["inputs"])
	printArray("outputs", response.Bins["outputs"])
	printArray("utxos", response.Bins["utxos"])

	fmt.Println()
}

func printArray(name string, ifc interface{}) {
	fmt.Printf("%-12s:", name)

	if ifc == nil {
		fmt.Printf(" <nil>\n")
		return
	}

	arr, ok := ifc.([]interface{})
	if !ok {
		fmt.Printf(" <not array>\n")
		return
	}

	if len(arr) == 0 {
		fmt.Printf(" <empty>\n")
		return
	}

	for i, item := range arr {
		if b, ok := item.([]byte); ok {
			if i == 0 {
				fmt.Printf(" %-5d : %x\n", i, b)
			} else {
				fmt.Printf("            : %-5d : %x\n", i, b)
			}
		} else {
			if i == 0 {
				fmt.Printf(" %-5d : %v\n", i, item)
			} else {
				fmt.Printf("            : %-5d : %v\n", i, item)
			}
		}
	}
}

func setupStore(_ *testing.T, client *aerospike.Client) *ubsv_aerospike.Store {
	s := &ubsv_aerospike.Store{}
	s.SetUtxoBatchSize(100)
	s.SetClient(&uaerospike.Client{Client: client})
	s.SetExternalStore(memory.New())
	s.SetNamespace(aerospikeNamespace)
	s.SetName(aerospikeSet)
	s.SetExpiration(10)

	return s
}

func readTransaction(t *testing.T, filePath string) *bt.Tx {
	txHex, err := os.ReadFile(filePath)
	require.NoError(t, err)

	tx, err := bt.NewTxFromString(string(txHex))
	require.NoError(t, err)

	return tx
}

func prepareBatchStoreItem(t *testing.T, s *ubsv_aerospike.Store, tx *bt.Tx, blockHeight uint32, blockIDs []uint32) (*ubsv_aerospike.BatchStoreItem, [][]*aerospike.Bin, bool) {
	txHash := tx.TxIDChainHash()
	isCoinbase := tx.IsCoinbase()

	binsToStore, hasUtxos, err := s.GetBinsToStore(tx, blockHeight, blockIDs, true, txHash, isCoinbase)
	require.NoError(t, err)
	require.NotNil(t, binsToStore)

	bItem := ubsv_aerospike.NewBatchStoreItem(
		txHash,
		isCoinbase,
		tx,
		blockHeight,
		blockIDs,
		0,
		make(chan error, 1),
	)

	return bItem, binsToStore, hasUtxos
}
