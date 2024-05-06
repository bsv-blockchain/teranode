package aerospikelua

import (
	"fmt"
	"testing"
	"time"

	_ "embed"

	"github.com/aerospike/aerospike-client-go/v7"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

//go:embed update.lua
var updateLUA []byte

//go:embed ttl.lua
var ttlLUA []byte

//go:embed spend.lua
var spendLUA []byte

func TestIterationDirect(t *testing.T) {
	client, key, wPolicy, cleanAerospike, err := setup()
	require.NoError(t, err)
	defer cleanAerospike()

	err = client.Put(wPolicy, key, aerospike.BinMap{"val": []byte("v1")})
	require.NoError(t, err)

	value, err := client.Get(nil, key, "val")
	require.NoError(t, err)

	assert.Equal(t, []byte("v1"), value.Bins["val"])
}

func TestIterationLua(t *testing.T) {
	client, key, wPolicy, cleanAerospike, err := setup()
	require.NoError(t, err)
	defer cleanAerospike()

	cleanLUA, err := setupLUA(client)
	require.NoError(t, err)
	defer cleanLUA()

	ret, err := client.Execute(wPolicy, key, "update", "updateRecord", aerospike.NewValue("v2"))
	require.NoError(t, err)
	assert.Equal(t, "OK", ret)

	value, err := client.Get(nil, key, "val")
	require.NoError(t, err)

	assert.Equal(t, "v2", value.Bins["val"])
}

func TestSpendLUA(t *testing.T) {
	client, key, wPolicy, cleanAerospike, err := setup()
	require.NoError(t, err)
	defer cleanAerospike()

	cleanLUA, err := setupLUA(client)
	require.NoError(t, err)
	defer cleanLUA()

	utxos := make(map[any]any)
	one := chainhash.HashH([]byte("one")).String()
	two := chainhash.HashH([]byte("two")).String()

	spendingTxID := chainhash.HashH([]byte("spendingTxID")).String()

	utxos[one] = aerospike.NewValue("0")
	utxos[two] = aerospike.NewValue("0")

	bins := []*aerospike.Bin{
		aerospike.NewBin("utxos", aerospike.NewMapValue(utxos)),
	}

	err = client.PutBins(wPolicy, key, bins...)
	require.NoError(t, err)

	ret, err := client.Execute(wPolicy, key, "spend", "spend", aerospike.NewValue(one), aerospike.NewValue(spendingTxID), aerospike.NewValue(uint32(100)), aerospike.NewValue(uint32(10)))
	require.NoError(t, err)
	assert.Equal(t, "OK", ret)

	// Spend again with same txid
	ret, err = client.Execute(wPolicy, key, "spend", "spend", aerospike.NewValue(one), aerospike.NewValue(spendingTxID), aerospike.NewValue(uint32(100)), aerospike.NewValue(uint32(10)))
	require.NoError(t, err)
	assert.Equal(t, "OK", ret)

	// Spend again with different txid
	ret, err = client.Execute(wPolicy, key, "spend", "spend", aerospike.NewValue(one), aerospike.NewValue(two), aerospike.NewValue(uint32(100)), aerospike.NewValue(uint32(10)))
	require.NoError(t, err)
	assert.Equal(t, "SPENT,3d0a1b051292265cfd4e0606df36e14063807292d1dd9e6541d6955d8955d8e1", ret)

	value, err := client.Get(nil, key, "utxos")
	require.NoError(t, err)

	m, ok := value.Bins["utxos"].(map[interface{}]interface{})
	require.True(t, ok)

	assert.Len(t, m, 2)

	t.Logf("%+v", m[one])

}

func TestTTLWithLUA(t *testing.T) {
	client, key, wPolicy, cleanAerospike, err := setup()
	require.NoError(t, err)
	defer cleanAerospike()

	cleanLUA, err := setupLUA(client)
	require.NoError(t, err)
	defer cleanLUA()

	value, err := client.Get(nil, key, "val")
	require.NoError(t, err)
	assert.Equal(t, []byte("v"), value.Bins["val"])

	ret, err := client.Execute(wPolicy, key, "ttl", "setTTL")
	require.NoError(t, err)
	assert.Equal(t, "OK", ret)

	value, err = client.Get(nil, key, "val")
	require.NoError(t, err)
	assert.Equal(t, []byte("v"), value.Bins["val"])

	time.Sleep(2 * time.Second)

	value, err = client.Get(nil, key, "val")
	require.ErrorIs(t, err, aerospike.ErrKeyNotFound)

	t.Logf("%#v", value)
}

// Write a benchmark test for the Aerospike client
func BenchmarkIteration(b *testing.B) {
	client, key, wPolicy, cleanAerospike, err := setup()
	require.NoError(b, err)
	defer cleanAerospike()

	for i := 0; i < b.N; i++ {
		if err := client.Put(wPolicy, key, aerospike.BinMap{"val": []byte(fmt.Sprintf("v%d", i))}); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkIterationLUA(b *testing.B) {
	b.StopTimer()

	client, key, wPolicy, cleanAerospike, err := setup()
	require.NoError(b, err)

	cleanLUA, err := setupLUA(client)
	require.NoError(b, err)

	defer func() {
		b.StopTimer()
		cleanLUA()
		cleanAerospike()
	}()

	b.ResetTimer()
	b.StartTimer()

	for i := 0; i < b.N; i++ {
		if _, err := client.Execute(wPolicy, key, "update", "updateRecord", aerospike.NewValue(fmt.Sprintf("v%d", i))); err != nil {
			b.Fatal(err)
		}
	}
}

func setup() (*aerospike.Client, *aerospike.Key, *aerospike.WritePolicy, func(), error) {
	client, err := aerospike.NewClient("localhost", 3000)
	if err != nil {
		return nil, nil, nil, nil, err
	}

	namespace := "test"

	key, err := aerospike.NewKey(namespace, "test", []byte("k"))
	if err != nil {
		return nil, nil, nil, nil, err
	}

	_, err = client.Delete(nil, key)
	if err != nil {
		return nil, nil, nil, nil, err
	}

	wPolicy := util.GetAerospikeWritePolicy(0, 0)
	wPolicy.CommitLevel = aerospike.COMMIT_ALL // strong consistency
	wPolicy.RecordExistsAction = aerospike.CREATE_ONLY

	bins := aerospike.BinMap{
		"val": []byte("v"),
	}

	err = client.Put(wPolicy, key, bins)
	if err != nil {
		return nil, nil, nil, nil, err
	}

	wPolicy.RecordExistsAction = aerospike.UPDATE

	value, err := client.Get(nil, key, "val")
	if err != nil {
		return nil, nil, nil, nil, err
	}

	if string(value.Bins["val"].([]byte)) != "v" {
		return nil, nil, nil, nil, fmt.Errorf("unexpected value: %v", value.Bins["val"])
	}

	return client, key, wPolicy, func() {
		client.Close()
	}, nil
}

func setupLUA(client *aerospike.Client) (func(), error) {
	task1, err := client.RegisterUDF(nil, updateLUA, "update.lua", aerospike.LUA)
	if err != nil {
		return nil, err
	}

	task2, err := client.RegisterUDF(nil, ttlLUA, "ttl.lua", aerospike.LUA)
	if err != nil {
		return nil, err
	}

	task3, err := client.RegisterUDF(nil, spendLUA, "spend.lua", aerospike.LUA)
	if err != nil {
		return nil, err
	}

	// Wait for the task to complete
	<-task1.OnComplete()
	<-task2.OnComplete()
	<-task3.OnComplete()

	return func() {
		if task, err := client.RemoveUDF(nil, "update.lua"); err == nil {
			// Wait for task to complete
			<-task.OnComplete()
		}

		if task, err := client.RemoveUDF(nil, "ttl.lua"); err == nil {
			// Wait for task to complete
			<-task.OnComplete()
		}

		if task, err := client.RemoveUDF(nil, "spend.lua"); err == nil {
			// Wait for task to complete
			<-task.OnComplete()
		}

	}, nil
}
