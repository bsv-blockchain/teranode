//go:build test_all || test_stores || test_utxo || test_stores_redis2

package redis2

import (
	"context"
	"fmt"
	"net/url"
	"testing"
	"time"

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/stores/utxo"
	"github.com/bitcoin-sv/teranode/stores/utxo/meta"
	storeRedis "github.com/bitcoin-sv/teranode/stores/utxo/redis2"
	"github.com/bitcoin-sv/teranode/stores/utxo/tests"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/bitcoin-sv/teranode/util"
	redisTest "github.com/bitcoin-sv/testcontainers-redis-go"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/chainhash"
	redis_db "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// go test -v -tags test_stores_redis2 ./test/...

var (
	coinbaseKey *chainhash.Hash
	tx, _       = bt.NewTxFromString("010000000000000000ef0152a9231baa4e4b05dc30c8fbb7787bab5f460d4d33b039c39dd8cc006f3363e4020000006b483045022100ce3605307dd1633d3c14de4a0cf0df1439f392994e561b648897c4e540baa9ad02207af74878a7575a95c9599e9cdc7e6d73308608ee59abcd90af3ea1a5c0cca41541210275f8390df62d1e951920b623b8ef9c2a67c4d2574d408e422fb334dd1f3ee5b6ffffffff706b9600000000001976a914a32f7eaae3afd5f73a2d6009b93f91aa11d16eef88ac05404b4c00000000001976a914aabb8c2f08567e2d29e3a64f1f833eee85aaf74d88ac80841e00000000001976a914a4aff400bef2fa074169453e703c611c6b9df51588ac204e0000000000001976a9144669d92d46393c38594b2f07587f01b3e5289f6088ac204e0000000000001976a914a461497034343a91683e86b568c8945fb73aca0288ac99fe2a00000000001976a914de7850e419719258077abd37d4fcccdb0a659b9388ac00000000")

	txHash           = tx.TxIDChainHash()
	spendingTxID1, _ = chainhash.NewHashFromStr("5e3bc5947f48cec766090aa17f309fd16259de029dcef5d306b514848c9687c7")
	spendingTxID2, _ = chainhash.NewHashFromStr("663bc5947f48cec766090aa17f309fd16259de029dcef5d306b514848c9687c8")

	coinbaseTx, _ = bt.NewTxFromString("01000000010000000000000000000000000000000000000000000000000000000000000000ffffffff17032dff0c2f71646c6e6b2f5e931c7f7b6199adf35e1300ffffffff01d15fa012000000001976a91417db35d440a673a218e70a5b9d07f895facf50d288ac00000000")

	utxoHash0, _ = util.UTXOHashFromOutput(txHash, tx.Outputs[0], 0)
	utxoHash1, _ = util.UTXOHashFromOutput(txHash, tx.Outputs[1], 1)
	utxoHash2, _ = util.UTXOHashFromOutput(txHash, tx.Outputs[2], 2)
	utxoHash3, _ = util.UTXOHashFromOutput(txHash, tx.Outputs[3], 3)
	utxoHash4, _ = util.UTXOHashFromOutput(txHash, tx.Outputs[4], 4)

	txWithOPReturn, _ = bt.NewTxFromString("010000000000000000ef01977da9cf1e56bc7447e6561aa7d404e06343c3fd6034d5934eedddb222a928cc010000006b483045022100f7cd34af663f7ff3ab447476c1078610b0a258e88241bc98f93bec1275c65ace02205945dc2be5e855846e428c58e3758413b3f531f59a53528a3e4a75dfa09e894b4121033188d07302a394cdefba66bf83adf52b0922f16251a8dfb448cca061617f8953fffffffff5262400000000001976a9147f07da316209da8f3250d5ef06aa4fdf5179ffe288ac0200000000000000008a6a22314c74794d45366235416e4d6f70517242504c6b3446474e3855427568784b71726e0101357b2274223a32312e36362c2268223a38332c2270223a313031332c2263223a31372c227773223a312e35372c227764223a3232357d22314361674478397973596b4b79667952524a524d78793737454256776a64344c52780a31353638343830323731a2252400000000001976a9147f07da316209da8f3250d5ef06aa4fdf5179ffe288ac00000000")

	spend = &utxo.Spend{
		TxID:         txHash,
		Vout:         0,
		UTXOHash:     utxoHash0,
		SpendingTxID: spendingTxID1,
	}
	spends = []*utxo.Spend{spend}

	spends2 = []*utxo.Spend{{
		TxID:         txHash,
		Vout:         0,
		UTXOHash:     utxoHash0,
		SpendingTxID: spendingTxID2,
	}}

	spends3 = []*utxo.Spend{{
		TxID:         txHash,
		Vout:         0,
		UTXOHash:     utxoHash0,
		SpendingTxID: utxoHash3,
	}}

	spendsAll = []*utxo.Spend{{
		TxID:         txHash,
		Vout:         0,
		UTXOHash:     utxoHash0,
		SpendingTxID: spendingTxID2,
	}, {
		TxID:         txHash,
		Vout:         1,
		UTXOHash:     utxoHash1,
		SpendingTxID: spendingTxID2,
	}, {
		TxID:         txHash,
		Vout:         2,
		UTXOHash:     utxoHash2,
		SpendingTxID: spendingTxID2,
	}, {
		TxID:         txHash,
		Vout:         3,
		UTXOHash:     utxoHash3,
		SpendingTxID: spendingTxID2,
	}, {
		TxID:         txHash,
		Vout:         4,
		UTXOHash:     utxoHash4,
		SpendingTxID: spendingTxID2,
	}}
)

func TestRedis(t *testing.T) {
	ctx := context.Background()

	store, _, deferFn := initRedis(t)
	defer deferFn()
	redis := store.GetClient()

	parentTxHash := tx.Inputs[0].PreviousTxIDChainHash()

	coinbaseTx, err := bt.NewTxFromString("01000000010000000000000000000000000000000000000000000000000000000000000000ffffffff19031404002f6d332d617369612fdf5128e62eda1a07e94dbdbdffffffff0500ca9a3b000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac00ca9a3b000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac00ca9a3b000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac00ca9a3b000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac00ca9a3b000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac00000000")
	require.NoError(t, err)

	coinbaseKey = coinbaseTx.TxIDChainHash()

	blockID := uint32(123)
	blockID2 := uint32(124)

	t.Cleanup(func() {
		redis.Del(ctx, spendingTxID1.String())
	})

	t.Run("create", func(t *testing.T) {
		// This transaction has 1 OP_RETURN in the middle of the other ouputs.
		tx, err := bt.NewTxFromString("010000000000000000ef02c3c43bc34e9f7e1a2ead4ba7cf4645913272fa64a72b0d9e955864346b779238010000008b483045022100e4a16d5af4b589f81524fbd388b1c1837e5b0212cdb233feb077eadd8072d413022064aba214257cc3320c21ee81784d8caa0f78f2b4058f29c35cda9e19b5e3811c014104b5fd25825746d62249bcde7c245a751c96cea10531058e180635ebe632d8d2e899ea5ec2acd0a9c8c03e62e34017622ac292601b5f75785550a83a058ca2ab60ffffffff706f9800000000001976a9143425c1789ef28eee4d6c2b750adeee1a2bfad8ee88acc3c43bc34e9f7e1a2ead4ba7cf4645913272fa64a72b0d9e955864346b779238030000008c49304602210086814d55093b46dc455d02edaa0b1308a4027034c2ba5d0076d66fc7b21594420221008acac3ed9e96cc5d259eeeb66b14f7da9f13bb36d65f5d07fc8e49dcc9ccfd76014104f5678e5949d2253163ff8a9d902d596cda21376337717c7a4d3e39ee1ee239472d30d5e3882398f4ac9e2dffc0088ae5faaa8502be5cdad6bc999856c450a63affffffff5042d952000000001976a914ba83cee72fe722e4683fc3400ad8313b91b9b64988ac0410270000000000001976a914cd709ef0812e5ec671b7538b2760d41d884f69bb88ac60489800000000001976a9143425c1789ef28eee4d6c2b750adeee1a2bfad8ee88ac0000000000000000096a073132332062726f401bd952000000001976a914ba83cee72fe722e4683fc3400ad8313b91b9b64988ac00000000")
		require.NoError(t, err)

		err = store.Delete(ctx, tx.TxIDChainHash())
		require.NoError(t, err)

		meta, err := store.Create(ctx, tx, 0)
		require.NoError(t, err)
		assert.NotNil(t, meta)

		// raw redis get
		value, err := redis.HGetAll(ctx, tx.TxIDChainHash().String()).Result()
		require.NoError(t, err)

		assert.Equal(t, "10000", value["fee"])
		assert.Equal(t, "491", value["sizeInBytes"])
		assert.Equal(t, "565", value["extendedSize"])
		assert.Equal(t, "0", value["isCoinbase"])
		assert.Equal(t, "0", value["spentUtxos"])
		assert.Equal(t, "10270000000000001976a914cd709ef0812e5ec671b7538b2760d41d884f69bb88ac", value["output:0"])
		assert.Equal(t, "60489800000000001976a9143425c1789ef28eee4d6c2b750adeee1a2bfad8ee88ac", value["output:1"])
		assert.Equal(t, "", value["output:2"]) // OP_RETURN
		assert.Equal(t, "401bd952000000001976a914ba83cee72fe722e4683fc3400ad8313b91b9b64988ac", value["output:3"])
		assert.Equal(t, "14aba4a50c2362293e4b8a3fb456ad9eb00f6acd564df181d10c23cf0038c5f2", value["utxo:0"])
		assert.Equal(t, "1ffdcaaa25a96df2e02a4b5e86fb5d27619082dd862642eb01ac895c9b283c4b", value["utxo:1"])
		assert.Equal(t, "", value["utxo:2"]) // OP_RETURN
		assert.Equal(t, "54179da3b4f21c2e2121e823013dd8a5c07f5e0d7924a4fbf59857d9703abc0c", value["utxo:3"])
	})

	t.Run("redis store", func(t *testing.T) {
		cleanDB(t, redis, txHash, tx)

		_, err = store.Create(ctx, tx, 0)
		require.NoError(t, err)

		// raw redis get
		value, err := redis.HGetAll(ctx, txHash.String()).Result()
		require.NoError(t, err)

		assert.Equal(t, "215", value["fee"])
		assert.Equal(t, "328", value["sizeInBytes"])
		assert.Equal(t, "368", value["extendedSize"])
		assert.Equal(t, "0", value["isCoinbase"])
		assert.Equal(t, "0", value["spentUtxos"])
		assert.Equal(t, "404b4c00000000001976a914aabb8c2f08567e2d29e3a64f1f833eee85aaf74d88ac", value["output:0"])
		assert.Equal(t, "80841e00000000001976a914a4aff400bef2fa074169453e703c611c6b9df51588ac", value["output:1"])
		assert.Equal(t, "204e0000000000001976a9144669d92d46393c38594b2f07587f01b3e5289f6088ac", value["output:2"])
		assert.Equal(t, "204e0000000000001976a914a461497034343a91683e86b568c8945fb73aca0288ac", value["output:3"])
		assert.Equal(t, "99fe2a00000000001976a914de7850e419719258077abd37d4fcccdb0a659b9388ac", value["output:4"])
		assert.Equal(t, "5cee463416702311eace06a42e700f3d95ee7793d3ae52af9c051a4981e8345a", value["utxo:0"])
		assert.Equal(t, "b067b2d2a51cb3f63678cc2bf12efaa5d57235d296bcba09ead42f4147b63bf7", value["utxo:1"])
		assert.Equal(t, "0ab59604a1c249d0cbfe18f01fe423df3035840f9a609395ccd177d2b217cae6", value["utxo:2"])
		assert.Equal(t, "08c3d6e8388415d8f6190a40c0acb9328b41a89a5854468e62c2bbd1dc740460", value["utxo:3"])
		assert.Equal(t, "72629cff00e9f33dc7a96976717b7c86d4d168252c3550d3f24ae9f7bbe5cc68", value["utxo:4"])

		_, err = store.Create(ctx, tx, 0)
		require.Error(t, err)
		assert.True(t, errors.Is(err, errors.ErrTxExists))

		err = store.SetMined(ctx, txHash, blockID)
		require.NoError(t, err)

		value, err = redis.HGetAll(ctx, txHash.String()).Result()
		require.NoError(t, err)

		assert.Equal(t, "123", value["blockIDs"])

		err = store.SetMined(ctx, txHash, blockID2)
		require.NoError(t, err)

		value, err = redis.HGetAll(ctx, txHash.String()).Result()
		require.NoError(t, err)

		assert.Equal(t, "123,124", value["blockIDs"])

		meta, err := store.Get(ctx, txHash)
		require.NoError(t, err)
		assert.Equal(t, uint32(1), meta.Tx.Version)
		assert.Equal(t, 1, len(meta.Tx.Inputs))
		assert.Equal(t, txHash, meta.Tx.TxIDChainHash())
	})

	t.Run("redis store coinbase", func(t *testing.T) {
		cleanDB(t, redis, txHash)

		_, err = store.Create(ctx, coinbaseTx, 0)
		require.NoError(t, err)

		// raw redis get
		res, err := redis.HGetAll(ctx, coinbaseKey.String()).Result()
		require.NoError(t, err)
		assert.Equal(t, "1", res["isCoinbase"])

		txMeta, err := store.Get(ctx, coinbaseTx.TxIDChainHash())
		require.NoError(t, err)
		assert.True(t, txMeta.IsCoinbase)
		assert.Equal(t, txMeta.Tx.ExtendedBytes(), coinbaseTx.ExtendedBytes())
	})

	t.Run("redis get", func(t *testing.T) {
		cleanDB(t, redis, txHash, tx)

		_, err = store.Create(ctx, tx, 0)
		require.NoError(t, err)

		value, err := store.Get(ctx, txHash)
		require.NoError(t, err)
		assert.Equal(t, uint64(215), value.Fee)
		assert.Equal(t, uint64(328), value.SizeInBytes)
		assert.Len(t, value.ParentTxHashes, 1)
		assert.Equal(t, []chainhash.Hash{*parentTxHash}, value.ParentTxHashes)
		assert.Len(t, value.BlockIDs, 0)

		err = store.SetMined(ctx, txHash, blockID2)
		require.NoError(t, err)

		value, err = store.Get(ctx, txHash)
		require.NoError(t, err)
		assert.Equal(t, uint64(215), value.Fee)
		assert.Len(t, value.BlockIDs, 1)
		assert.Equal(t, []uint32{blockID2}, value.BlockIDs)
	})

	t.Run("redis get spend", func(t *testing.T) {
		cleanDB(t, redis, txHash, tx)

		txMeta, err := store.Create(ctx, tx, 0)
		assert.NotNil(t, txMeta)
		require.NoError(t, err)

		resp, err := store.GetSpend(ctx, spend)
		require.NoError(t, err)
		assert.Equal(t, int(utxo.Status_OK), resp.Status)
		assert.Nil(t, resp.SpendingTxID)

		err = store.Spend(ctx, spends, 0)
		require.NoError(t, err)

		resp, err = store.GetSpend(ctx, spend)
		require.NoError(t, err)
		assert.Equal(t, int(utxo.Status_SPENT), resp.Status)
		assert.Equal(t, spendingTxID1, resp.SpendingTxID)
	})

	t.Run("redis store", func(t *testing.T) {
		cleanDB(t, redis, txHash, tx)

		txMeta, err := store.Create(ctx, tx, 0)
		assert.NotNil(t, txMeta)
		require.NoError(t, err)

		data, err := store.Get(ctx, txHash)
		require.NoError(t, err)

		assert.Len(t, data.Tx.Inputs, 1)
		assert.Equal(t, tx.Inputs[0], data.Tx.Inputs[0])
		assert.Len(t, data.Tx.Outputs, 5)
		assert.Equal(t, uint64(215), data.Fee)
		assert.Equal(t, uint64(328), data.SizeInBytes)
		assert.Equal(t, tx.Version, data.Tx.Version)

		// UTXOs are held in Redis, but not returned in the Get() response
		// They are accessed via the Spend(), UnGetSpend().
		// To check they are correctly stored, we can check the raw Redis data
		value, err := redis.HGetAll(ctx, txHash.String()).Result()
		require.NoError(t, err)

		for i := 0; i < len(data.Tx.Outputs); i++ {
			utxo, ok := value[fmt.Sprintf("utxo:%d", i)]
			require.True(t, ok)
			assert.Len(t, utxo, 64)
		}

		assert.Len(t, data.ParentTxHashes, 1)
		assert.Equal(t, tx.Inputs[0].PreviousTxIDChainHash(), &data.ParentTxHashes[0])

		assert.Len(t, data.BlockIDs, 0)

		txMeta, err = store.Create(ctx, tx, 0)
		assert.Nil(t, txMeta)
		require.True(t, errors.Is(err, errors.ErrTxExists))

		err = store.Spend(ctx, spends, 0)
		require.NoError(t, err)

		txMeta, err = store.Create(ctx, tx, 0)
		assert.Nil(t, txMeta)
		require.True(t, errors.Is(err, errors.ErrTxExists))
	})

	t.Run("redis spend", func(t *testing.T) {
		cleanDB(t, redis, txHash, tx)

		txMeta, err := store.Create(ctx, tx, 0)
		assert.NotNil(t, txMeta)
		require.NoError(t, err)

		err = store.Spend(ctx, spends, 0)
		require.NoError(t, err)

		data, err := redis.HGetAll(ctx, txHash.String()).Result()
		require.NoError(t, err)

		utxo, ok := data["utxo:0"]
		require.True(t, ok)
		assert.Len(t, utxo, 128)

		assert.Equal(t, spendingTxID1.String(), utxo[64:])

		// try to spend with different txid
		err = store.Spend(ctx, spends2, 0)
		assert.Error(t, err)
		assert.True(t, errors.Is(err, errors.ErrSpent))
	})

	t.Run("redis 1 record spend 1 and not expire no blockIDs", func(t *testing.T) {
		cleanDB(t, redis, txHash, tx)

		txMeta, err := store.Create(ctx, tx, 0)
		assert.NotNil(t, txMeta)
		require.NoError(t, err)

		ttl, err := redis.TTL(ctx, txHash.String()).Result()
		require.NoError(t, err)
		require.Equal(t, time.Duration(-1), ttl)

		err = store.Spend(ctx, spends, 0)
		require.NoError(t, err)

		ttl, err = redis.TTL(ctx, txHash.String()).Result()
		require.NoError(t, err)
		require.Equal(t, time.Duration(-1), ttl)

		// Now spend all the remaining utxos
		spendsRemaining := []*utxo.Spend{{
			TxID:         txHash,
			Vout:         1,
			UTXOHash:     utxoHash1,
			SpendingTxID: spendingTxID2,
		}, {
			TxID:         txHash,
			Vout:         2,
			UTXOHash:     utxoHash2,
			SpendingTxID: spendingTxID2,
		}, {
			TxID:         txHash,
			Vout:         3,
			UTXOHash:     utxoHash3,
			SpendingTxID: spendingTxID2,
		}, {
			TxID:         txHash,
			Vout:         4,
			UTXOHash:     utxoHash4,
			SpendingTxID: spendingTxID2,
		}}

		err = store.Spend(ctx, spendsRemaining, 0)
		require.NoError(t, err)

		ttl, err = redis.TTL(ctx, txHash.String()).Result()
		require.NoError(t, err)
		require.Equal(t, time.Duration(-1), ttl) // Expiration is -1 because the tx has not been in a block yet

		err = store.SetMined(ctx, txHash, blockID)
		require.NoError(t, err)

		ttl, err = redis.TTL(ctx, txHash.String()).Result()
		require.NoError(t, err)
		require.Greater(t, ttl, time.Duration(0)) // Now TTL should be set as all UTXOs are spent
	})

	t.Run("redis 1 record spend 1 and not expire", func(t *testing.T) {
		cleanDB(t, redis, txHash, tx)

		txMeta, err := store.Create(ctx, tx, 0, utxo.WithBlockIDs(1, 2, 3)) // Important that blockIDs are set
		assert.NotNil(t, txMeta)
		require.NoError(t, err)

		err = store.Spend(ctx, spends, 0)
		require.NoError(t, err)

		ttl, err := redis.TTL(ctx, txHash.String()).Result()
		require.NoError(t, err)
		require.Equal(t, time.Duration(-1), ttl) // Expiration is -1 because the tx still has UTXOs

		// Now spend all the remaining utxos
		spendsRemaining := []*utxo.Spend{{
			TxID:         txHash,
			Vout:         1,
			UTXOHash:     utxoHash1,
			SpendingTxID: spendingTxID2,
		}, {
			TxID:         txHash,
			Vout:         2,
			UTXOHash:     utxoHash2,
			SpendingTxID: spendingTxID2,
		}, {
			TxID:         txHash,
			Vout:         3,
			UTXOHash:     utxoHash3,
			SpendingTxID: spendingTxID2,
		}, {
			TxID:         txHash,
			Vout:         4,
			UTXOHash:     utxoHash4,
			SpendingTxID: spendingTxID2,
		}}

		err = store.Spend(ctx, spendsRemaining, 0)
		require.NoError(t, err)

		ttl, err = redis.TTL(ctx, txHash.String()).Result()
		require.NoError(t, err)
		assert.Greater(t, ttl, int64(0)) // Now TTL should be set as all UTXOs are spent
	})

	t.Run("redis spend all and expire", func(t *testing.T) {
		cleanDB(t, redis, txHash, tx)

		txMeta, err := store.Create(ctx, tx, 0)
		assert.NotNil(t, txMeta)
		require.NoError(t, err)

		err = store.Spend(ctx, spendsAll, 0)
		require.NoError(t, err)

		ttl, err := redis.TTL(ctx, txHash.String()).Result()
		require.NoError(t, err)
		require.Equal(t, time.Duration(-1), ttl) // Expiration is -1 because the tx has not yet been mined

		// Now call SetMinedMulti
		err = store.SetMinedMulti(ctx, []*chainhash.Hash{txHash}, 1)
		require.NoError(t, err)

		ttl, err = redis.TTL(ctx, txHash.String()).Result()
		require.NoError(t, err)
		require.Greater(t, ttl, int64(0)) // Now TTL should be set as all UTXOs are spent

		// try to spend with different txid
		err = store.Spend(ctx, spends3, 0)
		require.Error(t, err)
		// require.ErrorIs(t, err, utxo.ErrTypeSpent)

		// try to spend with different txid
		err = store.Spend(ctx, spends3, 0)
		require.Error(t, err)
		require.True(t, errors.Is(err, errors.ErrSpent))
	})

	t.Run("redis reset", func(t *testing.T) {
		tx2 := tx.Clone()
		cleanDB(t, redis, txHash, tx2)

		txMeta, err := store.Create(ctx, tx2, 0)
		assert.NotNil(t, txMeta)
		require.NoError(t, err)

		_ = store.SetBlockHeight(101)

		utxoHash0, _ := util.UTXOHashFromOutput(tx2.TxIDChainHash(), tx2.Outputs[0], 0)
		spends := []*utxo.Spend{{
			TxID:         tx2.TxIDChainHash(),
			Vout:         0,
			UTXOHash:     utxoHash0,
			SpendingTxID: spendingTxID1,
		}}

		err = store.Spend(ctx, spends, 0)
		require.NoError(t, err)

		value, err := redis.HGetAll(ctx, tx2.TxIDChainHash().String()).Result()
		require.NoError(t, err)

		utxo, ok := value["utxo:0"]
		require.True(t, ok)
		require.Len(t, utxo, 128)
		require.Equal(t, spendingTxID1.String(), utxo[64:])

		// try to reset the utxo
		err = store.UnSpend(ctx, spends)
		require.NoError(t, err)

		value, err = redis.HGetAll(ctx, tx2.TxIDChainHash().String()).Result()
		require.NoError(t, err)

		utxo, ok = value["utxo:0"]
		require.True(t, ok)
		require.Len(t, utxo, 64)
	})

	t.Run("CreateWithBlockIDs", func(t *testing.T) {
		cleanDB(t, redis, txHash, tx)

		var blockHeight uint32

		txMeta, err := store.Create(ctx, tx, blockHeight, utxo.WithBlockIDs(1, 2, 3))
		require.NoError(t, err)
		assert.NotNil(t, txMeta)

		data, err := store.Get(ctx, txHash)
		require.NoError(t, err)
		require.Len(t, data.BlockIDs, 3)

		assert.Equal(t, uint32(1), data.BlockIDs[0])
		assert.Equal(t, uint32(2), data.BlockIDs[1])
		assert.Equal(t, uint32(3), data.BlockIDs[2])
	})

	t.Run("TestStoreOPReturn", func(t *testing.T) {
		cleanDB(t, redis, txHash, tx)

		txMeta, err := store.Create(ctx, txWithOPReturn, 0)
		require.NoError(t, err)
		assert.NotNil(t, txMeta)

		nrUTXOs, err := redis.HGet(ctx, txWithOPReturn.TxIDChainHash().String(), "nrUtxos").Result()
		require.NoError(t, err)
		assert.Equal(t, "1", nrUTXOs)
	})

	t.Run("FrozenTX", func(t *testing.T) {
		cleanDB(t, redis, txHash, tx)

		txMeta, err := store.Create(ctx, tx, 0)
		require.NoError(t, err)
		assert.NotNil(t, txMeta)

		res := redis.HSet(ctx, txHash.String(), "frozen", 1)
		require.NoError(t, res.Err())

		err = store.Spend(ctx, spends, 0)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "FROZEN:TX is frozen")
	})

	t.Run("FrozenUTXO", func(t *testing.T) {
		cleanDB(t, redis, txHash, tx)

		txMeta, err := store.Create(ctx, tx, 0)
		require.NoError(t, err)
		assert.NotNil(t, txMeta)

		frozenMarker, err := chainhash.NewHashFromStr("FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF")
		require.NoError(t, err)

		err = store.Spend(ctx, []*utxo.Spend{
			{
				TxID:         txHash,
				Vout:         0,
				UTXOHash:     utxoHash0,
				SpendingTxID: frozenMarker,
			},
		}, 0)
		require.NoError(t, err)

		err = store.Spend(ctx, spends, 0)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "FROZEN:UTXO is frozen")
	})
}

func TestCoinbase(t *testing.T) {
	ctx := context.Background()

	store, _, deferFn := initRedis(t)
	defer deferFn()
	redis := store.GetClient()

	coinbaseTxHash := coinbaseTx.TxIDChainHash()

	res := redis.Del(ctx, coinbaseTxHash.String())
	require.NoError(t, res.Err())

	txMeta, err := store.Create(ctx, coinbaseTx, 0)
	require.NoError(t, err)
	assert.NotNil(t, txMeta)
	assert.True(t, txMeta.IsCoinbase)

	utxoHash, err := util.UTXOHashFromOutput(coinbaseTxHash, coinbaseTx.Outputs[0], 0)
	require.NoError(t, err)

	spend := &utxo.Spend{
		TxID:         coinbaseTxHash,
		Vout:         0,
		UTXOHash:     utxoHash,
		SpendingTxID: spendingTxID1,
	}

	err = store.Spend(ctx, []*utxo.Spend{spend}, 0)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "Coinbase UTXO can only be spent after 100 blocks")

	err = store.SetBlockHeight(5000)
	require.NoError(t, err)

	err = store.Spend(ctx, []*utxo.Spend{spend}, 0)
	require.NoError(t, err)
}

func TestStoreDecorate(t *testing.T) {
	ctx := context.Background()

	store, _, deferFn := initRedis(t)
	defer deferFn()
	client := store.GetClient()

	t.Run("redis BatchDecorate", func(t *testing.T) {
		cleanDB(t, client, spendingTxID1, tx)
		txMeta, err := store.Create(ctx, tx, 0)

		txID := txHash.String()
		_ = txID

		assert.NotNil(t, txMeta)
		require.NoError(t, err)

		items := []*utxo.UnresolvedMetaData{
			{
				Hash: *txHash,
				Idx:  0,
			},
			{
				Hash: *txHash,
				Idx:  1,
			},
			{
				Hash: *txHash,
				Idx:  2,
			},
			{
				Hash: *txHash,
				Idx:  3,
			},
			{
				Hash: *txHash,
				Idx:  4,
			},
		}
		err = store.BatchDecorate(ctx, items)
		require.NoError(t, err)

		// check field values
		for _, item := range items {
			assert.Equal(t, uint64(215), item.Data.Fee)
			assert.Equal(t, uint64(328), item.Data.SizeInBytes)
		}
	})

	t.Run("redis PreviousOutputsDecorate", func(t *testing.T) {
		cleanDB(t, client, spendingTxID1, tx)
		txMeta, err := store.Create(ctx, tx, 0)

		txID := txHash.String()
		_ = txID

		assert.NotNil(t, txMeta)
		require.NoError(t, err)

		items := []*meta.PreviousOutput{
			{
				PreviousTxID: *txHash,
				Vout:         0,
				Idx:          0,
			},
			{
				PreviousTxID: *txHash,
				Vout:         4,
				Idx:          1,
			},
			{
				PreviousTxID: *txHash,
				Vout:         3,
				Idx:          2,
			},
			{
				PreviousTxID: *txHash,
				Vout:         2,
				Idx:          3,
			},
			{
				PreviousTxID: *txHash,
				Vout:         1,
				Idx:          4,
			},
		}
		err = store.PreviousOutputsDecorate(ctx, items)
		require.NoError(t, err)

		// check field values for vout 0 - item 0
		assert.Len(t, items[0].LockingScript, 25)
		assert.Equal(t, uint64(5_000_000), items[0].Satoshis)

		// check field values for vout 4 - item 1
		assert.Len(t, items[1].LockingScript, 25)
		assert.Equal(t, uint64(2_817_689), items[1].Satoshis)

		// check field values for vout 3 - item 2
		assert.Len(t, items[2].LockingScript, 25)
		assert.Equal(t, uint64(20_000), items[2].Satoshis)

		// check field values for vout 2 - item 3
		assert.Len(t, items[3].LockingScript, 25)
		assert.Equal(t, uint64(20_000), items[3].Satoshis)

		// check field values for vout 1 - item 4
		assert.Len(t, items[4].LockingScript, 25)
		assert.Equal(t, uint64(2_000_000), items[4].Satoshis)
	})
}

// func TestLargeUTXO(t *testing.T) {
//	// For this test, we will assume that aerospike can never store more than 2 utxos in a single record
//	client, aeroErr := aero.NewClient(aerospikeHost, aerospikePort)
//	require.NoError(t, aeroErr)
//
//	aeroURL, err := url.Parse(fmt.Sprintf(aerospikeURLFormat, aerospikeHost, aerospikePort, aerospikeNamespace, aerospikeSet, aerospikeExpiration))
//	require.NoError(t, err)
//
//	// teranode db client
//	var db *storeRedis.Store
//	db, err = New(ulogger.TestLogger{}, aeroURL)
//	require.NoError(t, err)
//
//	fParent, err := os.Open("testdata/ac4849b3b03e44d5fcba8becfc642a8670049b59436d6c7ab89a4d3873d9a3ef.bin")
//	require.NoError(t, err)
//	defer fParent.Close()
//
//	parentTx := new(bt.Tx)
//	_, err = parentTx.ReadFrom(fParent)
//	require.NoError(t, err)
//	require.Equal(t, "ac4849b3b03e44d5fcba8becfc642a8670049b59436d6c7ab89a4d3873d9a3ef", parenttxHash.String())
//
//	fChild, err := os.Open("testdata/1bd4f08ffbeefbb67d82a340dd35259a97c5626368f8a6efa056571b293fae52.bin")
//	require.NoError(t, err)
//	defer fChild.Close()
//
//	childTx := new(bt.Tx)
//	_, err = childTx.ReadFrom(fChild)
//	require.NoError(t, err)
//	require.Equal(t, "1bd4f08ffbeefbb67d82a340dd35259a97c5626368f8a6efa056571b293fae52", childtxHash.String())
//
//	keyParent, err := aero.NewKey(db.namespace, db.setName, parenttxHash.CloneBytes())
//	require.NoError(t, err)
//	assert.NotNil(t, keyParent)
//
//	_, err = client.Delete(nil, keyParent)
//	require.NoError(t, err)
//
//	parentMeta, err := db.Create(context.Background(), parentTx, 0)
//	require.NoError(t, err)
//	assert.NotNil(t, parentMeta)
//
//	keyChild, err := aero.NewKey(db.namespace, db.setName, childtxHash.CloneBytes())
//	require.NoError(t, err)
//	assert.NotNil(t, keyChild)
//
//	_, err = client.Delete(nil, keyChild)
//	require.NoError(t, err)
//
//	childMeta, err := db.Create(context.Background(), childTx, 0)
//	require.NoError(t, err)
//	assert.NotNil(t, childMeta)
//
//	previousTxID, err := chainhash.NewHashFromStr("ac4849b3b03e44d5fcba8becfc642a8670049b59436d6c7ab89a4d3873d9a3ef")
//	require.NoError(t, err)
//
//	previousOutput := &meta.PreviousOutput{
//		PreviousTxID: *previousTxID,
//		Vout:         31243,
//	}
//
//	assert.Nil(t, previousOutput.LockingScript)
//
//	err = db.PreviousOutputsDecorate(context.Background(), []*meta.PreviousOutput{previousOutput})
//	require.NoError(t, err)
//	assert.NotNil(t, previousOutput.LockingScript)
//	// t.Log(previousOutput)
//}

func TestSmokeTests(t *testing.T) {
	store, ctx, deferFn := initRedis(t)
	defer deferFn()

	t.Run("redis store", func(t *testing.T) {
		err := store.Delete(ctx, tests.TXHash)
		require.NoError(t, err)

		tests.Store(t, store)
	})

	t.Run("redis spend", func(t *testing.T) {
		err := store.Delete(ctx, tests.TXHash)
		require.NoError(t, err)

		tests.Spend(t, store)
	})

	t.Run("redis reset", func(t *testing.T) {
		err := store.Delete(ctx, tests.TXHash)
		require.NoError(t, err)

		tests.Restore(t, store)
	})

	t.Run("redis freeze", func(t *testing.T) {
		err := store.Delete(ctx, tests.TXHash)
		require.NoError(t, err)

		tests.Freeze(t, store)
	})

	t.Run("redis reassign", func(t *testing.T) {
		err := store.Delete(ctx, tests.TXHash)
		require.NoError(t, err)

		tests.ReAssign(t, store)
	})
}

func initRedis(t *testing.T) (*storeRedis.Store, context.Context, func()) {
	ctx := context.Background()

	container, err := redisTest.RunContainer(ctx)
	require.NoError(t, err)

	t.Cleanup(func() {
		err = container.Terminate(ctx)
		require.NoError(t, err)
	})

	host, err := container.Host(ctx)
	require.NoError(t, err)

	port, err := container.ServicePort(ctx)
	require.NoError(t, err)

	redisContainerURL := fmt.Sprintf("redis2://%s:%d?expiration=10s&transactionStore=file://./data/transaction_store?hashPrefix=2", host, port)
	redisURL, err := url.Parse(redisContainerURL)
	require.NoError(t, err)

	// teranode redisStore client
	var redisStore *storeRedis.Store
	redisStore, err = storeRedis.New(ctx, ulogger.TestLogger{}, redisURL)
	require.NoError(t, err)

	return redisStore, ctx, func() {
		redisStore.GetClient().Close()
	}
}

func cleanDB(t *testing.T, client *redis_db.Client, key *chainhash.Hash, txs ...*bt.Tx) {
	ctx := context.Background()

	res := client.Del(ctx, key.String())
	require.NoError(t, res.Err())

	if coinbaseKey != nil {
		res = client.Del(ctx, coinbaseKey.String())
		require.NoError(t, res.Err())
	}

	if len(txs) > 0 {
		for _, tx := range txs {
			res = client.Del(ctx, tx.TxIDChainHash().String())
			require.NoError(t, res.Err())
		}
	}
}
