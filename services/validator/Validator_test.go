/*
Package validator implements Bitcoin SV transaction validation functionality.

This package provides comprehensive transaction validation for Bitcoin SV nodes,
including script verification, UTXO management, and policy enforcement. It supports
multiple script interpreters (GoBT, GoSDK, GoBDK) and implements the full Bitcoin
transaction validation ruleset.

Key features:
  - Transaction validation against Bitcoin consensus rules
  - UTXO spending and creation
  - Script verification using multiple interpreters
  - Policy enforcement
  - Block assembly integration
  - Kafka integration for transaction metadata

Usage:

	validator := NewTxValidator(logger, policy, params)
	err := validator.ValidateTransaction(tx, blockHeight, nil)
*/
package validator

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"log"
	"net/url"
	"os"
	"reflect"
	"testing"
	"time"

	bt "github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockassembly"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/blob/memory"
	utxostore "github.com/bsv-blockchain/teranode/stores/utxo"
	teranode_aerospike "github.com/bsv-blockchain/teranode/stores/utxo/aerospike"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/stores/utxo/nullstore"
	"github.com/bsv-blockchain/teranode/stores/utxo/sql"
	"github.com/bsv-blockchain/teranode/stores/utxo/tests"
	"github.com/bsv-blockchain/teranode/test/utils/transactions"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/kafka"
	kafkamessage "github.com/bsv-blockchain/teranode/util/kafka/kafka_message"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/bsv-blockchain/teranode/util/tracing"
	aeroTest "github.com/bsv-blockchain/testcontainers-aerospike-go"
	"github.com/ordishs/gocore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func BenchmarkValidator(b *testing.B) {
	tx, err := bt.NewTxFromString("010000000000000000ef01f3f0d33a5c5afd524043762f8b812999caa5a225e6e20ecdb71a7e0e1c207b43530000006a473044022049e20908f21bdcb901b5c5a9a93b238446606267e19db4e662df1a7c4a5bae08022036960a340515e2cfee79b9c194093f24f253d4243bf9d0baa97352983e2263fa412102a98c1a3be041da2591761fbef4b2ab0f147aef36c308aee66df0b9825218de23ffffffff10000000000000001976a914a8d6bd6648139d95dac35d411c592b05bc0973aa88ac01000000000000000070006a0963657274696861736822314c6d763150594d70387339594a556e374d3948565473446b64626155386b514e4a403263333934306361313334353331373035326334346630613861636362323162323165633131386465646330396538643764393064323166333935663063613000000000")
	if err != nil {
		panic(err)
	}

	tSettings := test.CreateBaseTestSettings(b)

	blockAssemblyClient, err := blockassembly.NewClient(context.Background(), ulogger.TestLogger{}, tSettings)
	require.NoError(b, err)

	ctx := context.Background()
	logger := ulogger.TestLogger{}

	utxoStoreURL, err := url.Parse("sqlitememory:///test")
	require.NoError(b, err)

	utxoStore, err := sql.New(ctx, logger, tSettings, utxoStoreURL)
	require.NoError(b, err)

	v, err := New(ctx, logger, tSettings, utxoStore, nil, nil, blockAssemblyClient, nil)
	if err != nil {
		panic(err)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if _, err = v.Validate(context.Background(), tx, chaincfg.GenesisActivationHeight, WithSkipPolicyChecks(true)); err != nil {
			log.Printf("ERROR: %v\n", err)
		} else {
			fmt.Println("asd")
		}
	}
}

func TestValidate_CoinbaseTransaction(t *testing.T) {
	tracing.SetupMockTracer()

	coinbase, err := bt.NewTxFromString(model.CoinbaseHex)
	require.NoError(t, err)

	// need to add spendable utxo to utxo store

	// delete spends set to false
	ctx := context.Background()
	logger := ulogger.NewErrorTestLogger(t)

	tSettings := test.CreateBaseTestSettings(t)

	utxoStoreURL, err := url.Parse("sqlitememory:///test")
	require.NoError(t, err)

	utxoStore, err := sql.New(ctx, logger, tSettings, utxoStoreURL)
	require.NoError(t, err)

	blockAssemblyClient, err := blockassembly.NewClient(context.Background(), ulogger.TestLogger{}, tSettings)
	require.NoError(t, err)

	v, err := New(context.Background(), ulogger.TestLogger{}, tSettings, utxoStore, nil, nil, blockAssemblyClient, nil)
	if err != nil {
		panic(err)
	}

	height, err := util.ExtractCoinbaseHeight(coinbase)
	require.NoError(t, err)

	_, err = v.Validate(context.Background(), coinbase, height, WithSkipPolicyChecks(true))
	require.Error(t, err)
}

func TestValidate_ValidTransaction(t *testing.T) {
	t.Run("Mined transactions return blockIDs", func(t *testing.T) {
		tracing.SetupMockTracer()

		// delete spends set to false
		ctx := context.Background()
		logger := ulogger.NewErrorTestLogger(t)

		tSettings := test.CreateBaseTestSettings(t)

		utxoStoreURL, err := url.Parse("sqlitememory:///test")
		require.NoError(t, err)

		utxoStore, err := sql.New(ctx, logger, tSettings, utxoStoreURL)
		require.NoError(t, err)

		txMeta, err := utxoStore.Create(ctx, tests.ParentTx, 122)
		require.NoError(t, err)

		assert.Len(t, txMeta.BlockIDs, 0)

		txMeta, err = utxoStore.Create(ctx, tests.Tx, 123)
		require.NoError(t, err)

		assert.Len(t, txMeta.BlockIDs, 0)

		blockAssemblyClient, err := blockassembly.NewClient(context.Background(), ulogger.TestLogger{}, tSettings)
		require.NoError(t, err)

		v, err := New(ctx, logger, tSettings, utxoStore, nil, nil, blockAssemblyClient, nil)
		require.NoError(t, err)

		// validate the transaction and make sure we are not getting blockIDs
		txMeta, err = v.Validate(ctx, tests.Tx, 123)
		require.NoError(t, err)

		assert.Len(t, txMeta.BlockIDs, 0)

		// set the transaction as mined
		blockIDsMap, err := utxoStore.SetMinedMulti(t.Context(), []*chainhash.Hash{tests.Tx.TxIDChainHash()}, utxostore.MinedBlockInfo{
			BlockID:     125,
			BlockHeight: 123,
			SubtreeIdx:  0,
		})
		require.NoError(t, err)
		require.Len(t, blockIDsMap, 1)
		require.Equal(t, []uint32{125}, blockIDsMap[*tests.Tx.TxIDChainHash()])

		// validate the transaction and make sure we are getting blockIDs
		txMeta, err = v.Validate(ctx, tests.Tx, 123)
		require.NoError(t, err)

		assert.Len(t, txMeta.BlockIDs, 1)
		assert.Equal(t, uint32(125), txMeta.BlockIDs[0])
	})
}

func TestValidate_BlockAssemblyAndTxMetaChannels(t *testing.T) {
	tracing.SetupMockTracer()

	txHex := "010000000000000000ef01febe0cbd7d87d44cbd4b5adac0a5bfcdbd2b672c9113f5d74a6459a2b85569db010000008b48304502207ec38d0a4ef79c3a4286ba3e5a5b6ede1fa678af9242465140d78a901af9e4e0022100c26c377d44b761469cf0bdcdbf4931418f2c5a02ce6b72bbb7af52facd7228c1014104bc9eb4fe4cb53e35df7e7734c4c3cd91c6af7840be80f4a1fff283e2cd6ae8f7713cb263a4590263240e3c01ec36bc603c32281ac08773484dc69b8152e48cecffffffff60b74700000000001976a9148ac9bdc626352d16e18c26f431e834f9aae30e2888ac0230424700000000001976a9148ac9bdc626352d16e18c26f431e834f9aae30e2888ac1027000000000000166a148ac9bdc626352d16e18c26f431e834f9aae30e2800000000"
	tx, err := bt.NewTxFromString(txHex)
	require.NoError(t, err)

	utxoStore, _ := nullstore.NewNullStore()
	_ = utxoStore.SetBlockHeight(257727)
	//nolint:gosec
	_ = utxoStore.SetMedianBlockTime(uint32(time.Now().Unix()))

	initPrometheusMetrics()

	tSettings := settings.NewSettings()
	tSettings.ChainCfgParams = &chaincfg.MainNetParams

	txmetaKafkaProducerClient := kafka.NewKafkaAsyncProducerMock()
	rejectedTxKafkaProducerClient := kafka.NewKafkaAsyncProducerMock()

	blockAssembler := &MockBlockAssemblyStore{}

	v := &Validator{
		logger:                        ulogger.TestLogger{},
		settings:                      tSettings,
		txValidator:                   NewTxValidator(ulogger.TestLogger{}, tSettings),
		utxoStore:                     utxoStore,
		blockAssembler:                blockAssembler,
		saveInParallel:                true,
		stats:                         gocore.NewStat("validator"),
		txmetaKafkaProducerClient:     txmetaKafkaProducerClient,
		rejectedTxKafkaProducerClient: rejectedTxKafkaProducerClient,
	}

	txMeta, err := v.Validate(t.Context(), tx, 257727, WithSkipPolicyChecks(true))
	require.NoError(t, err)

	// check the kafka channels
	require.Equal(t, 1, len(txmetaKafkaProducerClient.PublishChannel()), "txMetaKafkaChan should have 1 message")
	require.Equal(t, 0, len(rejectedTxKafkaProducerClient.PublishChannel()), "rejectedTxKafkaChan should be empty")

	msg := <-txmetaKafkaProducerClient.PublishChannel()
	assert.NotNil(t, msg)

	// unmarshal the kafka message
	var kafkaMsg kafkamessage.KafkaTxMetaTopicMessage
	err = proto.Unmarshal(msg.Value, &kafkaMsg)
	require.NoError(t, err)

	assert.Equal(t, tx.TxID(), kafkaMsg.TxHash)

	// check the block assembly store
	assert.Len(t, blockAssembler.storedTxs, 1)
	assert.Len(t, blockAssembler.removedTxs, 0)

	assert.Equal(t, tx.TxID(), blockAssembler.storedTxs[0].txHash.String())
	assert.Equal(t, txMeta.Fee, blockAssembler.storedTxs[0].fee)
	assert.Equal(t, txMeta.SizeInBytes, blockAssembler.storedTxs[0].size)
}

func TestValidate_RejectedTransactionChannel(t *testing.T) {
	tracing.SetupMockTracer()

	txHex := "010000000000000000ef01febe0cbd7d87d44cbd4b5adac0a5bfcdbd2b672c9113f5d74a6459a2b85569db010000008b48304502207ec38d0a4ef79c3a4286ba3e5a5b6ede1fa678af9242465140d78a901af9e4e0022100c26c377d44b761469cf0bdcdbf4931418f2c5a02ce6b72bbb7af52facd7228c1014104bc9eb4fe4cb53e35df7e7734c4c3cd91c6af7840be80f4a1fff283e2cd6ae8f7713cb263a4590263240e3c01ec36bc603c32281ac08773484dc69b8152e48cecffffffff60b74700000000001976a9148ac9bdc626352d16e18c26f431e834f9aae30e2888ac0230424700000000001976a9148ac9bdc626352d16e18c26f431e834f9aae30e2888ac1027000000000000166a148ac9bdc626352d16e18c26f431e834f9aae30e2800000000"
	tx, err := bt.NewTxFromString(txHex)
	require.NoError(t, err)

	// set previous sats to 0, which makes the tx invalid
	tx.Inputs[0].PreviousTxSatoshis = 0

	ctx := context.Background()
	logger := ulogger.NewErrorTestLogger(t)

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.ChainCfgParams, _ = chaincfg.GetChainParams("mainnet")

	utxoStoreURL, err := url.Parse("sqlitememory:///test")
	require.NoError(t, err)

	utxoStore, err := sql.New(ctx, logger, tSettings, utxoStoreURL)
	require.NoError(t, err)

	initPrometheusMetrics()

	txmetaKafkaProducerClient := kafka.NewKafkaAsyncProducerMock()
	rejectedTxKafkaProducerClient := kafka.NewKafkaAsyncProducerMock()

	v := &Validator{
		logger:                        logger,
		settings:                      tSettings,
		txValidator:                   NewTxValidator(logger, tSettings),
		utxoStore:                     utxoStore,
		blockAssembler:                nil,
		saveInParallel:                true,
		stats:                         gocore.NewStat("validator"),
		txmetaKafkaProducerClient:     txmetaKafkaProducerClient,
		rejectedTxKafkaProducerClient: rejectedTxKafkaProducerClient,
	}

	_, err = v.Validate(context.Background(), tx, 100, WithSkipPolicyChecks(false))
	require.Error(t, err)

	require.Equal(t, 0, len(txmetaKafkaProducerClient.PublishChannel()), "txMetaKafkaChan should be empty")
	require.Equal(t, 1, len(rejectedTxKafkaProducerClient.PublishChannel()), "rejectedTxKafkaChan should have 1 message")
}

func TestValidate_InValidDoubleSpendTx(t *testing.T) {
}

func TestValidate_TxMetaStoreError(t *testing.T) {
}

func TestValidate_BlockAssemblyError(t *testing.T) {
	tracing.SetupMockTracer()

	txHex := "010000000000000000ef01febe0cbd7d87d44cbd4b5adac0a5bfcdbd2b672c9113f5d74a6459a2b85569db010000008b48304502207ec38d0a4ef79c3a4286ba3e5a5b6ede1fa678af9242465140d78a901af9e4e0022100c26c377d44b761469cf0bdcdbf4931418f2c5a02ce6b72bbb7af52facd7228c1014104bc9eb4fe4cb53e35df7e7734c4c3cd91c6af7840be80f4a1fff283e2cd6ae8f7713cb263a4590263240e3c01ec36bc603c32281ac08773484dc69b8152e48cecffffffff60b74700000000001976a9148ac9bdc626352d16e18c26f431e834f9aae30e2888ac0230424700000000001976a9148ac9bdc626352d16e18c26f431e834f9aae30e2888ac1027000000000000166a148ac9bdc626352d16e18c26f431e834f9aae30e2800000000"
	tx, err := bt.NewTxFromString(txHex)
	require.NoError(t, err)

	parenTxHex := "010000000000000000ef01154d5d31268f7ea94c80a7bf6de54e47812712feec25c17b8feceb570dfd9daf000000008b4830450220612b3ec065ec2b2a1757d97b7f57fba3c363645355cf6e1a5a1834411e6ab425022100bd071b90d391eb75dc9e2eea8b6774f36bf9c55439a971f0d1f4470b6448aef601410426e4e0654f72721b97a03c8170417c9ddabadcef97fe8ea626176ea62665b55ca2ff485f84df12ddec171e01ee8f9c7472c6c8467b0cf74ae8b3b614ed16cbdbffffffff008a6600000000001976a91429be45311cc66a5a6cc4a42516dbb7c9b126a3c188ac0280841e00000000001976a914996ed5e55d68aef653c85339f83873fac1321f0788ac60b74700000000001976a9148ac9bdc626352d16e18c26f431e834f9aae30e2888ac00000000"

	parentTx, err := bt.NewTxFromString(parenTxHex)
	require.NoError(t, err)

	ctx := context.Background()
	logger := ulogger.NewErrorTestLogger(t)

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.ChainCfgParams = &chaincfg.MainNetParams

	utxoStoreURL, err := url.Parse("sqlitememory:///test")
	require.NoError(t, err)

	utxoStore, err := sql.New(ctx, logger, tSettings, utxoStoreURL)
	require.NoError(t, err)

	_ = utxoStore.SetBlockHeight(257727)
	//nolint:gosec
	_ = utxoStore.SetMedianBlockTime(uint32(time.Now().Unix()))

	_, err = utxoStore.Create(context.Background(), parentTx, 257726)
	require.NoError(t, err)

	initPrometheusMetrics()

	txmetaKafkaProducerClient := kafka.NewKafkaAsyncProducerMock()
	rejectedTxKafkaProducerClient := kafka.NewKafkaAsyncProducerMock()

	blockAssembler := &MockBlockAssemblyStore{}
	blockAssembler.returnError = errors.NewServiceError("block assembly error")

	v := &Validator{
		logger:                        logger,
		settings:                      tSettings,
		txValidator:                   NewTxValidator(logger, tSettings),
		utxoStore:                     utxoStore,
		blockAssembler:                blockAssembler,
		saveInParallel:                true,
		stats:                         gocore.NewStat("validator"),
		txmetaKafkaProducerClient:     txmetaKafkaProducerClient,
		rejectedTxKafkaProducerClient: rejectedTxKafkaProducerClient,
	}

	txMetaData, err := v.Validate(t.Context(), tx, 257727, WithSkipPolicyChecks(true))
	require.Error(t, err)
	require.Nil(t, txMetaData)

	// the tx should be stored in the utxo store as locked
	txMeta, err := utxoStore.Get(t.Context(), tx.TxIDChainHash())
	require.NoError(t, err)
	assert.Equal(t, true, txMeta.Locked)

	// check the kafka channels, nothing should have been sent, since the tx never was sent correctly to the block assembly
	require.Equal(t, 0, len(txmetaKafkaProducerClient.PublishChannel()), "txMetaKafkaChan should be empty")
	require.Equal(t, 0, len(rejectedTxKafkaProducerClient.PublishChannel()), "rejectedTxKafkaChan should have 1 message")
}

func TestValidateTx4da809a914526f0c4770ea19b5f25f89e9acf82a4184e86a0a3ae8ad250e3b80(t *testing.T) {
	initPrometheusMetrics()

	// [height 257727] tx 4da809a914526f0c4770ea19b5f25f89e9acf82a4184e86a0a3ae8ad250e3b80 failed validation: arc error 463: transaction output 1 has non 0 value op return
	txid := "4da809a914526f0c4770ea19b5f25f89e9acf82a4184e86a0a3ae8ad250e3b80"
	tx, err := bt.NewTxFromString("010000000000000000ef01febe0cbd7d87d44cbd4b5adac0a5bfcdbd2b672c9113f5d74a6459a2b85569db010000008b48304502207ec38d0a4ef79c3a4286ba3e5a5b6ede1fa678af9242465140d78a901af9e4e0022100c26c377d44b761469cf0bdcdbf4931418f2c5a02ce6b72bbb7af52facd7228c1014104bc9eb4fe4cb53e35df7e7734c4c3cd91c6af7840be80f4a1fff283e2cd6ae8f7713cb263a4590263240e3c01ec36bc603c32281ac08773484dc69b8152e48cecffffffff60b74700000000001976a9148ac9bdc626352d16e18c26f431e834f9aae30e2888ac0230424700000000001976a9148ac9bdc626352d16e18c26f431e834f9aae30e2888ac1027000000000000166a148ac9bdc626352d16e18c26f431e834f9aae30e2800000000")
	require.NoError(t, err)
	assert.Equal(t, txid, tx.TxID())

	height := uint32(257727)
	utxos := []uint32{253237}

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.ChainCfgParams, _ = chaincfg.GetChainParams("mainnet")

	v := &Validator{
		txValidator: NewTxValidator(
			ulogger.TestLogger{},
			tSettings,
		),
	}

	ctx := context.Background()

	ctx, _, endSpan := tracing.Tracer("validator").Start(ctx, "Test")
	defer endSpan()

	err = v.validateTransaction(ctx, tx, height, nil, &Options{})
	require.NoError(t, err)

	err = v.validateTransactionScripts(ctx, tx, height, utxos, &Options{SkipPolicyChecks: true})
	require.NoError(t, err)
}

func TestValidateTxda47bd83967d81f3cf6520f4ff81b3b6c4797bfe7ac2b5969aedbf01a840cda6(t *testing.T) {
	initPrometheusMetrics()

	// [height 249976] tx da47bd83967d81f3cf6520f4ff81b3b6c4797bfe7ac2b5969aedbf01a840cda6 failed validation: transaction input 2 unlocking script is not push only
	txid := "da47bd83967d81f3cf6520f4ff81b3b6c4797bfe7ac2b5969aedbf01a840cda6"
	tx, err := bt.NewTxFromString("010000000000000000ef03fe1a25c8774c1e827f9ebdae731fe609ff159d6f7c15094e1d467a99a01e03100000000002012affffffffa086010000000000018253a080075d834402e916390940782236b29d23db6f52dfc940a12b3eff99159c0000000000ffffffffa086010000000000100f5468616e6b7320456c69676975732161e4ed95239756bbb98d11dcf973146be0c17cc1cc94340deb8bc4d44cd88e92000000000a516352676a675168948cffffffff40548900000000000763516751676a680220aa4400000000001976a9149bc0bbdd3024da4d0c38ed1aecf5c68dd1d3fa1288ac20aa4400000000001976a914169ff4804fd6596deb974f360c21584aa1e19c9788ac00000000")
	require.NoError(t, err)
	assert.Equal(t, txid, tx.TxID())

	height := uint32(249976)
	utxos := []uint32{249957, 249957, 249957}

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.ChainCfgParams, _ = chaincfg.GetChainParams("mainnet")

	v := &Validator{
		txValidator: NewTxValidator(ulogger.TestLogger{}, tSettings),
	}

	ctx := context.Background()

	ctx, _, endSpan := tracing.Tracer("validator").Start(ctx, "Test")
	defer endSpan()

	err = v.validateTransaction(ctx, tx, height, nil, &Options{})
	require.NoError(t, err)

	err = v.validateTransactionScripts(ctx, tx, height, utxos, &Options{SkipPolicyChecks: true})
	require.NoError(t, err)
}

func TestValidateTx956685dffd466d3051c8372c4f3bdf0e061775ed054d7e8f0bc5695ca747d604(t *testing.T) {
	initPrometheusMetrics()

	// height 229369] tx 956685dffd466d3051c8372c4f3bdf0e061775ed054d7e8f0bc5695ca747d604 failed validation: arc error 462: transaction input total satoshis cannot be zero
	txid := "956685dffd466d3051c8372c4f3bdf0e061775ed054d7e8f0bc5695ca747d604"
	tx, err := bt.NewTxFromString("010000000000000000ef015400c3490d91f3f742e73e81bc37dfca4f24f9a73a17c90ccab3012ddbc795bb000000008a473044022006a960f73ea637af867f69ed69edd291bee1d6daec241649caf909fb864dcd3b022011c82189c4a3379aba85fdb907d341db8067e426d7660fbba05c12fa370fa8aa0141048e69627b4807fe4ab00002a01c4a26a50d558cce969708e75dc5bfb345bbe92f06082757c85cbcac4ff0bbb91e221c59d3f9e675125da07e8110fd7d9b0ab6eeffffffff00000000000000001976a9146f9e896bb7cd9d27ca5b18c3ec9587ff0be7895188ac0100000000000000001976a9144477154cba7f0474a578fe734e00bd60513fbab588ac00000000")
	require.NoError(t, err)
	assert.Equal(t, txid, tx.TxID())

	var height uint32 = 229369

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.ChainCfgParams, _ = chaincfg.GetChainParams("mainnet")
	tSettings.Policy.MinMiningTxFee = 0

	v := &Validator{
		txValidator: NewTxValidator(ulogger.TestLogger{}, tSettings),
	}

	ctx := context.Background()

	ctx, _, endSpan := tracing.Tracer("validator").Start(ctx, "Test")
	defer endSpan()

	err = v.validateTransaction(ctx, tx, height, nil, &Options{})
	require.NoError(t, err)

	err = v.validateTransactionScripts(ctx, tx, height, []uint32{height}, &Options{SkipPolicyChecks: true})
	require.NoError(t, err)
}

func TestValidateTx7f4244335dec8d941e3fc1847ac3d020fac9347a0c0335294bf56ede8aa5840f(t *testing.T) {
	initPrometheusMetrics()

	// Testnet [height 1553037] tx 7f4244335dec8d941e3fc1847ac3d020fac9347a0c0335294bf56ede8aa5840f
	txid := "7f4244335dec8d941e3fc1847ac3d020fac9347a0c0335294bf56ede8aa5840f"
	tx, err := bt.NewTxFromString("010000000000000000ef02ace09f3a7c70c37abc89fcbfc8bcbc74524b210ad75868341c8339f4f241eb06000000000160ffffffff0100000000000000fd860b264b5341412d314d2076302e3161736d0a7777772e746f6b656e6f766174652e636f6d0a0a0a0a757661608763616a6853795379517f517f517f517f517f517f517f517f517f517f517f517f517f517f517f517f517f517f517f517f517f517f517f517f517f517f517f517f517f517f517f517f517f517f517f517f75816b816b816b816b816b816b816b816b816b816b816b816b816b816b816b816b816b816b816b816b816b816b816b816b816b816b816b816b816b816b816b816b816b816b816b817f6c7f6c7f6c7f6c7f6c7f6c7f6c7f6c7f6c7f6c7f6c7f6c7f6c7f6c7f6c7f6c7f6c7f6c7f6c7f6c7f6c7f6c7f6c7f6c7f6c7f6c7f6c7f6c7f6c7f6c7f6c7f6c7f6c7f6c7f6c7f7c6b7e7c6b7e7c6b7e7c6b7e7c6b7e7c6b7e7c6b7e7c6b7e7c6b7e7c6b7e7c6b7e7c6b7e7c6b7e7c6b7e7c6b7e7c6b7e7c6b7e7c6b7ea87801697f77527f7c7f587f7c815188547f7c83697c517f7c76014c9f6476014c8763755167014d8852687f7c687f7c76264b5341412d314d2076302e3161736d0a7777772e746f6b656e6f766174652e636f6d0a0a0a0a8763757b207db001ff1daaf52a399624a358e30a645d4c9eb9a1fbd77d019907618fb4610588756c756c6c6c6c6c6c76074b5341412d314d886c6c6c6c7604474d4558886c756c76055452414445886c756c756c751f546f6b656e6f766174652068617665206372656174656420612074657374207c7e42206576656e7420666f7220474d45582e2054686973207472616465206973206265696e67206d616e616765642062793a0a427579657220637573746f6469616e3a207e5a7a7a607c827b7d9f637c0820202020202020207e7c7f677f68757e0c0a7573696e67206b65793a207e59797e423032303130323033303430353036303730383039313031313132313331343135313631373138313932303231323232333234323532363237323832393330333133327e587a607c827b7d9f637c0820202020202020207e7c7f677f68757e170a616e640a53656c6c657220637573746f6469616e3a207e577a7e10456c617320636172626f6e20202020207e537a7e0d0a7573696e67206b6579203a207e547a76074b5341412d314d887e423032313833313039386135313533643635653335363361343630333661646265636638613564313736336164343236303133363838396336346465343531333061647e537a7e090a0a666f72202020207e7c7e1e0a0a546869732074726164652077617320636f6e6669726d6564206174207e787e9b20616e642068617320612032346820736574746c656d656e7420706572696f642e0a54686520736574746c656d656e7420616374696f6e2063616e206265207472696767657265642062792061207369676e61747572652066726f6d2074686520546f6b656e6f76617465207465737420637573746f6479206c656467657220616e792074696d65206166746572206d69646e69676874206f6e207e7c7f517f527f517f527f517f527f517f537f7c6b7e7c6b7e7c6b7e7c6b7e7c547f517f527f517f6b7c6b7e7c6b7e07543a3a2e5a2d2d886c517f517f517f0130947c0130945a95937c013094016495937c01309402e80395936c517f0130947c0130945a95936c517f0130947c0130945a95936c02303094517f6e815a9f698152a1697c5a959302100e956c02303094517f6e815a9f6981569f697c5a9593013c95936c02303094517f6e815a9f6981569f697c815a9593936c0330303094527f815a9f69517f815a9f69815a9f697c727b7c8c76637c011f937c8c6876637c011c937c8c6876637c011f937c8c6876637c011e937c8c6876637c011f937c8c6876637c011e937c8c6876637c011f937c8c6876637c011f937c8c6876637c011e937c8c6876637c011f937c8c6863011e93687c02e407947654967c549776647b76013ca0638b68677b8b687c026d0195937c02b50595025547939303805101957d937c0380510193760380510196025547947602b505967c02b505977c54957c76026e01a1648c6876026d01967b9302e407937c026d019776011fa163517c0067011f9468766378549763011c67011d686ea16377527c0067946868766376011fa163537c0067011f946868766376011ea163547c0067011e946868766376011fa163557c0067011f946868766376011ea163567c0067011e946868766376011fa163577c0067011f946868766376011fa163587c0067011f946868766376011ea163597c0067011e946868766376011fa1635a7c0067011f946868766376011ea1635b7c0067011e94686876635c7c0068757b7602e803960130937c7602e803970164960130937c760164975a960130937c5a970130937e7e7e012d7e7b765a960130937c5a970130937e7e012d7e7c765a960130937c5a970130937e7e7b7c7e482e0a0a466f72206d6f726520696e666f726d6174696f6e207365653a2068747470733a2f2f726563656970747669657765722e746f6b656e6f766174652e636f6d2f74726164652f7e7b7e4f0a4b5341412d314d2056657273696f6e3a20302e310a54686973207472616465206973207265636f72646564206f6e2074686520546f6b656e6f76617465207472616465206c65646765722e0a0a0a7e827c7e527a7e82090100000000000000fd7c7e7c7e6777527a20479896769e7aef3cdf4e64de14a3ec9f0a0ccb13227e8186597280311135cfcb8801667f77607f5c7f7701427f01177f77607f5d7f7701427f567f77517f7701197f02d5007f775a7f4c4c54686973207472616e73616374696f6e20697320616e2061636b6e6f776c656467656d656e742062792074686520546f6b656e6f76617465207465737420637573746f6469616e20666f7220557a6c607c827b7d9f637c0820202020202020207e7c7f677f687578887e1e200a686173207369676e6564207573696e67207075626c6963206b6579207e54797e120a746861742064656c6976657279206f66207e537a7e1c0a746f207468652062757965722076696120637573746f6469616e207e547a7e0b0a7573696e67206b6579207e537a7e1b0a69732061626c6520746f2070726f6365656420746f6461792c207e52797e7c7e827c7e014d7c7e090000000000000000fd7c7e68aa537a01207f7b7b88547f757c547f517f527f517f777b7502303094517f817c815a95937c0230309481517f7c815a95937b0330303094517f517f517f817c815a95937c81016495937c8102e80395937c7b7c8c76637c011f937c8c6876637c011c937c8c6876637c011f937c8c6876637c011e937c8c6876637c011f937c8c6876637c011e937c8c6876637c011f937c8c6876637c011f937c8c6876637c011e937c8c6876637c011f937c8c6863011e93687c02e407947654967c549776647b76013ca0638b68677b8b687c026d0195937c02b5059502554793930380510195a27baa517f517f517f517f517f517f517f517f517f517f517f517f517f517f517f517f517f517f517f517f517f517f517f517f517f517f517f517f517f517f517f7c7e7c7e7c7e7c7e7c7e7c7e7c7e7c7e7c7e7c7e7c7e7c7e7c7e7c7e7c7e7c7e7c7e7c7e7c7e7c7e7c7e7c7e7c7e7c7e7c7e7c7e7c7e7c7e7c7e7c7e7c7e01007e8100011f80517e9321414136d08c5ed2bf3ba048afe6dcaebafeffffffffffffffffffffffffffffff007d5296789f637897785296789f639467776867776876927f76927f76927f76927f76927f76927f76927f76927f76927f76927f76927f76927f76927f76927f76927f76927f76927f76927f76927f76927f76927f76927f76927f76927f76927f76927f76927f76927f76927f76927f76927f7c7e7c7e7c7e7c7e7c7e7c7e7c7e7c7e7c7e7c7e7c7e7c7e7c7e7c7e7c7e7c7e7c7e7c7e7c7e7c7e7c7e7c7e7c7e7c7e7c7e7c7e7c7e7c7e7c7e7c7e7c7e827c7e23022079be667ef9dcbbac55a06295ce870b07029bfcdb2dce28d959f2815b16f81798027c7e827c7e01307c7e01c27e2102b405d7f0322a89d0f9f3a98e6f938fdc1c969a8d1382a2bf66a71ae74a1e83b0adab6d51bd9ee9fd08f78288f3362f9c31e13f3058b878993c6f1404c7ac2190495e00b84e02000000fffffffff4010000000000000151010100000000000000fd870b264b5341412d314d2076302e3161736d0a7777772e746f6b656e6f766174652e636f6d0a0a0a0a757661608763616a6853795379517f517f517f517f517f517f517f517f517f517f517f517f517f517f517f517f517f517f517f517f517f517f517f517f517f517f517f517f517f517f517f517f517f517f517f517f75816b816b816b816b816b816b816b816b816b816b816b816b816b816b816b816b816b816b816b816b816b816b816b816b816b816b816b816b816b816b816b816b816b816b816b817f6c7f6c7f6c7f6c7f6c7f6c7f6c7f6c7f6c7f6c7f6c7f6c7f6c7f6c7f6c7f6c7f6c7f6c7f6c7f6c7f6c7f6c7f6c7f6c7f6c7f6c7f6c7f6c7f6c7f6c7f6c7f6c7f6c7f6c7f6c7f7c6b7e7c6b7e7c6b7e7c6b7e7c6b7e7c6b7e7c6b7e7c6b7e7c6b7e7c6b7e7c6b7e7c6b7e7c6b7e7c6b7e7c6b7e7c6b7e7c6b7e7c6b7ea8527901697f77527f7c7f587f7c815188547f7c83697c517f7c76014c9f6476014c8763755167014d8852687f7c687f7c76264b5341412d314d2076302e3161736d0a7777772e746f6b656e6f766174652e636f6d0a0a0a0a8763757b207db001ff1daaf52a399624a358e30a645d4c9eb9a1fbd77d019907618fb4610588756c756c6c6c6c6c6c76074b5341412d314d886c6c6c6c7604474d4558886c756c76055452414445886c756c756c751f546f6b656e6f766174652068617665206372656174656420612074657374207c7e42206576656e7420666f7220474d45582e2054686973207472616465206973206265696e67206d616e616765642062793a0a427579657220637573746f6469616e3a207e5a7a7a607c827b7d9f637c0820202020202020207e7c7f677f68757e0c0a7573696e67206b65793a207e59797e423032303130323033303430353036303730383039313031313132313331343135313631373138313932303231323232333234323532363237323832393330333133327e587a607c827b7d9f637c0820202020202020207e7c7f677f68757e170a616e640a53656c6c657220637573746f6469616e3a207e577a7e10456c617320636172626f6e20202020207e537a7e0d0a7573696e67206b6579203a207e547a76074b5341412d314d887e423032313833313039386135313533643635653335363361343630333661646265636638613564313736336164343236303133363838396336346465343531333061647e537a7e090a0a666f72202020207e7c7e1e0a0a546869732074726164652077617320636f6e6669726d6564206174207e787e9b20616e642068617320612032346820736574746c656d656e7420706572696f642e0a54686520736574746c656d656e7420616374696f6e2063616e206265207472696767657265642062792061207369676e61747572652066726f6d2074686520546f6b656e6f76617465207465737420637573746f6479206c656467657220616e792074696d65206166746572206d69646e69676874206f6e207e7c7f517f527f517f527f517f527f517f537f7c6b7e7c6b7e7c6b7e7c6b7e7c547f517f527f517f6b7c6b7e7c6b7e07543a3a2e5a2d2d886c517f517f517f0130947c0130945a95937c013094016495937c01309402e80395936c517f0130947c0130945a95936c517f0130947c0130945a95936c02303094517f6e815a9f698152a1697c5a959302100e956c02303094517f6e815a9f6981569f697c5a9593013c95936c02303094517f6e815a9f6981569f697c815a9593936c0330303094527f815a9f69517f815a9f69815a9f697c727b7c8c76637c011f937c8c6876637c011c937c8c6876637c011f937c8c6876637c011e937c8c6876637c011f937c8c6876637c011e937c8c6876637c011f937c8c6876637c011f937c8c6876637c011e937c8c6876637c011f937c8c6863011e93687c02e407947654967c549776647b76013ca0638b68677b8b687c026d0195937c02b50595025547939303805101957d937c0380510193760380510196025547947602b505967c02b505977c54957c76026e01a1648c6876026d01967b9302e407937c026d019776011fa163517c0067011f9468766378549763011c67011d686ea16377527c0067946868766376011fa163537c0067011f946868766376011ea163547c0067011e946868766376011fa163557c0067011f946868766376011ea163567c0067011e946868766376011fa163577c0067011f946868766376011fa163587c0067011f946868766376011ea163597c0067011e946868766376011fa1635a7c0067011f946868766376011ea1635b7c0067011e94686876635c7c0068757b7602e803960130937c7602e803970164960130937c760164975a960130937c5a970130937e7e7e012d7e7b765a960130937c5a970130937e7e012d7e7c765a960130937c5a970130937e7e7b7c7e482e0a0a466f72206d6f726520696e666f726d6174696f6e207365653a2068747470733a2f2f726563656970747669657765722e746f6b656e6f766174652e636f6d2f74726164652f7e7b7e4f0a4b5341412d314d2056657273696f6e3a20302e310a54686973207472616465206973207265636f72646564206f6e2074686520546f6b656e6f76617465207472616465206c65646765722e0a0a0a7e827c7e527a7e82090100000000000000fd7c7e7c7e6777527a20479896769e7aef3cdf4e64de14a3ec9f0a0ccb13227e8186597280311135cfcb8801667f77607f5c7f7701427f01177f77607f5d7f7701427f567f77517f7701197f02d5007f775a7f4c4c54686973207472616e73616374696f6e20697320616e2061636b6e6f776c656467656d656e742062792074686520546f6b656e6f76617465207465737420637573746f6469616e20666f7220557a6c607c827b7d9f637c0820202020202020207e7c7f677f687578887e1e200a686173207369676e6564207573696e67207075626c6963206b6579207e54797e120a746861742064656c6976657279206f66207e537a7e1c0a746f207468652062757965722076696120637573746f6469616e207e547a7e0b0a7573696e67206b6579207e537a7e1b0a69732061626c6520746f2070726f6365656420746f6461792c207e52797e7c7e827c7e014d7c7e090000000000000000fd7c7e68aa537a01207f7b7b88547f757c547f517f527f517f777b7502303094517f817c815a95937c0230309481517f7c815a95937b0330303094517f517f517f817c815a95937c81016495937c8102e80395937c7b7c8c76637c011f937c8c6876637c011c937c8c6876637c011f937c8c6876637c011e937c8c6876637c011f937c8c6876637c011e937c8c6876637c011f937c8c6876637c011f937c8c6876637c011e937c8c6876637c011f937c8c6863011e93687c02e407947654967c549776647b76013ca0638b68677b8b687c026d0195937c02b5059502554793930380510195a27baa517f517f517f517f517f517f517f517f517f517f517f517f517f517f517f517f517f517f517f517f517f517f517f517f517f517f517f517f517f517f517f7c7e7c7e7c7e7c7e7c7e7c7e7c7e7c7e7c7e7c7e7c7e7c7e7c7e7c7e7c7e7c7e7c7e7c7e7c7e7c7e7c7e7c7e7c7e7c7e7c7e7c7e7c7e7c7e7c7e7c7e7c7e01007e8100011f80517e9321414136d08c5ed2bf3ba048afe6dcaebafeffffffffffffffffffffffffffffff007d5296789f637897785296789f639467776867776876927f76927f76927f76927f76927f76927f76927f76927f76927f76927f76927f76927f76927f76927f76927f76927f76927f76927f76927f76927f76927f76927f76927f76927f76927f76927f76927f76927f76927f76927f76927f7c7e7c7e7c7e7c7e7c7e7c7e7c7e7c7e7c7e7c7e7c7e7c7e7c7e7c7e7c7e7c7e7c7e7c7e7c7e7c7e7c7e7c7e7c7e7c7e7c7e7c7e7c7e7c7e7c7e7c7e7c7e827c7e23022079be667ef9dcbbac55a06295ce870b07029bfcdb2dce28d959f2815b16f81798027c7e827c7e01307c7e01c27e2102b405d7f0322a89d0f9f3a98e6f938fdc1c969a8d1382a2bf66a71ae74a1e83b0adab6d5100000000")
	require.NoError(t, err)
	assert.Equal(t, txid, tx.TxID())

	var height uint32 = 1553037

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.ChainCfgParams, _ = chaincfg.GetChainParams("testnet")

	v := &Validator{
		txValidator: NewTxValidator(ulogger.TestLogger{}, tSettings),
	}

	ctx, _, endSpan := tracing.Tracer("validator").Start(context.Background(), "Test")
	defer endSpan()

	err = v.validateTransaction(ctx, tx, height, nil, &Options{})
	require.NoError(t, err)

	err = v.validateTransactionScripts(ctx, tx, height, []uint32{1553030, 1550102}, &Options{SkipPolicyChecks: true})
	require.NoError(t, err)
}

func TestValidateTx956685dffd466d3051c8372c4f3bdf0e061775ed054d7e8f0bc5695ca747d604_high_fee(t *testing.T) {
	initPrometheusMetrics()

	// height 229369] tx 956685dffd466d3051c8372c4f3bdf0e061775ed054d7e8f0bc5695ca747d604 failed validation: arc error 462: transaction input total satoshis cannot be zero
	txid := "956685dffd466d3051c8372c4f3bdf0e061775ed054d7e8f0bc5695ca747d604"
	tx, err := bt.NewTxFromString("010000000000000000ef015400c3490d91f3f742e73e81bc37dfca4f24f9a73a17c90ccab3012ddbc795bb000000008a473044022006a960f73ea637af867f69ed69edd291bee1d6daec241649caf909fb864dcd3b022011c82189c4a3379aba85fdb907d341db8067e426d7660fbba05c12fa370fa8aa0141048e69627b4807fe4ab00002a01c4a26a50d558cce969708e75dc5bfb345bbe92f06082757c85cbcac4ff0bbb91e221c59d3f9e675125da07e8110fd7d9b0ab6eeffffffff00000000000000001976a9146f9e896bb7cd9d27ca5b18c3ec9587ff0be7895188ac0100000000000000001976a9144477154cba7f0474a578fe734e00bd60513fbab588ac00000000")
	require.NoError(t, err)
	assert.Equal(t, txid, tx.TxID())

	var height uint32 = 229369

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.ChainCfgParams, _ = chaincfg.GetChainParams("mainnet")
	tSettings.Policy.MinMiningTxFee = 1000 // high fee

	v := &Validator{
		txValidator: NewTxValidator(ulogger.TestLogger{}, tSettings),
	}

	ctx := context.Background()

	ctx, _, endSpan := tracing.Tracer("validator").Start(ctx, "Test")
	defer endSpan()

	err = v.validateTransaction(ctx, tx, height, nil, &Options{})
	require.Error(t, err)
	require.Contains(t, err.Error(), "transaction fee is too low")
}

// func TestValidateTxdad5ecab132387e8e9b4e0330910c71930e637d840a5818eb92928668e52bbe5(t *testing.T) {
// 	initPrometheusMetrics()

// 	// [BLOCK][00000000000005a493a0720037aa5ff71c8f25ef36c51bfddcff7ae91bf48bf7] error extracting coinbase height: %!w(MISSING): 61: the coinbase signature script must start with the serialized block height
// 	txid := "dad5ecab132387e8e9b4e0330910c71930e637d840a5818eb92928668e52bbe5"
// 	tx, err := bt.NewTxFromString("01000000010000000000000000000000000000000000000000000000000000000000000000ffffffff33fabe6d6da722619098d70f00c6766cf515725d8b83194469797f756d2e39c75e469c70d60100000000000000062f503253482fffffffffcfa7da0700000000001976a9142ce4bc0302990935f84afca3ae1a0fc8877953a788ac60dc0700000000001976a9147c3ddd3ff49f2c0565aecbec80476fb00cee3edf88acb53a0800000000001976a914677085ab0ad5edab1ae1609858a32b95d36c866488aca18e0800000000001976a91474c2105825da113e5a21fde9b0d557eb3e6185f988ac55a70800000000001976a914eb24b0a111784ae67d465509cd852140ba66eb2888ac052b0900000000001976a914e7639725ba2925155a3bb2987268ad57236c59e988acf5640900000000001976a9145a312a517cd74b094561f78bdcd268ea46cd9c9188ac3f970900000000001976a91458a08ccf0fb238718d3904714000b17a81f0060d88acc2031000000000001976a914006e5858de7a93f56c168e77e2acc273fa09ce8288aca78f1000000000001976a914cf70f86013e29f133fdcc3c602e034338a19a90d88acf2a41000000000001976a914e0bd337faa5e94981d19fda04d9d1b8713b9809a88ac20dc1000000000001976a914482abbed049cf6fac24de223002510ffc38413e388ac830c1100000000001976a91491bf8d466b9803625bd48ac340bedd79e207ab5788ace9191100000000001976a91403dd220097e6be6676ee974cac5b3f458fdf051088ac41221200000000001976a9140f198b4b4a80e33f0d186d370dab582aef4a2a5d88ac9d351900000000001976a9144b47eb499f82d9c6927c93d63937332ca2cd602a88ac1d5e1900000000001976a91486a1c0a9f1157f084cda0920b3561ac495a525bc88acc8ff1900000000001976a9142ff545d2b6440d9029c7a1303fd212dcb52f43cd88ac9f181a00000000001976a914e2aa191e9b0279b2203759b88eec218778bb905f88ac98e61a00000000001976a914adfc4367b204d75386c83775e97164b987d9297d88ac67ba2000000000001976a91418a99959bce205725051f5ead31c4b88379a4fcb88ac62cb2000000000001976a9141dc79b2c61ba9e2381fedb4a63f54ea8cf950fc888ac65e22000000000001976a914e295305eb7e0a8e86f79fd0c5125a1b5c44c6aa588ac52622100000000001976a914ab15c53ced26f585c651f5ac28f006533fbdfb9288ac45052300000000001976a914a26b7247c5792672787e661c4b8fb629c97b83ae88ac92102300000000001976a914dced5af0a68dd47868205ad2067e92d98ea404de88aca1b72300000000001976a9141fa35cac70662f25661512847857999a00ce658f88aca5cb2300000000001976a914336d7d0d979311d9e9a3030cacb096d1a3845a0988acafd72300000000001976a91422636bf2ed0a6cf7b0e0fe2262bc30d50aaaaecf88ac15522600000000001976a91455f59189c3ce1c01a3833d088548bbb4c0364bf688acb81f2900000000001976a914f1f630e1649d4c90b6e9731bfb5c9170e8e7853788acf9f82900000000001976a914161199f7bbed0222b94d03dcdd24b15a705f5f1188ac1bba2a00000000001976a91438458f419b21b7ca10e0d6e7fefc85a523142bf388ac45ef2b00000000001976a91409c11b45c26ebef128252033da7605ff23684cca88acbdba3000000000001976a91407f021706161902974f6d62eec4612a77ceed20a88ac821f3100000000001976a9146f8c32f2c4147e0df5341d5adc58aa3cea1197f588ac0d1b3200000000001976a9141f91d93c73bbbd2506d3a0897d127c5de539fbe088ac62363200000000001976a9147f829605652e03ae9eeb8c37f8f2eb067192efd288ac3fc33200000000001976a91406346c4a74b9ba1526b92d50d016205e985fdab088ace9853300000000001976a914c759eb303f3ac4816da9efb71cbfb7bead3c316c88acb7ca3400000000001976a914a1e3eaba6014b21ad41c787a56591b7d952d716788acb0483600000000001976a9147c651c6877484d6ae06cf431cc5913f25a680a6088acbd1b3900000000001976a91497e95d5f876b8091824df8f93b564a70649607b688acd3523b00000000001976a91490ea32f17da5f97879bf02c5a228fa39645f7d9088acf73e3d00000000001976a914fd20b8cc38caf437312b5ecfdc9bb5fd5b0b271888ac22403d00000000001976a914c3a2f3551409af20a481ecfdb03fd2e3848343d588ac538c3d00000000001976a914cf2013b84b2fdae45d89554268635a76a4f0e8b988ac54ed3d00000000001976a914f19595088875fe2c80a118e4c7e24a82370cfd8e88ac99153e00000000001976a914bd947691320e51efb5db70cb70ec78d12840153988ac4eea3e00000000001976a91408d0008497c7d48e222699e1b38b663e9531849e88ac6e383f00000000001976a914ae84e741f3242a9e211bc36ceebcecff23aaea8c88ac8deb4100000000001976a914e10542ae9e9513fa89b35ae85d3c5c05f5aed95888ac13884200000000001976a914a33e478d9859f0a1c67f63ad0664c7124dacbb5f88acb62d4300000000001976a9141dfd861550dc47711a83a8ae2149df95be229e5688acc34d4300000000001976a914b8b755603d7c9e683b977671ecbeb79241ffcd8988ac12564300000000001976a9140a57e95b1bf862cf3c03b1c12e602f0442ba71fb88acf37f4300000000001976a91454a22bab38b27589d32e2b9c8c2428905dc1702c88acc04f4500000000001976a9145f604b9bbf6ff99bf0a4da8976d093ddbad5e79088ac7d254600000000001976a9143e9e24b602b382961357ed807eda5445d080678488ace05b4600000000001976a9148012d70eea0df45f2138f39bb20db8c60a2a421788ac72154700000000001976a914d82c42385a8383eb49d16674ac52dff8cbbc8c1b88ac38904700000000001976a914e349adcc7bab6e87ef16b7980c052c893c40e98588acc6434900000000001976a914345281ac54380e98ec5001ecfaea019600da68ba88ac29304a00000000001976a91433c1b0a94ee653f820adc8a0ebb46f3e03b9787c88ac02274b00000000001976a914711081cae27af99f99373efa25ae4f64c6ef6ba788ac2bb84b00000000001976a9145c7aa6fee64734943022d863c50525ba0caccab088ace6694c00000000001976a914195146c454bb21d4beba91b9bcc917bd957a39a388acd3a74c00000000001976a914d7d8ffdac53afd1bc3e1d7d20b29c4190f546d3d88ac1bd34c00000000001976a91450b4b36a79926000b7639779b2e6983c7bd7663588ac33224d00000000001976a9141dc3351755df6f0de6dedefd7c6cf2e4b435795888ac409f4e00000000001976a914ff85800ad6297e8ce459a04f6746477dd01f570b88acaeac4e00000000001976a914e7f1b2240b9e590980cf9958b2fa82c7ef06edd988aca7fa5500000000001976a9147487fe51c0a6a2188317835d21ada968ffb2e35688ac1cdd5600000000001976a914754f36491050c2cc39925b15182e0af97c9f724c88acef975700000000001976a9140676d03b8b826effc748a062241fd861f4fab58388ac00485b00000000001976a9147e1ce11b9186cf0ef3cd6f95cd0c085875e4715c88ac18895b00000000001976a9147ae1dcf9f1d50785536ea8198b188d73804a78e088acacb75b00000000001976a91469f3c972ba4798b4e715a9ac551e827eac806eb288acf6c05e00000000001976a9147fcc2253397dcf99e698167d18904a06e57e0dd588ac26b26000000000001976a91491827adef9de1adf76225769dbffd946893fa33188acd95c6100000000001976a9141d15c59d7e2a423518da141027047d15462f3c9988acd1916400000000001976a914669a712c3073b70b009357f239d32c99e79a431d88ac8bed6400000000001976a914237c5b18b794fe15036b4d61cb89f650e7bb1bf188ac7ef46500000000001976a91416e90674cf5a97d4726b80932ce186c989da9f7d88ace2426600000000001976a9140382de4d788d215ff9b90fe587dcb61f1069528688ac2a296700000000001976a9145479d3a0114c8cddf9db7a9968c2080310099b5188acfcc46800000000001976a914e69f755fcbb511cfc0d12722c3186e18f3ce589688ac4d786c00000000001976a914c5a09a8e77c60888a127f03f27a50ca1101bae8f88ac91246d00000000001976a91436a5289cb68303ef16c92eff57170af92baf4e7088ac9cd16d00000000001976a91466e1c71eec5d8b98932176c25d6f8704483303ad88ac853a6e00000000001976a9140ddcabcf7f2ad748bbc2ee205e55d9c2c9aa79f888acd5c27100000000001976a9146a98b950eaf4683ac1e738890b6a26ad8870b76888aceade7100000000001976a9144ff8472b4bf4482dd19dd530e8f6bae3c337241b88ac5b997600000000001976a91421bd2c39039c8d2fbee1bd069105de8599d9c26f88acc69d7900000000001976a91431029f074bd861c8f236c1c0adf1ae855680830488acc5a87e00000000001976a9146f7b51c376fd6dea8edb2007d049f60bb6071a1488ac9c368000000000001976a914d1b47911a42a2d1c4d26c42a1b94ba626780e86288ac4b278200000000001976a914580d858c05806ce0a0c4d5c725fe70f95aeeee0688acb7f38200000000001976a914c52c18005c5773e5b513aadc31c8a1fed3fe7ad588ac78518600000000001976a914c008f4c184c98d0647002af1bd47511a4017dbd488ac0eaa8700000000001976a914e1fe17adaba3338ec693468fc797662b005deb6088acee7c8800000000001976a914093064e56638d7d2b7f122d073f36f2adbddae3988acac7e8800000000001976a9145d79c60441d02be3fe14028982be5ded8b8f283f88ac93a68800000000001976a914069c0fe194b28e2dcb5d259fe358e7ed93ef723c88ac37f18800000000001976a91483aa26f9c40165c65a7087157ce153ee063a295788ac69468a00000000001976a9142c7de0b3d2d0557da88435e95fad0ed78ebd280b88ac47cc8d00000000001976a9140cb6f1c0e7e968a543379bb5ce61d388c127a94488ac09a98f00000000001976a914625bffc6c22886c997cc87c8384a5c874e0a3f8b88acb6659100000000001976a91495d69476ecd7c53639097cc1258df7a12669efc288ac5f549400000000001976a91428846a2ab65473d7d42723fdcaad02c15dff5b1c88ac037e9c00000000001976a9148c9360e25a76046c63816a38476253821850035588ace1b9a400000000001976a914dd7a879a2330eaddbff7ca726c4f35442e9ba3fd88ac476daa00000000001976a914e42684e3f0797bb2c126cada06532eb3042d1bbe88ac7418af00000000001976a9144a26821fb905593af5dd4d59d3e60d23dfe2a56c88acd1adb200000000001976a9145f403767b993718e333a14e99dd36d85f7420ddc88acaf69b500000000001976a914509341b5913426c95a82c45522d723bd0994b17688acfdbcb500000000001976a914309467cf0306dd5a9d782132ae020215cc6a3e7e88acf926b700000000001976a9147f92bc474f4d80ac06c8f02d57915bf550c8447b88acd6a4ba00000000001976a914e205b664ff00fd8be0a78cbee242d60f83af69e188acf0bbbb00000000001976a91404a64323ddc9c0385aaeebe53eea5c8f2eed6cf588ac8a7fbc00000000001976a914c4ccb62dfe223631f2e86c47c3fd6663dd49d4a288acbd2dbd00000000001976a914da5837964c5edd60088af63aaaf97f862a6fe4c888aca563c400000000001976a9143a7ab2cefaeb6345aef29bae00530548f65bc3fe88ac7e85c400000000001976a914ec8f348275950702f627f83cf31613b035a7ca2e88ac9285c400000000001976a914fe8548ba52272233428c51268aca61f8b7046e8688ace647cb00000000001976a9147c459dc456f019604044dbbbe96a0108d781d5cd88ac434ed100000000001976a91464eb5bbd8ab5393cc2e22e07ed63d6ebef5a8e3888acb891e300000000001976a9149d1f7e94a069768d1a400cd008eb9bfef87eace688ac85f9ec00000000001976a914b21d7936b1f919be9129cecff10f7ee9948972ad88acb367ed00000000001976a914f770bd8837733b0e33eb0817ccc087ced11901ca88acaefeed00000000001976a914e9b71ac3599a3c431414b9554587d7d87dfa2fe488acd014ef00000000001976a91477d07e5e8923c38e7f1c76b3096cc1cad2f1e48c88acb2bdef00000000001976a91462322eadf7c572830bac797a0d43cb0e0afea08988ac1786f000000000001976a91485a837db1cfb5e0c0ac07b6deab52f96e2aadc9288aca959f500000000001976a914044ce4661a426460ee298a4fc0be56d9531cab3688ac5876ff00000000001976a914055dd083a5db9bae3771c7b59443ff65f3bcac3588acd51e0001000000001976a914aefe856bbf3082f969f18f45ddc0ef40d6d6e22988ac01a50d01000000001976a914ee3b1fd4dcea91460a2c55ae8193d6e4836717b688ac35c10d01000000001976a914eabe80ff2219adf238cee601eb86296760534d7a88ac26522301000000001976a914d2236f9f3440b372ff6b82adf163497207544a6d88acecbc2301000000001976a914c33286f9c471d838cfd03498e846d1d3c03d072088ac56cd2901000000001976a914a5aef750d2240b740172e349e7a4eb6ad2b608f088ac2c692b01000000001976a9147339aac893eafd1795bab89c18a7eac8c8e0632288ac8b0e2d01000000001976a914c0d73c61fc15d89f04595cefcbeeec809ebe939788acdddc3101000000001976a9144ffc55d9053f5955ea367f861773e6707c29f8bd88acbff33101000000001976a914465726c2fd42218e0f4efa15f9ca23607fa3aefe88ac604b3a01000000001976a914d4e2b8073c3d2b244547574afde509e7f6db9dd788ac42214301000000001976a9140aca7ad87e8ed090e2d8823028ea1b2253f2db9788ac5fbd4e01000000001976a914822d54065586040f7e8c15ac4c7053f8b83dfb1f88ac82145101000000001976a914e73203a76ecb13a763b9396ee1d15a0c4c6a201288ac576c5d01000000001976a91444ee365dc5924fdb46b8db0db9f8965f1528390e88ac70e75d01000000001976a91419429cadd990e47a8b5f9f8064a5c024139cd6b088acc6505e01000000001976a914bcf82a713a320942ce47e073787b48e73ed21bc388ac998a6501000000001976a9142b737f1c7a55c4dc3700ccaff637042b5268710088ac11606901000000001976a9140af27cf6a67f19572ab58189994866937284e13f88acc1fb7301000000001976a914f617548af99e3dedc5b6b49bd23dda66dab58c9f88ac58a78401000000001976a91472bdf07695dba63a8f66047a7fbaab1859531fdf88ac87b59101000000001976a914a739fc4a3078bded68ca37a8f51aab017565eb7c88ac4bb8a001000000001976a914d766fbabc1881c81a87c32b2aef4053f0039603488ac27d2a001000000001976a914015dbc353e51ced0794cd834666e715ff649ee6988ac4a3ba301000000001976a914211860c881a8c9adf853eb6b29d0eb36113c07c188ac1e3ea401000000001976a914d60a86e3722e05fc1edde4b3f18bf37c133169ec88ac5838b501000000001976a91481721c0ef7136b22fca8ba5386850e53479b53aa88ac7cf3b601000000001976a9143dbb1f0660fa3cc5b4fbe3bd916113794571f1a988ac3765c501000000001976a91470e9e21c844119978e6e4121081636a0968b3e1388ac0f1fd201000000001976a91495ab2e374619aad24375d0e374ee697e9075cb5488ac394bd401000000001976a9144ed72d734ce7080b17203f07263646b81c5f747488ac3551dd01000000001976a91410519f2852d61880aa241691187df0a2fab8631388ac1e071202000000001976a9149ef0d190d4e5ebb2f153ba2035cdb65b6ad3a62c88ace5531c02000000001976a914bc589b3115f57c322c367f9e044e69e67934cf4588acad574202000000001976a91425468390939811de50f08d42145f52466e6c5d9a88ace3b74302000000001976a914f025c7c67f6b5abbfd852906b6abea1f557ff75088ac7d914b02000000001976a9149fa4252895df699ccdf0f1e170dffe7d253d83e788accf0d5102000000001976a91458624438e431f9f2c842fbbe6aaf63e3825f4ab388ace34b5d02000000001976a914af947fd64a628b78ba7e26604787bff8337246dc88ac39537902000000001976a9145414002817073c9e52de5931bcad90121719eb1888acb9717e02000000001976a914160767ea477550a19b9fbb7d91cc3047e8adac3588ac46e79802000000001976a914e98e479f97347d6e9cca87bb924abbe1bda8028788ac4ea2a802000000001976a914d9f4097a319c5c7d7ca9024776e84ac482fa127988acd16ac202000000001976a914fa183499bcf8cc1bae1763cf50fd61f9601ec83088acfbb01403000000001976a914c168323170c52e9a28c4b069da735bffe8d651ab88ac305b1a03000000001976a914e75c0a4d431629abd09a64ef76739510e560c75288ac78cd4103000000001976a914c9f77268f2a519c756a066e6336740eac35812ac88acbf696f03000000001976a9148c2e965c648b96706212fa8802eb317b9f80615188acb9487703000000001976a914730f40ca59fa6c61a140ca07db33f33c143a649488acd7ba8e03000000001976a914626cfbd1437fb8d623b339c908dd2f966bc51ec988acbdaaee03000000001976a91480207c9f445b47ec5e2a4607861ea2d8fd87718188acb7ab0804000000001976a91466d60d45b8aa75b88fd1bb4d8b8a7950950dc6e588ac51e91404000000001976a91416a6074ae677b4b08c88f5f2b761bb46cebce82388ac03122c04000000001976a914062cf90576517139e955b1253b225e6b5007724f88ac15eb4504000000001976a914391ecba1418302c1822c7c65f9aeb647795ed81c88acfbd7ab04000000001976a9149e964bfd4d25ad4e168b50f86ed2a2f32b5203fe88ac6a71dc04000000001976a914ba0b189cd808746a1efc9921665dfcfe301222e288aca053e404000000001976a9145833a28fb708738333c4a660132d1f58fc51513488acf34e0a05000000001976a9140e395d4568e071968db0e8e775f891021d4c82f088ac5c747305000000001976a91459a41625ae4cfc02de7ab175163d9b58a05d469988ac54e07405000000001976a914de845847f9853f4bdb99753be0457e47e0863bc888acb5b9c105000000001976a9145361e583312eac7c94946d80314f89c0590e900888aca90c8806000000001976a91419d933d12a0c2d171344a691c4b8177bd934443288ac45b7df06000000001976a914bb7a6eaef7fb4e4605f1e0add3dd40cdaa9f578f88ac6f981909000000001976a91427cc7e0def9fb434d51d98ee523bcc1e7514390688ac47a16809000000001976a914e5145dd1908a5cd08ece0a7b7b1bf57133b39f1088ac186c8209000000001976a9140653fee4df7b43c93d22d9f90efa9856f256cecc88ac32efda0b000000001976a9149c7a3ddca3446a6e36160495a78317fe1cb8f1cc88ace4b6f22a000000001976a9147db536ce753378064c58ccc9cffdc222626fd0f788ac8267e20000000000434104ffd03de44a6e11b9917f3a29f9443283d9871c9d743ef30d5eddcd37094b64d1b3d8090496b53256786bf5c82932ec23c3b74d9f05a6f95a8b5529352656664bac00000000000000002120c388ce687e4c5d6d758584927b920632946411118fdb9aacac8c8f0906967a3400000000")
// 	require.NoError(t, err)
// 	assert.Equal(t, txid, tx.TxID())

// 	var height uint32 = 196794

// 	v := &Validator{}

// 	ctx := context.Background()

// 	ctx, span := tracing.Start(ctx, "Test")
// 	defer span.End()

// 	err = v.validateTransaction(span, tx, height)
// 	require.NoError(t, err)

// }

var testTransactions = []TestTx{
	{
		TxID:        "ba4f9786bb34571bd147448ab3c303ae4228b9c22c89e58cc50e26ff7538bf80",
		BlockHeight: 264084,
		ExtendedTx:  "010000000000000000ef0140ed240a0a018469a1cdd3656800c23ec796d698d82ea336c4b6fb72b141c9cd000000008b483045022100de3b4c67e8a3eb09f18e8b5834257c0a27812df490365b8ac0e30e1d3dcc01630220426997ee904736647ae2e0b93cb2a11511f7b33e8f8a8ce0c5265cbd5b113ae8504104b1796b0e02f327e1a6f61abdff028374de9c80d6189460b0b7035752a2d00364fb19f16868ba34a4e93350e49e6ff8bfb48d23ab15f14871b01d6562f69b9973ffffffff0065cd1d000000001976a914990a8e1eb7a69c41602bd46fed56b6a38fd9bc1e88ac0300e1f505000000001976a914ac625f248f3be5c1c17767b8b2b93dd03553984788ace0e2cf17000000001976a9142c00769e224ac558cf0e726c8e4d6aa9d34f6e5688ac107a0700000000001976a9144bd0ac767d24acc4c5af736767b7b3acd1a6776188ac00000000",
		UTXOHeights: []uint32{264074},
	},
	{
		TxID:        "0fb02dfd6811c72379ddcd4b25352ff85d77f8cc52723ee087c50042236ff470",
		BlockHeight: 478632,
		ExtendedTx:  "010000000000000000ef0225894047a817115970e49cc0b77dc2e1fd5d185632c244c055512dc490e82a5a000000006a47304402205bccbecf7d1658032759c4d182a8170174ad7dc687d58555707198a1e230f84f02203ae6804eef77319876223db1b354828e1beeccaeafb6d667879436eef7aa5f3141210300e3b001c4addf714e8c4d5ac1427fb19349d3d05e416e47fc6186cd2d95eb0effffffff962fb15b000000001976a9145632e2f253ac71e2dda1a8dcec6eac384a74251b88ac88ad2770b0bc82235a6414c68d4eb8764750c48178ffa01f6a93ba972dc1e418000000006b483045022100c348ff56a2f00b608fadb4583b76a9a91079bf25b507ef44da562e8aa90fbdd202204fc40afdb76409527a05cfb67fbc67d496e6b0bd720121a26779123b5dd30212412102381da97b922c1584ca700f97650f1a2c2dcdba8a480ff998541504263f5ce551ffffffff00e1f505000000001976a914f9878c4ef91c883c451bee25ca6daf17da20a29688ac0116b16d61000000001976a914cc401501b36a914bc31f2322612b673cbb98a2d788ac00000000",
		UTXOHeights: []uint32{478632, 478632},
	},
}

func TestValidateTransactions(t *testing.T) {
	initPrometheusMetrics()
	// interpreter.InjectExternalVerifySignatureFn(bdksecp256k1.VerifySignature) // Use import bdksecp256k1 "github.com/bitcoin-sv/bdk/module/gobdk/secp256k1"

	for _, testData := range testTransactions {
		tx, err := bt.NewTxFromString(testData.ExtendedTx)
		require.NoError(t, err)

		tSettings := test.CreateBaseTestSettings(t)
		tSettings.ChainCfgParams, _ = chaincfg.GetChainParams("mainnet")

		v := &Validator{
			txValidator: NewTxValidator(ulogger.TestLogger{}, tSettings),
		}

		ctx, _, endSpan := tracing.Tracer("validator").Start(context.Background(), "Test")
		defer endSpan()

		err = v.validateTransaction(ctx, tx, testData.BlockHeight, testData.UTXOHeights, &Options{})
		require.NoError(t, err)

		err = v.validateTransactionScripts(ctx, tx, testData.BlockHeight, testData.UTXOHeights, &Options{SkipPolicyChecks: true})
		require.NoError(t, err, fmt.Sprintf("Failed with TxID %v", testData.TxID))
	}
}

func TestValidateTxba4f9786bb34571bd147448ab3c303ae4228b9c22c89e58cc50e26ff7538bf80(t *testing.T) {
	initPrometheusMetrics()

	// [height 249976] tx ba4f9786bb34571bd147448ab3c303ae4228b9c22c89e58cc50e26ff7538bf80 failed validation: script execution failed: false stack entry at end of script execution
	txid := "ba4f9786bb34571bd147448ab3c303ae4228b9c22c89e58cc50e26ff7538bf80"
	tx, err := bt.NewTxFromString("010000000000000000ef0140ed240a0a018469a1cdd3656800c23ec796d698d82ea336c4b6fb72b141c9cd000000008b483045022100de3b4c67e8a3eb09f18e8b5834257c0a27812df490365b8ac0e30e1d3dcc01630220426997ee904736647ae2e0b93cb2a11511f7b33e8f8a8ce0c5265cbd5b113ae8504104b1796b0e02f327e1a6f61abdff028374de9c80d6189460b0b7035752a2d00364fb19f16868ba34a4e93350e49e6ff8bfb48d23ab15f14871b01d6562f69b9973ffffffff0065cd1d000000001976a914990a8e1eb7a69c41602bd46fed56b6a38fd9bc1e88ac0300e1f505000000001976a914ac625f248f3be5c1c17767b8b2b93dd03553984788ace0e2cf17000000001976a9142c00769e224ac558cf0e726c8e4d6aa9d34f6e5688ac107a0700000000001976a9144bd0ac767d24acc4c5af736767b7b3acd1a6776188ac00000000")
	require.NoError(t, err)
	assert.Equal(t, txid, tx.TxID())

	var height uint32 = 249976

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.ChainCfgParams, _ = chaincfg.GetChainParams("mainnet")

	v := &Validator{
		txValidator: NewTxValidator(ulogger.TestLogger{}, tSettings),
	}

	ctx, _, endSpan := tracing.Tracer("validator").Start(context.Background(), "Test")
	defer endSpan()

	err = v.validateTransaction(ctx, tx, height, nil, &Options{})
	require.NoError(t, err)

	err = v.validateTransactionScripts(ctx, tx, height, []uint32{height}, &Options{SkipPolicyChecks: true})
	require.NoError(t, err)
}

func TestValidateTx944d2299bbc9fbd46ce18de462690907341cad4730a4d3008d70637f41a363b7(t *testing.T) {
	initPrometheusMetrics()
	// interpreter.InjectExternalVerifySignatureFn(bdksecp256k1.VerifySignature) // Use import bdksecp256k1 "github.com/bitcoin-sv/bdk/module/gobdk/secp256k1"

	txid := "944d2299bbc9fbd46ce18de462690907341cad4730a4d3008d70637f41a363b7"
	tx, err := bt.NewTxFromString("010000000000000000ef01b136c673a9b815af2bfdeccc9479deec3273ee98a188c26d3c14b5e6bfcbca0b010000006b48304502200241ac9536c536f21e522dec152e69674094b371b14c26edf706e1db0e6487190221008ee66bdafc7d39ee041e1425a7b2df780702e9b066c3a1e9715b03b23fbd99be41210373c9cb2feaa59dd208ad90dc4c8f32dac7a30a65e590fa16e2a421637927ae63feffffff4004fb0b000000001976a91471902a65346b0d951358ec9a1b306ecd36d284ae88ac0280969800000000001976a914dd37ee4ce93278fbc398abcda001d1d855841e0788ac3cd35d0b000000001976a914d04ad25d93764cf83aca0ca0c7cbb7ba8850f75888ac00000000")
	require.NoError(t, err)
	assert.Equal(t, txid, tx.TxID())

	var height uint32 = 478631

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.ChainCfgParams, _ = chaincfg.GetChainParams("mainnet")

	v := &Validator{
		txValidator: NewTxValidator(ulogger.TestLogger{}, tSettings),
	}

	ctx, _, endSpan := tracing.Tracer("validator").Start(context.Background(), "Test")
	defer endSpan()

	err = v.validateTransaction(ctx, tx, height, nil, &Options{})
	require.NoError(t, err)

	err = v.validateTransactionScripts(ctx, tx, height, []uint32{height}, &Options{SkipPolicyChecks: true})
	require.NoError(t, err)
}

func TestIsFinal7b27e3ed7ebf878d985f5fcc35bfbf0e3116489ff75f5e7eec8480780975920c(t *testing.T) {
	tx, err := bt.NewTxFromString("010000000000000000ef01ffd5c45f91eac31aa75de5cb1cac9d559f25e732f620b09582bf5d9b8aaa6d41010000006b48304502207c2cc4f80d58a524384364c250611a78a312045ef1ed6ab7def42438ce743cfc022100c8686df6eadcd8638fc91d4aee36e4d60793bcc71ff298f9600c869e14a1058601210319564fb11701a3aa24057afcdc24db5b1880339ec8612914cbd0b6ad8ba9211f10ffffff124a1700000000001976a914335df8a6ed745a6fb37ea8c1c3ff3d4257d222aa88ac0240420f00000000001976a9140b655a2a9748b30d6a1c9df810f116d2c8386b3d88acc2e00700000000001976a9145ebf8758fba212df6ed18eceecc1533574a8dc8988ac602d2653")

	require.NoError(t, err)
	assert.Equal(t, "7b27e3ed7ebf878d985f5fcc35bfbf0e3116489ff75f5e7eec8480780975920c", tx.TxIDChainHash().String())

	err = util.IsTransactionFinal(tx, 290918, 1395011852) // Block time
	require.NoError(t, err)
}
func TestIsFinalb633531280f980108329e3e0b9335b2290892d120916f9e17a9e3033bde1260b(t *testing.T) {
	tx, err := bt.NewTxFromString("010000000000000000ef017eac59de00f046e6d0a68d24963ae6273dc743e6eb47435389feb797ba2eed11000000008b4830450220781bf3ae77e09c900c5b5bcd4ef8800d0bb2313cff3691eedec4193d0daed489022100b1c3b624e1d8ffeed997067dc14cf95587ac85aa4406cc2823ee1540ac69c6a101410420ecf1f137e2aec6a47ae844b695772ab5a9177d4a830b328fbe753c41dc2d99cee9e13de3de5bb85e56b83b6923a147365b19747e7afe16c92625d5b121374eff0000ffb8b10e00000000001976a914d20671f2248ff5176fc245b1c4f72256f77bc00988ac01a88a0e00000000001976a9143910660af6df1e1aed432be83d8e9036d69ed14988ac81cd7c53")

	require.NoError(t, err)
	assert.Equal(t, "b633531280f980108329e3e0b9335b2290892d120916f9e17a9e3033bde1260b", tx.TxIDChainHash().String())

	// Block time          1400689692
	// Median time past is 1400686491
	err = util.IsTransactionFinal(tx, 301926, 1400689692) // Block time
	require.NoError(t, err)
}

// txFeeSize represents transaction metadata for block assembly testing.
type txFeeSize struct {
	txHash     *chainhash.Hash
	fee        uint64
	size       uint64
	txInpoints subtree.TxInpoints
}

// MockBlockAssemblyStore provides a test double for block assembly storage operations.
type MockBlockAssemblyStore struct {
	returnError error
	storedTxs   []txFeeSize
	removedTxs  []chainhash.Hash
}

func (s *MockBlockAssemblyStore) Store(_ context.Context, hash *chainhash.Hash, fee, size uint64, txInpoints subtree.TxInpoints) (bool, error) {
	if s.returnError != nil {
		return false, s.returnError
	}

	if s.storedTxs == nil {
		s.storedTxs = make([]txFeeSize, 0)
	}

	s.storedTxs = append(s.storedTxs, txFeeSize{
		txHash:     hash,
		fee:        fee,
		size:       size,
		txInpoints: txInpoints,
	})

	return true, nil
}

func (s *MockBlockAssemblyStore) RemoveTx(_ context.Context, hash *chainhash.Hash) error {
	if s.returnError != nil {
		return s.returnError
	}

	if s.removedTxs == nil {
		s.removedTxs = make([]chainhash.Hash, 0)
	}

	s.removedTxs = append(s.removedTxs, *hash)

	return nil
}

func Benchmark_validateInternal(b *testing.B) {
	txF65eHex, err := os.ReadFile("./testdata/f65ec8dcc934c8118f3c65f86083c2b7c28dad0579becd0cfe87243e576d9ae9.bin")
	require.NoError(b, err)
	tx, err := bt.NewTxFromBytes(txF65eHex)
	require.NoError(b, err)

	tSettings := test.CreateBaseTestSettings(b)
	tSettings.ChainCfgParams, _ = chaincfg.GetChainParams("mainnet")

	v := &Validator{
		txValidator: NewTxValidator(ulogger.TestLogger{}, tSettings),
	}

	for i := 0; i < b.N; i++ {
		err = v.validateTransaction(context.Background(), tx, 740975, nil, &Options{})
		require.NoError(b, err)

		err = v.validateTransactionScripts(context.Background(), tx, 740975, []uint32{740975}, &Options{SkipPolicyChecks: true})
		require.NoError(b, err)
	}
}

func TestExtendedTxa1f6a4ffcfd7bb4775790932aff1f82ac6a9b3b3e76c8faf8b11328e948afcca(t *testing.T) {
	parentTx, err := bt.NewTxFromString("0100000001c323b444df62c6d1290d6be7a7db120a914c2c52ba330dcef240291e5251dc68010000006b48304502202b72aba30dd51ab93939397932ba7db51a728ab395cddfff161220f53959e03e022100cda87f332e80184c2e28eedb42b2334248e9a0ef90bb676b8504e449dcac924d012102786409cdbb55392b04e55d32d3f0c6964193b61dc537cc75f565c6535f4a9c5affffffff0101000000000000000000000000")
	require.NoError(t, err)

	script, err := hex.DecodeString("76a914bd29edb61cd56669749a8cad8debff14391f848f88ac")
	require.NoError(t, err)

	parentTx.Inputs[0].PreviousTxScript = bscript.NewFromBytes(script)
	parentTx.Inputs[0].PreviousTxSatoshis = 100_000_000

	require.True(t, parentTx.IsExtended())

	tx, err := bt.NewTxFromString("010000000165ad3fbf66422ec03e83850f2fdb7a6f8f8b099cf517e723a4d817c72bf7e638000000000151ffffffff0101000000000000001976a914ffca5cf550ad617598d10342b78317c2a563b77888ac00000000")
	require.NoError(t, err)
	assert.Equal(t, "a1f6a4ffcfd7bb4775790932aff1f82ac6a9b3b3e76c8faf8b11328e948afcca", tx.TxID())

	assert.False(t, tx.IsExtended())

	ctx := context.Background()

	container, err := aeroTest.RunContainer(ctx)
	require.NoError(t, err)

	t.Cleanup(func() {
		err = container.Terminate(ctx)
		require.NoError(t, err)
	})

	host, err := container.Host(ctx)
	require.NoError(t, err)

	port, err := container.ServicePort(ctx)
	require.NoError(t, err)

	aerospikeContainerURL := fmt.Sprintf("aerospike://%s:%d/test?set=test&expiration=1m&externalStore=file://./data/externalStore", host, port)
	aeroURL, err := url.Parse(aerospikeContainerURL)
	require.NoError(t, err)

	tSettings := test.CreateBaseTestSettings(t)

	// teranode db client
	var db *teranode_aerospike.Store
	db, err = teranode_aerospike.New(ctx, ulogger.TestLogger{}, tSettings, aeroURL)
	require.NoError(t, err)

	db.SetExternalStore(memory.New())

	_, err = db.Create(ctx, parentTx, 500_000)
	require.NoError(t, err)

	err = db.PreviousOutputsDecorate(ctx, tx)
	require.NoError(t, err)

	assert.True(t, tx.IsExtended())

	for _, input := range tx.Inputs {
		assert.NotNil(t, input.PreviousTxScript, "PreviousTxScript should not be nil")
	}
}

func Test_getUtxoBlockHeights(t *testing.T) {
	ctx := context.Background()

	tx, err := bt.NewTxFromString("010000000000000000ef03fe1a25c8774c1e827f9ebdae731fe609ff159d6f7c15094e1d467a99a01e03100000000002012affffffffa086010000000000018253a080075d834402e916390940782236b29d23db6f52dfc940a12b3eff99159c0000000000ffffffffa086010000000000100f5468616e6b7320456c69676975732161e4ed95239756bbb98d11dcf973146be0c17cc1cc94340deb8bc4d44cd88e92000000000a516352676a675168948cffffffff40548900000000000763516751676a680220aa4400000000001976a9149bc0bbdd3024da4d0c38ed1aecf5c68dd1d3fa1288ac20aa4400000000001976a914169ff4804fd6596deb974f360c21584aa1e19c9788ac00000000")
	require.NoError(t, err)

	txBytes := tx.Bytes() // non-extended tx
	txNonExtended, err := bt.NewTxFromBytes(txBytes)
	require.NoError(t, err)

	tSettings := settings.NewSettings()

	t.Run("not mined parent txs", func(t *testing.T) {
		mockUtxoStore := utxostore.MockUtxostore{}

		v := &Validator{
			settings:  tSettings,
			utxoStore: &mockUtxoStore,
		}

		mockUtxoStore.On("GetBlockState").Return(utxostore.BlockState{Height: 1000, MedianTime: 1000000000})

		mockUtxoStore.On("Get", mock.Anything, mock.Anything, mock.Anything).Return(&meta.Data{
			BlockHeights: make([]uint32, 0),
		}, nil)

		utxoHashes, err := v.getUtxoBlockHeightsAndExtendTx(ctx, tx, tx.TxID())
		require.NoError(t, err)

		expected := []uint32{1001, 1001, 1001}

		if !reflect.DeepEqual(utxoHashes, expected) {
			t.Errorf("getUtxoBlockHeightsAndExtendTx() got = %v, want %v", utxoHashes, expected)
		}
	})

	t.Run("mined parent txs", func(t *testing.T) {
		mockUtxoStore := utxostore.MockUtxostore{}

		v := &Validator{
			settings:  tSettings,
			utxoStore: &mockUtxoStore,
		}

		mockUtxoStore.On("GetBlockState").Return(utxostore.BlockState{Height: 1000, MedianTime: 1000000000})

		mockUtxoStore.On("Get", mock.Anything, mock.MatchedBy(func(hash *chainhash.Hash) bool {
			return hash.String() == "10031ea0997a461d4e09157c6f9d15ff09e61f73aebd9e7f821e4c77c8251afe"
		}), mock.Anything).Return(&meta.Data{
			BlockHeights: []uint32{125, 126},
		}, nil).Once()

		mockUtxoStore.On("Get", mock.Anything, mock.MatchedBy(func(hash *chainhash.Hash) bool {
			return hash.String() == "9c1599ff3e2ba140c9df526fdb239db236227840093916e90244835d0780a053"
		}), mock.Anything).Return(&meta.Data{
			BlockHeights: []uint32{},
		}, nil).Once()

		mockUtxoStore.On("Get", mock.Anything, mock.MatchedBy(func(hash *chainhash.Hash) bool {
			return hash.String() == "928ed84cd4c48beb0d3494ccc17cc1e06b1473f9dc118db9bb56972395ede461"
		}), mock.Anything).Return(&meta.Data{
			BlockHeights: []uint32{768, 769},
		}, nil).Once()

		utxoHashes, err := v.getUtxoBlockHeightsAndExtendTx(ctx, tx, tx.TxID())
		require.NoError(t, err)

		expected := []uint32{125, 1001, 768}

		if !reflect.DeepEqual(utxoHashes, expected) {
			t.Errorf("getUtxoBlockHeightsAndExtendTx() got = %v, want %v", utxoHashes, expected)
		}
	})

	t.Run("non extended mined parent txs", func(t *testing.T) {
		mockUtxoStore := utxostore.MockUtxostore{}

		v := &Validator{
			settings:  tSettings,
			utxoStore: &mockUtxoStore,
		}

		expectedOutputs := make(map[string][]*bt.Output)
		expectedOutputs["10031ea0997a461d4e09157c6f9d15ff09e61f73aebd9e7f821e4c77c8251afe"] = []*bt.Output{
			{
				LockingScript: bscript.NewFromBytes([]byte("10031ea0997a461d4e09157c6f9d15ff09e61f73aebd9e7f821e4c77c8251afe")),
				Satoshis:      1000000,
			},
		}
		expectedOutputs["9c1599ff3e2ba140c9df526fdb239db236227840093916e90244835d0780a053"] = []*bt.Output{
			{
				LockingScript: bscript.NewFromBytes([]byte("9c1599ff3e2ba140c9df526fdb239db236227840093916e90244835d0780a053")),
				Satoshis:      2000000,
			},
		}
		expectedOutputs["928ed84cd4c48beb0d3494ccc17cc1e06b1473f9dc118db9bb56972395ede461"] = []*bt.Output{
			{
				LockingScript: bscript.NewFromBytes([]byte("928ed84cd4c48beb0d3494ccc17cc1e06b1473f9dc118db9bb56972395ede461")),
				Satoshis:      3000000,
			},
		}

		mockUtxoStore.On("GetBlockState").Return(utxostore.BlockState{Height: 1000, MedianTime: 1000000000})

		mockUtxoStore.On("Get", mock.Anything, mock.MatchedBy(func(hash *chainhash.Hash) bool {
			return hash.String() == "10031ea0997a461d4e09157c6f9d15ff09e61f73aebd9e7f821e4c77c8251afe"
		}), mock.Anything).Return(&meta.Data{
			BlockHeights: []uint32{125, 126},
			Tx: &bt.Tx{
				Outputs: expectedOutputs["10031ea0997a461d4e09157c6f9d15ff09e61f73aebd9e7f821e4c77c8251afe"],
			},
		}, nil).Once()

		mockUtxoStore.On("Get", mock.Anything, mock.MatchedBy(func(hash *chainhash.Hash) bool {
			return hash.String() == "9c1599ff3e2ba140c9df526fdb239db236227840093916e90244835d0780a053"
		}), mock.Anything).Return(&meta.Data{
			BlockHeights: []uint32{},
			Tx: &bt.Tx{
				Outputs: expectedOutputs["9c1599ff3e2ba140c9df526fdb239db236227840093916e90244835d0780a053"],
			},
		}, nil).Once()

		mockUtxoStore.On("Get", mock.Anything, mock.MatchedBy(func(hash *chainhash.Hash) bool {
			return hash.String() == "928ed84cd4c48beb0d3494ccc17cc1e06b1473f9dc118db9bb56972395ede461"
		}), mock.Anything).Return(&meta.Data{
			BlockHeights: []uint32{768, 769},
			Tx: &bt.Tx{
				Outputs: expectedOutputs["928ed84cd4c48beb0d3494ccc17cc1e06b1473f9dc118db9bb56972395ede461"],
			},
		}, nil).Once()

		utxoHashes, err := v.getUtxoBlockHeightsAndExtendTx(ctx, txNonExtended, txNonExtended.TxID())
		require.NoError(t, err)

		expected := []uint32{125, 1001, 768}

		if !reflect.DeepEqual(utxoHashes, expected) {
			t.Errorf("getUtxoBlockHeightsAndExtendTx() got = %v, want %v", utxoHashes, expected)
		}

		for i, input := range txNonExtended.Inputs {
			if input.PreviousTxSatoshis != expectedOutputs[txNonExtended.Inputs[i].PreviousTxIDChainHash().String()][0].Satoshis {
				t.Errorf("Expected PreviousTxSatoshis %d, got %d", expectedOutputs[txNonExtended.Inputs[i].PreviousTxIDChainHash().String()][0].Satoshis, input.PreviousTxSatoshis)
			}

			if !bytes.Equal(input.PreviousTxScript.Bytes(), expectedOutputs[txNonExtended.Inputs[i].PreviousTxIDChainHash().String()][0].LockingScript.Bytes()) {
				t.Errorf("Expected PreviousTxScript %s, got %s", expectedOutputs[txNonExtended.Inputs[i].PreviousTxIDChainHash().String()][0].LockingScript, input.PreviousTxScript)
			}
		}
	})
}

var tx, _ = bt.NewTxFromString("010000000000000000ef01c2945d5f275f6eee3a4e0c98382f0851a670e839e7e56453fbe6c78ddc093ab7000000006a4730440220633afe2995ed52b7f67c8c01efc2e4db73490de57e9a619319987e8f850c661b022032f59a4987b5ecee94f1f7c1e0411bea8c7709cdcf358499bc92dffad5646523412103184f5441e86260412485efa64e31b7a6f9f7c078078abe685ca53db35701471effffffff00f2052a010000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88acfdf40180969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac80969800000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac00000000")

func TestFalseOrEmptyTopStackElementScriptError(t *testing.T) {
	tSettings := settings.NewSettings("operator.teratestnet")
	// tSettings.ChainCfgParams = &chaincfg.TeraTestNetParams
	tSettings.Policy.MinMiningTxFee = 0

	var height uint32 = 0

	v := &Validator{
		txValidator: NewTxValidator(ulogger.TestLogger{}, tSettings),
	}

	ctx := context.Background()

	ctx, _, endSpan := tracing.Tracer("validator").Start(ctx, "Test")
	defer endSpan()

	err := v.validateTransactionScripts(ctx, tx, height, []uint32{}, &Options{SkipPolicyChecks: true})
	require.Error(t, err)
}

func TestValidator_TwoPhaseCommitTransaction(t *testing.T) {
	tracing.SetupMockTracer()

	tx, err := bt.NewTxFromString("010000000000000000ef01b136c673a9b815af2bfdeccc9479deec3273ee98a188c26d3c14b5e6bfcbca0b010000006b48304502200241ac9536c536f21e522dec152e69674094b371b14c26edf706e1db0e6487190221008ee66bdafc7d39ee041e1425a7b2df780702e9b066c3a1e9715b03b23fbd99be41210373c9cb2feaa59dd208ad90dc4c8f32dac7a30a65e590fa16e2a421637927ae63feffffff4004fb0b000000001976a91471902a65346b0d951358ec9a1b306ecd36d284ae88ac0280969800000000001976a914dd37ee4ce93278fbc398abcda001d1d855841e0788ac3cd35d0b000000001976a914d04ad25d93764cf83aca0ca0c7cbb7ba8850f75888ac00000000")
	require.NoError(t, err)

	ctx := context.Background()
	logger := ulogger.NewErrorTestLogger(t)

	tSettings := test.CreateBaseTestSettings(t)

	utxoStoreURL, err := url.Parse("sqlitememory:///test")
	require.NoError(t, err)

	utxoStore, err := sql.New(ctx, logger, tSettings, utxoStoreURL)
	require.NoError(t, err)

	_, err = utxoStore.Create(ctx, tx, 100, utxostore.WithLocked(true))
	require.NoError(t, err)

	meta, err := utxoStore.GetMeta(ctx, tx.TxIDChainHash())
	require.NoError(t, err)
	require.True(t, meta.Locked)

	v := &Validator{
		logger:      ulogger.TestLogger{},
		utxoStore:   utxoStore,
		settings:    test.CreateBaseTestSettings(t),
		txValidator: NewTxValidator(ulogger.TestLogger{}, test.CreateBaseTestSettings(t)),
		stats:       gocore.NewStat("validator"),
	}

	ctx, _, endSpan := tracing.Tracer("validator").Start(context.Background(), "Test")
	defer endSpan()

	err = v.twoPhaseCommitTransaction(ctx, tx, tx.TxID())
	require.NoError(t, err)

	meta, err = utxoStore.GetMeta(ctx, tx.TxIDChainHash())
	require.NoError(t, err)
	assert.False(t, meta.Locked)
}

func TestValidator_TwoPhaseCommitTransaction_AlreadySpendable(t *testing.T) {
	tracing.SetupMockTracer()

	ctx := context.Background()

	tx, err := bt.NewTxFromString("010000000000000000ef01b136c673a9b815af2bfdeccc9479deec3273ee98a188c26d3c14b5e6bfcbca0b010000006b48304502200241ac9536c536f21e522dec152e69674094b371b14c26edf706e1db0e6487190221008ee66bdafc7d39ee041e1425a7b2df780702e9b066c3a1e9715b03b23fbd99be41210373c9cb2feaa59dd208ad90dc4c8f32dac7a30a65e590fa16e2a421637927ae63feffffff4004fb0b000000001976a91471902a65346b0d951358ec9a1b306ecd36d284ae88ac0280969800000000001976a914dd37ee4ce93278fbc398abcda001d1d855841e0788ac3cd35d0b000000001976a914d04ad25d93764cf83aca0ca0c7cbb7ba8850f75888ac00000000")
	require.NoError(t, err)

	logger := ulogger.NewErrorTestLogger(t)

	tSettings := test.CreateBaseTestSettings(t)

	utxoStoreURL, err := url.Parse("sqlitememory:///test")
	require.NoError(t, err)

	utxoStore, err := sql.New(ctx, logger, tSettings, utxoStoreURL)
	require.NoError(t, err)

	// Add the tx as spendable (Locked == false)
	_, err = utxoStore.Create(ctx, tx, 100, utxostore.WithLocked(false))
	require.NoError(t, err)

	meta, err := utxoStore.GetMeta(ctx, tx.TxIDChainHash())
	require.NoError(t, err)
	assert.False(t, meta.Locked, "TX should be spendable before 2-phase commit")

	v := &Validator{
		logger:      ulogger.TestLogger{},
		utxoStore:   utxoStore,
		settings:    test.CreateBaseTestSettings(t),
		txValidator: NewTxValidator(ulogger.TestLogger{}, test.CreateBaseTestSettings(t)),
		stats:       gocore.NewStat("validator"),
	}

	ctx, _, endSpan := tracing.Tracer("validator").Start(context.Background(), "Test")
	defer endSpan()

	err = v.twoPhaseCommitTransaction(ctx, tx, tx.TxID())
	assert.NoError(t, err)

	meta, err = utxoStore.GetMeta(ctx, tx.TxIDChainHash())
	require.NoError(t, err)
	assert.False(t, meta.Locked, "TX should remain spendable after 2-phase commit")
}

// FailingUtxoStore provides a test double that simulates UTXO store failures.
type FailingUtxoStore struct {
	*sql.Store
}

func (f *FailingUtxoStore) SetLocked(ctx context.Context, hashes []chainhash.Hash, locked bool) error {
	return errors.NewProcessingError("throws error")
}

func NewFailingUtxoStore(t *testing.T) *FailingUtxoStore {
	ctx := context.Background()
	logger := ulogger.TestLogger{}

	tSettings := test.CreateBaseTestSettings(t)

	utxoStoreURL, err := url.Parse("sqlitememory:///test")
	if err != nil {
		panic(err)
	}

	utxoStore, err := sql.New(ctx, logger, tSettings, utxoStoreURL)
	if err != nil {
		panic(err)
	}

	return &FailingUtxoStore{
		Store: utxoStore,
	}
}

func TestValidator_TwoPhaseCommitTransaction_SetLockedFails(t *testing.T) {
	tracing.SetupMockTracer()

	ctx := t.Context()

	tx, err := bt.NewTxFromString("010000000000000000ef01b136c673a9b815af2bfdeccc9479deec3273ee98a188c26d3c14b5e6bfcbca0b010000006b48304502200241ac9536c536f21e522dec152e69674094b371b14c26edf706e1db0e6487190221008ee66bdafc7d39ee041e1425a7b2df780702e9b066c3a1e9715b03b23fbd99be41210373c9cb2feaa59dd208ad90dc4c8f32dac7a30a65e590fa16e2a421637927ae63feffffff4004fb0b000000001976a91471902a65346b0d951358ec9a1b306ecd36d284ae88ac0280969800000000001976a914dd37ee4ce93278fbc398abcda001d1d855841e0788ac3cd35d0b000000001976a914d04ad25d93764cf83aca0ca0c7cbb7ba8850f75888ac00000000")
	require.NoError(t, err)

	utxoStore := NewFailingUtxoStore(t)
	_, _ = utxoStore.Create(ctx, tx, 100, utxostore.WithLocked(true))

	v := &Validator{
		logger:      ulogger.TestLogger{},
		utxoStore:   utxoStore,
		settings:    test.CreateBaseTestSettings(t),
		txValidator: NewTxValidator(ulogger.TestLogger{}, test.CreateBaseTestSettings(t)),
		stats:       gocore.NewStat("validator"),
	}

	ctx, _, endSpan := tracing.Tracer("validator").Start(context.Background(), "Test")
	defer endSpan()

	err = v.twoPhaseCommitTransaction(ctx, tx, tx.TxID())
	assert.Error(t, err)

	meta, err := utxoStore.GetMeta(ctx, tx.TxIDChainHash())
	require.NoError(t, err)
	require.True(t, meta.Locked)
}

func TestValidator_LockedFlagChangedIfBlockAssemblyStoreSucceeds(t *testing.T) {
	tracing.SetupMockTracer()

	ctx := context.Background()
	logger := ulogger.NewErrorTestLogger(t)

	tSettings := test.CreateBaseTestSettings(t)

	utxoStoreURL, err := url.Parse("sqlitememory:///test")
	require.NoError(t, err)

	utxoStore, err := sql.New(ctx, logger, tSettings, utxoStoreURL)
	require.NoError(t, err)

	txs := transactions.CreateTestTransactionChainWithCount(t, 3)

	_, err = utxoStore.Create(
		ctx,
		txs[0],
		1,
		utxostore.WithLocked(false),
	)
	require.NoError(t, err)

	blockAsmMock := blockassembly.NewMock()

	settings := test.CreateBaseTestSettings(t)
	settings.BlockAssembly.Disabled = false

	v := &Validator{
		logger:         ulogger.TestLogger{},
		utxoStore:      utxoStore,
		blockAssembler: blockAsmMock,
		settings:       settings,
		txValidator:    NewTxValidator(ulogger.TestLogger{}, test.CreateBaseTestSettings(t)),
		stats:          gocore.NewStat("validator"),
	}

	blockAsmMock.On("Store", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(true, nil).Times(1)

	err = utxoStore.SetBlockHeight(2)
	require.NoError(t, err)

	opts := &Options{AddTXToBlockAssembly: true}

	_, err = v.ValidateWithOptions(ctx, txs[1], 2, opts)
	require.NoError(t, err)

	meta, err := utxoStore.GetMeta(ctx, txs[1].TxIDChainHash())
	require.NoError(t, err)
	assert.False(t, meta.Locked, "Flag should be unset after successful block assembly")
}

func TestValidator_LockedFlagNotChangedIfBlockAssemblyDidNotStoreTx(t *testing.T) {
	tracing.SetupMockTracer()

	ctx := context.Background()

	logger := ulogger.NewErrorTestLogger(t)

	tSettings := test.CreateBaseTestSettings(t)

	utxoStoreURL, err := url.Parse("sqlitememory:///test")
	require.NoError(t, err)

	utxoStore, err := sql.New(ctx, logger, tSettings, utxoStoreURL)
	require.NoError(t, err)

	txs := transactions.CreateTestTransactionChainWithCount(t, 3)

	_, err = utxoStore.Create(
		ctx,
		txs[0],
		1,
		utxostore.WithLocked(false),
	)
	require.NoError(t, err)

	_, err = utxoStore.Create(ctx, txs[1], 2, utxostore.WithLocked(true))
	require.NoError(t, err)

	blockAsmMock := blockassembly.NewMock()

	settings := test.CreateBaseTestSettings(t)
	settings.BlockAssembly.Disabled = false

	v := &Validator{
		logger:         ulogger.TestLogger{},
		utxoStore:      utxoStore,
		blockAssembler: blockAsmMock,
		settings:       settings,
		txValidator:    NewTxValidator(ulogger.TestLogger{}, test.CreateBaseTestSettings(t)),
		stats:          gocore.NewStat("validator"),
	}

	blockAsmMock.On("Store", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(false, errors.New(errors.ERR_PROCESSING, "tx rejected")).Once()

	opts := &Options{AddTXToBlockAssembly: true}

	err = utxoStore.SetBlockHeight(2) // We need to set this for the SQL implementation
	require.NoError(t, err)

	_, err = v.ValidateWithOptions(ctx, txs[1], 2, opts)
	require.NoError(t, err)

	meta, err := utxoStore.GetMeta(ctx, txs[1].TxIDChainHash())
	require.NoError(t, err)
	assert.True(t, meta.Locked, "Flag should be set if block assembly did not store tx")
}
