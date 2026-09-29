package validator

import (
	"context"
	"net/url"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo/sql"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/kafka"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/bsv-blockchain/teranode/util/tracing"
	"github.com/ordishs/gocore"
	"github.com/stretchr/testify/require"
)

// TestValidate_MissingParentTransactionNotAnnounced pins that a transaction
// whose parent has not yet been seen is NOT published to the rejected-tx
// topic. An internal rejection carries an empty peer_id, and the p2p
// rejectedTxHandler re-broadcasts exactly those to the whole network, so
// announcing here would gossip "rejected" for a transaction that is valid and
// will succeed once its parent lands - at a rate proportional to the
// out-of-order delivery this Kafka topic produces by design.
//
// The drop stays observable through prometheusMissingParentTransactions,
// incremented on the Kafka intake path in Server.go.
func TestValidate_MissingParentTransactionNotAnnounced(t *testing.T) {
	tracing.SetupMockTracer()

	// Extended tx (has inline previous-output info) whose referenced parent
	// txid is deliberately never created in the UTXO store below.
	txHex := "010000000000000000ef01febe0cbd7d87d44cbd4b5adac0a5bfcdbd2b672c9113f5d74a6459a2b85569db010000008b48304502207ec38d0a4ef79c3a4286ba3e5a5b6ede1fa678af9242465140d78a901af9e4e0022100c26c377d44b761469cf0bdcdbf4931418f2c5a02ce6b72bbb7af52facd7228c1014104bc9eb4fe4cb53e35df7e7734c4c3cd91c6af7840be80f4a1fff283e2cd6ae8f7713cb263a4590263240e3c01ec36bc603c32281ac08773484dc69b8152e48cecffffffff60b74700000000001976a9148ac9bdc626352d16e18c26f431e834f9aae30e2888ac0230424700000000001976a9148ac9bdc626352d16e18c26f431e834f9aae30e2888ac1027000000000000166a148ac9bdc626352d16e18c26f431e834f9aae30e2800000000"
	tx, err := bt.NewTxFromString(txHex)
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

	// Deliberately do NOT create the parent tx referenced by tx's input:
	// the store's Get for it will come back ErrTxNotFound, which the
	// validator converts to ErrTxMissingParent.

	initPrometheusMetrics()

	txmetaKafkaProducerClient := kafka.NewKafkaAsyncProducerMock()
	rejectedTxKafkaProducerClient := kafka.NewKafkaAsyncProducerMock()

	v := &Validator{
		logger:                        logger,
		settings:                      tSettings,
		txValidator:                   NewTxValidator(logger, tSettings),
		utxoStore:                     utxoStore,
		stats:                         gocore.NewStat("validator"),
		txmetaKafkaProducerClient:     txmetaKafkaProducerClient,
		rejectedTxKafkaProducerClient: rejectedTxKafkaProducerClient,
	}

	_, err = v.Validate(ctx, tx, 257727, WithSkipPolicyChecks(true))
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.ErrTxMissingParent), "expected ErrTxMissingParent, got: %v", err)

	require.Equal(t, 0, len(txmetaKafkaProducerClient.PublishChannel()), "txMetaKafkaChan should be empty")
	require.Equal(t, 0, len(rejectedTxKafkaProducerClient.PublishChannel()),
		"a missing-parent drop must not reach the rejected-tx topic: internal rejections are re-broadcast to the whole p2p network")
}
